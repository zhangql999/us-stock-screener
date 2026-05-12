#!/usr/bin/env python3
import json, subprocess, sys, re, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter

import requests
import feedparser
from bs4 import BeautifulSoup

RECEIVE_ID  = "ou_d364ab80c415a76fc4de9a1667cd20d2"
SCREENER    = "/home/ht/US_stock/stock_screener.py"

# GitHub Models API (Copilot Pro 付费会员)
GITHUB_TOKEN = subprocess.check_output(["gh", "auth", "token"]).decode().strip()
GITHUB_MODELS_URL = "https://models.inference.ai.azure.com/chat/completions"
# 模型降级链: GPT-4o → GPT-4o-mini → Llama-405B
GITHUB_MODELS = ["gpt-4o", "gpt-4o-mini", "Meta-Llama-3.1-405B-Instruct"]

# Follow Builders — 中央 feed (公开, 无需 API key)
FOLLOW_BUILDERS_FEEDS = {
    "x":        "https://raw.githubusercontent.com/zarazhangrui/follow-builders/main/feed-x.json",
    "podcasts": "https://raw.githubusercontent.com/zarazhangrui/follow-builders/main/feed-podcasts.json",
    "blogs":    "https://raw.githubusercontent.com/zarazhangrui/follow-builders/main/feed-blogs.json",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def safe_get(url, timeout=15):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        r.encoding = r.apparent_encoding or "utf-8"
        return r
    except Exception as e:
        log(f"  GET fail {url[:55]}: {e}")
        return None

def fetch_rss(name, url, keywords=None, n=8):
    items = []
    try:
        feed = feedparser.parse(url)
        for e in feed.entries[:40]:
            title   = e.get("title", "")
            summary = BeautifulSoup(e.get("summary", ""), "html.parser").get_text()[:200]
            if keywords and not any(k.lower() in (title+summary).lower() for k in keywords):
                continue
            items.append(f"{title}. {summary}".strip())
            if len(items) >= n:
                break
    except Exception as ex:
        log(f"  RSS {name}: {ex}")
    return items

def fetch_jin10():
    items = []
    r = safe_get("https://flash.jin10.com/", timeout=20)
    if r:
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup.find_all(string=True):
            t = tag.strip()
            if 20 < len(t) < 300 and any(k in t for k in [
                "美股","美联储","纳斯达克","标普","道琼","关税","经济",
                "CPI","GDP","利率","非农","就业","债券","黄金","原油"
            ]):
                items.append(t)
                if len(items) >= 15:
                    break
    if len(items) < 3:
        r2 = safe_get("https://www.jin10.com/", timeout=20)
        if r2:
            soup = BeautifulSoup(r2.text, "html.parser")
            for tag in soup.select("[class*='flash'],[class*='news'],[class*='item']")[:30]:
                t = tag.get_text(strip=True)
                if 20 < len(t) < 300:
                    items.append(t)
    return items[:15]

def fetch_wallstreetcn():
    return fetch_rss("华尔街见闻", "https://wallstreetcn.com/rss",
        keywords=["美股","美联储","纳斯达克","标普","关税","CPI","GDP","利率","非农","黄金","原油"], n=8)

def fetch_fed():
    items = []
    for name, url in [
        ("Fed Speech",   "https://www.federalreserve.gov/feeds/speeches.xml"),
        ("Fed Monetary", "https://www.federalreserve.gov/feeds/press_monetary.xml"),
    ]:
        items.extend(fetch_rss(name, url, n=3))
    return items[:6]

def fetch_reuters():
    return fetch_rss("Reuters", "https://feeds.reuters.com/reuters/businessNews",
        keywords=["stock","fed","rate","economy","tariff","trade","inflation","nasdaq","s&p"], n=8)

def fetch_wsj():
    return fetch_rss("MarketWatch", "https://feeds.content.dowjones.io/public/rss/mw_bulletins",
        keywords=["market","stock","fed","rate","economy","earnings","nasdaq"], n=6)

def fetch_finviz():
    items = []
    r = safe_get("https://finviz.com/news.ashx", timeout=20)
    if r:
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True)[:80]:
            txt  = a.get_text(strip=True)
            href = a["href"]
            if len(txt) > 25 and any(x in href for x in ["news","story","article"]):
                items.append(txt)
                if len(items) >= 12:
                    break
    return items[:12]

def fetch_macro():
    result = {}
    tickers = {
        "SPY":      "S&P500",
        "QQQ":      "纳斯达克100",
        "%5EVIX":   "VIX恐慌指数",
        "TLT":      "20Y美债ETF",
        "GC%3DF":   "黄金",
        "CL%3DF":   "原油",
        "BTC-USD":  "比特币",
        "DX-Y.NYB": "美元指数",
    }
    for sym, name in tickers.items():
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d"
            r   = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
            meta  = r.json()["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice") or meta.get("previousClose", 0)
            prev  = meta.get("chartPreviousClose") or meta.get("previousClose", price)
            chg   = (price - prev) / prev * 100 if prev else 0
            result[name] = f"{price:.2f} {'▲' if chg>=0 else '▼'}{abs(chg):.2f}%"
        except Exception as e:
            log(f"  macro {name}: {e}")
    return result

def fetch_sector():
    sectors = {"XLK":"科技","XLF":"金融","XLE":"能源","XLV":"医疗","XLI":"工业","XLY":"消费"}
    result  = {}
    for sym, name in sectors.items():
        try:
            url  = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d"
            r    = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=8)
            meta = r.json()["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice", 0)
            prev  = meta.get("chartPreviousClose", price)
            chg   = (price - prev) / prev * 100 if prev else 0
            result[name] = f"{'▲' if chg>=0 else '▼'}{abs(chg):.2f}%"
        except Exception:
            pass
    return result

def fetch_x_rss():
    items = []
    for name, url in [
        ("ZeroHedge",     "https://feeds.feedburner.com/zerohedge/feed"),
        ("UnusualWhales", "https://unusualwhales.com/rss.xml"),
        ("MacroCompass",  "https://themacrocompass.substack.com/feed"),
    ]:
        got = fetch_rss(name, url, n=4)
        items.extend([f"[{name}] {x}" for x in got])
    return items[:12]

def fetch_calendar():
    items = []
    r = safe_get("https://finance.yahoo.com/calendar/economic/", timeout=20)
    if r:
        soup = BeautifulSoup(r.text, "html.parser")
        for row in soup.select("tr")[:15]:
            cells = [td.get_text(strip=True) for td in row.select("td")]
            line  = " | ".join(c for c in cells[:5] if c)
            if len(line) > 10:
                items.append(line)
    return items[:8] or ["（暂无数据）"]

def fetch_ai_builders():
    """从 Follow Builders 中央 feed 拉取 AI 大佬动态"""
    result = {"x_digest": [], "podcast_digest": [], "blog_digest": []}

    # X/Twitter — AI Builders 最新推文
    try:
        r = requests.get(FOLLOW_BUILDERS_FEEDS["x"], timeout=15)
        if r.ok:
            data = r.json()
            for builder in data.get("x", []):
                name = builder.get("name", "")
                handle = builder.get("handle", "")
                for tweet in builder.get("tweets", [])[:2]:  # 每人最多2条
                    text = tweet.get("text", "").replace("\n", " ")[:200]
                    if text:
                        result["x_digest"].append(f"@{handle} ({name}): {text}")
    except Exception as e:
        log(f"  AI Builders X feed: {e}")

    # Podcasts — AI 播客最新一期
    try:
        r = requests.get(FOLLOW_BUILDERS_FEEDS["podcasts"], timeout=15)
        if r.ok:
            data = r.json()
            for ep in data.get("podcasts", []):
                title = ep.get("title", "")
                show = ep.get("name", "")
                summary = ep.get("summary", "") or ep.get("description", "")
                if title:
                    line = f"[{show}] {title}"
                    if summary:
                        line += f" — {summary[:150]}"
                    result["podcast_digest"].append(line)
    except Exception as e:
        log(f"  AI Builders podcast feed: {e}")

    # Blogs — Anthropic/OpenAI 官方博客
    try:
        r = requests.get(FOLLOW_BUILDERS_FEEDS["blogs"], timeout=15)
        if r.ok:
            data = r.json()
            for post in data.get("blogs", []):
                title = post.get("title", "")
                source = post.get("name", "")
                content = post.get("content", "")[:200] or post.get("description", "")[:200]
                if title:
                    result["blog_digest"].append(f"[{source}] {title}: {content}")
    except Exception as e:
        log(f"  AI Builders blog feed: {e}")

    total = len(result["x_digest"]) + len(result["podcast_digest"]) + len(result["blog_digest"])
    return result, total

def run_screener():
    try:
        res = subprocess.run(["/usr/bin/python3", SCREENER],
                             capture_output=True, text=True, timeout=600)
        return res.stdout
    except subprocess.TimeoutExpired:
        return "[选股超时]"
    except Exception as e:
        return f"[选股异常:{e}]"

def parse_screener_full(raw):
    lines = raw.split("\n")

    short_re = re.compile(r"#\s*(\d+)\s+(\w+)\s+(.*?)\s+总分:\s*([\d.]+).*?涨跌:\s*([+\-\d.%]+).*?止损:(\S+)")
    long_re  = re.compile(r"#\s*(\d+)\s+(\w+)\s+(.*?)\s+总分:\s*([\d.]+).*?护城河:([\d.]+)/10.*?技术:([\d.]+)")
    top_re   = re.compile(r"#\s*(\d+)\s+(\w+)\s+(.*?)\s+总分:\s*([\d.]+).*?涨跌:\s*([+\-\d.%]+).*?\[(.+?)\].*?持有:(.+)")
    hold_re  = re.compile(r"建议持有[：:]\s*(.+)")
    tip_re   = re.compile(r"操作建议[：:]\s*(.+)")

    short_stocks, long_stocks, top_stocks = [], [], []
    section = None

    cur_stock = None
    stock_detail = {}

    for line in lines:
        if "短线交易" in line and "1-5" in line:
            section = "short"
        elif "长线布局" in line and "3-12" in line:
            section = "long"
        elif "综合排名" in line or "TOP 15" in line:
            section = "top"

        if section == "short":
            m = short_re.search(line)
            if m and len(short_stocks) < 8:
                short_stocks.append({
                    "rank": m.group(1), "ticker": m.group(2),
                    "name": m.group(3).strip(), "score": m.group(4),
                    "chg": m.group(5), "stoploss": m.group(6),
                    "tip": "", "hold": "1-5天"
                })
            elif short_stocks:
                t = tip_re.search(line)
                if t:
                    short_stocks[-1]["tip"] = t.group(1).strip()
                h = hold_re.search(line)
                if h:
                    short_stocks[-1]["hold"] = h.group(1).strip()
            if "🏢" in line and short_stocks:
                short_stocks[-1]["desc"] = line.strip().lstrip("🏢").strip()

        elif section == "long":
            m = long_re.search(line)
            if m and len(long_stocks) < 8:
                long_stocks.append({
                    "rank": m.group(1), "ticker": m.group(2),
                    "name": m.group(3).strip(), "score": m.group(4),
                    "moat": m.group(5), "tech": m.group(6),
                    "tip": "", "hold": "3-12月", "desc": ""
                })
            elif long_stocks:
                t = tip_re.search(line)
                if t:
                    long_stocks[-1]["tip"] = t.group(1).strip()
                h = hold_re.search(line)
                if h:
                    long_stocks[-1]["hold"] = h.group(1).strip()
            if "🏢" in line and long_stocks:
                long_stocks[-1]["desc"] = line.strip().lstrip("🏢").strip()

        elif section == "top":
            m = top_re.search(line)
            if m and len(top_stocks) < 5:
                top_stocks.append({
                    "rank": m.group(1), "ticker": m.group(2),
                    "name": m.group(3).strip(), "score": m.group(4),
                    "chg": m.group(5), "type": m.group(6),
                    "hold": m.group(7).strip()
                })

    return short_stocks, long_stocks, top_stocks

# ═══════════════════════════════════════════════════════════════════════
# 验证层: 多源交叉验证选股结果
# ═══════════════════════════════════════════════════════════════════════

def verify_stocks(stocks, macro, all_news):
    """
    对选股器输出的每只股票做多源交叉验证:
      1. yfinance: 量价确认 (量比>1.5? 站上MA5? 动量方向?)
      2. BTC/VIX: 风险环境 (VIX>25? BTC暴跌>5%?)
      3. 新闻情绪: 有无该股重大利空关键词
    返回带验证标签的 stocks 列表
    """
    if not stocks:
        return stocks

    # ── 全局风险环境判定 ──
    vix_str = macro.get("VIX恐慌指数", "")
    btc_str = macro.get("比特币", "")
    vix_val = _extract_num(vix_str)
    btc_chg = _extract_chg(btc_str)

    env_risk = 0  # 0=正常, 1=偏高, 2=恶劣
    env_warns = []
    if vix_val and vix_val > 30:
        env_risk = 2
        env_warns.append(f"VIX={vix_val:.1f}恐慌")
    elif vix_val and vix_val > 25:
        env_risk = 1
        env_warns.append(f"VIX={vix_val:.1f}偏高")
    if btc_chg is not None and btc_chg < -5:
        env_risk = max(env_risk, 1)
        env_warns.append(f"BTC{btc_chg:+.1f}%暴跌")

    # 合并所有新闻文本用于关键词匹配
    news_text = " ".join(all_news).lower()

    # ── 逐只验证 ──
    tickers = [s["ticker"] for s in stocks]
    price_data = _batch_verify_price(tickers)

    for s in stocks:
        ticker = s["ticker"]
        checks_pass = 0
        checks_fail = 0
        verify_notes = []

        # 1. 量价确认
        pd = price_data.get(ticker, {})
        vol_ratio = pd.get("vol_ratio", 0)
        above_ma5 = pd.get("above_ma5", None)
        momentum = pd.get("momentum", 0)  # 近5日涨幅

        if vol_ratio >= 1.5:
            checks_pass += 1
            verify_notes.append(f"量比{vol_ratio:.1f}x✓")
        elif vol_ratio > 0:
            checks_fail += 1
            verify_notes.append(f"量比{vol_ratio:.1f}x✗")

        if above_ma5 is True:
            checks_pass += 1
            verify_notes.append("站上MA5✓")
        elif above_ma5 is False:
            checks_fail += 1
            verify_notes.append("跌破MA5✗")

        if momentum > 0:
            checks_pass += 1
        elif momentum < -3:
            checks_fail += 1
            verify_notes.append(f"5日动量{momentum:.1f}%✗")

        # 2. 风险环境
        if env_risk >= 2:
            checks_fail += 1
            verify_notes.append("市场恐慌✗")
        elif env_risk == 0:
            checks_pass += 1

        # 3. 新闻利空检测
        tk_lower = ticker.lower()
        neg_keywords = ["lawsuit", "fraud", "sec investigation", "recall", "downgrade",
                        "诉讼", "欺诈", "调查", "召回", "下调", "暴雷", "爆雷", "退市"]
        has_neg = any(tk_lower in news_text and kw in news_text for kw in neg_keywords)
        if has_neg:
            checks_fail += 2
            verify_notes.append("新闻利空⚠")
        else:
            checks_pass += 1

        # ── 综合判定 ──
        if checks_fail >= 3 or (env_risk >= 2 and checks_fail >= 2):
            verdict = "❌否决"
        elif checks_fail >= 2 or env_risk >= 1:
            verdict = "⚠️存疑"
        else:
            verdict = "✅确认"

        s["verify"] = verdict
        s["verify_detail"] = " | ".join(verify_notes) if verify_notes else "数据不足"
        if env_warns:
            s["env_warn"] = "｜".join(env_warns)

    return stocks


def _extract_num(s):
    """从 '18.50 ▲0.32%' 中提取第一个数字"""
    m = re.search(r"([\d.]+)", s)
    return float(m.group(1)) if m else None


def _extract_chg(s):
    """从 '65000.00 ▼5.20%' 中提取涨跌幅"""
    m = re.search(r"[▲▼](\d+\.?\d*)", s)
    if not m:
        return None
    val = float(m.group(1))
    return -val if "▼" in s else val


def _batch_verify_price(tickers):
    """批量从 Yahoo chart 获取量价验证数据"""
    results = {}

    def _check_one(ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=10d"
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            data = r.json()["chart"]["result"][0]
            meta = data["meta"]
            indicators = data["indicators"]["quote"][0]

            closes = [c for c in (indicators.get("close") or []) if c is not None]
            volumes = [v for v in (indicators.get("volume") or []) if v is not None]

            if not closes or len(closes) < 5:
                return ticker, {}

            cur_price = closes[-1]
            ma5 = sum(closes[-5:]) / 5

            # 量比: 今日量 / 前5日均量
            avg_vol = sum(volumes[-6:-1]) / 5 if len(volumes) >= 6 else (sum(volumes[:-1]) / max(len(volumes)-1, 1))
            cur_vol = volumes[-1] if volumes else 0
            vol_ratio = cur_vol / max(avg_vol, 1)

            # 5日动量
            momentum = (closes[-1] / closes[-5] - 1) * 100 if len(closes) >= 5 else 0

            return ticker, {
                "vol_ratio": vol_ratio,
                "above_ma5": cur_price > ma5,
                "momentum": momentum,
                "cur_price": cur_price,
            }
        except Exception:
            return ticker, {}

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_check_one, t): t for t in tickers}
        for fut in as_completed(futures):
            ticker, data = fut.result()
            results[ticker] = data

    return results


def call_llm(prompt: str) -> str:
    """调用 GitHub Models API (OpenAI兼容格式), 支持降级重试"""
    for model in GITHUB_MODELS:
        for attempt in range(3):
            try:
                r = requests.post(
                    GITHUB_MODELS_URL,
                    headers={
                        "Authorization": f"Bearer {GITHUB_TOKEN}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": "你是顶级美股宏观交易员，擅长整合全球信息做每日操盘分析。回答简洁精准。"},
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.3,
                        "max_tokens": 1500,
                    },
                    timeout=60,
                )
                if r.status_code == 429:
                    wait = (attempt + 1) * 5
                    log(f"  {model} 限流(429), {wait}s后重试...")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r.json()["choices"][0]["message"]["content"].strip()
            except requests.exceptions.HTTPError as e:
                if r.status_code == 429:
                    wait = (attempt + 1) * 5
                    log(f"  {model} 限流(429), {wait}s后重试...")
                    time.sleep(wait)
                    continue
                log(f"  {model} HTTP错误: {e}")
                break
            except Exception as e:
                log(f"  {model} 异常: {e}")
                break
        log(f"  模型 {model} 失败, 尝试下一个...")
    log("  所有模型均失败")
    return None

def rule_based_6q(intel, top_stocks, macro, sectors):
    all_news = (intel.get("jin10",[]) + intel.get("wallstreetcn",[]) +
                intel.get("reuters",[]) + intel.get("wsj",[]) + intel.get("finviz",[]))[:30]
    vix_str  = macro.get("VIX恐慌指数", "N/A")
    spy_str  = macro.get("S&P500", "N/A")
    qqq_str  = macro.get("纳斯达克100", "N/A")
    gold_str = macro.get("黄金", "N/A")
    oil_str  = macro.get("原油", "N/A")

    vix_val = float(re.search(r"[\d.]+", vix_str).group()) if re.search(r"[\d.]+", vix_str) else 20
    risk    = "极度恐慌" if vix_val > 35 else ("市场恐慌" if vix_val > 25 else ("波动偏高" if vix_val > 20 else "市场平稳"))

    hot_kw = []
    for n in all_news:
        for kw in ["美联储","关税","CPI","GDP","非农","加息","降息","财报","芯片","AI","能源","地缘"]:
            if kw in n:
                hot_kw.append(kw)
    mainline   = "、".join(kw for kw, _ in Counter(hot_kw).most_common(3)) or "宏观情绪驱动"
    top_str    = "、".join(s["ticker"] for s in top_stocks[:3])
    sec_sorted = sorted(sectors.items(), key=lambda x: float(re.search(r"[\d.]+", x[1]).group() or 0), reverse=True)

    return f"""1. 今日主线是什么
当前主线围绕【{mainline}】展开。S&P500 {spy_str}，纳斯达克100 {qqq_str}，{sec_sorted[0][0] if sec_sorted else '科技'}板块最强。量化重点：{top_str}。

2. 最强信号是什么
黄金 {gold_str}，原油 {oil_str}。板块：{', '.join(f'{k}{v}' for k,v in sec_sorted[:3])}。选股系统三维共振标的见下方Top5。

3. 关键风险是什么
VIX {vix_str}（{risk}）。重点关注：美联储讲话、关税动态、财报雷区。VIX破25需降仓。

4. 今天只看哪三件事
① VIX是否突破25 ② 今日经济数据/美联储表态 ③ 量化Top标的（{top_str}）盘中量价是否确认

5. 哪些信息可以不用看
社媒短期情绪波动、无基本面支撑的个股炒作、隔夜期货<0.5%微小波动、与主线无关行业新闻。

6. 今天的核心问题是什么
在【{mainline}】背景下，主线板块能否延续？VIX能否维持低位？量化信号标的能否放量突破？"""

def build_6q(intel, short_stocks, long_stocks, top_stocks, macro, sectors):
    all_news = (intel.get("jin10",[]) + intel.get("wallstreetcn",[]) +
                intel.get("reuters",[]) + intel.get("wsj",[]) + intel.get("finviz",[]))

    short_str = "\n".join(
        f"  #{s['rank']} {s['ticker']} {s['name']} 总分{s['score']} {s['chg']} 止损{s['stoploss']} | {s.get('tip','')}"
        for s in short_stocks[:5]
    ) or "  暂无"
    long_str = "\n".join(
        f"  #{s['rank']} {s['ticker']} {s['name']} 总分{s['score']} 护城河{s['moat']} | {s.get('tip','')}"
        for s in long_stocks[:5]
    ) or "  暂无"

    prompt = f"""你是顶级美股宏观交易员，每天开盘前整合全球信息做 Daily Readout。今天是 {datetime.now().strftime('%Y年%m月%d日')}。

【宏观大盘（昨收）】
{chr(10).join(f'  {k}: {v}' for k,v in macro.items())}

【板块轮动】
{' | '.join(f'{k}{v}' for k,v in sectors.items())}

【美联储动态】
{chr(10).join(f'  {x[:150]}' for x in intel.get('fed',[])[:4]) or '  无最新讲话'}

【今日财经要闻（金十/华尔街见闻/路透/WSJ/Finviz）】
{chr(10).join(f'  - {x[:150]}' for x in all_news[:25])}

【社区情报（ZeroHedge/UnusualWhales）】
{chr(10).join(f'  {x[:150]}' for x in intel.get('x',[])[:8]) or '  暂无'}

【AI Builders 动态（Karpathy/Swyx/Sam Altman等）】
{chr(10).join(f'  {x[:150]}' for x in (intel.get('builders',{}).get('x_digest',[]) + intel.get('builders',{}).get('podcast_digest',[]))[:10]) or '  暂无'}

【今日经济日历】
{chr(10).join(f'  {x}' for x in intel.get('calendar',[])[:6]) or '  暂无'}

【量化短线精选（1-5天）】
{short_str}

【量化长线精选（3-12月）】
{long_str}

请严格按格式输出，每题2-4句话，简洁精准，直指要害：

1. 今日主线是什么
[回答]

2. 最强信号是什么
[回答]

3. 关键风险是什么
[回答]

4. 今天只看哪三件事
[回答]

5. 哪些信息可以不用看
[回答]

6. 今天的核心问题是什么
[回答]

---
【一句话操盘建议】
[回答]"""

    log("  调用 GitHub Models (GPT-4o)...")
    answer = call_llm(prompt)
    if answer:
        return answer
    log("  LLM 失败，切换规则兜底...")
    return rule_based_6q(intel, top_stocks, macro, sectors)

def get_feishu_token():
    cmd = ("curl -sS -X POST 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal' "
           "-H 'Content-Type: application/json' -d \"$(cat /home/ht/.sisyphus/feishu_app.json)\"")
    return json.loads(subprocess.check_output(cmd, shell=True).decode())["tenant_access_token"]

def send_feishu(token, text):
    import urllib.request
    payload = json.dumps({
        "receive_id": RECEIVE_ID, "msg_type": "text",
        "content": json.dumps({"text": text})
    }).encode()
    req = urllib.request.Request(
        "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id",
        data=payload, method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    )
    return json.loads(urllib.request.urlopen(req, timeout=30).read())

def build_message(intel, analysis, short_stocks, long_stocks, top_stocks, macro, sectors):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    out = [f"📡 美股情报雷达 | {now}", ""]

    if macro:
        out.append("━━━━ 📊 宏观大盘 ━━━━")
        out.extend(f"  {k}: {v}" for k, v in macro.items())
        out.append("")

    if sectors:
        out.append("━━━━ 🏭 板块轮动 ━━━━")
        out.append("  " + " | ".join(f"{k}{v}" for k, v in sectors.items()))
        out.append("")

    if intel.get("calendar"):
        out.append("━━━━ 📅 经济日历 ━━━━")
        out.extend(f"  {c}" for c in intel["calendar"][:5])
        out.append("")

    out.append("━━━━ 🧠 Daily Readout 6问 ━━━━")
    out.append(analysis)
    out.append("")

    if short_stocks:
        out.append("━━━━ 📈 短线精选 (1-5天) ━━━━")
        for s in short_stocks:
            v_tag = s.get("verify", "")
            out.append(f"  #{s['rank']} {s['ticker']} {s['name']} {v_tag}")
            out.append(f"       总分{s['score']} | {s['chg']} | 止损{s['stoploss']}")
            if s.get("verify_detail"):
                out.append(f"       🔍 验证: {s['verify_detail']}")
            if s.get("desc"):
                out.append(f"       {s['desc']}")
            if s.get("tip"):
                out.append(f"       💡 {s['tip']}")
            out.append(f"       ⏱ 建议持有: {s['hold']}")
        out.append("")

    if long_stocks:
        out.append("━━━━ 🏦 长线精选 (3-12月) ━━━━")
        for s in long_stocks:
            v_tag = s.get("verify", "")
            out.append(f"  #{s['rank']} {s['ticker']} {s['name']} {v_tag}")
            out.append(f"       总分{s['score']} | 护城河{s['moat']}/10 | 技术{s['tech']}")
            if s.get("verify_detail"):
                out.append(f"       🔍 验证: {s['verify_detail']}")
            if s.get("desc"):
                out.append(f"       {s['desc']}")
            if s.get("tip"):
                out.append(f"       💡 {s['tip']}")
            out.append(f"       ⏱ 建议持有: {s['hold']}")
        out.append("")

    if top_stocks:
        out.append("━━━━ 🏆 综合TOP5 ━━━━")
        for s in top_stocks:
            v_tag = s.get("verify", "")
            hold_tag = f"[{s.get('type','?')}] 持有:{s.get('hold','?')}"
            out.append(f"  #{s['rank']} {s['ticker']} {s['name']} 总分{s['score']} {s['chg']} | {hold_tag} {v_tag}")
            if s.get("verify_detail"):
                out.append(f"       🔍 {s['verify_detail']}")
        out.append("")

    hot = (intel.get("jin10",[]) + intel.get("wallstreetcn",[]) +
           intel.get("reuters",[]) + intel.get("wsj",[]))[:6]
    if hot:
        out.append("━━━━ 🔥 精选要闻 ━━━━")
        out.extend(f"  · {n[:120]}" for n in hot)
        out.append("")

    # AI Builders Digest
    builders = intel.get("builders", {})
    has_builders = any(builders.get(k) for k in ("x_digest", "podcast_digest", "blog_digest"))
    if has_builders:
        out.append("━━━━ 🧬 AI Builders Digest ━━━━")
        if builders.get("x_digest"):
            out.append("  📱 Builders on X:")
            for t in builders["x_digest"][:8]:
                out.append(f"    · {t[:140]}")
        if builders.get("podcast_digest"):
            out.append("  🎙 AI 播客:")
            for p in builders["podcast_digest"][:4]:
                out.append(f"    · {p[:140]}")
        if builders.get("blog_digest"):
            out.append("  📝 官方博客:")
            for b in builders["blog_digest"][:3]:
                out.append(f"    · {b[:140]}")
        out.append("")

    out.append("⚠️ 数据仅供参考，不构成投资建议")
    return "\n".join(out)

def main():
    log("=== 美股情报雷达启动 ===")

    log("[1/6] 并发抓取多源情报 + AI Builders...")
    intel = {}
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {
            ex.submit(fetch_jin10):        "jin10",
            ex.submit(fetch_wallstreetcn): "wallstreetcn",
            ex.submit(fetch_fed):          "fed",
            ex.submit(fetch_reuters):      "reuters",
            ex.submit(fetch_wsj):          "wsj",
            ex.submit(fetch_finviz):       "finviz",
            ex.submit(fetch_x_rss):        "x",
            ex.submit(fetch_calendar):     "calendar",
            ex.submit(fetch_macro):        "_macro",
            ex.submit(fetch_sector):       "_sector",
            ex.submit(fetch_ai_builders):  "_builders",
        }
        for fut in as_completed(futures):
            key = futures[fut]
            try:
                intel[key] = fut.result()
                n = len(intel[key])
                log(f"  ✓ {key}: {n} 条")
            except Exception as e:
                log(f"  ✗ {key}: {e}")
                intel[key] = {}

    macro   = intel.pop("_macro", {})
    sectors = intel.pop("_sector", {})
    builders_raw = intel.pop("_builders", ({}, 0))
    builders = builders_raw[0] if isinstance(builders_raw, tuple) else builders_raw
    intel["builders"] = builders

    log("[2/5] 运行量化选股...")
    screener_raw = run_screener()
    short_stocks, long_stocks, top_stocks = parse_screener_full(screener_raw)
    log(f"  短线{len(short_stocks)}只 长线{len(long_stocks)}只 TOP{len(top_stocks)}只")

    log("[3/6] 多源交叉验证...")
    all_news = (intel.get("jin10",[]) + intel.get("wallstreetcn",[]) +
                intel.get("reuters",[]) + intel.get("wsj",[]) + intel.get("finviz",[]))
    short_stocks = verify_stocks(short_stocks, macro, all_news)
    long_stocks = verify_stocks(long_stocks, macro, all_news)
    top_stocks = verify_stocks(top_stocks, macro, all_news)
    confirmed = sum(1 for s in (short_stocks + long_stocks) if s.get("verify") == "✅确认")
    doubted = sum(1 for s in (short_stocks + long_stocks) if s.get("verify") == "⚠️存疑")
    rejected = sum(1 for s in (short_stocks + long_stocks) if s.get("verify") == "❌否决")
    log(f"  验证结果: ✅确认{confirmed} ⚠️存疑{doubted} ❌否决{rejected}")

    log("[4/6] Gemini 6问分析...")
    analysis = build_6q(intel, short_stocks, long_stocks, top_stocks, macro, sectors)

    log("[5/6] 构建消息...")
    msg   = build_message(intel, analysis, short_stocks, long_stocks, top_stocks, macro, sectors)

    # 终端输出完整报告
    print("\n" + "═" * 70)
    print(msg)
    print("═" * 70 + "\n")

    log("[6/6] 发送飞书...")
    token = get_feishu_token()
    resp  = send_feishu(token, msg)
    if resp.get("code") == 0:
        log(f"  ✓ 飞书发送成功 msg_id={resp['data']['message_id']}")
    else:
        log(f"  ✗ 飞书发送失败: {resp}")
        sys.exit(1)

    log("=== 完成 ===")

if __name__ == "__main__":
    main()

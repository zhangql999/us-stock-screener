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
GEMINI_KEY  = open("/home/ht/.sisyphus/gemini_key").read().strip()
GEMINI_URL  = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}"

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

def call_gemini(prompt: str) -> str:
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 1200}
    }
    try:
        r = requests.post(GEMINI_URL, json=payload, timeout=60)
        r.raise_for_status()
        return r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as e:
        log(f"  Gemini error: {e}")
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

    log("  调用 Gemini 2.0 Flash...")
    answer = call_gemini(prompt)
    if answer:
        return answer
    log("  Gemini 失败，切换规则兜底...")
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
            out.append(f"  #{s['rank']} {s['ticker']} {s['name']}")
            out.append(f"       总分{s['score']} | {s['chg']} | 止损{s['stoploss']}")
            if s.get("desc"):
                out.append(f"       {s['desc']}")
            if s.get("tip"):
                out.append(f"       💡 {s['tip']}")
            out.append(f"       ⏱ 建议持有: {s['hold']}")
        out.append("")

    if long_stocks:
        out.append("━━━━ 🏦 长线精选 (3-12月) ━━━━")
        for s in long_stocks:
            out.append(f"  #{s['rank']} {s['ticker']} {s['name']}")
            out.append(f"       总分{s['score']} | 护城河{s['moat']}/10 | 技术{s['tech']}")
            if s.get("desc"):
                out.append(f"       {s['desc']}")
            if s.get("tip"):
                out.append(f"       💡 {s['tip']}")
            out.append(f"       ⏱ 建议持有: {s['hold']}")
        out.append("")

    if top_stocks:
        out.append("━━━━ 🏆 综合TOP5 ━━━━")
        for s in top_stocks:
            hold_tag = f"[{s.get('type','?')}] 持有:{s.get('hold','?')}"
            out.append(f"  #{s['rank']} {s['ticker']} {s['name']} 总分{s['score']} {s['chg']} | {hold_tag}")
        out.append("")

    hot = (intel.get("jin10",[]) + intel.get("wallstreetcn",[]) +
           intel.get("reuters",[]) + intel.get("wsj",[]))[:6]
    if hot:
        out.append("━━━━ 🔥 精选要闻 ━━━━")
        out.extend(f"  · {n[:120]}" for n in hot)
        out.append("")

    out.append("⚠️ 数据仅供参考，不构成投资建议")
    return "\n".join(out)

def main():
    log("=== 美股情报雷达启动 ===")

    log("[1/5] 并发抓取多源情报...")
    intel = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
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

    log("[2/5] 运行量化选股...")
    screener_raw = run_screener()
    short_stocks, long_stocks, top_stocks = parse_screener_full(screener_raw)
    log(f"  短线{len(short_stocks)}只 长线{len(long_stocks)}只 TOP{len(top_stocks)}只")

    log("[3/5] Gemini 6问分析...")
    analysis = build_6q(intel, short_stocks, long_stocks, top_stocks, macro, sectors)

    log("[4/5] 构建并发送飞书消息...")
    msg   = build_message(intel, analysis, short_stocks, long_stocks, top_stocks, macro, sectors)
    token = get_feishu_token()
    resp  = send_feishu(token, msg)
    if resp.get("code") == 0:
        log(f"  ✓ 成功 msg_id={resp['data']['message_id']}")
    else:
        log(f"  ✗ 失败: {resp}")
        sys.exit(1)

    log("=== 完成 ===")

if __name__ == "__main__":
    main()

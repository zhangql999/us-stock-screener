#!/usr/bin/env python3
"""
美股智能选股器 v4.0 - 长短线双轨筛选
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
双轨策略:
  [短线] 动量突破 + 量价配合 + 期权异动 + 关键位置
  [长线] 核心资产 + 护城河 + 趋势确认 + 估值安全边际

八大维度:
  [信号面] 盘前异动 | 财报日历 | 分析师评级 | 期权异动
  [质量面] 基本面质量 (ROE/毛利/负债/现金流/PE/股息)
  [技术面] 多周期 MACD + RSI + 均线系统 + 支撑阻力
  [资金面] 主力资金流向 + 筹码结构

选股口诀 (量化标准):
  ┌─────────────────────────────────────────────────┐
  │ ROE 十五以上           → ROE > 15%              │
  │ 现金覆盖九成强         → OCF / NetIncome > 90%  │
  │ 负债不过六成线         → 负债率 < 60%            │
  │ PE 不高有成长          → PEG < 2 或 PE < 25     │
  │ 股息大于两分利         → 股息率 > 2% (加分项)    │
  │ 毛利三十定价王         → 毛利率 > 30%            │
  │ 股东减少筹码聚         → 机构持仓 > 50% (加分)   │
  │ 周线 MACD 零上扬       → 周 MACD > 0 且上升     │
  │ RSI 四十到七乡         → 40 < RSI(14) < 70      │
  │ 均线多头排列涨         → MA5>MA10>MA20>MA60     │
  │ 量价齐升势头强         → 放量上涨确认趋势       │
  │ 突破关键阻力位         → 价格突破前高/平台      │
  └─────────────────────────────────────────────────┘

数据源: Yahoo Finance API (crumb auth) + Finviz Screener
"""

import requests
import json
import re
import sys
import time
import math
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# ─── 全局配置 ─────────────────────────────────────────────────────────
TOP_N = 15  # 增加输出数量
TOP_SHORT = 8  # 短线 TOP
TOP_LONG = 8   # 长线 TOP
MIN_MARKET_CAP = 500_000_000
MIN_PRICE = 5.0
PREMARKET_CHANGE_THRESHOLD = 1.5
VOLUME_RATIO_THRESHOLD = 1.2

# 基本面口诀阈值
MOAT_ROE_MIN = 0.15               # ROE > 15%
MOAT_GROSS_MARGIN_MIN = 0.30      # 毛利率 > 30%
MOAT_DEBT_RATIO_MAX = 0.60        # 负债率 < 60%
MOAT_OCF_COVERAGE_MIN = 0.90      # 经营现金流 / 净利润 > 90%
MOAT_PE_MAX = 25                   # PE < 25 (或 PEG < 2)
MOAT_PEG_MAX = 2.0                # PEG < 2
MOAT_DIVIDEND_MIN = 0.02          # 股息率 > 2%
MOAT_INSTITUTION_MIN = 0.50       # 机构持仓 > 50%

# 技术面阈值
TECH_RSI_LOW = 40
TECH_RSI_HIGH = 70
TECH_RSI_OVERSOLD = 30
TECH_RSI_OVERBOUGHT = 75

# 短线阈值
SHORT_VOL_RATIO_MIN = 1.5         # 短线要求量比 > 1.5
SHORT_BREAKOUT_PCT = 0.02         # 突破前高2%算有效突破

# ─── 公司信息库 (中文名 | 英文全称 | 大白话简介) ──────────────────────
# 格式: "TICKER": ("中文名", "English Full Name", "一句话说清楚干啥的")
COMPANY_INFO = {
    # ── 科技巨头 ──
    "AAPL":  ("苹果", "Apple Inc.", "卖iPhone、Mac电脑、iPad的，全球最赚钱的消费电子公司"),
    "MSFT":  ("微软", "Microsoft Corporation", "做Windows系统、Office办公软件、Azure云计算的"),
    "NVDA":  ("英伟达", "NVIDIA Corporation", "做显卡和AI芯片的，AI浪潮最大赢家"),
    "GOOGL": ("谷歌", "Alphabet Inc.", "做搜索引擎、YouTube、安卓系统、云计算的"),
    "GOOG":  ("谷歌", "Alphabet Inc.", "做搜索引擎、YouTube、安卓系统、云计算的"),
    "META":  ("Meta", "Meta Platforms Inc.", "Facebook/Instagram母公司，社交媒体+VR/AR"),
    "AMZN":  ("亚马逊", "Amazon.com Inc.", "全球最大电商+云计算(AWS)，啥都卖"),
    "TSLA":  ("特斯拉", "Tesla Inc.", "造电动车的，也搞自动驾驶和储能"),

    # ── 半导体/芯片 ──
    "AMD":   ("超微半导体", "Advanced Micro Devices Inc.", "做CPU和GPU芯片的，英伟达和英特尔的竞争对手"),
    "INTC":  ("英特尔", "Intel Corporation", "做电脑CPU芯片的老牌半导体公司"),
    "AVGO":  ("博通", "Broadcom Inc.", "做网络芯片和基础设施软件的半导体巨头"),
    "MU":    ("美光", "Micron Technology Inc.", "做内存和存储芯片(DRAM/NAND)的"),
    "ARM":   ("ARM", "Arm Holdings plc", "设计芯片架构的，几乎所有手机芯片都用它的技术"),
    "SMCI":  ("超微电脑", "Super Micro Computer Inc.", "做AI服务器的，给数据中心提供硬件"),
    "QCOM":  ("高通", "Qualcomm Inc.", "做手机芯片(骁龙)和5G通信技术的"),
    "TSM":   ("台积电", "Taiwan Semiconductor Mfg.", "全球最大芯片代工厂，帮苹果英伟达造芯片"),
    "ASML":  ("阿斯麦", "ASML Holding N.V.", "造光刻机的，芯片制造必备设备，全球垄断"),
    "TXN":   ("德州仪器", "Texas Instruments Inc.", "做模拟芯片和嵌入式处理器的老牌半导体"),
    "LRCX":  ("拉姆研究", "Lam Research Corporation", "做芯片制造刻蚀设备的"),
    "AMAT":  ("应用材料", "Applied Materials Inc.", "做芯片制造设备的，半导体设备龙头"),
    "KLAC":  ("科磊", "KLA Corporation", "做芯片检测和量测设备的"),
    "MRVL":  ("迈威尔", "Marvell Technology Inc.", "做数据中心和5G网络芯片的"),
    "ON":    ("安森美", "ON Semiconductor Corporation", "做汽车和工业用芯片的"),
    "ADI":   ("亚德诺", "Analog Devices Inc.", "做模拟和混合信号芯片的"),

    # ── 软件/云计算 ──
    "CRM":   ("Salesforce", "Salesforce Inc.", "做企业客户管理(CRM)云软件的，SaaS龙头"),
    "ORCL":  ("甲骨文", "Oracle Corporation", "做企业数据库和云计算的"),
    "ADBE":  ("Adobe", "Adobe Inc.", "做Photoshop、PDF、视频剪辑等创意软件的"),
    "NOW":   ("ServiceNow", "ServiceNow Inc.", "做企业IT服务管理云平台的"),
    "SNOW":  ("Snowflake", "Snowflake Inc.", "做云端数据仓库的，帮企业分析大数据"),
    "DDOG":  ("Datadog", "Datadog Inc.", "做云监控和数据分析的，帮程序员盯系统"),
    "PLTR":  ("帕兰提尔", "Palantir Technologies Inc.", "做大数据分析的，主要服务政府和军方"),
    "SHOP":  ("Shopify", "Shopify Inc.", "帮商家开网店的电商SaaS平台"),
    "WDAY":  ("Workday", "Workday Inc.", "做企业人力资源和财务管理云软件的"),
    "ZS":    ("Zscaler", "Zscaler Inc.", "做云端网络安全的，零信任架构"),
    "MDB":   ("MongoDB", "MongoDB Inc.", "做NoSQL数据库的，开发者很爱用"),

    # ── 网络安全 ──
    "PANW":  ("Palo Alto Networks", "Palo Alto Networks Inc.", "做网络安全的，防火墙和云安全龙头"),
    "CRWD":  ("CrowdStrike", "CrowdStrike Holdings Inc.", "做终端安全和云安全的，防黑客入侵"),
    "NET":   ("Cloudflare", "Cloudflare Inc.", "做网站加速(CDN)和网络安全的"),
    "FTNT":  ("飞塔", "Fortinet Inc.", "做网络安全防火墙的"),
    "S":     ("SentinelOne", "SentinelOne Inc.", "做AI驱动的终端安全防护的"),

    # ── 流媒体/娱乐 ──
    "NFLX":  ("奈飞", "Netflix Inc.", "全球最大流媒体平台，拍剧拍电影的"),
    "DIS":   ("迪士尼", "The Walt Disney Company", "做电影、主题乐园、Disney+流媒体的"),
    "SPOT":  ("Spotify", "Spotify Technology S.A.", "全球最大音乐流媒体平台"),
    "ROKU":  ("Roku", "Roku Inc.", "做智能电视系统和流媒体平台的"),

    # ── 电商/互联网 ──
    "BABA":  ("阿里巴巴", "Alibaba Group Holding Ltd.", "中国最大电商，淘宝天猫的母公司"),
    "JD":    ("京东", "JD.com Inc.", "中国第二大电商，自营物流，卖正品"),
    "PDD":   ("拼多多", "PDD Holdings Inc.", "拼多多+Temu母公司，低价电商"),
    "MELI":  ("MercadoLibre", "MercadoLibre Inc.", "拉美最大电商和支付平台，拉美版淘宝"),
    "SE":    ("Sea Limited", "Sea Limited", "东南亚互联网巨头，做游戏(Garena)、电商(Shopee)"),
    "GRAB":  ("Grab", "Grab Holdings Ltd.", "东南亚打车和外卖平台，东南亚版滴滴"),
    "EBAY":  ("eBay", "eBay Inc.", "老牌在线拍卖和电商平台"),

    # ── 金融科技 ──
    "COIN":  ("Coinbase", "Coinbase Global Inc.", "美国最大加密货币交易所"),
    "SOFI":  ("SoFi", "SoFi Technologies Inc.", "互联网银行，做贷款、投资、银行服务"),
    "SQ":    ("Block", "Block Inc.", "做移动支付(Cash App)和商户收款的，原名Square"),
    "PYPL":  ("PayPal", "PayPal Holdings Inc.", "在线支付平台，跨境电商付款常用"),
    "NU":    ("Nu Holdings", "Nu Holdings Ltd.", "拉美最大数字银行，巴西最火的互联网银行"),
    "AFRM":  ("Affirm", "Affirm Holdings Inc.", "做先买后付(BNPL)分期付款的"),
    "HOOD":  ("Robinhood", "Robinhood Markets Inc.", "零佣金炒股App，年轻人爱用"),

    # ── 出行/物流 ──
    "UBER":  ("优步", "Uber Technologies Inc.", "全球最大打车和外卖平台"),
    "LYFT":  ("Lyft", "Lyft Inc.", "美国第二大打车平台"),
    "DASH":  ("DoorDash", "DoorDash Inc.", "美国最大外卖配送平台"),

    # ── 加密货币挖矿 ──
    "MARA":  ("Marathon Digital", "Marathon Digital Holdings Inc.", "北美最大比特币矿企之一"),
    "RIOT":  ("Riot Platforms", "Riot Platforms Inc.", "挖比特币的，北美大矿场"),
    "MSTR":  ("MicroStrategy", "MicroStrategy Inc.", "大量囤比特币的软件公司，币圈风向标"),
    "CLSK":  ("CleanSpark", "CleanSpark Inc.", "挖比特币的，主打清洁能源挖矿"),

    # ── 电动车/新能源 ──
    "NIO":   ("蔚来", "NIO Inc.", "中国造高端电动车的，有换电服务"),
    "XPEV":  ("小鹏", "XPeng Inc.", "中国造智能电动车的，主打自动驾驶"),
    "LI":    ("理想", "Li Auto Inc.", "中国造增程式电动SUV的，家庭用车"),
    "RIVN":  ("Rivian", "Rivian Automotive Inc.", "美国造电动皮卡和SUV的"),
    "LCID":  ("Lucid", "Lucid Group Inc.", "美国造豪华电动轿车的"),
    "ENPH":  ("Enphase", "Enphase Energy Inc.", "做家用太阳能微型逆变器的"),
    "FSLR":  ("First Solar", "First Solar Inc.", "做太阳能电池板的"),

    # ── 传统金融 ──
    "JPM":   ("摩根大通", "JPMorgan Chase & Co.", "美国最大银行，投行+零售银行都做"),
    "V":     ("Visa", "Visa Inc.", "全球最大信用卡支付网络，刷卡就有它"),
    "MA":    ("万事达", "Mastercard Inc.", "全球第二大信用卡支付网络"),
    "GS":    ("高盛", "The Goldman Sachs Group Inc.", "华尔街顶级投行"),
    "MS":    ("摩根士丹利", "Morgan Stanley", "华尔街大投行，财富管理也很强"),
    "BAC":   ("美国银行", "Bank of America Corporation", "美国第二大银行"),
    "WFC":   ("富国银行", "Wells Fargo & Company", "美国大型零售银行"),
    "C":     ("花旗", "Citigroup Inc.", "全球性大银行"),
    "BRK.B": ("伯克希尔", "Berkshire Hathaway Inc.", "巴菲特的公司，投资控股集团"),
    "AXP":   ("美国运通", "American Express Company", "高端信用卡和支付公司"),
    "SCHW":  ("嘉信理财", "Charles Schwab Corporation", "美国最大在线券商之一"),

    # ── 医疗/制药 ──
    "UNH":   ("联合健康", "UnitedHealth Group Inc.", "美国最大医疗保险公司"),
    "LLY":   ("礼来", "Eli Lilly and Company", "做减肥药和糖尿病药的制药巨头"),
    "JNJ":   ("强生", "Johnson & Johnson", "做药品和医疗器械的老牌医药公司"),
    "ABBV":  ("艾伯维", "AbbVie Inc.", "做免疫和肿瘤药的制药公司"),
    "TMO":   ("赛默飞", "Thermo Fisher Scientific Inc.", "做实验室仪器和生命科学设备的"),
    "PFE":   ("辉瑞", "Pfizer Inc.", "大型制药公司，新冠疫苗让它火了一把"),
    "MRK":   ("默克", "Merck & Co. Inc.", "做肿瘤免疫药K药的制药巨头"),
    "BMY":   ("百时美施贵宝", "Bristol-Myers Squibb Company", "做肿瘤和心血管药的"),
    "AMGN":  ("安进", "Amgen Inc.", "做生物制药的，减肥药也在研发"),
    "GILD":  ("吉利德", "Gilead Sciences Inc.", "做抗病毒药的，丙肝和HIV药物很出名"),
    "ISRG":  ("直觉外科", "Intuitive Surgical Inc.", "做达芬奇手术机器人的"),
    "MRNA":  ("Moderna", "Moderna Inc.", "做mRNA疫苗和药物的生物科技公司"),
    "REGN":  ("再生元", "Regeneron Pharmaceuticals Inc.", "做生物制药的，眼科和免疫药强"),

    # ── 消费品/零售 ──
    "WMT":   ("沃尔玛", "Walmart Inc.", "全球最大零售超市连锁，啥都卖还便宜"),
    "COST":  ("好市多", "Costco Wholesale Corporation", "会员制仓储超市，美国版山姆店"),
    "HD":    ("家得宝", "The Home Depot Inc.", "美国最大家装建材零售商"),
    "PG":    ("宝洁", "The Procter & Gamble Company", "做日用品的，飘柔海飞丝汰渍都是它的"),
    "KO":    ("可口可乐", "The Coca-Cola Company", "卖可乐和各种饮料的，巴菲特最爱"),
    "PEP":   ("百事", "PepsiCo Inc.", "卖百事可乐和乐事薯片的"),
    "MCD":   ("麦当劳", "McDonald's Corporation", "全球最大快餐连锁，金拱门"),
    "SBUX":  ("星巴克", "Starbucks Corporation", "全球最大咖啡连锁店"),
    "NKE":   ("耐克", "NIKE Inc.", "全球最大运动品牌"),
    "LOW":   ("劳氏", "Lowe's Companies Inc.", "美国第二大家装零售商"),
    "TGT":   ("塔吉特", "Target Corporation", "美国大型连锁百货超市"),
    "LULU":  ("露露柠檬", "Lululemon Athletica Inc.", "做高端瑜伽裤和运动服的"),

    # ── 能源 ──
    "XOM":   ("埃克森美孚", "Exxon Mobil Corporation", "全球最大石油公司之一"),
    "CVX":   ("雪佛龙", "Chevron Corporation", "大型石油天然气公司"),
    "COP":   ("康菲", "ConocoPhillips", "美国大型独立石油开采公司"),
    "SLB":   ("斯伦贝谢", "SLB (Schlumberger)", "全球最大油田服务公司"),
    "OXY":   ("西方石油", "Occidental Petroleum Corporation", "石油开采公司，巴菲特重仓"),

    # ── 工业/航空航天 ──
    "BA":    ("波音", "The Boeing Company", "造飞机的，全球两大飞机制造商之一"),
    "CAT":   ("卡特彼勒", "Caterpillar Inc.", "造挖掘机和工程机械的全球龙头"),
    "DE":    ("迪尔", "Deere & Company", "造农业机械(拖拉机)的"),
    "GE":    ("通用电气", "GE Aerospace", "做航空发动机的，老牌工业巨头"),
    "HON":   ("霍尼韦尔", "Honeywell International Inc.", "做航空、自动化、特种材料的工业集团"),
    "RTX":   ("雷神", "RTX Corporation", "做军工武器和航空零部件的"),
    "LMT":   ("洛克希德马丁", "Lockheed Martin Corporation", "美国最大军工企业，造F-35战斗机"),
    "NOC":   ("诺斯罗普格鲁曼", "Northrop Grumman Corporation", "做军工和国防系统的"),
    "UPS":   ("联合包裹", "United Parcel Service Inc.", "全球最大快递物流公司之一"),
    "FDX":   ("联邦快递", "FedEx Corporation", "全球快递物流巨头"),

    # ── 通信/电信 ──
    "T":     ("AT&T", "AT&T Inc.", "美国大型电信运营商"),
    "VZ":    ("威瑞森", "Verizon Communications Inc.", "美国最大电信运营商之一"),
    "TMUS":  ("T-Mobile", "T-Mobile US Inc.", "美国第三大电信运营商，网速快"),

    # ── 其他 ──
    "BX":    ("黑石", "Blackstone Inc.", "全球最大另类资产管理公司，搞私募和房地产"),
    "BLK":   ("贝莱德", "BlackRock Inc.", "全球最大资产管理公司，管十万亿美元"),
    "ABNB":  ("爱彼迎", "Airbnb Inc.", "全球最大民宿短租平台"),
    "ZM":    ("Zoom", "Zoom Video Communications Inc.", "做视频会议的，疫情时火爆全球"),
    "SNAP":  ("Snapchat", "Snap Inc.", "做阅后即焚社交App的"),
    "PINS":  ("Pinterest", "Pinterest Inc.", "图片社交和灵感发现平台"),
    "RBLX":  ("Roblox", "Roblox Corporation", "做元宇宙游戏平台的，小孩子很爱玩"),
    "U":     ("Unity", "Unity Technologies Inc.", "做游戏引擎的，很多手游都用它开发"),
    "TTD":   ("Trade Desk", "The Trade Desk Inc.", "做程序化广告投放平台的"),
    "BILL":  ("Bill.com", "BILL Holdings Inc.", "帮中小企业自动化付账单的"),
    "TEAM":  ("Atlassian", "Atlassian Corporation", "做Jira和Confluence等团队协作工具的"),
    "VEEV":  ("Veeva", "Veeva Systems Inc.", "做医药行业云软件的"),
    "HUBS":  ("HubSpot", "HubSpot Inc.", "做营销和销售自动化软件的"),
    "OKTA":  ("Okta", "Okta Inc.", "做身份认证和访问管理的"),
    "TWLO":  ("Twilio", "Twilio Inc.", "做云通信API的，帮App发短信打电话"),
    "PATH":  ("UiPath", "UiPath Inc.", "做机器人流程自动化(RPA)的"),
    "MNDY":  ("Monday.com", "Monday.com Ltd.", "做团队项目管理协作工具的"),
    "AI":    ("C3.ai", "C3.ai Inc.", "做企业AI软件平台的"),
    "IONQ":  ("IonQ", "IonQ Inc.", "做量子计算机的"),
    "RGTI":  ("Rigetti", "Rigetti Computing Inc.", "做量子计算芯片和云平台的"),
}


def get_company_desc(ticker):
    """获取公司简介: 返回 '中文名 (英文全称) - 简介' 或 None"""
    info = COMPANY_INFO.get(ticker)
    if info:
        cn, en, desc = info
        return f"{cn} ({en}) - {desc}"
    return None


def get_company_short(ticker):
    """获取公司中文名, 没有则返回 None"""
    info = COMPANY_INFO.get(ticker)
    return info[0] if info else None


UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


# ─── Yahoo Finance 会话 ─────────────────────────────────────────────
class YahooSession:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})
        self.crumb = None
        self._auth()

    def _auth(self):
        try:
            self.session.get("https://fc.yahoo.com", timeout=10)
            r = self.session.get(
                "https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10
            )
            if r.status_code == 200:
                self.crumb = r.text.strip()
                print(f"  Yahoo 鉴权成功 (crumb: {self.crumb[:6]}...)")
            else:
                print(f"  [WARN] Yahoo crumb 失败: {r.status_code}")
        except Exception as e:
            print(f"  [WARN] Yahoo 鉴权异常: {e}")

    def quote(self, symbols):
        if not self.crumb:
            return {}
        if isinstance(symbols, list):
            symbols = ",".join(symbols)
        try:
            r = self.session.get(
                "https://query2.finance.yahoo.com/v7/finance/quote",
                params={"symbols": symbols, "crumb": self.crumb},
                timeout=15,
            )
            if r.status_code == 200:
                return {q["symbol"]: q for q in r.json().get("quoteResponse", {}).get("result", [])}
        except Exception as e:
            print(f"  [WARN] quote 失败: {e}")
        return {}

    def summary(self, ticker, modules):
        """获取 quoteSummary"""
        try:
            r = self.session.get(
                f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}",
                params={"modules": modules, "crumb": self.crumb},
                timeout=12,
            )
            if r.status_code == 200:
                return r.json().get("quoteSummary", {}).get("result", [{}])[0]
        except Exception:
            pass
        return {}

    def chart(self, ticker, range_="1y", interval="1wk"):
        """获取价格历史 (用于技术指标计算)"""
        try:
            r = self.session.get(
                f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}",
                params={"range": range_, "interval": interval, "crumb": self.crumb},
                timeout=12,
            )
            if r.status_code == 200:
                result = r.json().get("chart", {}).get("result", [{}])[0]
                quotes = result.get("indicators", {}).get("quote", [{}])[0]
                return {
                    "timestamps": result.get("timestamp", []),
                    "close": quotes.get("close", []),
                    "high": quotes.get("high", []),
                    "low": quotes.get("low", []),
                    "open": quotes.get("open", []),
                    "volume": quotes.get("volume", []),
                }
        except Exception:
            pass
        return None

    def get(self, url, params=None, timeout=15):
        try:
            r = self.session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"  [WARN] 请求失败: {url} -> {e}")
            return None


yahoo: YahooSession = None


def init_yahoo():
    global yahoo
    print("── 初始化 Yahoo Finance 会话 ──")
    yahoo = YahooSession()


# ─── 工具函数 ────────────────────────────────────────────────────────
def fetch(url, params=None, timeout=15):
    headers = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"}
    for attempt in range(2):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == 1:
                return None
            time.sleep(0.5)


def fmt_num(n):
    if n is None:
        return "N/A"
    if isinstance(n, str):
        return n
    if abs(n) >= 1e12:
        return f"{n/1e12:.1f}T"
    if abs(n) >= 1e9:
        return f"{n/1e9:.1f}B"
    if abs(n) >= 1e6:
        return f"{n/1e6:.1f}M"
    if abs(n) >= 1e3:
        return f"{n/1e3:.0f}K"
    return f"{n:.0f}"


def fmt_pct(val):
    if val is None:
        return "N/A"
    return f"{val:+.2f}%"


def _raw(d, default=None):
    """从 Yahoo 的 {raw:..., fmt:...} 结构提取 raw 值"""
    if isinstance(d, dict):
        return d.get("raw", default)
    return d if d is not None else default


# ─── 技术指标计算 ────────────────────────────────────────────────────
def calc_ema(data, period):
    """指数移动平均"""
    if not data or len(data) < period:
        return []
    k = 2 / (period + 1)
    ema = [data[0]]
    for i in range(1, len(data)):
        if data[i] is None:
            ema.append(ema[-1])
        else:
            ema.append(data[i] * k + ema[-1] * (1 - k))
    return ema


def calc_sma(data, period):
    """简单移动平均"""
    if not data or len(data) < period:
        return []
    sma = []
    for i in range(len(data)):
        if i < period - 1:
            sma.append(None)
        else:
            window = [x for x in data[i - period + 1:i + 1] if x is not None]
            sma.append(sum(window) / len(window) if window else None)
    return sma


def calc_macd(closes, fast=12, slow=26, signal=9):
    """计算 MACD (DIF, DEA, Histogram)"""
    if not closes or len(closes) < slow + signal:
        return None, None, None
    clean = [c for c in closes if c is not None]
    if len(clean) < slow + signal:
        return None, None, None

    ema_fast = calc_ema(clean, fast)
    ema_slow = calc_ema(clean, slow)
    dif = [f - s for f, s in zip(ema_fast, ema_slow)]
    dea = calc_ema(dif, signal)
    hist = [d - e for d, e in zip(dif, dea)]
    return dif, dea, hist


def calc_rsi(closes, period=14):
    """计算 RSI"""
    if not closes or len(closes) < period + 1:
        return None
    clean = [c for c in closes if c is not None]
    if len(clean) < period + 1:
        return None

    gains, losses = [], []
    for i in range(1, len(clean)):
        diff = clean[i] - clean[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calc_atr(highs, lows, closes, period=14):
    """计算 ATR (平均真实波动幅度)"""
    if not highs or len(highs) < period + 1:
        return None
    trs = []
    for i in range(1, len(highs)):
        if highs[i] is None or lows[i] is None or closes[i-1] is None:
            continue
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        )
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def detect_support_resistance(highs, lows, closes, n=20):
    """检测关键支撑阻力位"""
    if not closes or len(closes) < n:
        return [], []

    clean_highs = [h for h in highs[-n:] if h is not None]
    clean_lows = [l for l in lows[-n:] if l is not None]
    clean_closes = [c for c in closes[-n:] if c is not None]

    if not clean_highs or not clean_lows:
        return [], []

    # 简易支撑阻力: 局部高低点
    resistances = []
    supports = []

    for i in range(2, len(clean_highs) - 2):
        if clean_highs[i] > clean_highs[i-1] and clean_highs[i] > clean_highs[i-2] and \
           clean_highs[i] > clean_highs[i+1] and clean_highs[i] > clean_highs[i+2]:
            resistances.append(clean_highs[i])

    for i in range(2, len(clean_lows) - 2):
        if clean_lows[i] < clean_lows[i-1] and clean_lows[i] < clean_lows[i-2] and \
           clean_lows[i] < clean_lows[i+1] and clean_lows[i] < clean_lows[i+2]:
            supports.append(clean_lows[i])

    return sorted(set(resistances), reverse=True)[:3], sorted(set(supports))[:3]


def detect_volume_breakout(volumes, closes, lookback=10):
    """检测放量突破"""
    if not volumes or not closes or len(volumes) < lookback + 1:
        return False, 0

    recent_vols = [v for v in volumes[-lookback-1:-1] if v is not None]
    if not recent_vols:
        return False, 0

    avg_vol = sum(recent_vols) / len(recent_vols)
    current_vol = volumes[-1] if volumes[-1] is not None else 0
    vol_ratio = current_vol / max(avg_vol, 1)

    # 放量 + 收阳
    if len(closes) >= 2 and closes[-1] is not None and closes[-2] is not None:
        is_up = closes[-1] > closes[-2]
        if vol_ratio > 1.5 and is_up:
            return True, vol_ratio

    return False, vol_ratio


# ═══════════════════════════════════════════════════════════════════════
# 模块 1: 盘前异动
# ═══════════════════════════════════════════════════════════════════════
def fetch_premarket_movers():
    print("\n[1/7] 盘前异动扫描 ...")
    candidates = set()

    for filter_set in [
        "sh_avgvol_o500,sh_price_o5,cap_midover",
        "sh_avgvol_o200,sh_price_o5,ta_change_u3,cap_smallover",
        "n_upgrades,sh_price_o5,cap_smallover",
        "ta_change_u5,sh_price_o5,cap_smallover",
        # 新增: 突破新高 + 高量比
        "ta_highlow52w_nh,sh_avgvol_o300,cap_midover",
        # 新增: 强势回调反弹 (RSI从超卖反弹)
        "ta_rsi_os40,sh_price_o10,cap_midover,ta_change_u",
    ]:
        r = fetch("https://finviz.com/screener.ashx",
                  params={"v": "111", "f": filter_set, "ft": "4", "o": "-change"})
        if r:
            soup = BeautifulSoup(r.text, "lxml")
            for row in soup.select("tr[valign='top']"):
                cols = row.find_all("td")
                if len(cols) >= 2:
                    t = cols[1].get_text(strip=True)
                    if t and t.isalpha() and len(t) <= 5:
                        candidates.add(t)

    watchlist = [
        "AAPL", "MSFT", "NVDA", "TSLA", "META", "AMZN", "GOOGL", "AMD",
        "NFLX", "PLTR", "COIN", "SMCI", "ARM", "AVGO", "CRM", "MU",
        "MARA", "RIOT", "SOFI", "NIO", "BABA", "INTC", "BA", "DIS",
        "JPM", "V", "UNH", "LLY", "WMT", "XOM", "CVX", "JNJ",
        "PG", "KO", "PEP", "COST", "HD", "MCD", "ABBV", "TMO",
        "ORCL", "ADBE", "NOW", "PANW", "CRWD", "SNOW", "NET", "DDOG",
        "UBER", "SQ", "SHOP", "MELI", "SE", "GRAB", "NU",
    ]
    candidates.update(watchlist)
    print(f"  候选池: {len(candidates)} 只")

    tickers = list(candidates)
    all_quotes = {}
    batch_size = 40
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        quotes = yahoo.quote(batch)
        all_quotes.update(quotes)
        if i + batch_size < len(tickers):
            time.sleep(0.3)

    print(f"  获取到 {len(all_quotes)} 只报价")

    results = {}
    market_state = None

    for sym, q in all_quotes.items():
        if not market_state:
            market_state = q.get("marketState", "")

        price = q.get("regularMarketPrice", 0)
        mcap = q.get("marketCap", 0)
        if price < MIN_PRICE or mcap < MIN_MARKET_CAP:
            continue

        name = q.get("displayName") or q.get("shortName", "")
        avg_vol = q.get("averageDailyVolume3Month", 1)
        reg_vol = q.get("regularMarketVolume", 0)
        vol_ratio = reg_vol / max(avg_vol, 1)

        # 52周高低位置
        fifty_two_high = q.get("fiftyTwoWeekHigh", 0)
        fifty_two_low = q.get("fiftyTwoWeekLow", 0)
        pct_from_high = ((price - fifty_two_high) / fifty_two_high * 100) if fifty_two_high else 0
        pct_from_low = ((price - fifty_two_low) / fifty_two_low * 100) if fifty_two_low else 0

        # 均线位置
        ma50 = q.get("fiftyDayAverage", 0)
        ma200 = q.get("twoHundredDayAverage", 0)
        above_ma50 = price > ma50 if ma50 else None
        above_ma200 = price > ma200 if ma200 else None

        pre_change_pct = q.get("preMarketChangePercent")
        pre_price = q.get("preMarketPrice")
        post_change_pct = q.get("postMarketChangePercent")
        post_price = q.get("postMarketPrice")
        reg_change_pct = q.get("regularMarketChangePercent", 0)

        if market_state == "PRE" and pre_change_pct is not None:
            effective_change, effective_price, phase = pre_change_pct, pre_price, "盘前"
        elif market_state == "POST" and post_change_pct is not None:
            effective_change, effective_price, phase = post_change_pct, post_price, "盘后"
        elif market_state in ("REGULAR", "OPEN"):
            effective_change, effective_price, phase = reg_change_pct, price, "盘中"
        elif pre_change_pct is not None:
            effective_change, effective_price, phase = pre_change_pct, pre_price, "盘前"
        elif post_change_pct is not None:
            effective_change, effective_price, phase = post_change_pct, post_price, "盘后"
        else:
            effective_change, effective_price, phase = reg_change_pct, price, "收盘"

        if effective_change is None:
            continue
        if effective_change < PREMARKET_CHANGE_THRESHOLD and vol_ratio < VOLUME_RATIO_THRESHOLD * 2:
            continue

        results[sym] = {
            "name": name,
            "price": f"{effective_price:.2f}",
            "change_pct": effective_change,
            "phase": phase,
            "volume": reg_vol,
            "vol_ratio": vol_ratio,
            "market_cap": mcap,
            "pct_from_high": pct_from_high,
            "pct_from_low": pct_from_low,
            "above_ma50": above_ma50,
            "above_ma200": above_ma200,
            "fifty_two_high": fifty_two_high,
            "signal": (
                f"{phase}涨 {effective_change:+.2f}% "
                f"(${effective_price:.2f}) | "
                f"量比 {vol_ratio:.1f}x | 市值 {fmt_num(mcap)} | "
                f"距高点 {pct_from_high:.1f}%"
            ),
        }

    state_label = {"PRE": "盘前", "POST": "盘后", "REGULAR": "盘中", "OPEN": "盘中"}.get(market_state, market_state)
    print(f"  市场状态: {state_label} ({market_state})")
    print(f"  筛出 {len(results)} 只异动标的")
    return results, all_quotes


# ═══════════════════════════════════════════════════════════════════════
# 模块 2: 财报日历
# ═══════════════════════════════════════════════════════════════════════
def fetch_earnings_today():
    print("\n[2/7] 财报日历扫描 ...")
    results = {}
    today = datetime.now().strftime("%Y-%m-%d")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    for day_label, day_str in [("今日", today), ("明日", tomorrow)]:
        r = yahoo.get("https://finance.yahoo.com/calendar/earnings", params={"day": day_str})
        if not r:
            continue
        soup = BeautifulSoup(r.text, "lxml")
        for row in soup.select("table tbody tr")[:80]:
            cols = row.find_all("td")
            if len(cols) < 5:
                continue
            ticker = cols[0].get_text(strip=True)
            if not ticker or "." in ticker:
                continue
            company = cols[1].get_text(strip=True) if len(cols) > 1 else ""
            call_time = cols[2].get_text(strip=True) if len(cols) > 2 else ""
            eps_est = cols[3].get_text(strip=True) if len(cols) > 3 else "N/A"
            eps_act = cols[4].get_text(strip=True) if len(cols) > 4 else "-"

            timing = "盘后" if any(x in call_time for x in ["After", "AMC", "PM"]) else "盘前"
            beat_info, beat_flag = "", False
            try:
                if eps_act not in ("-", "", "N/A") and eps_est not in ("N/A", "-", ""):
                    diff = float(eps_act) - float(eps_est)
                    if diff > 0:
                        beat_info = f" | EPS超预期 +{diff:.2f}"
                        beat_flag = True
                    elif diff < 0:
                        beat_info = f" | EPS不及预期 {diff:.2f}"
            except ValueError:
                pass

            results[ticker] = {
                "company": company, "timing": timing, "day": day_label, "beat": beat_flag,
                "signal": f"{day_label}{timing}财报 (预期:{eps_est} 实际:{eps_act}{beat_info})",
            }

    print(f"  找到 {len(results)} 只财报标的")
    return results


# ═══════════════════════════════════════════════════════════════════════
# 模块 3: 分析师评级
# ═══════════════════════════════════════════════════════════════════════
def fetch_analyst_upgrades():
    print("\n[3/7] 分析师评级扫描 ...")
    results = {}

    etf_kw = ("tradr", "t-rex", "direxion", "proshares", "leverage shares",
              "2x long", "2x short", "3x long", "3x short", "daily target", "defiance daily")

    def _ok(t, c=""):
        return t and t.isalpha() and len(t) <= 5 and not any(k in c.lower() for k in etf_kw)

    for signal, label in [("n_upgrades", "升级"), ("n_initiation", "首次覆盖")]:
        r = fetch("https://finviz.com/screener.ashx",
                  params={"v": "111", "f": f"{signal},sh_price_o5,cap_smallover", "ft": "4", "o": "-change"})
        if not r:
            continue
        soup = BeautifulSoup(r.text, "lxml")
        for row in soup.select("tr[valign='top']")[:20]:
            cols = row.find_all("td")
            if len(cols) < 10:
                continue
            ticker, company = cols[1].get_text(strip=True), cols[2].get_text(strip=True)
            mcap, change = cols[6].get_text(strip=True), cols[9].get_text(strip=True)
            if not _ok(ticker, company) or mcap in ("-", ""):
                continue
            if ticker not in results:
                results[ticker] = {"action": label, "signal": f"今日分析师{label} | 涨:{change} | 市值:{mcap}"}

    r = fetch("https://finviz.com/screener.ashx",
              params={"v": "111", "f": "an_recom_strongbuy,sh_price_o5,cap_midover", "ft": "4", "o": "-change"})
    if r:
        soup = BeautifulSoup(r.text, "lxml")
        for row in soup.select("tr[valign='top']")[:25]:
            cols = row.find_all("td")
            if len(cols) < 10:
                continue
            t, c = cols[1].get_text(strip=True), cols[2].get_text(strip=True)
            if _ok(t, c) and t not in results:
                results[t] = {"action": "strong buy consensus", "signal": "分析师共识: 强烈买入"}

    r = yahoo.get("https://finance.yahoo.com/markets/stocks/upgrades-and-downgrades/")
    if r:
        soup = BeautifulSoup(r.text, "lxml")
        for row in soup.select("table tbody tr")[:30]:
            cols = row.find_all("td")
            if len(cols) < 2:
                continue
            t = cols[0].get_text(strip=True)
            c = cols[1].get_text(strip=True) if len(cols) > 1 else ""
            if _ok(t, c) and t not in results:
                results[t] = {"action": "Yahoo 关注", "signal": f"Yahoo 升降级关注 ({c[:20]})"}

    print(f"  找到 {len(results)} 只分析师看好的标的")
    return results


# ═══════════════════════════════════════════════════════════════════════
# 模块 4: 期权异动
# ═══════════════════════════════════════════════════════════════════════
def fetch_options_unusual(all_quotes=None):
    print("\n[4/7] 期权异动扫描 ...")
    results = {}

    r = fetch("https://www.barchart.com/options/unusual-activity/stocks")
    if r:
        soup = BeautifulSoup(r.text, "lxml")
        for tag in soup.select("[data-ng-init]"):
            match = re.search(r'(\[.*\])', tag.get("data-ng-init", ""))
            if match:
                try:
                    for item in json.loads(match.group(1))[:40]:
                        sym = item.get("baseSymbol", "")
                        if sym:
                            results[sym] = {
                                "type": item.get("putCall", ""), "cp_ratio": 0,
                                "signal": f"Barchart异动 {item.get('putCall','')} Vol:{fmt_num(item.get('volume',0))} OI:{fmt_num(item.get('openInterest',0))}",
                            }
                except (json.JSONDecodeError, TypeError):
                    pass

    tickers_to_check = []
    if all_quotes:
        sorted_q = sorted(all_quotes.items(),
            key=lambda x: x[1].get("preMarketChangePercent") or x[1].get("regularMarketChangePercent", 0),
            reverse=True)
        tickers_to_check = [s for s, _ in sorted_q[:30] if s not in results]
    else:
        tickers_to_check = ["NVDA", "TSLA", "AAPL", "AMD", "META", "AMZN", "PLTR", "COIN", "SMCI", "ARM"]

    def analyze_options(ticker):
        r = yahoo.get(f"https://query2.finance.yahoo.com/v7/finance/options/{ticker}",
                      params={"crumb": yahoo.crumb}, timeout=8)
        if not r:
            return None
        try:
            chain = r.json().get("optionChain", {}).get("result", [{}])[0]
            opts = chain.get("options", [{}])[0]
            calls, puts = opts.get("calls", []), opts.get("puts", [])
            cv = sum(c.get("volume", 0) for c in calls if isinstance(c.get("volume"), (int, float)))
            pv = sum(p.get("volume", 0) for p in puts if isinstance(p.get("volume"), (int, float)))
            total = cv + pv
            if total < 5000:
                return None
            cp = cv / max(pv, 1)
            big = len([c for c in calls if isinstance(c.get("volume"), (int, float))
                       and isinstance(c.get("openInterest"), (int, float))
                       and c["volume"] > c["openInterest"] * 1.5 and c["volume"] > 1000])
            return {"ticker": ticker, "call_vol": cv, "put_vol": pv, "cp_ratio": cp, "unusual": big}
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=6) as pool:
        for f in as_completed({pool.submit(analyze_options, t): t for t in tickers_to_check[:25]}):
            res = f.result()
            if not res:
                continue
            t = res["ticker"]
            if res["cp_ratio"] > 1.3 or res["unusual"] > 0:
                parts = [f"C/P {res['cp_ratio']:.1f}x", f"Call:{fmt_num(res['call_vol'])}", f"Put:{fmt_num(res['put_vol'])}"]
                if res["unusual"] > 0:
                    parts.append(f"大单x{res['unusual']}")
                if t in results:
                    results[t]["cp_ratio"] = res["cp_ratio"]
                    results[t]["signal"] += f" | {' '.join(parts)}"
                else:
                    results[t] = {"type": "Bullish", "cp_ratio": res["cp_ratio"], "unusual_count": res["unusual"],
                                  "signal": f"期权看涨 {' | '.join(parts)}"}

    print(f"  找到 {len(results)} 只期权异动标的")
    return results


# ═══════════════════════════════════════════════════════════════════════
# 模块 5: 基本面质量 - 核心资产筛选 (选股口诀)
# ═══════════════════════════════════════════════════════════════════════
def fetch_fundamentals(tickers):
    """
    对候选 ticker 列表做基本面质量评估
    返回 {ticker: {moat_score, moat_details, moat_signal}}

    评分维度 (满分 10):
      ROE > 15%          → +2
      毛利率 > 30%       → +1.5
      负债率 < 60%       → +1.5
      OCF/NI > 90%       → +1.5
      PE合理 (PEG<2)     → +1.5
      股息率 > 2%        → +1  (加分)
      机构持仓 > 50%     → +1  (加分)
    """
    print("\n[5/7] 基本面质量评估 (核心资产筛选) ...")
    results = {}

    modules = "financialData,defaultKeyStatistics,summaryDetail,earningsTrend"

    def evaluate(ticker):
        data = yahoo.summary(ticker, modules)
        if not data:
            return None

        fd = data.get("financialData", {})
        ks = data.get("defaultKeyStatistics", {})
        sd = data.get("summaryDetail", {})
        et = data.get("earningsTrend", {})

        roe = _raw(fd.get("returnOnEquity"))
        gross_margin = _raw(fd.get("grossMargins"))
        debt_equity = _raw(fd.get("debtToEquity"))  # 百分比, e.g. 102.63
        ocf = _raw(fd.get("operatingCashflow"))
        revenue_growth = _raw(fd.get("revenueGrowth"))
        earnings_growth = _raw(fd.get("earningsGrowth"))
        profit_margin = _raw(fd.get("profitMargins"))
        revenue_per_share = _raw(fd.get("revenuePerShare"))
        fcf = _raw(fd.get("freeCashflow"))

        trailing_pe = _raw(sd.get("trailingPE"))
        forward_pe = _raw(sd.get("forwardPE"))
        dividend_yield = _raw(sd.get("dividendYield"))
        peg = _raw(ks.get("pegRatio"))
        inst_pct = _raw(ks.get("heldPercentInstitutions"))
        price_to_book = _raw(ks.get("priceToBook"))
        beta = _raw(ks.get("beta"))

        # 盈利增长趋势 (从 earningsTrend 提取)
        earnings_trend_positive = False
        if et:
            trends = et.get("trend", [])
            if trends:
                for trend in trends:
                    growth = _raw(trend.get("growth"))
                    if growth and growth > 0.1:
                        earnings_trend_positive = True
                        break

        # 粗估负债率: debtToEquity → 负债率 = D/E / (1 + D/E)
        debt_ratio = None
        if debt_equity is not None and debt_equity >= 0:
            de = debt_equity / 100  # Yahoo 给的是百分比
            debt_ratio = de / (1 + de)

        # 粗估 OCF 覆盖率
        ocf_coverage = None
        trailing_eps = _raw(ks.get("trailingEps"))
        shares = _raw(ks.get("sharesOutstanding"))
        if ocf and trailing_eps and shares and trailing_eps > 0:
            net_income_est = trailing_eps * shares
            ocf_coverage = ocf / net_income_est

        # FCF yield (自由现金流收益率)
        fcf_yield = None
        mcap = _raw(sd.get("marketCap"))
        if fcf and mcap and mcap > 0:
            fcf_yield = fcf / mcap

        # ── 评分 ──
        score = 0
        details = []
        flags = {}

        # 1. ROE > 15%
        if roe is not None:
            flags["roe"] = roe
            if roe >= MOAT_ROE_MIN:
                score += 2
                details.append(f"ROE {roe*100:.1f}% ✓")
            else:
                details.append(f"ROE {roe*100:.1f}% ✗")
        else:
            details.append("ROE N/A")

        # 2. 毛利率 > 30%
        if gross_margin is not None:
            flags["gross_margin"] = gross_margin
            if gross_margin >= MOAT_GROSS_MARGIN_MIN:
                score += 1.5
                details.append(f"毛利 {gross_margin*100:.1f}% ✓")
            else:
                details.append(f"毛利 {gross_margin*100:.1f}% ✗")
        else:
            details.append("毛利 N/A")

        # 3. 负债率 < 60%
        if debt_ratio is not None:
            flags["debt_ratio"] = debt_ratio
            if debt_ratio <= MOAT_DEBT_RATIO_MAX:
                score += 1.5
                details.append(f"负债率 {debt_ratio*100:.0f}% ✓")
            else:
                details.append(f"负债率 {debt_ratio*100:.0f}% ✗")
        else:
            details.append("负债率 N/A")

        # 4. OCF 覆盖 > 90%
        if ocf_coverage is not None:
            flags["ocf_coverage"] = ocf_coverage
            if ocf_coverage >= MOAT_OCF_COVERAGE_MIN:
                score += 1.5
                details.append(f"现金覆盖 {ocf_coverage*100:.0f}% ✓")
            else:
                details.append(f"现金覆盖 {ocf_coverage*100:.0f}% ✗")
        else:
            details.append("现金覆盖 N/A")

        # 5. PE 合理 + 有成长
        pe = forward_pe or trailing_pe
        if pe is not None and pe > 0:
            flags["pe"] = pe
            flags["peg"] = peg
            if peg is not None and 0 < peg <= MOAT_PEG_MAX:
                score += 1.5
                details.append(f"PE {pe:.1f} PEG {peg:.1f} ✓")
            elif pe <= MOAT_PE_MAX:
                score += 1.5
                details.append(f"PE {pe:.1f} ✓")
            else:
                details.append(f"PE {pe:.1f}" + (f" PEG {peg:.1f}" if peg else "") + " ✗")
        else:
            details.append("PE N/A")

        # 6. 股息率 > 2% (加分项)
        if dividend_yield is not None:
            flags["dividend"] = dividend_yield
            if dividend_yield >= MOAT_DIVIDEND_MIN:
                score += 1
                details.append(f"股息 {dividend_yield*100:.2f}% ✓")
            else:
                details.append(f"股息 {dividend_yield*100:.2f}%")
        else:
            details.append("股息 N/A")

        # 7. 机构持仓 > 50% (筹码集中, 加分项)
        if inst_pct is not None:
            flags["institution"] = inst_pct
            if inst_pct >= MOAT_INSTITUTION_MIN:
                score += 1
                details.append(f"机构 {inst_pct*100:.0f}% ✓")
            else:
                details.append(f"机构 {inst_pct*100:.0f}%")

        # ── 额外指标 (不计入基础分, 用于分类) ──
        flags["revenue_growth"] = revenue_growth
        flags["earnings_growth"] = earnings_growth
        flags["earnings_trend_positive"] = earnings_trend_positive
        flags["fcf_yield"] = fcf_yield
        flags["beta"] = beta
        flags["profit_margin"] = profit_margin

        return {
            "ticker": ticker,
            "moat_score": round(score, 1),
            "moat_details": details,
            "moat_flags": flags,
            "revenue_growth": revenue_growth,
            "earnings_growth": earnings_growth,
            "signal": " | ".join(details),
        }

    # 并行评估
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(evaluate, t): t for t in tickers}
        done = 0
        for f in as_completed(futures):
            res = f.result()
            done += 1
            if done % 10 == 0:
                print(f"  ... 已评估 {done}/{len(tickers)}")
            if res:
                results[res["ticker"]] = res

    print(f"  完成 {len(results)} 只基本面评估")
    moat_count = sum(1 for r in results.values() if r["moat_score"] >= 6)
    print(f"  其中护城河合格 (>=6分): {moat_count} 只")
    return results


# ═══════════════════════════════════════════════════════════════════════
# 模块 6: 技术面 - 多周期分析 (升级版)
# ═══════════════════════════════════════════════════════════════════════
def fetch_technicals(tickers):
    """
    多周期技术分析:
      周线: MACD + RSI + 趋势确认
      日线: 均线排列 + 量价关系 + 支撑阻力 + 突破信号
    返回 {ticker: {tech_score, rsi, macd_hist, signal, trade_type, entry_signal}}
    """
    print("\n[6/7] 技术面多周期分析 ...")
    results = {}

    def analyze(ticker):
        # 获取周线数据 (趋势)
        weekly = yahoo.chart(ticker, range_="1y", interval="1wk")
        # 获取日线数据 (精确入场)
        daily = yahoo.chart(ticker, range_="6mo", interval="1d")

        if not weekly or not daily:
            return None

        w_closes = weekly.get("close", [])
        d_closes = daily.get("close", [])
        d_highs = daily.get("high", [])
        d_lows = daily.get("low", [])
        d_volumes = daily.get("volume", [])
        d_opens = daily.get("open", [])

        if not w_closes or len(w_closes) < 30:
            return None
        if not d_closes or len(d_closes) < 60:
            return None

        # ══ 周线分析 ══
        # MACD (周线用 12/26/9)
        w_dif, w_dea, w_hist = calc_macd(w_closes, 12, 26, 9)
        w_rsi = calc_rsi(w_closes, 14)

        if w_dif is None or w_rsi is None:
            return None

        w_macd_current = w_hist[-1] if w_hist else 0
        w_macd_prev = w_hist[-2] if w_hist and len(w_hist) >= 2 else 0
        w_dif_current = w_dif[-1] if w_dif else 0

        # ══ 日线分析 ══
        # MACD 日线
        d_dif, d_dea, d_hist = calc_macd(d_closes, 12, 26, 9)
        d_rsi = calc_rsi(d_closes, 14)

        # 均线系统
        ma5 = calc_sma(d_closes, 5)
        ma10 = calc_sma(d_closes, 10)
        ma20 = calc_sma(d_closes, 20)
        ma60 = calc_sma(d_closes, 60)

        # 当前均线值
        cur_ma5 = ma5[-1] if ma5 and ma5[-1] else 0
        cur_ma10 = ma10[-1] if ma10 and ma10[-1] else 0
        cur_ma20 = ma20[-1] if ma20 and ma20[-1] else 0
        cur_ma60 = ma60[-1] if ma60 and ma60[-1] else 0
        cur_price = d_closes[-1] if d_closes[-1] else 0

        # 均线多头排列
        ma_bullish = (cur_ma5 > cur_ma10 > cur_ma20 > cur_ma60 > 0) if all([cur_ma5, cur_ma10, cur_ma20, cur_ma60]) else False
        # 价格在所有均线上方
        price_above_all_ma = cur_price > max(cur_ma5, cur_ma10, cur_ma20, cur_ma60) if all([cur_ma5, cur_ma10, cur_ma20, cur_ma60]) else False

        # 量价分析
        vol_breakout, vol_ratio = detect_volume_breakout(d_volumes, d_closes, 10)

        # 支撑阻力
        resistances, supports = detect_support_resistance(d_highs, d_lows, d_closes, 30)

        # ATR (波动率)
        atr = calc_atr(d_highs, d_lows, d_closes, 14)
        atr_pct = (atr / cur_price * 100) if atr and cur_price else 0

        # 突破检测: 价格突破最近阻力
        breakout = False
        if resistances and cur_price > 0:
            nearest_resistance = min(resistances, key=lambda r: abs(r - cur_price))
            if cur_price > nearest_resistance * (1 + SHORT_BREAKOUT_PCT):
                breakout = True

        # 回调到支撑位
        at_support = False
        if supports and cur_price > 0:
            nearest_support = min(supports, key=lambda s: abs(s - cur_price))
            if abs(cur_price - nearest_support) / cur_price < 0.03:
                at_support = True

        # 日线 MACD 金叉
        d_golden_cross = False
        if d_dif and d_dea and len(d_dif) >= 2 and len(d_dea) >= 2:
            if d_dif[-1] > d_dea[-1] and d_dif[-2] <= d_dea[-2]:
                d_golden_cross = True

        # ══ 综合评分 ══
        score = 0
        details = []
        trade_signals = []

        # 周线 MACD > 0 且上升 (趋势确认, 长线核心)
        w_macd_above_zero = w_dif_current > 0
        w_macd_rising = w_macd_current > w_macd_prev

        if w_macd_above_zero and w_macd_rising:
            score += 2
            details.append(f"周MACD零上扬 ✓")
            trade_signals.append("长线趋势强")
        elif w_macd_above_zero:
            score += 1
            details.append(f"周MACD零上")
        elif w_macd_rising:
            score += 0.5
            details.append(f"周MACD上升中")
        else:
            details.append(f"周MACD偏弱")

        # 周线 RSI 40-70
        if TECH_RSI_LOW <= w_rsi <= TECH_RSI_HIGH:
            score += 1.5
            details.append(f"周RSI {w_rsi:.0f} ✓")
        elif w_rsi < TECH_RSI_LOW:
            score += 0.5
            details.append(f"周RSI {w_rsi:.0f} 超卖")
            trade_signals.append("超卖反弹机会")
        elif w_rsi <= TECH_RSI_OVERBOUGHT:
            score += 0.5
            details.append(f"周RSI {w_rsi:.0f} 偏高")
        else:
            details.append(f"周RSI {w_rsi:.0f} 超买")

        # 均线多头排列 (加分)
        if ma_bullish:
            score += 1.5
            details.append("均线多头排列 ✓")
            trade_signals.append("多头趋势")
        elif price_above_all_ma:
            score += 1
            details.append("价在均线上")

        # 放量突破 (短线入场信号)
        if vol_breakout:
            score += 1.5
            details.append(f"放量突破 量比{vol_ratio:.1f}x ✓")
            trade_signals.append("短线入场")
        elif vol_ratio > 1.3:
            score += 0.5
            details.append(f"量比{vol_ratio:.1f}x")

        # 日线 MACD 金叉
        if d_golden_cross:
            score += 1
            details.append("日线MACD金叉 ✓")
            trade_signals.append("金叉买入")

        # 突破阻力位
        if breakout:
            score += 1
            details.append("突破阻力 ✓")
            trade_signals.append("突破买入")

        # 回调到支撑
        if at_support:
            score += 0.5
            details.append("回踩支撑")
            trade_signals.append("支撑买入")

        # ══ 交易类型判定 ══
        trade_type = "观望"
        entry_signal = ""

        # 短线条件: 量价突破 + 日线金叉/突破阻力
        short_score = 0
        if vol_breakout:
            short_score += 2
        if d_golden_cross:
            short_score += 1.5
        if breakout:
            short_score += 1.5
        if d_rsi and 40 < d_rsi < 65:
            short_score += 1

        # 长线条件: 周线趋势 + 均线 + 周RSI健康
        long_score = 0
        if w_macd_above_zero and w_macd_rising:
            long_score += 2
        if ma_bullish:
            long_score += 2
        if TECH_RSI_LOW <= w_rsi <= TECH_RSI_HIGH:
            long_score += 1.5
        if at_support:
            long_score += 1

        if short_score >= 3 and long_score >= 3:
            trade_type = "长短皆宜"
            entry_signal = "强势突破+趋势确认, 可重仓"
        elif short_score >= 3:
            trade_type = "短线"
            entry_signal = "量价突破, 快进快出"
        elif long_score >= 3:
            trade_type = "长线"
            entry_signal = "趋势良好, 逢低布局"
        elif w_rsi < TECH_RSI_OVERSOLD or (d_rsi and d_rsi < 30):
            trade_type = "抄底观察"
            entry_signal = "超卖区间, 关注企稳信号"

        # 止损位建议
        stop_loss = None
        if supports and cur_price > 0:
            nearest_support = min(supports, key=lambda s: abs(s - cur_price))
            if nearest_support < cur_price:
                stop_loss = nearest_support * 0.98  # 支撑位下方2%
        if stop_loss is None and atr and cur_price > 0:
            stop_loss = cur_price - 2 * atr  # 2ATR 止损

        return {
            "ticker": ticker,
            "tech_score": round(score, 1),
            "rsi": round(w_rsi, 1),
            "d_rsi": round(d_rsi, 1) if d_rsi else None,
            "macd_hist": round(w_macd_current, 3),
            "macd_dif": round(w_dif_current, 3),
            "macd_rising": w_macd_rising,
            "ma_bullish": ma_bullish,
            "vol_breakout": vol_breakout,
            "vol_ratio": vol_ratio,
            "breakout": breakout,
            "d_golden_cross": d_golden_cross,
            "at_support": at_support,
            "trade_type": trade_type,
            "entry_signal": entry_signal,
            "short_score": short_score,
            "long_score": long_score,
            "stop_loss": round(stop_loss, 2) if stop_loss else None,
            "atr_pct": round(atr_pct, 2),
            "tech_details": details,
            "trade_signals": trade_signals,
            "signal": " | ".join(details),
        }

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(analyze, t): t for t in tickers}
        done = 0
        for f in as_completed(futures):
            res = f.result()
            done += 1
            if done % 10 == 0:
                print(f"  ... 已分析 {done}/{len(tickers)}")
            if res:
                results[res["ticker"]] = res

    print(f"  完成 {len(results)} 只技术分析")
    good = sum(1 for r in results.values() if r["tech_score"] >= 4)
    print(f"  技术面良好 (>=4分): {good} 只")
    short_ready = sum(1 for r in results.values() if r["trade_type"] in ("短线", "长短皆宜"))
    long_ready = sum(1 for r in results.values() if r["trade_type"] in ("长线", "长短皆宜"))
    print(f"  短线机会: {short_ready} 只 | 长线布局: {long_ready} 只")
    return results


# ═══════════════════════════════════════════════════════════════════════
# 模块 7: 资金面 - 主力动向
# ═══════════════════════════════════════════════════════════════════════
def fetch_money_flow(tickers, all_quotes):
    """
    资金面分析:
      - 大单净流入
      - 连续放量天数
      - 北向/机构加仓迹象 (通过量价模式推断)
    """
    print("\n[7/7] 资金面分析 ...")
    results = {}

    for ticker in tickers:
        q = all_quotes.get(ticker, {})
        if not q:
            continue

        avg_vol = q.get("averageDailyVolume3Month", 1)
        avg_vol_10d = q.get("averageDailyVolume10Day", 1)
        reg_vol = q.get("regularMarketVolume", 0)

        # 短期量能趋势: 10日均量 vs 3月均量
        vol_trend = avg_vol_10d / max(avg_vol, 1)

        # 量比
        vol_ratio = reg_vol / max(avg_vol, 1)

        # 资金流入强度判定
        flow_score = 0
        flow_details = []

        if vol_trend > 1.5:
            flow_score += 2
            flow_details.append(f"近期持续放量 {vol_trend:.1f}x")
        elif vol_trend > 1.2:
            flow_score += 1
            flow_details.append(f"量能温和放大 {vol_trend:.1f}x")

        if vol_ratio > 2:
            flow_score += 1.5
            flow_details.append(f"今日量比 {vol_ratio:.1f}x")
        elif vol_ratio > 1.5:
            flow_score += 0.5
            flow_details.append(f"今日量比 {vol_ratio:.1f}x")

        # 大市值+高量比=主力行为
        mcap = q.get("marketCap", 0)
        if mcap > 10e9 and vol_ratio > 1.5:
            flow_score += 1
            flow_details.append("大盘股异动")

        if flow_score > 0:
            results[ticker] = {
                "flow_score": flow_score,
                "vol_trend": vol_trend,
                "vol_ratio": vol_ratio,
                "flow_details": flow_details,
                "signal": " | ".join(flow_details),
            }

    print(f"  识别 {len(results)} 只资金异动标的")
    return results


# ═══════════════════════════════════════════════════════════════════════
# 综合评分引擎 v4 - 长短线双轨
# ═══════════════════════════════════════════════════════════════════════
def score_and_rank(premarket, earnings, analyst, options, fundamentals, technicals, money_flow, all_quotes):
    """
    七维度综合评分 + 长短线分类
    """
    all_tickers = set()
    all_tickers.update(premarket.keys(), earnings.keys(), analyst.keys(), options.keys())

    scored = []
    for ticker in all_tickers:
        score = 0
        reasons = []
        dimensions = 0
        detail = {}

        # ── 公司简介 ──
        company_desc = get_company_desc(ticker)
        if company_desc:
            reasons.append(f"🏢 {company_desc}")

        # ── 盘前异动 ──
        if ticker in premarket:
            dimensions += 1
            info = premarket[ticker]
            s = 2
            change = info.get("change_pct", 0) or 0
            if change > 10: s += 2
            elif change > 5: s += 1
            if info.get("vol_ratio", 0) > 2: s += 1
            # 接近52周新高加分
            if info.get("pct_from_high", -100) > -5: s += 1
            score += s
            reasons.append(f"📈 {info['signal']}")
            detail.update({"price": info.get("price"), "change_pct": change,
                           "name": get_company_short(ticker) or info.get("name", ""),
                           "market_cap": info.get("market_cap", 0)})

        # ── 财报 ──
        if ticker in earnings:
            dimensions += 1
            info = earnings[ticker]
            s = 2
            if info.get("beat"): s += 3
            if info.get("timing") == "盘前" and info.get("day") == "今日": s += 1
            score += s
            reasons.append(f"📊 {info['signal']}")

        # ── 分析师 ──
        if ticker in analyst:
            dimensions += 1
            info = analyst[ticker]
            s = 2
            a = info.get("action", "").lower()
            if "upgrade" in a or "升级" in a: s += 1
            if "strong" in a: s += 1
            score += s
            reasons.append(f"🎯 {info['signal']}")

        # ── 期权 ──
        if ticker in options:
            dimensions += 1
            info = options[ticker]
            s = 2
            cp = info.get("cp_ratio", 0)
            if cp > 3: s += 2
            elif cp > 2: s += 1
            if info.get("unusual_count", 0) > 0: s += 1
            score += s
            reasons.append(f"🔥 {info['signal']}")

        # ── 基本面质量 (核心资产口诀) ──
        if ticker in fundamentals:
            f_info = fundamentals[ticker]
            moat = f_info["moat_score"]
            score += moat
            if moat >= 8:
                reasons.append(f"🏰 核心资产 ({moat}/10): {f_info['signal']}")
            elif moat >= 6:
                reasons.append(f"🛡️ 优质基本面 ({moat}/10): {f_info['signal']}")
            elif moat >= 4:
                reasons.append(f"📋 基本面中等 ({moat}/10): {f_info['signal']}")
            else:
                reasons.append(f"⚠️ 基本面偏弱 ({moat}/10): {f_info['signal']}")

        # ── 技术面 ──
        if ticker in technicals:
            t_info = technicals[ticker]
            ts = t_info["tech_score"]
            score += ts
            trade_type = t_info.get("trade_type", "观望")
            entry = t_info.get("entry_signal", "")
            stop = t_info.get("stop_loss")
            stop_str = f" | 止损:${stop}" if stop else ""

            if ts >= 5:
                reasons.append(f"📐 技术面强 ({ts:.1f}) [{trade_type}]: {t_info['signal']}{stop_str}")
            elif ts >= 3:
                reasons.append(f"📐 技术面优 ({ts:.1f}) [{trade_type}]: {t_info['signal']}{stop_str}")
            elif ts >= 1.5:
                reasons.append(f"📐 技术面中 ({ts:.1f}) [{trade_type}]: {t_info['signal']}{stop_str}")
            else:
                reasons.append(f"📐 技术面弱 ({ts:.1f}): {t_info['signal']}")

            if entry:
                reasons.append(f"   💡 操作建议: {entry}")

        # ── 资金面 ──
        if ticker in money_flow:
            mf = money_flow[ticker]
            fs = mf["flow_score"]
            score += fs
            reasons.append(f"💰 资金面 ({fs:.1f}): {mf['signal']}")

        # ── 多维度共振 ──
        if dimensions >= 4:
            score += 6
            reasons.append(f"💎 {dimensions}维信号全面共振")
        elif dimensions >= 3:
            score += 4
            reasons.append(f"⭐ {dimensions}维信号共振")
        elif dimensions >= 2:
            score += 2
            reasons.append(f"✦ {dimensions}维信号共振")

        # ── 补充 detail ──
        cn_name = get_company_short(ticker)
        if ticker in all_quotes and "name" not in detail:
            q = all_quotes[ticker]
            yahoo_name = q.get("displayName") or q.get("shortName", "")
            detail["name"] = cn_name or yahoo_name
            detail["price"] = f'{q.get("regularMarketPrice", 0):.2f}'
            detail["market_cap"] = q.get("marketCap", 0)
            detail["change_pct"] = q.get("preMarketChangePercent") or q.get("regularMarketChangePercent", 0)
        elif cn_name and "name" in detail:
            detail["name"] = cn_name

        # 交易类型
        trade_type = technicals.get(ticker, {}).get("trade_type", "观望")

        scored.append({
            "ticker": ticker, "score": round(score, 1), "dimensions": dimensions,
            "reasons": reasons,
            "moat_score": fundamentals.get(ticker, {}).get("moat_score", 0),
            "tech_score": technicals.get(ticker, {}).get("tech_score", 0),
            "trade_type": trade_type,
            "short_score": technicals.get(ticker, {}).get("short_score", 0),
            "long_score": technicals.get(ticker, {}).get("long_score", 0),
            "stop_loss": technicals.get(ticker, {}).get("stop_loss"),
            **detail,
        })

    # 排序: 总分 → 护城河分 → 维度数
    scored.sort(key=lambda x: (x["score"], x["moat_score"], x["dimensions"]), reverse=True)

    # 分类输出
    short_term = [s for s in scored if s["trade_type"] in ("短线", "长短皆宜")]
    long_term = [s for s in scored if s["trade_type"] in ("长线", "长短皆宜")]
    # 按短线分数排短线, 按长线分数排长线
    short_term.sort(key=lambda x: (x["short_score"], x["score"]), reverse=True)
    long_term.sort(key=lambda x: (x["long_score"], x["moat_score"], x["score"]), reverse=True)

    return scored[:TOP_N], short_term[:TOP_SHORT], long_term[:TOP_LONG]


# ═══════════════════════════════════════════════════════════════════════
# 输出渲染
# ═══════════════════════════════════════════════════════════════════════
def print_results(top_stocks, short_term, long_term, market_state=""):
    try:
        from rich.console import Console
        from rich.table import Table
        from rich.panel import Panel
        _print_rich(top_stocks, short_term, long_term, market_state)
    except ImportError:
        _print_plain(top_stocks, short_term, long_term, market_state)


def _print_rich(top_stocks, short_term, long_term, market_state):
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    import shutil

    # 自适应终端宽度
    term_width = shutil.get_terminal_size((80, 24)).columns
    console = Console(width=term_width)

    # 根据终端宽度决定显示模式
    compact = term_width < 100
    narrow = term_width < 70

    state_map = {"PRE": "🌅 盘前", "POST": "🌙 盘后", "REGULAR": "📊 盘中", "OPEN": "📊 盘中"}
    state_label = state_map.get(market_state, market_state)

    console.print()
    if narrow:
        console.print(f"[bold cyan]美股选股 v4.0[/bold cyan] | {state_label}")
        console.print(f"[dim]{datetime.now().strftime('%Y-%m-%d %H:%M')}[/dim]")
    else:
        console.print(Panel.fit(
            f"[bold cyan]美股智能选股 v4.0 - 长短线双轨筛选[/bold cyan]\n"
            f"[dim]{datetime.now().strftime('%Y-%m-%d %H:%M')} | {state_label}[/dim]\n"
            f"[dim]口诀: ROE>15% | 毛利>30% | 负债<60% | 现金>90% | PE合理 | MACD零上扬 | RSI 40-70 | 均线多头 | 放量突破[/dim]",
            border_style="cyan",
        ))

    # 计算"买入理由"列的动态宽度 (占用剩余空间)
    # 固定列大约占: # 3 + 代码 6 + 公司 10 + 总分 5 + 价格 9 + 涨跌 8 + 止损 9 + 类型 8 + 边框~20 = ~78
    reason_width = max(20, term_width - 78) if not narrow else None

    def _truncate_reasons(reasons, max_lines=4, max_chars=None):
        """根据宽度截断理由"""
        lines = reasons[:max_lines]
        if max_chars:
            lines = [r[:max_chars] for r in lines]
        return "\n".join(lines)

    # ── 短线机会 ──
    if short_term:
        console.print(f"\n[bold red]━━━ 短线交易机会 (快进快出) ━━━[/bold red]")
        table = Table(show_header=True, header_style="bold red", show_lines=True, expand=True)
        table.add_column("#", style="dim", width=3, justify="center")
        table.add_column("代码", style="bold cyan", width=6, no_wrap=True)
        if not narrow:
            table.add_column("公司", width=10, no_wrap=True, overflow="ellipsis")
        table.add_column("总分", justify="center", width=5)
        table.add_column("价格", justify="right", width=8)
        table.add_column("涨跌", justify="right", width=7)
        if not narrow:
            table.add_column("止损", justify="right", width=8)
            table.add_column("类型", justify="center", width=6)
        table.add_column("买入理由", ratio=1, overflow="fold")

        for i, s in enumerate(short_term, 1):
            sc = s.get("score", 0)
            score_color = "bold green" if sc >= 15 else "green" if sc >= 10 else "yellow"
            chg = s.get("change_pct")
            chg_str = fmt_pct(chg) if chg else "N/A"
            chg_color = "green" if (chg or 0) > 0 else "red" if (chg or 0) < 0 else "white"
            stop = f"${s['stop_loss']}" if s.get("stop_loss") else "N/A"
            trade_color = "bold magenta" if s["trade_type"] == "长短皆宜" else "red"
            max_lines = 2 if compact else 4

            row = [str(i), s["ticker"]]
            if not narrow:
                row.append(s.get("name", "")[:10] or "")
            row.append(f"[{score_color}]{sc}[/{score_color}]")
            row.append(s.get("price", "N/A"))
            row.append(f"[{chg_color}]{chg_str}[/{chg_color}]")
            if not narrow:
                row.append(stop)
                row.append(f"[{trade_color}]{s['trade_type']}[/{trade_color}]")
            row.append(_truncate_reasons(s.get("reasons", []), max_lines))
            table.add_row(*row)
        console.print(table)

    # ── 长线布局 ──
    if long_term:
        console.print(f"\n[bold blue]━━━ 长线价值布局 (逢低配置) ━━━[/bold blue]")
        table = Table(show_header=True, header_style="bold blue", show_lines=True, expand=True)
        table.add_column("#", style="dim", width=3, justify="center")
        table.add_column("代码", style="bold cyan", width=6, no_wrap=True)
        if not narrow:
            table.add_column("公司", width=10, no_wrap=True, overflow="ellipsis")
        table.add_column("总分", justify="center", width=5)
        table.add_column("护城河", justify="center", width=5)
        if not compact:
            table.add_column("技术", justify="center", width=5)
        table.add_column("价格", justify="right", width=8)
        if not narrow:
            table.add_column("市值", justify="right", width=6)
            table.add_column("类型", justify="center", width=6)
        table.add_column("布局理由", ratio=1, overflow="fold")

        for i, s in enumerate(long_term, 1):
            sc = s.get("score", 0)
            score_color = "bold green" if sc >= 15 else "green" if sc >= 10 else "yellow"
            moat = s.get("moat_score", 0)
            moat_color = "bold green" if moat >= 8 else "green" if moat >= 6 else "yellow" if moat >= 4 else "red"
            tech = s.get("tech_score", 0)
            tech_color = "green" if tech >= 4 else "yellow" if tech >= 2 else "red"
            trade_color = "bold magenta" if s["trade_type"] == "长短皆宜" else "blue"
            max_lines = 2 if compact else 4

            row = [str(i), s["ticker"]]
            if not narrow:
                row.append(s.get("name", "")[:10] or "")
            row.append(f"[{score_color}]{sc}[/{score_color}]")
            row.append(f"[{moat_color}]{moat}/10[/{moat_color}]")
            if not compact:
                row.append(f"[{tech_color}]{tech:.1f}[/{tech_color}]")
            row.append(s.get("price", "N/A"))
            if not narrow:
                row.append(fmt_num(s.get("market_cap", 0)))
                row.append(f"[{trade_color}]{s['trade_type']}[/{trade_color}]")
            row.append(_truncate_reasons(s.get("reasons", []), max_lines))
            table.add_row(*row)
        console.print(table)

    # ── 综合排名 ──
    console.print(f"\n[bold green]━━━ 综合排名 TOP {TOP_N} ━━━[/bold green]")
    table = Table(show_header=True, header_style="bold magenta", show_lines=True, expand=True)
    table.add_column("#", style="dim", width=3, justify="center")
    table.add_column("代码", style="bold cyan", width=6, no_wrap=True)
    if not narrow:
        table.add_column("公司", width=10, no_wrap=True, overflow="ellipsis")
    table.add_column("总分", justify="center", width=5)
    if not compact:
        table.add_column("护城河", justify="center", width=5)
        table.add_column("技术", justify="center", width=5)
    table.add_column("价格", justify="right", width=8)
    table.add_column("涨跌", justify="right", width=7)
    if not narrow:
        table.add_column("类型", justify="center", width=6)
    table.add_column("买入理由", ratio=1, overflow="fold")

    for i, s in enumerate(top_stocks, 1):
        sc = s.get("score", 0)
        score_color = "bold green" if sc >= 15 else "green" if sc >= 10 else "yellow" if sc >= 6 else "white"
        moat = s.get("moat_score", 0)
        moat_color = "bold green" if moat >= 8 else "green" if moat >= 6 else "yellow" if moat >= 4 else "red"
        tech = s.get("tech_score", 0)
        tech_color = "green" if tech >= 4 else "yellow" if tech >= 2 else "red"
        chg = s.get("change_pct")
        chg_str = fmt_pct(chg) if chg else "N/A"
        chg_color = "green" if (chg or 0) > 0 else "red" if (chg or 0) < 0 else "white"
        tt = s.get("trade_type", "观望")
        tt_color = "bold magenta" if tt == "长短皆宜" else "red" if tt == "短线" else "blue" if tt == "长线" else "dim"
        max_lines = 3 if compact else 5

        row = [str(i), s["ticker"]]
        if not narrow:
            row.append(s.get("name", "")[:10] or "")
        row.append(f"[{score_color}]{sc}[/{score_color}]")
        if not compact:
            row.append(f"[{moat_color}]{moat}/10[/{moat_color}]")
            row.append(f"[{tech_color}]{tech:.1f}[/{tech_color}]")
        row.append(s.get("price", "N/A"))
        row.append(f"[{chg_color}]{chg_str}[/{chg_color}]")
        if not narrow:
            row.append(f"[{tt_color}]{tt}[/{tt_color}]")
        row.append(_truncate_reasons(s.get("reasons", []), max_lines))
        table.add_row(*row)

    console.print(table)
    console.print()
    console.print("[dim italic]⚠️  数据仅供参考，不构成投资建议。核心资产筛选基于历史数据，不代表未来表现。[/dim italic]")
    console.print("[dim italic]💡 短线: 设置严格止损, 量价背离即出; 长线: 分批建仓, 跌破MA60减仓[/dim italic]")
    console.print()


def _print_plain(top_stocks, short_term, long_term, market_state):
    print(f"\n{'═' * 100}")
    print(f"  美股智能选股 v4.0 - 长短线双轨筛选  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  {market_state}")
    print(f"  口诀: ROE>15% | 毛利>30% | 负债<60% | 现金>90% | PE合理 | MACD零上扬 | RSI 40-70 | 均线多头 | 放量突破")
    print(f"{'═' * 100}")

    if short_term:
        print(f"\n{'━' * 50} 短线交易机会 {'━' * 50}")
        for i, s in enumerate(short_term, 1):
            chg = fmt_pct(s.get('change_pct')) if s.get('change_pct') else 'N/A'
            stop = f"${s['stop_loss']}" if s.get("stop_loss") else "N/A"
            print(f"\n  #{i}  {s['ticker']:<6} {s.get('name','')[:12]:<12}  总分:{s.get('score',0):>5}  "
                  f"涨跌:{chg:>8}  止损:{stop}  [{s['trade_type']}]")
            for reason in s.get("reasons", [])[:3]:
                print(f"      {reason}")

    if long_term:
        print(f"\n{'━' * 50} 长线价值布局 {'━' * 50}")
        for i, s in enumerate(long_term, 1):
            print(f"\n  #{i}  {s['ticker']:<6} {s.get('name','')[:12]:<12}  总分:{s.get('score',0):>5}  "
                  f"护城河:{s.get('moat_score',0)}/10  技术:{s.get('tech_score',0):.1f}  [{s['trade_type']}]")
            for reason in s.get("reasons", [])[:3]:
                print(f"      {reason}")

    print(f"\n{'━' * 50} 综合排名 TOP {TOP_N} {'━' * 50}")
    for i, s in enumerate(top_stocks, 1):
        print(f"\n{'─' * 95}")
        name = s.get('name', '')[:15]
        chg = fmt_pct(s.get('change_pct')) if s.get('change_pct') else 'N/A'
        print(f"  #{i}  {s['ticker']:<6} {name:<15}  总分:{s.get('score',0):>5}  "
              f"护城河:{s.get('moat_score',0)}/10  技术:{s.get('tech_score',0):.1f}  "
              f"价格:{s.get('price','N/A'):>9}  涨跌:{chg:>8}  [{s.get('trade_type','观望')}]")
        print(f"{'─' * 95}")
        for reason in s.get("reasons", []):
            print(f"    {reason}")
    print(f"\n{'═' * 100}")
    print("  ⚠️  数据仅供参考，不构成投资建议。")
    print("  💡 短线: 设置严格止损, 量价背离即出; 长线: 分批建仓, 跌破MA60减仓")
    print(f"{'═' * 100}\n")


# ═══════════════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════════════
def main():
    print(f"\n{'━' * 60}")
    print(f"  🔍 美股智能选股器 v4.0 - 长短线双轨筛选")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'━' * 60}")

    init_yahoo()

    # ── Phase 1: 信号面采集 ──
    premarket, all_quotes = fetch_premarket_movers()

    with ThreadPoolExecutor(max_workers=3) as pool:
        f_earnings = pool.submit(fetch_earnings_today)
        f_analyst = pool.submit(fetch_analyst_upgrades)
        f_options = pool.submit(fetch_options_unusual, all_quotes)
        earnings = f_earnings.result()
        analyst = f_analyst.result()
        options = f_options.result()

    # ── Phase 2: 收集所有候选 ticker, 做深度分析 ──
    all_tickers = set()
    all_tickers.update(premarket.keys(), earnings.keys(), analyst.keys(), options.keys())

    print(f"\n{'━' * 60}")
    print(f"  Phase 2: 对 {len(all_tickers)} 只候选做深度分析")
    print(f"{'━' * 60}")

    ticker_list = list(all_tickers)
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_fund = pool.submit(fetch_fundamentals, ticker_list)
        f_tech = pool.submit(fetch_technicals, ticker_list)
        fundamentals = f_fund.result()
        technicals = f_tech.result()

    # ── Phase 3: 资金面分析 ──
    money_flow = fetch_money_flow(ticker_list, all_quotes)

    # ── Phase 4: 综合评分 ──
    print(f"\n{'━' * 60}")
    print("  📊 七维度综合评分 + 长短线分类")
    print(f"  盘前:{len(premarket)} | 财报:{len(earnings)} | 分析师:{len(analyst)} | "
          f"期权:{len(options)} | 基本面:{len(fundamentals)} | 技术:{len(technicals)} | 资金:{len(money_flow)}")

    market_state = ""
    for q in all_quotes.values():
        ms = q.get("marketState", "")
        if ms:
            market_state = ms
            break

    top, short_term, long_term = score_and_rank(
        premarket, earnings, analyst, options, fundamentals, technicals, money_flow, all_quotes
    )
    print_results(top, short_term, long_term, market_state)


if __name__ == "__main__":
    main()

"""生成测试数据 — 填充所有榜单所需的表（MySQL版）"""
import sys
import json
import random
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent))

from utils.db_utils import execute_insert, execute_query

today = datetime.now().strftime("%Y-%m-%d")
now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ============ 1. data_sources ============
execute_insert(
    "INSERT IGNORE INTO data_sources (name, source_type, base_url, enabled) VALUES (%s, %s, %s, %s)",
    ("测试数据源", "manual", "http://localhost", 1))
rows = execute_query("SELECT id FROM data_sources WHERE name='测试数据源'")
source_id = rows[0]["id"]

# ============ 2. stock_info ============
stocks = [
    ("600519", "贵州茅台", "食品饮料", "白酒", "sh"),
    ("300750", "宁德时代", "电力设备", "电池", "sz"),
    ("601318", "中国平安", "非银金融", "保险", "sh"),
    ("000858", "五粮液", "食品饮料", "白酒", "sz"),
    ("002594", "比亚迪", "汽车", "乘用车", "sz"),
    ("600036", "招商银行", "银行", "股份制银行", "sh"),
    ("601012", "隆基绿能", "电力设备", "光伏", "sh"),
    ("300059", "东方财富", "非银金融", "证券", "sz"),
    ("002475", "立讯精密", "电子", "消费电子", "sz"),
    ("600900", "长江电力", "公用事业", "水电", "sh"),
    ("688981", "中芯国际", "电子", "半导体", "sh"),
    ("300760", "迈瑞医疗", "医药生物", "医疗器械", "sz"),
]

for code, name, ind1, ind2, mkt in stocks:
    cap = random.uniform(500, 30000)
    execute_insert(
        """INSERT IGNORE INTO stock_info
        (stock_code, stock_name, industry_l1, industry_l2, market, market_cap)
        VALUES (%s, %s, %s, %s, %s, %s)""",
        (code, name, ind1, ind2, mkt, cap))

# ============ 3. raw_items + cleaned_items ============
macro_positive = [
    ("央行宣布降准0.5个百分点，释放长期资金约1万亿元", ["降准", "货币政策", "流动性"], 5),
    ("国务院发布促进民营经济发展壮大31条措施", ["民营经济", "政策利好", "营商环境"], 4),
    ("财政部：2026年将加大减税降费力度", ["减税", "财政政策", "企业减负"], 4),
    ("发改委批复多个重大基建项目，总投资超3000亿", ["基建", "投资", "稳增长"], 4),
    ("证监会：进一步优化IPO和再融资监管", ["IPO", "资本市场", "改革"], 3),
]
macro_negative = [
    ("美联储暗示可能再次加息，全球市场承压", ["美联储", "加息", "外部风险"], 4),
    ("1月CPI同比下降0.3%，通缩压力加大", ["CPI", "通缩", "消费疲软"], 4),
    ("房地产投资同比下降9.8%，降幅扩大", ["房地产", "投资下滑", "经济压力"], 3),
    ("中美贸易摩擦升级，部分商品加征关税", ["贸易摩擦", "关税", "出口压力"], 4),
]

industry_news = [
    ("新能源", "工信部发布新能源汽车产业发展规划，目标2027年渗透率达60%", ["新能源", "汽车", "政策"], 5),
    ("半导体", "国家大基金三期成立，注册资本3440亿元聚焦先进制程", ["半导体", "大基金", "国产替代"], 5),
    ("AI", "国内首个千亿参数开源大模型发布，性能对标GPT-4", ["AI", "大模型", "科技创新"], 4),
    ("光伏", "欧盟宣布2030年光伏装机目标上调至750GW", ["光伏", "出口", "欧盟"], 4),
    ("医药", "国家医保局：创新药纳入医保谈判周期缩短至6个月", ["医药", "创新药", "医保"], 4),
    ("消费电子", "苹果Vision Pro二代发布，供应链订单大幅增长", ["消费电子", "苹果", "MR"], 4),
    ("银行", "央行引导LPR下调，银行净息差有望企稳", ["银行", "LPR", "净息差"], 3),
    ("食品饮料", "春节消费数据超预期，白酒动销同比增长15%", ["白酒", "消费", "春节"], 4),
    ("电力设备", "特高压项目密集核准，投资规模超2000亿", ["特高压", "电网", "投资"], 3),
    ("汽车", "比亚迪1月销量突破30万辆，同比增长62%", ["比亚迪", "销量", "新能源车"], 4),
]

rows = execute_query("SELECT COALESCE(MAX(id),0) as max_id FROM raw_items")
raw_id_counter = rows[0]["max_id"]


def insert_news(event_type, sentiment, summary, tags, importance, industries=None, stock_codes=None):
    global raw_id_counter
    raw_id_counter += 1
    ext_id = f"test_{raw_id_counter}_{random.randint(1000,9999)}"
    execute_insert(
        """INSERT IGNORE INTO raw_items
        (source_id, external_id, title, content, fetched_at, processing_status, item_type)
        VALUES (%s, %s, %s, %s, %s, 'cleaned', 'news')""",
        (source_id, ext_id, summary[:50], summary, now_ts))
    rows = execute_query("SELECT id FROM raw_items WHERE external_id=%s", (ext_id,))
    raw_id = rows[0]["id"]

    ci_id = execute_insert(
        """INSERT INTO cleaned_items
        (raw_item_id, event_type, sentiment, importance, summary, tags_json, cleaned_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (raw_id, event_type, sentiment, importance, summary, json.dumps(tags, ensure_ascii=False), now_ts))

    if industries:
        for ind in industries:
            execute_insert(
                "INSERT INTO item_industries (cleaned_item_id, industry_name, impact) VALUES (%s, %s, %s)",
                (ci_id, ind, "positive" if sentiment == "positive" else "negative"))
    if stock_codes:
        for sc in stock_codes:
            sname = dict((s[0], s[1]) for s in stocks).get(sc, "")
            execute_insert(
                "INSERT INTO item_companies (cleaned_item_id, stock_code, stock_name, impact) VALUES (%s, %s, %s, %s)",
                (ci_id, sc, sname, sentiment))
    return ci_id


for summary, tags, imp in macro_positive:
    insert_news("macro_policy", "positive", summary, tags, imp)

for summary, tags, imp in macro_negative:
    insert_news("macro_policy", "negative", summary, tags, imp)

for ind, summary, tags, imp in industry_news:
    insert_news("industry_news", "positive", summary, tags, imp, industries=[ind])

stock_news = [
    ("600519", "贵州茅台2025年报净利润同比增长18%，超市场预期", ["茅台", "业绩", "白酒"], 5),
    ("300750", "宁德时代获特斯拉新一代电池大单，价值超200亿", ["宁德时代", "特斯拉", "电池"], 5),
    ("002594", "比亚迪发布第五代DM技术，油耗降至2.9L/100km", ["比亚迪", "DM技术", "新能源"], 4),
    ("601318", "中国平安寿险新业务价值同比增长23%", ["平安", "寿险", "NBV"], 4),
    ("688981", "中芯国际14nm产能利用率提升至90%", ["中芯国际", "半导体", "产能"], 4),
    ("002475", "立讯精密获苹果MR头显独家组装订单", ["立讯精密", "苹果", "MR"], 4),
    ("300059", "东方财富基金销售规模突破2万亿", ["东方财富", "基金", "互联网券商"], 3),
    ("601012", "隆基绿能BC电池量产效率突破26.5%", ["隆基", "光伏", "BC电池"], 4),
    ("600036", "招商银行零售AUM突破14万亿", ["招行", "零售银行", "AUM"], 3),
    ("300760", "迈瑞医疗海外收入占比首次超过50%", ["迈瑞", "医疗器械", "出海"], 4),
]
for code, summary, tags, imp in stock_news:
    insert_news("company_event", "positive", summary, tags, imp, stock_codes=[code])

neg_stock_news = [
    ("600519", "茅台批价短期回落至2700元，渠道库存偏高", ["茅台", "批价", "库存"], 3),
    ("601012", "隆基绿能硅片价格持续下跌，盈利承压", ["隆基", "硅片", "价格战"], 3),
]
for code, summary, tags, imp in neg_stock_news:
    insert_news("company_event", "negative", summary, tags, imp, stock_codes=[code])

# ============ 4. stock_daily (180天日线) ============
print("生成日线数据...")
for code, name, _, _, _ in stocks:
    base_price = {"600519": 1680, "300750": 210, "601318": 48, "000858": 155,
                  "002594": 260, "600036": 36, "601012": 22, "300059": 16,
                  "002475": 35, "600900": 28, "688981": 45, "300760": 290}.get(code, 50)
    price = base_price
    for d in range(180, -1, -1):
        dt = (datetime.now() - timedelta(days=d)).strftime("%Y-%m-%d")
        wd = (datetime.now() - timedelta(days=d)).weekday()
        if wd >= 5:
            continue
        change = random.uniform(-0.05, 0.05)
        o = price
        c = price * (1 + change)
        h = max(o, c) * (1 + random.uniform(0, 0.02))
        l = min(o, c) * (1 - random.uniform(0, 0.02))
        vol = random.uniform(50000, 500000)
        amt = vol * (o + c) / 2
        tr = random.uniform(0.5, 5.0)
        execute_insert(
            """INSERT IGNORE INTO stock_daily
            (stock_code, trade_date, open, high, low, close, volume, amount, turnover_rate, change_pct)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (code, dt, round(o, 2), round(h, 2), round(l, 2), round(c, 2),
             round(vol), round(amt), round(tr, 2), round(change * 100, 2)))
        price = c

# ============ 5. capital_flow (个股资金流) ============
print("生成资金流数据...")
for code, name, _, _, _ in stocks:
    for d in range(30, -1, -1):
        dt = (datetime.now() - timedelta(days=d)).strftime("%Y-%m-%d")
        wd = (datetime.now() - timedelta(days=d)).weekday()
        if wd >= 5:
            continue
        main = random.uniform(-5e8, 5e8)
        super_l = main * random.uniform(0.3, 0.6)
        large = main * random.uniform(0.2, 0.4)
        medium = -main * random.uniform(0.1, 0.3)
        small = -main * random.uniform(0.1, 0.3)
        execute_insert(
            """INSERT IGNORE INTO capital_flow
            (stock_code, trade_date, main_net_inflow, super_large_net, large_net, medium_net, small_net)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (code, dt, round(main), round(super_l), round(large), round(medium), round(small)))

# ============ 6. industry_capital_flow ============
print("生成行业资金流数据...")
industries_list = ["食品饮料", "电力设备", "非银金融", "汽车", "银行", "电子", "公用事业", "医药生物",
                   "计算机", "通信", "机械设备", "化工", "有色金属", "钢铁", "房地产"]
for ind in industries_list:
    net = random.uniform(-20e8, 20e8)
    chg = random.uniform(-3, 3)
    lead = random.choice([s[1] for s in stocks])
    execute_insert(
        """INSERT IGNORE INTO industry_capital_flow
        (industry_name, trade_date, net_inflow, change_pct, leading_stock)
        VALUES (%s, %s, %s, %s, %s)""",
        (ind, today, round(net), round(chg, 2), lead))

# ============ 7. financial_reports (财报超预期) ============
print("生成财报数据...")
for code, name, _, _, _ in stocks:
    rev_yoy = random.uniform(-10, 50)
    prof_yoy = random.uniform(-20, 60)
    beat = 1 if random.random() > 0.4 else 0
    actual_vs = random.uniform(5, 30) if beat else random.uniform(-10, 5)
    eps = random.uniform(0.5, 15)
    execute_insert(
        """INSERT IGNORE INTO financial_reports
        (stock_code, report_period, revenue_yoy, profit_yoy, beat_expectation, actual_vs_consensus, eps)
        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (code, "2025Q3", round(rev_yoy, 1), round(prof_yoy, 1), beat, round(actual_vs, 1), round(eps, 2)))

# ============ 8. research_reports (券商覆盖) ============
print("生成券商研报数据...")
brokers = ["中信证券", "中金公司", "华泰证券", "国泰君安", "海通证券",
           "招商证券", "广发证券", "申万宏源", "兴业证券", "东方证券"]
for code, name, _, _, _ in stocks:
    n_reports = random.randint(3, 8)
    for _ in range(n_reports):
        broker = random.choice(brokers)
        days_ago = random.randint(0, 90)
        rdate = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        tp = random.uniform(10, 300)
        ext_id = f"rr_{code}_{broker}_{rdate}_{random.randint(1000,9999)}"
        execute_insert(
            """INSERT IGNORE INTO raw_items
            (source_id, external_id, title, content, fetched_at, processing_status, item_type)
            VALUES (%s, %s, %s, %s, %s, 'cleaned', 'report')""",
            (source_id, ext_id, f"{broker}研报:{name}", f"{broker}发布{name}研报", rdate))
        rows = execute_query("SELECT id FROM raw_items WHERE external_id=%s", (ext_id,))
        raw_id = rows[0]["id"]
        ci_id = execute_insert(
            """INSERT INTO cleaned_items
            (raw_item_id, event_type, sentiment, importance, summary, tags_json, cleaned_at)
            VALUES (%s, 'research_report', 'positive', %s, %s, %s, %s)""",
            (raw_id, random.randint(3, 5), f"{broker}发布{name}研报，目标价{tp:.0f}",
             json.dumps([name, broker], ensure_ascii=False), rdate))
        execute_insert(
            """INSERT IGNORE INTO research_reports
            (cleaned_item_id, broker_name, report_type, rating, target_price, stock_code, stock_name, report_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (ci_id, broker, random.choice(["initiate", "maintain", "upgrade"]),
             random.choice(["buy", "overweight"]), round(tp, 2), code, name, rdate))

# ============ 9. northbound_flow (北向资金30天) ============
print("生成北向资金数据...")
for d in range(30, -1, -1):
    dt = (datetime.now() - timedelta(days=d)).strftime("%Y-%m-%d")
    wd = (datetime.now() - timedelta(days=d)).weekday()
    if wd >= 5:
        continue
    sh = random.uniform(-80e8, 80e8)
    sz = random.uniform(-60e8, 60e8)
    execute_insert(
        """INSERT IGNORE INTO northbound_flow
        (trade_date, sh_net, sz_net, total_net)
        VALUES (%s, %s, %s, %s)""",
        (dt, round(sh), round(sz), round(sh + sz)))

# ============ 10. macro_indicators ============
print("生成宏观指标数据...")
indicators = [
    ("GDP同比增速", 5.2, "%"),
    ("CPI同比", -0.3, "%"),
    ("PPI同比", -2.1, "%"),
    ("M2同比增速", 9.7, "%"),
    ("社融存量同比", 9.5, "%"),
    ("PMI", 50.1, ""),
    ("10年期国债收益率", 2.15, "%"),
    ("美元兑人民币", 7.18, ""),
]
for name, val, unit in indicators:
    execute_insert(
        """INSERT IGNORE INTO macro_indicators
        (indicator_name, indicator_date, value, unit, source)
        VALUES (%s, %s, %s, %s, %s)""",
        (name, today, val, unit, "测试数据"))

# ============ 11. dashboard_tag_frequency ============
print("生成标签频次数据...")
all_tags = [
    ("新能源", "industry"), ("半导体", "industry"), ("AI", "theme"), ("光伏", "industry"),
    ("医药", "industry"), ("消费电子", "industry"), ("白酒", "industry"), ("银行", "industry"),
    ("降准", "macro"), ("货币政策", "macro"), ("贸易摩擦", "macro"), ("基建", "macro"),
    ("国产替代", "theme"), ("大模型", "theme"), ("MR", "theme"), ("出海", "theme"),
    ("比亚迪", "stock"), ("宁德时代", "stock"), ("茅台", "stock"), ("中芯国际", "stock"),
]

for d in range(14, -1, -1):
    dt = (datetime.now() - timedelta(days=d)).strftime("%Y-%m-%d")
    for tag_name, tag_type in all_tags:
        dashboards = random.sample(range(1, 9), random.randint(1, 4))
        for db_type in dashboards:
            execute_insert(
                """INSERT IGNORE INTO dashboard_tag_frequency
                (tag_name, tag_type, dashboard_type, appear_date, rank_position)
                VALUES (%s, %s, %s, %s, %s)""",
                (tag_name, tag_type, db_type, dt, random.randint(1, 20)))

# ============ 12. investment_opportunities ============
print("生成投资机会数据...")
opps = [
    ("300750", "宁德时代", "growth", "A", "特斯拉大单+储能爆发，业绩高增长确定性强"),
    ("002594", "比亚迪", "momentum", "A", "DM5.0技术领先+海外扩张，销量持续超预期"),
    ("688981", "中芯国际", "event", "B", "大基金三期注资+14nm突破，国产替代加速"),
    ("600519", "贵州茅台", "value", "A", "估值回归合理区间，分红率提升预期"),
    ("300760", "迈瑞医疗", "growth", "B", "海外收入占比突破50%，全球化进入收获期"),
]
for code, name, otype, rating, summary in opps:
    execute_insert(
        """INSERT IGNORE INTO investment_opportunities
        (stock_code, stock_name, opportunity_type, source, rating, summary, status)
        VALUES (%s, %s, %s, 'deep_research', %s, %s, 'active')""",
        (code, name, otype, rating, summary))

# ============ 13. watchlist ============
print("生成跟踪列表数据...")
watchlist_items = [
    ("600519", "贵州茅台", "holding", '["白酒","消费"]', "核心持仓"),
    ("300750", "宁德时代", "holding", '["新能源","电池"]', "重仓"),
    ("002594", "比亚迪", "interested", '["新能源车","比亚迪"]', "关注DM5.0进展"),
    ("688981", "中芯国际", "interested", '["半导体","国产替代"]', "等待回调"),
    ("601318", "中国平安", "holding", '["保险","金融"]', "底仓"),
    ("300760", "迈瑞医疗", "interested", '["医疗器械","出海"]', "关注海外订单"),
]
for code, name, wtype, tags, notes in watchlist_items:
    execute_insert(
        """INSERT IGNORE INTO watchlist
        (stock_code, stock_name, watch_type, related_tags, notes)
        VALUES (%s, %s, %s, %s, %s)""",
        (code, name, wtype, tags, notes))

# ============ 14. tag_groups ============
print("生成标签组数据...")
tag_groups = [
    ("新能源产业链", '["新能源","电池","光伏","比亚迪","宁德时代"]', "新能源上下游产业链联动", 7, 85),
    ("科技自主可控", '["半导体","AI","国产替代","大模型"]', "科技自主可控主线", 7, 72),
    ("消费复苏", '["白酒","消费","食品饮料","茅台"]', "消费复苏+春节效应", 14, 58),
]
for gname, tags_json, logic, tr, freq in tag_groups:
    execute_insert(
        """INSERT IGNORE INTO tag_groups
        (group_name, tags_json, group_logic, time_range, total_frequency)
        VALUES (%s, %s, %s, %s, %s)""",
        (gname, tags_json, logic, tr, freq))

# ============ 15. holding_positions ============
print("生成持仓数据...")
positions = [
    ("600519", "贵州茅台", "2025-06-15", 1580, 100),
    ("300750", "宁德时代", "2025-09-20", 195, 500),
    ("601318", "中国平安", "2025-11-01", 45, 2000),
]
for code, name, bdate, bprice, qty in positions:
    execute_insert(
        """INSERT IGNORE INTO holding_positions
        (stock_code, stock_name, buy_date, buy_price, quantity, status)
        VALUES (%s, %s, %s, %s, %s, 'open')""",
        (code, name, bdate, bprice, qty))

print(f"\n测试数据生成完成！日期: {today}")
print("涵盖: stock_info(12只), raw_items, cleaned_items(宏观+行业+个股),")
print("       stock_daily(180天), capital_flow(30天), industry_capital_flow,")
print("       financial_reports, research_reports, northbound_flow,")
print("       macro_indicators, dashboard_tag_frequency(14天),")
print("       investment_opportunities, watchlist, tag_groups, holding_positions")

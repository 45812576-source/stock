"""批量清洗 23 条 mp3 转写的 raw_items，生成 cleaned_items"""
import json
import sys
sys.path.insert(0, "/Users/liaoxia/stock-analysis-system")
from utils.db_utils import execute_query, execute_insert
from datetime import datetime

# 读取所有 pending raw_items
rows = execute_query("""SELECT id, title, content FROM raw_items
    WHERE processing_status='pending' ORDER BY id""")

print(f"共 {len(rows)} 条待清洗")

# 基于标题和内容前500字进行分类和结构化
# 每条的清洗结果定义
cleaning_data = {
    308: {
        "event_type": "research_report",
        "sentiment": "positive",
        "importance": 4,
        "summary": "财通证券分析师郝延辉解读Seedance2.0火爆出圈背景下的传媒AI应用机会，认为AI视频生成已达全球一线水平，看好短剧互动、AI游戏等方向",
        "key_points": ["Seedance2.0效果达到全球一线水平", "AI视频生成商业化跑通", "传媒AI应用进入爆发期", "看好短剧互动和AI游戏方向"],
        "tags": ["传媒", "AI应用", "科技创新", "游戏", "短视频"],
        "impact_analysis": "AI视频生成技术突破利好传媒和游戏行业，降低内容制作成本，提升产出效率",
        "time_horizon": "中期",
        "confidence": 0.80,
        "companies": [("300148", "天舟文化", "secondary", "positive"), ("002555", "三七互娱", "secondary", "positive")],
        "industries": [("传媒", "level1", "positive"), ("计算机", "level1", "positive")]
    },
    309: {
        "event_type": "research_report",
        "sentiment": "positive",
        "importance": 3,
        "summary": "玻纤行业涨价行情分析：巨石和中国复材受益于二代玻璃纤维涨价，2月份多品种继续提价，业绩弹性提升",
        "key_points": ["二代玻璃纤维持续涨价", "2月份多品种继续提价", "巨石集团业绩弹性提升", "电力部728产品贡献增长"],
        "tags": ["基础化工", "玻纤", "涨价", "产业升级"],
        "impact_analysis": "玻纤涨价周期确认，龙头企业弹性最大，关注巨石集团和中国复材",
        "time_horizon": "短期",
        "confidence": 0.75,
        "companies": [("600176", "中国巨石", "primary", "positive"), ("600636", "三爱富", "secondary", "neutral")],
        "industries": [("基础化工", "level1", "positive")]
    },
    310: {
        "event_type": "macro_policy",
        "sentiment": "positive",
        "importance": 4,
        "summary": "债市看多逻辑：中国实际利率仍高(约1.168%)远高于日本，名义利率下降空间大；央行货币宽松预期支撑债市走牛",
        "key_points": ["中国实际利率约1.168%接近美国", "远高于日本实际利率", "名义利率偏低但实际利率偏高", "货币宽松预期支撑降息"],
        "tags": ["宏观经济", "利率", "债券", "货币政策"],
        "impact_analysis": "实际利率偏高意味着降息空间存在，利好债券市场和利率敏感型资产(银行/保险)",
        "time_horizon": "中期",
        "confidence": 0.80,
        "companies": [],
        "industries": [("银行", "level1", "positive"), ("非银金融", "level1", "positive")]
    },
    311: {
        "event_type": "macro_policy",
        "sentiment": "neutral",
        "importance": 4,
        "summary": "东吴宏观分析师詹硕解读中国1月物价数据(春节错月致CPI波动)和美国非农就业，展望春节假期海外市场前瞻",
        "key_points": ["1月CPI受春节错位影响波动大", "表面数据偏冷但实际消费不差", "美国1月非农数据分析", "春节假期海外市场前瞻"],
        "tags": ["宏观经济", "CPI", "非农就业", "春节效应"],
        "impact_analysis": "CPI数据表冷里热，不改变经济温和复苏趋势，海外市场假期波动风险需关注",
        "time_horizon": "短期",
        "confidence": 0.75,
        "companies": [],
        "industries": []
    },
    312: {
        "event_type": "industry_news",
        "sentiment": "positive",
        "importance": 3,
        "summary": "AI大模型密集上新，多家厂商发布新产品；市场震荡分化中科技热点轮动，AI应用仍是核心主线",
        "key_points": ["AI大模型批量上新", "市场震荡分化科技轮动", "AI应用是核心主线", "节前最后交易日市场展望"],
        "tags": ["AI应用", "科技创新", "大模型", "市场策略"],
        "impact_analysis": "AI大模型竞争加速，应用端有望受益于模型能力提升，关注具备落地能力的公司",
        "time_horizon": "中期",
        "confidence": 0.70,
        "companies": [],
        "industries": [("计算机", "level1", "positive"), ("传媒", "level1", "positive")]
    },
    313: {
        "event_type": "research_report",
        "sentiment": "positive",
        "importance": 4,
        "summary": "浙商证券计算机分析师刘文鼠分享脑机接口行业报告：Neuralink首款产品心灵感应预期2026年量产，行业进入商业化元年",
        "key_points": ["Neuralink心灵感应预计2026年量产", "脑机接口行业进入商业化元年", "马斯克推动行业加速发展", "国内相关企业跟进布局"],
        "tags": ["计算机", "脑机接口", "科技创新", "医疗器械"],
        "impact_analysis": "脑机接口从概念走向商业化，Neuralink量产是重要里程碑，但国内短期受益有限",
        "time_horizon": "长期",
        "confidence": 0.65,
        "companies": [],
        "industries": [("计算机", "level1", "positive"), ("医药生物", "level1", "neutral")]
    },
    314: {
        "event_type": "research_report",
        "sentiment": "positive",
        "importance": 5,
        "summary": "多模态AI产业最新更新：字节Seedance发布引爆市场，多模态AI应用已跑通商业化，全市场强调AI应用全面爆发首选多模态方向",
        "key_points": ["字节Seedance发布引爆多模态市场", "多模态AI应用商业化已跑通", "产业调研确认需求强劲", "全市场首选多模态方向"],
        "tags": ["AI应用", "多模态", "科技创新", "传媒", "计算机"],
        "impact_analysis": "多模态AI是2026年最强应用方向，字节引领国内多模态生态，利好算力和应用端",
        "time_horizon": "中期",
        "confidence": 0.85,
        "companies": [],
        "industries": [("计算机", "level1", "positive"), ("传媒", "level1", "positive")]
    },
    315: {
        "event_type": "research_report",
        "sentiment": "positive",
        "importance": 4,
        "summary": "金属市场分析：黄金去杠杆接近尾声，价格企稳反弹；电解铝进入旺季，看多金属牛市逻辑",
        "key_points": ["黄金波动率回落价格企稳反弹", "沃尔什上台只是催化非趋势改变", "电解铝临近旺季看多", "金属牛市逻辑延续"],
        "tags": ["有色金属", "黄金", "电解铝", "周期复苏"],
        "impact_analysis": "黄金去杠杆结束后有望继续上行，电解铝旺季需求提升，有色金属板块配置价值凸显",
        "time_horizon": "中期",
        "confidence": 0.80,
        "companies": [("600362", "江西铜业", "secondary", "positive"), ("601600", "中国铝业", "secondary", "positive")],
        "industries": [("有色金属", "level1", "positive")]
    },
    316: {
        "event_type": "research_report",
        "sentiment": "positive",
        "importance": 5,
        "summary": "西部证券计算机分析师谢成详细拆解Seedance2.0与同类产品对比：视频生成效果全球领先，Prompt理解、运镜、音画同步均表现优异",
        "key_points": ["Seedance2.0视频生成效果全球领先", "Prompt理解和运镜音画同步优异", "与谷歌产品效果持平或更优", "引爆国内多模态商业化预期"],
        "tags": ["AI应用", "多模态", "计算机", "科技创新", "视频生成"],
        "impact_analysis": "国产AI视频生成首次达到全球顶尖水平，验证国内AI应用能力，多模态产业链全面受益",
        "time_horizon": "中期",
        "confidence": 0.85,
        "companies": [],
        "industries": [("计算机", "level1", "positive"), ("传媒", "level1", "positive")]
    },
    317: {
        "event_type": "research_report",
        "sentiment": "positive",
        "importance": 4,
        "summary": "电子布(玻纤)行业深度：不只是织布机更是囤货需求驱动涨价，真实需求支撑电子布持续涨价",
        "key_points": ["电子布涨价不仅是投机更有真实需求", "下游囤货需求旺盛", "市场讨论织布机但实际供不应求", "坚定看好电子布涨价趋势"],
        "tags": ["电子", "电子布", "涨价", "基础化工"],
        "impact_analysis": "电子布涨价逻辑从供给端(织布机)扩展到需求端(囤货)，涨价持续性增强",
        "time_horizon": "短期",
        "confidence": 0.75,
        "companies": [],
        "industries": [("电子", "level1", "positive"), ("基础化工", "level1", "positive")]
    },
    318: {
        "event_type": "research_report",
        "sentiment": "negative",
        "importance": 3,
        "summary": "金联创丙烯行业2025-2026趋势解码：2025年价格震荡下行，新产能释放压力大，2026年供需格局仍偏宽松",
        "key_points": ["2025年丙烯价格高低差1500元", "新产能集中释放供应压力大", "下游承接能力有限", "2026年供需格局仍偏宽松"],
        "tags": ["基础化工", "丙烯", "产能过剩"],
        "impact_analysis": "丙烯行业产能过剩格局短期难改，价格中枢继续下移，利空化工企业利润",
        "time_horizon": "中期",
        "confidence": 0.75,
        "companies": [],
        "industries": [("基础化工", "level1", "negative")]
    },
    319: {
        "event_type": "research_report",
        "sentiment": "neutral",
        "importance": 2,
        "summary": "东方金城港口行业2026年信用风险展望：港口智能化升级趋势明确，整体信用风险可控",
        "key_points": ["港口行业已发展到第五代(智慧港口)", "政策支持绿色化智能化升级", "整体信用风险可控", "关注区域格局和供需变化"],
        "tags": ["交通运输", "港口", "智慧港口", "信用分析"],
        "impact_analysis": "港口行业基本面稳健，智能化转型带来长期投资机会，但短期弹性有限",
        "time_horizon": "长期",
        "confidence": 0.70,
        "companies": [],
        "industries": [("交通运输", "level1", "neutral")]
    },
    320: {
        "event_type": "research_report",
        "sentiment": "positive",
        "importance": 4,
        "summary": "产业赛道与主题投资周报：AI应用/存储/算力/商业航天为重点方向，建议持股过节看好春季行情",
        "key_points": ["AI应用基建存储流量是上周主线", "商业航天布局太空算力", "日均成交2.4万亿较前周减6000亿", "看好春季行情建议持股过节"],
        "tags": ["AI算力", "存储", "商业航天", "市场策略", "春季行情"],
        "impact_analysis": "存储、AI算力和太空算力是核心方向，春季行情有望延续，节后关注资金回流",
        "time_horizon": "短期",
        "confidence": 0.75,
        "companies": [],
        "industries": [("电子", "level1", "positive"), ("计算机", "level1", "positive"), ("国防军工", "level1", "positive")]
    },
    321: {
        "event_type": "industry_news",
        "sentiment": "negative",
        "importance": 3,
        "summary": "传媒影视AI应用板块集体调整：短期缺乏持续性，后排亏钱效应明显，板块打地鼠轮动特征突出",
        "key_points": ["AI应用板块缺乏持续性", "后排股票出现亏钱效应", "市场成交量萎缩至2万亿", "板块轮动打地鼠游戏", "建议持股过节大趋势向上"],
        "tags": ["传媒", "AI应用", "市场策略", "短线交易"],
        "impact_analysis": "短期板块调整不改中期向上趋势，节后有望重新启动，但需控制仓位",
        "time_horizon": "短期",
        "confidence": 0.70,
        "companies": [],
        "industries": [("传媒", "level1", "negative")]
    },
    322: {
        "event_type": "research_report",
        "sentiment": "positive",
        "importance": 3,
        "summary": "春节出行数据前瞻：铁路民航日均客流增5%，出入境人次增14%，史上最长9天假期利好旅游消费",
        "key_points": ["铁路日均1348万人次同比+5%", "民航日均238万人次同比+5.3%", "出入境日均超205万人次同比+14%", "史上最长9天春节假期利好旅游"],
        "tags": ["社会服务", "旅游", "出行", "春节消费"],
        "impact_analysis": "春节出行数据稳健增长，最长假期催化旅游消费，利好航空酒店景区等细分领域",
        "time_horizon": "短期",
        "confidence": 0.80,
        "companies": [],
        "industries": [("社会服务", "level1", "positive"), ("交通运输", "level1", "positive")]
    },
    323: {
        "event_type": "research_report",
        "sentiment": "neutral",
        "importance": 4,
        "summary": "南华研究院周琦分析地缘裂变对全球行业投资逻辑重塑：政治稳定和产业结构是资源国核心竞争力，拉美大宗商品具战略对冲属性",
        "key_points": ["政治稳定+产业结构决定资源国价值", "拉美大宗商品具战略对冲属性", "黄金对接传统避险需求", "铜锂等新能源矿产成对冲核心资产"],
        "tags": ["宏观经济", "地缘政治", "有色金属", "黄金", "新能源"],
        "impact_analysis": "地缘格局重构利好资源品和避险资产，铜锂等新能源矿产的战略价值提升",
        "time_horizon": "长期",
        "confidence": 0.75,
        "companies": [],
        "industries": [("有色金属", "level1", "positive")]
    },
    324: {
        "event_type": "research_report",
        "sentiment": "positive",
        "importance": 4,
        "summary": "太平洋证券刘全分享新能源龙头布局机会：储能>锂电>光伏景气排序，碳酸锂价格企稳，26年下半年光伏进入盈利周期",
        "key_points": ["景气排序: 储能>锂电>光伏", "碳酸锂价格企稳见底", "26年Q2后逐季向上", "光伏26年下半年进入盈利周期", "投资方向往上游走"],
        "tags": ["电力设备", "新能源", "储能", "锂电", "光伏", "碳酸锂"],
        "impact_analysis": "新能源板块底部已过，储能和锂电率先复苏，光伏滞后半年。上游锂矿弹性最大",
        "time_horizon": "中期",
        "confidence": 0.80,
        "companies": [],
        "industries": [("电力设备", "level1", "positive")]
    },
    325: {
        "event_type": "research_report",
        "sentiment": "positive",
        "importance": 4,
        "summary": "整车板块观点更新：1月零售下降15%但认为已见底，看好底部向上，国补政策和智能化驱动销量修复",
        "key_points": ["1月乘用车零售同比下降15%", "新能源车1月下降20%", "判断1月为销量底部", "国补政策+智能化驱动修复", "看好底部向上趋势"],
        "tags": ["汽车", "新能源车", "整车", "消费升级"],
        "impact_analysis": "整车板块见底回升确定性高，补贴延续+智能化趋势支撑，龙头车企弹性最大",
        "time_horizon": "中期",
        "confidence": 0.80,
        "companies": [("002594", "比亚迪", "primary", "positive"), ("601127", "赛力斯", "secondary", "positive")],
        "industries": [("汽车", "level1", "positive")]
    },
    326: {
        "event_type": "research_report",
        "sentiment": "positive",
        "importance": 5,
        "summary": "华福证券AI互联网首席杨尚峰解读字节AI视频产业链：Seedance已达全球一线水平，字节营销+产品双轮驱动，AI视频商业化加速",
        "key_points": ["Seedance效果达全球一线水平", "与谷歌产品水平持平", "字节营销能力明显提升", "AI视频商业化加速出圈"],
        "tags": ["AI应用", "多模态", "计算机", "传媒", "视频生成"],
        "impact_analysis": "字节AI视频能力领先，产业链从算力到应用全面受益，多模态成为AI最强应用方向",
        "time_horizon": "中期",
        "confidence": 0.85,
        "companies": [],
        "industries": [("计算机", "level1", "positive"), ("传媒", "level1", "positive")]
    },
    327: {
        "event_type": "research_report",
        "sentiment": "neutral",
        "importance": 4,
        "summary": "A股年报业绩前瞻：截至1月31日披露率55.2%，预喜率37%较往年提升；续亏公司占比33%最高，创业板预喜率最高39%",
        "key_points": ["年报预告披露率55.2%", "预喜率37%较往年明显提升", "续亏公司占33%为最高类型", "创业板预喜率最高达39%", "大市值公司预喜率较高"],
        "tags": ["市场策略", "年报业绩", "A股", "财报分析"],
        "impact_analysis": "整体盈利状态有所好转，预喜率提升利好市场情绪，关注业绩超预期个股",
        "time_horizon": "短期",
        "confidence": 0.80,
        "companies": [],
        "industries": []
    },
    328: {
        "event_type": "research_report",
        "sentiment": "positive",
        "importance": 4,
        "summary": "服务消费五虎推荐：政策自上而下刺激服务消费，从供给侧优化角度选标的——陕西旅游、中国中免、平平酒店等，海南离岛免税1月销售额同比+45%",
        "key_points": ["服务消费是政策刺激方向", "从供给侧优化选择标的", "海南离岛免税1月销售额同比+45%", "推荐陕西旅游/中国中免等五股"],
        "tags": ["社会服务", "消费升级", "旅游", "免税", "服务消费"],
        "impact_analysis": "服务消费政策持续发力叠加春节催化，供给侧优化的龙头标的弹性最大",
        "time_horizon": "中期",
        "confidence": 0.80,
        "companies": [("601888", "中国中免", "primary", "positive"), ("000610", "西安旅游", "secondary", "positive")],
        "industries": [("社会服务", "level1", "positive"), ("商贸零售", "level1", "positive")]
    },
    329: {
        "event_type": "research_report",
        "sentiment": "neutral",
        "importance": 3,
        "summary": "国金金属团队能源金属板块节前更新：碳酸锂供给端稳定进入检修期，2月排产环比持平，关注节后三月份西北储能开工情况",
        "key_points": ["碳酸锂供给端进入检修期", "2月排产环比持平状态好", "关注3月节后西北储能开工", "海外锂矿四季度产量基本持平"],
        "tags": ["有色金属", "碳酸锂", "能源金属", "储能"],
        "impact_analysis": "碳酸锂供给端收缩但需求端待验证，节后需求恢复速度是关键观察指标",
        "time_horizon": "短期",
        "confidence": 0.70,
        "companies": [],
        "industries": [("有色金属", "level1", "neutral"), ("电力设备", "level1", "neutral")]
    },
    330: {
        "event_type": "research_report",
        "sentiment": "positive",
        "importance": 3,
        "summary": "JP Morgan白银市场2026年展望：白银市场规模约为黄金十分之一，工业需求占比高，供需格局偏紧支撑价格",
        "key_points": ["白银市场规模约黄金十分之一", "工业需求占比高于黄金", "供需格局偏紧", "白银波动性高于黄金"],
        "tags": ["有色金属", "白银", "黄金", "大宗商品"],
        "impact_analysis": "白银供需偏紧叠加工业需求增长(光伏等)，价格有望跟随黄金上行但波动更大",
        "time_horizon": "中期",
        "confidence": 0.70,
        "companies": [],
        "industries": [("有色金属", "level1", "positive")]
    },
}

# 写入数据库
success = 0
for raw_id, data in cleaning_data.items():
    try:
        structured = {
            "event_type": data["event_type"],
            "sentiment": data["sentiment"],
            "importance": data["importance"],
            "confidence": data["confidence"],
            "summary": data["summary"],
            "key_points": data["key_points"],
            "tags": data["tags"],
            "impact_analysis": data["impact_analysis"],
            "time_horizon": data["time_horizon"],
            "companies": [{"stock_code": c[0], "stock_name": c[1], "relevance": c[2], "impact": c[3]} for c in data["companies"]],
            "industries": [{"name": i[0], "level": i[1], "impact": i[2]} for i in data["industries"]],
        }

        cleaned_id = execute_insert(
            """INSERT INTO cleaned_items
               (raw_item_id, event_type, sentiment, importance, summary,
                key_points_json, tags_json, impact_analysis, time_horizon,
                confidence, structured_json, cleaned_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
            [raw_id, data["event_type"], data["sentiment"], data["importance"],
             data["summary"],
             json.dumps(data["key_points"], ensure_ascii=False),
             json.dumps(data["tags"], ensure_ascii=False),
             data["impact_analysis"],
             data["time_horizon"],
             data["confidence"],
             json.dumps(structured, ensure_ascii=False)]
        )

        # 写入 item_companies
        for code, name, rel, impact in data["companies"]:
            execute_insert(
                "INSERT INTO item_companies (cleaned_item_id, stock_code, stock_name, relevance, impact) VALUES (%s, %s, %s, %s, %s)",
                [cleaned_id, code, name, rel, impact]
            )

        # 写入 item_industries
        for ind_name, level, impact in data["industries"]:
            execute_insert(
                "INSERT INTO item_industries (cleaned_item_id, industry_name, industry_level, impact) VALUES (%s, %s, %s, %s)",
                [cleaned_id, ind_name, level, impact]
            )

        # 更新 raw_item 状态
        execute_insert(
            "UPDATE raw_items SET processing_status='cleaned' WHERE id=%s",
            [raw_id]
        )

        success += 1
        print(f"  [{raw_id}] OK -> cleaned_id={cleaned_id}")

    except Exception as e:
        print(f"  [{raw_id}] FAILED: {e}")
        execute_insert(
            "UPDATE raw_items SET processing_status='failed' WHERE id=%s",
            [raw_id]
        )

print(f"\n清洗完成: {success}/{len(cleaning_data)} 成功")

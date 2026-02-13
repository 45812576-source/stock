"""Claude API清洗处理器 — 调用Claude做结构化信息清洗"""
import json
import logging
import time
from utils.claude_client import call_claude_json
from utils.db_utils import execute_insert, execute_query

logger = logging.getLogger(__name__)

# 主清洗prompt — 对接information-cleaning-structuring skill
CLEANING_SYSTEM_PROMPT = """你是专业的金融信息结构化清洗专家。将原始金融信息按以下JSON格式输出，不要包含任何其他文字。

## 输出JSON结构

```json
{
  "basic": {
    "date": "YYYY-MM-DD",
    "info_type": "公告|新闻|宏观研报|行业研报|个股研报",
    "title": "原始标题或提炼标题"
  },
  "items": [
    {
      "id": 1,
      "fact": "客观事实陈述",
      "opinion": "与该事实相关的观点或解读（无则null）",
      "opinion_source": "观点发布方（公司/机构/分析师/媒体）",
      "assumption": "该观点基于什么假设",
      "evidence": "支持该观点的具体数据或证据",
      "logic_chain": "从假设和数据到观点的推理过程"
    }
  ],
  "summary": {
    "core_facts": ["核心事实1", "核心事实2", "核心事实3"],
    "opinions": [
      {
        "opinion": "观点内容",
        "source": "机构/分析师",
        "assumption": "关键假设",
        "evidence": "数据来源",
        "logic": "推理逻辑"
      }
    ]
  },
  "tags": {
    "market": "A股|港股|两市",
    "board": "主板|创业板|科创板|北交所|港股主板|null",
    "sw_industry_l1": "申万一级行业名称（31个之一）",
    "sw_industry_l2": "二级行业",
    "sw_industry_l3": "三级行业",
    "invest_theme": "科技创新|消费升级|产业升级|政策红利|周期复苏|其他",
    "sub_theme": ["细分主题标签1", "细分主题标签2"],
    "event_type": "业绩|重组|融资|政策|技术|市场|其他",
    "event_nature": "利好|利空|中性",
    "impact_level": "重大|中等|轻微",
    "timeliness": "即时|短期|中期|长期",
    "persistence": "一次性|持续性|周期性"
  },
  "opportunity": {
    "超预期财报": {
      "hit": false,
      "detail": {"indicator": "", "actual_vs_expected": "", "magnitude": "", "reason": ""}
    },
    "机构密集覆盖": {
      "hit": false,
      "detail": {"count": 0, "institutions": [], "consensus_rating": "", "core_logic": ""}
    },
    "重大利好": {
      "hit": false,
      "detail": {"type": "", "content": "", "impact_quantified": "", "realization_time": ""}
    },
    "政策风向": {
      "hit": false,
      "detail": {"policy": "", "level": "", "fit_degree": "", "support_method": ""}
    },
    "overall": {
      "level": "⭐⭐⭐⭐|⭐⭐⭐|⭐⭐|⭐|○",
      "opp_type": "成长|价值|周期|主题|事件驱动",
      "logic": "综合投资逻辑",
      "catalyst": "近期催化因素",
      "risk": "主要风险",
      "attention": "高|中|低",
      "action": "立即关注|持续跟踪|暂时观望"
    }
  },
  "type_specific": {},
  "companies": [
    {"stock_code": "600519", "stock_name": "贵州茅台", "relevance": "primary", "impact": "positive"}
  ],
  "industries": [
    {"industry_name": "白酒", "industry_level": "level1", "impact": "positive"}
  ],
  "research_report": null,
  "event_type": "macro_policy|industry_news|company_event|earnings|research_report",
  "sentiment": "positive|negative|neutral",
  "importance": 1,
  "confidence": 0.8
}
```

## type_specific 字段规则

**公告**: {"company":"公司名","stock_code":"代码","announcement_type":"业绩预告|重大事项|股权变动|融资|其他","key_data":{}}
**宏观新闻**: {"level":"宏观","domain":"货币政策|财政政策|国际贸易|经济数据|其他","scope":"全市场|特定板块","affected_industries":[{"name":"行业","impact":"说明"}],"transmission_path":"传导路径"}
**行业新闻**: {"level":"行业","industry_chain":"一级>二级>三级","chain_position":"上游|中游|下游|全产业链","chain_analysis":{"upstream":"","midstream":"","downstream":""},"key_stocks":[{"code":"","name":"","position":"","impact":"高|中|低","logic":""}]}
**个股新闻**: {"level":"个股","stocks":[{"code":"","name":"","degree":"主要|次要","nature":"利好|利空|中性"}],"industry":"一级>二级>三级","chain_position":"","competitors":"","chain_links":{"upstream":"","downstream":""}}
**宏观研报**: {"level":"宏观","institution":"","analyst":"","rating":"","core_view":"","data_support":[],"invest_advice":"","benefited_industries":[{"name":"","reason":""}]}
**行业研报**: {"level":"行业","institution":"","analyst":"","industry":"一级>二级","industry_rating":"增持|中性|减持","trend":"上行|平稳|下行","drivers":[],"chain_analysis":{"upstream":"","midstream":"","downstream":""},"recommended_stocks":[{"code":"","name":"","reason":"","target_price":null,"rating":""}]}
**个股研报**: {"level":"个股","institution":"","analyst":"","company":"","stock_code":"","rating":"买入|增持|中性|减持|卖出","target_price":null,"current_price":null,"upside":"","industry":"一级>二级>三级","chain_position":"","core_logic":[],"earnings_forecast":[{"year":"","revenue":"","rev_growth":"","net_profit":"","np_growth":"","eps":"","pe":""}],"valuation":"","risks":[]}

## 关键规则
1. items数组：逐条拆解原文，严格区分事实与观点，不遗漏重要信息
2. 申万一级行业31个：农林牧渔/基础化工/钢铁/有色金属/电子/汽车/家用电器/食品饮料/纺织服饰/轻工制造/医药生物/公用事业/交通运输/房地产/商贸零售/社会服务/银行/非银金融/建筑材料/建筑装饰/电力设备/国防军工/计算机/传媒/通信/煤炭/石油石化/环保/美容护理/机械设备/综合
3. 投资机会四类标准严格判断，hit为true时detail必须填写
4. importance: 5=重大政策/颠覆性事件 4=重要趋势 3=一般新闻 2=常规披露 1=低价值
5. 信息不足的字段填null，不要编造"""

# 事件分析专用prompt — 对接stock-event-analysis skill
EVENT_ANALYSIS_PROMPT = """你是股票事件分析专家。请分析以下事件对相关股票的具体影响。

分析维度：
1. 短期影响（1-5个交易日）
2. 中期影响（1-3个月）
3. 长期影响（3个月以上）
4. 影响确定性（高/中/低）
5. 建议操作（买入/持有/观望/减仓）

输出JSON格式：
{
    "short_term": {"direction": "up|down|flat", "magnitude": "large|medium|small", "reason": "..."},
    "medium_term": {"direction": "up|down|flat", "magnitude": "large|medium|small", "reason": "..."},
    "long_term": {"direction": "up|down|flat", "magnitude": "large|medium|small", "reason": "..."},
    "certainty": "high|medium|low",
    "action": "buy|hold|watch|reduce",
    "analysis": "综合分析..."
}"""


def clean_single_item(raw_item_id):
    """清洗单条原始数据"""
    rows = execute_query("SELECT * FROM raw_items WHERE id=?", [raw_item_id])
    if not rows:
        return None

    item = rows[0]
    title = item["title"] or ""
    content = item["content"] or ""

    # 跳过内容过短的条目
    if len(title) + len(content) < 10:
        execute_insert(
            "UPDATE raw_items SET processing_status='failed' WHERE id=?",
            [raw_item_id],
        )
        logger.warning(f"内容过短，跳过 raw_item_id={raw_item_id}")
        return None

    # 截断过长内容（研报PDF全文可能很长）
    if len(content) > 8000:
        content = content[:8000] + "...(截断)"

    user_msg = f"标题: {title}\n\n内容: {content}"

    # 如果有meta信息，附加上下文
    if item.get("meta_json"):
        try:
            meta = json.loads(item["meta_json"])
            if meta.get("source"):
                user_msg += f"\n\n来源: {meta['source']}"
            if meta.get("category"):
                user_msg += f"\n分类: {meta['category']}"
        except (json.JSONDecodeError, TypeError):
            pass

    try:
        result = call_claude_json(CLEANING_SYSTEM_PROMPT, user_msg, max_tokens=8192)
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败 raw_item_id={raw_item_id}: {e}")
        execute_insert(
            "UPDATE raw_items SET processing_status='failed' WHERE id=?",
            [raw_item_id],
        )
        return None
    except Exception as e:
        logger.error(f"清洗失败 raw_item_id={raw_item_id}: {e}")
        execute_insert(
            "UPDATE raw_items SET processing_status='failed' WHERE id=?",
            [raw_item_id],
        )
        return None

    # 数据校验 — 兼容旧字段
    result.setdefault("event_type", "company_event")
    result.setdefault("sentiment", "neutral")
    result.setdefault("importance", 3)
    result.setdefault("confidence", 0.5)

    # 从新结构中提取兼容字段
    basic = result.get("basic", {})
    summary_obj = result.get("summary", {})
    tags_obj = result.get("tags", {})

    summary_text = ""
    if summary_obj and isinstance(summary_obj, dict):
        facts = summary_obj.get("core_facts", [])
        summary_text = "；".join(facts[:3]) if facts else ""
    if not summary_text:
        summary_text = basic.get("title", title[:100])

    # 提取标签列表（从MECE标签体系）
    tag_list = []
    if tags_obj and isinstance(tags_obj, dict):
        for k in ["sw_industry_l1", "sw_industry_l2", "invest_theme", "event_type"]:
            v = tags_obj.get(k)
            if v and v != "null":
                tag_list.append(v)
        for st in tags_obj.get("sub_theme", []) or []:
            tag_list.append(st)

    # 提取要点
    key_points = []
    if summary_obj and isinstance(summary_obj, dict):
        key_points = summary_obj.get("core_facts", [])[:5]

    # 确保 structured_json 列存在（自动迁移）
    try:
        execute_query("SELECT structured_json FROM cleaned_items LIMIT 1")
    except Exception:
        execute_insert("ALTER TABLE cleaned_items ADD COLUMN structured_json TEXT")

    # 保存清洗结果
    cleaned_id = execute_insert(
        """INSERT INTO cleaned_items (raw_item_id, event_type, sentiment, importance,
           summary, key_points_json, tags_json, impact_analysis, time_horizon,
           confidence, structured_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [raw_item_id, result["event_type"], result["sentiment"],
         result["importance"], summary_text,
         json.dumps(key_points, ensure_ascii=False),
         json.dumps(tag_list, ensure_ascii=False),
         result.get("opportunity", {}).get("overall", {}).get("logic", ""),
         tags_obj.get("timeliness", "short") if isinstance(tags_obj, dict) else "short",
         result["confidence"],
         json.dumps(result, ensure_ascii=False)],
    )

    # 保存关联公司
    for comp in result.get("companies", []):
        if comp.get("stock_code"):
            execute_insert(
                """INSERT INTO item_companies (cleaned_item_id, stock_code, stock_name, relevance, impact)
                   VALUES (?, ?, ?, ?, ?)""",
                [cleaned_id, comp["stock_code"], comp.get("stock_name"),
                 comp.get("relevance", "mentioned"), comp.get("impact", "neutral")],
            )

    # 保存关联行业
    for ind in result.get("industries", []):
        if ind.get("industry_name"):
            execute_insert(
                """INSERT INTO item_industries (cleaned_item_id, industry_name, industry_level, impact)
                   VALUES (?, ?, ?, ?)""",
                [cleaned_id, ind["industry_name"],
                 ind.get("industry_level", "level1"), ind.get("impact", "neutral")],
            )

    # 保存研报信息
    rr = result.get("research_report")
    if rr and isinstance(rr, dict) and rr.get("broker_name"):
        execute_insert(
            """INSERT INTO research_reports (cleaned_item_id, broker_name, analyst_name,
               report_type, rating, target_price, stock_code, stock_name, report_date)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, date('now'))""",
            [cleaned_id, rr.get("broker_name"), rr.get("analyst_name"),
             rr.get("report_type"), rr.get("rating"), rr.get("target_price"),
             rr.get("stock_code"), rr.get("stock_name")],
        )

    # 更新原始条目状态
    execute_insert(
        "UPDATE raw_items SET processing_status='cleaned' WHERE id=?",
        [raw_item_id],
    )

    logger.info(f"清洗完成 raw_item_id={raw_item_id} -> cleaned_id={cleaned_id}")
    return cleaned_id


def clean_with_event_analysis(raw_item_id):
    """清洗+事件深度分析（投资机会>=⭐⭐⭐时触发）"""
    cleaned_id = clean_single_item(raw_item_id)
    if not cleaned_id:
        return None

    # 检查投资机会评级，>=3星才做深度分析
    cleaned = execute_query("SELECT * FROM cleaned_items WHERE id=?", [cleaned_id])
    if not cleaned:
        return cleaned_id

    item = cleaned[0]
    # 从structured_json中读取投资机会星级
    stars = 0
    try:
        sj = json.loads(item.get("structured_json") or "{}")
        level = sj.get("opportunity", {}).get("overall", {}).get("level", "")
        stars = level.count("⭐")
    except (json.JSONDecodeError, TypeError):
        pass

    if stars < 3:
        return cleaned_id
    raw = execute_query("SELECT * FROM raw_items WHERE id=?", [raw_item_id])
    if not raw:
        return cleaned_id

    try:
        user_msg = f"事件: {item['summary']}\n\n详情: {raw[0]['content'] or ''}"
        event_result = call_claude_json(EVENT_ANALYSIS_PROMPT, user_msg)

        # 将事件分析结果追加到impact_analysis
        enhanced_analysis = (item.get("impact_analysis") or "") + "\n\n【深度分析】\n"
        enhanced_analysis += event_result.get("analysis", "")
        enhanced_analysis += f"\n建议: {event_result.get('action', '观望')}"

        execute_insert(
            "UPDATE cleaned_items SET impact_analysis=? WHERE id=?",
            [enhanced_analysis, cleaned_id],
        )
    except Exception as e:
        logger.warning(f"事件深度分析失败 cleaned_id={cleaned_id}: {e}")

    return cleaned_id

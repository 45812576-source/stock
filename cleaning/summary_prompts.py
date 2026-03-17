"""四族摘要 Prompt 模板

族1 (structured): announcement / financial_report / data_release / policy_doc / xlsx_data
族2 (analysis):   research_report / strategy_report / feature_news / roadshow_notes
族3 (informal):   social_post / chat_record
族4 (brief):      flash_news / market_commentary / digest_news拆条 / other

所有 prompt 均要求直接输出 JSON，不要前置说明。
"""

from config.doc_types import FAMILY_MAP

# ── 通用：AI 判断 doc_type ────────────────────────────────────────────────────

DOC_TYPE_CLASSIFY_PROMPT = """你是金融文档分类专家。请根据文本内容判断其文档类型。

直接输出 JSON（无其他内容）：
{"doc_type": "类型标识"}

类型标识枚举（选最匹配的一个）：
- announcement      公司公告（定期报告、重大事项、股权变动等）
- financial_report  财报正文（年报/半年报/季报）
- policy_doc        政策文件（政策文件/监管通知）
- research_report   研报（深度分析，含评级/目标价）
- strategy_report   策略报告（宏观/行业/主题，无具体标的评级）
- roadshow_notes    路演纪要（路演/调研/电话会议纪要）
- feature_news      专题新闻（有深度，有背景分析）
- flash_news        快讯（单条，事件驱动，短文本）
- digest_news       拼盘快讯（多条汇总，列表形式）
- data_release      数据播报（PMI、CPI、就业等经济数据发布）
- market_commentary 市场评论（盘中/盘后市场评论）
- social_post       社媒帖子（雪球、微博等，非正式语气）
- chat_record       聊天记录（对话形式，碎片化）
- xlsx_data         统计数据（Excel/CSV表格数据）
- other             其他"""


# ── 族4 前置：digest_news 拆条 ───────────────────────────────────────────────

DIGEST_SPLIT_PROMPT = """你是金融信息处理专家。以下是一篇拼盘快讯，包含多条独立新闻。

请将其拆分为独立的新闻条目，每条应是完整的一条信息。

直接输出 JSON 数组（无其他内容）：
[
  {"title": "条目标题或首句摘要（20字以内）", "text": "完整原文内容"},
  ...
]

注意：
- 保留每条的完整原文，不要缩写
- 如果实在无法拆分（文本本身就是单条），返回只含一个元素的数组
- 最多拆 20 条"""

# ── 族3 前置：social_post 拆条 ───────────────────────────────────────────────

SOCIAL_POST_SPLIT_PROMPT = """你是金融社媒内容分析专家。以下是一条社媒帖子，可能包含多个主题/公司的信息。

请按主题/公司拆分为独立条目。

直接输出 JSON 数组（无其他内容）：
[
  {
    "topic": "主题简述（20字以内）",
    "topics": ["主题1", "主题2"],
    "text": "该主题相关的完整原文片段",
    "has_data": true,
    "stocks": [{"name": "公司名", "code": "股票代码或空串"}]
  }
]

注意：
- has_data 为 true 表示该条包含具体数据/数字（如增速、价格、规模等）
- 如果帖子只有一个主题，返回只含一个元素的数组
- stocks 仅填写明确提到的上市公司，无则为空数组
- 最多拆 10 条"""


# ── 族1：结构化提取 ──────────────────────────────────────────────────────────

_FAMILY1_COMMON = """你是专业的金融信息提取专家。请对以下{type_label}进行结构化提取。

直接输出 JSON（无其他内容）：
{{
  "doc_type": "{doc_type}",
  "summary": "一句话核心摘要（50字以内）",
  "subject_entities": "涉及主体，逗号分隔（公司名/机构名/政策发布方）",
  "key_facts": {key_facts_schema},
  "key_data": {key_data_schema},
  "effective_date": "生效或发布日期 YYYY-MM-DD，无则留空",
  "impact_scope": "影响的行业/公司范围（50字以内）",
  "indicators": [
    {{
      "industry_l2": "二级行业名称（如：动力电池）",
      "metric_name": "指标名称（如：出货量同比增速）",
      "metric_type": "growth_rate/absolute/ratio/index",
      "value": 18.5,
      "value_raw": "原文表述（如：同比增长18.5%）",
      "period_label": "统计周期（如：2024Q3）",
      "period_year": 2024,
      "data_type": "actual/forecast",
      "confidence": "high/medium/low",
      "source_snippet": "原文句子（不超过60字）"
    }}
  ]
}}

注意：indicators 可为空数组 []，仅在文章中有明确行业量化数值时填写（有具体数字才填）。"""

PROMPTS_FAMILY1 = {
    "announcement": _FAMILY1_COMMON.format(
        type_label="公司公告",
        doc_type="announcement",
        key_facts_schema='{"type": "公告类型（分红/增发/回购/业绩预告等）", "terms": ["关键条款1", "条款2"], "conditions": ["前提条件"]}',
        key_data_schema='{"amount": "涉及金额", "ratio": "比例（如分红率）", "date": "重要时间节点"}',
    ),
    "financial_report": _FAMILY1_COMMON.format(
        type_label="财报",
        doc_type="financial_report",
        key_facts_schema='{"period": "报告期（如2024Q3）", "revenue": "营收", "revenue_yoy": "营收同比", "net_profit": "净利润", "net_profit_yoy": "净利润同比", "gross_margin": "毛利率", "segments": [{"name": "业务线", "revenue": "收入"}], "guidance": "业绩指引"}',
        key_data_schema='{"eps": "每股收益", "roe": "净资产收益率", "debt_ratio": "资产负债率", "cash": "货币资金"}',
    ),
    "data_release": _FAMILY1_COMMON.format(
        type_label="数据播报",
        doc_type="data_release",
        key_facts_schema='{"indicator": "指标名称（如CPI/PMI）", "actual": "实际值", "expected": "预期值", "previous": "前值", "trend": "趋势方向（上行/下行/持平）"}',
        key_data_schema='{"yoy": "同比", "mom": "环比", "region": "统计区域"}',
    ),
    "policy_doc": _FAMILY1_COMMON.format(
        type_label="政策文件",
        doc_type="policy_doc",
        key_facts_schema='{"issuer": "发布机构", "policy_type": "政策类型", "key_points": ["要点1", "要点2", "要点3"], "timeline": "实施时间表"}',
        key_data_schema='{"target_industries": ["受影响行业"], "enforcement_level": "执行力度（强制/指导/鼓励）"}',
    ),
    "xlsx_data": _FAMILY1_COMMON.format(
        type_label="统计数据表",
        doc_type="xlsx_data",
        key_facts_schema='{"dimensions": ["数据维度/列名"], "row_count": "大致行数", "time_range": "时间范围", "key_metrics": ["核心指标"]}',
        key_data_schema='{"max_values": {"指标": "最大值"}, "min_values": {"指标": "最小值"}, "notable_anomalies": ["异常/亮点"]}',
    ),
}

# 兜底：未知类型也走族1格式
PROMPTS_FAMILY1["other"] = _FAMILY1_COMMON.format(
    type_label="文档",
    doc_type="other",
    key_facts_schema='{"main_points": ["要点1", "要点2"]}',
    key_data_schema='{}',
)


# ── 族2：深度FOE分析 ─────────────────────────────────────────────────────────

_FAMILY2_COMMON = """你是专业的金融信息分析专家，擅长区分事实与观点（FOE分析）。请对以下{type_label}进行深度结构化分析。

直接输出 JSON（无其他内容）：
{{
  "doc_type": "{doc_type}",
  "summary": "一句话核心摘要（50字以内）",
  "fact_summary": "关键事实要点，换行分隔（可验证的客观信息）",
  "opinion_summary": "主要观点和判断（作者/机构/管理层的主观判断）",
  "evidence_assessment": "证据质量评估（数据来源、可信度、时效性）",
  "info_gaps": "重要但未提及的信息缺口",
  "key_arguments": {key_arguments_schema},
  "type_fields": {type_fields_schema},
  "indicators": [
    {{
      "industry_l2": "二级行业名称（如：动力电池）",
      "metric_name": "指标名称（如：出货量同比增速）",
      "metric_type": "growth_rate/absolute/ratio/index",
      "value": 18.5,
      "value_raw": "原文表述（如：同比增长18.5%）",
      "period_label": "统计周期（如：2024Q3）",
      "period_year": 2024,
      "data_type": "actual/forecast",
      "confidence": "high/medium/low",
      "source_snippet": "原文句子（不超过60字）"
    }}
  ]
}}

注意：indicators 可为空数组 []，仅在文章中有明确行业量化数值时填写（有具体数字才填）。"""

PROMPTS_FAMILY2 = {
    "research_report": _FAMILY2_COMMON.format(
        type_label="研究报告",
        doc_type="research_report",
        key_arguments_schema='[{"claim": "核心论点", "evidence": "支撑证据", "strength": "强/中/弱"}]',
        type_fields_schema='{"institution": "研究机构", "analyst": "分析师", "rating": "评级（买入/增持/中性/减持/卖出）", "target_price": "目标价", "current_price": "当前价（如有）", "eps_forecast": {"year": "预测年份", "eps": "EPS预测"}, "valuation_method": "估值方法", "risk_factors": ["主要风险1", "风险2"]}',
    ),
    "strategy_report": _FAMILY2_COMMON.format(
        type_label="策略报告",
        doc_type="strategy_report",
        key_arguments_schema='[{"claim": "核心观点", "evidence": "依据", "strength": "强/中/弱"}]',
        type_fields_schema='{"market_view": "市场观点（偏多/中性/偏空）", "sector_allocation": ["推荐行业"], "key_themes": ["主要投资主题"], "risk_factors": ["主要风险"], "time_horizon": "投资周期"}',
    ),
    "feature_news": _FAMILY2_COMMON.format(
        type_label="专题新闻",
        doc_type="feature_news",
        key_arguments_schema='[{"claim": "核心论点", "evidence": "支撑证据", "perspective": "谁的观点"}]',
        type_fields_schema='{"news_level": "宏观/行业/个股", "industry_chain": ["产业链受影响环节"], "multiple_perspectives": ["观点1", "观点2"], "background": "事件背景（50字）"}',
    ),
    "roadshow_notes": _FAMILY2_COMMON.format(
        type_label="路演/调研纪要",
        doc_type="roadshow_notes",
        key_arguments_schema='[{"question": "提问", "answer": "关键回答（50字以内）"}]',
        type_fields_schema='{"company": "公司名称", "management_guidance": ["管理层前瞻指引1", "指引2"], "new_disclosures": ["首次披露信息"], "key_qa": [{"q": "问题摘要", "a": "回答要点"}]}',
    ),
}


# ── 族3：非正式内容 ──────────────────────────────────────────────────────────

PROMPTS_FAMILY3 = {
    "social_post": """你是金融社媒内容分析专家。请对以下社媒帖子进行结构化提取。

直接输出 JSON（无其他内容）：
{
  "doc_type": "social_post",
  "summary": "一句话核心摘要（50字以内）",
  "speaker": "发帖人ID或名称",
  "speaker_type": "kol（有影响力）/institution（机构）/retail（散户）/unknown",
  "key_claims": [
    {"claim": "声称的事实", "verifiable": true或false, "importance": "高/中/低"}
  ],
  "opinions": [
    {"opinion": "观点内容", "sentiment": "bullish/bearish/neutral", "target": "针对的股票/行业"}
  ],
  "sentiment": "整体情绪：bullish/bearish/neutral",
  "indicators": []
}

注意：indicators 可为空数组 []，仅在帖子中有明确行业量化数值时填写（有具体数字才填）。""",

    "chat_record": """你是金融信息提取专家。请对以下聊天记录/群消息进行结构化提取。

直接输出 JSON（无其他内容）：
{
  "doc_type": "chat_record",
  "summary": "一句话核心摘要（50字以内）",
  "speaker": "主要发言人（如有）",
  "speaker_type": "kol/institution/retail/unknown",
  "key_claims": [
    {"claim": "可追踪的关键信息点", "verifiable": true或false, "importance": "高/中/低"}
  ],
  "opinions": [
    {"opinion": "观点内容", "sentiment": "bullish/bearish/neutral", "target": "针对的股票/行业"}
  ],
  "sentiment": "整体情绪：bullish/bearish/neutral",
  "indicators": []
}

注意：indicators 可为空数组 []，仅在对话中有明确行业量化数值时填写（有具体数字才填）。""",
}


# ── 族4：轻量摘要 ───────────────────────────────────────────────────────────

PROMPTS_FAMILY4 = {
    "flash_news": """你是金融快讯提取专家。请对以下快讯进行简洁结构化提取。

直接输出 JSON（无其他内容）：
{
  "doc_type": "flash_news",
  "summary": "一句话核心摘要（50字以内）",
  "event_what": "发生了什么事（30字以内）",
  "event_who": "涉及主体（公司/人物/机构）",
  "impact_target": "影响谁或哪个板块",
  "sentiment": "bullish/bearish/neutral",
  "indicators": []
}

注意：indicators 可为空数组 []，仅在快讯中有明确行业量化数值时填写（有具体数字才填）。""",

    "market_commentary": """你是金融市场评论分析专家。请对以下市场评论进行结构化提取。

直接输出 JSON（无其他内容）：
{
  "doc_type": "market_commentary",
  "summary": "一句话核心摘要（50字以内）",
  "event_what": "今日市场核心动态（客观事实，40字以内）",
  "event_who": "主要涉及板块/个股",
  "impact_target": "分析师归因/展望（主观判断，40字以内）",
  "sentiment": "整体市场情绪：bullish/bearish/neutral",
  "indicators": []
}

注意：indicators 可为空数组 []，仅在评论中有明确行业量化数值时填写（有具体数字才填）。""",

    "digest_news": """你是金融信息处理专家。这是一条从拼盘快讯拆出的单条新闻，请进行简洁结构化提取。

直接输出 JSON（无其他内容）：
{
  "doc_type": "flash_news",
  "summary": "一句话核心摘要（50字以内）",
  "event_what": "发生了什么事（30字以内）",
  "event_who": "涉及主体",
  "impact_target": "影响谁或哪个板块",
  "sentiment": "bullish/bearish/neutral",
  "indicators": []
}

注意：indicators 可为空数组 []，仅在新闻中有明确行业量化数值时填写（有具体数字才填）。""",

    "other": """你是金融信息处理专家。请对以下文本进行简洁结构化提取。

直接输出 JSON（无其他内容）：
{
  "doc_type": "other",
  "summary": "一句话核心摘要（50字以内）",
  "event_what": "主要内容（40字以内）",
  "event_who": "涉及主体",
  "impact_target": "可能影响的对象",
  "sentiment": "bullish/bearish/neutral",
  "indicators": []
}

注意：indicators 可为空数组 []，仅在文本中有明确行业量化数值时填写（有具体数字才填）。""",
}


# ── 统一分发入口 ─────────────────────────────────────────────────────────────

def get_summary_prompt(doc_type: str) -> tuple[str, int]:
    """根据 doc_type 返回 (prompt, family)

    Returns:
        (prompt_text, family_number)
    """
    family = FAMILY_MAP.get(doc_type, 4)

    if family == 1:
        prompt = PROMPTS_FAMILY1.get(doc_type, PROMPTS_FAMILY1["other"])
    elif family == 2:
        prompt = PROMPTS_FAMILY2.get(doc_type)
        if not prompt:
            # 兜底用 feature_news 格式
            prompt = PROMPTS_FAMILY2["feature_news"]
    elif family == 3:
        prompt = PROMPTS_FAMILY3.get(doc_type, PROMPTS_FAMILY3["social_post"])
    else:
        prompt = PROMPTS_FAMILY4.get(doc_type, PROMPTS_FAMILY4["other"])

    return prompt, family

"""llm_calls — LLM Call 1（关键词提取）+ Call 2（筛选条件拆解）

使用 claude_client.call_claude，prompt 极短，任务单一。
"""
import json
import logging
import re
from utils.claude_client import call_claude

logger = logging.getLogger(__name__)

# ==================== Call 1: 关键词提取 ====================

_CALL1_SYSTEM = """你是一个投资关键词提取器。从用户文本中提取投资相关关键词，严格输出JSON，不要任何解释。"""

_CALL1_TEMPLATE = """从以下文本中提取投资相关的关键词。

文本：{text}

要求：
1. 提取3-8个最关键的投资概念词（行业名、政策名、宏观指标、商品名、主题词）
2. 判断这些词最可能对应的实体类型（可选：theme/policy/industry/macro_indicator/commodity/consumer_good/industry_chain/company）
3. 判断用户意图对应的关系方向（受益=benefits, 受损=hurts, 供应链=supplier_of, 同行=competitor, 无特定=belongs_to_industry）

严格JSON输出（不要markdown代码块）：
{{"keywords":["词1","词2"],"entity_types":["type1","type2"],"relation_hint":"benefits"}}"""


def call1_extract_keywords(user_message: str) -> dict:
    """LLM Call 1: 从用户输入提取关键词和意图

    Returns:
        {"keywords": [...], "entity_types": [...], "relation_hint": "..."}
        失败时返回空结构
    """
    text = user_message[:2000]
    prompt = _CALL1_TEMPLATE.format(text=text)
    try:
        raw = call_claude(_CALL1_SYSTEM, prompt, max_tokens=300, timeout=60)
        return _parse_json(raw, {"keywords": [], "entity_types": [], "relation_hint": "benefits"})
    except Exception as e:
        logger.error(f"call1_extract_keywords failed: {e}")
        return {"keywords": [], "entity_types": [], "relation_hint": "benefits"}


# ==================== Call 2: 筛选条件拆解 ====================

_CALL2_SYSTEM = """你是一个股票筛选条件解析器。分析用户的筛选意图，输出结构化JSON，不要任何解释。"""

_CALL2_TEMPLATE = """用户想从一批股票中筛选，分析他的筛选条件。

用户原话：{text}

可用的筛选模块：
1. kline_calc: K线技术（consecutive_yang连阳/ma_bullish均线多头/break_ma突破均线/macd_divergence背离/volume_breakout放量突破/pullback_support回踩/box_breakout箱体突破/macd_momentum动能/cumulative_change涨跌幅/max_drawdown回撤）
2. capital_tracker: 资金流向（consecutive_inflow连续净流入/net_inflow_sum净流入合计/shareholder_decrease股东户数下降）
3. financial_scanner: 财务指标（roe_stable_high/revenue_accelerating/profit_accelerating/cashflow_quality/margin_improving）
4. holder_tracker: 机构持仓（institution_new_entry/insider_net_buy/equity_incentive）
5. peer_ranker: 行业排名（pe_lowest_quantile/valuation_history_low/high_dividend/low_attention/laggard_catch_up）

如果用户没有明确筛选条件，输出空modules数组。

严格JSON输出（不要markdown代码块）：
{{"modules":[{{"skill":"模块名","action":"动作","params":{{}}}}],"combine":"AND","sort_by":"","limit":10}}"""


def call2_parse_filters(user_message: str) -> dict:
    """LLM Call 2: 解析用户筛选条件为结构化模块列表

    Returns:
        {"modules": [...], "combine": "AND", "sort_by": "", "limit": 10}
    """
    text = user_message[:500]
    prompt = _CALL2_TEMPLATE.format(text=text)
    default = {"modules": [], "combine": "AND", "sort_by": "", "limit": 10}
    try:
        raw = call_claude(_CALL2_SYSTEM, prompt, max_tokens=400, timeout=60)
        result = _parse_json(raw, default)
        # 确保 limit 有值
        if not result.get("limit"):
            result["limit"] = 10
        return result
    except Exception as e:
        logger.error(f"call2_parse_filters failed: {e}")
        return default


# ==================== Call 4: 推荐理由 ====================

_CALL4_SYSTEM = """你是一个股票推荐理由生成器。用一句话说明推荐原因，不超过60字，不要任何解释。"""

_CALL4_TEMPLATE = """为以下股票生成一句话推荐理由。

股票：{name}（{code}）
KG关联路径：{paths}
命中筛选条件：{conditions}

输出一句话推荐理由（不超过60字）："""


def call4_reason(code: str, name: str, paths: list[str], conditions: list[str]) -> str:
    """LLM Call 4: 生成单只股票推荐理由"""
    paths_str = "；".join(paths[:3]) if paths else "无"
    cond_str = "、".join(conditions[:5]) if conditions else "无"
    prompt = _CALL4_TEMPLATE.format(name=name, code=code, paths=paths_str, conditions=cond_str)
    try:
        return call_claude(_CALL4_SYSTEM, prompt, max_tokens=100, timeout=30).strip()
    except Exception as e:
        logger.error(f"call4_reason failed for {code}: {e}")
        return f"{name}符合筛选条件"


# ==================== Call 5: 整体总结（可选） ====================

_CALL5_SYSTEM = """你是一个投资分析师。用2-3句话总结选股结果，简洁客观，不超过100字。"""

_CALL5_TEMPLATE = """以下是选股结果，请给出2-3句整体判断。

用户问题：{question}
入选股票：
{stocks}

输出2-3句整体判断："""


def call5_summary(question: str, stocks: list[dict]) -> str:
    """LLM Call 5: 整体总结（复杂问题才触发）"""
    stocks_str = "\n".join(
        f"- {s['name']}({s['code']}): {s.get('reason', '')}" for s in stocks[:10]
    )
    prompt = _CALL5_TEMPLATE.format(question=question[:300], stocks=stocks_str)
    try:
        return call_claude(_CALL5_SYSTEM, prompt, max_tokens=200, timeout=60).strip()
    except Exception as e:
        logger.error(f"call5_summary failed: {e}")
        return ""


# ==================== 工具函数 ====================

def _parse_json(raw: str, default: dict) -> dict:
    """从 LLM 输出中提取 JSON，容错处理"""
    if not raw:
        return default
    # 去掉 markdown 代码块
    raw = re.sub(r"```(?:json)?\s*", "", raw).strip().rstrip("`").strip()
    # 找第一个 { 到最后一个 }
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1:
        logger.warning(f"_parse_json: no JSON found in: {raw[:200]}")
        return default
    try:
        return json.loads(raw[start:end + 1])
    except json.JSONDecodeError as e:
        logger.warning(f"_parse_json: decode error {e}, raw: {raw[:200]}")
        return default

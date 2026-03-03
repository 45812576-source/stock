"""产业需求传导数据提取器

在 Step1 完成后、Step2 执行前运行。
RAG 检索下游行业增速、上游成本价格、公司产能、竞对扩产，
经 LLM 结构化提取后注入 Step2 prompt。

复用 indicator_data_fetcher 的 _search_with_time_decay + _get_embed_model 模式。
"""
import logging
from typing import Optional

from research.indicator_data_fetcher import (
    _search_with_time_decay,
    _multi_dim_search,
)
from utils.model_router import call_model_json as _call_model_json

logger = logging.getLogger(__name__)


# ── LLM 提取 prompt ──────────────────────────────────────────

_DOWNSTREAM_EXTRACT_PROMPT = """\
你是产业研究数据提取专家。从以下研报/新闻摘要中提取下游行业增速和行业供需数据。

要求：
1. 只提取有明确数据来源的事实，不推测
2. downstream_growth: 按下游行业分组，每个行业提取最新实际增速和预测增速
3. industry_supply_demand: 提取行业整体产能、需求、库存信号
4. 如无相关数据，对应数组/字段为空或null

输出 JSON：
{
    "downstream_growth": [
        {
            "industry": "行业名",
            "recent_growth_pct": 数字或null,
            "period": "如2024",
            "forecast_growth_pct": 数字或null,
            "forecast_period": "如2025E",
            "data_type": "actual或estimated",
            "source_snippet": "原文30字"
        }
    ],
    "industry_supply_demand": {
        "total_new_capacity": "描述或null",
        "total_demand_growth": "描述或null",
        "balance": "shortage/balance/surplus或null",
        "inventory_level": "high/normal/low或null",
        "source_snippet": "原文30字或null"
    }
}"""

_UPSTREAM_EXTRACT_PROMPT = """\
你是产业研究数据提取专家。从以下研报/新闻摘要中提取上游成本价格、公司产能信号和竞对扩产数据。

要求：
1. 只提取有明确数据来源的事实，不推测
2. upstream_costs: 提取关键投入品价格走势
3. company_capacity: 区分 direct(公告)和 indirect(推断)信号
4. competitor_expansion: 主要竞对的扩产计划
5. 如无相关数据，对应数组为空

输出 JSON：
{
    "upstream_costs": [
        {
            "input_name": "投入品名",
            "price_trend": "上涨/下跌/平稳",
            "recent_price": "价格描述或null",
            "yoy_change_pct": 数字或null,
            "data_type": "actual或estimated",
            "source_snippet": "原文30字"
        }
    ],
    "company_capacity": [
        {
            "project": "项目名",
            "signal_type": "direct或indirect",
            "detail": "描述",
            "expected_time": "时间或null",
            "source_snippet": "原文30字"
        }
    ],
    "competitor_expansion": [
        {
            "competitor": "公司名",
            "expansion": "扩产描述",
            "impact_on_supply": "对供给的影响",
            "source_snippet": "原文30字"
        }
    ]
}"""


# ── RAG 检索 ──────────────────────────────────────────────────

def _query_indicator_db(industry_name: str, period_year: int = 2024) -> list[dict]:
    """从 industry_indicators 表精准查询下游行业增速数据。

    返回格式与 _extract_downstream() 的 downstream_growth[] 兼容：
    [{"industry": str, "recent_growth_pct": float|None, "period": str,
      "forecast_growth_pct": float|None, "forecast_period": str,
      "data_type": str, "source_snippet": str, "_from_indicator_db": True}]
    """
    try:
        from utils.db_utils import query_industry_indicator
    except ImportError:
        return []

    # 查实际增速
    actual_rows = query_industry_indicator(
        industry_name, metric_type="growth_rate", period_year=period_year, data_type="actual"
    )
    # 查预测增速
    forecast_rows = query_industry_indicator(
        industry_name, metric_type="growth_rate", period_year=period_year, data_type="forecast"
    )

    if not actual_rows and not forecast_rows:
        # 降级：扩大年份范围再试
        actual_rows = query_industry_indicator(
            industry_name, metric_type="growth_rate", period_year=period_year - 1
        )

    if not actual_rows and not forecast_rows:
        return []

    result = {
        "industry": industry_name,
        "recent_growth_pct": None,
        "period": "",
        "forecast_growth_pct": None,
        "forecast_period": "",
        "data_type": "actual",
        "source_snippet": "",
        "_from_indicator_db": True,
    }

    if actual_rows:
        r = actual_rows[0]
        result["recent_growth_pct"] = float(r["value"]) if r.get("value") is not None else None
        result["period"] = r.get("period_label") or str(r.get("period_year") or "")
        result["source_snippet"] = r.get("source_snippet") or r.get("value_raw") or ""
        result["data_type"] = "actual"

    if forecast_rows:
        f = forecast_rows[0]
        result["forecast_growth_pct"] = float(f["value"]) if f.get("value") is not None else None
        result["forecast_period"] = f.get("forecast_target_label") or f.get("period_label") or ""

    return [result]


def _build_downstream_queries(stock_name: str, customer_industries: list[dict]) -> list[str]:
    """批1: 按下游行业构建多级查询（精准→细分/相关→终端应用）

    三级降级策略：
      L1 精准行业名（优先，精确匹配）
      L2 细分/相关行业（行业找不到时，搜子行业或上位行业）
      L3 终端应用/产量指标（再退一步，搜终端需求代理指标）
    """
    # 行业名 → (L2相关词, L3终端词) 映射
    _FUZZY_MAP = {
        "新能源电池": ("锂电池 动力电池 储能电池", "新能源汽车销量 储能装机量 电化学储能"),
        "电池制造": ("锂电池 动力电池 储能电池", "新能源汽车销量 储能装机量"),
        "新能源汽车": ("电动汽车 纯电动 插混", "乘用车销量 新能源渗透率"),
        "特种钢": ("高强钢 合金钢 工具钢 优特钢", "机械制造 汽车用钢 工程机械"),
        "高端制造": ("高端装备 精密制造 数控机床", "工业机器人 航空航天 半导体设备"),
        "电力基建": ("输变电 电网投资 特高压 配网", "电网工程量 变电容量 电力装机"),
        "光伏": ("光伏组件 光伏电站 太阳能", "光伏装机量 组件出货量 GW"),
        "风电": ("风力发电 陆上风电 海上风电", "风电装机量 风机出货量"),
        "半导体": ("芯片 集成电路 晶圆", "半导体设备 晶圆产能 芯片出货"),
        "航空航天": ("飞机 商用飞机 军机 航空发动机", "飞机交付量 民航客运量"),
        "建筑": ("房地产 基础设施 建材", "新开工面积 施工面积 基建投资"),
        "农业": ("农业机械 农用化学品 农药化肥", "粮食产量 农机保有量"),
    }

    queries = []
    for ci in (customer_industries or [])[:6]:
        name = ci.get("name", "")
        if not name:
            continue
        # L1: 精准搜索
        queries.append(f"{name} 行业增速 产值 产出 市场规模 2024 2025")
        # L2/L3: 模糊降级搜索（遍历映射表，名称含关键词即触发）
        for key, (l2_terms, l3_terms) in _FUZZY_MAP.items():
            if key in name or name in key:
                queries.append(f"{l2_terms} 增速 规模 2024 2025")
                queries.append(f"{l3_terms} 数据 增长 2024 2025")
                break
        else:
            # 没有精确映射：通用降级——加"细分行业"和"产量/装机量/出货量"
            queries.append(f"{name} 细分 子行业 增速 需求 2024")
            queries.append(f"{name} 产量 出货量 装机量 产值 同比 2024")

    if not queries:
        queries.append(f"{stock_name} 下游行业 需求增速 市场规模 2024 2025")
    return queries


def _build_upstream_queries(stock_name: str, industry: str,
                             cost_breakdown: list[dict]) -> list[str]:
    """批2: 上游成本 + 公司产能"""
    cost_names = " ".join(c.get("name", "") for c in (cost_breakdown or [])[:4])
    return [
        f"{stock_name} {industry} 原材料价格 成本 {cost_names}".strip(),
        f"{stock_name} 产能 在建工程 扩产 投产 项目进度",
    ]


def _build_competition_queries(stock_name: str, industry: str) -> list[str]:
    """批3: 竞对扩产 + 行业供需"""
    return [
        f"{industry} 新增产能 扩产 产能过剩 供需 库存",
        f"{stock_name} 竞争对手 市场份额 行业集中度",
    ]


# ── LLM 提取 ─────────────────────────────────────────────────

def _extract_downstream(raw_texts: list[dict], stock_name: str,
                          customer_industries: list[dict]) -> dict:
    """调 DeepSeek 从 RAG 文本中提取下游需求 + 行业供需"""
    if not raw_texts:
        return {}

    text_block = "\n---\n".join(t["text"][:600] for t in raw_texts[:15])
    ci_names = "、".join(ci.get("name", "") for ci in (customer_industries or [])[:6])

    user_message = (
        f"股票：{stock_name}\n"
        f"需关注的下游行业：{ci_names or '（未指定）'}\n\n"
        f"以下为RAG检索到的相关信息：\n{text_block}"
    )

    try:
        result = _call_model_json("cleaning", _DOWNSTREAM_EXTRACT_PROMPT,
                                   user_message, max_tokens=2048, timeout=60)
        return result if isinstance(result, dict) else {}
    except Exception as e:
        logger.warning(f"下游需求提取失败: {e}")
        return {}


def _extract_upstream(raw_texts: list[dict], stock_name: str) -> dict:
    """调 DeepSeek 从 RAG 文本中提取上游成本 + 产能 + 竞对"""
    if not raw_texts:
        return {}

    text_block = "\n---\n".join(t["text"][:600] for t in raw_texts[:15])

    user_message = (
        f"股票：{stock_name}\n\n"
        f"以下为RAG检索到的相关信息：\n{text_block}"
    )

    try:
        result = _call_model_json("cleaning", _UPSTREAM_EXTRACT_PROMPT,
                                   user_message, max_tokens=2048, timeout=60)
        return result if isinstance(result, dict) else {}
    except Exception as e:
        logger.warning(f"上游成本提取失败: {e}")
        return {}


# ── 注入文本格式化 ────────────────────────────────────────────

def _format_injection_text(downstream: dict, upstream: dict) -> str:
    """格式化为可注入 Step2 user_message 的文本"""
    parts = []

    # 下游行业增速
    dg = downstream.get("downstream_growth") or []
    if dg:
        parts.append("=== RAG检索：下游行业增速数据 ===")
        for d in dg:
            industry = d.get("industry", "未知")
            recent = d.get("recent_growth_pct")
            period = d.get("period", "")
            forecast = d.get("forecast_growth_pct")
            fperiod = d.get("forecast_period", "")
            dtype = d.get("data_type", "")
            snippet = d.get("source_snippet", "")
            line = f"  [{industry}]"
            if recent is not None:
                line += f" {period}{'实际' if dtype == 'actual' else '估算'}增速: {recent:+.1f}%"
            if snippet:
                line += f" (来源: {snippet})"
            if forecast is not None:
                line += f" | {fperiod}预测: {forecast:+.1f}%"
            parts.append(line)

    # 行业供需
    sd = downstream.get("industry_supply_demand") or {}
    if sd.get("balance") or sd.get("total_new_capacity"):
        parts.append("\n=== RAG检索：行业供需格局 ===")
        if sd.get("total_new_capacity"):
            parts.append(f"  [新增产能] {sd['total_new_capacity']}")
        if sd.get("total_demand_growth"):
            parts.append(f"  [需求增量] {sd['total_demand_growth']}")
        if sd.get("balance"):
            parts.append(f"  [供需平衡] {sd['balance']}")
        if sd.get("inventory_level"):
            parts.append(f"  [库存水平] {sd['inventory_level']}")

    # 上游成本价格
    uc = upstream.get("upstream_costs") or []
    if uc:
        parts.append("\n=== RAG检索：上游成本价格 ===")
        for u in uc:
            name = u.get("input_name", "未知")
            trend = u.get("price_trend", "")
            price = u.get("recent_price", "")
            yoy = u.get("yoy_change_pct")
            dtype = u.get("data_type", "")
            line = f"  [{name}]"
            if price:
                line += f" 当前: {price}"
            if yoy is not None:
                line += f", YoY: {yoy:+.1f}%"
            if trend:
                line += f" ({trend})"
            if dtype:
                line += f" [{dtype}]"
            parts.append(line)

    # 公司产能信号
    cc = upstream.get("company_capacity") or []
    if cc:
        parts.append("\n=== RAG检索：公司产能信号 ===")
        for c in cc:
            st = c.get("signal_type", "unknown")
            project = c.get("project", "")
            detail = c.get("detail", "")
            time = c.get("expected_time", "")
            line = f"  [{st}] {project}"
            if detail:
                line += f" — {detail}"
            if time:
                line += f" ({time})"
            parts.append(line)

    # 竞对扩产
    ce = upstream.get("competitor_expansion") or []
    if ce:
        parts.append("\n=== RAG检索：竞对扩产 ===")
        for c in ce:
            comp = c.get("competitor", "")
            exp = c.get("expansion", "")
            impact = c.get("impact_on_supply", "")
            line = f"  [{comp}] {exp}"
            if impact:
                line += f" → {impact}"
            parts.append(line)

    return "\n".join(parts)


# ── 主入口 ────────────────────────────────────────────────────

def fetch_industry_demand_data(
    stock_code: str,
    stock_name: str,
    industry_l1: str,
    customer_industries: list[dict],
    revenue_segments: list[dict],
    cost_breakdown: list[dict],
    step1_for_valuation: dict,
) -> dict:
    """主入口：编排 RAG多维搜索 → LLM结构化提取 → 格式化注入文本

    Returns:
        {
            "downstream": {...},    # 下游需求 + 行业供需
            "upstream": {...},      # 上游成本 + 产能 + 竞对
            "injection_text": str,  # 可直接注入 Step2 user_message
        }
    """
    industry = industry_l1 or ""

    # ── 优先查 industry_indicators 结构化库 ─────────────────────────
    db_downstream_growth = []
    for ci in (customer_industries or [])[:6]:
        ci_name = ci.get("name", "")
        if not ci_name:
            continue
        db_rows = _query_indicator_db(ci_name, period_year=2024)
        db_downstream_growth.extend(db_rows)
        if db_rows:
            logger.info(f"[demand_fetcher] SQL命中: {ci_name} → {db_rows[0].get('recent_growth_pct')}%")

    # 仅对 SQL 未命中的行业做 RAG 搜索
    db_covered_industries = {r["industry"] for r in db_downstream_growth}
    uncovered_cis = [ci for ci in (customer_industries or []) if ci.get("name") not in db_covered_industries]

    # 批1: 下游需求查询
    dq = _build_downstream_queries(stock_name, uncovered_cis)
    downstream_texts = _multi_dim_search(stock_code, dq, top_k=6)
    logger.info(f"[demand_fetcher] 批1下游需求: {len(downstream_texts)}条")

    # 批2: 上游成本 + 产能
    uq = _build_upstream_queries(stock_name, industry, cost_breakdown)
    # 批3: 竞对 + 供需
    cq = _build_competition_queries(stock_name, industry)
    upstream_texts = _multi_dim_search(stock_code, uq + cq, top_k=8)
    logger.info(f"[demand_fetcher] 批2+3上游/竞对: {len(upstream_texts)}条")

    # LLM 结构化提取
    downstream = _extract_downstream(downstream_texts, stock_name, customer_industries)
    upstream = _extract_upstream(upstream_texts, stock_name)

    # 合并 SQL 命中的数据到 downstream 结构
    if db_downstream_growth:
        existing = downstream.get("downstream_growth") or []
        existing_industries = {d.get("industry") for d in existing}
        for db_row in db_downstream_growth:
            if db_row["industry"] not in existing_industries:
                existing.append(db_row)
        downstream["downstream_growth"] = existing

    # 格式化注入文本
    injection_text = _format_injection_text(downstream, upstream)

    logger.info(
        f"[demand_fetcher] 结果: downstream_growth={len(downstream.get('downstream_growth', []))}, "
        f"upstream_costs={len(upstream.get('upstream_costs', []))}, "
        f"company_capacity={len(upstream.get('company_capacity', []))}, "
        f"competitor_expansion={len(upstream.get('competitor_expansion', []))}, "
        f"injection_text={len(injection_text)}字"
    )

    return {
        "downstream": downstream,
        "upstream": upstream,
        "injection_text": injection_text,
    }

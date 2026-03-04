"""KG + Industry Indicator 桥接层

三层降级取数：
  L1 KG名称桥接 → resolve_industry_names → 标准实体名 → query_industry_indicator
  L2 KG产业链展开 → expand_company_chain → customer_of/supplier_of → query_industry_indicator
  L3 返回未覆盖列表由调用方做 RAG 兜底

对外接口：
  query_downstream_indicators(stock_code, customer_industries, period_year) -> list[dict]
  query_upstream_indicators(stock_code, cost_breakdown, period_year) -> list[dict]
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── 行业类 entity_type 集合 ─────────────────────────────────────
_INDUSTRY_TYPES = {"industry", "industry_chain"}


# ── Layer 1: KG 名称桥接 ─────────────────────────────────────────

def resolve_industry_names(fuzzy_names: list[str]) -> dict[str, list[str]]:
    """将 LLM 生成的模糊行业名映射到 KG 中的标准实体名。

    对每个 fuzzy_name，搜索 kg_entities（entity_type=industry 或 industry_chain）。
    返回 {fuzzy_name: [标准entity_name_1, ...]}，未匹配的映射到空列表。
    """
    try:
        from knowledge_graph.kg_query import search_entities
    except ImportError:
        logger.warning("[kg_bridge] kg_query 不可用，跳过 L1 桥接")
        return {n: [] for n in fuzzy_names}

    result: dict[str, list[str]] = {}
    for name in fuzzy_names:
        if not name:
            result[name] = []
            continue
        matched: list[str] = []
        for etype in ("industry", "industry_chain"):
            rows = search_entities(name, entity_type=etype, limit=5)
            if rows:
                matched.extend(r["entity_name"] for r in rows)
        # 去重保序
        seen: set[str] = set()
        deduped: list[str] = []
        for n in matched:
            if n not in seen:
                seen.add(n)
                deduped.append(n)
        result[name] = deduped
        if deduped:
            logger.debug(f"[kg_bridge] L1 {name!r} → {deduped[:3]}")
    return result


# ── Layer 2: KG 产业链展开 ─────────────────────────────────────────

def expand_company_chain(stock_code: str) -> dict:
    """获取公司的产业链关联实体名称。

    Returns:
        {
            "industries": [str],          # 公司所属行业名
            "customer_of": [str],         # 下游实体（公司是其供应商）
            "supplier_of": [str],         # 上游实体（公司是其客户）
            "cost_elements": [str],       # 主要成本项目名
            "revenue_elements": [str],    # 主要收入项目名
        }
    """
    empty = {
        "industries": [],
        "customer_of": [],
        "supplier_of": [],
        "cost_elements": [],
        "revenue_elements": [],
    }
    try:
        from knowledge_graph.kg_query import get_company_context, search_entities
    except ImportError:
        logger.warning("[kg_bridge] kg_query 不可用，跳过 L2 展开")
        return empty

    ctx = get_company_context(stock_code)
    if not ctx:
        logger.debug(f"[kg_bridge] get_company_context({stock_code!r}) 无结果")
        return empty

    industries = [r["entity_name"] for r in (ctx.get("industries") or [])]
    cost_elements = [r["entity_name"] for r in (ctx.get("cost_elements") or [])]
    revenue_elements = [r["entity_name"] for r in (ctx.get("revenue_elements") or [])]

    customer_of: list[str] = []
    supplier_of: list[str] = []
    for sc in (ctx.get("supply_chain") or []):
        rel = sc.get("relation_type", "")
        name = sc.get("entity_name", "")
        if not name:
            continue
        if rel == "customer_of":
            customer_of.append(name)
        elif rel == "supplier_of":
            supplier_of.append(name)

    # 对产业链实体进一步展开 depth=1：找它们关联的 industry 实体
    def _expand_to_industry(entity_names: list[str]) -> list[str]:
        extra: list[str] = []
        for ename in entity_names[:6]:
            for etype in ("industry", "industry_chain"):
                rows = search_entities(ename, entity_type=etype, limit=3)
                extra.extend(r["entity_name"] for r in rows)
        return list(dict.fromkeys(extra))  # 去重保序

    expanded_customer_industries = _expand_to_industry(customer_of)
    expanded_supplier_industries = _expand_to_industry(supplier_of)

    logger.debug(
        f"[kg_bridge] L2 {stock_code}: industries={industries}, "
        f"customer_of={customer_of[:3]}, supplier_of={supplier_of[:3]}, "
        f"expanded_downstream={expanded_customer_industries[:3]}, "
        f"expanded_upstream={expanded_supplier_industries[:3]}"
    )

    return {
        "industries": industries,
        "customer_of": customer_of,
        "customer_industries": expanded_customer_industries,  # 下游展开行业
        "supplier_of": supplier_of,
        "supplier_industries": expanded_supplier_industries,  # 上游展开行业
        "cost_elements": cost_elements,
        "revenue_elements": revenue_elements,
    }


# ── 批量查指标 ─────────────────────────────────────────────────────

def _query_indicators_batch(
    industry_names: list[str],
    metric_type: str,
    period_year: int,
) -> list[dict]:
    """批量查 industry_indicators，对每个行业名查询，去重合并结果。"""
    try:
        from utils.db_utils import query_industry_indicator
    except ImportError:
        return []

    results: list[dict] = []
    seen_industries: set[str] = set()

    for name in industry_names:
        if not name or name in seen_industries:
            continue
        rows = query_industry_indicator(
            name, metric_type=metric_type, period_year=period_year
        )
        if rows:
            seen_industries.add(name)
            results.extend(rows)
            logger.debug(f"[kg_bridge] indicator命中: {name} ({metric_type}) → {len(rows)}条")

    return results


# ── 下游需求增速 ──────────────────────────────────────────────────

def query_downstream_indicators(
    stock_code: str,
    customer_industries: list[dict],
    period_year: int = 2024,
) -> list[dict]:
    """三层降级查下游行业增速数据。

    Args:
        stock_code: 股票代码（用于 KG 产业链展开）
        customer_industries: Step1 输出的下游行业列表，每项含 "name" 字段
        period_year: 优先查询的年份

    Returns:
        与 _query_indicator_db 兼容的结构：
        [{
            "industry": str,
            "recent_growth_pct": float|None,
            "period": str,
            "forecast_growth_pct": float|None,
            "forecast_period": str,
            "data_type": str,
            "source_snippet": str,
            "_from": "kg_bridge"|"kg_expand"|"direct",  # 来源标记
            "_from_indicator_db": True,
        }]
        以及 "_uncovered_names": list[str]（供调用方 RAG 兜底）
    """
    fuzzy_names = [ci.get("name", "") for ci in (customer_industries or []) if ci.get("name")]
    if not fuzzy_names:
        return []

    covered: set[str] = set()  # 已覆盖的原始 fuzzy_name
    all_results: list[dict] = []

    def _indicator_rows_to_downstream(rows: list[dict], orig_name: str, source_tag: str) -> list[dict]:
        """将 indicator DB 行转换为 downstream_growth 格式。"""
        actual_rows = [r for r in rows if r.get("data_type") == "actual"]
        forecast_rows = [r for r in rows if r.get("data_type") == "forecast"]
        # 也尝试 period_year-1 兜底（已在 _query_indicators_batch → query_industry_indicator 内降级）

        if not actual_rows and not forecast_rows:
            return []

        result = {
            "industry": orig_name,
            "recent_growth_pct": None,
            "period": "",
            "forecast_growth_pct": None,
            "forecast_period": "",
            "data_type": "actual",
            "source_snippet": "",
            "_from": source_tag,
            "_from_indicator_db": True,
        }
        if actual_rows:
            r = actual_rows[0]
            result["recent_growth_pct"] = float(r["value"]) if r.get("value") is not None else None
            result["period"] = r.get("period_label") or str(r.get("period_year") or "")
            result["source_snippet"] = r.get("source_snippet") or r.get("value_raw") or ""
        if forecast_rows:
            f = forecast_rows[0]
            result["forecast_growth_pct"] = float(f["value"]) if f.get("value") is not None else None
            result["forecast_period"] = f.get("forecast_target_label") or f.get("period_label") or ""
        return [result]

    # ── L1: KG 名称桥接 ───────────────────────────────────────────
    l1_mapping = resolve_industry_names(fuzzy_names)

    for orig_name in fuzzy_names:
        std_names = l1_mapping.get(orig_name) or []
        # 先试原始名
        candidate_names = list(dict.fromkeys([orig_name] + std_names))
        rows = _query_indicators_batch(candidate_names, "growth_rate", period_year)
        if not rows:
            # 降级：period_year - 1
            rows = _query_indicators_batch(candidate_names, "growth_rate", period_year - 1)
        if rows:
            converted = _indicator_rows_to_downstream(rows, orig_name, "kg_bridge" if std_names else "direct")
            if converted:
                all_results.extend(converted)
                covered.add(orig_name)

    # ── L2: KG 产业链展开 ─────────────────────────────────────────
    uncovered_after_l1 = [n for n in fuzzy_names if n not in covered]
    if uncovered_after_l1:
        chain = expand_company_chain(stock_code)
        # 从 customer_of 实体 + customer_industries 找行业名
        l2_candidates = list(dict.fromkeys(
            (chain.get("customer_industries") or []) +
            (chain.get("customer_of") or [])
        ))

        if l2_candidates:
            rows = _query_indicators_batch(l2_candidates, "growth_rate", period_year)
            if not rows:
                rows = _query_indicators_batch(l2_candidates, "growth_rate", period_year - 1)

            # 将 L2 结果分配给未覆盖的 orig_name（简单策略：按顺序匹配）
            if rows:
                # 为每个未覆盖行业尝试最佳匹配的 L2 行
                for orig_name in uncovered_after_l1:
                    converted = _indicator_rows_to_downstream(rows, orig_name, "kg_expand")
                    if converted:
                        all_results.extend(converted)
                        covered.add(orig_name)
                        break  # 只用一次 L2 结果（避免重复）

    # 记录未覆盖的行业供 RAG 兜底
    uncovered = [n for n in fuzzy_names if n not in covered]
    if uncovered:
        logger.info(f"[kg_bridge] 下游未覆盖（RAG兜底）: {uncovered}")
    else:
        logger.info(f"[kg_bridge] 下游指标全部命中，覆盖: {list(covered)}")

    # 附加元信息方便调用方
    for r in all_results:
        r.setdefault("_uncovered_names", uncovered)

    return all_results


# ── 上游成本价格 ──────────────────────────────────────────────────

def query_upstream_indicators(
    stock_code: str,
    cost_breakdown: list[dict],
    period_year: int = 2024,
) -> list[dict]:
    """三层降级查上游成本/价格数据。

    Args:
        stock_code: 股票代码
        cost_breakdown: Step1 输出的成本结构列表，每项含 "name" 字段
        period_year: 优先查询年份

    Returns:
        与 _extract_upstream()['upstream_costs'] 兼容的结构：
        [{
            "input_name": str,
            "price_trend": str,
            "recent_price": str,
            "yoy_change_pct": float|None,
            "data_type": str,
            "source_snippet": str,
            "_from": "kg_bridge"|"kg_expand"|"direct",
            "_from_indicator_db": True,
        }]
    """
    cost_names = [c.get("name", "") for c in (cost_breakdown or []) if c.get("name")]

    covered: set[str] = set()
    all_results: list[dict] = []

    def _rows_to_upstream_cost(rows: list[dict], orig_name: str, source_tag: str) -> list[dict]:
        """将 indicator DB 行转为 upstream_costs 格式。"""
        if not rows:
            return []
        r = rows[0]
        value = float(r["value"]) if r.get("value") is not None else None
        price_trend = "平稳"
        if value is not None:
            if value > 5:
                price_trend = "上涨"
            elif value < -5:
                price_trend = "下跌"
        return [{
            "input_name": orig_name,
            "price_trend": price_trend,
            "recent_price": r.get("value_raw") or (f"{value:+.1f}%" if value is not None else ""),
            "yoy_change_pct": value,
            "data_type": r.get("data_type", "actual"),
            "source_snippet": r.get("source_snippet") or r.get("value_raw") or "",
            "_from": source_tag,
            "_from_indicator_db": True,
        }]

    # ── L1: KG 名称桥接 ───────────────────────────────────────────
    l1_mapping = resolve_industry_names(cost_names) if cost_names else {}

    for orig_name in cost_names:
        std_names = l1_mapping.get(orig_name) or []
        candidate_names = list(dict.fromkeys([orig_name] + std_names))
        rows = _query_indicators_batch(candidate_names, "price", period_year)
        if not rows:
            rows = _query_indicators_batch(candidate_names, "price", period_year - 1)
        if rows:
            converted = _rows_to_upstream_cost(rows, orig_name, "kg_bridge" if std_names else "direct")
            if converted:
                all_results.extend(converted)
                covered.add(orig_name)

    # ── L2: KG 产业链展开 ─────────────────────────────────────────
    uncovered_after_l1 = [n for n in cost_names if n not in covered]
    if uncovered_after_l1:
        chain = expand_company_chain(stock_code)
        # 成本相关：cost_elements + supplier_of + supplier_industries
        l2_candidates = list(dict.fromkeys(
            (chain.get("cost_elements") or []) +
            (chain.get("supplier_industries") or []) +
            (chain.get("supplier_of") or [])
        ))

        if l2_candidates:
            rows = _query_indicators_batch(l2_candidates, "price", period_year)
            if not rows:
                rows = _query_indicators_batch(l2_candidates, "price", period_year - 1)
            if rows:
                for orig_name in uncovered_after_l1:
                    converted = _rows_to_upstream_cost(rows, orig_name, "kg_expand")
                    if converted:
                        all_results.extend(converted)
                        covered.add(orig_name)
                        break

    uncovered = [n for n in cost_names if n not in covered]
    if uncovered:
        logger.info(f"[kg_bridge] 上游未覆盖（RAG兜底）: {uncovered}")
    else:
        logger.info(f"[kg_bridge] 上游指标全部命中，覆盖: {list(covered)}")

    return all_results

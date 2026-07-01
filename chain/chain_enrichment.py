"""产业链认知 Baseline — 行业数据自动补充模块

流程:
  Phase 0: LLM 查询规划 — 理解每个 element 的实际含义，生成具体搜索问题列表
  Phase 1: 用问题列表查 industry_indicators 表（精确匹配）
  Phase 2: 未命中的问题查 hybrid_search 知识库（向量+KG）
  Phase 3: 仍未命中的用问财实时查询（限额使用）

核心原则：宁缺毋滥 + LLM 驱动的精确查询
"""
import logging
import time
from datetime import datetime
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.db_utils import execute_cloud_query

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
# 主入口
# ══════════════════════════════════════════════════════════════════

def enrich_baseline_industry_data(
    baseline_json: dict,
    chain_name: str,
    progress_callback=None,
) -> dict:
    """Baseline 生成后调用，为 drivers[] 中每个 element 补充真实行业数据。

    修改 baseline_json in-place 并返回。
    """
    drivers = baseline_json.get("drivers", [])
    structure = baseline_json.get("structure", [])

    # 收集所有需要处理的 element
    tasks = []
    for driver in drivers:
        tier_key = driver.get("tier_key", "")
        tier_label = _find_tier_label(structure, tier_key)
        for el in driver.get("cost_elements", []):
            if isinstance(el, dict):
                tasks.append((el, "cost", tier_key, tier_label))
        for el in driver.get("revenue_elements", []):
            if isinstance(el, dict):
                tasks.append((el, "revenue", tier_key, tier_label))

    total = len(tasks)
    if total == 0:
        return baseline_json

    logger.info(f"[enrich] 开始补充行业数据: chain={chain_name}, elements={total}")

    if progress_callback:
        progress_callback("LLM 规划查询问题...", 10)

    # ─── Phase 0: LLM 查询规划 ─────────────────────────────────────
    # 让 LLM 理解每个 element 的上下文，生成具体搜索问题
    from chain.query_planner import plan_queries_for_baseline
    query_plan = plan_queries_for_baseline(baseline_json, chain_name)

    logger.info(f"[enrich] 查询规划完成: {len(query_plan)} elements, "
               f"total queries={sum(len(v) for v in query_plan.values())}")

    if progress_callback:
        progress_callback(f"查询规划完成, 开始检索...", 25)

    # ─── Phase 1+2+3: 按问题列表逐 element 检索 ───────────────────
    wencai_budget = [5]  # 问财预算
    processed = [0]

    def _process_one(task_tuple):
        el, el_type, tier_key, tier_label = task_tuple
        element_name = el.get("name", "")
        if not element_name:
            return

        llm_original = el.get("industry_data", "") if isinstance(el.get("industry_data"), str) else ""

        # 获取该 element 的查询计划
        queries = query_plan.get(element_name, [])
        if not queries:
            # 无查询计划时，element 保持 LLM_only
            el["industry_data"] = _build_industry_data([], llm_original, "LLM_only")
            processed[0] += 1
            return

        # 按查询计划执行三层检索
        items, coverage = _execute_query_plan(
            queries, chain_name, tier_label, wencai_budget
        )

        # 组装结构化 industry_data
        el["industry_data"] = _build_industry_data(items, llm_original, coverage)

        processed[0] += 1
        if progress_callback:
            pct = 25 + int(70 * processed[0] / total)
            progress_callback(f"补充行业数据 ({processed[0]}/{total})", pct)

    # 使用线程池并发处理 (IO密集)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(_process_one, t) for t in tasks]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                logger.warning(f"[enrich] element 处理异常: {e}")

    logger.info(f"[enrich] 完成: chain={chain_name}, processed={processed[0]}/{total}")
    return baseline_json


# ══════════════════════════════════════════════════════════════════
# 三层检索执行器（按 LLM 规划的问题列表）
# ══════════════════════════════════════════════════════════════════

def _execute_query_plan(
    queries: List[dict],
    chain_name: str,
    tier_label: str,
    wencai_budget: list,
) -> tuple:
    """按查询计划执行三层降级检索。

    对每个 query:
      1. 先查 industry_indicators（精确匹配）
      2. 未命中 → 查 hybrid_search 知识库
      3. 仍未命中且 target=wencai → 问财实时查（受预算限制）

    返回: (items_list, coverage_str)
    """
    all_items = []
    sources_hit = set()
    current_year = datetime.now().year
    min_year = current_year - 2

    industry_keywords = [chain_name]
    if tier_label and tier_label != chain_name:
        industry_keywords.append(tier_label)

    for q in queries:
        query_text = q.get("query", "")
        target = q.get("target", "indicator_db")
        if not query_text:
            continue

        # ── L1: industry_indicators 精确查 ──
        items = _search_indicator_by_query(query_text, industry_keywords, min_year)
        if items:
            all_items.extend(items)
            sources_hit.add("L1")
            if len(all_items) >= 8:
                break
            continue

        # ── L2: hybrid_search 知识库 ──
        items = _search_hybrid_by_query(query_text, chain_name, tier_label)
        if items:
            all_items.extend(items)
            sources_hit.add("L2")
            if len(all_items) >= 8:
                break
            continue

        # ── L3: 问财（仅对 target=wencai 的或所有查询都未命中时）──
        if target == "wencai" and wencai_budget[0] > 0:
            items = _search_wencai_by_query(query_text, chain_name)
            if items:
                all_items.extend(items)
                sources_hit.add("L3")
            wencai_budget[0] -= 1

    # 确定 coverage
    if "L1" in sources_hit:
        coverage = "L1"
    elif "L2" in sources_hit:
        coverage = "L2"
    elif "L3" in sources_hit:
        coverage = "L3"
    elif all_items:
        coverage = "L2"
    else:
        coverage = "LLM_only"

    return all_items[:8], coverage


# ══════════════════════════════════════════════════════════════════
# L1: industry_indicators 查询（带行业约束，宁缺毋滥）
# ══════════════════════════════════════════════════════════════════

def _search_indicator_by_query(
    query_text: str,
    industry_keywords: list,
    min_year: int,
) -> list:
    """用 LLM 规划的具体问题查 industry_indicators。

    策略:
      1. 先带行业约束查（query + chain constraint）
      2. 如果 query 本身足够具体（>=4字），尝试无约束直查
      3. 二次验证：排除明显不相关的结果
    """
    # 策略1: 带行业约束
    items = _query_with_industry_constraint(query_text, industry_keywords, min_year, limit=3)
    if items:
        return items

    # 策略2: query 本身足够具体时（如"螺纹钢价格"），无约束直查
    if len(query_text) >= 4:
        items = _query_direct(query_text, min_year, limit=3)
        if items:
            return items

    return []


def _query_with_industry_constraint(
    metric_keyword: str,
    industry_keywords: list,
    min_year: int,
    limit: int = 3,
) -> list:
    """带行业约束的精确查询。"""
    if not metric_keyword or not industry_keywords:
        return []

    industry_clauses = []
    params = []
    for kw in industry_keywords:
        industry_clauses.append(
            "(industry_l2 LIKE %s OR industry_l1 LIKE %s OR industry_l3 LIKE %s)"
        )
        params.extend([f"%{kw}%", f"%{kw}%", f"%{kw}%"])

    industry_where = " OR ".join(industry_clauses)
    params.append(f"%{metric_keyword}%")
    params.append(min_year)
    params.append(limit)

    sql = f"""SELECT * FROM industry_indicators
        WHERE ({industry_where})
          AND metric_name LIKE %s
          AND period_year >= %s
        ORDER BY publish_date DESC, confidence DESC
        LIMIT %s"""

    try:
        rows = execute_cloud_query(sql, params)
    except Exception as e:
        logger.warning(f"[enrich] L1 约束查询异常: {e}")
        return []

    if not rows:
        return []

    # 二次验证
    validated = [r for r in rows
                 if any(kw in f"{r.get('industry_l1','')} {r.get('industry_l2','')} {r.get('industry_l3','')}"
                        for kw in industry_keywords)]
    return _convert_indicator_rows(validated)[:limit] if validated else []


def _query_direct(
    query_text: str,
    min_year: int,
    limit: int = 3,
) -> list:
    """无行业约束的直接查询（仅用于足够具体的商品/指标名）。

    防护机制: 拒绝抽象通用词，避免跨行业误匹配。
    """
    # 拒绝抽象通用词 — 这些词在任何行业都存在，不应无约束搜索
    _GENERIC_BLOCKLIST = {
        "成本", "费用", "价格", "收入", "利润", "投入", "产量", "增速",
        "原材料", "运输", "人工", "研发", "销售", "采购", "能源",
        "原材料成本", "运输成本", "人工成本", "能源成本",
        "研发费用", "销售费用", "采购成本", "产品销量",
    }
    if query_text in _GENERIC_BLOCKLIST:
        return []

    # 要求查询词足够长且不全是通用词
    if len(query_text) < 4:
        return []

    sql = """SELECT * FROM industry_indicators
        WHERE metric_name LIKE %s
          AND period_year >= %s
        ORDER BY publish_date DESC, confidence DESC
        LIMIT %s"""

    try:
        rows = execute_cloud_query(sql, [f"%{query_text}%", min_year, limit])
    except Exception as e:
        logger.warning(f"[enrich] L1 直查异常: {e}")
        return []

    return _convert_indicator_rows(rows)[:limit] if rows else []


def _convert_indicator_rows(rows: list) -> list:
    """将 industry_indicators 行转为标准 item 格式。"""
    items = []
    for r in rows:
        value_str = ""
        if r.get("value") is not None:
            try:
                v = float(r["value"])
                raw = r.get("value_raw", "")
                value_str = raw if raw else str(v)
            except (ValueError, TypeError):
                value_str = str(r.get("value_raw", ""))

        items.append({
            "metric_name": r.get("metric_name", ""),
            "value": value_str,
            "trend": None,
            "yoy_change": None,
            "period": r.get("period_label", "") or str(r.get("period_year", "")),
            "source": "industry_indicators",
            "source_snippet": (r.get("source_snippet") or "")[:200],
            "confidence": r.get("confidence", "medium"),
        })
    return items


# ══════════════════════════════════════════════════════════════════
# L2: hybrid_search 知识库检索
# ══════════════════════════════════════════════════════════════════

def _search_hybrid_by_query(query_text: str, chain_name: str, tier_label: str) -> list:
    """用 LLM 规划的问题查知识库。"""
    try:
        from retrieval.hybrid import hybrid_search
    except ImportError:
        logger.debug("[enrich] hybrid_search 不可用，跳过 L2")
        return []

    # 构造检索 query: 加入产业链上下文
    search_query = f"{chain_name} {tier_label} {query_text}"
    try:
        hr = hybrid_search(search_query, context={"theme_tags": [chain_name]}, top_k=5)
    except Exception as e:
        logger.warning(f"[enrich] hybrid_search 失败: {e}")
        return []

    items = []
    for chunk in (hr.chunks or [])[:3]:
        text = getattr(chunk, "chunk_text", "") or getattr(chunk, "text", "")
        if len(text) > 30:
            items.append({
                "metric_name": query_text,
                "value": None,
                "trend": None,
                "yoy_change": None,
                "period": "",
                "source": "hybrid_search",
                "source_snippet": text[:300],
                "confidence": "medium",
            })
    return items


# ══════════════════════════════════════════════════════════════════
# L3: 问财实时查询（用 LLM 规划的精确问题）
# ══════════════════════════════════════════════════════════════════

def _search_wencai_by_query(query_text: str, chain_name: str) -> list:
    """用 LLM 规划的问题查问财。"""
    try:
        from ingestion.wencai_indicator_fetcher import query_wencai_with_retry
    except ImportError:
        logger.debug("[enrich] wencai_indicator_fetcher 不可用，跳过 L3")
        return []

    # 直接用 LLM 规划的问题作为问财查询（已经足够具体）
    try:
        articles = query_wencai_with_retry(query_text)
    except Exception as e:
        logger.warning(f"[enrich] wencai 查询失败: {e}")
        return []

    time.sleep(15)  # 反爬间隔

    if not articles:
        return []

    snippets = [a.get("summary", "")[:200] for a in articles[:2] if a.get("summary")]
    if not snippets:
        return []

    return [{
        "metric_name": query_text,
        "value": None,
        "trend": None,
        "yoy_change": None,
        "period": (articles[0].get("date", "") or "")[:7] if articles else "",
        "source": "wencai_realtime",
        "source_snippet": " | ".join(snippets),
        "confidence": "low",
    }]


# ══════════════════════════════════════════════════════════════════
# 辅助函数
# ══════════════════════════════════════════════════════════════════

def _find_tier_label(structure: list, tier_key: str) -> str:
    """从 structure[] 中根据 tier_key 找到 tier_label。"""
    for s in structure:
        if s.get("tier_key") == tier_key:
            return s.get("tier_label", "")
    return ""


def _build_industry_data(items: list, llm_original: str, coverage: str) -> dict:
    """组装结构化 industry_data 对象。"""
    return {
        "items": items[:8],
        "coverage": coverage,
        "enriched_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "llm_summary": llm_original if llm_original else None,
    }

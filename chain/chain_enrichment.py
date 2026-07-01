"""产业链认知 Baseline — 行业数据自动补充模块

Baseline 生成完成后，为每个 cost_element / revenue_element 执行三层降级检索：
  L1: industry_indicators 表 + KG名称桥接
  L2: hybrid_search 向量+KG混合检索
  L3: query_wencai_with_retry 实时问财（限额 5 次/chain）

将检索到的真实行业数据写入 industry_data 字段（结构化对象）。
"""
import logging
import time
from datetime import datetime
from typing import Optional
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

    # L1 批量预热：收集所有 element name 做 KG 名称桥接
    all_names = list(set(t[0].get("name", "") for t in tasks if t[0].get("name")))
    name_mapping = _batch_resolve_names(all_names)

    wencai_budget = [5]  # 用 list 便于闭包内修改
    processed = [0]

    def _process_one(task_tuple):
        el, el_type, tier_key, tier_label = task_tuple
        element_name = el.get("name", "")
        if not element_name:
            return

        llm_original = el.get("industry_data", "") if isinstance(el.get("industry_data"), str) else ""
        metric_type = "cost" if el_type == "cost" else "demand"

        # L1: industry_indicators (必须带 chain_name 约束)
        items = _search_indicator_db(element_name, chain_name, tier_label, metric_type, name_mapping)

        # L2: hybrid_search (如果 L1 少于 2 条)
        if len(items) < 2:
            l2_items = _search_hybrid(element_name, chain_name, tier_label)
            items.extend(l2_items)

        # L3: 问财 (仅当 L1+L2 都没有且预算充足时)
        coverage = "L1" if any(i["source"] == "industry_indicators" for i in items) else (
            "L2" if items else "LLM_only"
        )
        if not items and wencai_budget[0] > 0:
            l3_items = _search_wencai(element_name, chain_name)
            if l3_items:
                items.extend(l3_items)
                coverage = "L3"
            wencai_budget[0] -= 1

        # 组装结构化 industry_data
        el["industry_data"] = _build_industry_data(items, llm_original, coverage)

        processed[0] += 1
        if progress_callback:
            pct = 60 + int(35 * processed[0] / total)
            progress_callback(f"补充行业数据 ({processed[0]}/{total})", pct)

    # 使用线程池并发处理 L1+L2 (IO密集)
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
# L1: industry_indicators 表查询（精确匹配，宁缺毋滥）
# ══════════════════════════════════════════════════════════════════

def _search_indicator_db(
    element_name: str,
    chain_name: str,
    tier_label: str,
    metric_type: str,
    name_mapping: dict,
) -> list:
    """L1 层：从 industry_indicators 表检索匹配指标。

    核心原则：**宁缺毋滥**。必须用产业链名称做强约束，
    绝不返回跨行业的不相关数据。
    """
    current_year = datetime.now().year
    min_year = current_year - 2  # 最近两年数据

    # 构建行业约束关键词：chain_name + tier_label
    # 例如 chain_name="风电", tier_label="风电整机" → 搜索条件包含"风电"
    industry_keywords = [chain_name]
    if tier_label and tier_label != chain_name:
        industry_keywords.append(tier_label)

    # 策略1: chain_name 约束 + metric_name 匹配
    # 查询: industry_l2/industry_l1 匹配产业链 AND metric_name 匹配要素名
    items = _query_with_industry_constraint(
        element_name, industry_keywords, min_year, limit=5
    )
    if items:
        return items

    # 策略2: KG 桥接名 + chain 约束
    std_names = name_mapping.get(element_name, [])
    for sn in std_names[:2]:
        items = _query_with_industry_constraint(
            sn, industry_keywords, min_year, limit=5
        )
        if items:
            return items

    # 策略3: chain 约束下用 tier_label + element_name 组合搜 metric_name
    if tier_label:
        combined = f"{tier_label}{element_name}"
        items = _query_with_industry_constraint(
            combined, industry_keywords, min_year, limit=5
        )
        if items:
            return items

    # 宁缺毋滥：都不命中就返回空，不做无约束搜索
    return []


def _query_with_industry_constraint(
    metric_keyword: str,
    industry_keywords: list,
    min_year: int,
    limit: int = 5,
) -> list:
    """带行业约束的精确查询。

    SQL 逻辑:
      WHERE (industry_l2 LIKE %chain% OR industry_l1 LIKE %chain%)
        AND (metric_name LIKE %element%)
        AND period_year >= min_year
    """
    if not metric_keyword or not industry_keywords:
        return []

    # 构建行业约束条件（OR 多个关键词）
    industry_clauses = []
    params = []
    for kw in industry_keywords:
        industry_clauses.append("(industry_l2 LIKE %s OR industry_l1 LIKE %s OR industry_l3 LIKE %s)")
        params.extend([f"%{kw}%", f"%{kw}%", f"%{kw}%"])

    industry_where = " OR ".join(industry_clauses)

    # metric_name 匹配
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
        logger.warning(f"[enrich] L1 查询异常: {e}")
        return []

    if not rows:
        return []

    # 二次验证：确认返回数据的 industry 与查询意图相关
    validated = []
    for r in rows:
        ind_context = f"{r.get('industry_l1', '')} {r.get('industry_l2', '')} {r.get('industry_l3', '')}"
        # 至少有一个行业关键词出现在结果的行业字段中
        if any(kw in ind_context for kw in industry_keywords):
            validated.append(r)

    if not validated:
        return []

    return _convert_indicator_rows(validated)[:limit]


def _convert_indicator_rows(rows: list) -> list:
    """将 industry_indicators 行转为标准 item 格式。"""
    items = []
    for r in rows:
        value_str = ""
        if r.get("value") is not None:
            v = float(r["value"])
            raw = r.get("value_raw", "")
            value_str = raw if raw else str(v)

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
# L2: hybrid_search 混合检索
# ══════════════════════════════════════════════════════════════════

def _search_hybrid(element_name: str, chain_name: str, tier_label: str) -> list:
    """L2 层：从知识库检索相关文本 chunk。"""
    try:
        from retrieval.hybrid import hybrid_search
    except ImportError:
        logger.debug("[enrich] hybrid_search 不可用，跳过 L2")
        return []

    query = f"{chain_name} {tier_label} {element_name} 行业数据 价格 增速"
    try:
        hr = hybrid_search(query, context={"theme_tags": [chain_name]}, top_k=5)
    except Exception as e:
        logger.warning(f"[enrich] hybrid_search 失败: {e}")
        return []

    items = []
    for chunk in (hr.chunks or [])[:3]:
        text = getattr(chunk, "chunk_text", "") or getattr(chunk, "text", "")
        if len(text) > 30:
            items.append({
                "metric_name": element_name,
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
# L3: 问财实时查询（受限使用）
# ══════════════════════════════════════════════════════════════════

def _search_wencai(element_name: str, chain_name: str) -> list:
    """L3 层：实时查问财（每次间隔 15s）。"""
    try:
        from ingestion.wencai_indicator_fetcher import query_wencai_with_retry
    except ImportError:
        logger.debug("[enrich] wencai_indicator_fetcher 不可用，跳过 L3")
        return []

    question = f"{chain_name} {element_name} 最新市场数据 价格 产量 增速 2025年"
    try:
        articles = query_wencai_with_retry(question)
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
        "metric_name": element_name,
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


def _batch_resolve_names(names: list) -> dict:
    """批量 KG 名称桥接：将模糊名映射到标准实体名。"""
    try:
        from research.kg_indicator_bridge import resolve_industry_names
        return resolve_industry_names(names)
    except Exception as e:
        logger.debug(f"[enrich] KG 名称桥接不可用: {e}")
        return {n: [] for n in names}


def _build_industry_data(items: list, llm_original: str, coverage: str) -> dict:
    """组装结构化 industry_data 对象。"""
    return {
        "items": items[:8],  # 最多保留8条
        "coverage": coverage,
        "enriched_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "llm_summary": llm_original if llm_original else None,
    }

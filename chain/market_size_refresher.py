"""产业链环节市场规模数据刷新器

流程（与个股研究同思路）:
  L1: 查 industry_indicators 表 (metric_type=market_size)
  L2: hybrid_search 知识库混合检索
  L3: 问财外部渠道实时查询 (zhishi 模式)

最后由 LLM 从检索结果中提取结构化的市场规模数值。
"""
import json
import logging
import time
from datetime import datetime
from typing import Optional, Callable

from utils.db_utils import execute_cloud_query, execute_cloud_insert

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
# 主入口
# ══════════════════════════════════════════════════════════════════

def refresh_market_size(
    chain_name: str,
    progress_callback: Optional[Callable] = None,
) -> dict:
    """刷新产业链所有环节的市场规模数据。

    三层降级: industry_indicators → hybrid_search → 问财
    LLM 提取后写回 chain_baseline。

    Returns:
        {"updated": int, "skipped": int, "errors": int, "details": [...]}
    """
    # 获取当前 baseline
    row = execute_cloud_query(
        "SELECT version, baseline_json FROM chain_baseline "
        "WHERE chain_name=%s ORDER BY version DESC LIMIT 1",
        [chain_name],
    )
    if not row:
        return {"updated": 0, "skipped": 0, "errors": 1,
                "details": [{"error": "No baseline found"}]}

    version = row[0]["version"]
    bl = json.loads(row[0]["baseline_json"]) if isinstance(row[0]["baseline_json"], str) else row[0]["baseline_json"]
    structure = bl.get("structure", [])

    # 收集所有环节
    segments = []
    for tier in structure:
        tier_key = tier.get("tier_key", "")
        tier_label = tier.get("tier_label", tier_key)
        for seg in tier.get("key_segments", []):
            if isinstance(seg, str):
                segments.append({"name": seg, "tier_key": tier_key, "tier_label": tier_label, "_ref": None})
            elif isinstance(seg, dict):
                segments.append({"name": seg.get("name", ""), "tier_key": tier_key,
                                 "tier_label": tier_label, "_ref": seg})

    total = len(segments)
    if total == 0:
        return {"updated": 0, "skipped": 0, "errors": 0, "details": []}

    if progress_callback:
        progress_callback(f"开始刷新 {total} 个环节...", 5)

    updated = 0
    skipped = 0
    errors = 0
    details = []
    current_year = datetime.now().year

    for i, seg_info in enumerate(segments):
        seg_name = seg_info["name"]
        if not seg_name:
            skipped += 1
            continue

        try:
            result = _refresh_one_segment(
                seg_name=seg_name,
                chain_name=chain_name,
                tier_label=seg_info["tier_label"],
                current_year=current_year,
            )

            if result and result.get("market_size_billion"):
                # 写回到 baseline structure
                _update_segment_in_structure(
                    structure, seg_info["tier_key"], seg_name, result
                )
                updated += 1
                details.append({"segment": seg_name, "status": "updated", **result})
            else:
                skipped += 1
                details.append({"segment": seg_name, "status": "no_data"})

        except Exception as e:
            logger.warning(f"[market_size] 刷新失败 [{seg_name}]: {e}")
            errors += 1
            details.append({"segment": seg_name, "status": "error", "error": str(e)})

        if progress_callback:
            pct = 5 + int(90 * (i + 1) / total)
            progress_callback(f"刷新中 ({i+1}/{total}): {seg_name}", pct)

    # 写回数据库
    if updated > 0:
        # 重新计算各 tier 的 tier_market_size_billion
        for tier in structure:
            segs = tier.get("key_segments", [])
            total_size = sum(s.get("market_size_billion") or 0 for s in segs if isinstance(s, dict))
            tier["tier_market_size_billion"] = total_size if total_size > 0 else None

        bl["structure"] = structure
        new_json = json.dumps(bl, ensure_ascii=False)
        execute_cloud_insert(
            "UPDATE chain_baseline SET baseline_json=%s WHERE chain_name=%s AND version=%s",
            [new_json, chain_name, version],
        )

    if progress_callback:
        progress_callback(f"完成: 更新{updated}个, 跳过{skipped}个", 100)

    return {"updated": updated, "skipped": skipped, "errors": errors, "details": details}


# ══════════════════════════════════════════════════════════════════
# 单环节三层降级刷新
# ══════════════════════════════════════════════════════════════════

def _refresh_one_segment(
    seg_name: str,
    chain_name: str,
    tier_label: str,
    current_year: int,
) -> Optional[dict]:
    """对单个环节执行三层降级检索 + LLM 提取市场规模。

    Returns:
        {"market_size_billion": float, "data_year": int, "data_source": str}
        or None if no data found.
    """
    industry_keywords = [chain_name, tier_label, seg_name]
    min_year = current_year - 2
    all_evidence = []

    # ── L1: industry_indicators 精确查 ──────────────────────────
    l1_items = _query_indicator_market_size(seg_name, industry_keywords, min_year)
    if l1_items:
        all_evidence.extend(l1_items)
        logger.info(f"[market_size] L1命中 [{seg_name}]: {len(l1_items)}条")

    # ── L2: hybrid_search 知识库 ────────────────────────────────
    if not all_evidence:
        l2_items = _search_hybrid_market_size(seg_name, chain_name, tier_label)
        if l2_items:
            all_evidence.extend(l2_items)
            logger.info(f"[market_size] L2命中 [{seg_name}]: {len(l2_items)}条")

    # ── L3: 问财外部查询 ───────────────────────────────────────
    if not all_evidence:
        l3_items = _search_wencai_market_size(seg_name, chain_name, current_year)
        if l3_items:
            all_evidence.extend(l3_items)
            logger.info(f"[market_size] L3命中 [{seg_name}]: {len(l3_items)}条")

    if not all_evidence:
        logger.info(f"[market_size] 三层均未命中 [{seg_name}]")
        return None

    # ── LLM 结构化提取市场规模数值 ─────────────────────────────
    result = _llm_extract_market_size(seg_name, chain_name, all_evidence)
    return result


# ══════════════════════════════════════════════════════════════════
# L1: industry_indicators 查询
# ══════════════════════════════════════════════════════════════════

def _query_indicator_market_size(
    seg_name: str,
    industry_keywords: list,
    min_year: int,
) -> list:
    """查 industry_indicators 中 metric_type=market_size 的数据。"""
    # 策略1: 用环节名精确查
    rows = execute_cloud_query(
        """SELECT metric_name, value, value_raw, period_year, period_label,
                  source_snippet, confidence
           FROM industry_indicators
           WHERE metric_type = 'market_size'
             AND (metric_name LIKE %s OR industry_l3 LIKE %s OR industry_l2 LIKE %s)
             AND period_year >= %s
           ORDER BY period_year DESC, confidence DESC
           LIMIT 5""",
        [f"%{seg_name}%", f"%{seg_name}%", f"%{seg_name}%", min_year],
    ) or []

    if not rows:
        # 策略2: 用行业关键词约束
        for kw in industry_keywords:
            if not kw or kw == seg_name:
                continue
            rows = execute_cloud_query(
                """SELECT metric_name, value, value_raw, period_year, period_label,
                          source_snippet, confidence
                   FROM industry_indicators
                   WHERE metric_type = 'market_size'
                     AND (industry_l2 LIKE %s OR industry_l3 LIKE %s)
                     AND metric_name LIKE %s
                     AND period_year >= %s
                   ORDER BY period_year DESC, confidence DESC
                   LIMIT 3""",
                [f"%{kw}%", f"%{kw}%", f"%{seg_name}%", min_year],
            ) or []
            if rows:
                break

    return [
        {
            "source": "industry_indicators",
            "metric_name": r.get("metric_name", ""),
            "value": r.get("value"),
            "value_raw": r.get("value_raw", ""),
            "period_year": r.get("period_year"),
            "period_label": r.get("period_label", ""),
            "snippet": r.get("source_snippet", ""),
        }
        for r in rows
    ]


# ══════════════════════════════════════════════════════════════════
# L2: hybrid_search 知识库
# ══════════════════════════════════════════════════════════════════

def _search_hybrid_market_size(seg_name: str, chain_name: str, tier_label: str) -> list:
    """用混合检索查知识库中的市场规模数据。"""
    try:
        from retrieval.hybrid import hybrid_search
    except ImportError:
        logger.debug("[market_size] hybrid_search 不可用，跳过 L2")
        return []

    query = f"{seg_name} 中国市场规模 行业规模 产值 收入"
    try:
        hr = hybrid_search(query, context={"theme_tags": [chain_name]}, top_k=5)
    except Exception as e:
        logger.warning(f"[market_size] hybrid_search 失败: {e}")
        return []

    items = []
    for chunk in (hr.chunks or [])[:5]:
        text = getattr(chunk, "text", "") or getattr(chunk, "chunk_text", "")
        if len(text) > 30:
            items.append({
                "source": "hybrid_search",
                "snippet": text[:400],
            })
    return items


# ══════════════════════════════════════════════════════════════════
# L3: 问财外部查询
# ══════════════════════════════════════════════════════════════════

def _search_wencai_market_size(seg_name: str, chain_name: str, year: int) -> list:
    """用问财 zhishi 模式查询市场规模。"""
    try:
        from ingestion.wencai_indicator_fetcher import query_wencai_with_retry
    except ImportError:
        logger.debug("[market_size] wencai 不可用，跳过 L3")
        return []

    query = f"{year}年中国{seg_name}市场规模"
    try:
        articles = query_wencai_with_retry(query)
    except Exception as e:
        logger.warning(f"[market_size] wencai 查询失败: {e}")
        return []

    time.sleep(10)  # 反爬间隔

    if not articles:
        return []

    items = []
    for a in articles[:3]:
        summary = a.get("summary", "")
        if summary:
            items.append({
                "source": "wencai",
                "snippet": summary[:400],
                "date": a.get("date", ""),
            })
    return items


# ══════════════════════════════════════════════════════════════════
# LLM 结构化提取
# ══════════════════════════════════════════════════════════════════

_EXTRACT_PROMPT = """\
你是行业市场规模数据提取专家。从以下检索结果中提取指定环节的**中国市场规模**（不是全球，不是股票市值）。

规则：
1. 提取的是行业市场规模/产值/收入（TAM），单位为亿元人民币
2. 不要用概念板块总市值替代行业规模
3. 优先取最近年份的实际数据，其次取预测数据
4. 如有多个来源数值差异大，取可信度最高的（研报/行业协会 > 新闻 > 估算）
5. 如确实无法提取到市场规模数据，返回 null

输出JSON（不要markdown代码块）：
{
  "market_size_billion": 数值或null,
  "data_year": 年份整数或null,
  "data_source": "数据来源描述（≤40字）或null",
  "confidence": "high/medium/low"
}"""


def _llm_extract_market_size(
    seg_name: str, chain_name: str, evidence: list
) -> Optional[dict]:
    """调 LLM 从检索证据中提取市场规模数值。"""
    from utils.model_router import call_model_json

    # 构建证据文本
    evidence_parts = []
    for ev in evidence[:8]:
        src = ev.get("source", "")
        if ev.get("value") is not None:
            evidence_parts.append(
                f"[{src}] {ev.get('metric_name','')}: "
                f"{ev.get('value_raw') or ev.get('value')} "
                f"({ev.get('period_label') or ev.get('period_year','')})"
            )
        elif ev.get("snippet"):
            evidence_parts.append(f"[{src}] {ev['snippet']}")

    if not evidence_parts:
        return None

    evidence_text = "\n---\n".join(evidence_parts)
    user_msg = (
        f"产业链: {chain_name}\n"
        f"环节: {seg_name}\n\n"
        f"检索到的证据:\n{evidence_text}"
    )

    try:
        result = call_model_json("cleaning", _EXTRACT_PROMPT, user_msg,
                                  max_tokens=512, timeout=30)
        if not isinstance(result, dict):
            return None

        size = result.get("market_size_billion")
        if size is None:
            return None

        return {
            "market_size_billion": float(size),
            "data_year": result.get("data_year"),
            "data_source": result.get("data_source"),
        }
    except Exception as e:
        logger.warning(f"[market_size] LLM提取失败 [{seg_name}]: {e}")
        return None


# ══════════════════════════════════════════════════════════════════
# 辅助
# ══════════════════════════════════════════════════════════════════

def _update_segment_in_structure(structure: list, tier_key: str, seg_name: str, result: dict):
    """将刷新结果写回 structure 中对应的 segment。"""
    for tier in structure:
        if tier.get("tier_key") != tier_key:
            continue
        for seg in tier.get("key_segments", []):
            if isinstance(seg, dict) and seg.get("name") == seg_name:
                seg["market_size_billion"] = result["market_size_billion"]
                seg["_data_year"] = result.get("data_year")
                seg["_data_source"] = result.get("data_source")
                return

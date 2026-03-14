"""财务指标真实数据提取器 V2

从 Step1/Step2 已验证结论 + RAG 检索中提取量价数据，
结构化后注入 Step3 prompt，让 LLM 基于真实数据推理而非编造。
"""
import logging
import threading
from datetime import datetime
from typing import Optional

from utils.db_utils import execute_query

logger = logging.getLogger(__name__)

# ── 向量模型懒加载（复用 rag_context.py 模式）────────────────
_embed_model = None
_embed_model_lock = threading.Lock()


def _get_embed_model():
    global _embed_model
    if _embed_model is None:
        with _embed_model_lock:
            if _embed_model is None:
                try:
                    from sentence_transformers import SentenceTransformer
                    _embed_model = SentenceTransformer("BAAI/bge-base-zh-v1.5")
                    logger.info("indicator_data_fetcher 向量模型加载完成")
                except Exception as e:
                    logger.warning(f"向量模型加载失败: {e}")
    return _embed_model


# ── 1a. 从 Step1/Step2 提取已验证数据点 ──────────────────────

def _extract_verified_from_steps(step1_result: dict, step2_result: dict) -> list[dict]:
    """从 Step1/Step2 已有结论中提取有数据支撑的指标"""
    verified = []

    # Step1: topline_indicators / bottomline_indicators
    for side, key in [("topline", "topline_indicators"), ("bottomline", "bottomline_indicators")]:
        for ind in (step1_result.get(key) or []):
            status = ind.get("current_status")
            if not status:
                continue
            verified.append({
                "indicator": ind.get("name", ""),
                "value": status,
                "source": "Step1商业模式",
                "side": side,
                "confidence": "verified",
            })

    # Step2: segment_positions[].notes
    for seg in (step2_result.get("segment_positions") or []):
        for note in (seg.get("notes") or []):
            if not isinstance(note, dict):
                continue
            for data_key in ("market_data", "industry_data"):
                data_val = note.get(data_key)
                if data_val:
                    verified.append({
                        "indicator": note.get("topic", data_key),
                        "value": data_val if isinstance(data_val, str) else str(data_val),
                        "source": "Step2产业链",
                        "side": "topline",
                        "confidence": "verified",
                    })

    return verified


# ── 1b. 带时间衰减的 RAG 搜索 ───────────────────────────────

def _search_with_time_decay(stock_code: str, query: str, top_k: int = 8) -> list[dict]:
    """检索股票相关文本，按相似度 × 时间衰减排序。

    Returns: [{"text": str, "created_at": str, "score": float}]
    """
    # 从 content_summaries 拉文本 + created_at
    cs_rows = execute_query(
        """SELECT cs.summary, cs.created_at
           FROM content_summaries cs
           JOIN stock_mentions sm ON cs.extracted_text_id = sm.extracted_text_id
           WHERE sm.stock_code = %s
             AND cs.created_at >= DATE_SUB(NOW(), INTERVAL 120 DAY)
             AND cs.summary IS NOT NULL AND cs.summary != ''
           ORDER BY cs.created_at DESC
           LIMIT 100""",
        [stock_code],
    ) or []

    # cleaned_items 补充
    ci_rows = execute_query(
        """SELECT ci.summary, ci.created_at
           FROM cleaned_items ci
           JOIN item_companies ic ON ci.id = ic.cleaned_item_id
           WHERE ic.stock_code = %s
             AND ci.created_at >= DATE_SUB(NOW(), INTERVAL 180 DAY)
             AND ci.summary IS NOT NULL AND ci.summary != ''
           ORDER BY ci.created_at DESC
           LIMIT 100""",
        [stock_code],
    ) or []

    items = []
    for r in cs_rows:
        if r.get("summary"):
            items.append({"text": r["summary"], "created_at": r.get("created_at")})
    for r in ci_rows:
        if r.get("summary"):
            items.append({"text": r["summary"], "created_at": r.get("created_at")})

    if not items:
        return []

    texts = [it["text"] for it in items]
    now = datetime.now()

    model = _get_embed_model()
    if model is None:
        # 退化到关键词匹配 + 时间衰减
        keywords = query.split()
        scored = []
        for it in items:
            kw_score = sum(1 for kw in keywords if kw in it["text"]) / max(len(keywords), 1)
            days_ago = (now - it["created_at"]).days if it.get("created_at") else 90
            time_factor = max(0.3, 1 - days_ago / 180)
            scored.append({**it, "score": kw_score * time_factor, "created_at": str(it.get("created_at", ""))})
        scored.sort(key=lambda x: x["score"], reverse=True)
        return [s for s in scored[:top_k] if s["score"] > 0]

    try:
        import numpy as np
        from sklearn.metrics.pairwise import cosine_similarity

        q_vec = model.encode([query], normalize_embeddings=True)
        t_vecs = model.encode(texts, normalize_embeddings=True, batch_size=32, show_progress_bar=False)
        sims = cosine_similarity(q_vec, t_vecs)[0]

        scored = []
        for i, it in enumerate(items):
            days_ago = (now - it["created_at"]).days if it.get("created_at") else 90
            time_factor = max(0.3, 1 - days_ago / 180)
            final_score = float(sims[i]) * time_factor
            scored.append({
                "text": it["text"],
                "created_at": str(it.get("created_at", "")),
                "score": final_score,
            })

        scored.sort(key=lambda x: x["score"], reverse=True)
        return [s for s in scored[:top_k] if s["score"] > 0.05]

    except Exception as e:
        logger.warning(f"向量检索失败: {e}")
        return []


# ── 1c. 结构化提取 ──────────────────────────────────────────

_TOPLINE_EXTRACT_PROMPT = """\
你是财务数据提取专家。从以下研报/新闻摘要中提取与指定指标相关的量化数据。

要求：
1. 只提取有明确数据来源的事实，不推测
2. 同一指标多时间点取最新的 latest_value，新旧差异>20%需在 note 中注明
3. 区分 actual（已披露）和 estimated（券商/行业估算）

输出 JSON：
{
    "indicator_data": [
        {"name": "指标名", "latest_value": "最新值", "period": "时间", "source_type": "actual/estimated", "note": "备注或null"}
    ],
    "demand_context": "下游需求环境概述（100字以内，无数据则null）",
    "company_supply": "公司供给端概述（产能/份额/竞争，100字以内，无数据则null）"
}"""

_BOTTOMLINE_EXTRACT_PROMPT = """\
你是财务数据提取专家。从以下研报/新闻摘要中提取与指定成本/利润指标相关的量化数据。

要求：
1. 只提取有明确数据来源的事实，不推测
2. 同一指标多时间点取最新的 latest_value，新旧差异>20%需在 note 中注明
3. 区分 actual（已披露）和 estimated（券商/行业估算）

输出 JSON：
{
    "indicator_data": [
        {"name": "指标名", "latest_value": "最新值", "period": "时间", "source_type": "actual/estimated", "note": "备注或null"}
    ],
    "cost_structure": "成本结构及变动趋势概述（100字以内，无数据则null）"
}"""


def _extract_structured(raw_texts: list[dict], indicator_names: list[str],
                         side: str, stock_name: str) -> dict:
    """调 DeepSeek 从 RAG 文本中做结构化提取"""
    if not raw_texts:
        return {}

    from utils.model_router import call_model_json

    text_block = "\n---\n".join(t["text"][:600] for t in raw_texts[:12])
    ind_list = "、".join(indicator_names[:12]) if indicator_names else "（未指定）"

    system_prompt = _TOPLINE_EXTRACT_PROMPT if side == "topline" else _BOTTOMLINE_EXTRACT_PROMPT
    user_message = (f"股票：{stock_name}\n"
                    f"需提取的指标：{ind_list}\n\n"
                    f"以下为RAG检索到的相关信息：\n{text_block}")

    try:
        result = call_model_json("cleaning", system_prompt, user_message,
                                  max_tokens=2048, timeout=60)
        return result if isinstance(result, dict) else {}
    except Exception as e:
        logger.warning(f"结构化提取失败({side}): {e}")
        return {}


# ── 1d. 格式化注入文本 ──────────────────────────────────────

def _format_for_injection(verified_data: list[dict],
                           topline_rag: dict,
                           bottomline_rag: dict) -> str:
    """拼成可直接注入 dp_input 的文本"""
    parts = []

    # 已验证数据
    if verified_data:
        parts.append("=== 已验证数据（来自前序分析步骤，最高优先级）===")
        for vd in verified_data:
            parts.append(f"  [{vd['side']}] {vd['indicator']}: {vd['value']} （{vd['source']}，{vd['confidence']}）")

    # Topline RAG
    tl_items = topline_rag.get("indicator_data") or []
    tl_demand = topline_rag.get("demand_context")
    tl_supply = topline_rag.get("company_supply")
    if tl_items or tl_demand or tl_supply:
        parts.append("\n=== RAG检索真实数据-Topline（第二优先级）===")
        for it in tl_items:
            tag = "实际" if it.get("source_type") == "actual" else "估算"
            note = f" ※{it['note']}" if it.get("note") else ""
            parts.append(f"  {it['name']}: {it.get('latest_value','—')} ({it.get('period','')}, {tag}){note}")
        if tl_demand:
            parts.append(f"  [需求环境] {tl_demand}")
        if tl_supply:
            parts.append(f"  [供给端] {tl_supply}")

    # Bottomline RAG
    bl_items = bottomline_rag.get("indicator_data") or []
    bl_cost = bottomline_rag.get("cost_structure")
    if bl_items or bl_cost:
        parts.append("\n=== RAG检索真实数据-Bottomline（第二优先级）===")
        for it in bl_items:
            tag = "实际" if it.get("source_type") == "actual" else "估算"
            note = f" ※{it['note']}" if it.get("note") else ""
            parts.append(f"  {it['name']}: {it.get('latest_value','—')} ({it.get('period','')}, {tag}){note}")
        if bl_cost:
            parts.append(f"  [成本结构] {bl_cost}")

    return "\n".join(parts)


# ── 1e. 主入口 ──────────────────────────────────────────────

def fetch_indicator_data(stock_code: str, stock_name: str, industry_l1: str,
                          topline_indicators: list[dict],
                          bottomline_indicators: list[dict],
                          step1_result: dict, step2_result: dict) -> dict:
    """主入口：编排 已验证提取 → RAG多维搜索 → 结构化提取 → 格式化

    Returns:
        {"verified_data": [...], "topline_rag": {...}, "bottomline_rag": {...}, "injection_text": str}
    """
    # 1. 提取已验证数据
    verified = _extract_verified_from_steps(step1_result or {}, step2_result or {})
    logger.info(f"[fetcher] 已验证数据点: {len(verified)}")

    # 2. 构建查询维度
    tl_names = [i.get("name", "") for i in (topline_indicators or []) if i.get("name")]
    bl_names = [i.get("name", "") for i in (bottomline_indicators or []) if i.get("name")]

    industry = industry_l1 or ""

    # Topline 三维度查询
    tl_queries = [
        f"{stock_name} {industry} {' '.join(tl_names[:4])}",
        f"{stock_name} {industry} 下游需求 市场规模 增长率",
        f"{stock_name} 产能 产量 市场份额 竞争优势",
    ]

    # Bottomline 三维度查询
    bl_queries = [
        f"{stock_name} {industry} {' '.join(bl_names[:4])}",
        f"{stock_name} 原材料成本 能源成本 人工成本",
        f"{stock_name} 毛利率 费用率 盈利能力",
    ]

    # 3. RAG 搜索（各维度 top_k=8，去重合并）
    tl_texts = _multi_dim_search(stock_code, tl_queries, top_k=8)
    bl_texts = _multi_dim_search(stock_code, bl_queries, top_k=8)

    logger.info(f"[fetcher] RAG topline {len(tl_texts)}条, bottomline {len(bl_texts)}条")

    # 4. 结构化提取
    topline_rag = _extract_structured(tl_texts, tl_names, "topline", stock_name) if tl_texts else {}
    bottomline_rag = _extract_structured(bl_texts, bl_names, "bottomline", stock_name) if bl_texts else {}

    # 5. 格式化
    injection_text = _format_for_injection(verified, topline_rag, bottomline_rag)

    return {
        "verified_data": verified,
        "topline_rag": topline_rag,
        "bottomline_rag": bottomline_rag,
        "injection_text": injection_text,
    }


def _multi_dim_search(stock_code: str, queries: list[str], top_k: int = 8) -> list[dict]:
    """多维度搜索，去重合并"""
    seen: set[str] = set()
    merged: list[dict] = []

    for query in queries:
        results = _search_with_time_decay(stock_code, query, top_k=top_k)
        for r in results:
            key = r["text"][:80]
            if key not in seen:
                seen.add(key)
                merged.append(r)

    # 按 score 降序，取前 top_k * 2（跨维度合并后适当放宽）
    merged.sort(key=lambda x: x["score"], reverse=True)
    return merged[:top_k * 2]

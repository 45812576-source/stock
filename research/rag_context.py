"""个股研究专用 RAG 检索封装

优先尝试 retrieval.hybrid.hybrid_search()（需要 Milvus），
降级到 sentence_transformers + cosine_similarity（从 MySQL 拉文本）。
"""
import logging
import threading
from typing import Optional

from utils.db_utils import execute_query

logger = logging.getLogger(__name__)

# ── 向量模型懒加载（复用 project_chat.py 模式）────────────────
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
                    logger.info("RAG向量模型 bge-base-zh-v1.5 加载完成")
                except Exception as e:
                    logger.warning(f"RAG向量模型加载失败: {e}")
    return _embed_model


# ── 内部：sentence_transformers 降级检索 ──────────────────────

def _fallback_search(texts: list[str], query: str, top_k: int) -> list[str]:
    """用 sentence_transformers + cosine_similarity 从 texts 中取 top_k"""
    if not texts:
        return []
    model = _get_embed_model()
    if model is None:
        # 模型不可用，退化为关键词匹配
        keywords = query.split()
        scored = []
        for t in texts:
            score = sum(1 for kw in keywords if kw in t)
            scored.append((score, t))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [t for _, t in scored[:top_k] if _]

    try:
        import numpy as np
        from sklearn.metrics.pairwise import cosine_similarity

        q_vec = model.encode([query], normalize_embeddings=True)
        t_vecs = model.encode(texts, normalize_embeddings=True, batch_size=32, show_progress_bar=False)
        sims = cosine_similarity(q_vec, t_vecs)[0]
        top_idx = np.argsort(sims)[::-1][:top_k]
        return [texts[i] for i in top_idx if sims[i] > 0.1]
    except Exception as e:
        logger.warning(f"向量检索失败，退化关键词: {e}")
        keywords = query.split()
        scored = [(sum(1 for kw in keywords if kw in t), t) for t in texts]
        scored.sort(key=lambda x: x[0], reverse=True)
        return [t for s, t in scored[:top_k] if s]


def _fetch_stock_texts(stock_code: str, days: int = 90) -> list[str]:
    """从 content_summaries + cleaned_items 拉该股票相关文本"""
    texts = []

    # content_summaries（通过 stock_mentions 关联，90天）
    cs_rows = execute_query(
        """SELECT cs.summary
           FROM content_summaries cs
           JOIN stock_mentions sm ON cs.extracted_text_id = sm.extracted_text_id
           WHERE sm.stock_code = %s
             AND cs.created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
             AND cs.summary IS NOT NULL AND cs.summary != ''
           ORDER BY cs.created_at DESC
           LIMIT 100""",
        [stock_code, days],
    ) or []
    texts += [r["summary"] for r in cs_rows if r.get("summary")]

    # cleaned_items（180天）
    ci_rows = execute_query(
        """SELECT ci.summary
           FROM cleaned_items ci
           JOIN item_companies ic ON ci.id = ic.item_id
           WHERE ic.stock_code = %s
             AND ci.created_at >= DATE_SUB(NOW(), INTERVAL 180 DAY)
             AND ci.summary IS NOT NULL AND ci.summary != ''
           ORDER BY ci.created_at DESC
           LIMIT 100""",
        [stock_code],
    ) or []
    texts += [r["summary"] for r in ci_rows if r.get("summary")]

    return texts


def _fetch_industry_texts(industry: str, days: int = 90) -> list[str]:
    """从 content_summaries + cleaned_items 拉行业相关文本"""
    texts = []

    # content_summaries 通过 industry 字段（若有）
    cs_rows = execute_query(
        """SELECT cs.summary
           FROM content_summaries cs
           WHERE cs.summary LIKE %s
             AND cs.created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
             AND cs.summary IS NOT NULL
           ORDER BY cs.created_at DESC
           LIMIT 80""",
        [f"%{industry}%", days],
    ) or []
    texts += [r["summary"] for r in cs_rows if r.get("summary")]

    # cleaned_items 通过 item_industries
    ci_rows = execute_query(
        """SELECT ci.summary
           FROM cleaned_items ci
           JOIN item_industries ii ON ci.id = ii.item_id
           WHERE ii.industry_name LIKE %s
             AND ci.created_at >= DATE_SUB(NOW(), INTERVAL 180 DAY)
             AND ci.summary IS NOT NULL
           ORDER BY ci.created_at DESC
           LIMIT 80""",
        [f"%{industry}%"],
    ) or []
    texts += [r["summary"] for r in ci_rows if r.get("summary")]

    return texts


def _truncate_context(texts: list[str], max_chars: int = 2000) -> str:
    """将文本列表拼接并截断至 max_chars"""
    result = []
    total = 0
    for t in texts:
        t = t.strip()
        if not t:
            continue
        if total + len(t) > max_chars:
            remaining = max_chars - total
            if remaining > 50:
                result.append(t[:remaining] + "…")
            break
        result.append(t)
        total += len(t)
    return "\n---\n".join(result)


# ── 公开接口 ─────────────────────────────────────────────────

def search_stock_context(stock_code: str, query: str, top_k: int = 8) -> str:
    """检索个股相关信息，返回拼接上下文（限2000字）

    优先 hybrid_search()，降级到 sentence_transformers。
    """
    # 优先尝试 Milvus hybrid_search
    try:
        from retrieval.hybrid import hybrid_search
        result = hybrid_search(query, context={"stock_codes": [stock_code]}, top_k=top_k)
        if result and result.merged_context and len(result.merged_context) > 100:
            logger.info(f"RAG hybrid_search 命中: {stock_code} ({len(result.merged_context)}字)")
            return result.merged_context[:2000]
    except Exception as e:
        logger.debug(f"hybrid_search 不可用，降级: {e}")

    # 降级：sentence_transformers
    texts = _fetch_stock_texts(stock_code)
    if not texts:
        return ""
    top_texts = _fallback_search(texts, query, top_k)
    ctx = _truncate_context(top_texts)
    if ctx:
        logger.info(f"RAG fallback 命中: {stock_code} {len(top_texts)}条结果")
    return ctx


def search_industry_context(industry: str, query: str, top_k: int = 8) -> str:
    """检索行业相关信息，返回拼接上下文（限2000字）"""
    try:
        from retrieval.hybrid import hybrid_search
        result = hybrid_search(query, top_k=top_k)
        if result and result.merged_context and len(result.merged_context) > 100:
            return result.merged_context[:2000]
    except Exception as e:
        logger.debug(f"hybrid_search 不可用，降级: {e}")

    texts = _fetch_industry_texts(industry)
    if not texts:
        return ""
    top_texts = _fallback_search(texts, query, top_k)
    return _truncate_context(top_texts)


def search_stock_multi_topic(stock_code: str, stock_name: str, industry: str = "", top_k_per_topic: int = 5) -> str:
    """多主题RAG检索，覆盖产业链/价格成本/产能竞争/政策四个维度，去重后合并（限4000字）"""
    topics = [
        f"{stock_name} {industry} 产业链 上下游 供应商 客户",
        f"{stock_name} 价格 成本 原材料 涨价 降价",
        f"{stock_name} 产能 出货量 市场份额 竞争",
        f"{stock_name} 政策 补贴 环保 监管",
    ]
    all_texts = _fetch_stock_texts(stock_code, days=120)
    if not all_texts:
        return ""

    seen: set[str] = set()
    merged: list[str] = []
    for query in topics:
        top = _fallback_search(all_texts, query, top_k_per_topic)
        for t in top:
            key = t[:80]
            if key not in seen:
                seen.add(key)
                merged.append(t)

    return _truncate_context(merged, max_chars=4000)


def search_news_detail(title_keywords: str, stock_code: str = None) -> dict:
    """根据新闻标题关键词检索详细摘要，供前端展开用。

    从 content_summaries（join extracted_texts）和 cleaned_items（join raw_items）中
    按标题关键词模糊搜索，返回第一条匹配结果的详细字段。

    Returns:
        dict with keys: summary, fact_summary, opinion_summary, full_text_snippet, source, date
        or empty dict if nothing found.
    """
    result = {}

    # 用关键词的前40字做模糊搜索
    kw = title_keywords.strip()[:40] if title_keywords else ""
    if not kw:
        return result

    # 1. 从 content_summaries 查（新管线）
    cs_rows = execute_query(
        """SELECT cs.summary, cs.fact_summary, cs.opinion_summary,
                  et.extracted_text, sd.source_name, sd.created_at, sd.title
           FROM content_summaries cs
           LEFT JOIN extracted_texts et ON cs.extracted_text_id = et.id
           LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
           WHERE (cs.summary LIKE %s OR sd.title LIKE %s)
             AND cs.summary IS NOT NULL
           ORDER BY cs.created_at DESC
           LIMIT 5""",
        [f"%{kw}%", f"%{kw}%"],
    ) or []

    if not cs_rows and stock_code:
        # 按股票关联再查一次，放宽关键词限制
        cs_rows = execute_query(
            """SELECT cs.summary, cs.fact_summary, cs.opinion_summary,
                      et.extracted_text, sd.source_name, sd.created_at, sd.title
               FROM content_summaries cs
               JOIN stock_mentions sm ON cs.extracted_text_id = sm.extracted_text_id
               LEFT JOIN extracted_texts et ON cs.extracted_text_id = et.id
               LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
               WHERE sm.stock_code = %s
                 AND cs.summary IS NOT NULL
               ORDER BY cs.created_at DESC
               LIMIT 10""",
            [stock_code],
        ) or []
        # 再做关键词过滤
        cs_rows = [r for r in cs_rows if kw in (r.get("summary") or "") or kw in (r.get("title") or "")][:3]

    if cs_rows:
        row = cs_rows[0]
        full_text = (row.get("extracted_text") or "")[:500]
        result = {
            "summary": row.get("summary") or "",
            "fact_summary": row.get("fact_summary") or "",
            "opinion_summary": row.get("opinion_summary") or "",
            "full_text_snippet": full_text,
            "source": row.get("source_name") or "",
            "date": str(row.get("created_at") or ""),
            "title": row.get("title") or "",
        }
        return result

    # 2. 从 cleaned_items 查（旧管线）
    ci_rows = execute_query(
        """SELECT ci.summary, ci.fact_summary, ci.opinion_summary,
                  ri.content, ri.source_name, ci.cleaned_at, ri.title
           FROM cleaned_items ci
           LEFT JOIN raw_items ri ON ci.raw_item_id = ri.id
           WHERE (ci.summary LIKE %s OR ri.title LIKE %s)
             AND ci.summary IS NOT NULL
           ORDER BY ci.cleaned_at DESC
           LIMIT 5""",
        [f"%{kw}%", f"%{kw}%"],
    ) or []

    if ci_rows:
        row = ci_rows[0]
        full_text = (row.get("content") or "")[:500]
        result = {
            "summary": row.get("summary") or "",
            "fact_summary": row.get("fact_summary") or "",
            "opinion_summary": row.get("opinion_summary") or "",
            "full_text_snippet": full_text,
            "source": row.get("source_name") or "",
            "date": str(row.get("cleaned_at") or ""),
            "title": row.get("title") or "",
        }

    return result


def search_with_fallback(query: str, top_k: int = 8) -> str:
    """通用检索，优先 hybrid_search()，降级到关键词检索"""
    try:
        from retrieval.hybrid import hybrid_search
        result = hybrid_search(query, top_k=top_k)
        if result and result.merged_context and len(result.merged_context) > 100:
            return result.merged_context[:2000]
    except Exception as e:
        logger.debug(f"hybrid_search 不可用，降级: {e}")

    # 通用降级：从 content_summaries 关键词搜
    keywords = query[:50]
    rows = execute_query(
        """SELECT summary FROM content_summaries
           WHERE summary LIKE %s
             AND created_at >= DATE_SUB(NOW(), INTERVAL 90 DAY)
             AND summary IS NOT NULL
           ORDER BY created_at DESC
           LIMIT 50""",
        [f"%{keywords}%"],
    ) or []
    texts = [r["summary"] for r in rows if r.get("summary")]
    top_texts = _fallback_search(texts, query, top_k)
    return _truncate_context(top_texts)

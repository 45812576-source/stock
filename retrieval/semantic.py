"""L1 语义检索 — bge-m3 → Milvus ANN → MySQL text_chunks 补全"""
import logging
from typing import Optional

from retrieval.models import ChunkResult

logger = logging.getLogger(__name__)


def semantic_search(
    query: str,
    top_k: int = 10,
    filters: Optional[dict] = None,
) -> list[ChunkResult]:
    """L1 纯向量语义检索

    Args:
        query: 查询文本
        top_k: 返回数量
        filters: 过滤条件
            - doc_types: list[str]  文档类型过滤
            - date_range: tuple[str, str]  (start, end) 日期范围
            - stock_codes: list[str]  股票代码过滤（通过 text_chunks metadata）
    Returns:
        [ChunkResult, ...] 按相关性分数降序
    """
    from retrieval.embedding import embed_query
    from retrieval.vector_store import search, ensure_collection

    ensure_collection()

    # 编码查询
    query_vec = embed_query(query)

    # 构建 Milvus filter 表达式
    filter_expr = _build_filter_expr(filters)

    # Milvus 搜索
    hits = search(query_vec, top_k=top_k, filter_expr=filter_expr)
    if not hits:
        return []

    # 从 MySQL 补全 chunk 文本和元数据
    chunk_ids = [h["chunk_id"] for h in hits]
    score_map = {h["chunk_id"]: h["score"] for h in hits}

    from retrieval.chunker import get_chunks_by_ids
    rows = get_chunks_by_ids(chunk_ids)
    row_map = {r["id"]: r for r in rows}

    results = []
    for cid in chunk_ids:
        row = row_map.get(cid)
        if not row:
            continue
        results.append(ChunkResult(
            chunk_id=cid,
            text=row["chunk_text"],
            score=score_map.get(cid, 0.0),
            extracted_text_id=row["extracted_text_id"],
            doc_type=row.get("doc_type") or "",
            file_type=row.get("file_type") or "",
            publish_time=str(row.get("publish_time") or ""),
            source_doc_title=row.get("source_doc_title") or "",
        ))

    return results


def _build_filter_expr(filters: Optional[dict]) -> Optional[str]:
    """构建 Milvus 过滤表达式"""
    if not filters:
        return None

    parts = []

    doc_types = filters.get("doc_types")
    if doc_types and isinstance(doc_types, list):
        quoted = ", ".join(f'"{dt}"' for dt in doc_types)
        parts.append(f"doc_type in [{quoted}]")

    date_range = filters.get("date_range")
    if date_range:
        start, end = date_range
        if start:
            parts.append(f'publish_time >= "{start[:10]}"')
        if end:
            parts.append(f'publish_time <= "{end[:10]}"')

    if not parts:
        return None
    return " and ".join(parts)

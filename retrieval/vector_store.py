"""Milvus 向量存储 — Docker Standalone 连接、collection 管理、CRUD"""
import logging
from typing import Optional

from config import MILVUS_HOST, MILVUS_PORT, EMBEDDING_DIM

logger = logging.getLogger(__name__)

COLLECTION_NAME = "text_chunks"

_connection_alias = "default"
_connected = False


def _ensure_connected():
    """确保已连接 Milvus"""
    global _connected
    if _connected:
        return
    from pymilvus import connections
    connections.connect(alias=_connection_alias, host=MILVUS_HOST, port=MILVUS_PORT)
    _connected = True
    logger.info(f"Milvus 已连接: {MILVUS_HOST}:{MILVUS_PORT}")


def ensure_collection():
    """创建 text_chunks collection（如果不存在），HNSW 索引"""
    from pymilvus import (
        Collection, CollectionSchema, FieldSchema, DataType,
        utility,
    )
    _ensure_connected()

    if utility.has_collection(COLLECTION_NAME):
        col = Collection(COLLECTION_NAME)
        col.load()
        logger.info(f"Collection '{COLLECTION_NAME}' 已存在，已加载")
        return col

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=EMBEDDING_DIM),
        FieldSchema(name="extracted_text_id", dtype=DataType.INT64),
        FieldSchema(name="doc_type", dtype=DataType.VARCHAR, max_length=50),
        FieldSchema(name="publish_time", dtype=DataType.VARCHAR, max_length=20),
    ]
    schema = CollectionSchema(fields, description="text_chunks 向量索引")
    col = Collection(COLLECTION_NAME, schema)

    # HNSW 索引
    col.create_index(
        field_name="embedding",
        index_params={
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 16, "efConstruction": 256},
        },
    )
    col.load()
    logger.info(f"Collection '{COLLECTION_NAME}' 创建完成，HNSW 索引已建立")
    return col


def upsert_chunks(
    chunk_ids: list[int],
    embeddings: list[list[float]],
    extracted_text_ids: list[int],
    doc_types: list[str],
    publish_times: list[str],
):
    """批量写入/更新向量

    Args:
        chunk_ids: text_chunks.id 列表
        embeddings: 对应 embedding 向量列表
        extracted_text_ids: 对应 extracted_text_id
        doc_types: 文档类型
        publish_times: 发布时间字符串 (YYYY-MM-DD)
    """
    from pymilvus import Collection
    _ensure_connected()

    col = Collection(COLLECTION_NAME)
    data = [
        chunk_ids,
        embeddings,
        extracted_text_ids,
        [dt[:50] if dt else "" for dt in doc_types],
        [pt[:20] if pt else "" for pt in publish_times],
    ]
    col.upsert(data)
    logger.debug(f"Milvus upsert {len(chunk_ids)} chunks")


def search(
    query_embedding: list[float],
    top_k: int = 10,
    filter_expr: Optional[str] = None,
) -> list[dict]:
    """向量相似度搜索

    Args:
        query_embedding: 查询向量 (1024维)
        top_k: 返回数量
        filter_expr: Milvus 过滤表达式，如 'doc_type == "research_report"'
    Returns:
        [{"chunk_id": int, "score": float, "extracted_text_id": int, "doc_type": str, "publish_time": str}]
    """
    from pymilvus import Collection
    _ensure_connected()

    col = Collection(COLLECTION_NAME)
    search_params = {"metric_type": "COSINE", "params": {"ef": 128}}

    results = col.search(
        data=[query_embedding],
        anns_field="embedding",
        param=search_params,
        limit=top_k,
        expr=filter_expr,
        output_fields=["extracted_text_id", "doc_type", "publish_time"],
    )

    hits = []
    for hit in results[0]:
        hits.append({
            "chunk_id": hit.id,
            "score": hit.score,
            "extracted_text_id": hit.entity.get("extracted_text_id"),
            "doc_type": hit.entity.get("doc_type"),
            "publish_time": hit.entity.get("publish_time"),
        })
    return hits


def get_collection_stats() -> dict:
    """获取 collection 统计信息"""
    from pymilvus import Collection, utility
    _ensure_connected()

    if not utility.has_collection(COLLECTION_NAME):
        return {"exists": False, "count": 0}

    col = Collection(COLLECTION_NAME)
    col.flush()
    return {"exists": True, "count": col.num_entities}

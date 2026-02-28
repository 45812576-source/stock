"""结构感知切片 — 文本切片 → MySQL text_chunks → Milvus embedding"""
import json
import logging
from typing import Optional

from config import CHUNK_SIZE, CHUNK_OVERLAP
from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)

# 结构感知分隔符：优先按 Markdown 标题切分
_SEPARATORS = ["\n\n##", "\n\n###", "\n\n", "\n", "。", "；", " "]


def _split_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """结构感知切片，优先按标题边界切分"""
    if len(text) <= chunk_size:
        return [text]
    try:
        from langchain_text_splitters import RecursiveCharacterTextSplitter
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=overlap,
            separators=_SEPARATORS,
        )
        return splitter.split_text(text)
    except ImportError:
        # 降级：简单滑窗
        chunks = []
        for i in range(0, len(text), chunk_size - overlap):
            chunks.append(text[i:i + chunk_size])
        return chunks


def chunk_and_index(
    extracted_text_id: int,
    full_text: str,
    doc_type: str = "",
    file_type: str = "",
    publish_time=None,
    source_doc_title: str = "",
    write_milvus: bool = True,
) -> int:
    """切片 → MySQL text_chunks → Milvus embedding

    Args:
        extracted_text_id: extracted_texts.id
        full_text: 完整文本（清洗后）
        doc_type: 文档类型
        file_type: 文件类型
        publish_time: 发布时间 (datetime or str)
        source_doc_title: 来源文档标题
        write_milvus: 是否同时写 Milvus（回填时可分批写）
    Returns:
        写入的 chunk 数
    """
    if not full_text or not full_text.strip():
        return 0

    chunks = _split_text(full_text)
    if not chunks:
        return 0

    pt_str = ""
    if publish_time:
        pt_str = str(publish_time)[:19]  # YYYY-MM-DD HH:MM:SS

    # 写 MySQL text_chunks
    chunk_ids = []
    embeddings_pending = []  # (chunk_id, chunk_text)
    char_offset = 0

    for idx, chunk_text in enumerate(chunks):
        char_start = full_text.find(chunk_text, char_offset)
        if char_start < 0:
            char_start = char_offset
        char_end = char_start + len(chunk_text)
        char_offset = max(char_start, char_offset)

        metadata = {}
        if file_type:
            metadata["file_type"] = file_type

        cid = execute_insert(
            """INSERT INTO text_chunks
               (extracted_text_id, chunk_index, chunk_text, char_start, char_end,
                doc_type, file_type, publish_time, source_doc_title, metadata_json)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
                chunk_text=VALUES(chunk_text), char_start=VALUES(char_start),
                char_end=VALUES(char_end), doc_type=VALUES(doc_type),
                metadata_json=VALUES(metadata_json)""",
            [extracted_text_id, idx, chunk_text, char_start, char_end,
             doc_type or None, file_type or None,
             pt_str or None, source_doc_title or None,
             json.dumps(metadata, ensure_ascii=False) if metadata else None],
        )
        if cid:
            chunk_ids.append(cid)
            embeddings_pending.append((cid, chunk_text))
        else:
            # ON DUPLICATE KEY UPDATE 不返回 new id，查回来
            existing = execute_query(
                "SELECT id FROM text_chunks WHERE extracted_text_id=%s AND chunk_index=%s",
                [extracted_text_id, idx],
            )
            if existing:
                eid = existing[0]["id"]
                chunk_ids.append(eid)
                embeddings_pending.append((eid, chunk_text))

    # 写 Milvus embedding
    if write_milvus and embeddings_pending:
        try:
            _write_embeddings(embeddings_pending, extracted_text_id, doc_type, pt_str)
        except Exception as e:
            logger.warning(f"Milvus 写入失败 et_id={extracted_text_id}: {e}")

    logger.info(f"切片完成 et_id={extracted_text_id}: {len(chunks)} chunks")
    return len(chunks)


def _write_embeddings(
    pending: list[tuple[int, str]],
    extracted_text_id: int,
    doc_type: str,
    publish_time_str: str,
    batch_size: int = 64,
):
    """批量生成 embedding 并写入 Milvus"""
    from retrieval.embedding import embed_texts
    from retrieval.vector_store import ensure_collection, upsert_chunks

    ensure_collection()

    for i in range(0, len(pending), batch_size):
        batch = pending[i:i + batch_size]
        ids = [b[0] for b in batch]
        texts = [b[1] for b in batch]

        embeddings = embed_texts(texts)

        upsert_chunks(
            chunk_ids=ids,
            embeddings=embeddings,
            extracted_text_ids=[extracted_text_id] * len(ids),
            doc_types=[doc_type] * len(ids),
            publish_times=[publish_time_str[:10]] * len(ids),
        )


def get_chunks_by_ids(chunk_ids: list[int]) -> list[dict]:
    """从 MySQL 批量获取 chunk 详情"""
    if not chunk_ids:
        return []
    placeholders = ",".join(["%s"] * len(chunk_ids))
    return execute_query(
        f"SELECT * FROM text_chunks WHERE id IN ({placeholders})",
        chunk_ids,
    )


def get_chunks_by_extracted_text(extracted_text_id: int) -> list[dict]:
    """获取某篇文档的所有 chunks"""
    return execute_query(
        "SELECT * FROM text_chunks WHERE extracted_text_id=%s ORDER BY chunk_index",
        [extracted_text_id],
    )

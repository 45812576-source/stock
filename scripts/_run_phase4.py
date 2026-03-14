#!/usr/bin/env python3
"""Phase 4: 切片 + 写入 Milvus（独立进程，Phase 3 完全退出后再跑）"""
import logging, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("HF_HUB_OFFLINE", "1")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("scripts/_run_phase4.log", encoding="utf-8")])
logger = logging.getLogger(__name__)

from retrieval.chunker import chunk_and_index
from retrieval.vector_store import ensure_collection
from utils.db_utils import execute_cloud_query, execute_query

logger.info("=== Phase 4: 切片写入 Milvus ===")
ensure_collection()

last_id = 0
ok = total_chunks = err = 0

while True:
    done_ids = {r["extracted_text_id"] for r in (
        execute_query("SELECT DISTINCT extracted_text_id FROM text_chunks WHERE extracted_text_id > %s", [last_id]) or []
    )}
    rows = execute_cloud_query(
        """SELECT et.id, et.full_text, et.publish_time, sd.doc_type, sd.file_type, sd.title
           FROM extracted_texts et LEFT JOIN source_documents sd ON sd.id = et.source_doc_id
           WHERE et.id > %s AND et.full_text IS NOT NULL AND et.full_text != ''
           ORDER BY et.id LIMIT 200""",
        [last_id],
    )
    if not rows:
        break

    for row in rows:
        last_id = row["id"]
        if row["id"] in done_ids:
            continue
        try:
            n = chunk_and_index(
                row["id"], row["full_text"],
                doc_type=row.get("doc_type") or "",
                file_type=row.get("file_type") or "",
                publish_time=row.get("publish_time"),
                source_doc_title=row.get("title") or "",
            )
            ok += 1
            total_chunks += n
        except Exception as e:
            logger.error(f"切片失败 et_id={row['id']}: {e}")
            err += 1

    logger.info(f"切片进度: {ok}条 → {total_chunks}个 chunks, err={err}")

logger.info(f"=== Phase 4 完成: {ok}条 → {total_chunks}个 chunks, {err}错误 ===")

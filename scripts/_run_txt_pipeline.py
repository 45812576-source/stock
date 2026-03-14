#!/usr/bin/env python3
"""txt 全量处理：提取 → 推入管线 → 下游 A/B2/C → 切片写入 Milvus

流程：
  Phase 1: pending/failed txt → DeepSeek 语义理解 → extract_status='extracted'
  Phase 2: extracted txt → push_to_extracted_texts → extract_status='ready_to_pipe'
  Phase 3: unified_pipeline A/B2/C 下游（content_summaries / stock_mentions / KG）
  Phase 4: backfill text_chunks → Milvus 向量索引

已 extracted 的 1474 条直接从 Phase 2 开始。
"""
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("HF_HUB_OFFLINE", "1")

from utils.db_utils import execute_cloud_query, execute_cloud_insert, execute_query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scripts/_run_txt_pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

EXTRACT_BATCH = 50
EXTRACT_WORKERS = 4


def process_txt_single(row: dict) -> dict:
    from ingestion.source_extractor import _extract_txt, _semantic_clean
    from config.doc_types import classify_doc_type
    doc_id = row["id"]
    try:
        text = _extract_txt(row)
        if not text or len(text.strip()) < 20:
            execute_cloud_insert(
                "UPDATE source_documents SET extract_status='skipped' WHERE id=%s", [doc_id]
            )
            return {"id": doc_id, "status": "skipped"}
        cleaned = _semantic_clean(text, "txt", doc_id, needs_understanding=True)
        new_doc_type = classify_doc_type(row.get("title") or "", (cleaned or "")[:200])
        execute_cloud_insert(
            "UPDATE source_documents SET extracted_text=%s, extract_status='extracted', doc_type=%s WHERE id=%s",
            [cleaned, new_doc_type, doc_id],
        )
        return {"id": doc_id, "status": "success", "chars": len(cleaned)}
    except Exception as e:
        logger.error(f"txt 提取失败 id={doc_id}: {e}")
        execute_cloud_insert(
            "UPDATE source_documents SET extract_status='failed' WHERE id=%s", [doc_id]
        )
        return {"id": doc_id, "status": "failed", "error": str(e)[:200]}


def phase1_extract():
    """提取 pending/failed txt"""
    total_count = execute_cloud_query(
        "SELECT COUNT(*) as n FROM source_documents WHERE file_type='txt' AND extract_status IN ('pending','failed')"
    )[0]["n"]
    logger.info(f"Phase 1: 待提取 txt {total_count} 条")
    if total_count == 0:
        return

    processed = success = failed = skipped = 0
    while True:
        rows = execute_cloud_query(
            """SELECT id, title, text_content, file_type
               FROM source_documents
               WHERE file_type='txt' AND extract_status IN ('pending','failed')
               ORDER BY id LIMIT %s""",
            [EXTRACT_BATCH],
        )
        if not rows:
            break
        with ThreadPoolExecutor(max_workers=EXTRACT_WORKERS) as pool:
            futures = {pool.submit(process_txt_single, r): r for r in rows}
            for fut in as_completed(futures):
                r = fut.result()
                processed += 1
                if r["status"] == "success": success += 1
                elif r["status"] == "failed": failed += 1
                else: skipped += 1
        if processed % 200 == 0:
            logger.info(f"  提取进度: {processed}/{total_count} 成功={success} 失败={failed} 跳过={skipped}")

    logger.info(f"Phase 1 完成: 提取={success} 跳过={skipped} 失败={failed}")


def phase2_push():
    """推入 extracted_texts（extracted → ready_to_pipe）"""
    logger.info("Phase 2: 推入 extracted_texts...")
    from ingestion.source_extractor import push_to_extracted_texts
    push_ok = 0
    while True:
        r = push_to_extracted_texts(limit=200)
        pushed = r.get("pushed", 0)
        push_ok += pushed
        logger.info(f"  push batch: pushed={pushed} skipped={r.get('skipped', 0)}")
        if pushed == 0:
            break
    logger.info(f"Phase 2 完成: 累计推入 {push_ok} 条")


def phase3_pipeline():
    """unified_pipeline 下游 A/B2/C"""
    logger.info("Phase 3: unified_pipeline A/B2/C...")
    from cleaning.unified_pipeline import process_pending
    result = process_pending(batch_size=50, max_workers=3)
    logger.info(f"Phase 3 完成: {result}")


def phase4_chunks():
    """切片 + 写入 Milvus（增量，跳过已处理）"""
    logger.info("Phase 4: 切片写入 Milvus...")
    from retrieval.chunker import chunk_and_index
    from retrieval.vector_store import ensure_collection
    ensure_collection()

    total = ok = err = 0
    last_id = 0

    while True:
        # 已处理的 extracted_text_id（本地）
        done_rows = execute_query(
            "SELECT DISTINCT extracted_text_id FROM text_chunks WHERE extracted_text_id > %s",
            [last_id],
        )
        done_ids = {r["extracted_text_id"] for r in (done_rows or [])}

        rows = execute_cloud_query(
            """SELECT et.id, et.full_text, et.publish_time,
                      sd.doc_type, sd.file_type, sd.title
               FROM extracted_texts et
               LEFT JOIN source_documents sd ON sd.id = et.source_doc_id
               WHERE et.id > %s
                 AND et.full_text IS NOT NULL AND et.full_text != ''
               ORDER BY et.id LIMIT 100""",
            [last_id],
        )
        if not rows:
            break

        for row in rows:
            et_id = row["id"]
            last_id = et_id
            if et_id in done_ids:
                continue
            try:
                n = chunk_and_index(
                    extracted_text_id=et_id,
                    full_text=row["full_text"],
                    doc_type=row.get("doc_type") or "",
                    file_type=row.get("file_type") or "",
                    publish_time=row.get("publish_time"),
                    source_doc_title=row.get("title") or "",
                )
                ok += 1
                total += n
                if ok % 100 == 0:
                    logger.info(f"  切片进度: {ok} 条 → {total} chunks")
            except Exception as e:
                logger.error(f"  切片失败 et_id={et_id}: {e}")
                err += 1

    logger.info(f"Phase 4 完成: {ok} 条 → {total} chunks，{err} 个错误")


def main():
    t0 = time.time()
    logger.info("=== txt 全量处理开始 ===")

    phase1_extract()
    phase2_push()
    phase3_pipeline()
    phase4_chunks()

    logger.info(f"=== 全部完成，总耗时: {(time.time()-t0)/60:.1f} 分钟 ===")


if __name__ == "__main__":
    main()

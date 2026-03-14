#!/usr/bin/env python3
"""txt 全量提取 → 推入管线 → 下游（跳过审核）

流程：
  1. 取 pending txt 批量提取（DeepSeek 语义理解）→ extract_status='extracted'
  2. 推入 extracted_texts（push_to_extracted_texts_by_ids）
  3. 对新写入的 extracted_text_id 运行 unified_pipeline（A/B2/C）
"""
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import execute_cloud_query, execute_cloud_insert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scripts/_run_txt_full.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 50
WORKERS = 4


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


def main():
    start = time.time()
    total_count = execute_cloud_query(
        "SELECT COUNT(*) as cnt FROM source_documents WHERE file_type='txt' AND extract_status IN ('pending','failed')"
    )[0]["cnt"]
    logger.info(f"待提取 txt: {total_count} 条")

    # ── Phase 1: 提取 ──────────────────────────────────────────────────────────
    processed = success = failed = skipped = 0
    while True:
        rows = execute_cloud_query(
            """SELECT id, title, text_content, file_type
               FROM source_documents
               WHERE file_type='txt' AND extract_status IN ('pending','failed')
               ORDER BY id LIMIT %s""",
            [BATCH_SIZE],
        )
        if not rows:
            break

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(process_txt_single, r): r for r in rows}
            for fut in as_completed(futures):
                r = fut.result()
                processed += 1
                if r["status"] == "success": success += 1
                elif r["status"] == "failed": failed += 1
                else: skipped += 1

        if processed % 200 == 0 or processed >= total_count:
            logger.info(f"  提取进度: {processed}/{total_count} 成功={success} 失败={failed} 跳过={skipped}")

    logger.info(f"Phase 1 完成: 提取={success} 跳过={skipped} 失败={failed} 耗时={int(time.time()-start)}s")

    if success == 0:
        logger.warning("没有成功提取的文档，退出")
        return

    # ── Phase 2: 推入 extracted_texts ─────────────────────────────────────────
    logger.info("Phase 2: 推入 extracted_texts（extracted → ready_to_pipe）...")
    from ingestion.source_extractor import push_to_extracted_texts

    push_total = push_ok = push_skip = 0
    while True:
        r = push_to_extracted_texts(limit=200)
        pushed = r.get("pushed", 0)
        push_ok += pushed
        push_skip += r.get("skipped", 0)
        push_total += r.get("total", 0)
        logger.info(f"  push batch: pushed={pushed} skipped={r.get('skipped',0)}")
        if pushed == 0:
            break

    logger.info(f"Phase 2 完成: push_ok={push_ok} 耗时={int(time.time()-start)}s")

    # ── Phase 3: unified_pipeline 下游 ────────────────────────────────────────
    logger.info("Phase 3: unified_pipeline A/B2/C...")
    from cleaning.unified_pipeline import process_pending

    pipeline_result = process_pending(limit=10000, max_workers=3)
    logger.info(f"Phase 3 完成: {pipeline_result}")
    logger.info(f"全部完成，总耗时: {(time.time()-start)/60:.1f} 分钟")


if __name__ == "__main__":
    main()

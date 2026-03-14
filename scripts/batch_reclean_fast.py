#!/usr/bin/env python3
"""快速批量清洗 — 仅 DeepSeek 文本清洗，不重新提取

任务 1: txt 快速清洗 — text_content → _extract_txt() → _semantic_clean()
任务 2: PDF/image/mixed 已有 extracted_text → 直接 _semantic_clean()

不下载文件，不调 Qwen 视觉，只调 DeepSeek 做语义清洗。

用法：
  python scripts/batch_reclean_fast.py txt                # 只处理 txt
  python scripts/batch_reclean_fast.py pdf                # 只处理已提取的 pdf
  python scripts/batch_reclean_fast.py all                # txt + pdf + image + mixed
  python scripts/batch_reclean_fast.py txt --batch-size 50 --workers 3
  python scripts/batch_reclean_fast.py txt --dry-run
"""
import argparse
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
        logging.FileHandler("scripts/batch_reclean_fast.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def process_txt_single(row: dict) -> dict:
    """处理单条 txt：提取 text_content → DeepSeek 清洗"""
    from ingestion.source_extractor import _extract_txt, _semantic_clean

    doc_id = row["id"]
    try:
        # 1) 从 text_content 提取纯文本（解析 ZSXQ 标签）
        text = _extract_txt(row)
        if not text or len(text.strip()) < 20:
            return {"id": doc_id, "status": "skipped", "reason": "text_too_short"}

        # 2) DeepSeek 语义清洗（txt 走理解整理）
        cleaned = _semantic_clean(text, "txt", doc_id, needs_understanding=True)

        # 3) 写回 source_documents
        from config.doc_types import classify_doc_type
        new_doc_type = classify_doc_type(row.get("title") or "", (cleaned or "")[:200])
        execute_cloud_insert(
            """UPDATE source_documents
               SET extracted_text=%s, extract_status='extracted', doc_type=%s
               WHERE id=%s""",
            [cleaned, new_doc_type, doc_id],
        )
        return {"id": doc_id, "status": "success", "chars": len(cleaned)}

    except Exception as e:
        logger.error(f"txt 处理失败 id={doc_id}: {e}")
        execute_cloud_insert(
            "UPDATE source_documents SET extract_status='failed' WHERE id=%s",
            [doc_id],
        )
        return {"id": doc_id, "status": "failed", "error": str(e)[:200]}


def process_existing_text_single(row: dict) -> dict:
    """处理已有 extracted_text 的文档：直接 DeepSeek 清洗"""
    from ingestion.source_extractor import _semantic_clean

    doc_id = row["id"]
    file_type = row["file_type"]
    text = (row.get("extracted_text") or "").strip()

    if not text or len(text) < 20:
        return {"id": doc_id, "status": "skipped", "reason": "no_extracted_text"}

    try:
        # PDF 已提取过的 → 轻度清洗（假设正常提取已过 Qwen）
        # image/mixed 已提取过的 → 也轻度清洗
        needs_understanding = False
        cleaned = _semantic_clean(text, file_type, doc_id, needs_understanding)

        execute_cloud_insert(
            """UPDATE source_documents
               SET extracted_text=%s, extract_status='extracted'
               WHERE id=%s""",
            [cleaned, doc_id],
        )
        return {"id": doc_id, "status": "success", "chars": len(cleaned)}

    except Exception as e:
        logger.error(f"{file_type} 清洗失败 id={doc_id}: {e}")
        return {"id": doc_id, "status": "failed", "error": str(e)[:200]}


def run_batch(file_types: list, batch_size: int, workers: int, dry_run: bool):
    """批量处理"""
    for ft in file_types:
        logger.info(f"\n{'='*60}")
        logger.info(f"开始处理 file_type={ft}")
        logger.info(f"{'='*60}")

        if ft == "txt":
            # txt: 处理所有 pending + 之前被旧流程 reset 的
            count_rows = execute_cloud_query(
                """SELECT COUNT(*) as cnt FROM source_documents
                   WHERE file_type='txt' AND extract_status IN ('pending', 'failed')"""
            )
            total = count_rows[0]["cnt"]
            logger.info(f"  待处理 txt: {total}")

            if dry_run:
                continue

            processed = 0
            success = 0
            failed = 0
            skipped = 0

            while True:
                rows = execute_cloud_query(
                    """SELECT id, title, text_content, file_type
                       FROM source_documents
                       WHERE file_type='txt' AND extract_status IN ('pending', 'failed')
                       ORDER BY id
                       LIMIT %s""",
                    [batch_size],
                )
                if not rows:
                    break

                if workers > 1:
                    with ThreadPoolExecutor(max_workers=workers) as pool:
                        futures = {pool.submit(process_txt_single, r): r for r in rows}
                        for future in as_completed(futures):
                            result = future.result()
                            processed += 1
                            if result["status"] == "success":
                                success += 1
                            elif result["status"] == "failed":
                                failed += 1
                            else:
                                skipped += 1
                            if processed % 50 == 0:
                                logger.info(f"  txt 进度: {processed}/{total} (成功={success} 失败={failed} 跳过={skipped})")
                else:
                    for r in rows:
                        result = process_txt_single(r)
                        processed += 1
                        if result["status"] == "success":
                            success += 1
                        elif result["status"] == "failed":
                            failed += 1
                        else:
                            skipped += 1
                        if processed % 50 == 0:
                            logger.info(f"  txt 进度: {processed}/{total} (成功={success} 失败={failed} 跳过={skipped})")

            logger.info(f"  txt 完成: 总计={processed} 成功={success} 失败={failed} 跳过={skipped}")

        else:
            # pdf/image/mixed: 只处理已有 extracted_text 的（done 状态，有文本）
            # 先把 done 的改为 cleaning 标记，避免重复处理
            count_rows = execute_cloud_query(
                """SELECT COUNT(*) as cnt FROM source_documents
                   WHERE file_type=%s AND extract_status IN ('extracted','ready_to_pipe','done')
                     AND extracted_text IS NOT NULL AND LENGTH(extracted_text) >= 20""",
                [ft],
            )
            total = count_rows[0]["cnt"]
            logger.info(f"  待清洗 {ft} (已有提取文本): {total}")

            if dry_run:
                continue

            processed = 0
            success = 0
            failed = 0
            skipped = 0

            while True:
                rows = execute_cloud_query(
                    """SELECT id, title, file_type, extracted_text
                       FROM source_documents
                       WHERE file_type=%s AND extract_status IN ('extracted','ready_to_pipe','done')
                         AND extracted_text IS NOT NULL AND LENGTH(extracted_text) >= 20
                       ORDER BY id
                       LIMIT %s""",
                    [ft, batch_size],
                )
                if not rows:
                    break

                # 先标记为 cleaning 防止重复取
                ids = [r["id"] for r in rows]
                placeholders = ",".join(["%s"] * len(ids))
                execute_cloud_insert(
                    f"UPDATE source_documents SET extract_status='cleaning' WHERE id IN ({placeholders})",
                    ids,
                )

                if workers > 1:
                    with ThreadPoolExecutor(max_workers=workers) as pool:
                        futures = {pool.submit(process_existing_text_single, r): r for r in rows}
                        for future in as_completed(futures):
                            result = future.result()
                            processed += 1
                            if result["status"] == "success":
                                success += 1
                            elif result["status"] == "failed":
                                failed += 1
                            else:
                                skipped += 1
                            if processed % 20 == 0:
                                logger.info(f"  {ft} 进度: {processed}/{total} (成功={success} 失败={failed} 跳过={skipped})")
                else:
                    for r in rows:
                        result = process_existing_text_single(r)
                        processed += 1
                        if result["status"] == "success":
                            success += 1
                        elif result["status"] == "failed":
                            failed += 1
                        else:
                            skipped += 1
                        if processed % 20 == 0:
                            logger.info(f"  {ft} 进度: {processed}/{total} (成功={success} 失败={failed} 跳过={skipped})")

            logger.info(f"  {ft} 完成: 总计={processed} 成功={success} 失败={failed} 跳过={skipped}")


def main():
    parser = argparse.ArgumentParser(description="快速批量清洗（仅 DeepSeek，不重新提取）")
    parser.add_argument("target", choices=["txt", "pdf", "image", "mixed", "all"],
                        help="处理目标")
    parser.add_argument("--batch-size", type=int, default=30,
                        help="每批从 DB 取出的数量（默认 30）")
    parser.add_argument("--workers", type=int, default=3,
                        help="并发 DeepSeek 调用数（默认 3）")
    parser.add_argument("--dry-run", action="store_true",
                        help="只统计数量")
    args = parser.parse_args()

    if args.target == "all":
        targets = ["txt", "pdf", "image", "mixed"]
    else:
        targets = [args.target]

    logger.info(f"目标: {targets}, batch_size={args.batch_size}, workers={args.workers}")

    start = time.time()
    run_batch(targets, args.batch_size, args.workers, args.dry_run)
    elapsed = time.time() - start
    logger.info(f"\n总耗时: {elapsed/60:.1f} 分钟")


if __name__ == "__main__":
    main()

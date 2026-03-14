#!/usr/bin/env python3
"""批量重新提取 image/mixed/pdf(pending) — 带 URL 失效检测

对 pending 状态的 image/mixed/pdf 做全量重提取+清洗。
源链接失效（401/403/404/超时）的标记 extract_status='url_expired'，
以便后续重新采集时精准命中这些条目。

用法：
  python scripts/batch_extract_media.py image --workers 2
  python scripts/batch_extract_media.py mixed --workers 2
  python scripts/batch_extract_media.py pdf --workers 1
  python scripts/batch_extract_media.py all --workers 2
  python scripts/batch_extract_media.py all --dry-run
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
        logging.FileHandler("scripts/batch_extract_media.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# URL 失效相关的错误关键词
_URL_EXPIRED_KEYWORDS = [
    "401", "403", "404", "Unauthorized", "Forbidden", "Not Found",
    "AccessDenied", "expired", "InvalidAccessKeyId", "NoSuchKey",
    "SignatureDoesNotMatch", "Request has expired", "ConnectionError",
    "MaxRetryError", "SSLError",
]


def _is_url_expired(error_str: str) -> bool:
    """判断错误是否属于 URL 失效类"""
    for kw in _URL_EXPIRED_KEYWORDS:
        if kw.lower() in error_str.lower():
            return True
    return False


def process_single(row: dict) -> dict:
    """提取+清洗单条 image/mixed/pdf"""
    from ingestion.source_extractor import _extract_and_clean_single

    doc_id = row["id"]
    file_type = row["file_type"]
    title = (row.get("title") or "")[:50]

    try:
        extracted = _extract_and_clean_single(row)

        if extracted and len(extracted.strip()) >= 20:
            from config.doc_types import classify_doc_type
            new_doc_type = classify_doc_type(
                row.get("title") or "",
                (extracted or "")[:200],
            )
            execute_cloud_insert(
                """UPDATE source_documents
                   SET extracted_text=%s, extract_status='extracted', doc_type=%s
                   WHERE id=%s""",
                [extracted, new_doc_type, doc_id],
            )
            return {"id": doc_id, "status": "success", "chars": len(extracted)}
        else:
            execute_cloud_insert(
                """UPDATE source_documents
                   SET extracted_text=%s, extract_status='extracted'
                   WHERE id=%s""",
                [extracted or "", doc_id],
            )
            return {"id": doc_id, "status": "short", "chars": len(extracted or "")}

    except Exception as e:
        err_str = str(e)
        if _is_url_expired(err_str):
            logger.warning(f"  URL失效 id={doc_id} {file_type} {title}: {err_str[:120]}")
            execute_cloud_insert(
                "UPDATE source_documents SET extract_status='url_expired' WHERE id=%s",
                [doc_id],
            )
            return {"id": doc_id, "status": "url_expired", "error": err_str[:200]}
        else:
            logger.error(f"  提取失败 id={doc_id} {file_type} {title}: {err_str[:200]}")
            execute_cloud_insert(
                "UPDATE source_documents SET extract_status='failed' WHERE id=%s",
                [doc_id],
            )
            return {"id": doc_id, "status": "failed", "error": err_str[:200]}


def run_batch(file_types: list, batch_size: int, workers: int, dry_run: bool):
    """批量处理"""
    for ft in file_types:
        logger.info(f"\n{'='*60}")
        logger.info(f"开始处理 file_type={ft}")
        logger.info(f"{'='*60}")

        count_rows = execute_cloud_query(
            """SELECT COUNT(*) as cnt FROM source_documents
               WHERE file_type=%s AND extract_status IN ('pending', 'failed')""",
            [ft],
        )
        total = count_rows[0]["cnt"]
        logger.info(f"  待提取 {ft}: {total}")

        if dry_run or total == 0:
            continue

        processed = 0
        success = 0
        failed = 0
        url_expired = 0
        short = 0

        while True:
            rows = execute_cloud_query(
                """SELECT id, doc_type, file_type, title, text_content, oss_url, extract_status
                   FROM source_documents
                   WHERE file_type=%s AND extract_status IN ('pending', 'failed')
                   ORDER BY id
                   LIMIT %s""",
                [ft, batch_size],
            )
            if not rows:
                break

            if workers > 1:
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futures = {pool.submit(process_single, r): r for r in rows}
                    for future in as_completed(futures):
                        result = future.result()
                        processed += 1
                        st = result["status"]
                        if st == "success":
                            success += 1
                        elif st == "url_expired":
                            url_expired += 1
                        elif st == "failed":
                            failed += 1
                        elif st == "short":
                            short += 1
                        if processed % 20 == 0:
                            logger.info(
                                f"  {ft} 进度: {processed}/{total} "
                                f"(成功={success} 失效={url_expired} 失败={failed} 短文={short})"
                            )
            else:
                for r in rows:
                    result = process_single(r)
                    processed += 1
                    st = result["status"]
                    if st == "success":
                        success += 1
                    elif st == "url_expired":
                        url_expired += 1
                    elif st == "failed":
                        failed += 1
                    elif st == "short":
                        short += 1
                    if processed % 20 == 0:
                        logger.info(
                            f"  {ft} 进度: {processed}/{total} "
                            f"(成功={success} 失效={url_expired} 失败={failed} 短文={short})"
                        )

        logger.info(
            f"  {ft} 完成: 总计={processed} 成功={success} "
            f"URL失效={url_expired} 失败={failed} 短文={short}"
        )


def main():
    parser = argparse.ArgumentParser(description="批量提取 image/mixed/pdf（带 URL 失效检测）")
    parser.add_argument("target", choices=["image", "mixed", "pdf", "all"],
                        help="处理目标")
    parser.add_argument("--batch-size", type=int, default=10,
                        help="每批数量（默认 10）")
    parser.add_argument("--workers", type=int, default=2,
                        help="并发数（默认 2，PDF 建议 1）")
    parser.add_argument("--dry-run", action="store_true",
                        help="只统计数量")
    args = parser.parse_args()

    if args.target == "all":
        targets = ["image", "mixed", "pdf"]
    else:
        targets = [args.target]

    logger.info(f"目标: {targets}, batch_size={args.batch_size}, workers={args.workers}")

    start = time.time()
    run_batch(targets, args.batch_size, args.workers, args.dry_run)
    elapsed = time.time() - start
    logger.info(f"\n总耗时: {elapsed/60:.1f} 分钟")


if __name__ == "__main__":
    main()

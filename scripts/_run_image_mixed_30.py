#!/usr/bin/env python3
"""image + mixed 先提取 30 条 → 停下等人工确认

提取完成后状态为 extracted，不自动推入管线。
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
        logging.FileHandler("scripts/_run_image_mixed_30.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

LIMIT = 30
WORKERS = 2

_URL_EXPIRED_KEYWORDS = [
    "401", "403", "404", "Unauthorized", "Forbidden", "Not Found",
    "AccessDenied", "expired", "InvalidAccessKeyId", "NoSuchKey",
    "SignatureDoesNotMatch", "Request has expired", "ConnectionError",
    "MaxRetryError", "SSLError",
]

def _is_url_expired(err: str) -> bool:
    return any(kw.lower() in err.lower() for kw in _URL_EXPIRED_KEYWORDS)


def process_single(row: dict) -> dict:
    from ingestion.source_extractor import _extract_and_clean_single
    from config.doc_types import classify_doc_type
    doc_id = row["id"]
    file_type = row["file_type"]
    try:
        extracted = _extract_and_clean_single(row)
        if extracted and len(extracted.strip()) >= 20:
            new_doc_type = classify_doc_type(row.get("title") or "", extracted[:200])
            execute_cloud_insert(
                "UPDATE source_documents SET extracted_text=%s, extract_status='extracted', doc_type=%s WHERE id=%s",
                [extracted, new_doc_type, doc_id],
            )
            return {"id": doc_id, "status": "success", "chars": len(extracted), "file_type": file_type}
        else:
            execute_cloud_insert(
                "UPDATE source_documents SET extracted_text=%s, extract_status='extracted' WHERE id=%s",
                [extracted or "", doc_id],
            )
            return {"id": doc_id, "status": "short", "chars": len(extracted or ""), "file_type": file_type}
    except Exception as e:
        err_str = str(e)
        if _is_url_expired(err_str):
            execute_cloud_insert(
                "UPDATE source_documents SET extract_status='url_expired' WHERE id=%s", [doc_id]
            )
            return {"id": doc_id, "status": "url_expired", "file_type": file_type, "error": err_str[:150]}
        else:
            execute_cloud_insert(
                "UPDATE source_documents SET extract_status='failed' WHERE id=%s", [doc_id]
            )
            return {"id": doc_id, "status": "failed", "file_type": file_type, "error": err_str[:150]}


def main():
    start = time.time()

    rows = execute_cloud_query(
        """SELECT id, doc_type, file_type, title, text_content, oss_url, extract_status
           FROM source_documents
           WHERE file_type IN ('image','mixed') AND extract_status IN ('pending','failed')
           ORDER BY id LIMIT %s""",
        [LIMIT],
    )
    logger.info(f"取到 {len(rows)} 条 image/mixed 待提取")

    success = failed = url_expired = short = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(process_single, r): r for r in rows}
        for fut in as_completed(futures):
            r = fut.result()
            st = r["status"]
            if st == "success": success += 1
            elif st == "short": short += 1
            elif st == "url_expired": url_expired += 1
            else: failed += 1
            logger.info(f"  id={r['id']} {r['file_type']} → {st} chars={r.get('chars','-')}")

    logger.info(
        f"\n完成 {len(rows)} 条: 成功={success} 短文={short} 链接失效={url_expired} 失败={failed}"
    )
    logger.info(f"耗时: {int(time.time()-start)}s")
    logger.info("已停止，请在前端审核 extracted 状态文档后手动 Pipe It")


if __name__ == "__main__":
    main()

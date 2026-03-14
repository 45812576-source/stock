"""对 DeepSeek 余额不足时原文透传的 extracted_texts 记录重跑语义清洗

判断标准：LENGTH(full_text) >= LENGTH(text_content) * 0.90，即基本未压缩
只处理 eastmoney_report 来源的研报。

用法：
    python scripts/reclean_passthrough.py            # 3并发
    python scripts/reclean_passthrough.py --workers 5
    python scripts/reclean_passthrough.py --dry-run
"""
import sys
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, ".")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scripts/reclean_passthrough.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def get_passthrough_ids() -> list[dict]:
    """找出 full_text 长度接近原始 text_content 的记录（未清洗）"""
    from utils.db_utils import execute_cloud_query
    rows = execute_cloud_query(
        "SELECT et.id, et.full_text, et.source_doc_id, sd.file_type "
        "FROM extracted_texts et "
        "JOIN source_documents sd ON sd.id = et.source_doc_id "
        "WHERE sd.source = %s "
        "  AND LENGTH(et.full_text) >= LENGTH(sd.text_content) * 0.90 "
        "  AND LENGTH(et.full_text) > 500 "
        "ORDER BY et.id",
        ["eastmoney_report"]
    ) or []
    return rows


def reclean_one(row: dict) -> bool:
    """对单条 extracted_texts 记录重跑语义清洗，成功则 UPDATE full_text"""
    from utils.db_utils import execute_cloud_insert
    from ingestion.source_extractor import _semantic_clean

    et_id = row["id"]
    raw_text = row["full_text"]
    file_type = row.get("file_type") or "txt"

    try:
        cleaned = _semantic_clean(raw_text, file_type, row["source_doc_id"], needs_understanding=False)
        if cleaned and cleaned != raw_text and len(cleaned) > 50:
            execute_cloud_insert(
                "UPDATE extracted_texts SET full_text=%s WHERE id=%s",
                [cleaned, et_id]
            )
            return True
        else:
            logger.debug(f"[{et_id}] 清洗后无变化或过短，跳过")
            return False
    except Exception as e:
        logger.error(f"[{et_id}] 清洗失败: {e}")
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=3, help="并发数（默认3）")
    parser.add_argument("--dry-run", action="store_true", help="只统计不执行")
    args = parser.parse_args()

    rows = get_passthrough_ids()
    logger.info(f"待重跑清洗: {len(rows)} 条")

    if not rows or args.dry_run:
        print(f"共 {len(rows)} 条需要重跑（dry-run）")
        return

    done = 0
    skipped = 0
    failed = 0
    total = len(rows)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(reclean_one, row): row["id"] for row in rows}
        for i, future in enumerate(as_completed(futures), 1):
            et_id = futures[future]
            try:
                result = future.result()
                if result:
                    done += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.error(f"[{et_id}] 异常: {e}")
                failed += 1

            if i % 100 == 0:
                logger.info(f"进度 {i}/{total}  清洗={done} 跳过={skipped} 失败={failed}")

    logger.info(f"完成！清洗={done} 跳过={skipped} 失败={failed}")


if __name__ == "__main__":
    main()

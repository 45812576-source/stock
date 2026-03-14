"""回填历史 source_documents.doc_type

对所有 doc_type 为 'news'/'report'/'other'/NULL 的记录，
用 classify_doc_type 重新分类并写回云端。

用法:
    python scripts/backfill_doc_type.py [--batch 500] [--dry-run]
"""
import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.doc_types import classify_doc_type
from utils.db_utils import execute_cloud_query, execute_cloud_insert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def backfill(batch: int = 500, dry_run: bool = False):
    offset = 0
    total_updated = 0

    while True:
        rows = execute_cloud_query(
            """SELECT id, doc_type, title, text_content, extracted_text
               FROM source_documents
               ORDER BY id
               LIMIT %s OFFSET %s""",
            [batch, offset],
        )
        if not rows:
            break

        updates = []
        for row in rows:
            title = row.get("title") or ""
            content = row.get("extracted_text") or row.get("text_content") or ""
            new_type = classify_doc_type(title, content[:200])
            old_type = row.get("doc_type") or ""
            if new_type != old_type:
                updates.append((new_type, row["id"], old_type))

        if updates and not dry_run:
            for new_type, doc_id, old_type in updates:
                execute_cloud_insert(
                    "UPDATE source_documents SET doc_type=%s WHERE id=%s",
                    [new_type, doc_id],
                )

        logger.info(
            f"offset={offset} rows={len(rows)} updates={len(updates)}"
            + (" [dry-run]" if dry_run else "")
        )
        total_updated += len(updates)
        offset += len(rows)

        if len(rows) < batch:
            break

    logger.info(f"回填完成: 共更新 {total_updated} 条" + (" [dry-run，未写入]" if dry_run else ""))
    return total_updated


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="回填 source_documents.doc_type")
    parser.add_argument("--batch", type=int, default=500, help="每批处理数量")
    parser.add_argument("--dry-run", action="store_true", help="仅统计，不写入")
    args = parser.parse_args()
    backfill(batch=args.batch, dry_run=args.dry_run)

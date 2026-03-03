#!/usr/bin/env python3
"""回填脚本：对已有研报/行业分析类文档跑 Pipeline D 指标抽取

用法：
    python scripts/backfill_pipeline_d.py              # 全量（慢）
    python scripts/backfill_pipeline_d.py --limit 50   # 只跑50条
    python scripts/backfill_pipeline_d.py --dry-run    # 只统计数量
"""
import argparse
import logging
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import execute_cloud_query
from config.doc_types import classify_doc_type, FAMILY_MAP
from cleaning.industry_indicator_extractor import run_pipeline_d

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TARGET_FAMILY = 2  # 研报策略/行业分析/路演纪要/深度特稿

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # 查研报/行业分析类文档（从云端库）
    fetch_limit = args.limit * 5 if args.limit else 10000
    sql = """
        SELECT et.id, et.full_text, sd.doc_type, sd.title, et.publish_time
        FROM extracted_texts et
        LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
        WHERE et.full_text IS NOT NULL AND LENGTH(et.full_text) > 200
        ORDER BY et.id DESC
        LIMIT %s
    """
    rows = execute_cloud_query(sql, [fetch_limit])
    logger.info(f"候选文档 {len(rows)} 条")

    target_rows = []
    for r in rows:
        title = r.get("title") or ""
        doc_type_raw = r.get("doc_type") or ""
        full_text = r.get("full_text") or ""
        # 用已有 doc_type 直接查 FAMILY_MAP，若无则用 classify_doc_type
        if doc_type_raw and doc_type_raw in FAMILY_MAP:
            family = FAMILY_MAP[doc_type_raw]
        else:
            classified = classify_doc_type(title, full_text[:200])
            family = FAMILY_MAP.get(classified, 4)
        if family == TARGET_FAMILY:
            target_rows.append(r)
        if args.limit and len(target_rows) >= args.limit:
            break

    logger.info(f"研报/行业分析类(family=2): {len(target_rows)} 条")
    if args.dry_run:
        return

    total = 0
    for i, r in enumerate(target_rows):
        try:
            count = run_pipeline_d(r["id"], r["full_text"] or "")
            total += count
            logger.info(f"[{i+1}/{len(target_rows)}] id={r['id']} → {count} 条指标")
        except Exception as e:
            logger.error(f"id={r['id']} 失败: {e}")

    logger.info(f"回填完成，共写入 {total} 条指标")

if __name__ == "__main__":
    main()

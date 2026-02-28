"""存量 extracted_texts 切片回填脚本

用法:
    python scripts/backfill_chunks.py              # 全量回填（断点续传）
    python scripts/backfill_chunks.py --limit 100  # 只处理100条
    python scripts/backfill_chunks.py --batch 50   # 每批50条

预估时间: ~13,938条 → ~50-80K chunks → bge-m3 约15-30分钟
"""
import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def get_last_processed_id() -> int:
    """从 system_config 读取断点 ID"""
    from utils.db_utils import execute_query
    rows = execute_query(
        "SELECT value FROM system_config WHERE config_key='backfill_chunks_last_id'"
    )
    if rows:
        try:
            return int(rows[0]["value"])
        except (ValueError, TypeError):
            pass
    return 0


def save_last_processed_id(last_id: int):
    """保存断点 ID 到 system_config"""
    from utils.db_utils import execute_insert
    execute_insert(
        """INSERT INTO system_config (config_key, value)
           VALUES ('backfill_chunks_last_id', %s)
           ON DUPLICATE KEY UPDATE value = %s""",
        [str(last_id), str(last_id)],
    )


def main():
    parser = argparse.ArgumentParser(description="存量 text_chunks 回填")
    parser.add_argument("--limit", type=int, default=0, help="最多处理条数（0=全量）")
    parser.add_argument("--batch", type=int, default=50, help="每批大小")
    parser.add_argument("--reset", action="store_true", help="重置断点，从头开始")
    args = parser.parse_args()

    from utils.db_utils import execute_cloud_query, execute_query
    from retrieval.chunker import chunk_and_index
    from retrieval.vector_store import ensure_collection

    ensure_collection()

    if args.reset:
        save_last_processed_id(0)
        logger.info("断点已重置")

    last_id = get_last_processed_id()
    logger.info(f"从 id > {last_id} 开始回填")

    total_processed = 0
    total_chunks = 0
    total_errors = 0

    while True:
        # 查找还没有 text_chunks 的 extracted_texts
        rows = execute_cloud_query(
            """SELECT et.id, et.full_text, et.publish_time,
                      sd.file_type, sd.title
               FROM extracted_texts et
               LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
               WHERE et.id > %s
                 AND et.full_text IS NOT NULL
                 AND et.full_text != ''
                 AND et.id NOT IN (
                   SELECT DISTINCT extracted_text_id FROM text_chunks
                   WHERE extracted_text_id > %s
                 )
               ORDER BY et.id
               LIMIT %s""",
            [last_id, last_id, args.batch],
        )

        if not rows:
            logger.info("没有更多待处理记录")
            break

        for row in rows:
            et_id = row["id"]
            full_text = row["full_text"] or ""
            if not full_text.strip():
                last_id = et_id
                continue

            try:
                n = chunk_and_index(
                    extracted_text_id=et_id,
                    full_text=full_text,
                    file_type=row.get("file_type") or "",
                    publish_time=row.get("publish_time"),
                    source_doc_title=row.get("title") or "",
                )
                total_chunks += n
                total_processed += 1
                if total_processed % 50 == 0:
                    logger.info(
                        f"进度: {total_processed} 条 / {total_chunks} chunks / {total_errors} 错误"
                    )
            except Exception as e:
                logger.error(f"回填失败 id={et_id}: {e}")
                total_errors += 1

            last_id = et_id
            save_last_processed_id(last_id)

            if args.limit and total_processed >= args.limit:
                break

        if args.limit and total_processed >= args.limit:
            break

    logger.info(
        f"回填完成: {total_processed} 条文档 → {total_chunks} chunks，{total_errors} 个错误"
    )
    logger.info(f"断点 ID 保存为: {last_id}")


if __name__ == "__main__":
    main()

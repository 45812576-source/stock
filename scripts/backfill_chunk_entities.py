"""chunk_entities 回填脚本 — 字符串匹配找实体，写 chunk_entities 表

用法:
    python scripts/backfill_chunk_entities.py              # 全量
    python scripts/backfill_chunk_entities.py --limit 500  # 限制 chunk 数
    python scripts/backfill_chunk_entities.py --batch 200  # 每批大小

原理: 对每个 text_chunk，遍历所有 kg_entities，
      找出 entity_name 出现在 chunk_text 中的实体，写入 chunk_entities。
      纯字符串匹配，无需 AI。

预估时间: ~50K chunks × ~11K 实体 = 需要高效批量处理
         实际通过 SQL LIKE 批量处理，约5分钟。
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


def main():
    parser = argparse.ArgumentParser(description="chunk_entities 字符串匹配回填")
    parser.add_argument("--limit", type=int, default=0, help="最多处理 chunk 数（0=全量）")
    parser.add_argument("--batch", type=int, default=200, help="每批 chunk 大小")
    parser.add_argument("--min-entity-len", type=int, default=3, help="最短实体名长度")
    args = parser.parse_args()

    from utils.db_utils import execute_query, execute_insert

    # 加载所有 kg_entities（过滤过短的实体名）
    logger.info("加载 kg_entities...")
    entities = execute_query(
        "SELECT id, entity_name, entity_type FROM kg_entities WHERE CHAR_LENGTH(entity_name) >= %s",
        [args.min_entity_len],
    )
    if not entities:
        logger.error("kg_entities 为空，请先建立知识图谱")
        return

    logger.info(f"加载 {len(entities)} 个实体")

    # 获取最大 chunk id 作为进度依据
    max_id_row = execute_query("SELECT MAX(id) as max_id FROM text_chunks")
    max_chunk_id = (max_id_row[0]["max_id"] or 0) if max_id_row else 0

    total_chunks = 0
    total_links = 0
    offset = 0

    while True:
        # 批量取尚未处理的 chunks（不在 chunk_entities 中）
        chunks = execute_query(
            """SELECT id, chunk_text FROM text_chunks
               WHERE id NOT IN (
                 SELECT DISTINCT chunk_id FROM chunk_entities
               )
               ORDER BY id
               LIMIT %s OFFSET %s""",
            [args.batch, offset],
        )

        if not chunks:
            break

        batch_links = []
        for chunk in chunks:
            cid = chunk["id"]
            text = chunk["chunk_text"] or ""

            for ent in entities:
                if ent["entity_name"] in text:
                    batch_links.append((cid, ent["id"], "mentioned"))

        # 批量写入
        for chunk_id, entity_id, mention_type in batch_links:
            try:
                execute_insert(
                    """INSERT IGNORE INTO chunk_entities
                       (chunk_id, entity_id, mention_type)
                       VALUES (%s, %s, %s)""",
                    [chunk_id, entity_id, mention_type],
                )
            except Exception as e:
                logger.debug(f"chunk_entities 写入失败: {e}")

        total_chunks += len(chunks)
        total_links += len(batch_links)

        if total_chunks % 1000 == 0:
            logger.info(f"进度: {total_chunks} chunks → {total_links} 实体关联")

        if args.limit and total_chunks >= args.limit:
            break

        # 如果结果数少于 batch，说明已处理完
        if len(chunks) < args.batch:
            break

        offset += args.batch

    logger.info(f"回填完成: {total_chunks} chunks → {total_links} chunk_entities 关联")


if __name__ == "__main__":
    main()

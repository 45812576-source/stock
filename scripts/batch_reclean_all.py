#!/usr/bin/env python3
"""批量重新清洗全量数据

步骤：
  1. 删除 mp3/audio 的 source_documents 和 extracted_texts
  2. 清空 ABC 管线输出（content_summaries, stock_mentions, kg_entities, kg_relationships）
  3. 清空 extracted_texts 全表
  4. 对所有非 audio 的 source_documents 重新 extract+clean
  5. 重新灌入 extracted_texts

用法：
  python scripts/batch_reclean_all.py                     # 完整执行
  python scripts/batch_reclean_all.py --step clean-only    # 只清空数据不重新提取
  python scripts/batch_reclean_all.py --step extract-only  # 只重新提取（跳过清空）
  python scripts/batch_reclean_all.py --resume             # 断点续传（跳过已 done 的）
  python scripts/batch_reclean_all.py --dry-run            # 预览数据量
  python scripts/batch_reclean_all.py --batch-size 20      # 调整批量大小
"""
import argparse
import logging
import sys
import time
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import (
    execute_cloud_query, execute_cloud_insert,
    execute_query, execute_insert,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scripts/batch_reclean.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def survey_data():
    """统计当前数据量"""
    print("\n========== 数据量统计 ==========")

    # 云端表
    for table in ["content_summaries", "stock_mentions", "extracted_texts"]:
        rows = execute_cloud_query(f"SELECT COUNT(*) as cnt FROM {table}")
        print(f"  云端 {table}: {rows[0]['cnt']}")

    # source_documents 分类统计
    rows = execute_cloud_query("""
        SELECT file_type, extract_status, COUNT(*) as cnt
        FROM source_documents
        GROUP BY file_type, extract_status
        ORDER BY file_type, extract_status
    """)
    print("\n  source_documents 分布:")
    for r in rows:
        print(f"    {r['file_type']:8s} | {r['extract_status']:10s} | {r['cnt']}")

    # 本地表
    for table in ["kg_entities", "kg_relationships"]:
        try:
            rows = execute_query(f"SELECT COUNT(*) as cnt FROM {table}")
            print(f"  本地 {table}: {rows[0]['cnt']}")
        except Exception as e:
            print(f"  本地 {table}: 查询失败 ({e})")

    print("================================\n")


def step1_delete_audio():
    """删除所有 mp3/audio 记录"""
    logger.info("===== Step 1: 删除 mp3/audio =====")

    # 删除 extracted_texts 中的音频
    rows = execute_cloud_query(
        "SELECT COUNT(*) as cnt FROM extracted_texts WHERE source_format='audio'"
    )
    cnt = rows[0]["cnt"]
    if cnt > 0:
        execute_cloud_insert(
            "DELETE FROM extracted_texts WHERE source_format='audio'"
        )
        logger.info(f"  删除 extracted_texts 音频: {cnt} 条")
    else:
        logger.info("  extracted_texts 无音频记录")

    # 标记 source_documents 中的 mp3/audio 为 rejected
    rows = execute_cloud_query(
        "SELECT COUNT(*) as cnt FROM source_documents WHERE file_type IN ('mp3', 'audio')"
    )
    cnt = rows[0]["cnt"]
    if cnt > 0:
        execute_cloud_insert(
            "UPDATE source_documents SET extract_status='rejected' WHERE file_type IN ('mp3', 'audio')"
        )
        logger.info(f"  标记 source_documents mp3/audio 为 rejected: {cnt} 条")
    else:
        logger.info("  source_documents 无 mp3/audio 记录")


def step2_clear_pipeline_outputs():
    """清空 ABC 管线输出"""
    logger.info("===== Step 2: 清空 ABC 管线输出 =====")

    # 云端表
    for table in ["content_summaries", "stock_mentions"]:
        rows = execute_cloud_query(f"SELECT COUNT(*) as cnt FROM {table}")
        cnt = rows[0]["cnt"]
        if cnt > 0:
            execute_cloud_insert(f"DELETE FROM {table}")
            logger.info(f"  清空云端 {table}: {cnt} 条")
        else:
            logger.info(f"  云端 {table} 已为空")

    # 本地表
    for table in ["kg_entities", "kg_relationships"]:
        try:
            rows = execute_query(f"SELECT COUNT(*) as cnt FROM {table}")
            cnt = rows[0]["cnt"]
            if cnt > 0:
                execute_insert(f"DELETE FROM {table}")
                logger.info(f"  清空本地 {table}: {cnt} 条")
            else:
                logger.info(f"  本地 {table} 已为空")
        except Exception as e:
            logger.warning(f"  本地 {table} 操作失败: {e}")


def step3_clear_extracted_texts():
    """清空 extracted_texts 全表"""
    logger.info("===== Step 3: 清空 extracted_texts =====")
    rows = execute_cloud_query("SELECT COUNT(*) as cnt FROM extracted_texts")
    cnt = rows[0]["cnt"]
    if cnt > 0:
        execute_cloud_insert("DELETE FROM extracted_texts")
        logger.info(f"  清空 extracted_texts: {cnt} 条")
    else:
        logger.info("  extracted_texts 已为空")


def step4_reextract_and_clean(batch_size=10, resume=False):
    """对所有非 audio 的 source_documents 重新 extract+clean

    Args:
        batch_size: 每批处理数量
        resume: 是否断点续传（跳过已 extract_status='done' 且有 extracted_text 的）
    """
    logger.info("===== Step 4: 重新提取+清洗 =====")

    from ingestion.source_extractor import _extract_and_clean_single

    # 先重置所有非 audio 的 extract_status 为 pending（除非断点续传）
    if not resume:
        execute_cloud_insert(
            """UPDATE source_documents
               SET extract_status='pending', extracted_text=NULL
               WHERE file_type NOT IN ('mp3', 'audio')
                 AND extract_status != 'rejected'"""
        )
        logger.info("  已重置所有非 audio 文档状态为 pending")

    # 统计待处理数量
    if resume:
        count_rows = execute_cloud_query(
            """SELECT COUNT(*) as cnt FROM source_documents
               WHERE file_type NOT IN ('mp3', 'audio')
                 AND extract_status != 'rejected'
                 AND (extract_status != 'done' OR extracted_text IS NULL OR extracted_text = '')"""
        )
    else:
        count_rows = execute_cloud_query(
            """SELECT COUNT(*) as cnt FROM source_documents
               WHERE file_type NOT IN ('mp3', 'audio')
                 AND extract_status = 'pending'"""
        )
    total = count_rows[0]["cnt"]
    logger.info(f"  待处理文档总数: {total}")

    processed = 0
    success = 0
    failed = 0
    skipped = 0

    while True:
        # 每次取一批
        if resume:
            rows = execute_cloud_query(
                """SELECT id, doc_type, file_type, title, text_content, oss_url, extract_status
                   FROM source_documents
                   WHERE file_type NOT IN ('mp3', 'audio')
                     AND extract_status != 'rejected'
                     AND (extract_status != 'done' OR extracted_text IS NULL OR extracted_text = '')
                   ORDER BY id
                   LIMIT %s""",
                [batch_size],
            )
        else:
            rows = execute_cloud_query(
                """SELECT id, doc_type, file_type, title, text_content, oss_url, extract_status
                   FROM source_documents
                   WHERE file_type NOT IN ('mp3', 'audio')
                     AND extract_status = 'pending'
                   ORDER BY id
                   LIMIT %s""",
                [batch_size],
            )

        if not rows:
            break

        for row in rows:
            doc_id = row["id"]
            file_type = row["file_type"]
            title = (row.get("title") or "")[:50]

            try:
                logger.info(f"  [{processed+1}/{total}] 处理 id={doc_id} type={file_type} {title}")
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
                    success += 1
                else:
                    execute_cloud_insert(
                        """UPDATE source_documents
                           SET extracted_text=%s, extract_status='extracted'
                           WHERE id=%s""",
                        [extracted or "", doc_id],
                    )
                    skipped += 1
                    logger.warning(f"    提取结果过短 id={doc_id}: {len(extracted or '')} 字")

            except Exception as e:
                err_str = str(e)
                if "401" in err_str or "403" in err_str or "Unauthorized" in err_str:
                    logger.warning(f"    跳过 id={doc_id} (URL 过期): {err_str[:100]}")
                    execute_cloud_insert(
                        "UPDATE source_documents SET extract_status='skipped' WHERE id=%s",
                        [doc_id],
                    )
                    skipped += 1
                else:
                    logger.error(f"    处理失败 id={doc_id}: {e}")
                    execute_cloud_insert(
                        "UPDATE source_documents SET extract_status='failed' WHERE id=%s",
                        [doc_id],
                    )
                    failed += 1

            processed += 1

            # 每 10 条输出进度
            if processed % 10 == 0:
                logger.info(f"  进度: {processed}/{total} (成功={success}, 失败={failed}, 跳过={skipped})")

    logger.info(f"  提取+清洗完成: 总计={processed}, 成功={success}, 失败={failed}, 跳过={skipped}")
    return {"total": processed, "success": success, "failed": failed, "skipped": skipped}


def step5_push_to_extracted_texts(batch_size=50):
    """重新灌入 extracted_texts"""
    logger.info("===== Step 5: 灌入 extracted_texts =====")

    from ingestion.source_extractor import push_to_extracted_texts

    total_pushed = 0
    total_skipped = 0
    total_failed = 0
    batch_num = 0

    while True:
        batch_num += 1
        result = push_to_extracted_texts(limit=batch_size)
        total_pushed += result["pushed"]
        total_skipped += result["skipped"]
        total_failed += result["failed"]

        logger.info(
            f"  批次 {batch_num}: pushed={result['pushed']}, "
            f"skipped={result['skipped']}, failed={result['failed']}"
        )

        if result["total"] == 0 or result["pushed"] == 0:
            break

    logger.info(
        f"  灌入完成: pushed={total_pushed}, skipped={total_skipped}, failed={total_failed}"
    )


def main():
    parser = argparse.ArgumentParser(description="批量重新清洗全量数据")
    parser.add_argument("--step", choices=["clean-only", "extract-only", "push-only"],
                        help="只执行指定步骤")
    parser.add_argument("--resume", action="store_true",
                        help="断点续传：跳过已 done 且有 extracted_text 的")
    parser.add_argument("--dry-run", action="store_true",
                        help="只统计数据量，不执行操作")
    parser.add_argument("--batch-size", type=int, default=10,
                        help="每批处理数量（默认 10）")
    parser.add_argument("--yes", "-y", action="store_true",
                        help="跳过确认提示")
    args = parser.parse_args()

    survey_data()

    if args.dry_run:
        print("--dry-run 模式，仅统计数据量")
        return

    if not args.yes:
        if args.step:
            msg = f"即将执行步骤: {args.step}"
        else:
            msg = "即将执行完整清洗流程（删除音频 → 清空管线 → 清空extracted_texts → 重新提取+清洗 → 灌入）"
        confirm = input(f"\n{msg}\n确认执行？(y/n): ")
        if confirm.lower() != "y":
            print("已取消")
            return

    start = time.time()

    if args.step == "clean-only":
        step1_delete_audio()
        step2_clear_pipeline_outputs()
        step3_clear_extracted_texts()
    elif args.step == "extract-only":
        step4_reextract_and_clean(batch_size=args.batch_size, resume=args.resume)
    elif args.step == "push-only":
        step5_push_to_extracted_texts(batch_size=args.batch_size)
    else:
        step1_delete_audio()
        step2_clear_pipeline_outputs()
        step3_clear_extracted_texts()
        step4_reextract_and_clean(batch_size=args.batch_size, resume=args.resume)
        step5_push_to_extracted_texts(batch_size=args.batch_size)

    elapsed = time.time() - start
    logger.info(f"\n完成！耗时 {elapsed/60:.1f} 分钟")
    survey_data()


if __name__ == "__main__":
    main()

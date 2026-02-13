"""批量清洗调度器 — 支持进度回调、并发控制、重试"""
import logging
import time
from datetime import datetime
from utils.db_utils import execute_query, execute_insert
from cleaning.claude_processor import clean_single_item, clean_with_event_analysis

logger = logging.getLogger(__name__)


def batch_clean(limit=50, deep_analysis=False, progress_callback=None):
    """批量清洗待处理条目

    Args:
        limit: 最大处理条数
        deep_analysis: 是否对高重要性条目做深度分析
        progress_callback: 进度回调函数 callback(current, total, item_title)
    """
    # 记录流水线
    run_id = execute_insert(
        "INSERT INTO pipeline_runs (pipeline_name) VALUES ('batch_clean')"
    )

    pending = execute_query(
        "SELECT id, title FROM raw_items WHERE processing_status='pending' ORDER BY fetched_at LIMIT ?",
        [limit],
    )

    total = len(pending)
    success = 0
    failed = 0

    for i, item in enumerate(pending):
        # 进度回调
        if progress_callback:
            progress_callback(i + 1, total, item.get("title", ""))

        # 标记为处理中
        execute_insert(
            "UPDATE raw_items SET processing_status='processing' WHERE id=?",
            [item["id"]],
        )

        try:
            if deep_analysis:
                result = clean_with_event_analysis(item["id"])
            else:
                result = clean_single_item(item["id"])

            if result:
                success += 1
            else:
                failed += 1
        except Exception as e:
            logger.error(f"清洗异常 id={item['id']}: {e}")
            execute_insert(
                "UPDATE raw_items SET processing_status='failed' WHERE id=?",
                [item["id"]],
            )
            failed += 1

        # 请求间隔，避免API限流
        time.sleep(0.5)

    # 更新流水线记录
    status = "success" if failed == 0 else ("failed" if success == 0 else "partial")
    execute_insert(
        """UPDATE pipeline_runs SET finished_at=CURRENT_TIMESTAMP,
           status=?, items_processed=?, details_json=?
           WHERE id=?""",
        [status, success + failed,
         f'{{"success": {success}, "failed": {failed}, "total": {total}}}',
         run_id],
    )

    logger.info(f"批量清洗完成: 成功{success}, 失败{failed}, 总计{total}")
    return {"success": success, "failed": failed, "total": total, "run_id": run_id}


def retry_failed(limit=20, progress_callback=None):
    """重试失败的条目"""
    failed_items = execute_query(
        "SELECT id FROM raw_items WHERE processing_status='failed' ORDER BY fetched_at LIMIT ?",
        [limit],
    )

    # 重置状态为pending
    for item in failed_items:
        execute_insert(
            "UPDATE raw_items SET processing_status='pending' WHERE id=?",
            [item["id"]],
        )

    if failed_items:
        return batch_clean(limit=len(failed_items), progress_callback=progress_callback)
    return {"success": 0, "failed": 0, "total": 0}


def get_cleaning_stats():
    """获取清洗统计"""
    stats = {}
    for status in ["pending", "processing", "cleaned", "failed"]:
        rows = execute_query(
            "SELECT COUNT(*) as cnt FROM raw_items WHERE processing_status=?",
            [status],
        )
        stats[status] = rows[0]["cnt"] if rows else 0

    # 今日统计
    today = datetime.now().strftime("%Y-%m-%d")
    rows = execute_query(
        "SELECT COUNT(*) as cnt FROM cleaned_items WHERE date(cleaned_at)=?",
        [today],
    )
    stats["today_cleaned"] = rows[0]["cnt"] if rows else 0

    rows = execute_query(
        "SELECT COUNT(*) as cnt FROM raw_items WHERE date(fetched_at)=?",
        [today],
    )
    stats["today_fetched"] = rows[0]["cnt"] if rows else 0

    return stats

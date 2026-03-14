"""批量清洗调度器 — 支持进度回调、并发控制、重试"""
import logging
import time
from datetime import datetime
from utils.db_utils import execute_cloud_query, execute_cloud_insert
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
    run_id = execute_cloud_insert(
        "INSERT INTO pipeline_runs (pipeline_name, stage) VALUES ('batch_clean', 'cleaning')"
    )

    pending = execute_cloud_query(
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
        execute_cloud_insert(
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
            execute_cloud_insert(
                "UPDATE raw_items SET processing_status='failed' WHERE id=?",
                [item["id"]],
            )
            failed += 1

        # 请求间隔，避免API限流
        time.sleep(0.5)

    # 更新流水线记录
    status = "success" if failed == 0 else ("failed" if success == 0 else "partial")
    execute_cloud_insert(
        """UPDATE pipeline_runs SET finished_at=CURRENT_TIMESTAMP,
           status=?, items_processed=?, details_json=?
           WHERE id=?""",
        [status, success + failed,
         f'{{"success": {success}, "failed": {failed}, "total": {total}}}',
         run_id],
    )

    logger.info(f"批量清洗完成: 成功{success}, 失败{failed}, 总计{total}")
    return {"success": success, "failed": failed, "total": total, "run_id": run_id}


def batch_clean_parallel(limit=500, workers=5, short_first=True, progress_callback=None):
    """并发批量清洗 — 多 worker 同时调用 Claude

    Args:
        limit: 最大处理条数
        workers: 并发 worker 数
        short_first: True 则按内容长度升序（短内容优先）
        progress_callback: callback(current, total, item_title)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    run_id = execute_cloud_insert(
        "INSERT INTO pipeline_runs (pipeline_name, stage) VALUES ('batch_clean_parallel', 'cleaning')"
    )

    order = "LENGTH(content) ASC" if short_first else "fetched_at ASC"
    pending = execute_cloud_query(
        f"SELECT id, title FROM raw_items WHERE processing_status='pending' ORDER BY {order} LIMIT ?",
        [limit],
    )

    total = len(pending)
    counter = {"success": 0, "failed": 0, "done": 0}
    lock = threading.Lock()

    def _process(item):
        rid = item["id"]
        try:
            execute_cloud_insert(
                "UPDATE raw_items SET processing_status='processing' WHERE id=?",
                [rid],
            )
            result = clean_single_item(rid)
            with lock:
                if result:
                    counter["success"] += 1
                else:
                    counter["failed"] += 1
                counter["done"] += 1
                if progress_callback:
                    progress_callback(counter["done"], total, item.get("title", ""))
        except Exception as e:
            logger.error(f"清洗异常 id={rid}: {e}")
            execute_cloud_insert(
                "UPDATE raw_items SET processing_status='failed' WHERE id=?",
                [rid],
            )
            with lock:
                counter["failed"] += 1
                counter["done"] += 1

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_process, item) for item in pending]
        for f in as_completed(futures):
            f.result()  # propagate exceptions if any

    status = "success" if counter["failed"] == 0 else (
        "failed" if counter["success"] == 0 else "partial")
    execute_cloud_insert(
        """UPDATE pipeline_runs SET finished_at=CURRENT_TIMESTAMP,
           status=?, items_processed=?, details_json=?
           WHERE id=?""",
        [status, counter["success"] + counter["failed"],
         f'{{"success": {counter["success"]}, "failed": {counter["failed"]}, "total": {total}}}',
         run_id],
    )

    logger.info(f"并发清洗完成: 成功{counter['success']}, 失败{counter['failed']}, 总计{total}")
    return {"success": counter["success"], "failed": counter["failed"],
            "total": total, "run_id": run_id}


def retry_failed(limit=20, progress_callback=None):
    """重试失败的条目"""
    failed_items = execute_cloud_query(
        "SELECT id FROM raw_items WHERE processing_status='failed' ORDER BY fetched_at LIMIT ?",
        [limit],
    )

    # 重置状态为pending
    for item in failed_items:
        execute_cloud_insert(
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
        rows = execute_cloud_query(
            "SELECT COUNT(*) as cnt FROM raw_items WHERE processing_status=?",
            [status],
        )
        stats[status] = rows[0]["cnt"] if rows else 0

    # 今日统计
    today = datetime.now().strftime("%Y-%m-%d")
    rows = execute_cloud_query(
        "SELECT COUNT(*) as cnt FROM cleaned_items WHERE date(cleaned_at)=?",
        [today],
    )
    stats["today_cleaned"] = rows[0]["cnt"] if rows else 0

    rows = execute_cloud_query(
        "SELECT COUNT(*) as cnt FROM raw_items WHERE date(fetched_at)=?",
        [today],
    )
    stats["today_fetched"] = rows[0]["cnt"] if rows else 0

    return stats


def batch_summarize(limit=50, workers=3, progress_callback=None):
    """批量总结 extracted_texts（Pipeline A）

    Args:
        limit: 最大处理条数
        workers: 并发 worker 数
        progress_callback: callback(current, total, msg)
    Returns:
        {"success": int, "failed": int, "total": int, "run_id": int}
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    from cleaning.content_summarizer import summarize_single

    run_id = execute_cloud_insert(
        "INSERT INTO pipeline_runs (pipeline_name, stage) VALUES ('batch_summarize', 'summarizing')"
    )

    pending = execute_cloud_query(
        """SELECT id FROM extracted_texts
           WHERE (summary_status IS NULL OR summary_status='pending') AND extract_quality='pass'
           ORDER BY id ASC LIMIT %s""",
        [limit],
    )

    total = len(pending)
    counter = {"success": 0, "failed": 0, "done": 0}
    lock = threading.Lock()

    def _process(item):
        eid = item["id"]
        try:
            result = summarize_single(eid)
            with lock:
                if result:
                    counter["success"] += 1
                else:
                    counter["failed"] += 1
                counter["done"] += 1
                if progress_callback:
                    progress_callback(counter["done"], total,
                                      f"总结 extracted_text id={eid}")
        except Exception as e:
            logger.error(f"总结异常 id={eid}: {e}")
            with lock:
                counter["failed"] += 1
                counter["done"] += 1

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_process, item) for item in pending]
        for f in as_completed(futures):
            f.result()

    status = "success" if counter["failed"] == 0 else (
        "failed" if counter["success"] == 0 else "partial")
    execute_cloud_insert(
        """UPDATE pipeline_runs SET finished_at=CURRENT_TIMESTAMP,
           status=%s, items_processed=%s, details_json=%s
           WHERE id=%s""",
        [status, counter["success"] + counter["failed"],
         f'{{"success": {counter["success"]}, "failed": {counter["failed"]}, "total": {total}}}',
         run_id],
    )

    logger.info(f"批量总结完成: 成功{counter['success']}, 失败{counter['failed']}, 总计{total}")
    return {"success": counter["success"], "failed": counter["failed"],
            "total": total, "run_id": run_id}

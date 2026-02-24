"""定时任务调度 — KG自动构建 + 推理

规则:
- 每天 06:00 和 20:00 自动执行 KG 增量构建（structured模式，不调Claude）
- 每次构建完成后自动运行一次推理引擎
- 手动触发构建完成后也自动跟一次推理
"""
import logging
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

# ── 状态追踪 ──────────────────────────────────────────────────

def _ensure_state_table():
    execute_insert(
        """CREATE TABLE IF NOT EXISTS scheduler_state (
            `key` VARCHAR(255) PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )""", []
    )

def _get_state(key, default=None):
    _ensure_state_table()
    rows = execute_query("SELECT value FROM scheduler_state WHERE `key`=%s", [key])
    return rows[0]["value"] if rows else default

def _set_state(key, value):
    _ensure_state_table()
    execute_insert(
        """INSERT INTO scheduler_state (`key`, value, updated_at)
           VALUES (%s, %s, CURRENT_TIMESTAMP)
           ON DUPLICATE KEY UPDATE value=VALUES(value), updated_at=CURRENT_TIMESTAMP""",
        [key, str(value)],
    )


# ── KG 构建任务 ──────────────────────────────────────────────

def run_kg_update():
    """增量构建KG：只处理上次构建后新增的 cleaned_items"""
    since = _get_state("kg_last_auto_update")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"[Scheduler] KG自动构建开始, since={since}")

    try:
        from knowledge_graph.kg_updater import update_from_cleaned_items
        result = update_from_cleaned_items(since_date=since, use_claude=False)
        _set_state("kg_last_auto_update", now_str)
        logger.info(f"[Scheduler] KG构建完成: {result}")

        # 构建完成 → 自动跑推理
        run_inference_after_build()
        return result
    except Exception as e:
        logger.exception(f"[Scheduler] KG自动构建失败: {e}")
        return {"error": str(e)}


def run_inference_after_build():
    """构建完成后自动运行推理引擎（4条规则全跑）"""
    logger.info("[Scheduler] 自动推理开始")
    try:
        from routers.knowledge_graph import _run_inference_sync
        discovered = _run_inference_sync(rule_type="all", auto_accept=True)
        logger.info(f"[Scheduler] 自动推理完成, 发现 {len(discovered)} 条关系, 已自动采纳高置信度结果")
        return discovered
    except Exception as e:
        logger.exception(f"[Scheduler] 自动推理失败: {e}")
        return []


# ── 调度器启停 ──────────────────────────────────────────────

def start_scheduler():
    """启动定时任务（FastAPI启动时调用）"""
    if scheduler.running:
        return

    # 每天 06:00
    scheduler.add_job(
        run_kg_update, CronTrigger(hour=6, minute=0),
        id="kg_auto_morning", replace_existing=True,
        name="KG早间自动构建",
    )
    # 每天 20:00
    scheduler.add_job(
        run_kg_update, CronTrigger(hour=20, minute=0),
        id="kg_auto_evening", replace_existing=True,
        name="KG晚间自动构建",
    )

    scheduler.start()
    logger.info("[Scheduler] 定时任务已启动: 06:00 + 20:00 KG自动构建")


def stop_scheduler():
    """停止定时任务（FastAPI关闭时调用）"""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("[Scheduler] 定时任务已停止")

"""分层批量标签更新器

更新规则（分层）：
  Tier A — 全量股票库（~5000只）：只跑 L1 量化规则（纯计算，无AI调用）
  Tier B — 有新闻提及的股票（近30天 stock_mentions 有记录）：跑 L2 AI轻量标注
  Tier C — 重点股票（watchlist + portfolio + 已有≥2个选股标签）：跑 L3 AI深度分析

触发方式：
  - 手动：系统设置页面按钮
  - 定时：scheduler.py 每日 08:00 跑 Tier A，每日 09:00 跑 Tier B，每周一 09:30 跑 Tier C

进度回调：callback(stage, done, total, message)
"""
import logging
import threading
from datetime import datetime, timedelta
from typing import Callable, Optional

logger = logging.getLogger(__name__)


def _lq(sql, params=None):
    from utils.db_utils import execute_query
    return execute_query(sql, params or []) or []


# ── 股票范围查询 ──────────────────────────────────────────────────────────────

def _get_all_stock_codes() -> list[str]:
    """全量股票库"""
    rows = _lq("SELECT stock_code FROM stock_info ORDER BY stock_code")
    return [r["stock_code"] for r in rows]


def _get_tier_b_codes() -> list[str]:
    """近30天有 stock_mentions 记录的股票"""
    rows = _lq(
        """SELECT DISTINCT stock_code FROM stock_mentions
           WHERE stock_code IS NOT NULL AND stock_code != ''
             AND mention_time >= %s""",
        [(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")],
    )
    return [r["stock_code"] for r in rows]


def _get_tier_c_codes() -> list[str]:
    """重点股票：watchlist + portfolio + 已有≥2个选股标签"""
    watchlist = _lq("SELECT stock_code FROM watchlist")
    portfolio = _lq("SELECT DISTINCT stock_code FROM holding_positions WHERE status='open'")
    tagged = _lq(
        """SELECT stock_code FROM stock_rule_tags
           WHERE matched=1
           GROUP BY stock_code HAVING COUNT(*) >= 2"""
    )
    codes = set()
    for rows in [watchlist, portfolio, tagged]:
        codes.update(r["stock_code"] for r in rows)
    return list(codes)


# ── 单层更新 ──────────────────────────────────────────────────────────────────

def run_tier_a(
    stock_codes: Optional[list[str]] = None,
    callback: Optional[Callable] = None,
) -> dict:
    """Tier A：全量 L1 量化计算"""
    from tagging.l1_quant_engine import run_l1_for_stock

    codes = stock_codes or _get_all_stock_codes()
    total = len(codes)
    done = errors = 0

    if callback:
        callback("tier_a", 0, total, f"Tier A 开始：{total} 只股票")

    for code in codes:
        try:
            run_l1_for_stock(code)
            done += 1
        except Exception as e:
            errors += 1
            logger.warning(f"Tier A L1 error {code}: {e}")
        if callback and done % 100 == 0:
            callback("tier_a", done, total, f"Tier A 进度 {done}/{total}")

    if callback:
        callback("tier_a", total, total, f"Tier A 完成：{done} 成功 {errors} 失败")
    return {"tier": "A", "total": total, "done": done, "errors": errors}


def run_tier_b(
    stock_codes: Optional[list[str]] = None,
    limit_per_run: int = 200,
    callback: Optional[Callable] = None,
) -> dict:
    """Tier B：有新闻提及的股票跑 L2 AI轻量标注"""
    from tagging.l2_ai_engine import run_l2_batch

    codes = stock_codes or _get_tier_b_codes()
    total = len(codes)

    if callback:
        callback("tier_b", 0, total, f"Tier B 开始：{total} 只股票（L2 AI轻量）")

    # L2 是按 stock_mentions 批量处理，不是按 stock_code 逐只
    result = run_l2_batch(limit=min(limit_per_run, 500))

    if callback:
        callback("tier_b", total, total,
                 f"Tier B 完成：处理 {result.get('processed', 0)} 条提及，标注 {result.get('tagged', 0)} 条")
    return {"tier": "B", "total": total, **result}


def run_tier_c(
    stock_codes: Optional[list[str]] = None,
    callback: Optional[Callable] = None,
) -> dict:
    """Tier C：重点股票跑 L3 AI深度分析"""
    from tagging.l3_deep_engine import run_l3_for_stock

    codes = stock_codes or _get_tier_c_codes()
    total = len(codes)
    done = errors = 0

    if callback:
        callback("tier_c", 0, total, f"Tier C 开始：{total} 只重点股票（L3 AI深度）")

    for code in codes:
        try:
            run_l3_for_stock(code)
            done += 1
        except Exception as e:
            errors += 1
            logger.warning(f"Tier C L3 error {code}: {e}")
        if callback:
            callback("tier_c", done, total, f"Tier C 进度 {done}/{total}")

    if callback:
        callback("tier_c", total, total, f"Tier C 完成：{done} 成功 {errors} 失败")
    return {"tier": "C", "total": total, "done": done, "errors": errors}


# ── 全量更新（A+B+C 串行） ────────────────────────────────────────────────────

def run_full_update(
    tiers: list[str] = None,
    callback: Optional[Callable] = None,
) -> dict:
    """全量标签更新，tiers 默认 ['A', 'B', 'C']"""
    tiers = tiers or ["A", "B", "C"]
    results = {}

    if "A" in tiers:
        results["A"] = run_tier_a(callback=callback)
    if "B" in tiers:
        results["B"] = run_tier_b(callback=callback)
    if "C" in tiers:
        results["C"] = run_tier_c(callback=callback)

    return results


# ── 后台任务管理 ──────────────────────────────────────────────────────────────

_batch_tasks: dict = {}
_task_lock = threading.Lock()


def start_batch_update_task(tiers: list[str] = None, stock_code: str = None) -> str:
    """启动后台批量更新任务，返回 task_id"""
    task_id = f"batch_tag_{int(datetime.now().timestamp())}"
    task = {
        "task_id": task_id,
        "status": "running",
        "tiers": tiers or ["A", "B", "C"],
        "stock_code": stock_code,
        "progress": [],
        "result": None,
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
    }
    with _task_lock:
        _batch_tasks[task_id] = task

    def _callback(stage, done, total, message):
        with _task_lock:
            t = _batch_tasks.get(task_id)
            if t:
                t["progress"].append({
                    "stage": stage, "done": done, "total": total, "message": message
                })
                # 只保留最近50条进度
                if len(t["progress"]) > 50:
                    t["progress"] = t["progress"][-50:]

    def _run():
        try:
            if stock_code:
                # 单只股票：跑所有层
                from tagging.l1_quant_engine import run_l1_for_stock
                from tagging.l2_ai_engine import run_l2_batch
                from tagging.l3_deep_engine import run_l3_for_stock
                _callback("tier_a", 0, 1, f"L1 量化计算: {stock_code}")
                run_l1_for_stock(stock_code)
                _callback("tier_a", 1, 1, "L1 完成")
                _callback("tier_b", 0, 1, f"L2 AI轻量: {stock_code}")
                run_l2_batch(limit=50)
                _callback("tier_b", 1, 1, "L2 完成")
                _callback("tier_c", 0, 1, f"L3 AI深度: {stock_code}")
                run_l3_for_stock(stock_code)
                _callback("tier_c", 1, 1, "L3 完成")
                result = {"stock_code": stock_code, "done": True}
            else:
                result = run_full_update(tiers=tiers, callback=_callback)

            with _task_lock:
                t = _batch_tasks.get(task_id)
                if t:
                    t["status"] = "done"
                    t["result"] = result
                    t["finished_at"] = datetime.now().isoformat()
        except Exception as e:
            logger.error(f"批量标签更新失败: {e}")
            with _task_lock:
                t = _batch_tasks.get(task_id)
                if t:
                    t["status"] = "error"
                    t["result"] = {"error": str(e)[:300]}
                    t["finished_at"] = datetime.now().isoformat()

    threading.Thread(target=_run, daemon=True).start()
    return task_id


def get_batch_task_status(task_id: str) -> Optional[dict]:
    with _task_lock:
        task = _batch_tasks.get(task_id)
        if not task:
            return None
        return dict(task)


def get_all_batch_tasks() -> list[dict]:
    with _task_lock:
        return sorted(_batch_tasks.values(), key=lambda t: t["started_at"], reverse=True)[:10]

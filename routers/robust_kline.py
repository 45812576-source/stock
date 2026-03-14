"""routers/robust_kline.py — Robust Kline 路由

GET  /robust-kline              → 页面
POST /robust-kline/api/scan     → 手动触发全流程扫描
GET  /robust-kline/api/mentions → 查询报告提及
GET  /robust-kline/api/candidates → 查询爸爸备选
"""
import logging
import threading
from datetime import date, datetime
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/robust-kline", tags=["robust_kline"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# ── 扫描状态追踪 ────────────────────────────────────────────────────
_scan_lock = threading.Lock()
_scan_status: dict = {"running": False, "last_result": None, "last_run": None}


# ── 全流程入口（供定时任务 + 手动触发） ──────────────────────────────

def run_robust_kline_scan(scan_date: date = None):
    """全流程：扫描 → 过滤 → 亮点填充"""
    if scan_date is None:
        scan_date = date.today()

    with _scan_lock:
        if _scan_status["running"]:
            logger.warning("[RobustKline] 扫描已在运行，跳过")
            return {"error": "already_running"}
        _scan_status["running"] = True
        _scan_status["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    result = {}
    try:
        logger.info(f"[RobustKline] 开始扫描 scan_date={scan_date}")

        from robust_kline.scanner import scan_today
        scan_result = scan_today(scan_date)
        result["scan"] = scan_result
        logger.info(f"[RobustKline] 扫描完成: {scan_result}")

        from robust_kline.filter import filter_candidates
        filter_result = filter_candidates(scan_date)
        result["filter"] = filter_result
        logger.info(f"[RobustKline] 过滤完成: {filter_result}")

        from robust_kline.highlight import fill_highlights
        highlight_result = fill_highlights(scan_date)
        result["highlight"] = highlight_result
        logger.info(f"[RobustKline] 亮点填充完成: {highlight_result}")

    except Exception as e:
        logger.exception(f"[RobustKline] 扫描失败: {e}")
        result["error"] = str(e)
    finally:
        with _scan_lock:
            _scan_status["running"] = False
            _scan_status["last_result"] = result

    return result


# ── 页面路由 ─────────────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
async def robust_kline_page(request: Request):
    # 默认展示今日数据
    today = str(date.today())
    return templates.TemplateResponse(
        "robust_kline.html",
        {"request": request, "today": today, "active_page": "robust_kline"},
    )


# ── API 路由 ─────────────────────────────────────────────────────────

@router.post("/api/scan")
async def api_scan(background_tasks: BackgroundTasks):
    """手动触发全流程扫描（后台执行）"""
    with _scan_lock:
        if _scan_status["running"]:
            return JSONResponse({"ok": False, "msg": "扫描正在进行中，请稍后"})

    background_tasks.add_task(run_robust_kline_scan)
    return JSONResponse({"ok": True, "msg": "扫描已启动，请稍后刷新查看结果"})


@router.get("/api/scan-status")
async def api_scan_status():
    """获取扫描状态"""
    return JSONResponse({
        "running": _scan_status["running"],
        "last_run": _scan_status["last_run"],
        "last_result": _scan_status["last_result"],
    })


@router.get("/api/mentions")
async def api_mentions():
    """查询报告提及（返回全部日期，前端自行筛选）"""
    rows = execute_query(
        """SELECT id, scan_date, stock_name, stock_code, industry, theme,
                  source_title, highlight
           FROM robust_kline_mentions
           ORDER BY scan_date DESC, industry, stock_name
           LIMIT 2000""",
    )
    items = []
    for r in (rows or []):
        items.append({
            "id": r["id"],
            "scan_date": str(r["scan_date"]) if r.get("scan_date") else "",
            "stock_name": r.get("stock_name") or "",
            "stock_code": r.get("stock_code") or "",
            "industry": r.get("industry") or "",
            "theme": r.get("theme") or "",
            "source_title": r.get("source_title") or "",
            "highlight": r.get("highlight") or "",
        })
    return JSONResponse({"total": len(items), "items": items})


@router.get("/api/candidates")
async def api_candidates():
    """查询爸爸备选（按 stock_code+match_type 去重，保留最新日期，汇总出现日期）"""
    rows = execute_query(
        """SELECT id, scan_date, stock_code, stock_name, industry,
                  match_type, yang_months, gain_pct, latest_price,
                  mention_count, highlight
           FROM robust_kline_candidates
           ORDER BY scan_date DESC, match_type ASC, mention_count DESC
           LIMIT 1000""",
    )

    # 按 (stock_code, match_type) 去重：保留最新日期，汇总 scan_dates 和 mention_count
    seen: dict = {}  # key = (stock_code, match_type)
    for r in (rows or []):
        key = (r.get("stock_code"), r.get("match_type"))
        sd = str(r["scan_date"]) if r.get("scan_date") else ""
        if key not in seen:
            seen[key] = {
                "id": r["id"],
                "scan_date": sd,           # 最新日期（已按 DESC 排序，首次即最新）
                "scan_dates": [sd] if sd else [],
                "stock_code": r.get("stock_code") or "",
                "stock_name": r.get("stock_name") or "",
                "industry": r.get("industry") or "",
                "match_type": r.get("match_type"),
                "match_label": {1: "连续3月阳线", 2: "4月内3月阳线", 3: "2月+3周阳"}.get(r.get("match_type"), ""),
                "yang_months": r.get("yang_months") or "",
                "gain_pct": r.get("gain_pct"),
                "latest_price": r.get("latest_price"),
                "mention_count": r.get("mention_count", 1),
                "highlight": r.get("highlight") or "",
            }
        else:
            # 追加出现日期、累加提及次数
            if sd and sd not in seen[key]["scan_dates"]:
                seen[key]["scan_dates"].append(sd)
            seen[key]["mention_count"] = (seen[key]["mention_count"] or 0) + (r.get("mention_count") or 1)

    items = list(seen.values())
    # scan_dates 排序后转字符串展示
    for item in items:
        item["scan_dates_str"] = "、".join(sorted(item["scan_dates"]))
        del item["scan_dates"]

    return JSONResponse({"total": len(items), "items": items})


@router.get("/api/dates")
async def api_dates():
    """获取有数据的日期列表"""
    rows = execute_query(
        """SELECT DISTINCT scan_date FROM robust_kline_mentions
           ORDER BY scan_date DESC LIMIT 30"""
    )
    dates = [str(r["scan_date"]) for r in (rows or [])]
    return JSONResponse({"dates": dates})

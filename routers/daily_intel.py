"""routers/daily_intel.py — 每日情报路由

GET  /daily-intel                → 页面
GET  /daily-intel/api/stocks     → 情报列表（date/event_type/industry 筛选）
GET  /daily-intel/api/candidates → 爸爸备选
POST /daily-intel/api/manual     → 手动录入 {text, date?}
POST /daily-intel/api/scan       → 手动触发全流程扫描
GET  /daily-intel/api/scan-status→ 扫描状态
GET  /daily-intel/api/kline/{code}   → 月K(6月)+周K(3周) OHLCV
GET  /daily-intel/api/capital/{code} → 近15天资金流
GET  /daily-intel/api/etf/{code}     → ETF 持有情况
GET  /daily-intel/api/dates      → 有数据的日期列表
"""
import logging
import threading
from datetime import date, datetime
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from utils.db_utils import execute_query, execute_insert, execute_cloud_query, execute_cloud_insert

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/daily-intel", tags=["daily_intel"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# ── 扫描状态追踪 ─────────────────────────────────────────────────

_scan_lock = threading.Lock()
_scan_status: dict = {"running": False, "last_result": None, "last_run": None}

# ── 当天首次触发追踪（chain_sync + theme_merger）──────────────────
_daily_sync_last_date: str = ""


def _trigger_daily_sync_if_needed():
    """当天首次打开页面时，后台并发运行 chain_sync + theme_merger"""
    global _daily_sync_last_date
    today = str(date.today())
    if _daily_sync_last_date == today:
        return
    _daily_sync_last_date = today

    def _run():
        try:
            from config.chain_sync import run_chain_sync
            run_chain_sync(scan_date=today)
        except Exception as e:
            logger.warning(f"[ChainSync] 后台同步失败: {e}")
        try:
            from daily_intel.theme_merger import run_theme_merge
            run_theme_merge(scan_date=today)
        except Exception as e:
            logger.warning(f"[ThemeMerger] 后台归纳失败: {e}")

    threading.Thread(target=_run, daemon=True).start()
    logger.info("[DailySync] 已在后台触发当天首次同步（chain_sync + theme_merger）")


def run_daily_intel_scan(scan_date: date = None):
    """全流程入口（供定时任务 + 手动触发）"""
    if scan_date is None:
        scan_date = date.today()

    with _scan_lock:
        if _scan_status["running"]:
            logger.warning("[DailyIntel] 扫描已在运行，跳过")
            return {"error": "already_running"}
        _scan_status["running"] = True
        _scan_status["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    result = {}
    try:
        logger.info(f"[DailyIntel] 开始扫描 scan_date={scan_date}")
        from daily_intel.scanner import run_daily_intel_pipeline
        result = run_daily_intel_pipeline(scan_date)
    except Exception as e:
        logger.exception(f"[DailyIntel] 扫描失败: {e}")
        result["error"] = str(e)
    finally:
        with _scan_lock:
            _scan_status["running"] = False
            _scan_status["last_result"] = result

    return result


# ── 页面路由 ─────────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
async def daily_intel_page(request: Request):
    today = str(date.today())
    _trigger_daily_sync_if_needed()
    return templates.TemplateResponse(
        "daily_intel.html",
        {"request": request, "today": today, "active_page": "daily_intel"},
    )


# ── API: 情报列表 ─────────────────────────────────────────────────

@router.get("/api/stocks")
async def api_stocks(
    scan_date: str = "",
    event_type: str = "",
    industry: str = "",
    stock: str = "",
):
    """查询 daily_intel_stocks（支持 date/event_type/industry/stock 筛选）"""
    where = ["1=1"]
    params = []

    if scan_date:
        where.append("scan_date = %s")
        params.append(scan_date)
    if event_type:
        where.append("event_type = %s")
        params.append(event_type)
    if industry:
        where.append("industry = %s")
        params.append(industry)
    if stock:
        where.append("(stock_name LIKE %s OR stock_code LIKE %s)")
        params += [f"%{stock}%", f"%{stock}%"]

    sql = f"""
        SELECT id, scan_date, source_type, source_title,
               stock_name, stock_code, industry, business_desc,
               event_type, event_summary, created_at
        FROM daily_intel_stocks
        WHERE {' AND '.join(where)}
        ORDER BY scan_date DESC, id DESC
        LIMIT 2000
    """
    rows = execute_cloud_query(sql, params) or []
    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "scan_date": str(r["scan_date"]) if r.get("scan_date") else "",
            "source_type": r.get("source_type") or "",
            "source_title": r.get("source_title") or "",
            "stock_name": r.get("stock_name") or "",
            "stock_code": r.get("stock_code") or "",
            "industry": r.get("industry") or "",
            "business_desc": r.get("business_desc") or "",
            "event_type": r.get("event_type") or "",
            "event_summary": r.get("event_summary") or "",
        })
    return JSONResponse({"total": len(items), "items": items})


# ── API: 爸爸备选 ─────────────────────────────────────────────────

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

    seen: dict = {}
    for r in (rows or []):
        key = (r.get("stock_code"), r.get("match_type"))
        sd = str(r["scan_date"]) if r.get("scan_date") else ""
        if key not in seen:
            seen[key] = {
                "id": r["id"],
                "scan_date": sd,
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
            if sd and sd not in seen[key]["scan_dates"]:
                seen[key]["scan_dates"].append(sd)
            seen[key]["mention_count"] = (seen[key]["mention_count"] or 0) + (r.get("mention_count") or 1)

    items = list(seen.values())
    for item in items:
        item["scan_dates_str"] = "、".join(sorted(item["scan_dates"]))
        del item["scan_dates"]

    return JSONResponse({"total": len(items), "items": items})


# ── API: 手动录入 ─────────────────────────────────────────────────

@router.post("/api/manual")
async def api_manual(request: Request, background_tasks: BackgroundTasks):
    """手动录入文本 → 写 daily_intel_items(pending) → 后台处理"""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "msg": "请求格式错误"}, status_code=400)

    text = (body.get("text") or "").strip()
    if not text:
        return JSONResponse({"ok": False, "msg": "文本不能为空"}, status_code=400)

    input_date = body.get("date") or str(date.today())

    try:
        # 确保表存在
        from daily_intel.scanner import _ensure_tables
        _ensure_tables()
        execute_cloud_insert(
            "INSERT INTO daily_intel_items (input_text, input_date, process_status) VALUES (%s, %s, 'pending')",
            [text, input_date],
        )
    except Exception as e:
        logger.error(f"[DailyIntel] 手动录入写入失败: {e}")
        return JSONResponse({"ok": False, "msg": str(e)}, status_code=500)

    # 后台立即处理
    background_tasks.add_task(_process_manual_bg)
    return JSONResponse({"ok": True, "msg": "已录入，正在后台处理..."})


def _process_manual_bg():
    """后台处理手动录入（不阻塞主流程）"""
    try:
        from daily_intel.scanner import process_manual_items
        process_manual_items()
    except Exception as e:
        logger.error(f"[DailyIntel] 后台处理手动录入失败: {e}")


# ── API: 扫描触发 / 状态 ──────────────────────────────────────────

@router.post("/api/scan")
async def api_scan(background_tasks: BackgroundTasks):
    """手动触发全流程扫描（后台执行）"""
    with _scan_lock:
        if _scan_status["running"]:
            return JSONResponse({"ok": False, "msg": "扫描正在进行中，请稍后"})
    background_tasks.add_task(run_daily_intel_scan)
    return JSONResponse({"ok": True, "msg": "扫描已启动，请稍后刷新查看结果"})


@router.get("/api/scan-status")
async def api_scan_status():
    return JSONResponse({
        "running": _scan_status["running"],
        "last_run": _scan_status["last_run"],
        "last_result": _scan_status["last_result"],
    })


# ── API: K线数据 ──────────────────────────────────────────────────

@router.get("/api/kline/{code}")
async def api_kline(code: str):
    """月K(6月)+周K(3周) OHLCV 数据"""
    from stock_selector.kline_calc import _fetch_daily, _resample_monthly, _resample_weekly

    daily_map = _fetch_daily([code], days=280)
    daily = daily_map.get(code, [])
    if not daily:
        return JSONResponse({"monthly": [], "weekly": []})

    monthly = _resample_monthly(daily)
    weekly = _resample_weekly(daily)

    # 取最近6个完整月（去掉当月未完成的最后一根）
    m_bars = monthly[:-1] if len(monthly) > 1 else monthly
    m_bars = m_bars[-6:]

    # 取最近3个完整周
    w_bars = weekly[:-1] if len(weekly) > 1 else weekly
    w_bars = w_bars[-3:]

    def fmt_monthly(bars):
        return [{
            "label": b["ym"][:4] + "-" + b["ym"][4:],
            "open": b["open"], "high": b["high"],
            "low": b["low"], "close": b["close"],
            "yang": b["close"] > b["open"],
        } for b in bars]

    def fmt_weekly(bars):
        return [{
            "label": b["wk"],
            "open": b["open"], "high": b["high"],
            "low": b["low"], "close": b["close"],
            "yang": b["close"] > b["open"],
        } for b in bars]

    return JSONResponse({
        "monthly": fmt_monthly(m_bars),
        "weekly": fmt_weekly(w_bars),
    })


# ── API: 资金流向 ─────────────────────────────────────────────────

@router.get("/api/capital/{code}")
async def api_capital(code: str):
    """近15天资金流（从本地 capital_flow 表）"""
    rows = execute_query(
        """SELECT trade_date, main_net_inflow, big_net_inflow, medium_net_inflow, small_net_inflow
           FROM capital_flow
           WHERE stock_code = %s
           ORDER BY trade_date DESC
           LIMIT 15""",
        [code],
    ) or []
    items = []
    for r in reversed(rows):
        items.append({
            "trade_date": str(r["trade_date"]) if r.get("trade_date") else "",
            "main_net": float(r.get("main_net_inflow") or 0),
            "big_net": float(r.get("big_net_inflow") or 0),
            "medium_net": float(r.get("medium_net_inflow") or 0),
            "small_net": float(r.get("small_net_inflow") or 0),
        })
    return JSONResponse({"items": items})


# ── API: ETF 持有 ─────────────────────────────────────────────────

@router.get("/api/etf/{code}")
async def api_etf(code: str):
    """ETF 持有情况（从本地 etf_constituent 表）"""
    rows = execute_query(
        """SELECT etf_code, etf_name, weight, shares, amount, report_date
           FROM etf_constituent
           WHERE stock_code = %s
           ORDER BY weight DESC
           LIMIT 20""",
        [code],
    ) or []
    items = []
    for r in rows:
        items.append({
            "etf_code": r.get("etf_code") or "",
            "etf_name": r.get("etf_name") or "",
            "weight": float(r.get("weight") or 0),
            "shares": float(r.get("shares") or 0),
            "amount": float(r.get("amount") or 0),
            "report_date": str(r["report_date"]) if r.get("report_date") else "",
        })
    return JSONResponse({"items": items})


# ── API: 有数据的日期列表 ─────────────────────────────────────────

@router.get("/api/dates")
async def api_dates():
    rows = execute_cloud_query(
        """SELECT DISTINCT scan_date FROM daily_intel_stocks
           ORDER BY scan_date DESC LIMIT 30"""
    )
    dates = [str(r["scan_date"]) for r in (rows or [])]
    return JSONResponse({"dates": dates})

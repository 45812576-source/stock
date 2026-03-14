"""产业链全景模块 — routers/chain.py"""
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from utils.db_utils import execute_query, execute_insert, execute_cloud_query, cloud_stockdb_query, cloud_stockanalysis_query
from config.chain_config import CHAINS, CHAIN_ORDER

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/chain", tags=["chain"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# ── 简易内存缓存（ETF数据） ─────────────────────────────────────
_etf_cache: dict = {}
ETF_CACHE_TTL = 600  # 10 分钟


def _cached_etf(key, fetcher):
    now = time.time()
    if key in _etf_cache and now - _etf_cache[key]["ts"] < ETF_CACHE_TTL:
        return _etf_cache[key]["data"]
    try:
        data = fetcher()
        _etf_cache[key] = {"data": data, "ts": now}
        return data
    except Exception as e:
        logger.warning(f"ETF缓存获取失败 [{key}]: {e}")
        return _etf_cache.get(key, {}).get("data")


def _resolve_stock_codes(names: list[str]) -> dict:
    """根据公司名称批量查股票代码，返回 {name: code}"""
    if not names:
        return {}
    ph = ",".join(["%s"] * len(names))
    rows = execute_query(
        f"SELECT stock_code, stock_name FROM stock_info WHERE stock_name IN ({ph})",
        names,
    ) or []
    return {r["stock_name"]: r["stock_code"] for r in rows}


def _get_all_chain_codes() -> dict:
    """一次性拿到所有产业链股票代码，返回 {chain_name: {tier: [code, ...]}}"""
    all_names = set()
    for chain in CHAINS.values():
        for tier in chain["tiers"].values():
            all_names.update(tier["stocks"])

    name_to_code = _resolve_stock_codes(list(all_names))

    result = {}
    for chain_name, chain in CHAINS.items():
        result[chain_name] = {}
        for tier_key, tier in chain["tiers"].items():
            result[chain_name][tier_key] = [
                name_to_code[n] for n in tier["stocks"] if n in name_to_code
            ]
    return result


# ── 页面入口 ──────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
def chain_page(request: Request):
    return templates.TemplateResponse("chain.html", {
        "request": request,
        "active_page": "chain",
    })


# ── API: 可用交易日列表 ───────────────────────────────────────────

@router.get("/api/available-dates")
def api_available_dates():
    """返回 fund_flow_history 最近60个有效交易日"""
    rows = cloud_stockdb_query(
        "SELECT DISTINCT trade_date FROM fund_flow_history "
        "ORDER BY trade_date DESC LIMIT 60"
    ) or []
    dates = [str(r["trade_date"]) for r in rows]
    return {"ok": True, "dates": dates, "latest": dates[0] if dates else ""}


# ── API: 所有产业链概览 ─────────────────────────────────────────

@router.get("/api/list")
def api_chain_list(date: str = ""):
    """返回所有产业链的概览信息，含 tiers_summary，按指定日期资金流排序"""
    chain_codes = _get_all_chain_codes()

    # 确定查询日期（默认最新）
    if date:
        query_date = date
    else:
        row = cloud_stockdb_query(
            "SELECT MAX(trade_date) AS mx FROM fund_flow_history"
        ) or []
        query_date = str(row[0]["mx"]) if row and row[0]["mx"] else None

    # 一次性批量查所有股票在该日期的资金流
    all_codes_set = set()
    chain_codes_map = {}
    for chain_name in CHAIN_ORDER:
        if chain_name not in CHAINS:
            continue
        codes = []
        for tier_codes in chain_codes.get(chain_name, {}).values():
            codes.extend(tier_codes)
        codes = list(set(codes))
        chain_codes_map[chain_name] = codes
        all_codes_set.update(codes)

    # 按天+股票查资金流明细
    window_dates = []
    flow_days = 0
    # {symbol: {date_str: flow_yi}}
    flow_by_code_date: dict = {}
    if all_codes_set and query_date:
        all_codes_list = list(all_codes_set)
        ph = ",".join(["%s"] * len(all_codes_list))
        date_rows = cloud_stockdb_query(
            """SELECT DISTINCT trade_date FROM fund_flow_history
               WHERE trade_date <= %s ORDER BY trade_date DESC LIMIT 7""",
            [query_date],
        ) or []
        # 按时间正序排列
        window_dates = sorted([r["trade_date"] for r in date_rows])
        flow_days = len(window_dates)
        if window_dates:
            dp = ",".join(["%s"] * len(window_dates))
            flow_rows = cloud_stockdb_query(
                f"""SELECT symbol, trade_date, main_net_inflow
                    FROM fund_flow_history
                    WHERE symbol IN ({ph}) AND trade_date IN ({dp})""",
                all_codes_list + window_dates,
            ) or []
            for r in flow_rows:
                sym = r["symbol"]
                dt = str(r["trade_date"])
                flow_by_code_date.setdefault(sym, {})[dt] = float(r["main_net_inflow"] or 0)

    result = []
    for chain_name in CHAIN_ORDER:
        if chain_name not in CHAINS:
            continue
        chain = CHAINS[chain_name]
        codes = chain_codes_map.get(chain_name, [])

        # 按天汇总（万元转亿元）
        daily_flows = []
        for d in window_dates:
            day_total = sum(
                flow_by_code_date.get(c, {}).get(str(d), 0) for c in codes
            ) / 10000
            daily_flows.append(round(day_total, 2))

        total_flow = round(sum(daily_flows), 2)

        tiers_summary = [
            {"key": tier_key, "label": tier_def["label"]}
            for tier_key, tier_def in chain["tiers"].items()
        ]

        result.append({
            "name": chain_name,
            "icon": chain["icon"],
            "color": chain["color"],
            "stock_count": len(codes),
            "today_flow": total_flow,
            "daily_flows": daily_flows,
            "daily_dates": [str(d)[5:] for d in window_dates],  # MM-DD
            "tier_count": len(chain["tiers"]),
            "tiers_summary": tiers_summary,
        })

    result.sort(key=lambda x: x["today_flow"], reverse=True)

    return {"ok": True, "chains": result, "query_date": query_date or "", "flow_days": flow_days}


# ── API: 产业链详情（旧接口，保留兼容） ──────────────────────────

@router.get("/api/detail")
def api_chain_detail(name: str, days: int = 15):
    """返回某产业链的详细数据（按层级分组的股票 + 资金流热力图 + 最新行情）"""
    if name not in CHAINS:
        return JSONResponse({"ok": False, "error": f"产业链不存在: {name}"})

    days = max(7, min(30, days))
    chain = CHAINS[name]

    all_names = []
    for tier in chain["tiers"].values():
        all_names.extend(tier["stocks"])
    name_to_code = _resolve_stock_codes(all_names)
    code_to_name = {v: k for k, v in name_to_code.items()}

    all_codes = list(set(name_to_code.values()))
    if not all_codes:
        return {"ok": True, "name": name, "tiers": [], "dates": [], "message": "无匹配股票"}

    date_rows = execute_query(
        "SELECT DISTINCT trade_date FROM capital_flow "
        "WHERE LENGTH(stock_code)=6 AND main_net_inflow != 0 "
        "ORDER BY trade_date DESC LIMIT %s",
        [days],
    ) or []
    if not date_rows:
        return {"ok": True, "name": name, "tiers": [], "dates": [], "message": "暂无资金流数据"}

    dates = sorted([r["trade_date"] for r in date_rows])
    codes_ph = ",".join(["%s"] * len(all_codes))
    dates_ph = ",".join(["%s"] * len(dates))

    cf_rows = execute_query(
        f"""SELECT stock_code, trade_date,
                   ROUND(main_net_inflow / 10000) AS net_wan
            FROM capital_flow
            WHERE stock_code IN ({codes_ph}) AND trade_date IN ({dates_ph})""",
        all_codes + dates,
    ) or []

    sd_rows = execute_query(
        f"""SELECT stock_code, trade_date, change_pct,
                   ROUND(amount / NULLIF(turnover_rate, 0) / 1e8) AS cap_yi
            FROM stock_daily
            WHERE stock_code IN ({codes_ph}) AND trade_date IN ({dates_ph})""",
        all_codes + dates,
    ) or []

    info_rows = execute_query(
        f"SELECT stock_code, stock_name, market_cap FROM stock_info WHERE stock_code IN ({codes_ph})",
        all_codes,
    ) or []
    info_map = {r["stock_code"]: r for r in info_rows}

    cf_map: dict = {}
    for r in cf_rows:
        cf_map.setdefault(r["stock_code"], {})[r["trade_date"]] = int(r["net_wan"] or 0)

    sd_map: dict = {}
    for r in sd_rows:
        sd_map.setdefault(r["stock_code"], {})[r["trade_date"]] = {
            "pct": float(r["change_pct"] or 0),
            "cap": float(r["cap_yi"] or 0),
        }

    def build_stock(code):
        info = info_map.get(code, {})
        name_str = info.get("stock_name") or code_to_name.get(code, code)
        flow = [cf_map.get(code, {}).get(d, 0) for d in dates]
        pct = [sd_map.get(code, {}).get(d, {}).get("pct", None) for d in dates]
        cap = [sd_map.get(code, {}).get(d, {}).get("cap", None) for d in dates]
        total_flow = sum(flow)
        si_cap = float(info.get("market_cap") or 0) / 1e8 if info.get("market_cap") else None
        latest_cap = si_cap or next((v for v in reversed(cap) if v is not None), None)
        latest_pct = next((v for v in reversed(pct) if v is not None), None)
        return {
            "code": code,
            "name": name_str,
            "flow": flow,
            "pct": pct,
            "cap": cap,
            "total_flow": total_flow,
            "latest_cap": round(latest_cap, 1) if latest_cap else None,
            "latest_pct": round(latest_pct, 2) if latest_pct is not None else None,
        }

    tiers_out = []
    for tier_key, tier_def in chain["tiers"].items():
        codes_in_tier = [name_to_code[n] for n in tier_def["stocks"] if n in name_to_code]
        stocks = [build_stock(c) for c in codes_in_tier]
        stocks = [s for s in stocks if s["flow"] and max(abs(v) for v in s["flow"]) >= 1]
        stocks.sort(key=lambda s: abs(s["total_flow"]), reverse=True)
        tier_flow = sum(s["total_flow"] for s in stocks)
        tiers_out.append({
            "key": tier_key,
            "label": tier_def["label"],
            "stocks": stocks,
            "tier_flow": tier_flow,
        })

    return {
        "ok": True,
        "name": name,
        "color": chain["color"],
        "dates": dates,
        "tiers": tiers_out,
    }


# ── API: 产业链明细页数据（新版） ────────────────────────────────

@router.get("/api/detail-v2")
def api_chain_detail_v2(name: str):
    """明细页数据：15日逐日资金流 + 各环节股票概览"""
    if name not in CHAINS:
        return JSONResponse({"ok": False, "error": f"产业链不存在: {name}"})

    chain = CHAINS[name]

    # 收集所有股票名称 & 代码
    all_names = []
    for tier in chain["tiers"].values():
        all_names.extend(tier["stocks"])
    name_to_code = _resolve_stock_codes(all_names)
    code_to_name = {v: k for k, v in name_to_code.items()}
    all_codes = list(set(name_to_code.values()))

    if not all_codes:
        return {"ok": True, "name": name, "flow_dates": [], "flow_total": [], "tiers": []}

    codes_ph = ",".join(["%s"] * len(all_codes))

    # 最近15个有效交易日（云端 fund_flow_history）
    date_rows = cloud_stockdb_query(
        f"""SELECT DISTINCT trade_date FROM fund_flow_history
            WHERE symbol IN ({codes_ph})
            ORDER BY trade_date DESC LIMIT 15""",
        all_codes,
    ) or []
    dates = sorted([r["trade_date"] for r in date_rows])

    # 各股资金流（15日，云端，main_net_inflow 单位：万元）
    cf_map: dict = {}
    if dates:
        dates_ph = ",".join(["%s"] * len(dates))
        cf_rows = cloud_stockdb_query(
            f"""SELECT symbol, trade_date, main_net_inflow
                FROM fund_flow_history
                WHERE symbol IN ({codes_ph}) AND trade_date IN ({dates_ph})""",
            all_codes + dates,
        ) or []
        for r in cf_rows:
            # 万元 → 亿元，key 统一用字符串日期
            cf_map.setdefault(r["symbol"], {})[str(r["trade_date"])] = float(r["main_net_inflow"] or 0) / 10000

    dates_str = [str(d) for d in dates]

    # 全链逐日汇总
    flow_total = []
    for d in dates_str:
        total = sum(cf_map.get(code, {}).get(d, 0) for code in all_codes)
        flow_total.append(round(total, 2))

    # 最新一天行情（本地 stock_daily）
    latest_sd_map: dict = {}
    latest_trade = execute_query(
        f"SELECT MAX(trade_date) AS mx FROM stock_daily WHERE stock_code IN ({codes_ph})",
        all_codes,
    ) or []
    latest_trade_date = latest_trade[0]["mx"] if latest_trade else None

    sd_rows = []
    if latest_trade_date:
        sd_rows = execute_query(
            f"""SELECT stock_code, close AS latest_price, change_pct AS latest_pct
                FROM stock_daily
                WHERE stock_code IN ({codes_ph}) AND trade_date = %s""",
            all_codes + [latest_trade_date],
        ) or []
    for r in sd_rows:
        latest_sd_map[r["stock_code"]] = {
            "latest_price": float(r["latest_price"] or 0),
            "latest_pct": float(r["latest_pct"] or 0),
        }

    # 股票基础信息
    info_rows = execute_query(
        f"SELECT stock_code, stock_name, market_cap FROM stock_info WHERE stock_code IN ({codes_ph})",
        all_codes,
    ) or []
    info_map = {r["stock_code"]: r for r in info_rows}

    from config.chain_config import STOCK_TAGS

    def build_stock_v2(code):
        info = info_map.get(code, {})
        sd = latest_sd_map.get(code, {})
        name_str = info.get("stock_name") or code_to_name.get(code, code)
        mc = info.get("market_cap")
        market_cap_yi = round(float(mc) / 1e8, 1) if mc else None
        return {
            "code": code,
            "name": name_str,
            "latest_price": sd.get("latest_price"),
            "latest_pct": sd.get("latest_pct"),
            "market_cap": market_cap_yi,
            "tag": STOCK_TAGS.get(name_str, ""),
        }

    # 按环节组织
    tiers_out = []
    for tier_key, tier_def in chain["tiers"].items():
        codes_in_tier = [name_to_code[n] for n in tier_def["stocks"] if n in name_to_code]
        stocks = [build_stock_v2(c) for c in codes_in_tier]

        # 该环节15日资金流汇总
        tier_flow_15d = round(sum(
            sum(cf_map.get(c, {}).get(d, 0) for d in dates_str)
            for c in codes_in_tier
        ), 2)

        tiers_out.append({
            "key": tier_key,
            "label": tier_def["label"],
            "tier_flow_15d": tier_flow_15d,
            "stocks": stocks,
        })

    return {
        "ok": True,
        "name": name,
        "icon": chain["icon"],
        "color": chain["color"],
        "flow_dates": dates_str,
        "flow_total": flow_total,
        "tiers": tiers_out,
    }


# ── API: 批量K线（月K + 周K 缩略图） ─────────────────────────────

@router.post("/api/batch-kline")
def api_batch_kline(body: dict):
    """批量返回月K(近6完整月) + 周K(近4完整周)"""
    from stock_selector.kline_calc import _fetch_daily, _resample_monthly, _resample_weekly
    from datetime import date

    codes = body.get("codes", [])
    if not codes:
        return {"ok": True, "data": {}}

    # 拉取约300天日线（本地库）
    daily_map = _fetch_daily(codes, days=300)

    today = date.today()
    current_ym = today.strftime("%Y%m")
    # 当周（按 %Y-W%W 格式）
    from datetime import datetime as _dt
    current_wk = _dt.now().strftime("%Y-W%W")

    result = {}
    for code, daily in daily_map.items():
        monthly_all = _resample_monthly(daily)
        weekly_all = _resample_weekly(daily)

        # 去掉当月，取最近6个完整月
        monthly = [b for b in monthly_all if b["ym"] != current_ym][-6:]

        # 去掉当周，取最近4个完整周
        weekly = [b for b in weekly_all if b["wk"] != current_wk][-4:]

        def fmt_bars(bars, key):
            out = []
            for b in bars:
                o, h, l, c = (
                    float(b["open"] or 0),
                    float(b["high"] or 0),
                    float(b["low"] or 0),
                    float(b["close"] or 0),
                )
                out.append({
                    "open": o, "high": h, "low": l, "close": c,
                    "yang": c >= o,
                    "label": b[key],
                })
            return out

        result[code] = {
            "monthly": fmt_bars(monthly, "ym"),
            "weekly": fmt_bars(weekly, "wk"),
        }

    return {"ok": True, "data": result}


# ── API: 批量涨幅（1周/1月/3月） ──────────────────────────────────

@router.post("/api/batch-perf")
def api_batch_perf(body: dict):
    """批量计算 1W/1M/3M 涨幅"""
    codes = body.get("codes", [])
    if not codes:
        return {"ok": True, "data": {}}

    from datetime import date, timedelta
    today = date.today()
    since = today - timedelta(days=95)  # 拉取约95天足够覆盖3个月

    codes_ph = ",".join(["%s"] * len(codes))
    rows = execute_query(
        f"""SELECT stock_code, trade_date, close
            FROM stock_daily
            WHERE stock_code IN ({codes_ph})
              AND trade_date >= %s
            ORDER BY stock_code, trade_date ASC""",
        codes + [str(since)],
    ) or []

    # 按 code 分组
    from collections import defaultdict
    daily_map = defaultdict(list)
    for r in rows:
        daily_map[r["stock_code"]].append((r["trade_date"], float(r["close"] or 0)))

    def pct_change(bars, days):
        if len(bars) < 2:
            return None
        latest_price = bars[-1][1]
        # 找到 N 个交易日前的收盘价
        ref_date = today - timedelta(days=days)
        # 取 ref_date 之后最近一条
        past = [b for b in bars if b[0] <= ref_date]
        if not past:
            past = [bars[0]]
        ref_price = past[-1][1]
        if ref_price == 0:
            return None
        return round((latest_price - ref_price) / ref_price * 100, 2)

    result = {}
    for code, bars in daily_map.items():
        result[code] = {
            "pct_1w": pct_change(bars, 7),
            "pct_1m": pct_change(bars, 30),
            "pct_3m": pct_change(bars, 91),
        }

    return {"ok": True, "data": result}


# ── API: 产业链相关新闻 ───────────────────────────────────────────

@router.get("/api/news")
def api_chain_news(name: str):
    """从 daily_intel_stocks 查产业链相关新闻（最近10条）"""
    if name not in CHAINS:
        return JSONResponse({"ok": False, "error": f"产业链不存在: {name}"})

    chain = CHAINS[name]
    all_names = []
    for tier in chain["tiers"].values():
        all_names.extend(tier["stocks"])
    name_to_code = _resolve_stock_codes(all_names)
    all_codes = list(set(name_to_code.values()))

    if not all_codes:
        return {"ok": True, "news": []}

    ph = ",".join(["%s"] * len(all_codes))
    rows = execute_cloud_query(
        f"""SELECT scan_date, stock_name, stock_code, event_type, event_summary, source_title
            FROM daily_intel_stocks
            WHERE stock_code IN ({ph})
            ORDER BY scan_date DESC, id DESC
            LIMIT 10""",
        all_codes,
    ) or []

    news = []
    for r in rows:
        news.append({
            "scan_date": str(r["scan_date"])[:10],
            "stock_name": r["stock_name"],
            "stock_code": r["stock_code"],
            "event_type": r["event_type"],
            "event_summary": r["event_summary"],
            "source_title": r["source_title"],
        })

    return {"ok": True, "name": name, "news": news}


# ── API: ETF持仓查询 ──────────────────────────────────────────

@router.get("/api/etf")
def api_chain_etf(name: str):
    """返回该产业链各股被基金持有的情况（最新季度，etf_constituent）"""
    if name not in CHAINS:
        return JSONResponse({"ok": False, "error": f"产业链不存在: {name}"})

    chain = CHAINS[name]
    all_names = []
    for tier in chain["tiers"].values():
        all_names.extend(tier["stocks"])
    name_to_code = _resolve_stock_codes(all_names)
    all_codes = list(set(name_to_code.values()))

    if not all_codes:
        return {"ok": True, "report_date": "", "stocks": []}

    ph = ",".join(["%s"] * len(all_codes))

    # 最新季度各股汇总持仓
    rows = cloud_stockanalysis_query(
        f"""SELECT stock_code, stock_name,
                   COUNT(DISTINCT etf_code) AS etf_count,
                   ROUND(SUM(shares), 1) AS total_shares
            FROM etf_constituent
            WHERE stock_code IN ({ph})
              AND report_date = (SELECT MAX(report_date) FROM etf_constituent)
            GROUP BY stock_code, stock_name
            ORDER BY total_shares DESC""",
        all_codes,
    ) or []

    # 最新 report_date
    date_row = cloud_stockanalysis_query(
        "SELECT MAX(report_date) AS mx FROM etf_constituent"
    ) or []
    report_date = str(date_row[0]["mx"]) if date_row and date_row[0]["mx"] else ""

    stocks = [
        {
            "code": r["stock_code"],
            "name": r["stock_name"],
            "etf_count": int(r["etf_count"]),
            "total_shares": float(r["total_shares"] or 0),
        }
        for r in rows
    ]

    return {"ok": True, "name": name, "report_date": report_date, "stocks": stocks}


# ── API: 资金流汇总 ───────────────────────────────────────────────

@router.get("/api/flow-summary")
def api_chain_flow_summary(name: str, days: int = 15):
    """返回某产业链按层级汇总的资金流入"""
    if name not in CHAINS:
        return JSONResponse({"ok": False, "error": f"产业链不存在: {name}"})

    detail = api_chain_detail(name=name, days=days)
    if not detail.get("ok"):
        return detail

    summary = []
    for tier in detail["tiers"]:
        summary.append({
            "tier": tier["key"],
            "label": tier["label"],
            "tier_flow": tier["tier_flow"],
            "stock_count": len(tier["stocks"]),
        })

    return {
        "ok": True,
        "name": name,
        "dates": detail["dates"],
        "summary": summary,
    }


# ── API: 待观察 — 添加 ─────────────────────────────────────────────

@router.post("/api/watchlist/add")
def api_chain_watchlist_add(body: dict):
    """添加股票到待观察（需指定 chain_name + tier_key）"""
    stock_code = (body.get("stock_code") or "").strip()
    stock_name = (body.get("stock_name") or "").strip()
    industry = (body.get("industry") or "").strip()
    chain_name = (body.get("chain_name") or "").strip()
    tier_key = (body.get("tier_key") or "").strip()
    notes = (body.get("notes") or "").strip()
    if not stock_code or not chain_name or not tier_key:
        return JSONResponse({"ok": False, "error": "需要 stock_code, chain_name, tier_key"})
    execute_insert(
        """INSERT INTO chain_watchlist
             (stock_code, stock_name, industry, chain_name, tier_key, notes, added_date)
           VALUES (%s, %s, %s, %s, %s, %s, CURDATE())
           ON DUPLICATE KEY UPDATE
             stock_name=VALUES(stock_name),
             industry=VALUES(industry),
             notes=VALUES(notes),
             added_date=CURDATE()""",
        [stock_code, stock_name, industry, chain_name, tier_key, notes],
    )
    return {"ok": True}


# ── API: 待观察 — 移除 ─────────────────────────────────────────────

@router.post("/api/watchlist/remove")
def api_chain_watchlist_remove(body: dict):
    """从待观察移除"""
    stock_code = (body.get("stock_code") or "").strip()
    chain_name = (body.get("chain_name") or "").strip()
    tier_key = (body.get("tier_key") or "").strip()
    if not stock_code:
        return JSONResponse({"ok": False, "error": "缺少 stock_code"})
    if chain_name and tier_key:
        execute_insert(
            "DELETE FROM chain_watchlist WHERE stock_code=%s AND chain_name=%s AND tier_key=%s",
            [stock_code, chain_name, tier_key],
        )
    else:
        execute_insert("DELETE FROM chain_watchlist WHERE stock_code=%s", [stock_code])
    return {"ok": True}


# ── API: 待观察 — 已有 codes（供 daily intel 页面标记） ───────────

@router.get("/api/watchlist/codes")
def api_chain_watchlist_codes():
    rows = execute_query("SELECT DISTINCT stock_code FROM chain_watchlist") or []
    return {"ok": True, "codes": [r["stock_code"] for r in rows]}


# ── API: 待观察 — 概览列表（类 Tab1 龙头股格式） ─────────────────

@router.get("/api/watchlist/list")
def api_watchlist_list(date: str = ""):
    """返回待观察产业链概览，格式与 /api/list 一致"""
    # 获取所有待观察记录
    watch_rows = execute_query(
        "SELECT stock_code, stock_name, chain_name, tier_key FROM chain_watchlist"
    ) or []
    if not watch_rows:
        return {"ok": True, "chains": [], "query_date": date}

    # 按产业链分组
    chain_map: dict = {}  # chain_name -> {tier_key -> [codes]}
    for r in watch_rows:
        cn = r["chain_name"]
        tk = r["tier_key"]
        if cn not in chain_map:
            chain_map[cn] = {}
        chain_map[cn].setdefault(tk, []).append(r["stock_code"])

    # 确定查询日期
    if not date:
        row = cloud_stockdb_query(
            "SELECT MAX(trade_date) AS mx FROM fund_flow_history"
        ) or []
        date = str(row[0]["mx"]) if row and row[0]["mx"] else ""

    # 批量查资金流（query_date 前含当天最近7个交易日累计）
    all_codes = list(set(r["stock_code"] for r in watch_rows))
    flow_by_code: dict = {}
    flow_days = 0
    if all_codes and date:
        ph = ",".join(["%s"] * len(all_codes))
        date_rows = cloud_stockdb_query(
            """SELECT DISTINCT trade_date FROM fund_flow_history
               WHERE trade_date <= %s ORDER BY trade_date DESC LIMIT 7""",
            [date],
        ) or []
        window_dates = [r["trade_date"] for r in date_rows]
        flow_days = len(window_dates)
        if window_dates:
            dp = ",".join(["%s"] * len(window_dates))
            flow_rows = cloud_stockdb_query(
                f"""SELECT symbol, SUM(main_net_inflow) AS total_inflow
                    FROM fund_flow_history
                    WHERE symbol IN ({ph}) AND trade_date IN ({dp})
                    GROUP BY symbol""",
                all_codes + window_dates,
            ) or []
            for r in flow_rows:
                flow_by_code[r["symbol"]] = float(r["total_inflow"] or 0)

    # 获取产业链 meta（icon/color），优先用静态配置，否则给默认值
    result = []
    for cn, tiers in chain_map.items():
        static = CHAINS.get(cn, {})
        icon = static.get("icon", "category")
        color = static.get("color", "#64748b")

        codes = list(set(c for tier_codes in tiers.values() for c in tier_codes))
        total_flow = sum(flow_by_code.get(c, 0) for c in codes) / 10000

        tiers_summary = []
        for tk, tier_codes in tiers.items():
            static_label = ""
            if cn in CHAINS and tk in CHAINS[cn].get("tiers", {}):
                static_label = CHAINS[cn]["tiers"][tk]["label"]
            tiers_summary.append({
                "key": tk,
                "label": static_label or tk,
            })

        result.append({
            "name": cn,
            "icon": icon,
            "color": color,
            "stock_count": len(codes),
            "today_flow": round(total_flow, 2),
            "tier_count": len(tiers),
            "tiers_summary": tiers_summary,
        })

    result.sort(key=lambda x: x["today_flow"], reverse=True)
    return {"ok": True, "chains": result, "query_date": date}


# ── API: 待观察 — 产业链明细 ─────────────────────────────────────

@router.get("/api/watchlist/detail-v2")
def api_watchlist_detail_v2(name: str):
    """待观察产业链明细，格式与 /api/detail-v2 一致"""
    # 获取该链的待观察股票
    watch_rows = execute_query(
        "SELECT stock_code, stock_name, tier_key FROM chain_watchlist WHERE chain_name=%s",
        [name],
    ) or []
    if not watch_rows:
        return {"ok": True, "name": name, "flow_dates": [], "flow_total": [], "tiers": []}

    static = CHAINS.get(name, {})

    all_codes = list(set(r["stock_code"] for r in watch_rows))
    codes_ph = ",".join(["%s"] * len(all_codes))

    # 最近15个有效交易日（云端 fund_flow_history）
    date_rows = cloud_stockdb_query(
        f"""SELECT DISTINCT trade_date FROM fund_flow_history
            WHERE symbol IN ({codes_ph})
            ORDER BY trade_date DESC LIMIT 15""",
        all_codes,
    ) or []
    dates = sorted([r["trade_date"] for r in date_rows])
    dates_str = [str(d) for d in dates]

    # 资金流（万元 → 亿元）
    cf_map: dict = {}
    if dates:
        dates_ph = ",".join(["%s"] * len(dates))
        cf_rows = cloud_stockdb_query(
            f"""SELECT symbol, trade_date, main_net_inflow
                FROM fund_flow_history
                WHERE symbol IN ({codes_ph}) AND trade_date IN ({dates_ph})""",
            all_codes + dates,
        ) or []
        for r in cf_rows:
            cf_map.setdefault(r["symbol"], {})[str(r["trade_date"])] = float(r["main_net_inflow"] or 0) / 10000

    flow_total = [
        round(sum(cf_map.get(code, {}).get(d, 0) for code in all_codes), 2)
        for d in dates_str
    ]

    # 最新行情
    latest_sd_map: dict = {}
    latest_trade = execute_query(
        f"SELECT MAX(trade_date) AS mx FROM stock_daily WHERE stock_code IN ({codes_ph})",
        all_codes,
    ) or []
    latest_trade_date = latest_trade[0]["mx"] if latest_trade else None
    if latest_trade_date:
        sd_rows = execute_query(
            f"""SELECT stock_code, close AS latest_price, change_pct AS latest_pct
                FROM stock_daily
                WHERE stock_code IN ({codes_ph}) AND trade_date = %s""",
            all_codes + [latest_trade_date],
        ) or []
        for r in sd_rows:
            latest_sd_map[r["stock_code"]] = {
                "latest_price": float(r["latest_price"] or 0),
                "latest_pct": float(r["latest_pct"] or 0),
            }

    info_rows = execute_query(
        f"SELECT stock_code, stock_name, market_cap FROM stock_info WHERE stock_code IN ({codes_ph})",
        all_codes,
    ) or []
    info_map = {r["stock_code"]: r for r in info_rows}

    # 按 tier 分组
    tier_stocks: dict = {}
    for r in watch_rows:
        tier_stocks.setdefault(r["tier_key"], []).append(r["stock_code"])

    from config.chain_config import STOCK_TAGS as _STOCK_TAGS

    tiers_out = []
    for tk, codes_in_tier in tier_stocks.items():
        stocks = []
        for code in codes_in_tier:
            info = info_map.get(code, {})
            sd = latest_sd_map.get(code, {})
            mc = info.get("market_cap")
            name_str = info.get("stock_name") or code
            stocks.append({
                "code": code,
                "name": name_str,
                "latest_price": sd.get("latest_price"),
                "latest_pct": sd.get("latest_pct"),
                "market_cap": round(float(mc) / 1e8, 1) if mc else None,
                "tag": _STOCK_TAGS.get(name_str, "news"),  # watchlist 来源默认 news
            })

        tier_flow_15d = round(sum(
            sum(cf_map.get(c, {}).get(d, 0) for d in dates_str)
            for c in codes_in_tier
        ), 2)

        static_label = ""
        if name in CHAINS and tk in CHAINS[name].get("tiers", {}):
            static_label = CHAINS[name]["tiers"][tk]["label"]

        tiers_out.append({
            "key": tk,
            "label": static_label or tk,
            "tier_flow_15d": tier_flow_15d,
            "stocks": stocks,
        })

    return {
        "ok": True,
        "name": name,
        "icon": static.get("icon", "category"),
        "color": static.get("color", "#64748b"),
        "flow_dates": dates_str,
        "flow_total": flow_total,
        "tiers": tiers_out,
    }


# ── API: 待观察 — AI 自动分类并添加 ─────────────────────────────

@router.post("/api/watchlist/auto-add")
def api_watchlist_auto_add(body: dict):
    """传入 intel_id，AI 判断产业链/环节后自动添加到待观察"""
    from utils.model_router import call_model_json

    intel_id = body.get("intel_id")
    if not intel_id:
        return JSONResponse({"ok": False, "error": "缺少 intel_id"})

    # 查情报详情
    row = execute_cloud_query(
        "SELECT * FROM daily_intel_stocks WHERE id=%s", [intel_id]
    ) or []
    if not row:
        return JSONResponse({"ok": False, "error": "情报不存在"})

    intel = dict(row[0])
    stock_code = intel.get("stock_code") or ""
    stock_name = intel.get("stock_name") or ""
    industry = intel.get("industry") or ""

    if not stock_code:
        return JSONResponse({"ok": False, "error": "该情报无股票代码"})

    # 检查是否已存在
    exists = execute_query(
        "SELECT id FROM chain_watchlist WHERE stock_code=%s LIMIT 1", [stock_code]
    )
    if exists:
        return {"ok": True, "message": "已在待观察中", "already": True}

    # 构建产业链上下文
    lines = []
    for chain_name, chain in CHAINS.items():
        for tier_key, tier in chain["tiers"].items():
            lines.append(f"{chain_name} > {tier_key}（{tier['label']}）")
    chain_context = "\n".join(lines)

    system = """你是A股产业链分类专家。根据股票信息，判断它最合适属于哪条产业链的哪个环节。

规则：
1. 优先从已有产业链列表中选择最匹配的
2. 若确实不属于任何已有链，可新建（is_new=true），给出合理的chain_name和tier_key
3. tier_key 格式参考已有列表（上上游/上游/中游/下游/下下游/配套等）
4. 必须返回合法JSON，不要输出其他内容

返回格式：
{"chain_name":"xxx","tier_key":"xxx","tier_label":"xxx","is_new":false}"""

    user = f"""已有产业链列表：
{chain_context}

待分类股票：
股票名称：{stock_name}
所属行业：{industry}
业务描述：{intel.get('business_desc', '') or ''}
事件类型：{intel.get('event_type', '') or ''}
事件摘要：{intel.get('event_summary', '') or ''}

请输出JSON分类结果。"""

    try:
        result = call_model_json("hotspot", system, user)
        if not result or "chain_name" not in result:
            logger.warning(f"AI分类返回异常: {result}")
            return JSONResponse({"ok": False, "error": "AI分类失败，返回格式异常"})
    except Exception as e:
        logger.error(f"AI分类失败 intel_id={intel_id}: {e}")
        return JSONResponse({"ok": False, "error": f"AI分类失败: {e}"})

    chain_name = result["chain_name"]
    tier_key = result["tier_key"]

    execute_insert(
        """INSERT INTO chain_watchlist
             (stock_code, stock_name, industry, chain_name, tier_key, notes, added_date)
           VALUES (%s, %s, %s, %s, %s, %s, CURDATE())
           ON DUPLICATE KEY UPDATE
             stock_name=VALUES(stock_name),
             industry=VALUES(industry),
             notes=VALUES(notes),
             added_date=CURDATE()""",
        [stock_code, stock_name, industry, chain_name, tier_key,
         f"{result.get('tier_label', '')}"],
    )

    return {
        "ok": True,
        "stock_code": stock_code,
        "stock_name": stock_name,
        "chain_name": chain_name,
        "tier_key": tier_key,
        "tier_label": result.get("tier_label", ""),
        "is_new": result.get("is_new", False),
    }


# ── 明细页入口（必须放在所有 /api/* 之后） ───────────────────────

@router.get("/{name}", response_class=HTMLResponse)
def chain_detail_page(request: Request, name: str):
    if name not in CHAINS:
        return HTMLResponse("<h1>404 — 产业链不存在</h1>", status_code=404)
    chain = CHAINS[name]
    return templates.TemplateResponse("chain_detail.html", {
        "request": request,
        "active_page": "chain",
        "chain_name": name,
        "chain_icon": chain["icon"],
        "chain_color": chain["color"],
    })

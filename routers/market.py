"""市场总览 — 大盘指数 + 资金流 Breakdown + 股票搜索"""
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from utils.db_utils import execute_query
from config import AKSHARE_DELAY

logger = logging.getLogger(__name__)

# ── 富途可用性检测（延迟导入，不影响启动） ────────────────────
_futu_checked = False
_futu_ok = False


def _check_futu():
    global _futu_checked, _futu_ok
    if _futu_checked:
        return _futu_ok
    _futu_checked = True
    try:
        from ingestion.futu_source import _is_futu_ready
        _futu_ok = _is_futu_ready()
    except Exception:
        _futu_ok = False
    if _futu_ok:
        logger.info("富途行情可用，优先使用 Futu 数据源")
    return _futu_ok
router = APIRouter(prefix="/market", tags=["market"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# ── 简易内存缓存 ──────────────────────────────────────────────
_cache = {}
CACHE_TTL = 300  # 5 分钟


def _cached(key, fetcher, ttl=CACHE_TTL):
    now = time.time()
    if key in _cache and now - _cache[key]["ts"] < ttl:
        return _cache[key]["data"]
    try:
        data = fetcher()
        _cache[key] = {"data": data, "ts": now}
        return data
    except Exception as e:
        logger.warning(f"缓存获取失败 [{key}]: {e}")
        return _cache.get(key, {}).get("data")


# ── 指数配置 ──────────────────────────────────────────────────
INDEX_LIST = [
    {"code": "000001", "name": "上证指数", "market": "sh", "color": "#ef4444"},
    {"code": "399001", "name": "深证成指", "market": "sz", "color": "#f97316"},
    {"code": "399006", "name": "创业板指", "market": "sz", "color": "#8b5cf6"},
    {"code": "000688", "name": "科创50",   "market": "sh", "color": "#06b6d4"},
    {"code": "HSI",    "name": "恒生指数", "market": "hk", "color": "#10b981"},
    {"code": "HSTECH", "name": "恒生科技", "market": "hk", "color": "#ec4899"},
]


def _fetch_index_daily(idx, days=30):
    """拉取单个指数的日线数据（仅收盘价，用于指数卡片 sparkline）"""
    time.sleep(AKSHARE_DELAY)
    try:
        if idx["market"] == "hk":
            df = ak.stock_hk_index_daily_sina(symbol=idx["code"])
        else:
            symbol = idx["market"] + idx["code"]  # e.g. sh000001, sz399001
            df = ak.stock_zh_index_daily(symbol=symbol)
    except Exception:
        return []
    if df is None or df.empty:
        return []
    # 列名统一为英文: date, open, high, low, close, volume
    df["date"] = df["date"].astype(str).str[:10]
    df = df.sort_values("date").tail(days)
    return df[["date", "close"]].to_dict("records")


def _fetch_index_ohlc(idx, days=60):
    """拉取单个指数的 OHLC K线数据（用于K线图）"""
    time.sleep(AKSHARE_DELAY)
    try:
        if idx["market"] == "hk":
            df = ak.stock_hk_index_daily_sina(symbol=idx["code"])
        else:
            symbol = idx["market"] + idx["code"]  # e.g. sh000001, sz399001
            df = ak.stock_zh_index_daily(symbol=symbol)
    except Exception:
        return {"dates": [], "ohlc": [], "volume": []}
    if df is None or df.empty:
        return {"dates": [], "ohlc": [], "volume": []}
    df["date"] = df["date"].astype(str).str[:10]
    df = df.sort_values("date").tail(days)

    dates = df["date"].tolist()
    ohlc = [[row["open"], row["high"], row["low"], row["close"]] for _, row in df.iterrows()]
    volume = df["volume"].tolist() if "volume" in df.columns else []
    return {"dates": dates, "ohlc": ohlc, "volume": volume}


# ── 页面路由 ──────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
def market_page(request: Request):
    return templates.TemplateResponse("market.html", {
        "request": request,
        "active_page": "market",
        "index_list": INDEX_LIST,
    })


# ── API: 指数数据 ─────────────────────────────────────────────

@router.get("/api/indices", response_class=JSONResponse)
def api_indices(days: int = 20):
    """返回所有指数的日线数据 + 涨跌幅"""
    try:
        # 尝试用 Futu 获取指数实时价格
        futu_snapshot = {}
        if _check_futu():
            try:
                from ingestion.futu_source import get_market_snapshot
                hk_codes = ["800000", "800700"]
                a_codes = ["000001", "399001", "399006", "000688"]
                futu_snapshot = get_market_snapshot(a_codes + hk_codes)
            except Exception as e:
                logger.debug(f"Futu 指数快照失败，降级 AKShare: {e}")

        # 并行获取所有指数数据
        def _fetch_one(idx):
            cache_key = f"idx_{idx['code']}_{days}"
            return idx, _cached(cache_key, lambda i=idx, d=days: _fetch_index_daily(i, d))

        idx_records = {}
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(_fetch_one, idx): idx["code"] for idx in INDEX_LIST}
            for fut in as_completed(futures):
                try:
                    idx, records = fut.result()
                    idx_records[idx["code"]] = (idx, records)
                except Exception:
                    pass

        results = []
        for idx in INDEX_LIST:
            idx_data, records = idx_records.get(idx["code"], (idx, None))
            if not records:
                results.append({**idx, "records": [], "change_pct": None, "latest": None})
                continue
            latest = records[-1]["close"]
            first = records[0]["close"]
            change_pct = round((latest - first) / first * 100, 2) if first else None

            # 用 Futu 实时价覆盖最新价
            snap_code = {"HSI": "800000", "HSTECH": "800700"}.get(idx["code"], idx["code"])
            if snap_code in futu_snapshot:
                snap = futu_snapshot[snap_code]
                if snap.get("price"):
                    latest = snap["price"]
                    change_pct = snap.get("change_pct", change_pct)

            results.append({
                **idx,
                "records": records,
                "latest": round(latest, 2),
                "change_pct": change_pct,
            })
        return results
    except Exception:
        return []


# ── API: 指数K线图 ─────────────────────────────────────────────

@router.get("/api/index-chart", response_class=JSONResponse)
def api_index_chart(code: str = "000001", days: int = 60):
    """返回指数K线数据 (OHLC + Volume)

    code: 000001(上证), 399001(深证), 399006(创业板)
    返回: {dates: [], ohlc: [[o,h,l,c], ...], volume: []}
    """
    # 查找指数配置
    idx = next((i for i in INDEX_LIST if i["code"] == code), None)
    if not idx:
        return {"error": f"未知指数代码: {code}", "dates": [], "ohlc": [], "volume": []}

    cache_key = f"idx_ohlc_{code}_{days}"
    return _cached(cache_key, lambda i=idx, d=days: _fetch_index_ohlc(i, d), ttl=600)







# ── API: 自定义标签组合资金流 ─────────────────────────────────

@router.get("/api/tag-flow", response_class=JSONResponse)
def api_tag_flow(tags: str = "", days: int = 5):
    """根据标签关键词聚合关联股票的资金流（含环比）"""
    if not tags:
        return {"current": {}, "prev": {}, "stocks": []}
    try:
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]
        # 通过 cleaned_items 的 tags_json 找到关联股票
        stock_codes = set()
        for tag in tag_list:
            rows = execute_query(
                """SELECT DISTINCT ic.stock_code FROM item_companies ic
                   JOIN cleaned_items ci ON ic.cleaned_item_id = ci.id
                   WHERE ci.tags_json LIKE ?
                   LIMIT 50""",
                [f"%{tag}%"],
            )
            for r in rows or []:
                stock_codes.add(r["stock_code"])
        if not stock_codes:
            return {"current": {}, "prev": {}, "stocks": [], "tag_list": tag_list,
                    "matched_count": 0, "message": "未找到关联股票"}

        placeholders = ",".join(["?"] * len(stock_codes))
        codes = list(stock_codes)
        today = datetime.now().strftime("%Y-%m-%d")

        # 当期：最近 days 天
        cur_end = today
        cur_start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        cur_rows = execute_query(
            f"""SELECT stock_code, SUM(main_net_inflow) as net
                FROM capital_flow
                WHERE stock_code IN ({placeholders})
                  AND trade_date >= ? AND trade_date <= ?
                GROUP BY stock_code ORDER BY net DESC""",
            codes + [cur_start, cur_end],
        )
        cur_total = sum((r["net"] or 0) for r in (cur_rows or []))

        # 环比上期：前 days 天
        prev_end = (datetime.now() - timedelta(days=days + 1)).strftime("%Y-%m-%d")
        prev_start = (datetime.now() - timedelta(days=days * 2 + 1)).strftime("%Y-%m-%d")
        prev_rows = execute_query(
            f"""SELECT SUM(main_net_inflow) as net
                FROM capital_flow
                WHERE stock_code IN ({placeholders})
                  AND trade_date >= ? AND trade_date <= ?""",
            codes + [prev_start, prev_end],
        )
        prev_total = (prev_rows[0]["net"] or 0) if prev_rows else 0

        # 个股明细（当期）
        stocks = []
        for r in cur_rows or []:
            info = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=?", [r["stock_code"]])
            name = info[0]["stock_name"] if info else r["stock_code"]
            net = r["net"] or 0
            stocks.append({"code": r["stock_code"], "name": name, "net_inflow": round(net, 2)})

        return {
            "current": {"start": cur_start, "end": cur_end, "total": round(cur_total, 2)},
            "prev":    {"start": prev_start, "end": prev_end, "total": round(prev_total, 2)},
            "stocks": stocks[:20],
            "tag_list": tag_list,
            "matched_count": len(stock_codes),
            "message": f"匹配到 {len(stock_codes)} 只股票" + (f"，其中 {len(stocks)} 只有资金流数据" if stocks else "，但均无资金流数据"),
        }
    except Exception:
        return {"current": {}, "prev": {}, "stocks": []}


# ── API: 股票搜索 ─────────────────────────────────────────────

@router.get("/api/search-stocks", response_class=JSONResponse)
def api_search_stocks(q: str = "", tags: str = "", days: int = 20):
    """搜索股票：精确代码/名称 + 模糊标签 + 多标签组"""
    if not q.strip() and not tags.strip():
        return []
    try:
        stock_codes = set()

        if q.strip():
            q = q.strip()
            # 1. 精确匹配代码或名称
            exact = execute_query(
                "SELECT stock_code FROM stock_info WHERE stock_code=%s OR stock_name=%s",
                [q, q],
            )
            for r in exact or []:
                stock_codes.add(r["stock_code"])

            # 2. 模糊匹配名称
            fuzzy = execute_query(
                "SELECT stock_code FROM stock_info WHERE stock_name LIKE %s LIMIT 20",
                [f"%{q}%"],
            )
            for r in fuzzy or []:
                stock_codes.add(r["stock_code"])

            # 3. 标签匹配 — 通过 cleaned_items tags_json 关联
            tag_stocks = execute_query(
                """SELECT DISTINCT ic.stock_code FROM item_companies ic
                   JOIN cleaned_items ci ON ic.cleaned_item_id = ci.id
                   WHERE ci.tags_json LIKE %s
                   LIMIT 30""",
                [f"%{q}%"],
            )
            for r in tag_stocks or []:
                stock_codes.add(r["stock_code"])

            # 4. 知识图谱实体关联
            kg_stocks = execute_query(
                """SELECT DISTINCT ic.stock_code FROM kg_entities ke
                   JOIN kg_relationships kr ON ke.id = kr.source_entity_id OR ke.id = kr.target_entity_id
                   JOIN kg_entities ke2 ON (kr.target_entity_id = ke2.id OR kr.source_entity_id = ke2.id)
                   JOIN item_companies ic ON ic.stock_name LIKE CONCAT('%%', ke2.entity_name, '%%')
                   WHERE ke.entity_name LIKE %s AND ke2.entity_type = 'company'
                   LIMIT 20""",
                [f"%{q}%"],
            )
            for r in kg_stocks or []:
                stock_codes.add(r["stock_code"])

        # 5. 多标签组搜索 — tags 为逗号分隔的标签，OR 逻辑
        if tags.strip():
            tag_list = [t.strip() for t in tags.split(",") if t.strip()]
            for tag in tag_list:
                tag_stocks = execute_query(
                    """SELECT DISTINCT ic.stock_code FROM item_companies ic
                       JOIN cleaned_items ci ON ic.cleaned_item_id = ci.id
                       WHERE ci.tags_json LIKE %s
                       LIMIT 30""",
                    [f"%{tag}%"],
                )
                for r in tag_stocks or []:
                    stock_codes.add(r["stock_code"])

        if not stock_codes:
            return []

        # 获取行情数据
        results = []
        for code in list(stock_codes)[:50]:
            stock = _build_stock_row(code, days)
            if stock:
                results.append(stock)
        results.sort(key=lambda x: x.get("change_pct") or 0, reverse=True)
        return results
    except Exception:
        return []


def _build_stock_row(stock_code, days=20):
    """构建单只股票的行情数据行"""
    info = execute_query(
        "SELECT * FROM stock_info WHERE stock_code=?", [stock_code]
    )
    if not info:
        return None
    si = dict(info[0])

    # 最新行情
    daily = execute_query(
        """SELECT * FROM stock_daily WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT 1""",
        [stock_code],
    )
    d = dict(daily[0]) if daily else {}

    # 区间涨跌幅
    period = execute_query(
        """SELECT close FROM stock_daily WHERE stock_code=? AND trade_date >= date('now', ?)
           ORDER BY trade_date ASC LIMIT 1""",
        [stock_code, f"-{days} days"],
    )
    period_start = period[0]["close"] if period else None
    period_pct = None
    if period_start and d.get("close"):
        period_pct = round((d["close"] - period_start) / period_start * 100, 2)

    # 资金流
    flow = execute_query(
        """SELECT SUM(main_net_inflow) as net FROM capital_flow
           WHERE stock_code=? AND trade_date >= date('now', ?)""",
        [stock_code, f"-{days} days"],
    )
    net_inflow = flow[0]["net"] if flow and flow[0]["net"] else None

    return {
        "stock_code": stock_code,
        "stock_name": si.get("stock_name", ""),
        "industry": si.get("industry_l1", ""),
        "market_cap": si.get("market_cap"),
        "float_shares": si.get("float_shares"),
        "latest_price": d.get("close"),
        "change_pct": d.get("change_pct"),
        "period_pct": period_pct,
        "volume": d.get("volume"),
        "amount": d.get("amount"),
        "turnover_rate": d.get("turnover_rate"),
        "amplitude": d.get("amplitude"),
        "main_net_inflow": round(net_inflow, 2) if net_inflow else None,
        "trade_date": d.get("trade_date"),
    }


# ── API: 实时报价（Futu） ──────────────────────────────────────

@router.get("/api/realtime-quote", response_class=JSONResponse)
def api_realtime_quote(codes: str = ""):
    """实时报价接口，支持 A 股 + 港股。
    codes: 逗号分隔的股票代码，如 "000001,00700,600519"
    """
    if not codes:
        return {"error": "请提供 codes 参数", "data": []}
    code_list = [c.strip() for c in codes.split(",") if c.strip()]

    # 优先 Futu
    if _check_futu():
        try:
            from ingestion.futu_source import fetch_realtime_quote
            data = fetch_realtime_quote(code_list)
            if data:
                return {"source": "futu", "data": data}
        except Exception as e:
            logger.warning(f"Futu 实时报价失败，降级 DB: {e}")

    # 降级：从 stock_realtime 缓存表或 stock_daily 读取
    results = []
    for code in code_list:
        # 先查 stock_realtime 缓存
        rt = execute_query(
            "SELECT * FROM stock_realtime WHERE stock_code=?", [code]
        )
        if rt:
            r = dict(rt[0])
            results.append(r)
            continue
        # 再查 stock_daily 最新一条
        row = _build_stock_row(code, days=1)
        if row:
            results.append(row)
    return {"source": "cache", "data": results}


# ── API: 筹码分布 ─────────────────────────────────────────────

@router.get("/api/chip-distribution", response_class=JSONResponse)
def api_chip_distribution(code: str = "", days: int = 120):
    """筹码分布分析：获利盘/套牢盘/筹码峰/集中度"""
    if not code:
        return {"error": "请提供 code 参数"}
    try:
        from ingestion.futu_source import calc_chip_distribution
        result = calc_chip_distribution(code.strip(), days=days)
        if result:
            return {"code": code, **result}
        return {"error": "K线数据不足，无法计算筹码分布"}
    except Exception as e:
        logger.warning(f"筹码分布计算失败 {code}: {e}")
        return {"error": str(e)}


# ── API: 资金流历史 ────────────────────────────────────────────

@router.get("/api/capital-flow-history", response_class=JSONResponse)
def api_capital_flow_history(code: str = "", days: int = 30):
    """日级资金流历史（主力/超大/大/中/小单净流入）"""
    if not code:
        return {"error": "请提供 code 参数"}
    code = code.strip()

    # 优先从 DB 读取
    rows = execute_query(
        """SELECT trade_date, main_net_inflow, super_large_net,
                  large_net, medium_net, small_net
           FROM capital_flow WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT ?""",
        [code, days],
    )
    if rows and len(rows) >= days // 2:
        return {"code": code, "source": "db",
                "data": [dict(r) for r in reversed(rows)]}

    # DB 数据不足，尝试 Futu 拉取
    if _check_futu():
        try:
            from ingestion.futu_source import fetch_capital_flow_history
            count = fetch_capital_flow_history(code, days=days)
            if count > 0:
                rows = execute_query(
                    """SELECT trade_date, main_net_inflow, super_large_net,
                              large_net, medium_net, small_net
                       FROM capital_flow WHERE stock_code=?
                       ORDER BY trade_date DESC LIMIT ?""",
                    [code, days],
                )
                return {"code": code, "source": "futu",
                        "data": [dict(r) for r in reversed(rows or [])]}
        except Exception as e:
            logger.warning(f"Futu 资金流拉取失败 {code}: {e}")

    return {"code": code, "source": "none", "data": [dict(r) for r in reversed(rows or [])]}


# ── 资金面 API (从 capital.py 迁移) ──────────────────────────

from utils.db_utils import cloud_stockdb_query as _cq

_cap_cache = {}
_CAP_TTL = 600


def _cap_cached(key, fetcher, ttl=_CAP_TTL):
    import time
    now = time.time()
    if key in _cap_cache and now - _cap_cache[key]["ts"] < ttl:
        return _cap_cache[key]["data"]
    try:
        data = fetcher()
        _cap_cache[key] = {"data": data, "ts": now}
        return data
    except Exception as e:
        logger.warning(f"资金缓存失败 [{key}]: {e}")
        return _cap_cache.get(key, {}).get("data")


@router.get("/api/capital/market-flow", response_class=JSONResponse)
def api_capital_market_flow(start: str = "", end: str = ""):
    """两市每日成交额 + 主力资金净流入"""
    from datetime import datetime, timedelta
    if not start or not end:
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    def _fetch():
        amount_rows = _cq(
            """SELECT trade_date,
                      SUM(amount) as total_amount,
                      SUM(volume) as total_volume
               FROM stock_data
               WHERE trade_date >= %s AND trade_date <= %s
               GROUP BY trade_date ORDER BY trade_date""",
            [start, end],
        )
        flow_rows = _cq(
            """SELECT trade_date,
                      SUM(buy_lg_amount + buy_elg_amount - sell_lg_amount - sell_elg_amount) as main_net,
                      SUM(buy_elg_amount - sell_elg_amount) as elg_net,
                      SUM(buy_lg_amount - sell_lg_amount) as lg_net
               FROM fund_flow_history
               WHERE trade_date >= %s AND trade_date <= %s
               GROUP BY trade_date ORDER BY trade_date""",
            [start, end],
        )
        flow_map = {}
        for r in flow_rows or []:
            td = r["trade_date"]
            if hasattr(td, "strftime"):
                td = td.strftime("%Y-%m-%d")
            flow_map[str(td)] = r
        result = []
        for r in amount_rows or []:
            td = r["trade_date"]
            if hasattr(td, "strftime"):
                td = td.strftime("%Y-%m-%d")
            td = str(td)
            f = flow_map.get(td, {})
            result.append({
                "trade_date": td,
                "total_amount": float(r["total_amount"] or 0),
                "total_volume": float(r["total_volume"] or 0),
                "main_net": float(f.get("main_net") or 0),
                "elg_net": float(f.get("elg_net") or 0),
                "lg_net": float(f.get("lg_net") or 0),
            })
        return result

    return _cap_cached(f"mkt_flow_{start}_{end}", _fetch) or []


@router.get("/api/capital/margin", response_class=JSONResponse)
def api_capital_margin(start: str = "", end: str = ""):
    """沪深两市融资余额"""
    import time as _time
    from datetime import datetime, timedelta
    if not start or not end:
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=40)).strftime("%Y%m%d")
    else:
        start = start.replace("-", "")
        end = end.replace("-", "")

    def _fetch():
        import akshare as ak
        _time.sleep(AKSHARE_DELAY)
        result = []
        try:
            df_sh = ak.stock_margin_sse(start_date=start, end_date=end)
            if df_sh is not None and not df_sh.empty:
                for _, row in df_sh.iterrows():
                    td = str(row.get("信用交易日期", ""))
                    if len(td) == 8:
                        td = f"{td[:4]}-{td[4:6]}-{td[6:8]}"
                    result.append({
                        "trade_date": td,
                        "margin_balance": float(row.get("融资余额", 0)),
                        "margin_buy": float(row.get("融资买入额", 0)),
                        "total_balance": float(row.get("融资融券余额", 0)),
                    })
        except Exception as e:
            logger.warning(f"融资余额获取失败: {e}")
        return sorted(result, key=lambda x: x["trade_date"])

    return _cap_cached(f"cap_margin_{start}_{end}", _fetch) or []


@router.get("/api/capital/southbound", response_class=JSONResponse)
def api_capital_southbound(start: str = "", end: str = ""):
    """港股通南向资金每日净买入"""
    import time as _time

    def _fetch():
        import akshare as ak
        _time.sleep(AKSHARE_DELAY)
        try:
            df = ak.stock_hsgt_hist_em(symbol="南向资金")
            if df is None or df.empty:
                return []
            result = []
            for _, row in df.iterrows():
                td = str(row.get("日期", ""))[:10]
                result.append({
                    "trade_date": td,
                    "net_buy": float(row.get("当日成交净买额", 0)),
                    "buy_amount": float(row.get("买入成交额", 0)),
                    "sell_amount": float(row.get("卖出成交额", 0)),
                    "cumulative": float(row.get("历史累计净买额", 0)),
                })
            result.sort(key=lambda x: x["trade_date"])
            if start and end:
                result = [r for r in result if start <= r["trade_date"] <= end]
            return result
        except Exception as e:
            logger.warning(f"南向资金获取失败: {e}")
            return []

    return _cap_cached(f"cap_south_{start}_{end}", _fetch, ttl=1800) or []


_KEY_ETFS = [
    {"code": "510300", "name": "沪深300ETF(华泰)"},
    {"code": "510500", "name": "中证500ETF(南方)"},
    {"code": "510050", "name": "上证50ETF(华夏)"},
    {"code": "159919", "name": "沪深300ETF(嘉实)"},
    {"code": "512100", "name": "中证1000ETF(南方)"},
    {"code": "159915", "name": "创业板ETF(易方达)"},
]


@router.get("/api/capital/etf-shares", response_class=JSONResponse)
def api_capital_etf_shares(start: str = "", end: str = ""):
    """关键宽基ETF成交量变化"""
    import time as _time
    from datetime import datetime, timedelta
    if not start or not end:
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=40)).strftime("%Y%m%d")
    else:
        start = start.replace("-", "")
        end = end.replace("-", "")

    def _fetch():
        import akshare as ak
        results = {}
        for etf in _KEY_ETFS:
            _time.sleep(AKSHARE_DELAY)
            try:
                df = ak.fund_etf_hist_em(
                    symbol=etf["code"], period="daily",
                    start_date=start, end_date=end, adjust="",
                )
                if df is None or df.empty:
                    continue
                records = []
                for _, row in df.iterrows():
                    td = str(row.get("日期", ""))[:10]
                    records.append({
                        "trade_date": td,
                        "close": float(row.get("收盘", 0)),
                        "volume": float(row.get("成交量", 0)),
                        "amount": float(row.get("成交额", 0)),
                    })
                results[etf["code"]] = {
                    "name": etf["name"],
                    "data": sorted(records, key=lambda x: x["trade_date"]),
                }
            except Exception as e:
                logger.warning(f"ETF {etf['code']} 获取失败: {e}")
        return results

    return _cap_cached(f"cap_etf_{start}_{end}", _fetch, ttl=1800) or {}


@router.get("/api/capital/industry-flow", response_class=JSONResponse)
def api_capital_industry_flow(start: str = "", end: str = ""):
    """按行业汇总主力资金净流入"""
    from datetime import datetime, timedelta
    if not start or not end:
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

    def _fetch():
        rows = _cq(
            """SELECT sl.industry,
                      SUM(ff.buy_lg_amount + ff.buy_elg_amount - ff.sell_lg_amount - ff.sell_elg_amount) as main_net,
                      COUNT(DISTINCT ff.symbol) as stock_count
               FROM fund_flow_history ff
               JOIN stock_list sl ON ff.symbol = sl.symbol
               WHERE ff.trade_date >= %s AND ff.trade_date <= %s
                 AND sl.industry IS NOT NULL AND sl.industry != ''
               GROUP BY sl.industry ORDER BY main_net DESC""",
            [start, end],
        )
        return [{"industry": r["industry"], "main_net": float(r["main_net"] or 0),
                 "stock_count": r["stock_count"]} for r in rows or []]

    return _cap_cached(f"cap_ind_flow_{start}_{end}", _fetch) or []


@router.get("/api/capital/summary", response_class=JSONResponse)
def api_capital_summary(start: str = "", end: str = "",
                         cmp_start: str = "", cmp_end: str = ""):
    """资金面综合概览：当期 vs 对比期"""
    from datetime import datetime, timedelta
    if not start or not end:
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

    def _calc_period(s, e):
        amt_rows = _cq(
            """SELECT SUM(amount) as total_amount, AVG(amount) as avg_amount,
                      COUNT(DISTINCT trade_date) as trade_days
               FROM stock_data WHERE trade_date >= %s AND trade_date <= %s""",
            [s, e],
        )
        flow_rows = _cq(
            """SELECT SUM(buy_lg_amount + buy_elg_amount - sell_lg_amount - sell_elg_amount) as main_net
               FROM fund_flow_history WHERE trade_date >= %s AND trade_date <= %s""",
            [s, e],
        )
        a = amt_rows[0] if amt_rows else {}
        f = flow_rows[0] if flow_rows else {}
        return {
            "total_amount": float(a.get("total_amount") or 0),
            "main_net": float(f.get("main_net") or 0),
            "avg_amount": float(a.get("avg_amount") or 0),
            "trade_days": int(a.get("trade_days") or 0),
        }

    current = _calc_period(start, end)
    compare = _calc_period(cmp_start, cmp_end) if cmp_start and cmp_end else None
    return {"current": current, "compare": compare, "start": start, "end": end}

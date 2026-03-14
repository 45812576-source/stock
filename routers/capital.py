"""资金面仪表盘 — 增量资金追踪"""
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from config import AKSHARE_DELAY

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/capital", tags=["capital"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# ── 简易内存缓存 ──────────────────────────────────────────────
_cache = {}
CACHE_TTL = 600  # 10 分钟


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


def _get_cloud_stockdb():
    """获取云端 stock_db 连接"""
    import pymysql
    import pymysql.cursors
    from config import CLOUD_MYSQL_HOST, CLOUD_MYSQL_PORT, CLOUD_MYSQL_USER, CLOUD_MYSQL_PASSWORD
    return pymysql.connect(
        host=CLOUD_MYSQL_HOST, port=CLOUD_MYSQL_PORT,
        user=CLOUD_MYSQL_USER, password=CLOUD_MYSQL_PASSWORD,
        database='stock_db', charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
    )


def _cloud_query(sql, params=None):
    """在云端 stock_db 执行查询"""
    conn = _get_cloud_stockdb()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()
    finally:
        conn.close()


# ── 页面路由 ──────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
def capital_page(request: Request):
    return RedirectResponse(url="/market", status_code=301)


# ── API: 两市成交额 + 主力资金净流入 ─────────────────────────

@router.get("/api/market-flow", response_class=JSONResponse)
def api_market_flow(start: str = "", end: str = ""):
    """两市每日成交额 + 主力资金净流入汇总"""
    if not start or not end:
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    def _fetch():
        # 成交额来自 stock_data，资金流来自 fund_flow_history
        amount_rows = _cloud_query(
            """SELECT trade_date,
                      SUM(amount) as total_amount,
                      SUM(volume) as total_volume
               FROM stock_data
               WHERE trade_date >= %s AND trade_date <= %s
               GROUP BY trade_date
               ORDER BY trade_date""",
            [start, end],
        )
        flow_rows = _cloud_query(
            """SELECT trade_date,
                      SUM(buy_lg_amount + buy_elg_amount - sell_lg_amount - sell_elg_amount) as main_net,
                      SUM(buy_elg_amount - sell_elg_amount) as elg_net,
                      SUM(buy_lg_amount - sell_lg_amount) as lg_net
               FROM fund_flow_history
               WHERE trade_date >= %s AND trade_date <= %s
               GROUP BY trade_date
               ORDER BY trade_date""",
            [start, end],
        )
        # 合并两张表的数据
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

    return _cached(f"mkt_flow_{start}_{end}", _fetch) or []


# ── API: 融资余额 ─────────────────────────────────────────────

@router.get("/api/margin", response_class=JSONResponse)
def api_margin(start: str = "", end: str = ""):
    """沪深两市融资余额"""
    if not start or not end:
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=40)).strftime("%Y%m%d")
    else:
        start = start.replace("-", "")
        end = end.replace("-", "")

    def _fetch():
        time.sleep(AKSHARE_DELAY)
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

    return _cached(f"margin_{start}_{end}", _fetch) or []


# ── API: 南向资金 ─────────────────────────────────────────────

@router.get("/api/southbound", response_class=JSONResponse)
def api_southbound(start: str = "", end: str = ""):
    """港股通南向资金每日净买入"""
    def _fetch():
        time.sleep(AKSHARE_DELAY)
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

    return _cached(f"south_{start}_{end}", _fetch, ttl=1800) or []


# ── API: ETF份额变化 ──────────────────────────────────────────

KEY_ETFS = [
    {"code": "510300", "name": "沪深300ETF(华泰)"},
    {"code": "510500", "name": "中证500ETF(南方)"},
    {"code": "510050", "name": "上证50ETF(华夏)"},
    {"code": "159919", "name": "沪深300ETF(嘉实)"},
    {"code": "512100", "name": "中证1000ETF(南方)"},
    {"code": "159915", "name": "创业板ETF(易方达)"},
]


@router.get("/api/etf-shares", response_class=JSONResponse)
def api_etf_shares(start: str = "", end: str = ""):
    """关键宽基ETF的成交量和净值变化（代理份额变化）"""
    if not start or not end:
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=40)).strftime("%Y%m%d")
    else:
        start = start.replace("-", "")
        end = end.replace("-", "")

    def _fetch():
        results = {}
        for etf in KEY_ETFS:
            time.sleep(AKSHARE_DELAY)
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

    return _cached(f"etf_{start}_{end}", _fetch, ttl=1800) or {}


# ── API: 行业资金流向 ─────────────────────────────────────────

@router.get("/api/industry-flow", response_class=JSONResponse)
def api_industry_flow(start: str = "", end: str = ""):
    """按行业汇总主力资金净流入 — 用 akshare 实时拉取"""
    def _fetch():
        time.sleep(AKSHARE_DELAY)
        try:
            df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流向")
            if df is None or df.empty:
                return []
            result = []
            for _, row in df.iterrows():
                try:
                    net_val = float(str(row.get("今日主力净流入-净额", 0)).replace(",", "") or 0)
                    result.append({
                        "industry": str(row.get("名称", "")),
                        "main_net": net_val * 1e4,  # 万元 → 元
                        "stock_count": 0,
                    })
                except Exception:
                    continue
            result.sort(key=lambda x: x["main_net"], reverse=True)
            return result
        except Exception as e:
            logger.warning(f"行业资金流向 akshare 失败: {e}")
            return []

    return _cached("ind_flow_today", _fetch, ttl=1800) or []


# ── API: 综合概览（汇总卡片数据）─────────────────────────────

@router.get("/api/summary", response_class=JSONResponse)
def api_summary(start: str = "", end: str = "",
                cmp_start: str = "", cmp_end: str = ""):
    """资金面综合概览：当期 vs 对比期"""
    if not start or not end:
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

    def _calc_period(s, e):
        amt_rows = _cloud_query(
            """SELECT SUM(amount) as total_amount,
                      AVG(amount) as avg_amount,
                      COUNT(DISTINCT trade_date) as trade_days
               FROM stock_data
               WHERE trade_date >= %s AND trade_date <= %s""",
            [s, e],
        )
        flow_rows = _cloud_query(
            """SELECT SUM(buy_lg_amount + buy_elg_amount - sell_lg_amount - sell_elg_amount) as main_net
               FROM fund_flow_history
               WHERE trade_date >= %s AND trade_date <= %s""",
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


# ── API: 陆港通15交易日成交量 ─────────────────────────────────

@router.get("/api/hsgt-flow", response_class=JSONResponse)
def api_hsgt_flow():
    """陆港通近15交易日：沪股通+深股通北向成交净买额，及南向合计"""
    def _fetch():
        time.sleep(AKSHARE_DELAY)
        try:
            df = ak.stock_hsgt_fund_flow_summary_em()
            if df is None or df.empty:
                return []
            result = {}
            for _, row in df.iterrows():
                td = str(row.get("交易日", ""))[:10]
                board = str(row.get("板块", ""))
                direction = str(row.get("资金方向", ""))
                net_buy = float(row.get("成交净买额") or 0)
                up = int(row.get("上涨数") or 0)
                down = int(row.get("下跌数") or 0)
                if td not in result:
                    result[td] = {"trade_date": td, "sh_north": 0, "sz_north": 0,
                                  "sh_south": 0, "sz_south": 0,
                                  "north_total": 0, "south_total": 0,
                                  "up": 0, "down": 0}
                if board == "沪股通" and direction == "北向":
                    result[td]["sh_north"] = net_buy
                    result[td]["up"] += up
                    result[td]["down"] += down
                elif board == "深股通" and direction == "北向":
                    result[td]["sz_north"] = net_buy
                    result[td]["up"] += up
                    result[td]["down"] += down
                elif board == "港股通(沪)" and direction == "南向":
                    result[td]["sh_south"] = net_buy
                elif board == "港股通(深)" and direction == "南向":
                    result[td]["sz_south"] = net_buy
            for v in result.values():
                v["north_total"] = v["sh_north"] + v["sz_north"]
                v["south_total"] = v["sh_south"] + v["sz_south"]
            rows = sorted(result.values(), key=lambda x: x["trade_date"])
            return rows[-15:]
        except Exception as e:
            logger.warning(f"陆港通数据获取失败: {e}")
            return []

    return _cached("hsgt_flow", _fetch, ttl=1800) or []


# ── API: 海外ETF资金流向 ──────────────────────────────────────

OVERSEAS_ETFS = [
    {"symbol": "KWEB", "name": "中概互联网ETF", "color": "#135bec"},
    {"symbol": "FXI",  "name": "中国大盘ETF",   "color": "#f97316"},
    {"symbol": "ASHR", "name": "A股ETF",        "color": "#10b981"},
]


@router.get("/api/overseas-etf", response_class=JSONResponse)
def api_overseas_etf():
    """KWEB/FXI/ASHR 近15交易日价格+成交量 + 最新持仓Top10"""
    def _fetch():
        try:
            import yfinance as yf
        except ImportError:
            return {}

        result = {}
        for etf in OVERSEAS_ETFS:
            sym = etf["symbol"]
            try:
                ticker = yf.Ticker(sym)
                hist = ticker.history(period="1mo")
                if hist is None or hist.empty:
                    continue
                hist = hist.tail(15).reset_index()
                records = []
                for _, row in hist.iterrows():
                    td = str(row["Date"])[:10]
                    records.append({
                        "trade_date": td,
                        "close": round(float(row["Close"]), 2),
                        "volume": int(row["Volume"]),
                    })
                holdings = []
                try:
                    fd = ticker.funds_data
                    if fd is not None:
                        top = fd.top_holdings
                        if top is not None and not top.empty:
                            for idx_sym, h_row in top.head(10).iterrows():
                                holdings.append({
                                    "symbol": str(idx_sym),
                                    "name": str(h_row.get("Name", idx_sym)),
                                    "pct": round(float(h_row.get("Holding Percent", 0)) * 100, 2),
                                })
                except Exception:
                    pass
                latest = records[-1] if records else {}
                prev = records[-2] if len(records) >= 2 else {}
                chg_pct = 0.0
                if latest and prev and prev["close"]:
                    chg_pct = round((latest["close"] - prev["close"]) / prev["close"] * 100, 2)
                result[sym] = {
                    "name": etf["name"],
                    "color": etf["color"],
                    "latest_close": latest.get("close", 0),
                    "chg_pct": chg_pct,
                    "latest_volume": latest.get("volume", 0),
                    "history": records,
                    "holdings": holdings,
                }
            except Exception as e:
                logger.warning(f"海外ETF {sym} 获取失败: {e}")
        return result

    return _cached("overseas_etf", _fetch, ttl=1800) or {}

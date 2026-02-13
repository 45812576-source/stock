"""市场总览 — 大盘指数 + 资金流 Breakdown + 股票搜索"""
import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from utils.db_utils import execute_query
from config import AKSHARE_DELAY

logger = logging.getLogger(__name__)
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
    """拉取单个指数的日线数据"""
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


# ── 页面路由 ──────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
async def market_page(request: Request):
    return templates.TemplateResponse("market.html", {
        "request": request,
        "active_page": "market",
        "index_list": INDEX_LIST,
    })


# ── API: 指数数据 ─────────────────────────────────────────────

@router.get("/api/indices", response_class=JSONResponse)
async def api_indices(days: int = 20):
    """返回所有指数的日线数据 + 涨跌幅"""
    try:
        results = []
        for idx in INDEX_LIST:
            cache_key = f"idx_{idx['code']}_{days}"
            records = _cached(cache_key, lambda i=idx, d=days: _fetch_index_daily(i, d))
            if not records:
                results.append({**idx, "records": [], "change_pct": None, "latest": None})
                continue
            latest = records[-1]["close"]
            first = records[0]["close"]
            change_pct = round((latest - first) / first * 100, 2) if first else None
            results.append({
                **idx, "records": records, "latest": round(latest, 2),
                "change_pct": change_pct,
            })
        return results
    except Exception:
        return []


# ── API: 行业资金流 ───────────────────────────────────────────

@router.get("/api/industry-flow", response_class=JSONResponse)
async def api_industry_flow(days: int = 5):
    """行业板块资金净流入排名"""
    def _fetch():
        rows = execute_query(
            """SELECT industry_name, SUM(net_inflow) as total_net,
                      AVG(change_pct) as avg_pct, GROUP_CONCAT(DISTINCT leading_stock) as leaders
               FROM industry_capital_flow
               WHERE trade_date >= date('now', ?) AND net_inflow IS NOT NULL
               GROUP BY industry_name ORDER BY total_net DESC""",
            [f"-{days} days"],
        )
        result = [dict(r) for r in (rows or []) if r.get("total_net") is not None]
        if result:
            return result
        # DB 无有效数据，尝试按 industry_name 返回（即使 net_inflow 为空也展示名称）
        rows2 = execute_query(
            """SELECT DISTINCT industry_name FROM industry_capital_flow
               WHERE trade_date >= date('now', ?) LIMIT 30""",
            [f"-{days} days"],
        )
        return [{"industry_name": r["industry_name"], "total_net": 0, "avg_pct": None, "leaders": None}
                for r in (rows2 or [])]
    try:
        return _cached(f"ind_flow_{days}", _fetch, ttl=600) or []
    except Exception:
        return []


# ── API: 北向资金 ─────────────────────────────────────────────

@router.get("/api/northbound", response_class=JSONResponse)
async def api_northbound(days: int = 20):
    """北向资金每日净流入"""
    def _fetch():
        # 优先从 DB 读取
        rows = execute_query(
            """SELECT trade_date, total_net FROM northbound_flow
               WHERE trade_date >= date('now', ?)
               ORDER BY trade_date""",
            [f"-{days} days"],
        )
        if rows:
            return [dict(r) for r in rows]
        # DB 无数据则实时拉取
        try:
            time.sleep(AKSHARE_DELAY)
            df = ak.stock_hsgt_hist_em(symbol="北向资金")
            if df is None or df.empty:
                return []
            df["trade_date"] = df["日期"].astype(str).str[:10]
            df = df.sort_values("trade_date").tail(days)
            result = []
            for _, row in df.iterrows():
                net = row.get("当日成交净买额")
                if net != net:  # NaN check
                    net = 0
                result.append({"trade_date": row["trade_date"], "total_net": net})
            return result
        except Exception:
            return []
    return _cached(f"north_{days}", _fetch, ttl=600) or []


# ── API: 融资融券 ─────────────────────────────────────────────

@router.get("/api/margin", response_class=JSONResponse)
async def api_margin(days: int = 20):
    """融资融券余额趋势"""
    def _fetch():
        try:
            time.sleep(AKSHARE_DELAY)
            start = (datetime.now() - timedelta(days=days + 10)).strftime("%Y%m%d")
            end = datetime.now().strftime("%Y%m%d")
            df = ak.stock_margin_sse(start_date=start, end_date=end)
            if df is None or df.empty:
                return []
            # 列名: 信用交易日期, 融资余额, 融资买入额, 融券余量, 融券余量金额, 融券卖出量, 融资融券余额
            result = []
            for _, row in df.iterrows():
                trade_date = str(row.get("信用交易日期", ""))
                if len(trade_date) == 8:
                    trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
                margin_balance = row.get("融资余额", 0)
                short_balance = row.get("融券余量金额", 0)
                result.append({
                    "trade_date": trade_date,
                    "margin_balance": margin_balance,
                    "short_balance": short_balance,
                })
            return sorted(result, key=lambda x: x["trade_date"])[-days:]
        except Exception:
            return []
    return _cached(f"margin_{days}", _fetch, ttl=600) or []


# ── API: 自定义标签组合资金流 ─────────────────────────────────

@router.get("/api/tag-flow", response_class=JSONResponse)
async def api_tag_flow(tags: str = "", days: int = 5):
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
async def api_search_stocks(q: str = "", days: int = 20):
    """搜索股票：精确代码/名称 + 模糊标签"""
    if not q or len(q.strip()) < 1:
        return []
    try:
        q = q.strip()
        stock_codes = set()

        # 1. 精确匹配代码或名称
        exact = execute_query(
            "SELECT stock_code FROM stock_info WHERE stock_code=? OR stock_name=?",
            [q, q],
        )
        for r in exact or []:
            stock_codes.add(r["stock_code"])

        # 2. 模糊匹配名称
        fuzzy = execute_query(
            "SELECT stock_code FROM stock_info WHERE stock_name LIKE ? LIMIT 20",
            [f"%{q}%"],
        )
        for r in fuzzy or []:
            stock_codes.add(r["stock_code"])

        # 3. 标签匹配 — 通过 cleaned_items tags_json 关联
        tag_stocks = execute_query(
            """SELECT DISTINCT ic.stock_code FROM item_companies ic
               JOIN cleaned_items ci ON ic.cleaned_item_id = ci.id
               WHERE ci.tags_json LIKE ?
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
               JOIN item_companies ic ON ic.stock_name LIKE '%' || ke2.entity_name || '%'
               WHERE ke.entity_name LIKE ? AND ke2.entity_type = 'company'
               LIMIT 20""",
            [f"%{q}%"],
        )
        for r in kg_stocks or []:
            stock_codes.add(r["stock_code"])

        if not stock_codes:
            return []

        # 获取行情数据
        results = []
        for code in list(stock_codes)[:50]:
            stock = _build_stock_row(code, days)
            if stock:
                results.append(stock)
    # 按涨跌幅排序
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

"""细分行业选股页 — routers/sector.py"""

import json
import logging
from urllib.parse import unquote

from fastapi import APIRouter
from fastapi.requests import Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from utils.db_utils import execute_query

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sector", tags=["sector"])
templates = Jinja2Templates(directory="templates")


# ── API: 股票列表 + 资金流 + 行情 ────────────────────────────────

@router.get("/api/stocks")
def api_sector_stocks(name: str, days: int = 15):
    """
    返回该细分行业的所有股票，及 days 个交易日的资金流/市值/涨跌幅。
    name: 行业名（query 参数，支持含斜杠）
    days: 7 / 15 / 30
    """
    name = unquote(name)
    days = max(7, min(30, days))

    # ── 1. KG 查该细分行业的股票列表 ──
    keyword = name.split("（")[0].split("(")[0].split("/")[0].strip()[:6]
    if len(keyword) < 2:
        return JSONResponse({"ok": False, "error": "行业名太短"})

    kg_inds = execute_query(
        "SELECT id FROM kg_entities WHERE entity_type='industry' AND entity_name LIKE %s",
        [f"%{keyword[:4]}%"],
    ) or []

    codes: set = set()
    for row in kg_inds:
        comp_rows = execute_query(
            """SELECT ke.external_id, ke.entity_name
               FROM kg_relationships kr
               JOIN kg_entities ke ON kr.source_entity_id = ke.id
               WHERE kr.target_entity_id = %s
                 AND kr.relation_type = 'belongs_to_industry'
                 AND ke.entity_type = 'company'
                 AND ke.external_id IS NOT NULL AND ke.external_id != ''""",
            [row["id"]],
        ) or []
        for r in comp_rows:
            codes.add(r["external_id"])

    if not codes:
        return {"ok": True, "dates": [], "stocks": [], "message": "KG中未找到关联股票"}

    codes = [c for c in codes if len(c) == 6]  # 过滤港股
    if not codes:
        return {"ok": True, "dates": [], "stocks": [], "message": "无A股股票"}

    # ── 2. 取最近 days 个有效交易日 ──
    date_rows = execute_query(
        "SELECT DISTINCT trade_date FROM capital_flow "
        "WHERE LENGTH(stock_code)=6 AND main_net_inflow != 0 "
        "ORDER BY trade_date DESC LIMIT %s",
        [days],
    ) or []
    if not date_rows:
        return {"ok": True, "dates": [], "stocks": [], "message": "暂无资金流数据"}

    dates = sorted([r["trade_date"] for r in date_rows])
    codes_ph = ",".join(["%s"] * len(codes))
    dates_ph = ",".join(["%s"] * len(dates))

    # ── 3. 批量查 capital_flow ──
    cf_rows = execute_query(
        f"""SELECT stock_code, trade_date,
                   ROUND(main_net_inflow / 10000) AS net_wan
            FROM capital_flow
            WHERE stock_code IN ({codes_ph}) AND trade_date IN ({dates_ph})""",
        codes + dates,
    ) or []

    # ── 4. 批量查 stock_daily（涨跌幅 + 市值估算）──
    sd_rows = execute_query(
        f"""SELECT stock_code, trade_date, change_pct,
                   ROUND(amount / NULLIF(turnover_rate, 0) / 1e8) AS cap_yi
            FROM stock_daily
            WHERE stock_code IN ({codes_ph}) AND trade_date IN ({dates_ph})""",
        codes + dates,
    ) or []

    # ── 5. 查股票名称 ──
    info_rows = execute_query(
        f"SELECT stock_code, stock_name, market_cap FROM stock_info WHERE stock_code IN ({codes_ph})",
        codes,
    ) or []
    info_map = {r["stock_code"]: r for r in info_rows}

    # ── 6. 整理成 per-stock 结构 ──
    # cf_map[code][date] = net_wan
    cf_map: dict = {}
    for r in cf_rows:
        cf_map.setdefault(r["stock_code"], {})[r["trade_date"]] = int(r["net_wan"] or 0)

    # sd_map[code][date] = {pct, cap}
    sd_map: dict = {}
    for r in sd_rows:
        sd_map.setdefault(r["stock_code"], {})[r["trade_date"]] = {
            "pct": float(r["change_pct"] or 0),
            "cap": float(r["cap_yi"] or 0),
        }

    stocks = []
    for code in codes:
        info = info_map.get(code, {})
        name_str = info.get("stock_name") or code

        flow = [cf_map.get(code, {}).get(d, 0) for d in dates]
        pct  = [sd_map.get(code, {}).get(d, {}).get("pct", None) for d in dates]
        cap  = [sd_map.get(code, {}).get(d, {}).get("cap", None) for d in dates]

        total_flow = sum(flow)
        # 若全程接近0（峰值 < 100万）则跳过
        if not flow or max(abs(v) for v in flow) < 100:
            continue

        latest_pct = next((v for v in reversed(pct) if v is not None), None)
        # 市值：优先用 stock_info.market_cap，再用 stock_daily 估算
        si_cap = float(info.get("market_cap") or 0) / 1e8 if info.get("market_cap") else None
        latest_cap = si_cap or next((v for v in reversed(cap) if v is not None), None)

        stocks.append({
            "code": code,
            "name": name_str,
            "flow": flow,
            "pct": pct,
            "cap": cap,
            "total_flow": total_flow,
            "latest_cap": round(latest_cap, 1) if latest_cap else None,
            "latest_pct": round(latest_pct, 2) if latest_pct is not None else None,
        })

    # 按15日（或N日）总流入绝对值降序
    stocks.sort(key=lambda s: abs(s["total_flow"]), reverse=True)

    return {"ok": True, "dates": dates, "stocks": stocks}


# ── API: 参数对比器 ────────────────────────────────────────────

@router.get("/api/compare")
def api_compare(codes: str):
    """
    横向对比最多4只股票的维度指标。
    codes: 逗号分隔，如 "600519,002415,300750"
    响应: { ok, stocks: [{code, name, dims: {key: value}}] }
    """
    code_list = [c.strip() for c in codes.split(",") if c.strip()][:4]
    if not code_list:
        return JSONResponse({"ok": False, "error": "请提供股票代码"})

    codes_ph = ",".join(["%s"] * len(code_list))

    # 基本信息
    info_rows = execute_query(
        f"SELECT stock_code, stock_name, market_cap, industry_l1 FROM stock_info WHERE stock_code IN ({codes_ph})",
        code_list,
    ) or []
    info_map = {r["stock_code"]: r for r in info_rows}

    # 最新涨跌幅（最新交易日）
    pct_rows = execute_query(
        f"""SELECT sd.stock_code, sd.change_pct, sd.trade_date
            FROM stock_daily sd
            JOIN (SELECT stock_code, MAX(trade_date) AS mx FROM stock_daily
                  WHERE stock_code IN ({codes_ph}) GROUP BY stock_code) t
              ON sd.stock_code = t.stock_code AND sd.trade_date = t.mx""",
        code_list + code_list,
    ) or []
    pct_map = {r["stock_code"]: float(r["change_pct"] or 0) for r in pct_rows}

    # 近30日主力净流入合计、近7日净流入
    flow30_rows = execute_query(
        f"""SELECT stock_code, ROUND(SUM(main_net_inflow)/1e8, 2) AS flow30
            FROM capital_flow
            WHERE stock_code IN ({codes_ph})
              AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 45 DAY)
            GROUP BY stock_code""",
        code_list,
    ) or []
    flow30_map = {r["stock_code"]: float(r["flow30"] or 0) for r in flow30_rows}

    flow7_rows = execute_query(
        f"""SELECT stock_code, ROUND(SUM(main_net_inflow)/1e8, 2) AS flow7
            FROM capital_flow
            WHERE stock_code IN ({codes_ph})
              AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 10 DAY)
            GROUP BY stock_code""",
        code_list,
    ) or []
    flow7_map = {r["stock_code"]: float(r["flow7"] or 0) for r in flow7_rows}

    # 近30日涨跌幅（最新close / 30日前close - 1）
    chg30_rows = execute_query(
        f"""SELECT a.stock_code,
                   ROUND((a.close / NULLIF(b.close, 0) - 1) * 100, 2) AS chg30
            FROM stock_daily a
            JOIN (SELECT stock_code, MAX(trade_date) AS mx FROM stock_daily
                  WHERE stock_code IN ({codes_ph}) GROUP BY stock_code) la
              ON a.stock_code = la.stock_code AND a.trade_date = la.mx
            JOIN (SELECT stock_code, close, trade_date FROM stock_daily) b
              ON b.stock_code = a.stock_code
            JOIN (SELECT stock_code, MAX(trade_date) AS mx30
                  FROM stock_daily
                  WHERE stock_code IN ({codes_ph})
                    AND trade_date <= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                  GROUP BY stock_code) lb
              ON b.stock_code = lb.stock_code AND b.trade_date = lb.mx30""",
        code_list * 3,
    ) or []
    chg30_map = {r["stock_code"]: float(r["chg30"] or 0) for r in chg30_rows}

    # KG：关联主题数、关联研报数（cleaned_items覆盖）
    kg_rows = execute_query(
        f"""SELECT ke.external_id AS code, COUNT(DISTINCT kr.id) AS theme_cnt
            FROM kg_entities ke
            JOIN kg_relationships kr ON ke.id = kr.source_entity_id OR ke.id = kr.target_entity_id
            WHERE ke.external_id IN ({codes_ph}) AND ke.entity_type = 'company'
            GROUP BY ke.external_id""",
        code_list,
    ) or []
    kg_map = {r["code"]: int(r["theme_cnt"] or 0) for r in kg_rows}

    report_rows = execute_query(
        f"""SELECT stock_code, COUNT(*) AS cnt FROM research_reports
            WHERE stock_code IN ({codes_ph}) GROUP BY stock_code""",
        code_list,
    ) or []
    report_map = {r["stock_code"]: int(r["cnt"] or 0) for r in report_rows}

    # 30日内是否有深度研究
    dr_rows = execute_query(
        f"""SELECT target, MAX(research_date) AS last_date
            FROM deep_research
            WHERE target IN ({codes_ph})
              AND research_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
            GROUP BY target""",
        code_list,
    ) or []
    dr_map = {r["target"]: r["last_date"] for r in dr_rows}

    result = []
    for code in code_list:
        info = info_map.get(code, {})
        mc = info.get("market_cap")
        mc_yi = round(float(mc) / 1e8, 1) if mc else None
        result.append({
            "code": code,
            "name": info.get("stock_name") or code,
            "has_research": code in dr_map,
            "last_research_date": str(dr_map[code]) if code in dr_map else None,
            "dims": {
                "market_cap":    mc_yi,            # 亿元
                "industry":      info.get("industry_l1"),
                "latest_pct":    pct_map.get(code),  # %
                "flow_30d":      flow30_map.get(code),  # 亿元
                "flow_7d":       flow7_map.get(code),   # 亿元
                "chg_30d":       chg30_map.get(code),   # %
                "kg_relations":  kg_map.get(code, 0),
                "report_count":  report_map.get(code, 0),
            },
        })

    return {"ok": True, "stocks": result}


# ── 页面入口（放最后，避免通配符拦截 /api/... 路由）────────────────

@router.get("/{sector_name:path}", response_class=HTMLResponse)
def sector_detail_page(request: Request, sector_name: str):
    name = unquote(sector_name)
    project_id = request.query_params.get("project", "")
    return templates.TemplateResponse("sector_detail.html", {
        "request": request,
        "sector_name": name,
        "project_id": project_id,
    })

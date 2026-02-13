"""每日概览 — FastAPI 路由 + 数据查询"""
import json
from datetime import datetime
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from utils.db_utils import execute_query

router = APIRouter(prefix="/overview", tags=["overview"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


# ── 数据查询函数 ──────────────────────────────────────────────

def get_metrics(date_str: str) -> dict:
    try:
        raw = execute_query(
            "SELECT COUNT(*) as cnt FROM raw_items WHERE date(fetched_at)=?", [date_str]
        )[0]["cnt"]
        cleaned = execute_query(
            "SELECT COUNT(*) as cnt FROM cleaned_items WHERE date(cleaned_at)=?", [date_str]
        )[0]["cnt"]
        failed = execute_query(
            "SELECT COUNT(*) as cnt FROM raw_items WHERE processing_status='failed' AND date(fetched_at)=?",
            [date_str],
        )[0]["cnt"]
        opportunities = execute_query(
            "SELECT COUNT(*) as cnt FROM investment_opportunities WHERE status='active'"
        )[0]["cnt"]
    except Exception:
        raw = cleaned = failed = opportunities = 0
    return {"raw": raw, "cleaned": cleaned, "failed": failed, "opportunities": opportunities}


def get_watchlist_alerts(date_str: str) -> list:
    try:
        stocks = execute_query(
            "SELECT stock_code, stock_name, watch_type FROM watchlist ORDER BY updated_at DESC LIMIT 6"
        )
        for s in stocks:
            # 最新涨跌幅
            daily = execute_query(
                "SELECT change_pct, close FROM stock_daily WHERE stock_code=? ORDER BY trade_date DESC LIMIT 1",
                [s["stock_code"]],
            )
            s["change_pct"] = daily[0]["change_pct"] if daily else None
            s["close"] = daily[0]["close"] if daily else None
            # 最新相关新闻
            news = execute_query(
                """SELECT ci.summary FROM item_companies ic
                   JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
                   WHERE ic.stock_code=? ORDER BY ci.cleaned_at DESC LIMIT 1""",
                [s["stock_code"]],
            )
            s["latest_news"] = news[0]["summary"] if news else None
        return stocks
    except Exception:
        return []


def get_opportunities(date_str: str) -> list:
    try:
        rows = execute_query(
            """SELECT ci.summary, ci.importance, ci.structured_json, ci.tags_json
               FROM cleaned_items ci
               WHERE ci.structured_json IS NOT NULL AND date(ci.cleaned_at)=?
               ORDER BY ci.importance DESC LIMIT 8""",
            [date_str],
        )
        results = []
        for r in rows:
            sj = json.loads(r.get("structured_json") or "{}")
            opp = sj.get("opportunity", {}).get("overall", {})
            level = opp.get("level", "")
            if level.count("⭐") >= 2 or r["importance"] >= 4:
                results.append({
                    "summary": r["summary"],
                    "importance": r["importance"],
                    "level": level,
                    "reason": opp.get("reason", ""),
                    "tags": json.loads(r.get("tags_json") or "[]")[:3],
                })
        return results[:5]
    except Exception:
        return []


def get_stock_attention(date_str: str) -> list:
    try:
        rows = execute_query(
            """SELECT ic.stock_code, ic.stock_name, COUNT(*) as freq
               FROM item_companies ic
               JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
               WHERE date(ci.cleaned_at)=?
               GROUP BY ic.stock_code
               ORDER BY freq DESC LIMIT 8""",
            [date_str],
        )
        if rows:
            max_freq = rows[0]["freq"]
            for r in rows:
                r["pct"] = int(r["freq"] / max_freq * 100) if max_freq else 0
        return rows
    except Exception:
        return []


def get_industry_heat(date_str: str) -> list:
    try:
        return execute_query(
            """SELECT industry_name, net_inflow, change_pct, leading_stock
               FROM industry_capital_flow WHERE trade_date=?
               ORDER BY net_inflow DESC LIMIT 12""",
            [date_str],
        )
    except Exception:
        return []


def get_macro_policy(date_str: str) -> list:
    try:
        rows = execute_query(
            """SELECT ci.summary, ci.importance, ci.sentiment, ci.tags_json
               FROM cleaned_items ci
               WHERE ci.event_type='macro_policy' AND date(ci.cleaned_at)=?
               ORDER BY ci.importance DESC LIMIT 6""",
            [date_str],
        )
        for r in rows:
            r["tags"] = json.loads(r.get("tags_json") or "[]")[:3]
        return rows
    except Exception:
        return []


def get_research_picks(date_str: str) -> list:
    try:
        rows = execute_query(
            """SELECT rr.stock_name, rr.broker_name, rr.rating, rr.target_price,
                      ci.importance, ci.summary
               FROM research_reports rr
               JOIN cleaned_items ci ON rr.cleaned_item_id=ci.id
               WHERE date(ci.cleaned_at)=?
               ORDER BY ci.importance DESC LIMIT 5""",
            [date_str],
        )
        if not rows:
            rows = execute_query(
                """SELECT ci.summary, ci.importance, ci.tags_json
                   FROM cleaned_items ci
                   WHERE ci.event_type='research_report' AND date(ci.cleaned_at)=?
                   ORDER BY ci.importance DESC LIMIT 5""",
                [date_str],
            )
        # Ensure all keys exist for template safety
        for r in rows:
            r.setdefault("broker_name", "")
            r.setdefault("rating", "")
            r.setdefault("target_price", None)
            r.setdefault("importance", 3)
            r.setdefault("summary", "")
        return rows
    except Exception:
        return []


def get_events(date_str: str) -> list:
    try:
        return execute_query(
            """SELECT ci.summary, ci.importance, ci.event_type, ci.sentiment,
                      ci.cleaned_at, ci.tags_json
               FROM cleaned_items ci
               WHERE ci.importance>=3 AND date(ci.cleaned_at)=?
               ORDER BY ci.cleaned_at DESC LIMIT 8""",
            [date_str],
        )
    except Exception:
        return []


def get_capital_flow(date_str: str) -> dict:
    try:
        northbound = execute_query(
            "SELECT trade_date, total_net, sh_net, sz_net FROM northbound_flow ORDER BY trade_date DESC LIMIT 5"
        )
        industry = execute_query(
            """SELECT industry_name, net_inflow FROM industry_capital_flow
               WHERE trade_date=? ORDER BY ABS(net_inflow) DESC LIMIT 5""",
            [date_str],
        )
        return {"northbound": list(reversed(northbound)), "industry": industry}
    except Exception:
        return {"northbound": [], "industry": []}


def get_risk_warnings(date_str: str) -> list:
    try:
        return execute_query(
            """SELECT ci.summary, ci.importance, ci.event_type, ci.tags_json, ci.impact_analysis
               FROM cleaned_items ci
               WHERE ci.sentiment='negative' AND ci.importance>=3 AND date(ci.cleaned_at)=?
               ORDER BY ci.importance DESC LIMIT 6""",
            [date_str],
        )
    except Exception:
        return []


# ── 页面路由 ──────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
async def overview_page(request: Request, date: str = None):
    date_str = date or datetime.now().strftime("%Y-%m-%d")

    ctx = {
        "request": request,
        "active_page": "overview",
        "date": date_str,
        "metrics": get_metrics(date_str),
        "watchlist_alerts": get_watchlist_alerts(date_str),
        "opportunities": get_opportunities(date_str),
        "stock_attention": get_stock_attention(date_str),
        "industry_heat": get_industry_heat(date_str),
        "macro_policy": get_macro_policy(date_str),
        "research_picks": get_research_picks(date_str),
        "events": get_events(date_str),
        "capital_flow": get_capital_flow(date_str),
        "risk_warnings": get_risk_warnings(date_str),
    }
    return templates.TemplateResponse("overview.html", ctx)


@router.post("/refresh", response_class=HTMLResponse)
async def refresh_dashboards(request: Request, date: str = None):
    date_str = date or datetime.now().strftime("%Y-%m-%d")
    error = None
    try:
        from dashboards.pipeline import generate_all_dashboards
        generate_all_dashboards(date_str)
    except Exception as e:
        error = str(e)

    ctx = {
        "request": request,
        "date": date_str,
        "refresh_error": error,
        "metrics": get_metrics(date_str),
        "watchlist_alerts": get_watchlist_alerts(date_str),
        "opportunities": get_opportunities(date_str),
        "stock_attention": get_stock_attention(date_str),
        "industry_heat": get_industry_heat(date_str),
        "macro_policy": get_macro_policy(date_str),
        "research_picks": get_research_picks(date_str),
        "events": get_events(date_str),
        "capital_flow": get_capital_flow(date_str),
        "risk_warnings": get_risk_warnings(date_str),
    }
    return templates.TemplateResponse("overview.html", ctx)

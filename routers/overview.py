"""每日概览 — FastAPI 路由 + 数据查询"""
import json
from datetime import datetime
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from utils.db_utils import execute_query
from utils.content_query import (
    query_content_summaries,
    query_stock_mentions_for_codes,
    get_kg_recommended_stocks,
    extract_keywords_from_summary,
)
from routers.market import INDEX_LIST

router = APIRouter(prefix="/overview", tags=["overview"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


# ── 数据查询函数 ──────────────────────────────────────────────

def _parse_structured_items(rows: list) -> list:
    """从 cleaned_items 行中解析出 structured fact/opinion/evidence
    优先读取新版 foe_tree（含观点类型/假设/全景评估），降级到 items 扁平格式"""
    results = []
    for r in rows:
        structured = json.loads(r.get("structured_json") or "{}") if r.get("structured_json") else {}

        parsed = []

        # 优先：新版 foe_tree 格式
        foe_tree = structured.get("foe_tree", [])
        if foe_tree:
            for f_node in foe_tree[:3]:
                fact_text = f_node.get("text", "")
                fact_level = f_node.get("fact_level", "")
                reliability = f_node.get("reliability", "")
                children = f_node.get("children", [])
                if children:
                    for o_node in children[:1]:  # 每棵树取第一个观点
                        opinion_text = o_node.get("text", "")
                        opinion_class = o_node.get("opinion_class_name", "")
                        opinion_source = o_node.get("source", "")
                        assumption = o_node.get("assumption", "")
                        evs = o_node.get("children", [])
                        evidence_text = evs[0].get("text", "") if evs else ""
                        parsed.append({
                            "fact": fact_text,
                            "fact_level": fact_level,
                            "reliability": reliability,
                            "opinion": opinion_text,
                            "opinion_class": opinion_class,
                            "opinion_source": opinion_source,
                            "assumption": assumption,
                            "evidence": evidence_text,
                        })
                else:
                    parsed.append({
                        "fact": fact_text,
                        "fact_level": fact_level,
                        "reliability": reliability,
                        "opinion": None,
                        "opinion_class": "",
                        "opinion_source": "",
                        "assumption": "",
                        "evidence": "",
                    })

        # 降级：旧版 items 扁平格式
        if not parsed:
            for item in structured.get("items", [])[:3]:
                parsed.append({
                    "fact": item.get("fact", ""),
                    "fact_level": "",
                    "reliability": "",
                    "opinion": item.get("opinion"),
                    "opinion_class": "",
                    "opinion_source": item.get("opinion_source"),
                    "assumption": item.get("assumption", ""),
                    "evidence": item.get("evidence", ""),
                })

        # 再降级：summary 文本
        if not parsed:
            parsed.append({
                "fact": r.get("summary", ""),
                "fact_level": "",
                "reliability": "",
                "opinion": r.get("impact_analysis") if r.get("impact_analysis") else None,
                "opinion_class": "",
                "opinion_source": None,
                "assumption": "",
                "evidence": "",
            })

        # 全景评估（新版独有）
        panorama = structured.get("panorama", {})

        results.append({
            "summary": r.get("summary", ""),
            "sentiment": r.get("sentiment", "neutral"),
            "importance": r.get("importance", 3),
            "details": parsed,
            "panorama": panorama,
        })
    return results


def _get_company_news(stock_code: str) -> list:
    """公司公告+财报+公司研报"""
    rows = execute_query("""
        SELECT cs.id, cs.summary, cs.fact_summary, cs.opinion_summary,
               cs.evidence_assessment, cs.info_gaps, cs.doc_type, et.publish_time
        FROM content_summaries cs
        JOIN extracted_texts et ON cs.extracted_text_id = et.id
        JOIN stock_mentions sm ON sm.extracted_text_id = et.id
        WHERE sm.stock_code = %s
          AND cs.doc_type IN ('announcement','financial_report','research_report')
          AND et.publish_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        ORDER BY et.publish_time DESC LIMIT 5
    """, [stock_code])
    return [dict(r) for r in (rows or [])]


def _get_industry_news(stock_code: str) -> list:
    """产业链+行业+主题新闻+产业研报"""
    ind = execute_query(
        "SELECT industry_l1, industry_l2 FROM stock_info WHERE stock_code=%s",
        [stock_code],
    )
    if not ind:
        return []
    ind_names = [v for v in [ind[0].get("industry_l1"), ind[0].get("industry_l2")] if v]
    if not ind_names:
        return []
    placeholders = ",".join(["%s"] * len(ind_names))
    rows = execute_query(f"""
        SELECT cs.id, cs.summary, cs.fact_summary, cs.opinion_summary,
               cs.evidence_assessment, cs.info_gaps, cs.doc_type, et.publish_time
        FROM content_summaries cs
        JOIN extracted_texts et ON cs.extracted_text_id = et.id
        WHERE cs.doc_type IN ('feature_news','flash_news','research_report','strategy_report')
          AND et.publish_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
          AND (cs.summary LIKE CONCAT('%%', %s, '%%')
               OR EXISTS (SELECT 1 FROM item_industries ii
                          JOIN cleaned_items ci ON ii.cleaned_item_id = ci.id
                          WHERE ii.industry_name IN ({placeholders})
                            AND ci.summary = cs.summary))
        ORDER BY et.publish_time DESC LIMIT 5
    """, [ind_names[0]] + ind_names)
    return [dict(r) for r in (rows or [])]


def _get_macro_news_for_stock(stock_code: str) -> list:
    """影响该公司和所在行业的宏观新闻"""
    rows = execute_query("""
        SELECT cs.id, cs.summary, cs.fact_summary, cs.opinion_summary,
               cs.evidence_assessment, cs.info_gaps, cs.doc_type, et.publish_time
        FROM content_summaries cs
        JOIN extracted_texts et ON cs.extracted_text_id = et.id
        WHERE cs.doc_type IN ('policy_doc','data_release','market_commentary','strategy_report')
          AND et.publish_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        ORDER BY et.publish_time DESC LIMIT 5
    """)
    return [dict(r) for r in (rows or [])]


def get_portfolio_holdings(date_str: str) -> list:
    """从默认收藏组(id=1)取出股票，查询行情和三栏新闻"""
    try:
        stocks = execute_query(
            """SELECT DISTINCT wls.stock_code, wls.stock_name
                FROM watchlist_list_stocks wls
                WHERE wls.list_id = 1 AND wls.status='active'
                LIMIT 20""",
        ) or []
        stocks = [dict(r) for r in stocks]

        for s in stocks:
            daily = execute_query(
                "SELECT change_pct, close, volume, amount, trade_date FROM stock_daily WHERE stock_code=%s ORDER BY trade_date DESC LIMIT 10",
                [s["stock_code"]],
            )
            if daily:
                s["market"] = {k: v for k, v in daily[0].items() if k != 'trade_date'}
                s["price_history"] = [float(d["close"]) for d in reversed(daily) if d.get("close") is not None]
            else:
                s["market"] = {}
                s["price_history"] = []

            s["company_news"] = _get_company_news(s["stock_code"])
            s["industry_news"] = _get_industry_news(s["stock_code"])
            s["macro_news"] = _get_macro_news_for_stock(s["stock_code"])

        return stocks
    except Exception:
        return []


def get_watchlist_alerts(date_str: str) -> list:
    """从 watchlist_lists (show_on_overview=1) 的各 list 中取出活跃股票，合并去重后查询行情和新闻"""
    try:
        # 查询 show_on_overview=1 的 list_id
        list_rows = execute_query(
            "SELECT id FROM watchlist_lists WHERE show_on_overview=1"
        ) or []
        list_ids = [r["id"] for r in list_rows]

        if not list_ids:
            # 降级：如果新表没有配置，回退到旧 watchlist 表
            stocks = execute_query(
                "SELECT stock_code, stock_name FROM watchlist ORDER BY updated_at DESC LIMIT 10"
            ) or []
        else:
            placeholders = ",".join(["%s"] * len(list_ids))
            rows = execute_query(
                f"""SELECT DISTINCT wls.stock_code, wls.stock_name
                    FROM watchlist_list_stocks wls
                    WHERE wls.list_id IN ({placeholders}) AND wls.status='active'
                    LIMIT 15""",
                list_ids,
            ) or []
            stocks = [dict(r) for r in rows]

        for s in stocks:
            # 行情 + 10日价格历史
            daily = execute_query(
                "SELECT change_pct, close, volume, amount, trade_date FROM stock_daily WHERE stock_code=%s ORDER BY trade_date DESC LIMIT 10",
                [s["stock_code"]],
            )
            if daily:
                s["market"] = {k: v for k, v in daily[0].items() if k != 'trade_date'}
                s["price_history"] = [float(d["close"]) for d in reversed(daily) if d.get("close") is not None]
            else:
                s["market"] = {}
                s["price_history"] = []

            # 公司新闻 — 从 stock_mentions → content_summaries
            mentions = query_stock_mentions_for_codes([s["stock_code"]], days=7)
            if mentions:
                ext_ids = list({m["extracted_text_id"] for m in mentions if m.get("extracted_text_id")})[:5]
                if ext_ids:
                    placeholders_m = ",".join(["%s"] * len(ext_ids))
                    cs_rows = execute_query(
                        f"""SELECT id, extracted_text_id, doc_type, summary, fact_summary,
                                   opinion_summary, evidence_assessment, info_gaps, created_at
                            FROM content_summaries
                            WHERE extracted_text_id IN ({placeholders_m})
                            ORDER BY created_at DESC LIMIT 3""",
                        ext_ids,
                    )
                    s["company_news"] = [dict(r) for r in (cs_rows or [])]
                else:
                    s["company_news"] = []
            else:
                s["company_news"] = []

            # 行业新闻
            industries = execute_query(
                "SELECT industry_l1, industry_l2 FROM stock_info WHERE stock_code=%s", [s["stock_code"]]
            )
            if industries:
                ind_names = [v for v in [industries[0].get("industry_l1"), industries[0].get("industry_l2")] if v]
                if ind_names:
                    placeholders2 = ",".join(["%s"] * len(ind_names))
                    industry_news = execute_query(f"""
                        SELECT ci.summary, ci.structured_json, ci.sentiment, ci.importance, ci.impact_analysis
                        FROM item_industries ii JOIN cleaned_items ci ON ii.cleaned_item_id=ci.id
                        WHERE ii.industry_name IN ({placeholders2}) AND ci.event_type='industry_news'
                        ORDER BY ci.cleaned_at DESC LIMIT 3
                    """, ind_names)
                    s["industry_news"] = _parse_structured_items(industry_news)
                else:
                    s["industry_news"] = []
            else:
                s["industry_news"] = []

            # 宏观新闻
            macro_news = execute_query("""
                SELECT ci.summary, ci.structured_json, ci.sentiment, ci.importance, ci.impact_analysis
                FROM cleaned_items ci WHERE ci.event_type='macro_policy'
                ORDER BY ci.cleaned_at DESC LIMIT 3
            """)
            s["macro_news"] = _parse_structured_items(macro_news)

        return stocks
    except Exception:
        return []


def get_industry_heat(date_str: str) -> dict:
    """行业资金热度：近7个交易日，同时提供净流入和毛流入/出两套口径"""
    try:
        dates = execute_query(
            "SELECT DISTINCT trade_date FROM capital_flow WHERE trade_date<=? ORDER BY trade_date DESC LIMIT 7",
            [date_str],
        )
        if not dates:
            return {"dates": [], "net": {}, "gross": {}, "daily_data": {}, "daily_gross": {}}

        date_list = [str(d["trade_date"]) for d in reversed(dates)]
        ndays = len(date_list)
        placeholders = ",".join(["%s"] * ndays)

        # 按行业+日期聚合：净流入 & 毛流入/毛流出
        all_rows = execute_query(f"""
            SELECT si.industry_l1 as industry_name, cf.trade_date,
                   SUM(cf.main_net_inflow) as net_inflow,
                   SUM(CASE WHEN cf.main_net_inflow > 0 THEN cf.main_net_inflow ELSE 0 END) as gross_inflow,
                   SUM(CASE WHEN cf.main_net_inflow < 0 THEN cf.main_net_inflow ELSE 0 END) as gross_outflow
            FROM capital_flow cf JOIN stock_info si ON cf.stock_code = si.stock_code
            WHERE cf.trade_date IN ({placeholders}) AND si.industry_l1 IS NOT NULL
            GROUP BY si.industry_l1, cf.trade_date
        """, date_list)

        daily_data = {d: {} for d in date_list}  # net per day
        daily_gross = {d: {} for d in date_list}  # {name: {inflow, outflow}} per day
        industry_net = {}
        industry_gross_in = {}
        industry_gross_out = {}
        market_daily_net = {d: 0.0 for d in date_list}

        for r in all_rows:
            name = r["industry_name"]
            d = str(r["trade_date"])
            net = float(r["net_inflow"] or 0)
            gin = float(r["gross_inflow"] or 0)
            gout = float(r["gross_outflow"] or 0)
            if d in daily_data:
                daily_data[d][name] = net
                daily_gross[d][name] = {"inflow": gin, "outflow": gout}
                market_daily_net[d] += net
            industry_net.setdefault(name, 0.0)
            industry_net[name] += net
            industry_gross_in.setdefault(name, 0.0)
            industry_gross_in[name] += gin
            industry_gross_out.setdefault(name, 0.0)
            industry_gross_out[name] += gout

        # ── 净流入口径 ──
        total_net_inflow = sum(v for v in industry_net.values() if v > 0) or 1
        total_net_outflow = sum(abs(v) for v in industry_net.values() if v < 0) or 1
        sorted_net = sorted(industry_net.items(), key=lambda x: x[1], reverse=True)
        net_inflow_top5 = [{"name": n, "total": v, "pct": v / total_net_inflow * 100}
                           for n, v in sorted_net[:5] if v > 0]
        net_outflow_top5 = sorted([{"name": n, "total": v, "pct": abs(v) / total_net_outflow * 100}
                                   for n, v in sorted_net if v < 0], key=lambda x: x["total"])[:5]

        # ── 毛流入/出口径 ──
        total_gross_in = sum(industry_gross_in.values()) or 1
        total_gross_out = sum(abs(v) for v in industry_gross_out.values()) or 1
        sorted_gin = sorted(industry_gross_in.items(), key=lambda x: x[1], reverse=True)
        gross_inflow_top5 = [{"name": n, "total": v, "pct": v / total_gross_in * 100}
                             for n, v in sorted_gin[:5] if v > 0]
        sorted_gout = sorted(industry_gross_out.items(), key=lambda x: x[1])
        gross_outflow_top5 = [{"name": n, "total": v, "pct": abs(v) / total_gross_out * 100}
                              for n, v in sorted_gout[:5] if v < 0]

        return {
            "dates": date_list,
            "net": {"inflow_top5": net_inflow_top5, "outflow_top5": net_outflow_top5},
            "gross": {"inflow_top5": gross_inflow_top5, "outflow_top5": gross_outflow_top5},
            "daily_data": daily_data,
            "daily_gross": daily_gross,
            "market_daily_net": market_daily_net,
        }
    except Exception:
        return {"dates": [], "net": {}, "gross": {}, "daily_data": {}, "daily_gross": {}}


def get_macro_news(date_str: str) -> list:
    """宏观新闻：从 content_summaries 查询，附 KG 推荐股票"""
    try:
        rows = query_content_summaries(
            doc_types=["policy_doc", "data_release", "strategy_report", "market_commentary"],
            date_str=date_str,
            limit=6,
            fallback_days=30,
        )
        for r in rows:
            kw = extract_keywords_from_summary(r.get("summary") or r.get("fact_summary") or "")
            r["stock_recs"] = get_kg_recommended_stocks(kw, limit=3)
        return rows
    except Exception:
        return []


def get_research_picks(date_str: str) -> list:
    """研报精选：从 content_summaries 查询 research_report，附 KG 推荐股票"""
    try:
        rows = query_content_summaries(
            doc_types=["research_report"],
            date_str=date_str,
            limit=5,
            fallback_days=30,
        )
        for r in rows:
            kw = extract_keywords_from_summary(r.get("summary") or r.get("fact_summary") or "")
            r["stock_recs"] = get_kg_recommended_stocks(kw, limit=3)
        return rows
    except Exception:
        return []


def get_events(date_str: str) -> list:
    """事件跟踪：从 content_summaries 查询，附 KG 推荐股票"""
    try:
        rows = query_content_summaries(
            doc_types=["announcement", "feature_news", "flash_news"],
            date_str=date_str,
            limit=8,
            fallback_days=30,
        )
        for r in rows:
            kw = extract_keywords_from_summary(r.get("summary") or r.get("fact_summary") or "")
            r["stock_recs"] = get_kg_recommended_stocks(kw, limit=3)
        return rows
    except Exception:
        return []



def get_capital_insight(date_str: str, industry_heat: dict) -> list:
    """资金热度解读：根据热力图结果，查找宏观资金面+top行业的关联新闻"""
    insights = []
    try:
        # 1. 宏观资金面新闻
        macro_rows = execute_query("""
            SELECT ci.summary, ci.sentiment, ci.importance, ci.structured_json
            FROM cleaned_items ci
            WHERE ci.event_type IN ('macro_policy', 'macro_event')
            ORDER BY ci.cleaned_at DESC LIMIT 4
        """)
        for r in macro_rows:
            structured = json.loads(r.get("structured_json") or "{}") if r.get("structured_json") else {}
            items = structured.get("items", [])
            item0 = items[0] if items else {}
            fact = item0.get("fact", r.get("summary", ""))
            opinion = item0.get("opinion", "")
            # 拼接原因：evidence + logic_chain / assumption
            reason_parts = []
            if item0.get("evidence"):
                reason_parts.append(item0["evidence"])
            if item0.get("logic_chain"):
                reason_parts.append(item0["logic_chain"])
            elif item0.get("assumption"):
                reason_parts.append(item0["assumption"])
            reason = "；".join(reason_parts) if reason_parts else ""
            insights.append({
                "category": "宏观资金面",
                "sentiment": r.get("sentiment", "neutral"),
                "importance": r.get("importance", 3),
                "fact": fact,
                "opinion": opinion,
                "reason": reason,
            })

        # 2. Top 行业相关新闻（从热力图的净流入/流出 top 行业中取）
        top_industries = []
        net_data = industry_heat.get("net", {})
        for ind in net_data.get("inflow_top5", [])[:3]:
            top_industries.append(ind["name"])
        for ind in net_data.get("outflow_top5", [])[:3]:
            top_industries.append(ind["name"])

        if top_industries:
            placeholders = ",".join(["%s"] * len(top_industries))
            ind_rows = execute_query(f"""
                SELECT ii.industry_name, ci.summary, ci.sentiment, ci.importance, ci.structured_json
                FROM item_industries ii JOIN cleaned_items ci ON ii.cleaned_item_id=ci.id
                WHERE ii.industry_name IN ({placeholders})
                ORDER BY ci.importance DESC, ci.cleaned_at DESC LIMIT 6
            """, top_industries)
            for r in ind_rows:
                structured = json.loads(r.get("structured_json") or "{}") if r.get("structured_json") else {}
                items = structured.get("items", [])
                item0 = items[0] if items else {}
                fact = item0.get("fact", r.get("summary", ""))
                opinion = item0.get("opinion", "")
                # 拼接原因：evidence + logic_chain / assumption
                reason_parts = []
                if item0.get("evidence"):
                    reason_parts.append(item0["evidence"])
                if item0.get("logic_chain"):
                    reason_parts.append(item0["logic_chain"])
                elif item0.get("assumption"):
                    reason_parts.append(item0["assumption"])
                reason = "；".join(reason_parts) if reason_parts else ""
                insights.append({
                    "category": r.get("industry_name", "行业"),
                    "sentiment": r.get("sentiment", "neutral"),
                    "importance": r.get("importance", 3),
                    "fact": fact,
                    "opinion": opinion,
                    "reason": reason,
                })

        return insights
    except Exception:
        return []


def get_risk_warnings(date_str: str) -> list:
    """风险预警：从 content_summaries 过滤负面关键词"""
    try:
        rows = query_content_summaries(
            doc_types=["policy_doc", "data_release", "strategy_report",
                       "market_commentary", "announcement", "feature_news", "flash_news"],
            date_str=date_str,
            limit=20,
            fallback_days=30,
        )
        risk_kw = {"风险", "下跌", "利空", "警告", "暴跌", "崩盘", "违约", "亏损", "减持", "退市"}
        result = []
        for r in rows:
            text = (r.get("summary") or "") + (r.get("fact_summary") or "") + (r.get("opinion_summary") or "")
            if any(kw in text for kw in risk_kw):
                result.append(r)
            if len(result) >= 6:
                break
        return result
    except Exception:
        return []


def _get_all_watchlist_lists() -> list:
    """返回所有 watchlist_lists 及各 list 的股票数，用于概览页配置面板"""
    try:
        rows = execute_query(
            """SELECT wl.id, wl.list_name, wl.list_type, wl.show_on_overview,
                      COUNT(wls.id) as stock_count
               FROM watchlist_lists wl
               LEFT JOIN watchlist_list_stocks wls ON wl.id=wls.list_id AND wls.status='active'
               GROUP BY wl.id
               ORDER BY wl.sort_order, wl.id"""
        ) or []
        return [dict(r) for r in rows]
    except Exception:
        return []


# ── 页面路由 ──────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
def overview_page(request: Request, date: str = None):
    date_str = date or datetime.now().strftime("%Y-%m-%d")

    industry_heat = get_industry_heat(date_str)
    ctx = {
        "request": request,
        "active_page": "overview",
        "date": date_str,
        "portfolio_holdings": get_portfolio_holdings(date_str),
        "index_list": INDEX_LIST,
        "industry_heat": industry_heat,
        "capital_insight": get_capital_insight(date_str, industry_heat),
    }
    return templates.TemplateResponse("overview.html", ctx)


@router.get("/api/news-feed")
def api_news_feed(days: int = 3):
    """新闻聚合器：四容器，最近 days 天，各20条，按 importance DESC"""
    from fastapi.responses import JSONResponse
    try:
        base_select = """
            SELECT cs.id, cs.doc_type, cs.summary, cs.fact_summary,
                   cs.opinion_summary, cs.evidence_assessment, cs.info_gaps,
                   et.publish_time,
                   ci.importance, ci.sentiment, ci.event_type
            FROM content_summaries cs
            JOIN extracted_texts et ON cs.extracted_text_id = et.id
            LEFT JOIN cleaned_items ci ON ci.summary = cs.summary
            WHERE et.publish_time >= DATE_SUB(NOW(), INTERVAL %s DAY)
        """

        macro = execute_query(f"""
            {base_select}
              AND cs.doc_type IN ('policy_doc','data_release','market_commentary','strategy_report')
            ORDER BY COALESCE(ci.importance, 3) DESC, et.publish_time DESC LIMIT 20
        """, [days]) or []

        industry = execute_query(f"""
            {base_select}
              AND cs.doc_type IN ('feature_news','flash_news','research_report')
              AND EXISTS (SELECT 1 FROM item_industries ii
                          JOIN cleaned_items ci2 ON ii.cleaned_item_id = ci2.id
                          WHERE ci2.summary = cs.summary)
            ORDER BY COALESCE(ci.importance, 3) DESC, et.publish_time DESC LIMIT 20
        """, [days]) or []

        stock = execute_query(f"""
            {base_select}
              AND cs.doc_type IN ('announcement','financial_report','feature_news','flash_news')
              AND EXISTS (SELECT 1 FROM stock_mentions sm
                          WHERE sm.extracted_text_id = cs.extracted_text_id)
            ORDER BY COALESCE(ci.importance, 3) DESC, et.publish_time DESC LIMIT 20
        """, [days]) or []

        risk = execute_query(f"""
            {base_select}
              AND (ci.sentiment = 'negative'
                   OR cs.summary LIKE '%%风险%%' OR cs.summary LIKE '%%下跌%%'
                   OR cs.summary LIKE '%%利空%%' OR cs.summary LIKE '%%减持%%'
                   OR cs.summary LIKE '%%违约%%' OR cs.summary LIKE '%%退市%%')
            ORDER BY COALESCE(ci.importance, 3) DESC, et.publish_time DESC LIMIT 20
        """, [days]) or []

        def _serialize(rows):
            result = []
            for r in rows:
                d = dict(r)
                if d.get("publish_time") and hasattr(d["publish_time"], "strftime"):
                    d["publish_time"] = d["publish_time"].strftime("%Y-%m-%d %H:%M")
                result.append(d)
            return result

        return JSONResponse({
            "macro": _serialize(macro),
            "industry": _serialize(industry),
            "stock": _serialize(stock),
            "risk": _serialize(risk),
        })
    except Exception:
        import traceback
        traceback.print_exc()
        return JSONResponse({"macro": [], "industry": [], "stock": [], "risk": []})@router.post("/refresh", response_class=HTMLResponse)
def refresh_dashboards(request: Request, date: str = None):
    date_str = date or datetime.now().strftime("%Y-%m-%d")
    error = None
    try:
        from dashboards.pipeline import generate_all_dashboards
        generate_all_dashboards(date_str)
    except Exception as e:
        error = str(e)

    industry_heat = get_industry_heat(date_str)
    ctx = {
        "request": request,
        "date": date_str,
        "refresh_error": error,
        "portfolio_holdings": get_portfolio_holdings(date_str),
        "index_list": INDEX_LIST,
        "industry_heat": industry_heat,
        "capital_insight": get_capital_insight(date_str, industry_heat),
    }
    return templates.TemplateResponse("overview.html", ctx)

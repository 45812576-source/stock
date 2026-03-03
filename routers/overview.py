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


def _flatten_cleaned_rows(rows: list) -> list:
    """将 cleaned_items 行解析为前端 renderFoeList 期望的 flat 结构：
    {summary, fact, opinion, evidence, event_type, sentiment, importance, publish_time}"""
    result = []
    for r in (rows or []):
        structured = json.loads(r.get("structured_json") or "{}") if r.get("structured_json") else {}
        # 取第一个 foe_tree 节点
        foe_tree = structured.get("foe_tree", [])
        fact, opinion, evidence = "", "", ""
        if foe_tree:
            node = foe_tree[0]
            fact = node.get("text", "")
            children = node.get("children", [])
            if children:
                opinion = children[0].get("text", "")
                evs = children[0].get("children", [])
                evidence = evs[0].get("text", "") if evs else ""
        # 降级到 items 格式
        if not fact:
            items = structured.get("items", [])
            if items:
                fact = items[0].get("fact", "")
                opinion = items[0].get("opinion", "") or ""
                evidence = items[0].get("evidence", "") or ""
        # 再降级到 summary
        if not fact:
            fact = r.get("summary", "")
        pt = r.get("publish_time")
        if pt and hasattr(pt, "strftime"):
            pt = pt.strftime("%Y-%m-%d %H:%M")
        result.append({
            "id": r.get("id"),
            "summary": r.get("summary", "") or fact,
            "fact": fact,
            "opinion": opinion,
            "evidence": evidence,
            "event_type": r.get("event_type", ""),
            "sentiment": r.get("sentiment", "neutral"),
            "importance": r.get("importance", 3),
            "publish_time": pt,
        })
    return result


def _get_company_news(stock_code: str) -> list:
    """公司公告+财报+公司研报 — 从 cleaned_items via item_companies"""
    rows = execute_query("""
        SELECT ci.id, ci.summary, ci.importance, ci.sentiment, ci.event_type,
               ci.structured_json, ci.cleaned_at as publish_time
        FROM item_companies ic
        JOIN cleaned_items ci ON ic.cleaned_item_id = ci.id
        WHERE ic.stock_code = %s
          AND ci.event_type IN ('company_event','earnings','research_report')
          AND ci.cleaned_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        ORDER BY ci.importance DESC, ci.cleaned_at DESC LIMIT 5
    """, [stock_code])
    return _flatten_cleaned_rows(rows or [])


def _get_industry_news(stock_code: str) -> list:
    """产业链+行业+主题新闻 — 从 cleaned_items via item_industries"""
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
        SELECT ci.id, ci.summary, ci.importance, ci.sentiment, ci.event_type,
               ci.structured_json, ci.cleaned_at as publish_time
        FROM item_industries ii
        JOIN cleaned_items ci ON ii.cleaned_item_id = ci.id
        WHERE ii.industry_name IN ({placeholders})
          AND ci.event_type IN ('industry_news','research_report')
          AND ci.cleaned_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        ORDER BY ci.importance DESC, ci.cleaned_at DESC LIMIT 5
    """, ind_names)
    return _flatten_cleaned_rows(rows or [])


def _get_macro_news_for_stock(stock_code: str) -> list:
    """影响该公司和所在行业的宏观新闻 — 从 cleaned_items"""
    rows = execute_query("""
        SELECT ci.id, ci.summary, ci.importance, ci.sentiment, ci.event_type,
               ci.structured_json, ci.cleaned_at as publish_time
        FROM cleaned_items ci
        WHERE ci.event_type IN ('macro_policy','market')
          AND ci.cleaned_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        ORDER BY ci.importance DESC, ci.cleaned_at DESC LIMIT 5
    """)
    return _flatten_cleaned_rows(rows or [])


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
    """行业资金热度：用 akshare 实时拉取今日行业资金流向排行。

    akshare 只提供当日单期快照，输出仍保持与热力图模板兼容的结构：
    dates 仅含今日一列，net/gross top5 基于今日数据，daily_data/daily_gross 含今日数据。
    """
    import time
    from config import AKSHARE_DELAY
    try:
        import akshare as ak
        time.sleep(AKSHARE_DELAY)

        today = datetime.now().strftime("%Y-%m-%d")
        date_list = [today]

        df = None
        # 优先尝试带 sector_type 参数的接口
        try:
            df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流向")
        except TypeError:
            # 部分 akshare 版本不支持 sector_type 参数
            try:
                df = ak.stock_sector_fund_flow_rank(indicator="今日")
            except Exception:
                pass
        except Exception:
            pass

        if df is None or df.empty:
            return {"dates": [], "net": {}, "gross": {}, "daily_data": {}, "daily_gross": {}}

        # 解析各行
        industry_net = {}       # name → 净流入金额（元）
        industry_gross_in = {}  # name → 毛流入（净流入为正的部分，元）
        industry_gross_out = {} # name → 毛流出（净流入为负的部分，元）

        for _, row in df.iterrows():
            name = str(row.get("名称", "")).strip()
            if not name:
                continue
            # akshare 返回单位为万元，统一转为元与热力图的 /1e8 显示逻辑兼容
            raw_net = row.get("今日主力净流入-净额", row.get("主力净流入-净额", 0))
            try:
                net_val = float(str(raw_net).replace(",", "") or 0) * 1e4  # 万元 → 元
            except (ValueError, TypeError):
                net_val = 0.0

            industry_net[name] = net_val
            if net_val >= 0:
                industry_gross_in[name] = net_val
                industry_gross_out[name] = 0.0
            else:
                industry_gross_in[name] = 0.0
                industry_gross_out[name] = net_val

        if not industry_net:
            return {"dates": [], "net": {}, "gross": {}, "daily_data": {}, "daily_gross": {}}

        # 行业市值（本地 stock_info，作为参考维度）
        industry_mktcap = {}
        try:
            mktcap_rows = execute_query("""
                SELECT industry_l1 as industry_name, SUM(market_cap) as total_mktcap
                FROM stock_info
                WHERE industry_l1 IS NOT NULL AND market_cap > 0
                GROUP BY industry_l1
            """) or []
            industry_mktcap = {r["industry_name"]: float(r["total_mktcap"] or 0) / 1e8
                               for r in mktcap_rows}
        except Exception:
            pass

        # ── daily_data / daily_gross（仅今日一列）──
        daily_data = {today: {name: v for name, v in industry_net.items()}}
        daily_gross = {
            today: {
                name: {"inflow": industry_gross_in.get(name, 0),
                       "outflow": industry_gross_out.get(name, 0)}
                for name in industry_net
            }
        }
        market_daily_net = {today: sum(industry_net.values())}

        # ── 净流入 top5 / 流出 top5 ──
        total_net_inflow = sum(v for v in industry_net.values() if v > 0) or 1
        total_net_outflow = sum(abs(v) for v in industry_net.values() if v < 0) or 1
        sorted_net = sorted(industry_net.items(), key=lambda x: x[1], reverse=True)
        net_inflow_top5 = [
            {"name": n, "total": v, "pct": v / total_net_inflow * 100,
             "mktcap": industry_mktcap.get(n, 0)}
            for n, v in sorted_net[:5] if v > 0
        ]
        net_outflow_top5 = sorted(
            [{"name": n, "total": v, "pct": abs(v) / total_net_outflow * 100,
              "mktcap": industry_mktcap.get(n, 0)}
             for n, v in sorted_net if v < 0],
            key=lambda x: x["total"],
        )[:5]

        # ── 毛流入/出 top5（akshare 单日快照中净流入≥0即为毛流入）──
        total_gross_in = sum(industry_gross_in.values()) or 1
        total_gross_out = sum(abs(v) for v in industry_gross_out.values()) or 1
        sorted_gin = sorted(industry_gross_in.items(), key=lambda x: x[1], reverse=True)
        gross_inflow_top5 = [
            {"name": n, "total": v, "pct": v / total_gross_in * 100,
             "mktcap": industry_mktcap.get(n, 0)}
            for n, v in sorted_gin[:5] if v > 0
        ]
        sorted_gout = sorted(industry_gross_out.items(), key=lambda x: x[1])
        gross_outflow_top5 = [
            {"name": n, "total": v, "pct": abs(v) / total_gross_out * 100,
             "mktcap": industry_mktcap.get(n, 0)}
            for n, v in sorted_gout[:5] if v < 0
        ]

        return {
            "dates": date_list,
            "net": {"inflow_top5": net_inflow_top5, "outflow_top5": net_outflow_top5},
            "gross": {"inflow_top5": gross_inflow_top5, "outflow_top5": gross_outflow_top5},
            "daily_data": daily_data,
            "daily_gross": daily_gross,
            "market_daily_net": market_daily_net,
        }
    except Exception:
        import traceback
        traceback.print_exc()
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
        # 1. 宏观资金面新闻 — 从 content_summaries 取最新
        macro_rows = query_content_summaries(
            doc_types=["policy_doc", "data_release", "market_commentary", "strategy_report"],
            date_str=None,
            limit=4,
            fallback_days=7,
        )
        for r in macro_rows:
            fact = r.get("fact_summary") or r.get("summary") or ""
            opinion = r.get("opinion_summary") or ""
            reason = r.get("evidence_assessment") or ""
            insights.append({
                "category": "宏观资金面",
                "sentiment": "neutral",
                "importance": 3,
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

        # 3. 大股东/高管增减持信号（最近7天，金额 > 500万）
        try:
            insider_rows = execute_query(
                """SELECT stock_code, stock_name, person_name, person_role,
                          direction, trade_amount, trade_date
                   FROM insider_trading
                   WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                     AND trade_amount IS NOT NULL AND trade_amount > 500
                   ORDER BY trade_amount DESC LIMIT 5"""
            ) or []
            if insider_rows:
                buy_rows = [r for r in insider_rows if "增持" in (r.get("direction") or "")]
                sell_rows = [r for r in insider_rows if "减持" in (r.get("direction") or "")]
                parts = []
                if buy_rows:
                    names = "、".join(f"{r['stock_name']}({r['stock_code']})" for r in buy_rows[:3])
                    total = sum(r.get("trade_amount", 0) or 0 for r in buy_rows)
                    parts.append(f"近7日大股东增持：{names}，合计{total:.0f}万元")
                if sell_rows:
                    names = "、".join(f"{r['stock_name']}({r['stock_code']})" for r in sell_rows[:3])
                    total = sum(r.get("trade_amount", 0) or 0 for r in sell_rows)
                    parts.append(f"近7日大股东减持：{names}，合计{total:.0f}万元")
                if parts:
                    insights.append({
                        "category": "股东动向",
                        "sentiment": "positive" if buy_rows and not sell_rows else (
                            "negative" if sell_rows and not buy_rows else "neutral"
                        ),
                        "importance": 4,
                        "fact": "；".join(parts),
                        "opinion": "",
                        "reason": "大股东增减持是反映公司内部人信心的重要信号",
                    })
        except Exception:
            pass

        return insights
    except Exception:
        return []


def get_risk_warnings(date_str: str) -> list:
    """风险预警：从 content_summaries 过滤负面关键词 + 大额减持信号"""
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

        # 大额减持信号（近7天，金额 > 500万元）
        try:
            sell_rows = execute_query(
                """SELECT stock_code, stock_name, person_name, person_role,
                          trade_amount, trade_date
                   FROM insider_trading
                   WHERE direction='减持'
                     AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                     AND trade_amount IS NOT NULL AND trade_amount > 500
                   ORDER BY trade_amount DESC LIMIT 5"""
            ) or []
            if sell_rows:
                for r in sell_rows:
                    result.append({
                        "summary": (
                            f"【减持预警】{r.get('stock_name','')}"
                            f"({r.get('stock_code','')}) "
                            f"{r.get('person_name','')}({r.get('person_role','')}) "
                            f"减持 {r.get('trade_amount',0):.0f}万元 "
                            f"（{r.get('trade_date','')}）"
                        ),
                        "fact_summary": "",
                        "opinion_summary": "",
                        "sentiment": "negative",
                        "importance": 4,
                        "_source": "insider_trading",
                    })
        except Exception:
            pass

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
def api_news_feed(days: int = 7):
    """新闻聚合器：四容器，最近 days 天，各20条"""
    from fastapi.responses import JSONResponse
    try:
        base_select = """
            SELECT cs.id, cs.doc_type,
                   cs.summary, cs.fact_summary, cs.opinion_summary,
                   cs.evidence_assessment, cs.info_gaps,
                   COALESCE(et.publish_time, cs.created_at) as publish_time
            FROM content_summaries cs
            LEFT JOIN extracted_texts et ON cs.extracted_text_id = et.id
            WHERE cs.created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
        """

        # 宏观：政策/数据/市场评述/策略报告
        macro = execute_query(
            base_select + " AND cs.doc_type IN ('policy_doc','data_release','market_commentary','strategy_report')"
                        " ORDER BY cs.created_at DESC LIMIT 20",
            [days]
        ) or []

        # 行业：研报+特稿+简讯（行业分析类）
        industry = execute_query(
            base_select + " AND cs.doc_type IN ('research_report','feature_news','flash_news','digest_news')"
                        " ORDER BY cs.created_at DESC LIMIT 20",
            [days]
        ) or []

        # 个股：公告/财报/路演纪要
        stock = execute_query(
            base_select + " AND cs.doc_type IN ('announcement','financial_report','roadshow_notes')"
                        " ORDER BY cs.created_at DESC LIMIT 20",
            [days]
        ) or []

        # 风险：全类型中包含负面关键词的条目
        risk_kw_filter = (
            " AND (cs.summary LIKE '%%风险%%' OR cs.summary LIKE '%%下跌%%'"
            " OR cs.summary LIKE '%%利空%%' OR cs.summary LIKE '%%减持%%'"
            " OR cs.summary LIKE '%%违约%%' OR cs.summary LIKE '%%退市%%'"
            " OR cs.fact_summary LIKE '%%风险%%' OR cs.fact_summary LIKE '%%下跌%%'"
            " OR cs.fact_summary LIKE '%%利空%%')"
        )
        risk = execute_query(
            base_select + risk_kw_filter + " ORDER BY cs.created_at DESC LIMIT 20",
            [days]
        ) or []

        def _serialize(rows):
            result = []
            for r in rows:
                d = dict(r)
                pt = d.get("publish_time")
                if pt and hasattr(pt, "strftime"):
                    d["publish_time"] = pt.strftime("%Y-%m-%d %H:%M")
                # event_type 别名（前端模板同时读 event_type 和 doc_type）
                d["event_type"] = d.get("doc_type") or ""
                # importance 在 content_summaries 中无此列，给默认值 3
                d.setdefault("importance", 3)
                # 确保所有文本字段都不为 None
                for fld in ("summary", "fact_summary", "opinion_summary",
                            "evidence_assessment", "info_gaps"):
                    d[fld] = d.get(fld) or ""
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
        return JSONResponse({"macro": [], "industry": [], "stock": [], "risk": []})


def _get_portfolio_stock_codes() -> list:
    """获取用户持仓和默认收藏组的股票代码列表"""
    codes = set()
    rows = execute_query(
        "SELECT DISTINCT stock_code FROM watchlist_list_stocks WHERE list_id=1 AND status='active' LIMIT 20"
    ) or []
    for r in rows:
        codes.add(r["stock_code"])
    rows2 = execute_query(
        "SELECT stock_code FROM holding_positions WHERE status='open'"
    ) or []
    for r in rows2:
        codes.add(r["stock_code"])
    return list(codes)


@router.post("/api/chat/send")
async def api_overview_chat_send(request: Request):
    """Overview AI 聊天 — 混合模式：pre-retrieval hybrid_search + DeepSeek Agent"""
    from fastapi.responses import JSONResponse
    import asyncio
    from agent.executor import run_agent
    from retrieval.hybrid import hybrid_search

    try:
        body = await request.json()
        message = (body.get("message") or "").strip()
        history = body.get("history") or []
        if not message:
            return JSONResponse({"ok": False, "error": "消息为空"})

        # Step 1: Pre-retrieval — hybrid_search 预检索
        try:
            hr = hybrid_search(message, top_k=6)
            pre_context = hr.merged_context[:3000] if hr.merged_context else ""
        except Exception:
            pre_context = ""

        # Step 2: 构建 extra_context
        extra_context = ""
        if pre_context:
            extra_context += f"## 系统检索到的相关信息\n{pre_context}\n\n"

        portfolio_codes = _get_portfolio_stock_codes()
        if portfolio_codes:
            extra_context += f"## 用户持仓股票\n{', '.join(portfolio_codes)}\n\n"

        extra_context += """## 新闻解读框架
当用户询问新闻影响时，请按以下层次分析：
1. 宏观层面：政策/经济环境影响
2. 行业层面：产业链上下游传导
3. 个股层面：对用户持仓的具体影响
对每层给出事实依据，区分确定性影响和不确定性推测。"""

        loop = asyncio.get_event_loop()
        reply = await loop.run_in_executor(
            None, lambda: run_agent(message, history=history[-20:], extra_context=extra_context)
        )

        # 解析推荐股票（兼容旧格式）
        recommendations = []
        import re
        m = re.search(r"```推荐股票\s*\n(.*?)\n```", reply, re.DOTALL)
        if m:
            try:
                recommendations = json.loads(m.group(1))
            except Exception:
                pass

        return JSONResponse({
            "ok": True,
            "reply": reply,
            "recommendations": recommendations,
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})



@router.get("/api/driver-alerts")
def api_driver_alerts():
    """获取持仓股票的驱动因子监控结果"""
    from fastapi.responses import JSONResponse
    try:
        from research.driver_monitor import get_portfolio_drivers, match_drivers_to_news
        codes = _get_portfolio_stock_codes()
        if not codes:
            return JSONResponse({"alerts": []})
        drivers = get_portfolio_drivers(codes)
        if not drivers:
            return JSONResponse({"alerts": []})
        alerts = match_drivers_to_news(drivers, days=3)
        return JSONResponse({"alerts": [a for a in alerts if a.get("has_news")]})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"alerts": [], "error": str(e)})


@router.get("/api/stock-name/{stock_code}")
def api_stock_name(stock_code: str):
    from fastapi.responses import JSONResponse
    row = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=%s", [stock_code])
    name = row[0]["stock_name"] if row else None
    return JSONResponse({"code": stock_code, "name": name})


@router.get("/api/tag-groups")
def api_tag_groups():
    from fastapi.responses import JSONResponse
    import json as _json
    rows = execute_query(
        "SELECT id, group_name, tags_json FROM tag_groups ORDER BY total_frequency DESC LIMIT 50"
    ) or []
    result = []
    for r in rows:
        tags = []
        try:
            tags = _json.loads(r.get("tags_json") or "[]")
        except Exception:
            pass
        result.append({"id": r["id"], "name": r["group_name"], "tags": tags})
    return JSONResponse(result)
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

"""Agent 工具注册表

每个工具 = Python 执行函数 + OpenAI function schema。
工具函数内部调用现有的 execute_query / kg_query 函数，
返回值统一为 JSON 字符串（DeepSeek tool result 要求字符串格式）。
"""
import json
import logging
from datetime import date, datetime

logger = logging.getLogger(__name__)


def _json(obj) -> str:
    """序列化为 JSON 字符串，处理 date/datetime 类型"""
    def _default(o):
        if isinstance(o, (date, datetime)):
            return str(o)
        raise TypeError(f"Object of type {type(o)} is not JSON serializable")
    return json.dumps(obj, ensure_ascii=False, default=_default)


def _safe_query(sql, params=None, limit=100):
    """安全只读查询，限制返回行数"""
    from utils.db_utils import execute_query
    rows = execute_query(sql, params)
    return rows[:limit] if rows else []


# ─────────────────────────────────────────────
# 工具函数实现
# ─────────────────────────────────────────────

def query_stock_info(stock_code: str) -> str:
    """查询股票基本信息"""
    rows = _safe_query(
        "SELECT * FROM stock_info WHERE stock_code=%s",
        [stock_code]
    )
    if not rows:
        return _json({"error": f"未找到股票 {stock_code} 的基本信息"})
    return _json(rows[0])


def query_stock_daily(stock_code: str, days: int = 30) -> str:
    """查询股票日K线行情"""
    rows = _safe_query(
        "SELECT trade_date, open, high, low, close, volume, amount, "
        "turnover_rate, change_pct FROM stock_daily "
        "WHERE stock_code=%s AND trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY) "
        "ORDER BY trade_date DESC",
        [stock_code, days], limit=days
    )
    return _json({"stock_code": stock_code, "days": days, "data": rows})


def query_capital_flow(stock_code: str, days: int = 10) -> str:
    """查询个股资金流向"""
    rows = _safe_query(
        "SELECT trade_date, main_net_inflow, super_large_net, large_net, "
        "medium_net, small_net, main_net_ratio FROM capital_flow "
        "WHERE stock_code=%s AND trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY) "
        "ORDER BY trade_date DESC",
        [stock_code, days], limit=days
    )
    return _json({"stock_code": stock_code, "data": rows})


def query_financial_reports(stock_code: str, limit: int = 8) -> str:
    """查询上市公司财报数据"""
    rows = _safe_query(
        "SELECT report_period, revenue, net_profit, revenue_yoy, profit_yoy, "
        "eps, roe, beat_expectation, report_date FROM financial_reports "
        "WHERE stock_code=%s ORDER BY report_period DESC",
        [stock_code], limit=limit
    )
    return _json({"stock_code": stock_code, "reports": rows})


def query_northbound_flow(days: int = 10) -> str:
    """查询北向资金（沪深港通）净流入"""
    rows = _safe_query(
        "SELECT trade_date, sh_net, sz_net, total_net, cumulative "
        "FROM northbound_flow ORDER BY trade_date DESC",
        [], limit=days
    )
    return _json({"days": days, "data": rows})


def query_macro_indicators(indicator_name: str, limit: int = 12) -> str:
    """查询宏观经济指标（CPI/PPI/PMI/社融等）"""
    rows = _safe_query(
        "SELECT indicator_name, indicator_date, value, unit FROM macro_indicators "
        "WHERE indicator_name LIKE %s ORDER BY indicator_date DESC",
        [f"%{indicator_name}%"], limit=limit
    )
    return _json({"indicator": indicator_name, "data": rows})


def search_news(keyword: str, days: int = 7, limit: int = 20) -> str:
    """搜索新闻/公告/研报（在清洗后的信息中搜索）"""
    rows = _safe_query(
        "SELECT ci.id, ci.event_type, ci.sentiment, ci.importance, ci.summary, "
        "ci.tags_json, ci.impact_analysis, ci.time_horizon, ci.cleaned_at, "
        "ri.title, ri.published_at "
        "FROM cleaned_items ci JOIN raw_items ri ON ci.raw_item_id=ri.id "
        "WHERE (ri.title LIKE %s OR ri.content LIKE %s OR ci.summary LIKE %s) "
        "AND ci.cleaned_at >= DATE_SUB(NOW(), INTERVAL %s DAY) "
        "ORDER BY ci.importance DESC, ci.cleaned_at DESC",
        [f"%{keyword}%", f"%{keyword}%", f"%{keyword}%", days],
        limit=limit
    )
    return _json({"keyword": keyword, "count": len(rows), "items": rows})


def search_research_reports(keyword: str, limit: int = 10) -> str:
    """搜索研报（按股票代码/名称或关键词）"""
    rows = _safe_query(
        "SELECT rr.broker_name, rr.analyst_name, rr.report_type, rr.rating, "
        "rr.target_price, rr.stock_code, rr.stock_name, rr.report_date, "
        "ci.summary "
        "FROM research_reports rr JOIN cleaned_items ci ON rr.cleaned_item_id=ci.id "
        "WHERE rr.stock_code=%s OR rr.stock_name LIKE %s "
        "ORDER BY rr.report_date DESC",
        [keyword, f"%{keyword}%"], limit=limit
    )
    return _json({"keyword": keyword, "reports": rows})


def query_watchlist() -> str:
    """查询自选股列表"""
    rows = _safe_query(
        "SELECT w.stock_code, w.stock_name, w.watch_type, w.related_tags, w.notes, "
        "w.added_at, sr.last_price, sr.change_pct "
        "FROM watchlist w LEFT JOIN stock_realtime sr ON w.stock_code=sr.stock_code "
        "ORDER BY w.added_at DESC"
    )
    return _json({"watchlist": rows})


def query_holdings() -> str:
    """查询当前持仓"""
    rows = _safe_query(
        "SELECT hp.stock_code, hp.stock_name, hp.buy_date, hp.buy_price, "
        "hp.quantity, hp.status, hp.notes, sr.last_price, sr.change_pct "
        "FROM holding_positions hp "
        "LEFT JOIN stock_realtime sr ON hp.stock_code=sr.stock_code "
        "WHERE hp.status='open' ORDER BY hp.buy_date DESC"
    )
    return _json({"holdings": rows})


def query_hotspot_tags(days: int = 7, limit: int = 30) -> str:
    """查询近期热点标签（出现频次排行）"""
    rows = _safe_query(
        "SELECT tag_name, tag_type, COUNT(*) as freq, MAX(appear_date) as last_seen "
        "FROM dashboard_tag_frequency "
        "WHERE appear_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY) "
        "GROUP BY tag_name, tag_type ORDER BY freq DESC",
        [days], limit=limit
    )
    return _json({"days": days, "tags": rows})


def query_tag_group_research(group_name: str) -> str:
    """查询标签组的最新深度研究报告"""
    groups = _safe_query(
        "SELECT id, group_name, tags_json, group_logic FROM tag_groups "
        "WHERE group_name LIKE %s ORDER BY created_at DESC",
        [f"%{group_name}%"], limit=3
    )
    if not groups:
        return _json({"error": f"未找到标签组: {group_name}"})
    g = groups[0]
    research = _safe_query(
        "SELECT research_date, macro_report, industry_report, "
        "top10_stocks_json, logic_synthesis_json, status "
        "FROM tag_group_research WHERE group_id=%s ORDER BY id DESC",
        [g["id"]], limit=1
    )
    return _json({
        "group": g,
        "latest_research": research[0] if research else None
    })


def query_kg_company(stock_code_or_name: str) -> str:
    """查询公司的知识图谱上下文（行业归属/成本结构/关联主题/供应链）"""
    from knowledge_graph.kg_query import get_company_context
    result = get_company_context(stock_code_or_name)
    if not result:
        return _json({"error": f"知识图谱中未找到: {stock_code_or_name}"})
    # 只返回文本摘要，避免数据过大
    return _json({
        "text": result["text"],
        "industries": result["industries"],
        "themes": result["themes"],
    })


def query_kg_impact(entity_name: str) -> str:
    """查询某事件/政策/主题对行业和公司的影响（知识图谱因果分析）"""
    from knowledge_graph.kg_query import impact_analysis
    from utils.db_utils import execute_query
    entities = execute_query(
        "SELECT id FROM kg_entities WHERE entity_name LIKE %s LIMIT 1",
        [f"%{entity_name}%"]
    )
    if not entities:
        return _json({"error": f"知识图谱中未找到实体: {entity_name}"})
    result = impact_analysis(entities[0]["id"], max_depth=3)
    return _json({
        "entity": entity_name,
        "affected_industries": result["affected_industries"][:10],
        "affected_companies": result["affected_companies"][:10],
    })


def search_cleaned_items(keyword: str = "", event_type: str = "",
                         sentiment: str = "", days: int = 30,
                         min_importance: int = 3, limit: int = 20) -> str:
    """按条件搜索清洗后的信息（支持关键词/事件类型/情绪/重要性过滤）"""
    conditions = ["ci.cleaned_at >= DATE_SUB(NOW(), INTERVAL %s DAY)",
                  "ci.importance >= %s"]
    params = [days, min_importance]
    if keyword:
        conditions.append("(ci.summary LIKE %s OR ri.title LIKE %s)")
        params += [f"%{keyword}%", f"%{keyword}%"]
    if event_type:
        conditions.append("ci.event_type=%s")
        params.append(event_type)
    if sentiment:
        conditions.append("ci.sentiment=%s")
        params.append(sentiment)
    where = " AND ".join(conditions)
    rows = _safe_query(
        f"SELECT ci.id, ci.event_type, ci.sentiment, ci.importance, ci.summary, "
        f"ci.tags_json, ci.impact_analysis, ci.cleaned_at, ri.title, ri.published_at "
        f"FROM cleaned_items ci JOIN raw_items ri ON ci.raw_item_id=ri.id "
        f"WHERE {where} ORDER BY ci.importance DESC, ci.cleaned_at DESC",
        params, limit=limit
    )
    return _json({"count": len(rows), "items": rows})


def query_market_capital_top(direction: str = "inflow", days: int = 5, limit: int = 10) -> str:
    """查询全市场主力资金净流入/流出 Top N"""
    order = "DESC" if direction != "outflow" else "ASC"
    rows = _safe_query(
        f"""SELECT cf.stock_code, si.stock_name, si.industry_l1,
                  SUM(cf.main_net_inflow)/10000 AS net_wan
           FROM capital_flow cf
           LEFT JOIN stock_info si ON cf.stock_code = si.stock_code
           WHERE cf.trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
             AND LENGTH(cf.stock_code) = 6
           GROUP BY cf.stock_code, si.stock_name, si.industry_l1
           ORDER BY net_wan {order}""",
        [days], limit=limit
    )
    return _json({"direction": direction, "days": days, "top": rows})


def query_basket_diagnosis(stock_codes: list) -> str:
    """一次性返回篮子股票的行业集中度 + 30日涨跌 + 5日资金流"""
    if not stock_codes:
        return _json({"error": "stock_codes 不能为空"})
    ph = ",".join(["%s"] * len(stock_codes))
    # 行业集中度
    industry_rows = _safe_query(
        f"SELECT industry_l1, COUNT(*) as cnt FROM stock_info WHERE stock_code IN ({ph}) GROUP BY industry_l1 ORDER BY cnt DESC",
        stock_codes, limit=20
    )
    # 30日涨跌
    perf_rows = _safe_query(
        f"""SELECT sd.stock_code, si.stock_name,
                  MAX(sd.close) as high_30d, MIN(sd.close) as low_30d,
                  (SELECT close FROM stock_daily WHERE stock_code=sd.stock_code ORDER BY trade_date DESC LIMIT 1) as last_close,
                  (SELECT close FROM stock_daily WHERE stock_code=sd.stock_code AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) ORDER BY trade_date ASC LIMIT 1) as close_30d_ago
           FROM stock_daily sd
           LEFT JOIN stock_info si ON sd.stock_code=si.stock_code
           WHERE sd.stock_code IN ({ph}) AND sd.trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
           GROUP BY sd.stock_code, si.stock_name""",
        stock_codes, limit=50
    )
    # 5日资金流
    cf_rows = _safe_query(
        f"""SELECT stock_code, SUM(main_net_inflow)/10000 as net_5d_wan
            FROM capital_flow
            WHERE stock_code IN ({ph}) AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 5 DAY)
            GROUP BY stock_code ORDER BY net_5d_wan DESC""",
        stock_codes, limit=50
    )
    return _json({
        "industry_concentration": industry_rows,
        "performance_30d": perf_rows,
        "capital_flow_5d": cf_rows,
    })


def query_industry_peers(stock_code: str, limit: int = 10) -> str:
    """查询同行业股票（排除自身），按近5日主力资金净流入排序"""
    info = _safe_query("SELECT industry_l1 FROM stock_info WHERE stock_code=%s", [stock_code], limit=1)
    if not info:
        return _json({"error": f"未找到股票 {stock_code} 的行业信息"})
    industry = info[0].get("industry_l1", "")
    rows = _safe_query(
        """SELECT si.stock_code, si.stock_name, si.industry_l1,
                  COALESCE(cf.net_5d, 0)/10000 as net_5d_wan
           FROM stock_info si
           LEFT JOIN (
               SELECT stock_code, SUM(main_net_inflow) as net_5d
               FROM capital_flow
               WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 5 DAY)
               GROUP BY stock_code
           ) cf ON si.stock_code = cf.stock_code
           WHERE si.industry_l1=%s AND si.stock_code != %s
           ORDER BY net_5d_wan DESC""",
        [industry, stock_code], limit=limit
    )
    return _json({"stock_code": stock_code, "industry": industry, "peers": rows})


def query_stock_tags(stock_code: str) -> str:
    """查询股票已匹配的 L1/L2/L3 规则标签（95维度选股标签体系）"""
    rows = _safe_query(
        """SELECT srt.layer, ssr.rule_name, ssr.category, srt.confidence, srt.evidence, srt.computed_at
           FROM stock_rule_tags srt
           JOIN stock_selection_rules ssr ON srt.rule_id = ssr.id
           WHERE srt.stock_code=%s AND srt.matched=1
           ORDER BY srt.layer, srt.confidence DESC""",
        [stock_code], limit=50
    )
    by_layer = {}
    for r in rows:
        layer = f"L{r['layer']}"
        by_layer.setdefault(layer, []).append(r)
    return _json({"stock_code": stock_code, "tags": by_layer, "total": len(rows)})


def query_stocks_by_tags(rule_names: list, limit: int = 20) -> str:
    """按规则标签筛选股票，返回同时匹配多个标签的股票（95维度标签体系）"""
    if not rule_names:
        return _json({"error": "rule_names 不能为空"})
    ph = ",".join(["%s"] * len(rule_names))
    rows = _safe_query(
        f"""SELECT srt.stock_code, si.stock_name, si.industry_l1,
                   COUNT(DISTINCT ssr.rule_name) as matched_count,
                   GROUP_CONCAT(ssr.rule_name ORDER BY srt.confidence DESC SEPARATOR ', ') as matched_rules,
                   AVG(srt.confidence) as avg_confidence
            FROM stock_rule_tags srt
            JOIN stock_selection_rules ssr ON srt.rule_id = ssr.id
            LEFT JOIN stock_info si ON srt.stock_code = si.stock_code
            WHERE ssr.rule_name IN ({ph}) AND srt.matched=1
            GROUP BY srt.stock_code, si.stock_name, si.industry_l1
            ORDER BY matched_count DESC, avg_confidence DESC""",
        rule_names, limit=limit
    )
    return _json({"rule_names": rule_names, "stocks": rows})


def query_project_context(project_id: int) -> str:
    """查询投资项目信息 + 篮子股票 + 关联策略（一次性获取完整上下文）"""
    # 项目基本信息（watchlist_lists 表）
    project = _safe_query(
        "SELECT id, list_name, description, created_at FROM watchlist_lists WHERE id=%s",
        [project_id], limit=1
    )
    if not project:
        # 尝试 holding_positions（project_id=1 为默认持仓组合）
        holdings = _safe_query(
            """SELECT hp.stock_code, hp.stock_name, hp.buy_price, hp.quantity,
                      sd.close as price, sd.change_pct, si.industry_l1
               FROM holding_positions hp
               LEFT JOIN stock_info si ON hp.stock_code=si.stock_code
               LEFT JOIN (
                   SELECT sd1.stock_code, sd1.close, sd1.change_pct
                   FROM stock_daily sd1
                   INNER JOIN (SELECT stock_code, MAX(trade_date) as mx FROM stock_daily GROUP BY stock_code) sd2
                   ON sd1.stock_code=sd2.stock_code AND sd1.trade_date=sd2.mx
               ) sd ON hp.stock_code=sd.stock_code
               WHERE hp.status='open' ORDER BY hp.buy_date DESC""",
            [], limit=50
        )
        return _json({"project_type": "portfolio", "holdings": holdings})

    proj = project[0]
    # 篮子股票
    basket = _safe_query(
        """SELECT wls.stock_code, wls.stock_name, wls.ai_reason,
                  sd.close as price, sd.change_pct, si.industry_l1
           FROM watchlist_list_stocks wls
           LEFT JOIN stock_info si ON wls.stock_code=si.stock_code
           LEFT JOIN (
               SELECT sd1.stock_code, sd1.close, sd1.change_pct
               FROM stock_daily sd1
               INNER JOIN (SELECT stock_code, MAX(trade_date) as mx FROM stock_daily GROUP BY stock_code) sd2
               ON sd1.stock_code=sd2.stock_code AND sd1.trade_date=sd2.mx
           ) sd ON wls.stock_code=sd.stock_code
           WHERE wls.list_id=%s AND wls.status='active' ORDER BY wls.added_at DESC""",
        [project_id], limit=50
    )
    # 关联策略
    strategies = _safe_query(
        """SELECT ist.strategy_name, ist.ai_rules_text
           FROM investment_strategies ist
           WHERE ist.id IN (
               SELECT strategy_id FROM project_chat_strategies pcs
               JOIN project_chat_messages pcm ON pcs.message_id=pcm.id
               WHERE pcm.project_id=%s ORDER BY pcm.id DESC LIMIT 5
           )""",
        [project_id], limit=5
    )
    return _json({
        "project": proj,
        "basket_stocks": basket,
        "strategies": strategies,
    })


def hybrid_search_tool(query: str, stock_codes: str = "", top_k: int = 8) -> str:
    """混合检索：向量语义检索 + KG 知识图谱，返回与查询最相关的信息上下文"""
    from retrieval.hybrid import hybrid_search
    context = {}
    if stock_codes:
        context["stock_codes"] = [c.strip() for c in stock_codes.split(",")]
    result = hybrid_search(query, context=context if context else None, top_k=top_k)
    chunks_summary = [
        {"text": c.text[:300], "score": c.score, "source": c.source_doc_title}
        for c in (result.chunks or [])[:5]
    ]
    return _json({
        "merged_context": result.merged_context[:4000] if result.merged_context else "",
        "chunks": chunks_summary,
        "kg_text": (result.kg.text[:2000] if result.kg else ""),
    })


def run_skill_analysis(skill_name: str, context: str, question: str = "") -> str:
    """使用专业分析 Skill 框架对给定数据进行深度分析。
    可用 skill_name: financial-model-construction, company-valuation,
    stock-chart-analysis, dynamic-stock-chart-predict,
    event-industry-impact, macro-stock-analysis, stock-event-analysis
    """
    from utils.skill_registry import get_skill_content
    from utils.model_router import call_model

    skill_content = get_skill_content(skill_name)
    if not skill_content:
        return _json({"error": f"Skill '{skill_name}' 不存在或未安装"})

    # 截取 Skill 关键部分（避免 token 过多）
    lines = skill_content.split("\n")
    # 保留前 200 行（约 3000 字符）
    excerpt = "\n".join(lines[:200])

    system_prompt = f"""你是专业的股票分析师，请严格按照以下分析框架进行分析：

{excerpt}

## 分析要求
- 基于提供的数据进行分析，不要编造数据
- 输出结构化的分析结论
- 使用中文回答"""

    user_msg = f"## 数据上下文\n{context}"
    if question:
        user_msg += f"\n\n## 分析问题\n{question}"

    try:
        result = call_model("chat", system_prompt, user_msg, max_tokens=3000)
        return _json({"skill": skill_name, "analysis": result})
    except Exception as e:
        return _json({"error": f"分析失败: {str(e)}"})


def execute_sql(sql: str) -> str:
    """执行只读 SQL 查询（仅允许 SELECT 语句，最多返回 50 行）"""
    sql_stripped = sql.strip().upper()
    if not sql_stripped.startswith("SELECT"):
        return _json({"error": "只允许 SELECT 查询"})
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
                 "TRUNCATE", "REPLACE", "GRANT", "REVOKE"]
    for kw in forbidden:
        if kw in sql_stripped:
            return _json({"error": f"禁止使用 {kw} 语句"})
    rows = _safe_query(sql, limit=50)
    return _json({"rows": rows, "count": len(rows)})


# ─────────────────────────────────────────────
# OpenAI function schema 定义
# ─────────────────────────────────────────────

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "query_stock_info",
            "description": "查询股票基本信息，包括股票名称、所属行业、市值、上市日期等",
            "parameters": {
                "type": "object",
                "properties": {
                    "stock_code": {"type": "string", "description": "6位股票代码，如 '600519'"}
                },
                "required": ["stock_code"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_stock_daily",
            "description": "查询股票日K线行情数据（开高低收、成交量、换手率、涨跌幅）",
            "parameters": {
                "type": "object",
                "properties": {
                    "stock_code": {"type": "string", "description": "6位股票代码"},
                    "days": {"type": "integer", "description": "查询最近多少天，默认30", "default": 30}
                },
                "required": ["stock_code"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_capital_flow",
            "description": "查询个股资金流向，包括主力净流入、超大单、大单净流入数据",
            "parameters": {
                "type": "object",
                "properties": {
                    "stock_code": {"type": "string", "description": "6位股票代码"},
                    "days": {"type": "integer", "description": "查询最近多少天，默认10", "default": 10}
                },
                "required": ["stock_code"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_financial_reports",
            "description": "查询上市公司财报数据，包括营收、净利润、同比增速、EPS、ROE等",
            "parameters": {
                "type": "object",
                "properties": {
                    "stock_code": {"type": "string", "description": "6位股票代码"},
                    "limit": {"type": "integer", "description": "返回最近几期，默认8", "default": 8}
                },
                "required": ["stock_code"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_northbound_flow",
            "description": "查询北向资金（沪深港通）每日净流入数据",
            "parameters": {
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "description": "查询最近多少天，默认10", "default": 10}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_macro_indicators",
            "description": "查询宏观经济指标历史数据，如CPI、PPI、PMI、社融、M2等",
            "parameters": {
                "type": "object",
                "properties": {
                    "indicator_name": {"type": "string", "description": "指标名称关键词，如 'CPI'、'PMI'、'社融'"},
                    "limit": {"type": "integer", "description": "返回最近几期，默认12", "default": 12}
                },
                "required": ["indicator_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_news",
            "description": "在AI清洗后的新闻/公告/研报中搜索，返回摘要、情绪、重要性、影响分析",
            "parameters": {
                "type": "object",
                "properties": {
                    "keyword": {"type": "string", "description": "搜索关键词，如公司名、行业名、事件名"},
                    "days": {"type": "integer", "description": "搜索最近多少天，默认7", "default": 7},
                    "limit": {"type": "integer", "description": "返回条数，默认20", "default": 20}
                },
                "required": ["keyword"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_research_reports",
            "description": "搜索研报，按股票代码或关键词查找券商研报的评级、目标价、摘要",
            "parameters": {
                "type": "object",
                "properties": {
                    "keyword": {"type": "string", "description": "股票代码（如'600519'）或股票名称/关键词"},
                    "limit": {"type": "integer", "description": "返回条数，默认10", "default": 10}
                },
                "required": ["keyword"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_watchlist",
            "description": "查询自选股列表，包含关注类型、备注和实时行情",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_holdings",
            "description": "查询当前持仓，包含买入价、数量、当前价格和涨跌幅",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_hotspot_tags",
            "description": "查询近期热点标签出现频次排行，了解市场热点主题",
            "parameters": {
                "type": "object",
                "properties": {
                    "days": {"type": "integer", "description": "统计最近多少天，默认7", "default": 7},
                    "limit": {"type": "integer", "description": "返回条数，默认30", "default": 30}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_tag_group_research",
            "description": "查询某个投资主题（标签组）的最新深度研究报告，包含宏观/行业/个股推荐",
            "parameters": {
                "type": "object",
                "properties": {
                    "group_name": {"type": "string", "description": "标签组名称关键词"}
                },
                "required": ["group_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_kg_company",
            "description": "查询公司在知识图谱中的上下文：所属行业、成本结构、收入结构、关联主题、供应链关系",
            "parameters": {
                "type": "object",
                "properties": {
                    "stock_code_or_name": {"type": "string", "description": "股票代码或公司名称"}
                },
                "required": ["stock_code_or_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_kg_impact",
            "description": "分析某事件/政策/主题对行业和公司的影响（基于知识图谱因果链推导）",
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_name": {"type": "string", "description": "事件/政策/主题/大宗商品名称，如'美联储加息'、'光伏补贴'、'铜价上涨'"}
                },
                "required": ["entity_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_cleaned_items",
            "description": "按条件搜索AI清洗后的信息，支持关键词、事件类型、情绪、重要性过滤",
            "parameters": {
                "type": "object",
                "properties": {
                    "keyword": {"type": "string", "description": "搜索关键词", "default": ""},
                    "event_type": {"type": "string", "description": "事件类型过滤", "default": ""},
                    "sentiment": {"type": "string", "description": "情绪过滤: positive/negative/neutral", "default": ""},
                    "days": {"type": "integer", "description": "最近多少天，默认30", "default": 30},
                    "min_importance": {"type": "integer", "description": "最低重要性(1-5)，默认3", "default": 3},
                    "limit": {"type": "integer", "description": "返回条数，默认20", "default": 20}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "execute_sql",
            "description": "执行自定义只读SQL查询（仅允许SELECT，最多返回50行）。当其他工具无法满足需求时使用",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SELECT SQL语句"}
                },
                "required": ["sql"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_market_capital_top",
            "description": "查询全市场主力资金净流入/流出 Top N 股票，了解市场整体资金动向",
            "parameters": {
                "type": "object",
                "properties": {
                    "direction": {"type": "string", "description": "inflow（净流入）或 outflow（净流出），默认inflow", "default": "inflow"},
                    "days": {"type": "integer", "description": "统计最近多少天，默认5", "default": 5},
                    "limit": {"type": "integer", "description": "返回条数，默认10", "default": 10}
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_basket_diagnosis",
            "description": "一次性诊断篮子股票：行业集中度 + 30日涨跌表现 + 5日主力资金流",
            "parameters": {
                "type": "object",
                "properties": {
                    "stock_codes": {"type": "array", "items": {"type": "string"}, "description": "股票代码列表，如 ['600519', '000858']"}
                },
                "required": ["stock_codes"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_industry_peers",
            "description": "查询某股票的同行业竞争对手，按近5日主力资金净流入排序",
            "parameters": {
                "type": "object",
                "properties": {
                    "stock_code": {"type": "string", "description": "6位股票代码"},
                    "limit": {"type": "integer", "description": "返回条数，默认10", "default": 10}
                },
                "required": ["stock_code"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_stock_tags",
            "description": "查询股票已匹配的选股规则标签（L1量化/L2AI/L3深度，共95个维度），了解股票的选股信号",
            "parameters": {
                "type": "object",
                "properties": {
                    "stock_code": {"type": "string", "description": "6位股票代码"}
                },
                "required": ["stock_code"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_stocks_by_tags",
            "description": "按选股规则标签筛选股票，返回匹配指定规则的股票列表（95维度标签体系）",
            "parameters": {
                "type": "object",
                "properties": {
                    "rule_names": {"type": "array", "items": {"type": "string"}, "description": "规则名称列表，如 ['均线多头排列', '主力资金连续净流入']"},
                    "limit": {"type": "integer", "description": "返回条数，默认20", "default": 20}
                },
                "required": ["rule_names"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_project_context",
            "description": "查询投资项目的完整上下文：项目信息、篮子股票列表、关联选股策略",
            "parameters": {
                "type": "object",
                "properties": {
                    "project_id": {"type": "integer", "description": "项目ID，1为默认持仓组合"}
                },
                "required": ["project_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "hybrid_search_tool",
            "description": "混合检索（向量语义+知识图谱）：输入自然语言查询，返回系统中最相关的新闻、研报、KG知识上下文。优先使用此工具获取背景信息，再用其他工具补充具体数据",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "自然语言检索查询"},
                    "stock_codes": {"type": "string", "description": "限定股票范围（逗号分隔），可选", "default": ""},
                    "top_k": {"type": "integer", "description": "返回结果数量，默认8", "default": 8}
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "run_skill_analysis",
            "description": "调用专业分析框架对数据进行深度分析。可用框架：financial-model-construction（财务建模）、company-valuation（估值分析）、stock-chart-analysis（技术分析）、dynamic-stock-chart-predict（买卖点预测）、event-industry-impact（行业影响）、macro-stock-analysis（宏观分析）、stock-event-analysis（事件分析）",
            "parameters": {
                "type": "object",
                "properties": {
                    "skill_name": {"type": "string", "description": "分析框架名称，如 'stock-chart-analysis'"},
                    "context": {"type": "string", "description": "要分析的数据上下文（从其他工具查询到的数据）"},
                    "question": {"type": "string", "description": "具体分析问题（可选）", "default": ""}
                },
                "required": ["skill_name", "context"]
            }
        }
    },
]

# 工具名称 → 执行函数的映射
TOOL_FUNCTIONS = {
    "query_stock_info": query_stock_info,
    "query_stock_daily": query_stock_daily,
    "query_capital_flow": query_capital_flow,
    "query_financial_reports": query_financial_reports,
    "query_northbound_flow": query_northbound_flow,
    "query_macro_indicators": query_macro_indicators,
    "search_news": search_news,
    "search_research_reports": search_research_reports,
    "query_watchlist": query_watchlist,
    "query_holdings": query_holdings,
    "query_hotspot_tags": query_hotspot_tags,
    "query_tag_group_research": query_tag_group_research,
    "query_kg_company": query_kg_company,
    "query_kg_impact": query_kg_impact,
    "search_cleaned_items": search_cleaned_items,
    "execute_sql": execute_sql,
    "query_market_capital_top": query_market_capital_top,
    "query_basket_diagnosis": query_basket_diagnosis,
    "query_industry_peers": query_industry_peers,
    "query_stock_tags": query_stock_tags,
    "query_stocks_by_tags": query_stocks_by_tags,
    "query_project_context": query_project_context,
    "run_skill_analysis": run_skill_analysis,
    "hybrid_search_tool": hybrid_search_tool,
}

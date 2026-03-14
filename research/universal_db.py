"""Universal数据库查询接口 — 为深度研究提供统一数据访问"""
import json
from utils.db_utils import execute_query


def get_stock_profile(stock_code):
    """获取个股完整画像"""
    info = execute_query("SELECT * FROM stock_info WHERE stock_code=?", [stock_code])
    daily = execute_query(
        """SELECT * FROM stock_daily WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT 250""",
        [stock_code],
    )
    capital = execute_query(
        """SELECT * FROM capital_flow WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT 60""",
        [stock_code],
    )
    financials = execute_query(
        """SELECT * FROM financial_reports WHERE stock_code=?
           ORDER BY report_period DESC LIMIT 8""",
        [stock_code],
    )
    news = execute_query(
        """SELECT ci.* FROM item_companies ic
           JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
           WHERE ic.stock_code=? ORDER BY ci.cleaned_at DESC LIMIT 30""",
        [stock_code],
    )
    reports = execute_query(
        """SELECT * FROM research_reports WHERE stock_code=?
           ORDER BY report_date DESC LIMIT 20""",
        [stock_code],
    )
    return {
        "info": info[0] if info else None,
        "daily": daily,
        "capital": capital,
        "financials": financials,
        "news": news,
        "reports": reports,
    }


def get_stock_technical_summary(stock_code):
    """获取个股技术面摘要"""
    daily = execute_query(
        """SELECT trade_date, open, high, low, close, volume, change_pct, turnover_rate
           FROM stock_daily WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT 60""",
        [stock_code],
    )
    if not daily:
        return None

    closes = [d["close"] for d in daily if d.get("close")]
    if not closes:
        return None

    latest = closes[0]
    summary = {"latest_price": latest, "days": len(daily)}

    # 均线
    for period in [5, 10, 20, 60]:
        if len(closes) >= period:
            summary[f"ma{period}"] = round(sum(closes[:period]) / period, 2)

    # 涨跌幅
    if len(closes) >= 5:
        summary["change_5d"] = round((closes[0] / closes[4] - 1) * 100, 2)
    if len(closes) >= 20:
        summary["change_20d"] = round((closes[0] / closes[19] - 1) * 100, 2)

    # 最高最低
    summary["high_60d"] = max(d.get("high", 0) for d in daily if d.get("high"))
    summary["low_60d"] = min(d.get("low", 999999) for d in daily if d.get("low"))

    # 平均成交量和换手率
    volumes = [d.get("volume", 0) for d in daily[:20] if d.get("volume")]
    if volumes:
        summary["avg_volume_20d"] = round(sum(volumes) / len(volumes))
    turnover = [d.get("turnover_rate", 0) for d in daily[:20] if d.get("turnover_rate")]
    if turnover:
        summary["avg_turnover_20d"] = round(sum(turnover) / len(turnover), 2)

    return summary


def get_peer_comparison(stock_code):
    """获取同行业个股对比数据"""
    info = execute_query("SELECT * FROM stock_info WHERE stock_code=?", [stock_code])
    if not info:
        return []
    industry = info[0].get("industry_l2") or info[0].get("industry_l1")
    if not industry:
        return []

    peers = execute_query(
        """SELECT si.stock_code, si.stock_name, si.market_cap,
                  sd.close, sd.change_pct
           FROM stock_info si
           LEFT JOIN (
               SELECT stock_code, close, change_pct,
                      ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY trade_date DESC) as rn
               FROM stock_daily
           ) sd ON si.stock_code=sd.stock_code AND sd.rn=1
           WHERE si.industry_l2=? AND si.stock_code!=?
           ORDER BY si.market_cap DESC LIMIT 10""",
        [industry, stock_code],
    )
    return peers


def get_industry_data(industry_name):
    """获取行业数据"""
    flows = execute_query(
        """SELECT * FROM industry_capital_flow WHERE industry_name LIKE ?
           ORDER BY trade_date DESC LIMIT 60""",
        [f"%{industry_name}%"],
    )
    news = execute_query(
        """SELECT ci.* FROM item_industries ii
           JOIN cleaned_items ci ON ii.cleaned_item_id=ci.id
           WHERE ii.industry_name LIKE ?
           ORDER BY ci.cleaned_at DESC LIMIT 30""",
        [f"%{industry_name}%"],
    )
    # 行业内个股
    stocks = execute_query(
        """SELECT si.stock_code, si.stock_name, si.market_cap
           FROM stock_info si
           WHERE si.industry_l2 LIKE ? OR si.industry_l1 LIKE ?
           ORDER BY si.market_cap DESC LIMIT 20""",
        [f"%{industry_name}%", f"%{industry_name}%"],
    )
    return {"flows": flows, "news": news, "stocks": stocks}


def get_macro_data():
    """获取宏观数据"""
    indicators = execute_query(
        "SELECT * FROM macro_indicators ORDER BY indicator_date DESC LIMIT 100"
    )
    northbound = execute_query(
        "SELECT * FROM northbound_flow ORDER BY trade_date DESC LIMIT 60"
    )
    macro_news = execute_query(
        """SELECT * FROM cleaned_items WHERE event_type='macro_policy'
           ORDER BY cleaned_at DESC LIMIT 30"""
    )
    return {"indicators": indicators, "northbound": northbound, "news": macro_news}


def get_sector_heat_detail(stock_code):
    """获取板块热度详情 — 15日资金流时序 + 细分行业 + 投资主题"""
    from tagging.stock_tag_service import get_theme_tags_from_kg, get_industry_tags_from_kg

    # 公司市值
    info = execute_query(
        "SELECT market_cap, industry_l2, industry_l1 FROM stock_info WHERE stock_code=?",
        [stock_code],
    )
    market_cap = info[0].get("market_cap") or 0 if info else 0
    industry_l2 = info[0].get("industry_l2") or info[0].get("industry_l1") or "" if info else ""

    # 公司15日资金流
    flow_rows = execute_query(
        """SELECT trade_date, main_net_inflow as net_inflow
           FROM capital_flow WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT 15""",
        [stock_code],
    )
    company_flow_15d = []
    for r in (flow_rows or []):
        net = r.get("net_inflow") or 0
        ratio = round(net / market_cap, 6) if market_cap and market_cap > 0 else 0
        company_flow_15d.append({
            "date": str(r.get("trade_date", "")),
            "net_inflow": net,
            "market_cap": market_cap,
            "ratio": ratio,
        })

    # 细分行业：从KG获取行业标签，再查行业资金流
    sub_industries = []
    try:
        industry_tags = get_industry_tags_from_kg(stock_code)
        if not industry_tags and industry_l2:
            industry_tags = [industry_l2]
        for ind_name in industry_tags[:3]:
            # 行业市值（按industry_l2聚合）
            ind_cap_rows = execute_query(
                """SELECT SUM(market_cap) as total_cap FROM stock_info
                   WHERE industry_l2 LIKE ? OR industry_l1 LIKE ?""",
                [f"%{ind_name}%", f"%{ind_name}%"],
            )
            ind_cap = ind_cap_rows[0].get("total_cap") or 0 if ind_cap_rows else 0

            # 行业15日资金流
            ind_flow_rows = execute_query(
                """SELECT trade_date, net_inflow FROM industry_capital_flow
                   WHERE industry_name LIKE ?
                   ORDER BY trade_date DESC LIMIT 15""",
                [f"%{ind_name}%"],
            )
            flow_15d = []
            for r in (ind_flow_rows or []):
                net = r.get("net_inflow") or 0
                ratio = round(net / ind_cap, 6) if ind_cap and ind_cap > 0 else 0
                flow_15d.append({
                    "date": str(r.get("trade_date", "")),
                    "net_inflow": net,
                    "ratio": ratio,
                })
            if flow_15d:
                sub_industries.append({
                    "industry_name": ind_name,
                    "market_cap": ind_cap,
                    "flow_15d": flow_15d,
                })
    except Exception:
        pass

    # 投资主题：从KG获取主题标签，聚合主题内股票市值和资金流
    investment_themes = []
    try:
        theme_tags = get_theme_tags_from_kg(stock_code)
        for theme_name in theme_tags[:3]:
            # 主题内股票（通过KG关联）
            theme_stocks = execute_query(
                """SELECT DISTINCT ke_src.external_id as stock_code
                   FROM kg_entities ke_src
                   JOIN kg_relationships kr ON kr.source_entity_id = ke_src.id
                   JOIN kg_entities ke_tgt ON kr.target_entity_id = ke_tgt.id
                   WHERE ke_src.entity_type = 'company'
                     AND ke_tgt.entity_type = 'theme'
                     AND ke_tgt.entity_name = ?
                     AND ke_src.external_id IS NOT NULL
                   LIMIT 50""",
                [theme_name],
            )
            theme_stock_codes = [r["stock_code"] for r in (theme_stocks or []) if r.get("stock_code")]
            if not theme_stock_codes:
                continue

            # 主题市值
            placeholders = ",".join(["?" for _ in theme_stock_codes])
            cap_rows = execute_query(
                f"SELECT SUM(market_cap) as total_cap FROM stock_info WHERE stock_code IN ({placeholders})",
                theme_stock_codes,
            )
            theme_cap = cap_rows[0].get("total_cap") or 0 if cap_rows else 0

            # 主题15日资金流（聚合主题内个股）
            flow_rows_theme = execute_query(
                f"""SELECT trade_date, SUM(main_net_inflow) as net_inflow
                    FROM capital_flow
                    WHERE stock_code IN ({placeholders})
                    GROUP BY trade_date
                    ORDER BY trade_date DESC LIMIT 15""",
                theme_stock_codes,
            )
            flow_15d = []
            for r in (flow_rows_theme or []):
                net = r.get("net_inflow") or 0
                ratio = round(net / theme_cap, 6) if theme_cap and theme_cap > 0 else 0
                flow_15d.append({
                    "date": str(r.get("trade_date", "")),
                    "net_inflow": net,
                    "ratio": ratio,
                })
            if flow_15d:
                investment_themes.append({
                    "theme_name": theme_name,
                    "theme_market_cap": theme_cap,
                    "flow_15d": flow_15d,
                })
    except Exception:
        pass

    # 行业7日逐日净流入（按与该股票匹配的 industry_l1/l2 过滤）
    industry_daily_flow = []
    industry_sub_breakdown = []
    try:
        ind_keyword = industry_l2 or ""
        if ind_keyword:
            # 7日逐日汇总（同一行业名前缀下所有子行业的总净流入）
            daily_rows = execute_query(
                """SELECT trade_date, SUM(net_inflow) as total_inflow
                   FROM industry_capital_flow
                   WHERE industry_name LIKE %s
                   GROUP BY trade_date
                   ORDER BY trade_date DESC LIMIT 7""",
                [f"%{ind_keyword}%"],
            ) or []
            for r in daily_rows:
                industry_daily_flow.append({
                    "date": str(r.get("trade_date", "")),
                    "net_inflow": r.get("total_inflow") or 0,
                })

            # 二级行业当日降序明细（最近一天，所有匹配子行业）
            sub_rows = execute_query(
                """SELECT industry_name, net_inflow, change_pct, leading_stock
                   FROM industry_capital_flow
                   WHERE industry_name LIKE %s
                     AND trade_date = (
                         SELECT MAX(trade_date) FROM industry_capital_flow WHERE industry_name LIKE %s
                     )
                   ORDER BY net_inflow DESC""",
                [f"%{ind_keyword}%", f"%{ind_keyword}%"],
            ) or []
            for r in sub_rows:
                industry_sub_breakdown.append({
                    "name": r.get("industry_name", ""),
                    "net_inflow": r.get("net_inflow") or 0,
                    "change_pct": r.get("change_pct") or 0,
                    "leading_stock": r.get("leading_stock") or "",
                })
    except Exception:
        pass

    return {
        "company_flow_15d": company_flow_15d,
        "sub_industries": sub_industries,
        "investment_themes": investment_themes,
        "industry_daily_flow": industry_daily_flow,
        "industry_sub_breakdown": industry_sub_breakdown,
    }

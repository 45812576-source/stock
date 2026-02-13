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

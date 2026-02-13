"""自动下载关联信息+交易数据"""
import logging
from datetime import datetime
from utils.db_utils import execute_query, execute_insert
from ingestion.akshare_source import fetch_stock_daily, fetch_capital_flow

logger = logging.getLogger(__name__)


def auto_update_watchlist():
    """自动更新跟踪列表中所有股票的数据"""
    stocks = execute_query("SELECT stock_code FROM watchlist")
    results = {"daily": 0, "capital": 0, "news_matched": 0}

    for s in stocks:
        code = s["stock_code"]
        results["daily"] += fetch_stock_daily(code)
        results["capital"] += fetch_capital_flow(code)

    # 匹配今日清洗数据中的关联信息
    today = datetime.now().strftime("%Y-%m-%d")
    for s in stocks:
        code = s["stock_code"]
        matched = execute_query(
            """SELECT ci.id FROM item_companies ic
               JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
               WHERE ic.stock_code=? AND date(ci.cleaned_at)=?""",
            [code, today],
        )
        results["news_matched"] += len(matched)

    logger.info(f"跟踪列表自动更新: {results}")
    return results

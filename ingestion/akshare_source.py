"""AKShare数据采集 — 行情、资金流、基础信息"""
import time
import json
import logging
import akshare as ak
import pandas as pd
from datetime import datetime
from utils.db_utils import get_db, execute_query
from config import AKSHARE_RETRY, AKSHARE_DELAY

logger = logging.getLogger(__name__)


def _retry(func, *args, retries=AKSHARE_RETRY, **kwargs):
    """带重试的AKShare调用"""
    for i in range(retries):
        try:
            time.sleep(AKSHARE_DELAY)
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"AKShare调用失败 (第{i+1}次): {e}")
            if i == retries - 1:
                raise
            time.sleep(2 ** i)


def fetch_stock_info():
    """拉取A股基础信息"""
    df = _retry(ak.stock_info_a_code_name)
    if df is None or df.empty:
        return 0
    count = 0
    with get_db() as conn:
        for _, row in df.iterrows():
            conn.execute(
                """INSERT OR REPLACE INTO stock_info (stock_code, stock_name)
                   VALUES (?, ?)""",
                [row["code"], row["name"]],
            )
            count += 1
    logger.info(f"更新股票基础信息: {count}条")
    return count


def fetch_stock_daily(stock_code, start_date="20240101", end_date=None):
    """拉取个股日线行情"""
    if end_date is None:
        end_date = datetime.now().strftime("%Y%m%d")
    try:
        df = _retry(
            ak.stock_zh_a_hist,
            symbol=stock_code, period="daily",
            start_date=start_date, end_date=end_date, adjust="qfq",
        )
    except Exception as e:
        logger.error(f"拉取{stock_code}行情失败: {e}")
        return 0
    if df is None or df.empty:
        return 0
    count = 0
    with get_db() as conn:
        for _, row in df.iterrows():
            trade_date = str(row["日期"])[:10]
            conn.execute(
                """INSERT OR REPLACE INTO stock_daily
                   (stock_code, trade_date, open, high, low, close, volume, amount,
                    turnover_rate, amplitude, change_pct, change_amount)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [stock_code, trade_date, row["开盘"], row["最高"], row["最低"],
                 row["收盘"], row["成交量"], row["成交额"], row.get("换手率"),
                 row.get("振幅"), row.get("涨跌幅"), row.get("涨跌额")],
            )
            count += 1
    logger.info(f"{stock_code} 日线数据: {count}条")
    return count


def fetch_capital_flow(stock_code):
    """拉取个股资金流向"""
    try:
        df = _retry(ak.stock_individual_fund_flow, stock=stock_code, market="sh")
    except Exception:
        try:
            df = _retry(ak.stock_individual_fund_flow, stock=stock_code, market="sz")
        except Exception as e:
            logger.error(f"拉取{stock_code}资金流向失败: {e}")
            return 0
    if df is None or df.empty:
        return 0
    count = 0
    with get_db() as conn:
        for _, row in df.iterrows():
            trade_date = str(row["日期"])[:10]
            conn.execute(
                """INSERT OR REPLACE INTO capital_flow
                   (stock_code, trade_date, main_net_inflow, super_large_net,
                    large_net, medium_net, small_net, main_net_ratio)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [stock_code, trade_date, row.get("主力净流入-净额"),
                 row.get("超大单净流入-净额"), row.get("大单净流入-净额"),
                 row.get("中单净流入-净额"), row.get("小单净流入-净额"),
                 row.get("主力净流入-净占比")],
            )
            count += 1
    logger.info(f"{stock_code} 资金流向: {count}条")
    return count


def fetch_industry_capital_flow():
    """拉取行业资金流向"""
    try:
        df = _retry(ak.stock_sector_fund_flow_rank, indicator="今日")
    except Exception as e:
        logger.error(f"拉取行业资金流向失败: {e}")
        return 0
    if df is None or df.empty:
        return 0
    today = datetime.now().strftime("%Y-%m-%d")
    count = 0
    with get_db() as conn:
        for _, row in df.iterrows():
            conn.execute(
                """INSERT OR REPLACE INTO industry_capital_flow
                   (industry_name, trade_date, net_inflow, change_pct, leading_stock)
                   VALUES (?, ?, ?, ?, ?)""",
                [row.get("名称"), today, row.get("主力净流入-净额"),
                 row.get("涨跌幅"), row.get("领涨股")],
            )
            count += 1
    logger.info(f"行业资金流向: {count}条")
    return count


def fetch_northbound_flow():
    """拉取北向资金"""
    try:
        df = _retry(ak.stock_hsgt_north_net_flow_in_em, symbol="北向")
    except Exception as e:
        logger.error(f"拉取北向资金失败: {e}")
        return 0
    if df is None or df.empty:
        return 0
    count = 0
    with get_db() as conn:
        for _, row in df.iterrows():
            trade_date = str(row.get("date", row.get("日期", "")))[:10]
            if not trade_date:
                continue
            conn.execute(
                """INSERT OR REPLACE INTO northbound_flow
                   (trade_date, total_net)
                   VALUES (?, ?)""",
                [trade_date, row.get("value", row.get("当日净流入", 0))],
            )
            count += 1
    logger.info(f"北向资金: {count}条")
    return count


def fetch_all_daily_data(stock_codes=None):
    """批量拉取所有日常数据"""
    results = {"stock_info": 0, "industry_flow": 0, "northbound": 0, "daily": 0, "capital": 0}

    results["stock_info"] = fetch_stock_info()
    results["industry_flow"] = fetch_industry_capital_flow()
    results["northbound"] = fetch_northbound_flow()

    if stock_codes is None:
        # 默认拉取watchlist中的股票
        rows = execute_query("SELECT stock_code FROM watchlist")
        stock_codes = [r["stock_code"] for r in rows]

    for code in stock_codes:
        results["daily"] += fetch_stock_daily(code)
        results["capital"] += fetch_capital_flow(code)

    logger.info(f"每日数据拉取完成: {results}")
    return results


def _parse_amount(text):
    """解析金额文本，如 '1.47亿' -> 14700, '8.91亿' -> 89100 (万元)"""
    if not text or text == 'False' or text == '--':
        return None
    text = str(text).strip()
    try:
        if '万亿' in text:
            return float(text.replace('万亿', '')) * 1e8
        elif '亿' in text:
            return float(text.replace('亿', '')) * 1e4
        elif '万' in text:
            return float(text.replace('万', ''))
        else:
            return float(text)
    except (ValueError, TypeError):
        return None


def _parse_pct(text):
    """解析百分比文本，如 '46.84%' -> 46.84"""
    if not text or text == 'False' or text == '--':
        return None
    text = str(text).strip().replace('%', '')
    try:
        return float(text)
    except (ValueError, TypeError):
        return None


def fetch_financial_data(stock_code):
    """拉取个股财务摘要数据（同花顺）"""
    try:
        df = _retry(ak.stock_financial_abstract_ths, symbol=stock_code, indicator="按报告期")
    except Exception as e:
        logger.error(f"拉取{stock_code}财务数据失败: {e}")
        return 0
    if df is None or df.empty:
        return 0
    count = 0
    with get_db() as conn:
        for _, row in df.iterrows():
            period = str(row.get("报告期", ""))[:10]
            if not period:
                continue
            revenue = _parse_amount(row.get("营业总收入"))
            net_profit = _parse_amount(row.get("净利润"))
            revenue_yoy = _parse_pct(row.get("营业总收入同比增长率"))
            profit_yoy = _parse_pct(row.get("净利润同比增长率"))
            eps_raw = row.get("基本每股收益")
            eps = float(eps_raw) if eps_raw and eps_raw != 'False' else None
            roe = _parse_pct(row.get("净资产收益率"))

            conn.execute(
                """INSERT OR REPLACE INTO financial_reports
                   (stock_code, report_period, revenue, net_profit,
                    revenue_yoy, profit_yoy, eps, roe)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [stock_code, period, revenue, net_profit,
                 revenue_yoy, profit_yoy, eps, roe],
            )
            count += 1
    logger.info(f"{stock_code} 财务数据: {count}条")
    return count


def fetch_stock_detail(stock_code):
    """拉取个股详细信息（行业、市值等）"""
    try:
        df = _retry(ak.stock_individual_info_em, symbol=stock_code)
    except Exception as e:
        logger.error(f"拉取{stock_code}详细信息失败: {e}")
        return False
    if df is None or df.empty:
        return False

    info = {}
    for _, row in df.iterrows():
        info[row["item"]] = row["value"]

    industry = info.get("行业", "")
    market_cap_raw = info.get("总市值")
    market_cap = round(float(market_cap_raw) / 1e8, 2) if market_cap_raw else None
    total_shares = info.get("总股本")
    float_shares = info.get("流通股")
    list_date = str(info.get("上市时间", ""))

    with get_db() as conn:
        conn.execute(
            """UPDATE stock_info SET
                industry_l1=?, market_cap=?, total_shares=?,
                float_shares=?, list_date=?, updated_at=CURRENT_TIMESTAMP
               WHERE stock_code=?""",
            [industry, market_cap, total_shares, float_shares, list_date, stock_code],
        )
    logger.info(f"{stock_code} 详细信息已更新: 行业={industry} 市值={market_cap}亿")
    return True

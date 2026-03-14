"""宏观数据采集模块 — 写云端 MySQL

采集指标：
- M2增速、社融、PMI → macro_indicators
- Shibor 1W → macro_indicators
- 融资余额 → margin_balance
- 全A PE分位 + 市盈率 → market_valuation
- 陆股通持股变动 → hsgt_holding
- 海外ETF (KWEB/FXI/ASHR) → overseas_etf
"""
import time
import logging
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from utils.db_utils import execute_cloud_insert, execute_cloud_query
from config import AKSHARE_RETRY, AKSHARE_DELAY

logger = logging.getLogger(__name__)

# 海外ETF列表
OVERSEAS_ETF_SYMBOLS = [
    ("KWEB", "KraneShares CSI China Internet ETF"),
    ("FXI", "iShares China Large-Cap ETF"),
    ("ASHR", "Xtrackers Harvest CSI 300 China A-Shares ETF"),
]


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


def _upsert_macro_indicator(indicator_name, indicator_date, value, unit="", source="akshare"):
    """写入 macro_indicators（云端）"""
    execute_cloud_insert(
        """INSERT INTO macro_indicators (indicator_name, indicator_date, value, unit, source)
           VALUES (%s, %s, %s, %s, %s)
           ON DUPLICATE KEY UPDATE value=VALUES(value), unit=VALUES(unit), source=VALUES(source)""",
        [indicator_name, indicator_date, value, unit, source],
    )


def fetch_macro_monthly() -> dict:
    """采集月度宏观指标：M2增速、社融、PMI → macro_indicators（云端）"""
    result = {"m2": 0, "social_finance": 0, "pmi": 0}

    # ── M2 增速 ──────────────────────────────────────────────────────────────
    try:
        df = _retry(ak.macro_china_money_supply)
        if df is not None and not df.empty:
            col_date = "月份" if "月份" in df.columns else df.columns[0]
            col_m2 = next((c for c in df.columns if "M2" in c and "同比" in c), None)
            if col_m2 is None:
                col_m2 = next((c for c in df.columns if "M2" in c), None)
            if col_m2:
                for _, row in df.iterrows():
                    date_str = str(row[col_date])[:7]  # YYYY-MM
                    val = row[col_m2]
                    if pd.notna(val):
                        try:
                            _upsert_macro_indicator("M2_yoy", date_str, float(val), "%")
                            result["m2"] += 1
                        except Exception:
                            pass
        logger.info(f"M2增速: {result['m2']}条")
    except Exception as e:
        logger.error(f"采集M2失败: {e}")

    # ── 社融 ─────────────────────────────────────────────────────────────────
    try:
        df = _retry(ak.macro_china_new_financial_credit)
        if df is not None and not df.empty:
            col_date = df.columns[0]
            col_val = next((c for c in df.columns if "当月" in c and "同比" not in c), None)
            col_yoy = next((c for c in df.columns if "当月" in c and "同比" in c), None)
            for _, row in df.iterrows():
                date_str = str(row[col_date])[:7]
                if col_val and pd.notna(row.get(col_val)):
                    try:
                        _upsert_macro_indicator("social_finance_monthly", date_str,
                                                float(row[col_val]), "亿元")
                        result["social_finance"] += 1
                    except Exception:
                        pass
                if col_yoy and pd.notna(row.get(col_yoy)):
                    try:
                        _upsert_macro_indicator("social_finance_yoy", date_str,
                                                float(row[col_yoy]), "%")
                    except Exception:
                        pass
        logger.info(f"社融: {result['social_finance']}条")
    except Exception as e:
        logger.error(f"采集社融失败: {e}")

    # ── PMI ──────────────────────────────────────────────────────────────────
    try:
        df = _retry(ak.macro_china_pmi)
        if df is not None and not df.empty:
            col_date = df.columns[0]
            col_mfg = next((c for c in df.columns if "制造业" in c and "指数" in c), None)
            col_svc = next((c for c in df.columns if "非制造业" in c and "指数" in c), None)
            for _, row in df.iterrows():
                date_str = str(row[col_date])[:7]
                if col_mfg and pd.notna(row.get(col_mfg)):
                    try:
                        _upsert_macro_indicator("pmi_manufacturing", date_str,
                                                float(row[col_mfg]), "")
                        result["pmi"] += 1
                    except Exception:
                        pass
                if col_svc and pd.notna(row.get(col_svc)):
                    try:
                        _upsert_macro_indicator("pmi_services", date_str,
                                                float(row[col_svc]), "")
                    except Exception:
                        pass
        logger.info(f"PMI: {result['pmi']}条")
    except Exception as e:
        logger.error(f"采集PMI失败: {e}")

    return result


def fetch_shibor_daily() -> int:
    """采集 Shibor 1W 日度数据 → macro_indicators（云端）"""
    count = 0
    try:
        df = _retry(ak.macro_china_shibor_all)
        if df is None or df.empty:
            return 0
        col_date = df.columns[0]
        col_1w = next((c for c in df.columns if "1W" in c or "1w" in c.lower()), None)
        if col_1w is None:
            logger.warning("Shibor数据中未找到1W列")
            return 0
        for _, row in df.iterrows():
            date_str = str(row[col_date])[:10]
            val = row[col_1w]
            if pd.notna(val):
                try:
                    _upsert_macro_indicator("shibor_1w", date_str, float(val), "%")
                    count += 1
                except Exception:
                    pass
        logger.info(f"Shibor 1W: {count}条")
    except Exception as e:
        logger.error(f"采集Shibor失败: {e}")
    return count


def fetch_margin_balance() -> int:
    """采集融资余额 → margin_balance（云端）"""
    count = 0
    try:
        df = _retry(ak.stock_margin_sse)
        if df is None or df.empty:
            return 0
        col_date = df.columns[0]
        col_mb = next((c for c in df.columns if "融资余额" in c and "买入" not in c), None)
        col_buy = next((c for c in df.columns if "融资买入额" in c), None)
        col_total = next((c for c in df.columns if "融资融券余额" in c), None)
        for _, row in df.iterrows():
            date_str = str(row[col_date])[:10]
            mb = float(row[col_mb]) if col_mb and pd.notna(row.get(col_mb)) else None
            buy = float(row[col_buy]) if col_buy and pd.notna(row.get(col_buy)) else None
            total = float(row[col_total]) if col_total and pd.notna(row.get(col_total)) else None
            try:
                execute_cloud_insert(
                    """INSERT INTO margin_balance (trade_date, margin_balance, margin_buy, total_balance)
                       VALUES (%s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE
                         margin_balance=VALUES(margin_balance),
                         margin_buy=VALUES(margin_buy),
                         total_balance=VALUES(total_balance)""",
                    [date_str, mb, buy, total],
                )
                count += 1
            except Exception:
                pass
        logger.info(f"融资余额: {count}条")
    except Exception as e:
        logger.error(f"采集融资余额失败: {e}")
    return count


def fetch_market_valuation() -> int:
    """采集全A PE分位数 + 市盈率 → market_valuation（云端）"""
    count = 0
    rows_map = {}  # trade_date -> dict

    # ── stock_a_ttm_lyr（PE分位数）────────────────────────────────────────────
    try:
        df = _retry(ak.stock_a_ttm_lyr)
        if df is not None and not df.empty:
            col_date = df.columns[0]
            col_pe_med = next((c for c in df.columns if "middlePETTM" in c or "中位" in c), None)
            col_q10 = next((c for c in df.columns if "quantileInRecent10" in c or "10年" in c), None)
            col_q_all = next((c for c in df.columns if "quantileInAll" in c or "历史" in c), None)
            for _, row in df.iterrows():
                date_str = str(row[col_date])[:10]
                d = rows_map.setdefault(date_str, {})
                if col_pe_med and pd.notna(row.get(col_pe_med)):
                    d["pe_ttm_median"] = float(row[col_pe_med])
                if col_q10 and pd.notna(row.get(col_q10)):
                    d["pe_quantile_10y"] = float(row[col_q10])
                if col_q_all and pd.notna(row.get(col_q_all)):
                    d["pe_quantile_all"] = float(row[col_q_all])
    except Exception as e:
        logger.warning(f"stock_a_ttm_lyr 失败: {e}")

    # ── stock_market_pe_lg（总市值+市盈率）───────────────────────────────────
    try:
        df = _retry(ak.stock_market_pe_lg, symbol="全部A股")
        if df is not None and not df.empty:
            col_date = df.columns[0]
            col_cap = next((c for c in df.columns if "总市值" in c), None)
            col_pe = next((c for c in df.columns if "市盈率" in c), None)
            for _, row in df.iterrows():
                date_str = str(row[col_date])[:10]
                d = rows_map.setdefault(date_str, {})
                if col_cap and pd.notna(row.get(col_cap)):
                    d["total_market_cap"] = float(row[col_cap])
                if col_pe and pd.notna(row.get(col_pe)):
                    d["market_pe"] = float(row[col_pe])
    except Exception as e:
        logger.warning(f"stock_market_pe_lg 失败: {e}")

    # ── 写入 market_valuation ─────────────────────────────────────────────────
    for date_str, d in rows_map.items():
        try:
            execute_cloud_insert(
                """INSERT INTO market_valuation
                   (trade_date, pe_ttm_median, pe_ttm_avg, pe_quantile_10y, pe_quantile_all,
                    total_market_cap, market_pe)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                     pe_ttm_median=COALESCE(VALUES(pe_ttm_median), pe_ttm_median),
                     pe_ttm_avg=COALESCE(VALUES(pe_ttm_avg), pe_ttm_avg),
                     pe_quantile_10y=COALESCE(VALUES(pe_quantile_10y), pe_quantile_10y),
                     pe_quantile_all=COALESCE(VALUES(pe_quantile_all), pe_quantile_all),
                     total_market_cap=COALESCE(VALUES(total_market_cap), total_market_cap),
                     market_pe=COALESCE(VALUES(market_pe), market_pe)""",
                [date_str,
                 d.get("pe_ttm_median"), d.get("pe_ttm_avg"),
                 d.get("pe_quantile_10y"), d.get("pe_quantile_all"),
                 d.get("total_market_cap"), d.get("market_pe")],
            )
            count += 1
        except Exception:
            pass

    logger.info(f"全A估值: {count}条")
    return count


def fetch_hsgt_holding() -> int:
    """采集陆股通持股变动（北向今日排行）→ hsgt_holding（云端）"""
    count = 0
    try:
        df = _retry(ak.stock_hsgt_hold_stock_em, market="北向", indicator="今日排行")
        if df is None or df.empty:
            return 0
        today = datetime.now().strftime("%Y-%m-%d")
        col_code = next((c for c in df.columns if "代码" in c or "股票代码" in c), None)
        col_name = next((c for c in df.columns if "名称" in c or "股票名称" in c), None)
        col_close = next((c for c in df.columns if "收盘价" in c or "最新价" in c), None)
        col_shares = next((c for c in df.columns if "持股" in c and "股数" in c), None)
        col_mv = next((c for c in df.columns if "持股" in c and "市值" in c), None)
        col_ratio_f = next((c for c in df.columns if "流通" in c and "%" in c), None)
        col_ratio_t = next((c for c in df.columns if "总股本" in c and "%" in c), None)
        col_chg_s = next((c for c in df.columns if "增持" in c and "股数" in c), None)
        col_chg_mv = next((c for c in df.columns if "增持" in c and "市值" in c), None)
        col_sector = next((c for c in df.columns if "行业" in c or "板块" in c), None)

        for _, row in df.iterrows():
            code = str(row[col_code]) if col_code else None
            if not code:
                continue
            try:
                execute_cloud_insert(
                    """INSERT INTO hsgt_holding
                       (trade_date, stock_code, stock_name, close_price,
                        holding_shares, holding_market_value, holding_ratio_float,
                        holding_ratio_total, change_shares, change_market_value, sector)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON DUPLICATE KEY UPDATE
                         stock_name=VALUES(stock_name),
                         close_price=VALUES(close_price),
                         holding_shares=VALUES(holding_shares),
                         holding_market_value=VALUES(holding_market_value),
                         holding_ratio_float=VALUES(holding_ratio_float),
                         holding_ratio_total=VALUES(holding_ratio_total),
                         change_shares=VALUES(change_shares),
                         change_market_value=VALUES(change_market_value),
                         sector=VALUES(sector)""",
                    [today, code,
                     str(row[col_name]) if col_name else None,
                     float(row[col_close]) if col_close and pd.notna(row.get(col_close)) else None,
                     float(row[col_shares]) if col_shares and pd.notna(row.get(col_shares)) else None,
                     float(row[col_mv]) if col_mv and pd.notna(row.get(col_mv)) else None,
                     float(row[col_ratio_f]) if col_ratio_f and pd.notna(row.get(col_ratio_f)) else None,
                     float(row[col_ratio_t]) if col_ratio_t and pd.notna(row.get(col_ratio_t)) else None,
                     float(row[col_chg_s]) if col_chg_s and pd.notna(row.get(col_chg_s)) else None,
                     float(row[col_chg_mv]) if col_chg_mv and pd.notna(row.get(col_chg_mv)) else None,
                     str(row[col_sector]) if col_sector and pd.notna(row.get(col_sector)) else None],
                )
                count += 1
            except Exception:
                pass
        logger.info(f"陆股通持股: {count}条")
    except Exception as e:
        logger.error(f"采集陆股通持股失败: {e}")
    return count


def fetch_overseas_etf() -> int:
    """采集海外ETF日线 (KWEB/FXI/ASHR) → overseas_etf（云端）"""
    count = 0
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")

    for symbol, etf_name in OVERSEAS_ETF_SYMBOLS:
        try:
            df = _retry(ak.stock_us_daily, symbol=symbol, adjust="qfq")
            if df is None or df.empty:
                continue
            col_date = df.columns[0]
            col_open = next((c for c in df.columns if c.lower() in ("open", "开盘")), None)
            col_high = next((c for c in df.columns if c.lower() in ("high", "最高")), None)
            col_low = next((c for c in df.columns if c.lower() in ("low", "最低")), None)
            col_close = next((c for c in df.columns if c.lower() in ("close", "收盘")), None)
            col_vol = next((c for c in df.columns if c.lower() in ("volume", "成交量")), None)
            for _, row in df.iterrows():
                date_str = str(row[col_date])[:10]
                try:
                    execute_cloud_insert(
                        """INSERT INTO overseas_etf
                           (symbol, etf_name, trade_date, open, high, low, close, volume)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                           ON DUPLICATE KEY UPDATE
                             open=VALUES(open), high=VALUES(high), low=VALUES(low),
                             close=VALUES(close), volume=VALUES(volume)""",
                        [symbol, etf_name, date_str,
                         float(row[col_open]) if col_open and pd.notna(row.get(col_open)) else None,
                         float(row[col_high]) if col_high and pd.notna(row.get(col_high)) else None,
                         float(row[col_low]) if col_low and pd.notna(row.get(col_low)) else None,
                         float(row[col_close]) if col_close and pd.notna(row.get(col_close)) else None,
                         float(row[col_vol]) if col_vol and pd.notna(row.get(col_vol)) else None],
                    )
                    count += 1
                except Exception:
                    pass
            logger.info(f"海外ETF {symbol}: 写入完成")
        except Exception as e:
            logger.error(f"采集海外ETF {symbol} 失败: {e}")

    return count


def fetch_all_macro() -> dict:
    """汇总调用所有宏观数据采集函数"""
    logger.info("开始采集宏观数据...")
    result = {}
    result["shibor"] = fetch_shibor_daily()
    result["margin_balance"] = fetch_margin_balance()
    result["market_valuation"] = fetch_market_valuation()
    result["hsgt_holding"] = fetch_hsgt_holding()
    result["overseas_etf"] = fetch_overseas_etf()
    logger.info(f"宏观数据采集完成: {result}")
    return result


def fetch_all_macro_monthly() -> dict:
    """月度宏观数据采集（M2/社融/PMI）"""
    logger.info("开始采集月度宏观数据...")
    result = fetch_macro_monthly()
    logger.info(f"月度宏观数据采集完成: {result}")
    return result

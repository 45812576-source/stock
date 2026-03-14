"""robust_kline/filter.py — 月K线筛选爸爸备选

从 robust_kline_mentions 取当日 stock_codes，
用本地 stock_daily → 月K线 + 周K线筛选：
  类型1：7个月内任意连续3个完整月均为阳线（close > open）
  类型2：7个月内任意连续4个月窗口中，至少3个月为阳线
  类型3（2+3）：最近2个完整月为阳线 且 最近3个完整周为阳线
  涨幅过滤：7个月累计涨幅 ≤ 50%
"""
import logging
from datetime import date
from typing import Optional, Tuple

from utils.db_utils import execute_query, execute_insert
from stock_selector.kline_calc import _fetch_daily, _resample_monthly, _resample_weekly

logger = logging.getLogger(__name__)

# 用多少天数据换月线（7个完整月 ≈ 210 天，留余量）
_DAILY_DAYS = 280


def _get_scan_codes(scan_date: date) -> list[dict]:
    """取当日 mentions 中有 stock_code 的去重列表"""
    rows = execute_query(
        """SELECT stock_code, stock_name, industry,
                  COUNT(*) AS mention_count
           FROM robust_kline_mentions
           WHERE scan_date = %s
             AND stock_code IS NOT NULL
             AND stock_code != ''
           GROUP BY stock_code, stock_name, industry
           ORDER BY mention_count DESC""",
        [str(scan_date)],
    )
    return rows or []


def _get_latest_price(code: str, daily: list[dict]) -> Optional[float]:
    if not daily:
        return None
    return daily[-1].get("close")


def _check_yang_monthly(monthly: list[dict]) -> Tuple[Optional[int], list, Optional[float]]:
    """
    对月K线（最近7个完整月，最新月排除当前未完成的）进行检测。

    Returns:
        (match_type, yang_months_list, gain_pct)
        match_type: 1 / 2 / None
        yang_months_list: 阳线月份
        gain_pct: 7个月累计涨幅%
    """
    # 去掉最新（可能未完成）月
    bars = monthly[:-1] if len(monthly) > 1 else monthly
    # 取最近7个完整月
    window = bars[-7:] if len(bars) >= 7 else bars

    if len(window) < 3:
        return None, [], None

    # 阳线判断
    yang_flags = [b["close"] > b["open"] for b in window]
    yang_months = [b["ym"] for b, flag in zip(window, yang_flags) if flag]

    # 涨幅过滤：7个月期间 open[0] → close[-1]
    try:
        gain_pct = (window[-1]["close"] - window[0]["open"]) / window[0]["open"] * 100
    except (ZeroDivisionError, TypeError):
        gain_pct = None

    if gain_pct is not None and gain_pct > 50:
        return None, yang_months, gain_pct

    # 类型1：任意连续3个完整月均为阳线
    n = len(yang_flags)
    for i in range(n - 2):
        if yang_flags[i] and yang_flags[i + 1] and yang_flags[i + 2]:
            return 1, yang_months, gain_pct

    # 类型2：任意连续4个月窗口中，至少3个月为阳线
    for i in range(n - 3):
        if sum(yang_flags[i:i + 4]) >= 3:
            return 2, yang_months, gain_pct

    return None, yang_months, gain_pct


def _check_type3(monthly: list[dict], weekly: list[dict]) -> Tuple[bool, list, Optional[float]]:
    """类型3（2+3）：最近2个完整月为阳线 且 最近3个完整周为阳线

    Returns:
        (matched, yang_labels, gain_pct)
    """
    # 月线：去掉最新（可能未完成）月，取最近2个
    m_bars = monthly[:-1] if len(monthly) > 1 else monthly
    if len(m_bars) < 2:
        return False, [], None

    last2m = m_bars[-2:]
    m_yang = all(b["close"] > b["open"] for b in last2m)
    if not m_yang:
        return False, [], None

    # 周线：去掉最新（可能未完成）周，取最近3个
    w_bars = weekly[:-1] if len(weekly) > 1 else weekly
    if len(w_bars) < 3:
        return False, [], None

    last3w = w_bars[-3:]
    w_yang = all(b["close"] > b["open"] for b in last3w)
    if not w_yang:
        return False, [], None

    # 涨幅：用月线的7个月窗口
    window = m_bars[-7:] if len(m_bars) >= 7 else m_bars
    try:
        gain_pct = (window[-1]["close"] - window[0]["open"]) / window[0]["open"] * 100
    except (ZeroDivisionError, TypeError):
        gain_pct = None

    if gain_pct is not None and gain_pct > 50:
        return False, [], gain_pct

    yang_labels = [b["ym"] for b in last2m] + [b["wk"] for b in last3w]
    return True, yang_labels, gain_pct


def filter_candidates(scan_date: date = None) -> dict:
    """筛选爸爸备选，写入 robust_kline_candidates

    Returns:
        {"scanned": n, "matched": n, "inserted": n}
    """
    if scan_date is None:
        scan_date = date.today()

    code_rows = _get_scan_codes(scan_date)
    if not code_rows:
        logger.info(f"[Filter] scan_date={scan_date} 无 mentions，跳过")
        return {"scanned": 0, "matched": 0, "inserted": 0}

    codes = [r["stock_code"] for r in code_rows]
    meta_map = {r["stock_code"]: r for r in code_rows}

    logger.info(f"[Filter] 待筛选股票 {len(codes)} 只")

    # 批量拉取日线
    daily_map = _fetch_daily(codes, days=_DAILY_DAYS)

    matched = 0
    inserted = 0

    for code in codes:
        daily = daily_map.get(code, [])
        if not daily:
            continue

        monthly = _resample_monthly(daily)
        weekly = _resample_weekly(daily)
        meta = meta_map[code]
        latest_price = _get_latest_price(code, daily)

        # 收集所有匹配的类型（一只股票可能同时命中多个类型）
        hits = []  # [(match_type, yang_str, gain_pct), ...]

        # 类型1 & 类型2
        mt12, ym12, gp12 = _check_yang_monthly(monthly)
        if mt12 is not None:
            hits.append((mt12, ",".join(ym12[:20]), gp12))

        # 类型3（2+3）
        t3_ok, t3_labels, gp3 = _check_type3(monthly, weekly)
        if t3_ok:
            hits.append((3, ",".join(t3_labels[:20]), gp3))

        for match_type, yang_months_str, gain_pct in hits:
            matched += 1
            try:
                execute_insert(
                    """INSERT INTO robust_kline_candidates
                       (stock_code, stock_name, industry, match_type, yang_months,
                        gain_pct, latest_price, mention_count, highlight, scan_date)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NULL,%s)
                       ON DUPLICATE KEY UPDATE
                         stock_name=VALUES(stock_name),
                         industry=VALUES(industry),
                         match_type=VALUES(match_type),
                         yang_months=VALUES(yang_months),
                         gain_pct=VALUES(gain_pct),
                         latest_price=VALUES(latest_price),
                         mention_count=VALUES(mention_count)""",
                    [
                        code,
                        meta.get("stock_name"),
                        meta.get("industry"),
                        match_type,
                        yang_months_str,
                        round(gain_pct, 2) if gain_pct is not None else None,
                        round(latest_price, 2) if latest_price is not None else None,
                        meta.get("mention_count", 1),
                        str(scan_date),
                    ],
                )
                inserted += 1
                logger.debug(f"[Filter] {code} 匹配类型{match_type}, 阳线={yang_months_str}, 涨幅={gain_pct}")
            except Exception as e:
                logger.warning(f"[Filter] {code} 写入失败: {e}")

    logger.info(f"[Filter] 扫描={len(codes)}, 匹配={matched}, 写入={inserted}")
    return {"scanned": len(codes), "matched": matched, "inserted": inserted}

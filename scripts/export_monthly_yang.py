#!/usr/bin/env python3
"""
导出近7个月曾出现连续3个月月K收阳、且当前股价较7个月前未涨超50%的股票清单。

条件：
  1. 近7个月（含最近不完整月）内，任意连续3个完整自然月均为阳线（月收盘 > 月开盘）
  2. 当前价 / 7个月前首个交易日开盘价 - 1 <= 50%（未过度追高）

输出：CSV 到桌面，含股票代码、名称、行业、当前价、7月前价格、涨幅、匹配的三连阳区间
"""
import csv
import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import execute_query

TODAY = datetime.date(2026, 3, 3)
# 7个月前（含当日往前推7个月）
START_DATE = (TODAY.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)  # 上月初
# 实际取7个完整月：从 7个月前的月初开始
import dateutil.relativedelta
SEVEN_MONTHS_AGO = TODAY - dateutil.relativedelta.relativedelta(months=7)
RANGE_START = SEVEN_MONTHS_AGO.strftime("%Y-%m-01")

OUTPUT_PATH = os.path.expanduser("~/Desktop/monthly_yang_stocks.csv")


def get_monthly_candles():
    """从 stock_daily 拉取原始日线，在 Python 侧计算月开盘/月收盘。"""
    rows = execute_query(
        "SELECT stock_code, trade_date, open, close FROM stock_daily "
        "WHERE trade_date >= %s ORDER BY stock_code, trade_date",
        [RANGE_START],
    ) or []

    from collections import defaultdict
    # 按 (stock_code, month) 分组，记录首日open 和 末日close
    group = defaultdict(list)  # key=(stock_code, month), value=[(trade_date, open, close)]
    for r in rows:
        month = r["trade_date"][:7]
        group[(r["stock_code"], month)].append(r)

    result = []
    for (stock_code, month), days in group.items():
        days_sorted = sorted(days, key=lambda x: x["trade_date"])
        result.append({
            "stock_code": stock_code,
            "month": month,
            "month_open": days_sorted[0]["open"],
            "month_close": days_sorted[-1]["close"],
        })
    result.sort(key=lambda x: (x["stock_code"], x["month"]))
    return result


def get_price_7m_ago():
    """获取每只股票7个月前首个交易日的开盘价。"""
    rows = execute_query(
        """
        SELECT stock_code,
               open AS price_7m_ago,
               trade_date
        FROM stock_daily d1
        WHERE trade_date = (
            SELECT MIN(trade_date) FROM stock_daily d2
            WHERE d2.stock_code = d1.stock_code
              AND trade_date >= %s
        )
        """,
        [RANGE_START],
    ) or []
    return {r["stock_code"]: r for r in rows}


def get_latest_price():
    """获取每只股票最近交易日的收盘价。"""
    rows = execute_query(
        """
        SELECT stock_code, close AS latest_price, trade_date AS latest_date
        FROM stock_daily d1
        WHERE trade_date = (
            SELECT MAX(trade_date) FROM stock_daily d2
            WHERE d2.stock_code = d1.stock_code
        )
        """
    ) or []
    return {r["stock_code"]: r for r in rows}


def get_stock_info():
    """获取股票名称和行业。"""
    rows = execute_query(
        "SELECT stock_code, stock_name, industry_l1, industry_l2 FROM stock_info"
    ) or []
    return {r["stock_code"]: r for r in rows}


def find_3_consecutive_yang(monthly_candles: list):
    """
    在月度K线列表中寻找任意3个连续完整月均收阳线。
    返回匹配区间字符串如 '2025-09 ~ 2025-11'，无则返回 None。
    """
    # 只取完整月（过滤掉当前未收完整的月份）
    current_month = TODAY.strftime("%Y-%m")
    full_months = [m for m in monthly_candles if m["month"] < current_month]

    for i in range(len(full_months) - 2):
        a, b, c = full_months[i], full_months[i + 1], full_months[i + 2]
        # 三个月均收阳（收盘 > 开盘）
        if (
            a["month_open"] and a["month_close"] and a["month_close"] > a["month_open"]
            and b["month_open"] and b["month_close"] and b["month_close"] > b["month_open"]
            and c["month_open"] and c["month_close"] and c["month_close"] > c["month_open"]
        ):
            return f"{a['month']} ~ {c['month']}"
    return None


def main():
    print("读取月度K线数据…")
    raw_monthly = get_monthly_candles()

    # 按股票分组
    from collections import defaultdict
    by_stock = defaultdict(list)
    for row in raw_monthly:
        by_stock[row["stock_code"]].append(row)

    print(f"共 {len(by_stock)} 只股票有数据，检索连续三月阳线…")

    price_7m = get_price_7m_ago()
    latest = get_latest_price()
    info = get_stock_info()

    results = []
    for stock_code, candles in by_stock.items():
        interval = find_3_consecutive_yang(candles)
        if not interval:
            continue

        p7m = price_7m.get(stock_code, {})
        lat = latest.get(stock_code, {})
        inf = info.get(stock_code, {})

        price_ago = p7m.get("price_7m_ago")
        price_now = lat.get("latest_price")
        if not price_ago or not price_now:
            continue

        # 涨幅过滤：不超过 50%
        gain_pct = (price_now - price_ago) / price_ago * 100
        if gain_pct > 50:
            continue

        results.append({
            "stock_code": stock_code,
            "stock_name": inf.get("stock_name", ""),
            "industry_l1": inf.get("industry_l1", ""),
            "industry_l2": inf.get("industry_l2", ""),
            "latest_price": round(price_now, 2),
            "price_7m_ago": round(price_ago, 2),
            "gain_pct": round(gain_pct, 2),
            "yang_interval": interval,
            "latest_date": str(lat.get("latest_date", "")),
            "price_7m_date": str(p7m.get("trade_date", "")),
        })

    # 按涨幅升序排列（未涨的排前面）
    results.sort(key=lambda x: x["gain_pct"])

    print(f"符合条件：{len(results)} 只股票")

    # 写 CSV
    fieldnames = [
        "stock_code", "stock_name", "industry_l1", "industry_l2",
        "latest_price", "price_7m_ago", "gain_pct",
        "yang_interval", "latest_date", "price_7m_date",
    ]
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"已导出 → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()

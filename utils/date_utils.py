"""交易日历工具"""
from datetime import datetime, timedelta


def is_trading_day(date_str):
    """简单判断是否为交易日（排除周末，节假日需后续完善）"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.weekday() < 5


def get_last_trading_day():
    """获取最近一个交易日"""
    today = datetime.now()
    dt = today - timedelta(days=1)
    while dt.weekday() >= 5:
        dt -= timedelta(days=1)
    return dt.strftime("%Y-%m-%d")


def get_trading_days(start_date, end_date):
    """获取日期范围内的交易日列表"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return days


def date_range_back(days=30):
    """从今天往回推N天，返回(start, end)"""
    end = datetime.now()
    start = end - timedelta(days=days)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

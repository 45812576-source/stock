"""技术指标计算 — 纯Python，零AI依赖"""
import math
from typing import Optional
from utils.db_utils import execute_query


def _ema(arr: list, n: int) -> list:
    k = 2 / (n + 1)
    result = [None] * len(arr)
    # 找第一个非None值
    start = next((i for i, v in enumerate(arr) if v is not None), None)
    if start is None:
        return result
    result[start] = arr[start]
    for i in range(start + 1, len(arr)):
        if arr[i] is None:
            result[i] = result[i - 1]
        else:
            result[i] = arr[i] * k + result[i - 1] * (1 - k)
    return result


def _sma(arr: list, n: int) -> list:
    result = []
    for i in range(len(arr)):
        if i < n - 1:
            result.append(None)
        else:
            window = [v for v in arr[i - n + 1:i + 1] if v is not None]
            result.append(sum(window) / len(window) if window else None)
    return result


def compute_all_indicators(stock_code: str, days: int = 180) -> dict:
    """
    预计算全部技术指标，返回结构化字典。

    Returns:
        {
            "dates": [...],
            "ohlcv": [{"date","open","high","low","close","volume","amount"}, ...],
            "ma5": [...], "ma10": [...], "ma20": [...], "ma60": [...],
            "boll_upper": [...], "boll_mid": [...], "boll_lower": [...],
            "rsi14": [...],
            "macd_dif": [...], "macd_dea": [...], "macd_hist": [...],
            "kdj_k": [...], "kdj_d": [...], "kdj_j": [...],
            "volume_ratio": [...],   # 当日量/20日均量
            "capital_flow": [{"date","main_net_inflow",...}, ...],
            "profit_ratio": [...],   # 估算获利盘比例 (0-100)
        }
    """
    # 多取60天用于指标预热
    fetch_days = days + 60
    rows = execute_query(
        """SELECT trade_date, open, high, low, close, volume, amount
           FROM stock_daily WHERE stock_code=%s
           ORDER BY trade_date DESC LIMIT %s""",
        [stock_code, fetch_days],
    )
    if not rows:
        return {}

    rows = list(reversed(rows))
    ohlcv = [dict(r) for r in rows]

    dates = [r["trade_date"] for r in ohlcv]
    closes = [float(r["close"] or 0) for r in ohlcv]
    highs  = [float(r["high"] or 0) for r in ohlcv]
    lows   = [float(r["low"] or 0) for r in ohlcv]
    vols   = [float(r["volume"] or 0) for r in ohlcv]

    # MA
    ma5  = _sma(closes, 5)
    ma10 = _sma(closes, 10)
    ma20 = _sma(closes, 20)
    ma60 = _sma(closes, 60)

    # BOLL (20, 2)
    boll_mid = ma20[:]
    boll_upper, boll_lower = [], []
    for i in range(len(closes)):
        if i < 19 or boll_mid[i] is None:
            boll_upper.append(None)
            boll_lower.append(None)
        else:
            window = closes[i - 19:i + 1]
            std = math.sqrt(sum((v - boll_mid[i]) ** 2 for v in window) / 20)
            boll_upper.append(boll_mid[i] + 2 * std)
            boll_lower.append(boll_mid[i] - 2 * std)

    # RSI(14)
    rsi14 = [None]
    avg_gain, avg_loss = 0.0, 0.0
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gain = max(delta, 0)
        loss = abs(min(delta, 0))
        if i <= 14:
            avg_gain += gain / 14
            avg_loss += loss / 14
            rsi14.append(None if i < 14 else (100 - 100 / (1 + avg_gain / avg_loss) if avg_loss else 100))
        else:
            avg_gain = (avg_gain * 13 + gain) / 14
            avg_loss = (avg_loss * 13 + loss) / 14
            rsi14.append(100 - 100 / (1 + avg_gain / avg_loss) if avg_loss else 100)

    # MACD (12, 26, 9)
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    dif = [a - b if a is not None and b is not None else None for a, b in zip(ema12, ema26)]
    dea = _ema(dif, 9)
    hist = [(d - e) * 2 if d is not None and e is not None else None for d, e in zip(dif, dea)]

    # KDJ (9, 3, 3)
    rsv = []
    for i in range(len(closes)):
        st = max(0, i - 8)
        h9 = max(highs[st:i + 1])
        l9 = min(lows[st:i + 1])
        rsv.append(50.0 if h9 == l9 else (closes[i] - l9) / (h9 - l9) * 100)
    kdj_k, kdj_d = [50.0], [50.0]
    for i in range(1, len(rsv)):
        kdj_k.append(2 / 3 * kdj_k[-1] + 1 / 3 * rsv[i])
        kdj_d.append(2 / 3 * kdj_d[-1] + 1 / 3 * kdj_k[-1])
    kdj_j = [3 * k - 2 * d for k, d in zip(kdj_k, kdj_d)]

    # 量比 (当日量 / 20日均量)
    vol_ratio = []
    for i in range(len(vols)):
        if i < 20:
            vol_ratio.append(None)
        else:
            avg20 = sum(vols[i - 20:i]) / 20
            vol_ratio.append(vols[i] / avg20 if avg20 > 0 else None)

    # 资金流向
    cap_rows = execute_query(
        """SELECT trade_date, main_net_inflow, super_large_net, large_net, medium_net, small_net
           FROM capital_flow WHERE stock_code=%s
           ORDER BY trade_date DESC LIMIT %s""",
        [stock_code, fetch_days],
    )
    capital_flow = [dict(r) for r in reversed(cap_rows)] if cap_rows else []
    cap_map = {r["trade_date"]: r for r in capital_flow}

    # 估算获利盘比例（简化：基于过去60日成交量加权平均成本 vs 当前价）
    profit_ratio = []
    for i in range(len(closes)):
        if i < 60:
            profit_ratio.append(None)
        else:
            window_v = vols[i - 60:i + 1]
            window_c = closes[i - 60:i + 1]
            total_vol = sum(window_v)
            if total_vol == 0:
                profit_ratio.append(50.0)
                continue
            vwap = sum(v * c for v, c in zip(window_v, window_c)) / total_vol
            # 简化：当前价高于VWAP则获利盘比例高
            ratio = min(100, max(0, 50 + (closes[i] - vwap) / vwap * 200))
            profit_ratio.append(round(ratio, 1))

    # 只返回最近 days 天的数据（去掉预热部分）
    trim = max(0, len(dates) - days)
    def _trim(lst):
        return lst[trim:] if lst else lst

    return {
        "dates":       _trim(dates),
        "ohlcv":       _trim(ohlcv),
        "ma5":         _trim(ma5),
        "ma10":        _trim(ma10),
        "ma20":        _trim(ma20),
        "ma60":        _trim(ma60),
        "boll_upper":  _trim(boll_upper),
        "boll_mid":    _trim(boll_mid),
        "boll_lower":  _trim(boll_lower),
        "rsi14":       _trim(rsi14),
        "macd_dif":    _trim(dif),
        "macd_dea":    _trim(dea),
        "macd_hist":   _trim(hist),
        "kdj_k":       _trim(kdj_k),
        "kdj_d":       _trim(kdj_d),
        "kdj_j":       _trim(kdj_j),
        "volume_ratio": _trim(vol_ratio),
        "capital_flow": capital_flow,  # 不trim，按日期查
        "profit_ratio": _trim(profit_ratio),
        "cap_map":     cap_map,
    }

"""kline_calc — K线技术指标计算引擎

对给定的 codes 列表批量计算技术指标，返回 {code: result} 字典。
所有计算基于本地 stock_daily 表，无需外部 API。
"""
import logging
from typing import Optional
from utils.db_utils import execute_query

logger = logging.getLogger(__name__)


def _fetch_daily(codes: list[str], days: int = 300) -> dict[str, list[dict]]:
    """批量拉取 stock_daily，按 code 分组，按日期升序"""
    if not codes:
        return {}
    ph = ",".join(["%s"] * len(codes))
    rows = execute_query(
        f"""SELECT stock_code, trade_date, open, high, low, close, volume, change_pct
            FROM stock_daily
            WHERE stock_code IN ({ph})
              AND trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            ORDER BY stock_code, trade_date ASC""",
        codes + [days],
    )
    result: dict[str, list[dict]] = {}
    for r in (rows or []):
        code = r["stock_code"]
        result.setdefault(code, []).append(dict(r))
    return result


def _resample_monthly(daily: list[dict]) -> list[dict]:
    """日线聚合为月线 OHLCV"""
    months: dict[str, list[dict]] = {}
    for d in daily:
        ym = d["trade_date"][:7].replace("-", "")  # 2026-02 → 202602
        months.setdefault(ym, []).append(d)
    result = []
    for ym in sorted(months):
        bars = months[ym]
        result.append({
            "ym": ym,
            "open": bars[0]["open"],
            "high": max(b["high"] for b in bars),
            "low": min(b["low"] for b in bars),
            "close": bars[-1]["close"],
            "volume": sum(b["volume"] or 0 for b in bars),
        })
    return result


def _resample_weekly(daily: list[dict]) -> list[dict]:
    """日线聚合为周线（按自然周 ISO week）"""
    from datetime import datetime
    weeks: dict[str, list[dict]] = {}
    for d in daily:
        try:
            dt = datetime.strptime(d["trade_date"], "%Y-%m-%d")
            wk = dt.strftime("%Y-W%W")
        except Exception:
            continue
        weeks.setdefault(wk, []).append(d)
    result = []
    for wk in sorted(weeks):
        bars = weeks[wk]
        result.append({
            "wk": wk,
            "open": bars[0]["open"],
            "high": max(b["high"] for b in bars),
            "low": min(b["low"] for b in bars),
            "close": bars[-1]["close"],
            "volume": sum(b["volume"] or 0 for b in bars),
        })
    return result


def _calc_ma(closes: list[float], period: int) -> list[Optional[float]]:
    mas = []
    for i in range(len(closes)):
        if i < period - 1:
            mas.append(None)
        else:
            mas.append(sum(closes[i - period + 1: i + 1]) / period)
    return mas


def _calc_ema(closes: list[float], period: int) -> list[float]:
    emas = []
    k = 2 / (period + 1)
    for i, c in enumerate(closes):
        if i == 0:
            emas.append(c)
        else:
            emas.append(c * k + emas[-1] * (1 - k))
    return emas


def _calc_macd(closes: list[float]):
    """返回 (dif, dea, hist) 列表"""
    ema12 = _calc_ema(closes, 12)
    ema26 = _calc_ema(closes, 26)
    dif = [a - b for a, b in zip(ema12, ema26)]
    dea = _calc_ema(dif, 9)
    hist = [2 * (d - e) for d, e in zip(dif, dea)]
    return dif, dea, hist


# ==================== 各 action 实现 ====================

def consecutive_yang(daily: list[dict], timeframe: str = "monthly", min_count: int = 3) -> dict:
    """N连阳：连续N根阳线（收盘>开盘）"""
    if timeframe == "monthly":
        bars = _resample_monthly(daily)
    elif timeframe == "weekly":
        bars = _resample_weekly(daily)
    else:
        bars = daily

    if len(bars) < min_count:
        return {"pass": False, "consecutive_count": 0}

    count = 0
    for bar in reversed(bars[:-1] if timeframe == "monthly" else bars):  # 最新月可能未完成
        if bar["close"] is None or bar["open"] is None:
            break
        if bar["close"] > bar["open"]:
            count += 1
        else:
            break

    return {"pass": count >= min_count, "consecutive_count": count}


def ma_bullish(daily: list[dict]) -> dict:
    """均线多头排列：MA5 > MA10 > MA20 > MA60"""
    closes = [d["close"] for d in daily if d["close"] is not None]
    if len(closes) < 60:
        return {"pass": False, "reason": "data_insufficient"}
    ma5 = _calc_ma(closes, 5)[-1]
    ma10 = _calc_ma(closes, 10)[-1]
    ma20 = _calc_ma(closes, 20)[-1]
    ma60 = _calc_ma(closes, 60)[-1]
    ok = ma5 > ma10 > ma20 > ma60
    return {"pass": ok, "ma5": ma5, "ma10": ma10, "ma20": ma20, "ma60": ma60}


def break_ma(daily: list[dict], ma_period: int = 250) -> dict:
    """突破N日均线：今日收盘上穿均线（昨日在均线下，今日在均线上）"""
    closes = [d["close"] for d in daily if d["close"] is not None]
    if len(closes) < ma_period + 1:
        return {"pass": False, "reason": "data_insufficient"}
    mas = _calc_ma(closes, ma_period)
    if mas[-1] is None or mas[-2] is None:
        return {"pass": False, "reason": "data_insufficient"}
    ok = closes[-2] < mas[-2] and closes[-1] >= mas[-1]
    return {"pass": ok, "close": closes[-1], "ma": mas[-1]}


def macd_divergence(daily: list[dict], divergence_type: str = "bottom") -> dict:
    """MACD底背离/顶背离：价格创新低但MACD柱不创新低（底背离）"""
    closes = [d["close"] for d in daily if d["close"] is not None]
    if len(closes) < 40:
        return {"pass": False, "reason": "data_insufficient"}
    _, _, hist = _calc_macd(closes)
    # 取最近30根
    recent_closes = closes[-30:]
    recent_hist = hist[-30:]
    if divergence_type == "bottom":
        price_low_idx = recent_closes.index(min(recent_closes))
        hist_low_idx = recent_hist.index(min(recent_hist))
        # 价格最低点在后，但MACD最低点在前（背离）
        ok = price_low_idx > hist_low_idx and min(recent_closes) < recent_closes[0]
    else:  # top
        price_high_idx = recent_closes.index(max(recent_closes))
        hist_high_idx = recent_hist.index(max(recent_hist))
        ok = price_high_idx > hist_high_idx and max(recent_closes) > recent_closes[0]
    return {"pass": ok}


def volume_breakout(daily: list[dict], volume_ratio: float = 2.0, price_break_days: int = 60) -> dict:
    """放量突破：成交量是N日均量的X倍，且价格突破N日高点"""
    if len(daily) < price_break_days + 5:
        return {"pass": False, "reason": "data_insufficient"}
    recent = daily[-price_break_days:]
    today = daily[-1]
    avg_vol = sum(d["volume"] or 0 for d in daily[-20:-1]) / 19
    high_n = max(d["high"] for d in recent[:-1])
    vol_ok = avg_vol > 0 and (today["volume"] or 0) >= avg_vol * volume_ratio
    price_ok = (today["close"] or 0) > high_n
    return {"pass": vol_ok and price_ok, "vol_ratio": round((today["volume"] or 0) / avg_vol, 2) if avg_vol else 0}


def pullback_support(daily: list[dict], ma_period: int = 20) -> dict:
    """缩量回踩均线支撑：价格回踩MA20附近（±2%），且成交量萎缩"""
    closes = [d["close"] for d in daily if d["close"] is not None]
    if len(closes) < ma_period + 5:
        return {"pass": False, "reason": "data_insufficient"}
    mas = _calc_ma(closes, ma_period)
    ma_now = mas[-1]
    close_now = closes[-1]
    avg_vol = sum(d["volume"] or 0 for d in daily[-20:-5]) / 15
    today_vol = daily[-1]["volume"] or 0
    near_ma = abs(close_now - ma_now) / ma_now < 0.02
    shrink = avg_vol > 0 and today_vol < avg_vol * 0.7
    return {"pass": near_ma and shrink, "near_ma": near_ma, "vol_shrink": shrink}


def box_breakout(daily: list[dict], min_days: int = 30) -> dict:
    """箱体突破：价格突破近N日震荡区间高点"""
    if len(daily) < min_days + 5:
        return {"pass": False, "reason": "data_insufficient"}
    box = daily[-min_days - 5:-5]
    box_high = max(d["high"] for d in box)
    box_low = min(d["low"] for d in box)
    box_range = (box_high - box_low) / box_low if box_low else 0
    # 箱体震荡幅度 < 20%，且今日突破
    in_box = box_range < 0.20
    today_close = daily[-1]["close"] or 0
    breakout = today_close > box_high * 1.005
    return {"pass": in_box and breakout, "box_high": box_high, "close": today_close}


def macd_momentum(daily: list[dict], direction: str = "strengthening") -> dict:
    """MACD动能转强：MACD柱连续3根放大"""
    closes = [d["close"] for d in daily if d["close"] is not None]
    if len(closes) < 35:
        return {"pass": False, "reason": "data_insufficient"}
    _, _, hist = _calc_macd(closes)
    last3 = hist[-3:]
    if direction == "strengthening":
        ok = last3[0] < last3[1] < last3[2] and last3[2] > 0
    else:
        ok = last3[0] > last3[1] > last3[2] and last3[2] < 0
    return {"pass": ok, "hist_last3": [round(h, 4) for h in last3]}


def cumulative_change(daily: list[dict], days: int = 30) -> dict:
    """N日累计涨跌幅"""
    closes = [d["close"] for d in daily if d["close"] is not None]
    if len(closes) < days + 1:
        return {"pass": True, "change_pct": None}
    pct = (closes[-1] - closes[-days - 1]) / closes[-days - 1] * 100
    return {"pass": True, "change_pct": round(pct, 2)}


def capital_tracker_inflow(codes: list[str], action: str, params: dict) -> dict[str, dict]:
    """capital_tracker 中依赖 capital_flow 表的 actions（主力净流入相关）"""
    if not codes:
        return {}
    ph = ",".join(["%s"] * len(codes))
    days = params.get("days", 30)
    rows = execute_query(
        f"""SELECT stock_code, trade_date, main_net_inflow
            FROM capital_flow
            WHERE stock_code IN ({ph})
              AND trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            ORDER BY stock_code, trade_date ASC""",
        codes + [days + 5],
    )
    by_code: dict[str, list[dict]] = {}
    for r in (rows or []):
        by_code.setdefault(r["stock_code"], []).append(dict(r))

    results = {}
    for code in codes:
        cf = by_code.get(code, [])
        if not cf:
            results[code] = {"pass": False, "reason": "no_data"}
            continue
        if action == "consecutive_inflow":
            min_amount = params.get("min_amount", 0)
            n = params.get("days", 5)
            recent = cf[-n:]
            ok = len(recent) >= n and all((r["main_net_inflow"] or 0) >= min_amount for r in recent)
            results[code] = {"pass": ok, "days": len(recent)}
        elif action == "net_inflow_sum":
            total = sum(r["main_net_inflow"] or 0 for r in cf)
            results[code] = {"pass": True, "net_inflow_sum": round(total, 2)}
        else:
            results[code] = {"pass": False, "reason": "unsupported_action"}
    return results


def max_drawdown(daily: list[dict], days: int = 250) -> dict:
    """N日最大回撤"""
    closes = [d["close"] for d in daily if d["close"] is not None]
    recent = closes[-days:] if len(closes) >= days else closes
    if not recent:
        return {"pass": True, "max_drawdown_pct": None}
    peak = recent[0]
    max_dd = 0.0
    for c in recent:
        if c > peak:
            peak = c
        dd = (peak - c) / peak * 100
        if dd > max_dd:
            max_dd = dd
    return {"pass": True, "max_drawdown_pct": round(max_dd, 2)}


# ==================== 批量执行入口 ====================

_ACTION_MAP = {
    "consecutive_yang": lambda d, p: consecutive_yang(d, p.get("timeframe", "monthly"), p.get("min_count", 3)),
    "ma_bullish": lambda d, p: ma_bullish(d),
    "break_ma": lambda d, p: break_ma(d, p.get("ma_period", 250)),
    "macd_divergence": lambda d, p: macd_divergence(d, p.get("type", "bottom")),
    "volume_breakout": lambda d, p: volume_breakout(d, p.get("volume_ratio", 2.0), p.get("price_break_days", 60)),
    "pullback_support": lambda d, p: pullback_support(d, p.get("ma_period", 20)),
    "box_breakout": lambda d, p: box_breakout(d, p.get("min_days", 30)),
    "macd_momentum": lambda d, p: macd_momentum(d, p.get("direction", "strengthening")),
    "cumulative_change": lambda d, p: cumulative_change(d, p.get("days", 30)),
    "max_drawdown": lambda d, p: max_drawdown(d, p.get("days", 250)),
}


def run_kline_calc(codes: list[str], action: str, params: dict) -> dict[str, dict]:
    """批量计算 kline_calc 指标

    Returns:
        {code: {"pass": bool, ...metrics}} — 无数据的 code 返回 {"pass": False, "reason": "no_data"}
    """
    if action not in _ACTION_MAP:
        logger.warning(f"kline_calc: unknown action {action}")
        return {code: {"pass": False, "reason": "unknown_action"} for code in codes}

    # 根据 action 决定需要多少天数据
    days_needed = max(params.get("days", 30), params.get("ma_period", 60), params.get("price_break_days", 60), 300)
    daily_map = _fetch_daily(codes, days=days_needed)

    results = {}
    fn = _ACTION_MAP[action]
    for code in codes:
        daily = daily_map.get(code, [])
        if not daily:
            results[code] = {"pass": False, "reason": "no_data"}
            continue
        try:
            results[code] = fn(daily, params)
        except Exception as e:
            logger.warning(f"kline_calc {action} failed for {code}: {e}")
            results[code] = {"pass": False, "reason": "calc_error"}
    return results

"""L1 量化计算引擎 — 从数据库直接计算技术/资金/估值/盈利规则标签

每个 compute_function 签名: (stock_code: str) -> dict | None
返回: {"matched": bool, "confidence": float, "evidence": str}
返回 None 表示数据不足无法计算
"""
import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


def _q(sql, params=None):
    from utils.db_utils import execute_query
    return execute_query(sql, params or [])


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def _get_daily(stock_code: str, days: int = 60):
    """获取最近 N 日行情数据，按日期升序"""
    return _q(
        "SELECT * FROM stock_daily WHERE stock_code=%s ORDER BY trade_date DESC LIMIT %s",
        [stock_code, days],
    ) or []


def _get_financial(stock_code: str, periods: int = 4):
    """获取最近 N 期财报"""
    return _q(
        "SELECT * FROM financial_reports WHERE stock_code=%s ORDER BY report_period DESC LIMIT %s",
        [stock_code, periods],
    ) or []


def _get_realtime(stock_code: str):
    rows = _q("SELECT * FROM stock_realtime WHERE stock_code=%s", [stock_code])
    return dict(rows[0]) if rows else None


def _get_capital(stock_code: str, days: int = 10):
    return _q(
        "SELECT * FROM capital_flow WHERE stock_code=%s ORDER BY trade_date DESC LIMIT %s",
        [stock_code, days],
    ) or []


def _get_stock_info(stock_code: str):
    rows = _q("SELECT * FROM stock_info WHERE stock_code=%s", [stock_code])
    return dict(rows[0]) if rows else None



# ── 技术形态规则 (11条) ────────────────────────────────────────────────────────

def compute_均线多头排列(stock_code: str):
    rows = _get_daily(stock_code, 65)
    if len(rows) < 60:
        return None
    closes = [r["close"] for r in reversed(rows)]
    def ma(n): return sum(closes[-n:]) / n
    ma5, ma10, ma20, ma60 = ma(5), ma(10), ma(20), ma(60)
    price = closes[-1]
    matched = price > ma5 > ma10 > ma20 > ma60
    spread_pct = (ma5 - ma60) / ma60 * 100 if ma60 else 0
    too_spread = spread_pct > 15
    if matched and too_spread:
        matched = False
    return {
        "matched": matched and not too_spread,
        "confidence": 0.9 if matched else 0.3,
        "evidence": f"价格={price:.2f} MA5={ma5:.2f} MA10={ma10:.2f} MA20={ma20:.2f} MA60={ma60:.2f} 发散={spread_pct:.1f}%",
    }


def compute_突破年线半年线(stock_code: str):
    rows = _get_daily(stock_code, 260)
    if len(rows) < 125:
        return None
    closes = [r["close"] for r in reversed(rows)]
    vols = [r["volume"] for r in reversed(rows)]
    price = closes[-1]
    prev_price = closes[-2] if len(closes) >= 2 else price
    ma120 = sum(closes[-120:]) / 120
    ma250 = sum(closes[-250:]) / 250 if len(closes) >= 250 else None
    avg_vol20 = sum(vols[-21:-1]) / 20 if len(vols) >= 21 else None
    cur_vol = vols[-1]
    # 突破：当前价在均线上方，前一日在均线下方
    broke_120 = prev_price < ma120 <= price
    broke_250 = ma250 and prev_price < ma250 <= price
    broke = broke_120 or broke_250
    vol_confirm = avg_vol20 and cur_vol >= avg_vol20 * 1.5
    matched = broke and vol_confirm
    return {
        "matched": bool(matched),
        "confidence": 0.85 if matched else 0.2,
        "evidence": f"价格={price:.2f} MA120={ma120:.2f}" + (f" MA250={ma250:.2f}" if ma250 else "") + f" 量比={cur_vol/avg_vol20:.1f}x" if avg_vol20 else "",
    }


def compute_底部放量长阳(stock_code: str):
    rows = _get_daily(stock_code, 65)
    if len(rows) < 25:
        return None
    rows_asc = list(reversed(rows))
    closes = [r["close"] for r in rows_asc]
    vols = [r["volume"] for r in rows_asc]
    lows = [r["low"] for r in rows_asc]
    change_pcts = [r["change_pct"] for r in rows_asc]
    min60 = min(lows[-60:]) if len(lows) >= 60 else min(lows)
    cur_low = lows[-1]
    cur_close = closes[-1]
    cur_change = change_pcts[-1] or 0
    avg_vol20 = sum(vols[-21:-1]) / 20 if len(vols) >= 21 else None
    cur_vol = vols[-1]
    near_bottom = (cur_low - min60) / min60 < 0.05 if min60 else False
    long_yang = cur_change >= 5
    vol_surge = avg_vol20 and cur_vol >= avg_vol20 * 3
    matched = near_bottom and long_yang and bool(vol_surge)
    return {
        "matched": matched,
        "confidence": 0.8 if matched else 0.2,
        "evidence": f"涨幅={cur_change:.1f}% 量比={cur_vol/avg_vol20:.1f}x 距底部={((cur_low-min60)/min60*100):.1f}%" if avg_vol20 else f"涨幅={cur_change:.1f}%",
    }


def compute_MACD底背离(stock_code: str):
    rows = _get_daily(stock_code, 60)
    if len(rows) < 35:
        return None
    closes = [r["close"] for r in reversed(rows)]
    # 计算 EMA12, EMA26, DIF, DEA
    def ema(data, n):
        k = 2 / (n + 1)
        result = [data[0]]
        for v in data[1:]:
            result.append(v * k + result[-1] * (1 - k))
        return result
    ema12 = ema(closes, 12)
    ema26 = ema(closes, 26)
    dif = [e12 - e26 for e12, e26 in zip(ema12, ema26)]
    dea = ema(dif, 9)
    macd_bar = [2 * (d - de) for d, de in zip(dif, dea)]
    # 检查底背离：价格新低但DIF未新低，且MACD柱由负转正
    price_low = min(closes[-10:])
    price_prev_low = min(closes[-20:-10]) if len(closes) >= 20 else closes[0]
    dif_low = min(dif[-10:])
    dif_prev_low = min(dif[-20:-10]) if len(dif) >= 20 else dif[0]
    price_new_low = price_low < price_prev_low
    dif_no_new_low = dif_low > dif_prev_low
    bar_turning = macd_bar[-1] > 0 and macd_bar[-2] <= 0
    matched = price_new_low and dif_no_new_low and bar_turning
    return {
        "matched": matched,
        "confidence": 0.75 if matched else 0.2,
        "evidence": f"价格新低={price_new_low} DIF未新低={dif_no_new_low} MACD翻红={bar_turning}",
    }


def compute_缩量回踩支撑位(stock_code: str):
    rows = _get_daily(stock_code, 65)
    if len(rows) < 25:
        return None
    rows_asc = list(reversed(rows))
    closes = [r["close"] for r in rows_asc]
    vols = [r["volume"] for r in rows_asc]
    change_pcts = [r["change_pct"] for r in rows_asc]
    ma20 = sum(closes[-20:]) / 20
    ma60 = sum(closes[-60:]) / 60 if len(closes) >= 60 else None
    price = closes[-1]
    # 近5日回踩
    recent_change = sum(change_pcts[-5:]) if len(change_pcts) >= 5 else 0
    pullback = -15 < recent_change < 0
    # 缩量
    peak_vol = max(vols[-20:-5]) if len(vols) >= 20 else None
    cur_vol = vols[-1]
    shrink = peak_vol and cur_vol <= peak_vol / 3
    # 在支撑位附近
    near_ma20 = abs(price - ma20) / ma20 < 0.02
    near_ma60 = ma60 and abs(price - ma60) / ma60 < 0.02
    near_support = near_ma20 or bool(near_ma60)
    matched = pullback and bool(shrink) and near_support
    return {
        "matched": matched,
        "confidence": 0.75 if matched else 0.2,
        "evidence": f"近5日涨跌={recent_change:.1f}% 缩量={shrink} 近MA20={near_ma20} 近MA60={near_ma60}",
    }


def compute_箱体突破(stock_code: str):
    rows = _get_daily(stock_code, 65)
    if len(rows) < 35:
        return None
    rows_asc = list(reversed(rows))
    closes = [r["close"] for r in rows_asc]
    highs = [r["high"] for r in rows_asc]
    vols = [r["volume"] for r in rows_asc]
    # 检查前30日是否横盘（振幅<15%）
    box_highs = highs[-31:-1]
    box_closes = closes[-31:-1]
    box_max = max(box_highs)
    box_min = min(box_closes)
    box_range = (box_max - box_min) / box_min if box_min else 1
    sideways = box_range < 0.15
    # 当前突破箱体上沿
    price = closes[-1]
    broke_out = price > box_max
    avg_vol20 = sum(vols[-21:-1]) / 20 if len(vols) >= 21 else None
    cur_vol = vols[-1]
    vol_confirm = avg_vol20 and cur_vol >= avg_vol20 * 1.5
    matched = sideways and broke_out and bool(vol_confirm)
    return {
        "matched": matched,
        "confidence": 0.8 if matched else 0.2,
        "evidence": f"箱体振幅={box_range*100:.1f}% 突破={broke_out} 量比={cur_vol/avg_vol20:.1f}x" if avg_vol20 else f"箱体振幅={box_range*100:.1f}%",
    }


def compute_周线级别趋势启动(stock_code: str):
    rows = _get_daily(stock_code, 130)
    if len(rows) < 30:
        return None
    rows_asc = list(reversed(rows))
    closes = [r["close"] for r in rows_asc]
    vols = [r["volume"] for r in rows_asc]
    # 按周聚合（每5日一周）
    def weekly(data):
        return [sum(data[i:i+5])/5 for i in range(0, len(data)-4, 5)]
    w_closes = [sum(closes[i:i+5])/5 for i in range(0, len(closes)-4, 5)]
    w_vols = [sum(vols[i:i+5]) for i in range(0, len(vols)-4, 5)]
    if len(w_closes) < 10:
        return None
    # 周线MACD
    def ema(data, n):
        k = 2 / (n + 1)
        result = [data[0]]
        for v in data[1:]:
            result.append(v * k + result[-1] * (1 - k))
        return result
    ema12 = ema(w_closes, 12)
    ema26 = ema(w_closes, 26)
    dif = [e12 - e26 for e12, e26 in zip(ema12, ema26)]
    dea = ema(dif, 9)
    # 金叉：DIF上穿DEA
    golden_cross = dif[-1] > dea[-1] and dif[-2] <= dea[-2]
    near_zero = abs(dif[-1]) < abs(w_closes[-1]) * 0.02
    # 放量
    avg_vol4 = sum(w_vols[-5:-1]) / 4 if len(w_vols) >= 5 else None
    cur_wvol = w_vols[-1]
    vol_up = avg_vol4 and cur_wvol >= avg_vol4 * 1.5
    # 周K线涨幅
    week_change = (w_closes[-1] - w_closes[-2]) / w_closes[-2] * 100 if w_closes[-2] else 0
    matched = golden_cross and bool(vol_up) and week_change >= 3
    return {
        "matched": matched,
        "confidence": 0.8 if matched else 0.2,
        "evidence": f"周线金叉={golden_cross} 近零轴={near_zero} 周涨幅={week_change:.1f}%",
    }


def compute_月线三连阳(stock_code: str):
    rows = _get_daily(stock_code, 130)
    if len(rows) < 60:
        return None
    rows_asc = list(reversed(rows))
    # 按月聚合（每20日一月）
    months = []
    for i in range(0, len(rows_asc) - 19, 20):
        chunk = rows_asc[i:i+20]
        open_ = chunk[0]["open"]
        close = chunk[-1]["close"]
        vols = [r["volume"] for r in chunk]
        months.append({"open": open_, "close": close, "vol": sum(vols)})
    if len(months) < 3:
        return None
    last3 = months[-3:]
    all_yang = all(m["close"] > m["open"] for m in last3)
    all_up2 = all((m["close"] - m["open"]) / m["open"] * 100 >= 2 for m in last3)
    total_gain = (last3[-1]["close"] - last3[0]["open"]) / last3[0]["open"] * 100 if last3[0]["open"] else 0
    too_high = total_gain > 50
    vol_shrink = last3[-1]["vol"] < last3[-2]["vol"] * 0.7
    matched = all_yang and all_up2 and not too_high and not vol_shrink
    return {
        "matched": matched,
        "confidence": 0.85 if matched else 0.2,
        "evidence": f"三月均阳={all_yang} 均涨≥2%={all_up2} 累计涨幅={total_gain:.1f}% 量缩={vol_shrink}",
    }


def compute_周线三连阳(stock_code: str):
    rows = _get_daily(stock_code, 30)
    if len(rows) < 15:
        return None
    rows_asc = list(reversed(rows))
    weeks = []
    for i in range(0, len(rows_asc) - 4, 5):
        chunk = rows_asc[i:i+5]
        open_ = chunk[0]["open"]
        close = chunk[-1]["close"]
        weeks.append({"open": open_, "close": close})
    if len(weeks) < 3:
        return None
    last3 = weeks[-3:]
    all_yang = all(w["close"] > w["open"] for w in last3)
    all_up1 = all((w["close"] - w["open"]) / w["open"] * 100 >= 1 for w in last3)
    # 检查动能衰竭
    gains = [(w["close"] - w["open"]) / w["open"] * 100 for w in last3]
    declining = gains[0] > gains[1] > gains[2]
    matched = all_yang and all_up1 and not declining
    return {
        "matched": matched,
        "confidence": 0.8 if matched else 0.2,
        "evidence": f"三周均阳={all_yang} 均涨≥1%={all_up1} 动能衰竭={declining}",
    }


def compute_日线五连阳(stock_code: str):
    rows = _get_daily(stock_code, 10)
    if len(rows) < 5:
        return None
    rows_asc = list(reversed(rows))
    last5 = rows_asc[-5:]
    all_yang = all(r["close"] > r["open"] for r in last5)
    change_pcts = [r["change_pct"] or 0 for r in last5]
    total_gain = sum(change_pcts)
    no_big_drop = all(c > -1 for c in change_pcts)
    has_limit_up = any(c >= 9.9 for c in change_pcts)
    too_high = total_gain > 20
    matched = all_yang and total_gain >= 8 and no_big_drop and not has_limit_up and not too_high
    return {
        "matched": matched,
        "confidence": 0.8 if matched else 0.2,
        "evidence": f"五连阳={all_yang} 累计涨幅={total_gain:.1f}% 有涨停={has_limit_up}",
    }


def compute_MACD动能转强(stock_code: str):
    rows = _get_daily(stock_code, 40)
    if len(rows) < 30:
        return None
    closes = [r["close"] for r in reversed(rows)]
    def ema(data, n):
        k = 2 / (n + 1)
        result = [data[0]]
        for v in data[1:]:
            result.append(v * k + result[-1] * (1 - k))
        return result
    ema12 = ema(closes, 12)
    ema26 = ema(closes, 26)
    dif = [e12 - e26 for e12, e26 in zip(ema12, ema26)]
    dea = ema(dif, 9)
    macd_bar = [2 * (d - de) for d, de in zip(dif, dea)]
    last3 = macd_bar[-3:]
    # 红柱连续放大
    red_expanding = all(b > 0 for b in last3) and last3[0] < last3[1] < last3[2]
    # 绿柱连续缩短
    green_shrinking = all(b < 0 for b in last3) and last3[0] < last3[1] < last3[2]
    # 排除零轴远下方的绿柱缩短
    near_zero = abs(dif[-1]) < abs(closes[-1]) * 0.03
    matched = red_expanding or (green_shrinking and near_zero)
    return {
        "matched": matched,
        "confidence": 0.75 if matched else 0.2,
        "evidence": f"红柱放大={red_expanding} 绿柱缩短={green_shrinking} 近零轴={near_zero}",
    }



# ── 资金面规则 (部分可算) ──────────────────────────────────────────────────────

def compute_主力资金连续净流入(stock_code: str):
    rows = _get_capital(stock_code, 7)
    if len(rows) < 5:
        return None
    rows_asc = list(reversed(rows))
    last5 = rows_asc[-5:]
    all_positive = all((r["main_net_inflow"] or 0) > 0 for r in last5)
    total_inflow = sum((r["main_net_inflow"] or 0) for r in last5)
    # 排除同期股价已涨>10%
    daily = _get_daily(stock_code, 7)
    if len(daily) >= 5:
        daily_asc = list(reversed(daily))
        price_change = sum((r["change_pct"] or 0) for r in daily_asc[-5:])
        if price_change > 10:
            return {"matched": False, "confidence": 0.3, "evidence": f"股价已涨{price_change:.1f}%，可能是对倒"}
    matched = all_positive and total_inflow >= 5000  # 万元
    return {
        "matched": matched,
        "confidence": 0.85 if matched else 0.3,
        "evidence": f"5日连续净流入={all_positive} 累计净流入={total_inflow:.0f}万",
    }


def compute_量价齐升突破(stock_code: str):
    rows = _get_daily(stock_code, 65)
    if len(rows) < 25:
        return None
    rows_asc = list(reversed(rows))
    closes = [r["close"] for r in rows_asc]
    vols = [r["volume"] for r in rows_asc]
    highs = [r["high"] for r in rows_asc]
    turnover = [r["turnover_rate"] or 0 for r in rows_asc]
    avg_vol20 = sum(vols[-21:-1]) / 20 if len(vols) >= 21 else None
    cur_vol = vols[-1]
    vol_surge = avg_vol20 and cur_vol >= avg_vol20 * 2
    high60 = max(highs[-61:-1]) if len(highs) >= 61 else max(highs[:-1])
    price = closes[-1]
    broke_high = price > high60
    # 排除换手率>20%（游资一日游）
    cur_turnover = turnover[-1]
    too_hot = cur_turnover > 20
    matched = bool(vol_surge) and broke_high and not too_hot
    return {
        "matched": matched,
        "confidence": 0.85 if matched else 0.2,
        "evidence": f"量比={cur_vol/avg_vol20:.1f}x 突破60日高={broke_high} 换手率={cur_turnover:.1f}%" if avg_vol20 else "",
    }


# ── 盈利质量规则 (数据充足的部分) ─────────────────────────────────────────────

def compute_ROE持续优秀(stock_code: str):
    rows = _get_financial(stock_code, 4)
    if len(rows) < 3:
        return None
    roes = [r["roe"] for r in rows if r["roe"] is not None]
    if len(roes) < 3:
        return None
    all_high = all(r >= 15 for r in roes[:3])
    avg_roe = sum(roes[:3]) / 3
    std_roe = (sum((r - avg_roe) ** 2 for r in roes[:3]) / 3) ** 0.5
    matched = all_high
    return {
        "matched": matched,
        "confidence": 0.9 if matched else 0.3,
        "evidence": f"近3期ROE={[f'{r:.1f}%' for r in roes[:3]]} 均值={avg_roe:.1f}% 标准差={std_roe:.1f}",
    }


def compute_营收加速增长(stock_code: str):
    rows = _get_financial(stock_code, 3)
    if len(rows) < 2:
        return None
    latest = rows[0]
    prev = rows[1]
    cur_yoy = latest.get("revenue_yoy")
    prev_yoy = prev.get("revenue_yoy")
    if cur_yoy is None or prev_yoy is None:
        return None
    accelerating = cur_yoy > prev_yoy
    high_growth = cur_yoy >= 20
    matched = accelerating and high_growth
    return {
        "matched": matched,
        "confidence": 0.85 if matched else 0.3,
        "evidence": f"本期营收增速={cur_yoy:.1f}% 上期={prev_yoy:.1f}% 加速={accelerating}",
    }


def compute_净利润加速增长(stock_code: str):
    rows = _get_financial(stock_code, 3)
    if len(rows) < 2:
        return None
    latest = rows[0]
    prev = rows[1]
    cur_yoy = latest.get("profit_yoy")
    prev_yoy = prev.get("profit_yoy")
    if cur_yoy is None or prev_yoy is None:
        return None
    accelerating = cur_yoy > prev_yoy
    high_growth = cur_yoy >= 30
    matched = accelerating and high_growth
    return {
        "matched": matched,
        "confidence": 0.85 if matched else 0.3,
        "evidence": f"本期净利润增速={cur_yoy:.1f}% 上期={prev_yoy:.1f}% 加速={accelerating}",
    }


# ── 估值规则 (数据充足的部分) ─────────────────────────────────────────────────

def compute_PB破净且ROE大于8(stock_code: str):
    rt = _get_realtime(stock_code)
    fin = _get_financial(stock_code, 2)
    if not rt:
        return None
    pb = rt.get("pb_ratio")
    if pb is None:
        return None
    roe = fin[0].get("roe") if fin else None
    if roe is None:
        return None
    pb_below1 = pb < 1
    roe_ok = roe > 8
    # 排除ROE持续下滑
    if len(fin) >= 2 and fin[1].get("roe") is not None:
        roe_declining = roe < fin[1]["roe"] * 0.9
    else:
        roe_declining = False
    matched = pb_below1 and roe_ok and not roe_declining
    return {
        "matched": matched,
        "confidence": 0.85 if matched else 0.3,
        "evidence": f"PB={pb:.2f} ROE={roe:.1f}% ROE下滑={roe_declining}",
    }


def compute_同行PE最低档(stock_code: str):
    rt = _get_realtime(stock_code)
    info = _get_stock_info(stock_code)
    if not rt or not info:
        return None
    pe = rt.get("pe_ratio")
    industry = info.get("industry_l1")
    if pe is None or not industry or pe <= 0:
        return None
    # 获取同行PE分布
    peers = _q(
        """SELECT sr.pe_ratio FROM stock_realtime sr
           JOIN stock_info si ON sr.stock_code = si.stock_code
           WHERE si.industry_l1=%s AND sr.pe_ratio > 0 AND sr.pe_ratio < 500""",
        [industry],
    ) or []
    if len(peers) < 5:
        return None
    peer_pes = sorted([r["pe_ratio"] for r in peers])
    p20 = peer_pes[int(len(peer_pes) * 0.2)]
    matched = pe <= p20
    return {
        "matched": matched,
        "confidence": 0.8 if matched else 0.3,
        "evidence": f"PE={pe:.1f} 行业20%分位={p20:.1f} 行业样本={len(peer_pes)}家",
    }


# ── 风险收益规则 (部分可算) ───────────────────────────────────────────────────

def compute_高股息低波动(stock_code: str):
    rt = _get_realtime(stock_code)
    if not rt:
        return None
    # 需要 dividend_yield 字段，暂时标记数据缺失
    return None  # data_missing: 需要 dividend_yield 字段


def compute_低负债率财务稳健(stock_code: str):
    # 需要 debt_ratio 字段，暂时标记数据缺失
    return None  # data_missing: 需要 debt_ratio 字段


def compute_安全边际破净加盈利(stock_code: str):
    rt = _get_realtime(stock_code)
    fin = _get_financial(stock_code, 2)
    if not rt or not fin:
        return None
    pb = rt.get("pb_ratio")
    roe = fin[0].get("roe") if fin else None
    if pb is None or roe is None:
        return None
    matched = pb < 1 and roe > 5
    return {
        "matched": matched,
        "confidence": 0.8 if matched else 0.3,
        "evidence": f"PB={pb:.2f} ROE={roe:.1f}%",
    }


def compute_Beta防御属性(stock_code: str):
    # 需要计算 Beta，暂时标记数据缺失
    return None  # data_missing: 需要历史收益率计算 Beta


def compute_最大回撤小抗跌能力强(stock_code: str):
    rows = _get_daily(stock_code, 250)
    if len(rows) < 60:
        return None
    rows_asc = list(reversed(rows))
    closes = [r["close"] for r in rows_asc]
    # 计算最大回撤
    peak = closes[0]
    max_dd = 0
    for c in closes:
        if c > peak:
            peak = c
        dd = (peak - c) / peak if peak else 0
        if dd > max_dd:
            max_dd = dd
    # 计算年涨幅
    year_change = (closes[-1] - closes[0]) / closes[0] * 100 if closes[0] else 0
    matched = max_dd < 0.15 and year_change >= 0
    return {
        "matched": matched,
        "confidence": 0.8 if matched else 0.3,
        "evidence": f"最大回撤={max_dd*100:.1f}% 年涨幅={year_change:.1f}%",
    }


def compute_现金流充裕零有息负债(stock_code: str):
    # 需要 total_debt 和 operating_cash_flow 字段
    return None  # data_missing


# ── 增量数据 helpers ───────────────────────────────────────────────────────────

def _ensure_extra(stock_code: str):
    """确保本地有该股票的增量数据，首次调用时会从云端同步。"""
    from utils.db_utils import ensure_stock_extra_data
    ensure_stock_extra_data(stock_code)


def _get_valuation_history(stock_code: str, days: int = 365) -> list:
    _ensure_extra(stock_code)
    return _q(
        "SELECT * FROM valuation_history WHERE stock_code=%s "
        "AND trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY) "
        "ORDER BY trade_date DESC",
        [stock_code, days],
    ) or []


def _get_shareholder_count(stock_code: str, periods: int = 8) -> list:
    _ensure_extra(stock_code)
    return _q(
        "SELECT * FROM shareholder_count WHERE stock_code=%s "
        "ORDER BY end_date DESC LIMIT %s",
        [stock_code, periods],
    ) or []


def _get_insider_trading(stock_code: str, days: int = 180) -> list:
    _ensure_extra(stock_code)
    return _q(
        "SELECT * FROM insider_trading WHERE stock_code=%s "
        "AND trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY) "
        "ORDER BY trade_date DESC",
        [stock_code, days],
    ) or []


def _get_institutional_holding(stock_code: str, periods: int = 4) -> list:
    _ensure_extra(stock_code)
    return _q(
        "SELECT * FROM institutional_holding WHERE stock_code=%s "
        "ORDER BY report_date DESC LIMIT %s",
        [stock_code, periods],
    ) or []


def _get_margin_trading(stock_code: str, days: int = 90) -> list:
    _ensure_extra(stock_code)
    return _q(
        "SELECT * FROM margin_trading WHERE stock_code=%s "
        "AND trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY) "
        "ORDER BY trade_date DESC",
        [stock_code, days],
    ) or []


def _get_etf_membership(stock_code: str) -> list:
    _ensure_extra(stock_code)
    return _q(
        "SELECT * FROM etf_constituent WHERE stock_code=%s ORDER BY report_date DESC",
        [stock_code],
    ) or []


# ── 新增规则：增量数据驱动 ────────────────────────────────────────────────────

def compute_估值历史分位低于20(stock_code: str):
    """当前PE在近3年序列中的分位数 < 0.2"""
    rows = _get_valuation_history(stock_code, days=1095)  # ~3年
    if len(rows) < 30:
        return None
    current_pe = rows[0].get("pe_ttm")
    if current_pe is None or current_pe <= 0:
        return None
    pe_series = [r["pe_ttm"] for r in rows if r.get("pe_ttm") and r["pe_ttm"] > 0]
    if len(pe_series) < 30:
        return None
    below = sum(1 for p in pe_series if p >= current_pe)
    quantile = below / len(pe_series)
    matched = quantile < 0.2
    return {
        "matched": matched,
        "confidence": 0.85 if matched else 0.3,
        "evidence": f"当前PE={current_pe:.1f} 近3年分位={quantile:.2%} 样本={len(pe_series)}条",
    }


def compute_融资余额持续增加(stock_code: str):
    """最近5期 margin_balance 环比正增长"""
    rows = _get_margin_trading(stock_code, days=90)
    if len(rows) < 5:
        return None
    # 按日期升序取最近5条
    recent = sorted(rows[:5], key=lambda r: str(r.get("trade_date", "")))
    balances = [r.get("margin_balance") for r in recent if r.get("margin_balance") is not None]
    if len(balances) < 5:
        return None
    # 环比全部正增长
    consecutive_growth = all(balances[i] > balances[i - 1] for i in range(1, len(balances)))
    total_chg_pct = (balances[-1] - balances[0]) / balances[0] * 100 if balances[0] else 0
    matched = consecutive_growth and total_chg_pct > 0
    return {
        "matched": matched,
        "confidence": 0.8 if matched else 0.3,
        "evidence": (
            f"融资余额={[f'{b/1e8:.2f}亿' for b in balances]} "
            f"连续增长={consecutive_growth} 累计涨幅={total_chg_pct:.1f}%"
        ),
    }


def compute_筹码集中股东户数下降(stock_code: str):
    """最近2期股东户数下降且 change_pct < -5%"""
    rows = _get_shareholder_count(stock_code, periods=4)
    if len(rows) < 2:
        return None
    latest = rows[0]
    prev = rows[1]
    latest_count = latest.get("holder_count")
    prev_count = prev.get("holder_count")
    change_pct = latest.get("change_pct")
    if latest_count is None or prev_count is None:
        return None
    count_down = latest_count < prev_count
    pct_ok = (change_pct is not None and change_pct < -5) or (
        prev_count and (latest_count - prev_count) / prev_count * 100 < -5
    )
    matched = count_down and pct_ok
    actual_pct = (latest_count - prev_count) / prev_count * 100 if prev_count else 0
    return {
        "matched": matched,
        "confidence": 0.8 if matched else 0.3,
        "evidence": (
            f"最新股东数={latest_count:,} 上期={prev_count:,} "
            f"变化={actual_pct:.1f}% 报告期={latest.get('end_date')}"
        ),
    }


def compute_ETF资金间接流入(stock_code: str):
    """股票在 ≥2 只规模>10亿 ETF 中"""
    rows = _get_etf_membership(stock_code)
    if not rows:
        return None
    # 取最新一期各ETF数据（每只ETF取最新日期）
    etf_latest = {}
    for r in rows:
        etf_code = r.get("etf_code")
        rd = str(r.get("report_date", ""))
        if etf_code not in etf_latest or rd > str(etf_latest[etf_code].get("report_date", "")):
            etf_latest[etf_code] = r
    if not etf_latest:
        return None
    # 筛选 amount > 10亿（amount 单位：元）
    large_etfs = [r for r in etf_latest.values() if r.get("amount") and r["amount"] >= 1e9]
    matched = len(large_etfs) >= 2
    etf_names = [r.get("etf_name", r.get("etf_code")) for r in large_etfs[:5]]
    return {
        "matched": matched,
        "confidence": 0.75 if matched else 0.3,
        "evidence": (
            f"所在大ETF数={len(large_etfs)} "
            f"ETF列表={etf_names}"
        ),
    }


def compute_高股息低波动(stock_code: str):
    """dividend_yield > 2.5% 且年化波动率低于历史均值"""
    vh_rows = _get_valuation_history(stock_code, days=365)
    if not vh_rows:
        return None
    current_dy = vh_rows[0].get("dividend_yield")
    if current_dy is None:
        return None
    dy_ok = current_dy > 2.5

    # 波动率：用本地 stock_daily 计算近60日日收益率标准差（年化）
    price_rows = _q(
        "SELECT close FROM stock_daily WHERE stock_code=%s ORDER BY trade_date DESC LIMIT 61",
        [stock_code],
    ) or []
    if len(price_rows) < 20:
        vol_ok = True  # 数据不足时放宽
        vol_str = "数据不足"
    else:
        closes = [r["close"] for r in reversed(price_rows)]
        returns = [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes)) if closes[i - 1]]
        import math
        vol = math.sqrt(sum(r ** 2 for r in returns) / len(returns)) * math.sqrt(252) * 100
        vol_ok = vol < 30  # 年化波动率 < 30% 视为低波动
        vol_str = f"{vol:.1f}%"

    matched = dy_ok and vol_ok
    return {
        "matched": matched,
        "confidence": 0.8 if matched else 0.3,
        "evidence": f"股息率={current_dy:.2f}% 年化波动率={vol_str}",
    }


# ── 规则映射表 ────────────────────────────────────────────────────────────────

RULE_COMPUTERS = {
    # 技术形态 (11条)
    "均线多头排列": compute_均线多头排列,
    "突破年线/半年线": compute_突破年线半年线,
    "底部放量长阳": compute_底部放量长阳,
    "MACD底背离": compute_MACD底背离,
    "缩量回踩支撑位": compute_缩量回踩支撑位,
    "箱体突破": compute_箱体突破,
    "周线级别趋势启动": compute_周线级别趋势启动,
    "月线三连阳": compute_月线三连阳,
    "周线三连阳": compute_周线三连阳,
    "日线五连阳": compute_日线五连阳,
    "MACD动能转强": compute_MACD动能转强,
    # 资金面 (部分)
    "主力资金连续净流入": compute_主力资金连续净流入,
    "量价齐升突破": compute_量价齐升突破,
    # 盈利质量 (部分)
    "ROE 持续优秀": compute_ROE持续优秀,
    "营收加速增长": compute_营收加速增长,
    "净利润加速增长": compute_净利润加速增长,
    # 估值 (部分)
    "PB破净且ROE>8%": compute_PB破净且ROE大于8,
    "同行PE最低档": compute_同行PE最低档,
    # 风险收益 (部分)
    "安全边际·破净+盈利": compute_安全边际破净加盈利,
    "最大回撤小·抗跌能力强": compute_最大回撤小抗跌能力强,
    # 增量数据规则（valuation_history / margin_trading / shareholder_count / etf_constituent）
    "估值历史分位<20%": compute_估值历史分位低于20,
    "融资余额持续增加": compute_融资余额持续增加,
    "筹码集中·股东户数下降": compute_筹码集中股东户数下降,
    "ETF资金间接流入": compute_ETF资金间接流入,
    "高股息低波动": compute_高股息低波动,
}

# 数据缺失的规则（等待字段补全后自动生效）
DATA_MISSING_RULES = {
    "毛利率行业领先": "需要 gross_margin 字段",
    "经营现金流/净利润 > 1": "需要 operating_cash_flow 字段",
    "净利率持续提升": "需要 net_margin 字段",
    "ROIC > WACC": "需要 ROIC 计算字段",
    "扣非净利润占比高": "需要 deducted_net_profit 字段",
    "费用率持续下降": "需要 total_expense_ratio 字段",
    "应收账款周转加速": "需要 accounts_receivable_turnover 字段",
    "自由现金流持续为正": "需要 free_cash_flow 字段",
    "存货周转效率提升": "需要 inventory_turnover 字段",
    "订单/合同负债高增长": "需要 contract_liabilities 字段",
    "产能释放拐点": "需要 construction_in_progress/fixed_assets 字段",
    "研发投入高增长": "需要 r_and_d_expense 字段",
    "海外收入占比快速提升": "需要海外收入分部数据",
    "PEG < 1 的高增长": "需要分析师一致预期数据",
    "员工人数快速扩张": "需要 employee_count 字段",
    "PS历史底部": "需要历史PS分位数据",
    "EV/EBITDA行业最低": "需要 EBITDA 和有息负债数据",
    "股息率>国债收益率": "需要 dividend_yield 字段",
    "北向资金持续加仓": "需要个股北向持仓数据",
    "大宗交易溢价成交": "需要大宗交易数据",
    "游资席位连续买入": "需要龙虎榜数据",
    "股票回购进行中": "需要回购公告数据",
    "低负债率·财务稳健": "需要 debt_ratio 字段",
    "Beta<0.8·防御属性": "需要历史收益率计算 Beta",
    "现金流充裕·零有息负债": "需要 total_debt 和 operating_cash_flow 字段",
}


# ── 主入口 ────────────────────────────────────────────────────────────────────

def run_l1_for_stock(stock_code: str) -> dict:
    """对单只股票跑所有 L1 规则，返回结果字典"""
    # 获取规则 ID 映射
    from utils.db_utils import execute_query, execute_insert
    rules = execute_query(
        "SELECT id, rule_name, category FROM stock_selection_rules WHERE layer=1 AND is_active=1"
    ) or []
    rule_id_map = {r["rule_name"]: {"id": r["id"], "category": r["category"]} for r in rules}

    results = {"computed": [], "skipped": [], "errors": []}

    for rule_name, compute_fn in RULE_COMPUTERS.items():
        rule_info = rule_id_map.get(rule_name)
        if not rule_info:
            continue
        rule_id = rule_info["id"]
        category = rule_info["category"]
        try:
            result = compute_fn(stock_code)
            if result is None:
                results["skipped"].append({"rule_name": rule_name, "reason": "数据不足"})
                continue
            execute_insert(
                """INSERT INTO stock_rule_tags
                   (stock_code, rule_id, rule_category, rule_name, matched, confidence, evidence, layer, computed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, 1, NOW())
                   ON DUPLICATE KEY UPDATE
                     matched=VALUES(matched), confidence=VALUES(confidence),
                     evidence=VALUES(evidence), computed_at=NOW()""",
                [stock_code, rule_id, category, rule_name,
                 1 if result["matched"] else 0,
                 result["confidence"], result["evidence"]],
            )
            results["computed"].append({
                "rule_name": rule_name,
                "matched": result["matched"],
                "confidence": result["confidence"],
                "evidence": result["evidence"],
            })
        except Exception as e:
            logger.warning(f"L1 compute error {stock_code}/{rule_name}: {e}")
            results["errors"].append({"rule_name": rule_name, "error": str(e)[:100]})

    return results


def run_l1_batch(stock_codes=None) -> dict:
    """批量跑 L1，默认跑 watchlist + portfolio"""
    from utils.db_utils import execute_query
    if stock_codes is None:
        wl = execute_query("SELECT stock_code FROM watchlist") or []
        hp = execute_query("SELECT DISTINCT stock_code FROM holding_positions WHERE status='open'") or []
        codes = list({r["stock_code"] for r in wl + hp})
    else:
        codes = list(stock_codes)

    summary = {"total": len(codes), "done": 0, "errors": 0, "details": {}}
    for code in codes:
        try:
            r = run_l1_for_stock(code)
            summary["done"] += 1
            summary["details"][code] = {
                "computed": len(r["computed"]),
                "matched": sum(1 for x in r["computed"] if x["matched"]),
                "skipped": len(r["skipped"]),
            }
        except Exception as e:
            summary["errors"] += 1
            logger.error(f"L1 batch error {code}: {e}")
    return summary

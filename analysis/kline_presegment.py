"""第一层：候选切割点生成 — 纯规则，零AI"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class CandidateSplit:
    date: str
    index: int          # 在 indicators["dates"] 中的索引
    reasons: list       # 触发原因列表
    snapshot: dict = field(default_factory=dict)  # 7维度快照


def _sign(v) -> int:
    if v is None:
        return 0
    return 1 if v > 0 else (-1 if v < 0 else 0)


def find_candidate_splits(indicators: dict) -> list:
    """
    基于硬指标交叉点生成候选分割边界。
    返回 CandidateSplit 列表（已去重、按日期排序）。
    """
    dates = indicators.get("dates", [])
    n = len(dates)
    if n < 10:
        return []

    closes      = [r["close"] for r in indicators["ohlcv"]]
    ma5         = indicators.get("ma5", [])
    ma20        = indicators.get("ma20", [])
    macd_hist   = indicators.get("macd_hist", [])
    rsi14       = indicators.get("rsi14", [])
    vol_ratio   = indicators.get("volume_ratio", [])
    boll_upper  = indicators.get("boll_upper", [])
    boll_lower  = indicators.get("boll_lower", [])
    cap_map     = indicators.get("cap_map", {})

    split_map: dict[int, list] = {}  # index → reasons

    def add(i, reason):
        split_map.setdefault(i, []).append(reason)

    for i in range(1, n):
        # 1. MA5×MA20 金叉/死叉
        if (i < len(ma5) and i < len(ma20) and
                ma5[i] is not None and ma20[i] is not None and
                ma5[i - 1] is not None and ma20[i - 1] is not None):
            if ma5[i - 1] < ma20[i - 1] and ma5[i] >= ma20[i]:
                add(i, "MA5×MA20金叉")
            elif ma5[i - 1] > ma20[i - 1] and ma5[i] <= ma20[i]:
                add(i, "MA5×MA20死叉")

        # 2. MACD柱变号
        if (i < len(macd_hist) and macd_hist[i] is not None and macd_hist[i - 1] is not None):
            if _sign(macd_hist[i - 1]) < 0 and _sign(macd_hist[i]) > 0:
                add(i, "MACD柱由负转正")
            elif _sign(macd_hist[i - 1]) > 0 and _sign(macd_hist[i]) < 0:
                add(i, "MACD柱由正转负")

        # 3. 成交量突变（>2倍均量 或 <0.3倍均量）
        if i < len(vol_ratio) and vol_ratio[i] is not None:
            if vol_ratio[i] > 2.0:
                add(i, f"成交量突增({vol_ratio[i]:.1f}x)")
            elif vol_ratio[i] < 0.3:
                add(i, f"成交量极度萎缩({vol_ratio[i]:.2f}x)")

        # 4. RSI穿越关键位 (30/50/70)
        if i < len(rsi14) and rsi14[i] is not None and rsi14[i - 1] is not None:
            for level in [30, 50, 70]:
                if rsi14[i - 1] < level <= rsi14[i]:
                    add(i, f"RSI上穿{level}")
                elif rsi14[i - 1] > level >= rsi14[i]:
                    add(i, f"RSI下穿{level}")

        # 5. 价格突破/跌破BOLL上下轨
        c, cp = closes[i], closes[i - 1]
        bu  = boll_upper[i]  if i < len(boll_upper) else None
        bl  = boll_lower[i]  if i < len(boll_lower) else None
        bup = boll_upper[i-1] if i-1 < len(boll_upper) else None
        blp = boll_lower[i-1] if i-1 < len(boll_lower) else None
        if bu is not None and bup is not None:
            if cp < bup and c >= bu:
                add(i, "价格突破BOLL上轨")
            elif cp > bup and c < bu:
                add(i, "价格跌破BOLL上轨")
        if bl is not None and blp is not None:
            if cp > blp and c <= bl:
                add(i, "价格跌破BOLL下轨")
            elif cp < blp and c > bl:
                add(i, "价格突破BOLL下轨")

        # 6. 资金流方向反转（连续3天净流入→净流出 或反之）
        if i >= 3:
            flow_window = []
            for j in range(i - 2, i + 1):
                d = dates[j]
                cap = cap_map.get(d) or cap_map.get(str(d))
                if cap:
                    flow_window.append(float(cap.get("main_net_inflow") or 0))
            if len(flow_window) == 3:
                if all(v > 0 for v in flow_window[:2]) and flow_window[2] < 0:
                    add(i, "主力资金由流入转流出")
                elif all(v < 0 for v in flow_window[:2]) and flow_window[2] > 0:
                    add(i, "主力资金由流出转流入")

    # 构建 CandidateSplit 列表
    splits = []
    for idx, reasons in sorted(split_map.items()):
        snap = _build_snapshot(idx, indicators)
        splits.append(CandidateSplit(
            date=str(dates[idx]),
            index=idx,
            reasons=reasons,
            snapshot=snap,
        ))

    # 合并相邻3天内的切割点（取最多原因的那个）
    splits = _merge_nearby(splits, window=3)
    return splits


def _build_snapshot(i: int, indicators: dict) -> dict:
    """构建某个时间点的7维度快照"""
    dates   = indicators["dates"]
    ohlcv   = indicators["ohlcv"]
    ma20    = indicators.get("ma20", [])
    boll_upper = indicators.get("boll_upper", [])
    boll_lower = indicators.get("boll_lower", [])
    rsi14   = indicators.get("rsi14", [])
    macd_hist = indicators.get("macd_hist", [])
    profit_ratio = indicators.get("profit_ratio", [])
    cap_map = indicators.get("cap_map", {})
    vol_ratio = indicators.get("volume_ratio", [])

    close = float(ohlcv[i]["close"] or 0)
    m20 = ma20[i] if i < len(ma20) else None
    bu  = boll_upper[i] if i < len(boll_upper) else None
    bl  = boll_lower[i] if i < len(boll_lower) else None

    # 价格相对MA20百分比
    price_vs_ma20 = round((close - m20) / m20 * 100, 2) if m20 else None

    # 价格在BOLL中的位置 (0=下轨, 1=上轨)
    boll_pos = None
    if bu is not None and bl is not None and bu != bl:
        boll_pos = round((close - bl) / (bu - bl), 3)

    # 资金流（近5日累计）
    cap_5d = 0.0
    for j in range(max(0, i - 4), i + 1):
        d = dates[j]
        cap = cap_map.get(d) or cap_map.get(str(d))
        if cap:
            cap_5d += float(cap.get("main_net_inflow") or 0)

    # MA排列（MA5>MA10>MA20=多头, 反之=空头）
    _ma5  = indicators.get("ma5", [])
    _ma10 = indicators.get("ma10", [])
    ma5_v  = _ma5[i]  if i < len(_ma5)  else None
    ma10_v = _ma10[i] if i < len(_ma10) else None
    if ma5_v and ma10_v and m20:
        if ma5_v > ma10_v > m20:
            ma_arr = "多头"
        elif ma5_v < ma10_v < m20:
            ma_arr = "空头"
        else:
            ma_arr = "混乱"
    else:
        ma_arr = None

    # MACD状态
    hist_v = macd_hist[i] if macd_hist and i < len(macd_hist) else None
    macd_state = "正" if hist_v and hist_v > 0 else ("负" if hist_v and hist_v < 0 else "零")

    _rsi14 = rsi14[i] if rsi14 and i < len(rsi14) else None
    _pr    = profit_ratio[i] if profit_ratio and i < len(profit_ratio) else None
    _vr    = vol_ratio[i] if vol_ratio and i < len(vol_ratio) else None

    return {
        "date":             str(dates[i]),
        "close":            close,
        "price_vs_ma20_pct": price_vs_ma20,
        "boll_position":    boll_pos,
        "rsi":              round(_rsi14, 1) if _rsi14 is not None else None,
        "profit_ratio":     _pr,
        "capital_flow_5d":  round(cap_5d / 1e8, 2),  # 亿元
        "ma_arrangement":   ma_arr,
        "macd_state":       macd_state,
        "volume_ratio":     round(_vr, 2) if _vr is not None else None,
    }


def _merge_nearby(splits: list, window: int = 3) -> list:
    """合并相邻 window 天内的切割点，保留原因最多的"""
    if not splits:
        return splits
    merged = [splits[0]]
    for sp in splits[1:]:
        last = merged[-1]
        # 比较日期差（简单字符串比较，格式YYYY-MM-DD）
        try:
            from datetime import date
            d1 = date.fromisoformat(str(last.date))
            d2 = date.fromisoformat(str(sp.date))
            diff = (d2 - d1).days
        except Exception:
            diff = window + 1
        if diff <= window:
            # 保留原因更多的
            if len(sp.reasons) > len(last.reasons):
                merged[-1] = sp
        else:
            merged.append(sp)
    return merged


def compute_segment_summaries(splits: list, indicators: dict) -> list:
    """
    计算每个候选区间（相邻切割点之间）的汇总指标。
    返回 list of dict，每个 dict 包含区间的均值/极值/趋势。
    """
    dates = indicators["dates"]
    n = len(dates)
    if not splits:
        return []

    # 构建区间边界（包含首尾）
    boundaries = [0] + [s.index for s in splits] + [n - 1]
    summaries = []

    for k in range(len(boundaries) - 1):
        start_i = boundaries[k]
        end_i   = boundaries[k + 1]
        if start_i >= end_i:
            continue

        seg_dates = [str(dates[i]) for i in range(start_i, end_i + 1)]
        seg_closes = [float(indicators["ohlcv"][i]["close"] or 0) for i in range(start_i, end_i + 1)]
        seg_rsi = [indicators["rsi14"][i] for i in range(start_i, end_i + 1) if indicators["rsi14"][i] is not None]
        seg_hist = [indicators["macd_hist"][i] for i in range(start_i, end_i + 1) if indicators["macd_hist"][i] is not None]
        seg_vr = [indicators["volume_ratio"][i] for i in range(start_i, end_i + 1) if indicators["volume_ratio"][i] is not None]
        seg_pr = [indicators["profit_ratio"][i] for i in range(start_i, end_i + 1) if indicators["profit_ratio"][i] is not None]

        cap_map = indicators.get("cap_map", {})
        seg_cap = []
        for i in range(start_i, end_i + 1):
            d = dates[i]
            cap = cap_map.get(d) or cap_map.get(str(d))
            if cap:
                seg_cap.append(float(cap.get("main_net_inflow") or 0))

        price_trend = "上涨" if seg_closes[-1] > seg_closes[0] else "下跌"
        price_chg_pct = round((seg_closes[-1] - seg_closes[0]) / seg_closes[0] * 100, 2) if seg_closes[0] else 0

        summaries.append({
            "start_date":    seg_dates[0],
            "end_date":      seg_dates[-1],
            "start_index":   start_i,
            "end_index":     end_i,
            "days":          end_i - start_i + 1,
            "price_change_pct": price_chg_pct,
            "price_trend":   price_trend,
            "avg_rsi":       round(sum(seg_rsi) / len(seg_rsi), 1) if seg_rsi else None,
            "avg_macd_hist": round(sum(seg_hist) / len(seg_hist), 4) if seg_hist else None,
            "avg_volume_ratio": round(sum(seg_vr) / len(seg_vr), 2) if seg_vr else None,
            "avg_profit_ratio": round(sum(seg_pr) / len(seg_pr), 1) if seg_pr else None,
            "total_capital_flow_bn": round(sum(seg_cap) / 1e8, 2) if seg_cap else 0,
            "start_snapshot": _build_snapshot(start_i, indicators),
            "end_snapshot":   _build_snapshot(end_i, indicators),
        })

    return summaries

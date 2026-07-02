"""
量化预测触发条件生成器

从当前段末尾的指标快照出发，对比目标情形的 SITUATION_CRITERIA，
计算出"指标从当前值需变化到什么具体数值"才进入目标情形。
输出结构化的、可程序化检测的触发条件列表。
"""
import logging
from typing import Optional

from analysis.situation_constants import SITUATION_CRITERIA, SITUATION_NAMES

logger = logging.getLogger(__name__)


def quantify_triggers(
    stock_code: str,
    indicators: dict,
    target_situation_id: int,
    end_idx: Optional[int] = None,
) -> list:
    """
    生成量化触发条件列表。

    Args:
        stock_code: 股票代码
        indicators: compute_all_indicators 返回的完整指标
        target_situation_id: 目标情形编号(1-17)
        end_idx: 末尾索引(默认取最后一天)

    Returns:
        [
            {
                "metric": "rsi14",
                "label": "RSI(14)",
                "operator": ">=",         # <=, >=, between, cross_above, cross_below, consecutive_positive, consecutive_negative
                "threshold": 35.0,        # 阈值
                "threshold_display": "",  # 人类可读显示(可选)
                "current_value": 28.3,
                "direction": "up",        # up/down/flip
                "gap_pct": 23.7,          # 距离触发的距离百分比
                "priority": 1,            # 1=核心, 2=辅助
                "satisfied": False,
            },
            ...
        ]
    """
    if target_situation_id not in SITUATION_CRITERIA:
        return []

    criteria = SITUATION_CRITERIA[target_situation_id]
    n = len(indicators["dates"])
    if n == 0:
        return []
    if end_idx is None:
        end_idx = n - 1

    # 获取当前快照值
    ohlcv = indicators["ohlcv"]
    close_now = float(ohlcv[end_idx]["close"] or 0)
    rsi_now = indicators["rsi14"][end_idx] if indicators["rsi14"][end_idx] is not None else 50
    ma20_list = indicators.get("ma20", [])
    ma20_now = ma20_list[end_idx] if end_idx < len(ma20_list) and ma20_list[end_idx] else close_now
    macd_hist_now = indicators["macd_hist"][end_idx] if indicators["macd_hist"][end_idx] is not None else 0
    vr_now = indicators["volume_ratio"][end_idx] if end_idx < len(indicators.get("volume_ratio", [])) and indicators["volume_ratio"][end_idx] is not None else 1.0
    pr_now = indicators["profit_ratio"][end_idx] if end_idx < len(indicators.get("profit_ratio", [])) and indicators["profit_ratio"][end_idx] is not None else 50

    # 资金流(最近3日)
    cap_map = indicators.get("cap_map", {})
    dates = indicators["dates"]
    recent_cap_days = 0
    for i in range(max(0, end_idx - 2), end_idx + 1):
        d = dates[i]
        cap = cap_map.get(d) or cap_map.get(str(d))
        if cap and float(cap.get("main_net_inflow") or 0) > 0:
            recent_cap_days += 1

    # RSI趋势(最近5日)
    rsi_vals = [indicators["rsi14"][i] for i in range(max(0, end_idx - 4), end_idx + 1)
                if indicators["rsi14"][i] is not None]
    rsi_slope_now = 0
    if len(rsi_vals) >= 3:
        if rsi_vals[-1] > rsi_vals[0] + 3:
            rsi_slope_now = 1
        elif rsi_vals[-1] < rsi_vals[0] - 3:
            rsi_slope_now = -1

    triggers = []

    # === 1. RSI ===
    rsi_lo, rsi_hi = criteria["rsi"]
    if rsi_now < rsi_lo:
        gap = (rsi_lo - rsi_now) / max(rsi_now, 1) * 100
        triggers.append({
            "metric": "rsi14", "label": "RSI(14)",
            "operator": ">=", "threshold": round(rsi_lo, 1),
            "threshold_display": "RSI升至{}以上".format(round(rsi_lo, 1)),
            "current_value": round(rsi_now, 1),
            "direction": "up", "gap_pct": round(gap, 1),
            "priority": 1, "satisfied": False,
        })
    elif rsi_now > rsi_hi:
        gap = (rsi_now - rsi_hi) / max(rsi_now, 1) * 100
        triggers.append({
            "metric": "rsi14", "label": "RSI(14)",
            "operator": "<=", "threshold": round(rsi_hi, 1),
            "threshold_display": "RSI降至{}以下".format(round(rsi_hi, 1)),
            "current_value": round(rsi_now, 1),
            "direction": "down", "gap_pct": round(gap, 1),
            "priority": 1, "satisfied": False,
        })
    else:
        # 已在范围内
        triggers.append({
            "metric": "rsi14", "label": "RSI(14)",
            "operator": "between", "threshold": [round(rsi_lo, 1), round(rsi_hi, 1)],
            "threshold_display": "RSI维持在{}-{}区间".format(round(rsi_lo, 1), round(rsi_hi, 1)),
            "current_value": round(rsi_now, 1),
            "direction": "range", "gap_pct": 0,
            "priority": 2, "satisfied": True,
        })

    # === 2. 价格 vs MA20 → 转为绝对价格 ===
    pvm_lo, pvm_hi = criteria["price_vs_ma20_pct"]
    target_price_lo = round(ma20_now * (1 + pvm_lo / 100), 2)
    target_price_hi = round(ma20_now * (1 + pvm_hi / 100), 2)

    if close_now > target_price_hi:
        gap = (close_now - target_price_hi) / close_now * 100
        triggers.append({
            "metric": "price", "label": "收盘价",
            "operator": "<=", "threshold": target_price_hi,
            "threshold_display": "价格回落至{}元以下".format(target_price_hi),
            "current_value": round(close_now, 2),
            "direction": "down", "gap_pct": round(gap, 1),
            "priority": 1, "satisfied": False,
        })
    elif close_now < target_price_lo:
        gap = (target_price_lo - close_now) / close_now * 100
        triggers.append({
            "metric": "price", "label": "收盘价",
            "operator": ">=", "threshold": target_price_lo,
            "threshold_display": "价格站上{}元".format(target_price_lo),
            "current_value": round(close_now, 2),
            "direction": "up", "gap_pct": round(gap, 1),
            "priority": 1, "satisfied": False,
        })
    else:
        triggers.append({
            "metric": "price", "label": "收盘价",
            "operator": "between", "threshold": [target_price_lo, target_price_hi],
            "threshold_display": "价格在{}-{}元区间".format(target_price_lo, target_price_hi),
            "current_value": round(close_now, 2),
            "direction": "range", "gap_pct": 0,
            "priority": 2, "satisfied": True,
        })

    # === 3. MACD方向 ===
    macd_lo, macd_hi = criteria["macd_hist_sign"]
    macd_sign_now = 1 if macd_hist_now > 0 else (-1 if macd_hist_now < 0 else 0)
    if macd_sign_now < macd_lo:
        # 需要MACD转正
        triggers.append({
            "metric": "macd_hist_sign", "label": "MACD柱",
            "operator": "cross_above", "threshold": 0,
            "threshold_display": "MACD柱转正(金叉)",
            "current_value": round(macd_hist_now, 4),
            "direction": "up", "gap_pct": 100,
            "priority": 1, "satisfied": False,
        })
    elif macd_sign_now > macd_hi:
        # 需要MACD转负
        triggers.append({
            "metric": "macd_hist_sign", "label": "MACD柱",
            "operator": "cross_below", "threshold": 0,
            "threshold_display": "MACD柱转负(死叉)",
            "current_value": round(macd_hist_now, 4),
            "direction": "down", "gap_pct": 100,
            "priority": 1, "satisfied": False,
        })
    else:
        triggers.append({
            "metric": "macd_hist_sign", "label": "MACD柱",
            "operator": "between", "threshold": [macd_lo, macd_hi],
            "threshold_display": "MACD方向符合",
            "current_value": round(macd_hist_now, 4),
            "direction": "range", "gap_pct": 0,
            "priority": 2, "satisfied": True,
        })

    # === 4. 资金流方向 ===
    cap_lo, cap_hi = criteria["capital_flow_sign"]
    cap_sign_now = 1 if recent_cap_days >= 2 else (-1 if recent_cap_days == 0 else 0)
    if cap_sign_now < cap_lo:
        # 需要资金转正
        triggers.append({
            "metric": "capital_flow_3d", "label": "主力资金",
            "operator": "consecutive_positive", "threshold": 3,
            "threshold_display": "连续3日主力净流入",
            "current_value": recent_cap_days,
            "direction": "up", "gap_pct": round((3 - recent_cap_days) / 3 * 100, 0),
            "priority": 1, "satisfied": False,
        })
    elif cap_sign_now > cap_hi:
        # 需要资金转负
        triggers.append({
            "metric": "capital_flow_3d", "label": "主力资金",
            "operator": "consecutive_negative", "threshold": 3,
            "threshold_display": "连续3日主力净流出",
            "current_value": recent_cap_days,
            "direction": "down", "gap_pct": round(recent_cap_days / 3 * 100, 0),
            "priority": 1, "satisfied": False,
        })
    else:
        triggers.append({
            "metric": "capital_flow_3d", "label": "主力资金",
            "operator": "between", "threshold": [cap_lo, cap_hi],
            "threshold_display": "资金方向符合",
            "current_value": recent_cap_days,
            "direction": "range", "gap_pct": 0,
            "priority": 2, "satisfied": True,
        })

    # === 5. 量比 ===
    vr_lo, vr_hi = criteria["volume_ratio"]
    if vr_now < vr_lo:
        gap = (vr_lo - vr_now) / max(vr_now, 0.1) * 100
        triggers.append({
            "metric": "volume_ratio", "label": "量比(20日)",
            "operator": ">=", "threshold": round(vr_lo, 2),
            "threshold_display": "量比升至{}倍以上".format(round(vr_lo, 2)),
            "current_value": round(vr_now, 2),
            "direction": "up", "gap_pct": round(gap, 1),
            "priority": 2, "satisfied": False,
        })
    elif vr_now > vr_hi:
        gap = (vr_now - vr_hi) / max(vr_now, 0.1) * 100
        triggers.append({
            "metric": "volume_ratio", "label": "量比(20日)",
            "operator": "<=", "threshold": round(vr_hi, 2),
            "threshold_display": "量比降至{}倍以下".format(round(vr_hi, 2)),
            "current_value": round(vr_now, 2),
            "direction": "down", "gap_pct": round(gap, 1),
            "priority": 2, "satisfied": False,
        })
    else:
        triggers.append({
            "metric": "volume_ratio", "label": "量比(20日)",
            "operator": "between", "threshold": [round(vr_lo, 2), round(vr_hi, 2)],
            "threshold_display": "量比维持{}-{}".format(round(vr_lo, 2), round(vr_hi, 2)),
            "current_value": round(vr_now, 2),
            "direction": "range", "gap_pct": 0,
            "priority": 2, "satisfied": True,
        })

    # === 6. RSI趋势方向 ===
    slope_lo, slope_hi = criteria["rsi_slope"]
    if rsi_slope_now < slope_lo:
        triggers.append({
            "metric": "rsi_slope", "label": "RSI趋势",
            "operator": ">=", "threshold": slope_lo,
            "threshold_display": "RSI连续3日上行" if slope_lo >= 1 else "RSI止跌企稳",
            "current_value": rsi_slope_now,
            "direction": "up", "gap_pct": 100,
            "priority": 2, "satisfied": False,
        })
    elif rsi_slope_now > slope_hi:
        triggers.append({
            "metric": "rsi_slope", "label": "RSI趋势",
            "operator": "<=", "threshold": slope_hi,
            "threshold_display": "RSI连续3日下行" if slope_hi <= -1 else "RSI涨势放缓",
            "current_value": rsi_slope_now,
            "direction": "down", "gap_pct": 100,
            "priority": 2, "satisfied": False,
        })
    else:
        triggers.append({
            "metric": "rsi_slope", "label": "RSI趋势",
            "operator": "between", "threshold": [slope_lo, slope_hi],
            "threshold_display": "RSI趋势符合",
            "current_value": rsi_slope_now,
            "direction": "range", "gap_pct": 0,
            "priority": 2, "satisfied": True,
        })

    # 只保留未满足的核心条件 + 未满足的辅助条件（最多6个），已满足的也保留用于显示
    # 按 priority ASC, gap_pct DESC 排序
    triggers.sort(key=lambda t: (t["priority"], -t["gap_pct"]))

    return triggers


def generate_trigger_summary(triggers: list) -> str:
    """从量化条件列表生成一句话摘要(用于前端trigger字段)"""
    unsatisfied = [t for t in triggers if not t.get("satisfied") and t["priority"] == 1]
    if not unsatisfied:
        unsatisfied = [t for t in triggers if not t.get("satisfied")]

    parts = [t["threshold_display"] for t in unsatisfied[:3]]
    return " + ".join(parts) if parts else "条件基本满足，关注确认信号"

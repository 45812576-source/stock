"""第三层：后验校验 — 规则回检DeepSeek输出"""
import logging
from analysis.situation_constants import (
    SITUATION_CRITERIA, is_transition_allowed, SITUATION_NAMES,
)

logger = logging.getLogger(__name__)

CONFIDENCE_HIGH   = 0.75
CONFIDENCE_MEDIUM = 0.50


def _avg(lst):
    vals = [v for v in lst if v is not None]
    return sum(vals) / len(vals) if vals else None


def _in_range(val, rng):
    """检查 val 是否在 (min, max) 范围内"""
    if val is None or rng is None:
        return None  # 无法判断
    return rng[0] <= val <= rng[1]


def _sign_match(val, rng):
    """检查符号是否匹配 (-1=负, 0=零, 1=正)"""
    if val is None:
        return None
    sign = 1 if val > 0 else (-1 if val < 0 else 0)
    return rng[0] <= sign <= rng[1]


def validate_stages(stages: list, indicators: dict) -> list:
    """
    校验每段的情形标注是否与实际指标匹配。
    返回带 computed_confidence / failed_checks / needs_retry 字段的 stages 列表。
    """
    dates = [str(d) for d in indicators.get("dates", [])]
    validated = []

    for idx, stage in enumerate(stages):
        sit_id = stage.get("situation_id")
        start_d = str(stage.get("start_date", ""))
        end_d   = str(stage.get("end_date", ""))

        criteria = SITUATION_CRITERIA.get(sit_id, {})
        checks = []

        # 收集该段的指标均值
        seg_indices = [i for i, d in enumerate(dates) if start_d <= str(d) <= end_d]
        if not seg_indices:
            stage["computed_confidence"] = 0.3
            stage["failed_checks"] = ["找不到对应日期数据"]
            stage["needs_retry"] = True
            validated.append(stage)
            continue

        seg_rsi    = [indicators["rsi14"][i] for i in seg_indices if indicators["rsi14"][i] is not None]
        seg_hist   = [indicators["macd_hist"][i] for i in seg_indices if indicators["macd_hist"][i] is not None]
        seg_vr     = [indicators["volume_ratio"][i] for i in seg_indices if indicators["volume_ratio"][i] is not None]
        seg_pr     = [indicators["profit_ratio"][i] for i in seg_indices if indicators["profit_ratio"][i] is not None]
        seg_ma20   = [indicators["ma20"][i] for i in seg_indices if indicators["ma20"][i] is not None]
        seg_closes = [float(indicators["ohlcv"][i]["close"] or 0) for i in seg_indices]

        avg_rsi  = _avg(seg_rsi)
        avg_hist = _avg(seg_hist)
        avg_vr   = _avg(seg_vr)
        avg_pr   = _avg(seg_pr)
        avg_ma20 = _avg(seg_ma20)
        avg_close = _avg(seg_closes)
        price_vs_ma20 = (avg_close - avg_ma20) / avg_ma20 * 100 if avg_ma20 else None

        # 资金流
        cap_map = indicators.get("cap_map", {})
        cap_vals = []
        for i in seg_indices:
            d = dates[i]
            cap = cap_map.get(d) or cap_map.get(str(d))
            if cap:
                cap_vals.append(float(cap.get("main_net_inflow") or 0))
        avg_cap_sign = (1 if sum(cap_vals) > 0 else -1) if cap_vals else None

        # ── 7维度校验 ──
        def chk(name, result):
            checks.append({"name": name, "passed": result})

        # 1. RSI范围
        rsi_rng = criteria.get("rsi")
        chk("RSI范围", _in_range(avg_rsi, rsi_rng))

        # 2. 价格相对MA20
        ma20_rng = criteria.get("price_vs_ma20_pct")
        chk("价格vs MA20", _in_range(price_vs_ma20, ma20_rng))

        # 3. MACD柱符号
        hist_rng = criteria.get("macd_hist_sign")
        chk("MACD方向", _sign_match(avg_hist, hist_rng))

        # 4. 资金流方向
        cap_rng = criteria.get("capital_flow_sign")
        chk("资金流方向", _sign_match(avg_cap_sign, cap_rng) if avg_cap_sign is not None else None)

        # 5. 获利盘比例
        pr_rng = criteria.get("profit_ratio_pct")
        chk("获利盘比例", _in_range(avg_pr, pr_rng))

        # 6. 量比
        vr_rng = criteria.get("volume_ratio")
        chk("量比范围", _in_range(avg_vr, vr_rng))

        # 7. 转换矩阵（与前一段）
        if idx > 0:
            prev_sit = stages[idx - 1].get("situation_id")
            if prev_sit:
                allowed = is_transition_allowed(prev_sit, sit_id)
                chk("转换矩阵", allowed)
        # 8. 时长校验
        days = len(seg_indices)
        chk("时长合理(1-30天)", 1 <= days <= 30)

        # 计算置信度（只统计有明确结果的检查）
        definite = [c for c in checks if c["passed"] is not None]
        passed   = [c for c in definite if c["passed"]]
        confidence = len(passed) / len(definite) if definite else 0.5

        failed = [c["name"] for c in checks if c["passed"] is False]

        stage["computed_confidence"] = round(confidence, 3)
        stage["failed_checks"] = failed
        stage["needs_retry"] = confidence < CONFIDENCE_MEDIUM and len(failed) > 2
        stage["check_detail"] = checks

        validated.append(stage)

    return validated


def build_retry_prompt(failed_stages: list, indicators: dict) -> str:
    """为置信度不足的段构建重分析提示"""
    lines = ["以下阶段的情形标注置信度不足，请重新分析：\n"]
    for s in failed_stages:
        lines.append(
            f"- {s['start_date']} ~ {s['end_date']}: "
            f"当前标注情形{s['situation_id']}({SITUATION_NAMES.get(s['situation_id'],'')}), "
            f"置信度{s['computed_confidence']:.0%}, "
            f"失败检查: {', '.join(s['failed_checks'])}"
        )
    lines.append("\n请重新为这些段选择更合适的情形编号，并确保与7维度指标匹配。")
    return "\n".join(lines)

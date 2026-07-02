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

    # 过滤弱信号：仅有1个弱原因的切割点丢弃
    WEAK_REASONS = {"RSI上穿50", "RSI下穿50", "主力资金由流入转流出", "主力资金由流出转流入"}
    splits = [
        s for s in splits
        if len(s.reasons) >= 2 or not all(r in WEAK_REASONS for r in s.reasons)
    ]

    # 基础去重：2个交易日内的重复点只保留信号最强的
    splits = _dedup_nearby(splits, min_gap=2)

    # 智能合并：基于情形置信度决定是否合并短段
    splits = _smart_merge(splits, indicators)
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


def _dedup_nearby(splits: list, min_gap: int = 2) -> list:
    """基础去重：相邻不足min_gap个交易日的切割点只保留信号最强的"""
    if not splits:
        return splits
    merged = [splits[0]]
    for sp in splits[1:]:
        last = merged[-1]
        if sp.index - last.index < min_gap:
            if len(sp.reasons) >= len(last.reasons):
                merged[-1] = sp
        else:
            merged.append(sp)
    return merged


def _score_segment(start_i: int, end_i: int, indicators: dict,
                   prev_situation: int = None) -> tuple:
    """
    计算一个段与17情形的最佳匹配。9维度加权评分。
    返回 (best_situation_id, confidence_score)

    评分公式:
      总分 = 基碀6维匹配分×0.5 + 价格变动匹配分×0.25 + RSI斜率匹配分×0.1 + 转换矩阵合理性×0.15
    """
    from analysis.situation_constants import SITUATION_CRITERIA, get_transition_prob

    rsi_vals = [indicators["rsi14"][i] for i in range(start_i, end_i + 1)
                if i < len(indicators["rsi14"]) and indicators["rsi14"][i] is not None]
    hist_vals = [indicators["macd_hist"][i] for i in range(start_i, end_i + 1)
                 if i < len(indicators["macd_hist"]) and indicators["macd_hist"][i] is not None]
    vr_vals = [indicators["volume_ratio"][i] for i in range(start_i, end_i + 1)
               if i < len(indicators["volume_ratio"]) and indicators["volume_ratio"][i] is not None]
    pr_vals = [indicators["profit_ratio"][i] for i in range(start_i, end_i + 1)
               if i < len(indicators["profit_ratio"]) and indicators["profit_ratio"][i] is not None]

    # 计算基础均值
    avg_rsi = sum(rsi_vals) / len(rsi_vals) if rsi_vals else 50
    avg_hist = sum(hist_vals) / len(hist_vals) if hist_vals else 0
    avg_vr = sum(vr_vals) / len(vr_vals) if vr_vals else 1.0
    avg_pr = sum(pr_vals) / len(pr_vals) if pr_vals else 50

    # price_vs_ma20
    ma20 = indicators.get("ma20", [])
    ohlcv = indicators["ohlcv"]
    pvm_vals = []
    for i in range(start_i, end_i + 1):
        if i < len(ma20) and ma20[i] and ma20[i] > 0:
            c = float(ohlcv[i]["close"] or 0)
            pvm_vals.append((c - ma20[i]) / ma20[i] * 100)
    avg_pvm = sum(pvm_vals) / len(pvm_vals) if pvm_vals else 0

    # 资金流方向
    cap_map = indicators.get("cap_map", {})
    dates = indicators["dates"]
    cap_sum = 0.0
    for i in range(start_i, end_i + 1):
        d = dates[i]
        cap = cap_map.get(d) or cap_map.get(str(d))
        if cap:
            cap_sum += float(cap.get("main_net_inflow") or 0)

    macd_sign = 1 if avg_hist > 0 else (-1 if avg_hist < 0 else 0)
    cap_sign = 1 if cap_sum > 0 else (-1 if cap_sum < 0 else 0)

    # === 动态维度7: 段内价格变动幅度 ===
    close_start = float(ohlcv[start_i]["close"] or 0)
    close_end = float(ohlcv[end_i]["close"] or 0)
    price_change_pct = ((close_end - close_start) / close_start * 100) if close_start > 0 else 0

    # === 动态维度8: RSI斜率方向 ===
    # 用简化的线性回归方向: 比较前半段与后半段的RSI均值
    if len(rsi_vals) >= 4:
        mid = len(rsi_vals) // 2
        rsi_first_half = sum(rsi_vals[:mid]) / mid
        rsi_second_half = sum(rsi_vals[mid:]) / (len(rsi_vals) - mid)
        rsi_diff = rsi_second_half - rsi_first_half
        if rsi_diff > 3:
            rsi_slope = 1   # 上行
        elif rsi_diff < -3:
            rsi_slope = -1  # 下行
        else:
            rsi_slope = 0   # 平稳
    else:
        rsi_slope = 0

    # === 对每个情形计算加权分 ===
    best_id = 1
    best_score = 0.0
    for sit_id, criteria in SITUATION_CRITERIA.items():
        # --- 基碀6维度 (权重0.5) ---
        base_dims = 0.0
        base_total = 6

        # RSI
        lo, hi = criteria["rsi"]
        if lo <= avg_rsi <= hi:
            base_dims += 1
        elif avg_rsi < lo:
            base_dims += max(0, 1 - (lo - avg_rsi) / 20)
        else:
            base_dims += max(0, 1 - (avg_rsi - hi) / 20)

        # price_vs_ma20
        lo, hi = criteria["price_vs_ma20_pct"]
        if lo <= avg_pvm <= hi:
            base_dims += 1
        elif avg_pvm < lo:
            base_dims += max(0, 1 - (lo - avg_pvm) / 15)
        else:
            base_dims += max(0, 1 - (avg_pvm - hi) / 15)

        # MACD柱方向
        lo, hi = criteria["macd_hist_sign"]
        if lo <= macd_sign <= hi:
            base_dims += 1

        # 资金流方向
        lo, hi = criteria["capital_flow_sign"]
        if lo <= cap_sign <= hi:
            base_dims += 1

        # 获利比例
        lo, hi = criteria["profit_ratio_pct"]
        if lo <= avg_pr <= hi:
            base_dims += 1
        elif avg_pr < lo:
            base_dims += max(0, 1 - (lo - avg_pr) / 25)
        else:
            base_dims += max(0, 1 - (avg_pr - hi) / 25)

        # 量比
        lo, hi = criteria["volume_ratio"]
        if lo <= avg_vr <= hi:
            base_dims += 1
        elif avg_vr < lo:
            base_dims += max(0, 1 - (lo - avg_vr) / 1.0)
        else:
            base_dims += max(0, 1 - (avg_vr - hi) / 2.0)

        base_score = base_dims / base_total

        # --- 价格变动匹配分 (权重0.25) ---
        lo, hi = criteria["price_change_pct"]
        if lo <= price_change_pct <= hi:
            price_score = 1.0
        elif price_change_pct < lo:
            price_score = max(0, 1 - (lo - price_change_pct) / 15)
        else:
            price_score = max(0, 1 - (price_change_pct - hi) / 15)

        # --- RSI斜率匹配分 (权重0.10) ---
        lo, hi = criteria["rsi_slope"]
        if lo <= rsi_slope <= hi:
            slope_score = 1.0
        else:
            slope_score = 0.0  # 方向不对直接0分

        # --- 转换矩阵合理性 (权重0.15) ---
        if prev_situation is not None:
            trans_prob = get_transition_prob(prev_situation, sit_id)
            # 0=禁止, 1=低, 2=中, 3=高 → 映射到0~1
            transition_score = trans_prob / 3.0
        else:
            transition_score = 0.5  # 无上下文时给中性分

        # --- 加权总分 ---
        total_score = (base_score * 0.50 +
                       price_score * 0.25 +
                       slope_score * 0.10 +
                       transition_score * 0.15)

        if total_score > best_score:
            best_score = total_score
            best_id = sit_id

    return best_id, best_score


def _smart_merge(splits: list, indicators: dict) -> list:
    """
    智能合并（基于情形置信度）：
    1. 相邻段匹配同一情形 → 合并（贪心分组，严格受MAX_SEGMENT_DAYS约束）
    2. 同阶段(phase)且至少一段短 → 合并后置信度不下降时合并
    3. 短段(<MIN天)或低置信度段 → 尝试与邻段合并，只在合并后置信度不下降时执行
    """
    from analysis.situation_constants import get_phase

    n_total = len(indicators["dates"])
    if not splits:
        return splits

    MIN_SEGMENT_DAYS = 4
    MAX_SEGMENT_DAYS = 30   # 单段上限，避免过度合并
    MAX_ITERATIONS = 10

    for iteration in range(MAX_ITERATIONS):
        merged_any = False

        # 构建当前段列表
        boundaries = [0] + [s.index for s in splits] + [n_total - 1]
        segments = []
        for k in range(len(boundaries) - 1):
            si, ei = boundaries[k], boundaries[k + 1]
            if si < ei:
                segments.append((si, ei))

        if len(segments) <= 1:
            break

        # 为每段计算最佳情形（带前段上下文）
        scores = []
        for idx, (s, e) in enumerate(segments):
            prev_sit = scores[idx - 1][0] if idx > 0 else None
            scores.append(_score_segment(s, e, indicators, prev_situation=prev_sit))

        # === Pass 1: 相邻同情形合并（贪心分组，防止级联超限）===
        # 用顺序扫描代替集合标记，跟踪当前合并组的起始索引
        to_remove = set()
        group_start_idx = 0  # 当前合并组的起始段索引
        for idx in range(len(segments) - 1):
            sit_a, _ = scores[idx]
            sit_b, _ = scores[idx + 1]
            if sit_a == sit_b:
                # 计算如果把 idx+1 加入当前组，总天数是否超限
                group_total_days = segments[idx + 1][1] - segments[group_start_idx][0]
                if group_total_days <= MAX_SEGMENT_DAYS:
                    to_remove.add(idx)  # 删除idx和idx+1之间的切割点
                    merged_any = True
                else:
                    # 超限了，开始新的合并组
                    group_start_idx = idx + 1
            else:
                # 不同情形，重置组起点
                group_start_idx = idx + 1

        if to_remove:
            for idx in sorted(to_remove, reverse=True):
                if idx < len(splits):
                    del splits[idx]
            continue  # 重新计算

        # === Pass 2: 同阶段(phase)短段合并 ===
        phase_merged = False
        for idx in range(len(segments) - 1):
            sit_a, conf_a = scores[idx]
            sit_b, conf_b = scores[idx + 1]
            days_a = segments[idx][1] - segments[idx][0]
            days_b = segments[idx + 1][1] - segments[idx + 1][0]
            if get_phase(sit_a) == get_phase(sit_b) and (days_a < MIN_SEGMENT_DAYS or days_b < MIN_SEGMENT_DAYS):
                merged_si = segments[idx][0]
                merged_ei = segments[idx + 1][1]
                merged_days = merged_ei - merged_si
                if merged_days > MAX_SEGMENT_DAYS:
                    continue
                _, merged_conf = _score_segment(merged_si, merged_ei, indicators)
                if merged_conf >= min(conf_a, conf_b) * 0.9:
                    if idx < len(splits):
                        del splits[idx]
                    phase_merged = True
                    merged_any = True
                    break  # 每轮只合并一个，重新计算

        if phase_merged:
            continue

        # === Pass 3: 短段/低置信度段合并 ===
        best_merge = None
        for idx, ((si, ei), (sit_id, conf)) in enumerate(zip(segments, scores)):
            days = ei - si
            if days >= MIN_SEGMENT_DAYS and conf >= 0.7:
                continue

            # 尝试向前合并
            if idx > 0:
                merged_si, merged_ei = segments[idx - 1][0], ei
                merged_days = merged_ei - merged_si
                if merged_days <= MAX_SEGMENT_DAYS:
                    _, merged_conf = _score_segment(merged_si, merged_ei, indicators)
                    prev_conf = scores[idx - 1][1]
                    if merged_conf >= max(conf, prev_conf) * 0.95:
                        if best_merge is None or merged_conf > best_merge[2]:
                            best_merge = (idx, 'prev', merged_conf)

            # 尝试向后合并
            if idx < len(segments) - 1:
                merged_si, merged_ei = si, segments[idx + 1][1]
                merged_days = merged_ei - merged_si
                if merged_days <= MAX_SEGMENT_DAYS:
                    _, merged_conf = _score_segment(merged_si, merged_ei, indicators)
                    next_conf = scores[idx + 1][1]
                    if merged_conf >= max(conf, next_conf) * 0.95:
                        if best_merge is None or merged_conf > best_merge[2]:
                            best_merge = (idx, 'next', merged_conf)

        if best_merge is None:
            break

        idx, direction, _ = best_merge
        if direction == 'prev':
            del splits[idx - 1]
        else:
            del splits[idx]
        merged_any = True

        if not merged_any:
            break

    return splits


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

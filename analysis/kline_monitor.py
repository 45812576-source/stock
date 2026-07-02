"""
K线阶段预测监控 — 每日收盘后检测量化trigger是否满足

流程:
1. 查询所有 status='active' 的 prediction_monitors
2. 对每个 monitor 的 stock_code 获取最新日行情指标
3. 逐条检测 triggers_json 中各条件是否满足
4. 更新满足状态，写入 prediction_trigger_events
5. 若触发逻辑满足 → 标记 triggered → 触发新一轮分析
"""
import json
import logging
from datetime import datetime, date

from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)


def run_prediction_monitor() -> dict:
    """主入口 — 每日收盘后由scheduler调用"""
    logger.info("[PredictionMonitor] 开始每日监控检测")

    # 1. 获取活跃监控项
    monitors = _get_active_monitors()
    if not monitors:
        logger.info("[PredictionMonitor] 无活跃监控项")
        return {"checked": 0, "triggered": 0, "expired": 0}

    # 2. 按stock_code分组
    stock_monitors = {}
    for m in monitors:
        stock_monitors.setdefault(m["stock_code"], []).append(m)

    triggered_count = 0
    checked_count = 0

    # 3. 逐股票检测
    for stock_code, mon_list in stock_monitors.items():
        try:
            from analysis.kline_indicators import compute_all_indicators
            indicators = compute_all_indicators(stock_code, days=30)
            if not indicators or not indicators.get("dates"):
                continue

            for monitor in mon_list:
                checked_count += 1
                result = _check_monitor(monitor, indicators)
                if result == "triggered":
                    triggered_count += 1

        except Exception as e:
            logger.error("[PredictionMonitor] {} 检测失败: {}".format(stock_code, e))

    # 4. 清理过期
    expired = _expire_stale_monitors(max_days=30)

    logger.info("[PredictionMonitor] 完成: checked={}, triggered={}, expired={}".format(
        checked_count, triggered_count, expired))
    return {"checked": checked_count, "triggered": triggered_count, "expired": expired}


def _get_active_monitors() -> list:
    """获取所有活跃监控项"""
    rows = execute_query(
        "SELECT * FROM prediction_monitors WHERE status='active' ORDER BY stock_code",
        []
    )
    return [dict(r) for r in rows] if rows else []


def _check_monitor(monitor: dict, indicators: dict) -> str:
    """
    检测单个monitor的所有条件。
    Returns: 'triggered' / 'updated' / 'unchanged'
    """
    triggers = json.loads(monitor["triggers_json"]) if monitor.get("triggers_json") else []
    if not triggers:
        return "unchanged"

    today_str = date.today().isoformat()
    newly_satisfied = []
    satisfied_count = 0

    for i, trigger in enumerate(triggers):
        was_satisfied = trigger.get("satisfied", False)
        is_now = _check_single_trigger(trigger, indicators)

        if is_now:
            satisfied_count += 1
            if not was_satisfied:
                trigger["satisfied"] = True
                newly_satisfied.append(trigger)

    # 更新 triggers_json + satisfied_count
    if newly_satisfied or satisfied_count != monitor.get("satisfied_count", 0):
        execute_insert(
            "UPDATE prediction_monitors SET triggers_json=%s, satisfied_count=%s WHERE id=%s",
            [json.dumps(triggers, ensure_ascii=False), satisfied_count, monitor["id"]]
        )

    # 记录新满足的条件事件
    for t in newly_satisfied:
        execute_insert(
            """INSERT INTO prediction_trigger_events
               (monitor_id, stock_code, event_type, event_detail, trade_date)
               VALUES (%s, %s, %s, %s, %s)""",
            [monitor["id"], monitor["stock_code"], "condition_met",
             json.dumps({"metric": t["metric"], "label": t["label"],
                         "threshold": t["threshold"]}, ensure_ascii=False),
             today_str]
        )

    # 判断是否整体触发
    if _evaluate_trigger_logic(triggers, monitor.get("trigger_logic", "priority_1_all")):
        _on_prediction_triggered(monitor, indicators)
        return "triggered"

    # 动态更新置信度
    if newly_satisfied:
        _update_probability(monitor, satisfied_count, len(triggers))
        return "updated"

    return "unchanged"


def _check_single_trigger(trigger: dict, indicators: dict) -> bool:
    """检测单个条件是否满足"""
    metric = trigger["metric"]
    op = trigger["operator"]
    threshold = trigger["threshold"]

    n = len(indicators["dates"])
    if n == 0:
        return False
    end_i = n - 1

    # 获取当前值
    value = None
    if metric == "rsi14":
        value = indicators["rsi14"][end_i]
    elif metric == "price":
        value = float(indicators["ohlcv"][end_i]["close"] or 0)
    elif metric == "volume_ratio":
        vr = indicators.get("volume_ratio", [])
        value = vr[end_i] if end_i < len(vr) and vr[end_i] is not None else None
    elif metric == "macd_hist_sign":
        hist = indicators["macd_hist"][end_i]
        value = hist if hist is not None else 0
    elif metric == "capital_flow_3d":
        cap_map = indicators.get("cap_map", {})
        dates = indicators["dates"]
        count = 0
        for i in range(max(0, end_i - 2), end_i + 1):
            d = dates[i]
            cap = cap_map.get(d) or cap_map.get(str(d))
            if cap and float(cap.get("main_net_inflow") or 0) > 0:
                count += 1
        value = count
    elif metric == "rsi_slope":
        rsi_vals = [indicators["rsi14"][i] for i in range(max(0, end_i - 4), end_i + 1)
                    if indicators["rsi14"][i] is not None]
        if len(rsi_vals) >= 3:
            value = 1 if rsi_vals[-1] > rsi_vals[0] + 3 else (-1 if rsi_vals[-1] < rsi_vals[0] - 3 else 0)
        else:
            value = 0
    else:
        return False

    if value is None:
        return False

    # 算子匹配
    if op == ">=":
        return value >= threshold
    elif op == "<=":
        return value <= threshold
    elif op == "between":
        if isinstance(threshold, list) and len(threshold) == 2:
            return threshold[0] <= value <= threshold[1]
        return False
    elif op == "cross_above":
        # MACD柱从负转正
        return value > 0
    elif op == "cross_below":
        # MACD柱从正转负
        return value < 0
    elif op == "consecutive_positive":
        return value >= threshold
    elif op == "consecutive_negative":
        # capital_flow: 0 days positive out of 3 = all negative
        return value == 0

    return False


def _evaluate_trigger_logic(triggers: list, logic: str) -> bool:
    """根据 trigger_logic 判断整体是否触发"""
    if logic == "priority_1_all":
        # 所有 priority=1 的条件必须满足
        p1_triggers = [t for t in triggers if t.get("priority") == 1]
        if not p1_triggers:
            return False
        return all(t.get("satisfied", False) for t in p1_triggers)

    elif logic.startswith("any_"):
        # any_N_of_M 格式
        parts = logic.split("_")
        if len(parts) >= 4:
            n_required = int(parts[1])
            satisfied = sum(1 for t in triggers if t.get("satisfied", False))
            return satisfied >= n_required
        return False

    elif logic == "all":
        return all(t.get("satisfied", False) for t in triggers)

    # 默认: priority_1_all
    p1_triggers = [t for t in triggers if t.get("priority") == 1]
    return all(t.get("satisfied", False) for t in p1_triggers) if p1_triggers else False


def _on_prediction_triggered(monitor: dict, indicators: dict):
    """触发后处理"""
    stock_code = monitor["stock_code"]
    today_str = date.today().isoformat()
    logger.info("[PredictionMonitor] {} 触发: 情形{} {}".format(
        stock_code, monitor["situation_id"], monitor.get("scenario_name", "")))

    # 1. 标记当前 monitor 为 triggered
    execute_insert(
        "UPDATE prediction_monitors SET status='triggered', triggered_at=NOW() WHERE id=%s",
        [monitor["id"]]
    )

    # 2. 同stock其他active monitors → superseded
    execute_insert(
        "UPDATE prediction_monitors SET status='superseded' WHERE stock_code=%s AND status='active' AND id!=%s",
        [stock_code, monitor["id"]]
    )

    # 3. 写入触发事件
    execute_insert(
        """INSERT INTO prediction_trigger_events
           (monitor_id, stock_code, event_type, event_detail, trade_date)
           VALUES (%s, %s, %s, %s, %s)""",
        [monitor["id"], stock_code, "all_triggered",
         json.dumps({"situation_id": monitor["situation_id"],
                     "scenario": monitor.get("scenario_name", "")}, ensure_ascii=False),
         today_str]
    )

    # 4. 触发新一轮分析（异步，避免阻塞监控循环）
    try:
        from analysis.kline_analyzer import run_full_analysis
        result = run_full_analysis(stock_code, days=180)
        if result and result.get("ok"):
            logger.info("[PredictionMonitor] {} 重分析完成".format(stock_code))
    except Exception as e:
        logger.error("[PredictionMonitor] {} 重分析失败: {}".format(stock_code, e))


def _update_probability(monitor: dict, satisfied: int, total: int):
    """动态更新置信度"""
    base_prob = monitor.get("probability", 0.5)
    created = monitor.get("created_at")
    days_elapsed = 0
    if created:
        try:
            if isinstance(created, datetime):
                days_elapsed = (datetime.now() - created).days
            else:
                days_elapsed = (datetime.now() - datetime.strptime(str(created)[:19], "%Y-%m-%d %H:%M:%S")).days
        except:
            pass

    # 满足加成
    satisfy_ratio = satisfied / total if total > 0 else 0
    boost = 1 + satisfy_ratio * 0.3

    # 时间衰减
    decay = max(0.5, 1 - max(0, days_elapsed - 15) * 0.02)

    new_prob = min(0.99, base_prob * boost * decay)

    execute_insert(
        "UPDATE prediction_monitors SET probability=%s WHERE id=%s",
        [round(new_prob, 3), monitor["id"]]
    )

    # 写入事件
    execute_insert(
        """INSERT INTO prediction_trigger_events
           (monitor_id, stock_code, event_type, event_detail, trade_date)
           VALUES (%s, %s, %s, %s, %s)""",
        [monitor["id"], monitor["stock_code"], "probability_update",
         json.dumps({"old": round(base_prob, 3), "new": round(new_prob, 3),
                     "satisfied": satisfied, "total": total}, ensure_ascii=False),
         date.today().isoformat()]
    )


def _expire_stale_monitors(max_days: int = 30) -> int:
    """超过max_days天未触发的 monitor 标记 expired"""
    result = execute_insert(
        """UPDATE prediction_monitors SET status='expired'
           WHERE status='active' AND created_at < DATE_SUB(NOW(), INTERVAL %s DAY)""",
        [max_days]
    )
    return result if isinstance(result, int) else 0

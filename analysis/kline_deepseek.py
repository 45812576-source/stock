"""第二层：DeepSeek tool_use 情形识别"""
import json
import logging
from analysis.situation_constants import (
    SITUATION_NAMES, SITUATION_CRITERIA, get_transition_prob, EXIT_SIGNALS,
    SITUATION_PHASES,
)

logger = logging.getLogger(__name__)

MAX_TOOL_ROUNDS = 40


def _get_api_key() -> str:
    try:
        from utils.db_utils import execute_query
        rows = execute_query("SELECT value FROM system_config WHERE config_key='deepseek_api_key'")
        if rows and rows[0].get("value"):
            return rows[0]["value"]
    except Exception:
        pass
    import os
    return os.environ.get("DEEPSEEK_API_KEY", "")


# ── Tool 函数 ─────────────────────────────────────────────────────────────────

def _tool_get_segment_detail(start_date: str, end_date: str, indicators: dict) -> str:
    dates = [str(d) for d in indicators.get("dates", [])]
    result = []
    for i, d in enumerate(dates):
        if d < start_date or d > end_date:
            continue
        row = indicators["ohlcv"][i]
        result.append({
            "date": d,
            "open": row["open"], "high": row["high"],
            "low": row["low"], "close": row["close"],
            "volume": row["volume"],
            "rsi": indicators["rsi14"][i],
            "macd_hist": indicators["macd_hist"][i],
            "ma20": indicators["ma20"][i],
            "volume_ratio": indicators["volume_ratio"][i],
        })
    return json.dumps(result[:30], ensure_ascii=False, default=str)


def _tool_get_momentum_at_point(date: str, indicators: dict) -> str:
    dates = [str(d) for d in indicators.get("dates", [])]
    try:
        i = dates.index(date)
    except ValueError:
        # 找最近的
        i = min(range(len(dates)), key=lambda x: abs(dates[x] > date))
    from analysis.kline_presegment import _build_snapshot
    snap = _build_snapshot(i, indicators)
    return json.dumps(snap, ensure_ascii=False)


def _tool_validate_transition(from_situation: int, to_situation: int) -> str:
    prob = get_transition_prob(from_situation, to_situation)
    labels = {0: "🚫禁止", 1: "⚠️低概率", 2: "✅中概率", 3: "✅✅高概率"}
    return json.dumps({
        "from": from_situation,
        "to": to_situation,
        "allowed": prob > 0,
        "probability_level": prob,
        "label": labels.get(prob, "未知"),
    }, ensure_ascii=False)


def _tool_get_capital_flow_trend(start_date: str, end_date: str, indicators: dict) -> str:
    cap_map = indicators.get("cap_map", {})
    total_main = 0.0
    total_small = 0.0
    days = 0
    for d, cap in cap_map.items():
        ds = str(d)
        if start_date <= ds <= end_date:
            total_main += float(cap.get("main_net_inflow") or 0)
            total_small += float(cap.get("small_net") or 0)
            days += 1
    trend = "净流入" if total_main > 0 else "净流出"
    return json.dumps({
        "start_date": start_date, "end_date": end_date,
        "total_main_flow_bn": round(total_main / 1e8, 2),
        "total_small_flow_bn": round(total_small / 1e8, 2),
        "days": days,
        "trend": trend,
        "daily_avg_main_bn": round(total_main / 1e8 / days, 3) if days else 0,
    }, ensure_ascii=False)


def _tool_get_situation_criteria(situation_id: int) -> str:
    name = SITUATION_NAMES.get(situation_id, "未知")
    criteria = SITUATION_CRITERIA.get(situation_id, {})
    exits = EXIT_SIGNALS.get(situation_id, [])
    phase = next((p for p, ids in SITUATION_PHASES.items() if situation_id in ids), "unknown")
    return json.dumps({
        "id": situation_id,
        "name": name,
        "phase": phase,
        "criteria": criteria,
        "exit_signals": exits,
    }, ensure_ascii=False)


# ── Tool Schemas ──────────────────────────────────────────────────────────────

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "get_segment_detail",
            "description": "查看某段时间的详细日线数据（OHLCV+指标）",
            "parameters": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "开始日期 YYYY-MM-DD"},
                    "end_date":   {"type": "string", "description": "结束日期 YYYY-MM-DD"},
                },
                "required": ["start_date", "end_date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_momentum_at_point",
            "description": "查看某个时间点的完整7维度指标快照",
            "parameters": {
                "type": "object",
                "properties": {
                    "date": {"type": "string", "description": "日期 YYYY-MM-DD"},
                },
                "required": ["date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "validate_transition",
            "description": "查询转换矩阵：从情形X到情形Y是否允许及概率",
            "parameters": {
                "type": "object",
                "properties": {
                    "from_situation": {"type": "integer", "description": "来源情形编号 1-17"},
                    "to_situation":   {"type": "integer", "description": "目标情形编号 1-17"},
                },
                "required": ["from_situation", "to_situation"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_capital_flow_trend",
            "description": "查看某段时间的资金流趋势汇总",
            "parameters": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string"},
                    "end_date":   {"type": "string"},
                },
                "required": ["start_date", "end_date"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_situation_criteria",
            "description": "查看某情形的7维度标准定义和退出信号",
            "parameters": {
                "type": "object",
                "properties": {
                    "situation_id": {"type": "integer", "description": "情形编号 1-17"},
                },
                "required": ["situation_id"],
            },
        },
    },
]


def _build_system_prompt(candidate_splits: list, segment_summaries: list) -> str:
    # 精简版情形列表
    sit_list = "\n".join(
        f"  {k}: {v}" for k, v in SITUATION_NAMES.items()
    )
    # 候选切割点摘要（只保留 date + reasons，去掉 snapshot 减少 token）
    splits_text = json.dumps(
        [{"date": s.date, "reasons": s.reasons} for s in candidate_splits],
        ensure_ascii=False
    )
    # 区间汇总（精简：去掉 start_snapshot/end_snapshot，减少 token；详情可通过工具查询）
    segs_slim = [
        {k: v for k, v in seg.items() if k not in ("start_snapshot", "end_snapshot", "start_index", "end_index")}
        for seg in segment_summaries
    ]
    segs_text = json.dumps(segs_slim, ensure_ascii=False)

    return f"""你是股票技术分析专家，专注于威科夫/阶段分析框架。

## 17情形分类框架
{sit_list}

## 阶段归属
- 吸筹阶段: 情形1-3
- 上涨阶段: 情形4-6
- 派发阶段: 情形7-10
- 下跌阶段: 情形11-15
- 再吸筹/底部: 情形16-17

## 你的任务
Python已预计算了候选切割点（基于MA交叉、MACD变号、量能突变等硬指标）。
你需要：
1. 从候选切割点中选择最终分割点（可以合并或跳过某些候选点）
2. 为每段标注情形编号(1-17)和简要summary（20字以内）
3. 分析最后一段（当前阶段）的退出条件
4. 预测下一阶段Top 3场景

## 约束条件（必须遵守）
- 每段时长1-30天（太短的段可以合并）
- 相邻段转换必须在转换矩阵中概率>0（用validate_transition工具确认）
- 每段情形必须与该段指标的7维度状态匹配（用get_situation_criteria查看标准）
- 如不确定，用get_segment_detail查看详细日线数据

## 候选切割点（Python预计算，只含日期和触发原因）
{splits_text}

## 各候选区间汇总指标（start_snapshot/end_snapshot可通过get_momentum_at_point工具查询）
{segs_text}

## 输出格式（严格JSON，不要有其他文字）
{{
  "stages": [
    {{
      "start_date": "YYYY-MM-DD",
      "end_date": "YYYY-MM-DD",
      "situation_id": 1,
      "name": "情形名称",
      "summary": "20字以内的阶段描述",
      "confidence": 0.8
    }}
  ],
  "current_stage": {{
    "situation_id": 4,
    "name": "真突破",
    "summary": "当前阶段描述",
    "exit_conditions": ["退出信号1", "退出信号2"],
    "days_in_stage": 5
  }},
  "predictions": [
    {{"scenario": "场景描述", "probability": 0.5, "entry_conditions": "进入条件"}},
    {{"scenario": "场景描述", "probability": 0.3, "entry_conditions": "进入条件"}},
    {{"scenario": "场景描述", "probability": 0.2, "entry_conditions": "进入条件"}}
  ]
}}"""


def run_stage_identification(candidate_splits: list, segment_summaries: list,
                              indicators: dict) -> dict:
    """
    DeepSeek tool_use 循环，返回阶段识别结果 dict。
    """
    import openai

    api_key = _get_api_key()
    if not api_key:
        return {"error": "未配置 DeepSeek API Key"}

    client = openai.OpenAI(
        api_key=api_key,
        base_url="https://api.deepseek.com/v1",
        timeout=180,
    )

    system_prompt = _build_system_prompt(candidate_splits, segment_summaries)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": "请分析以上候选切割点，输出最终阶段划分JSON。"},
    ]

    tool_dispatch = {
        "get_segment_detail":    lambda args: _tool_get_segment_detail(args["start_date"], args["end_date"], indicators),
        "get_momentum_at_point": lambda args: _tool_get_momentum_at_point(args["date"], indicators),
        "validate_transition":   lambda args: _tool_validate_transition(args["from_situation"], args["to_situation"]),
        "get_capital_flow_trend": lambda args: _tool_get_capital_flow_trend(args["start_date"], args["end_date"], indicators),
        "get_situation_criteria": lambda args: _tool_get_situation_criteria(args["situation_id"]),
    }

    for round_num in range(MAX_TOOL_ROUNDS):
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=messages,
            tools=TOOL_SCHEMAS,
            tool_choice="auto",
            max_tokens=4096,
            response_format={"type": "text"},
        )
        choice = resp.choices[0]
        msg = choice.message
        messages.append(msg.model_dump(exclude_none=True))

        if not msg.tool_calls:
            # 最终回答
            content = msg.content or ""
            # 提取JSON（支持多种格式）
            try:
                text = content.strip()
                # 去掉 markdown 代码块
                if "```" in text:
                    parts = text.split("```")
                    for part in parts:
                        p = part.strip()
                        if p.startswith("json"):
                            p = p[4:].strip()
                        if p.startswith("{"):
                            text = p
                            break
                # 如果前面有说明文字，找到第一个 { 开始
                brace_idx = text.find("{")
                if brace_idx > 0:
                    text = text[brace_idx:]
                return json.loads(text.strip())
            except json.JSONDecodeError as e:
                logger.error(f"DeepSeek输出JSON解析失败: {e}\n内容: {content[:500]}")
                return {"error": f"JSON解析失败: {e}", "raw": content}

        # 执行工具
        for tc in msg.tool_calls:
            tool_name = tc.function.name
            try:
                tool_args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                tool_args = {}
            fn = tool_dispatch.get(tool_name)
            result = fn(tool_args) if fn else json.dumps({"error": f"未知工具: {tool_name}"})
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result,
            })

    # 超过轮次：强制要求输出最终JSON
    logger.warning(f"超过 {MAX_TOOL_ROUNDS} 轮，强制要求输出JSON...")
    try:
        messages.append({"role": "user", "content": "请立即停止工具调用，直接输出最终的JSON结果，不要有任何说明文字。"})
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=messages,
            max_tokens=4096,
            response_format={"type": "text"},
        )
        content = resp.choices[0].message.content or ""
        text = content.strip()
        brace_idx = text.find("{")
        if brace_idx >= 0:
            text = text[brace_idx:]
        return json.loads(text.strip())
    except Exception as e:
        logger.error(f"强制输出JSON失败: {e}")
    return {"error": "超过最大工具调用轮次"}

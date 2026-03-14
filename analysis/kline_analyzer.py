"""编排入口 — 串联三层分析流程"""
import json
import logging
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)


def run_full_analysis(stock_code: str, days: int = 180) -> dict:
    """
    完整流程：指标计算 → 预分割 → DeepSeek识别 → 后验校验 → (重试) → 存储

    Returns:
        {"ok": True, "stages": [...], "current_stage": {...}, "predictions": [...]}
        或 {"ok": False, "error": "..."}
    """
    from analysis.kline_indicators import compute_all_indicators
    from analysis.kline_presegment import find_candidate_splits, compute_segment_summaries
    from analysis.kline_deepseek import run_stage_identification
    from analysis.kline_validator import validate_stages, build_retry_prompt
    from utils.db_utils import execute_query, execute_insert

    # ── 第一层：指标计算 ──────────────────────────────────────────────────────
    logger.info(f"[{stock_code}] 开始K线阶段分析，计算技术指标...")
    indicators = compute_all_indicators(stock_code, days=days)
    if not indicators or not indicators.get("dates"):
        return {"ok": False, "error": "无K线数据"}

    # ── 第一层：候选切割点 ────────────────────────────────────────────────────
    logger.info(f"[{stock_code}] 生成候选切割点...")
    candidate_splits = find_candidate_splits(indicators)
    segment_summaries = compute_segment_summaries(candidate_splits, indicators)
    logger.info(f"[{stock_code}] 候选切割点: {len(candidate_splits)}个, 区间: {len(segment_summaries)}段")

    # ── 第二层：DeepSeek情形识别 ──────────────────────────────────────────────
    logger.info(f"[{stock_code}] 调用DeepSeek进行情形识别...")
    result = run_stage_identification(candidate_splits, segment_summaries, indicators)

    if result.get("error"):
        return {"ok": False, "error": result["error"]}

    stages = result.get("stages", [])
    current_stage = result.get("current_stage", {})
    predictions = result.get("predictions", [])

    # ── 第三层：后验校验 ──────────────────────────────────────────────────────
    logger.info(f"[{stock_code}] 后验校验 {len(stages)} 个阶段...")
    validated_stages = validate_stages(stages, indicators)

    # 重试置信度不足的段（最多1次）
    failed = [s for s in validated_stages if s.get("needs_retry")]
    if failed:
        logger.info(f"[{stock_code}] {len(failed)} 个阶段置信度不足，尝试重分析...")
        retry_prompt = build_retry_prompt(failed, indicators)
        # 简单重试：把失败段的信息加入提示，重新调用
        retry_result = _retry_failed_stages(failed, retry_prompt, indicators)
        if retry_result:
            # 用重试结果替换失败段
            failed_dates = {(s["start_date"], s["end_date"]) for s in failed}
            validated_stages = [
                s for s in validated_stages
                if (s["start_date"], s["end_date"]) not in failed_dates
            ] + retry_result
            validated_stages.sort(key=lambda x: x["start_date"])

    # ── 存储结果 ──────────────────────────────────────────────────────────────
    today = date.today().isoformat()
    try:
        execute_insert(
            """INSERT INTO chart_analysis
               (stock_code, analysis_date, stages_json, current_stage_json, predictions_json)
               VALUES (%s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
               stages_json=VALUES(stages_json),
               current_stage_json=VALUES(current_stage_json),
               predictions_json=VALUES(predictions_json),
               created_at=NOW()""",
            [
                stock_code, today,
                json.dumps(validated_stages, ensure_ascii=False, default=str),
                json.dumps(current_stage, ensure_ascii=False, default=str),
                json.dumps(predictions, ensure_ascii=False, default=str),
            ],
        )
        logger.info(f"[{stock_code}] 分析结果已存储")
    except Exception as e:
        logger.error(f"[{stock_code}] 存储失败: {e}")

    return {
        "ok": True,
        "stages": validated_stages,
        "current_stage": current_stage,
        "predictions": predictions,
        "analysis_date": today,
    }


def _retry_failed_stages(failed_stages: list, retry_prompt: str, indicators: dict) -> list:
    """对失败段进行简单重试"""
    try:
        from analysis.kline_deepseek import run_stage_identification, _get_api_key
        from analysis.kline_presegment import CandidateSplit
        import openai

        api_key = _get_api_key()
        if not api_key:
            return []

        client = openai.OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com/v1",
            timeout=120,
        )

        # 构建简化的重试请求
        from analysis.situation_constants import SITUATION_NAMES
        sit_list = "\n".join(f"  {k}: {v}" for k, v in SITUATION_NAMES.items())

        messages = [
            {"role": "system", "content": f"你是股票技术分析专家。\n\n17情形:\n{sit_list}"},
            {"role": "user", "content": retry_prompt + "\n\n请直接输出修正后的JSON数组，格式：[{\"start_date\":\"...\",\"end_date\":\"...\",\"situation_id\":1,\"name\":\"...\",\"summary\":\"...\",\"confidence\":0.7}]"},
        ]

        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=messages,
            max_tokens=2048,
        )
        content = resp.choices[0].message.content or ""
        text = content.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        result = json.loads(text.strip())
        if isinstance(result, list):
            from analysis.kline_validator import validate_stages
            return validate_stages(result, indicators)
    except Exception as e:
        logger.error(f"重试失败: {e}")
    return []


def get_latest_analysis(stock_code: str) -> Optional[dict]:
    """从DB获取最新分析结果"""
    from utils.db_utils import execute_query
    rows = execute_query(
        """SELECT * FROM chart_analysis WHERE stock_code=%s
           ORDER BY analysis_date DESC LIMIT 1""",
        [stock_code],
    )
    if not rows:
        return None
    r = dict(rows[0])
    return {
        "analysis_date": r.get("analysis_date"),
        "stages": json.loads(r["stages_json"]) if r.get("stages_json") else [],
        "current_stage": json.loads(r["current_stage_json"]) if r.get("current_stage_json") else {},
        "predictions": json.loads(r["predictions_json"]) if r.get("predictions_json") else [],
        "created_at": r.get("created_at"),
    }

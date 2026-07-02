"""编排入口 — 串联三层分析流程"""
import json
import logging
import re
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)


def run_full_analysis(stock_code: str, days: int = 180,
                      start_date: str = None, end_date: str = None) -> dict:
    """
    完整流程：指标计算 → 预分割 → DeepSeek识别 → 后验校验 → (重试) → 存储 → 自动复盘

    Args:
        stock_code: 股票代码
        days: 天数模式时取最近N天（默认180）
        start_date: 日期范围模式起始日 YYYY-MM-DD
        end_date: 日期范围模式结束日 YYYY-MM-DD

    Returns:
        {"ok": True, "stages": [...], "current_stage": {...}, "predictions": [...], "review": {...}}
        或 {"ok": False, "error": "..."}
    """
    from analysis.kline_indicators import compute_all_indicators
    from analysis.kline_presegment import find_candidate_splits, compute_segment_summaries
    from analysis.kline_deepseek import run_stage_identification
    from analysis.kline_validator import validate_stages, build_retry_prompt
    from utils.db_utils import execute_query, execute_insert

    # ── 第一层：指标计算 ──────────────────────────────────────────────────────
    logger.info(f"[{stock_code}] 开始K线阶段分析，计算技术指标...")
    indicators = compute_all_indicators(stock_code, days=days,
                                        start_date=start_date, end_date=end_date)
    if not indicators or not indicators.get("dates"):
        return {"ok": False, "error": "无K线数据"}

    # ── 第一层：候选切割点 ────────────────────────────────────────────────────
    logger.info(f"[{stock_code}] 生成候选切割点...")
    candidate_splits = find_candidate_splits(indicators)
    segment_summaries = compute_segment_summaries(candidate_splits, indicators)
    logger.info(f"[{stock_code}] 候选切割点: {len(candidate_splits)}个, 区间: {len(segment_summaries)}段")

    # ── 检索历史复盘教训 ──────────────────────────────────────────────────────
    review_lessons = _get_lessons_for_prompt(stock_code)

    # ── 第二层：DeepSeek情形识别 ──────────────────────────────────────────────
    logger.info(f"[{stock_code}] 调用DeepSeek进行情形识别...")
    result = run_stage_identification(candidate_splits, segment_summaries, indicators,
                                      review_lessons=review_lessons)

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
        retry_result = _retry_failed_stages(failed, retry_prompt, indicators)
        if retry_result:
            failed_dates = {(s["start_date"], s["end_date"]) for s in failed}
            validated_stages = [
                s for s in validated_stages
                if (s["start_date"], s["end_date"]) not in failed_dates
            ] + retry_result
            validated_stages.sort(key=lambda x: x["start_date"])

    # ── 存储结果 ──────────────────────────────────────────────────────────────
    today = date.today().isoformat()
    # 确定实际分析的日期范围
    actual_start = start_date or (str(indicators["dates"][0]) if indicators["dates"] else None)
    actual_end = end_date or (str(indicators["dates"][-1]) if indicators["dates"] else None)

    new_analysis_id = None
    try:
        new_analysis_id = execute_insert(
            """INSERT INTO chart_analysis
               (stock_code, analysis_date, start_date_range, end_date_range,
                stages_json, current_stage_json, predictions_json)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
               start_date_range=VALUES(start_date_range),
               end_date_range=VALUES(end_date_range),
               stages_json=VALUES(stages_json),
               current_stage_json=VALUES(current_stage_json),
               predictions_json=VALUES(predictions_json),
               created_at=NOW()""",
            [
                stock_code, today, actual_start, actual_end,
                json.dumps(validated_stages, ensure_ascii=False, default=str),
                json.dumps(current_stage, ensure_ascii=False, default=str),
                json.dumps(predictions, ensure_ascii=False, default=str),
            ],
        )
        logger.info(f"[{stock_code}] 分析结果已存储 id={new_analysis_id}")
    except Exception as e:
        logger.error(f"[{stock_code}] 存储失败: {e}")

    # ── 自动复盘 ──────────────────────────────────────────────────────────────
    review = None
    if new_analysis_id:
        try:
            review = _run_review(stock_code, new_analysis_id, validated_stages)
        except Exception as e:
            logger.error(f"[{stock_code}] 自动复盘失败: {e}")

    # ── 注册预测监控 ──────────────────────────────────────────────────────────
    if new_analysis_id and predictions:
        try:
            _register_prediction_monitors(stock_code, new_analysis_id, predictions, indicators)
        except Exception as e:
            logger.error(f"[{stock_code}] 注册预测监控失败: {e}")

    return {
        "ok": True,
        "stages": validated_stages,
        "current_stage": current_stage,
        "predictions": predictions,
        "analysis_date": today,
        "review": review,
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
            model="deepseek-v4-pro",
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
        "start_date_range": str(r["start_date_range"]) if r.get("start_date_range") else None,
        "end_date_range": str(r["end_date_range"]) if r.get("end_date_range") else None,
        "stages": json.loads(r["stages_json"]) if r.get("stages_json") else [],
        "current_stage": json.loads(r["current_stage_json"]) if r.get("current_stage_json") else {},
        "predictions": json.loads(r["predictions_json"]) if r.get("predictions_json") else [],
        "created_at": r.get("created_at"),
    }


def get_review_history(stock_code: str, limit: int = 10) -> list:
    """获取历史复盘记录"""
    from utils.db_utils import execute_query
    rows = execute_query(
        """SELECT review_verdict, review_report, lessons_learned, reviewed_at
           FROM chart_analysis_reviews
           WHERE stock_code=%s
           ORDER BY reviewed_at DESC LIMIT %s""",
        [stock_code, limit],
    )
    return [dict(r) for r in rows] if rows else []


def get_review_history_full(stock_code: str, limit: int = 10) -> list:
    """获取完整复盘记录，含预测对比结构"""
    from utils.db_utils import execute_query
    rows = execute_query(
        """SELECT r.review_verdict, r.review_report, r.lessons_learned, r.reviewed_at,
                  r.previous_predictions_json, r.actual_stages_json,
                  p.analysis_date AS prev_analysis_date,
                  p.current_stage_json AS prev_current_stage_json
           FROM chart_analysis_reviews r
           LEFT JOIN chart_analysis p ON p.id = r.previous_analysis_id
           WHERE r.stock_code=%s
           ORDER BY r.reviewed_at DESC LIMIT %s""",
        [stock_code, limit],
    )
    if not rows:
        return []

    results = []
    for r in rows:
        row = dict(r)
        # 解析上次预测
        prev_preds = []
        if row.get("previous_predictions_json"):
            try:
                prev_preds = json.loads(row["previous_predictions_json"])
            except:
                pass
        # 解析实际发生的阶段
        actual_stages = []
        if row.get("actual_stages_json"):
            try:
                actual_stages = json.loads(row["actual_stages_json"])
            except:
                pass
        # 解析上次分析时的当前阶段
        prev_stage = {}
        if row.get("prev_current_stage_json"):
            try:
                prev_stage = json.loads(row["prev_current_stage_json"])
            except:
                pass

        results.append({
            "reviewed_at": str(row["reviewed_at"])[:10] if row.get("reviewed_at") else "",
            "prev_analysis_date": str(row["prev_analysis_date"]) if row.get("prev_analysis_date") else "",
            "verdict": row.get("review_verdict", ""),
            "prev_stage": {
                "situation_id": prev_stage.get("situation_id"),
                "name": prev_stage.get("name", ""),
                "summary": prev_stage.get("summary", ""),
            },
            "predictions": prev_preds,
            "actual": [{
                "situation_id": s.get("situation_id"),
                "name": s.get("name", ""),
                "start_date": s.get("start_date", ""),
                "end_date": s.get("end_date", ""),
                "summary": s.get("summary", ""),
            } for s in actual_stages[:3]],
            "explanation": row.get("review_report", ""),
            "lessons": row.get("lessons_learned", ""),
        })
    return results


# ── 复盘相关函数 ───────────────────────────────────────────────────────────

def _get_lessons_for_prompt(stock_code: str, max_chars: int = 500) -> str:
    """聚合最近5次复盘的教训，拼接为注入文本"""
    from utils.db_utils import execute_query
    rows = execute_query(
        """SELECT lessons_learned, review_verdict, reviewed_at
           FROM chart_analysis_reviews
           WHERE stock_code=%s AND lessons_learned IS NOT NULL AND lessons_learned != ''
           ORDER BY reviewed_at DESC LIMIT 5""",
        [stock_code],
    )
    if not rows:
        return ""

    verdict_map = {"hit": "预测命中", "partial": "部分命中", "miss": "预测失误"}
    lines = []
    for r in rows:
        label = verdict_map.get(r["review_verdict"], "")
        dt = str(r["reviewed_at"])[:10] if r.get("reviewed_at") else ""
        lines.append(f"- [{dt}][{label}] {r['lessons_learned']}")

    text = "\n".join(lines)
    return text[:max_chars] if len(text) > max_chars else text


def _run_review(stock_code: str, current_analysis_id: int, new_stages: list) -> Optional[dict]:
    """自动复盘：对比上次 predictions 与本次实际 stages"""
    from utils.db_utils import execute_query, execute_insert
    from analysis.kline_deepseek import _get_api_key

    # 1. 取上一次分析记录（排除本次）
    prev = execute_query(
        """SELECT id, predictions_json, analysis_date, end_date_range
           FROM chart_analysis
           WHERE stock_code=%s AND id != %s
           ORDER BY analysis_date DESC LIMIT 1""",
        [stock_code, current_analysis_id],
    )
    if not prev or not prev[0].get("predictions_json"):
        logger.info(f"[{stock_code}] 无上次分析记录，跳过复盘")
        return None

    prev_row = dict(prev[0])
    prev_predictions = json.loads(prev_row["predictions_json"])
    if not prev_predictions:
        return None

    # 2. 提取上次分析之后发生的实际阶段
    prev_date = str(prev_row["analysis_date"])
    actual_stages = _extract_post_stages(new_stages, prev_date)
    if not actual_stages:
        logger.info(f"[{stock_code}] 本次分析无上次之后的阶段，跳过复盘")
        return None

    # 3. 匹配判定
    match_results = _match_predictions_to_actual(prev_predictions, actual_stages)
    # 判定逻辑：最高概率预测命中 = top_hit，其他预测命中 = partial，全部未中 = miss
    top_pred_hit = False
    if match_results and match_results[0].get("match_result") in ("exact_hit", "phase_hit"):
        top_pred_hit = True
    has_any_hit = any(m["match_result"] in ("exact_hit", "phase_hit") for m in match_results)
    if top_pred_hit:
        verdict = "top_hit"
    elif has_any_hit:
        verdict = "partial"
    else:
        verdict = "miss"

    # 4. AI生成复盘报告
    review = _generate_review_report(stock_code, prev_predictions, actual_stages, match_results)
    report = review.get("report", "")
    lessons = review.get("lessons", "")

    # 5. 存储
    try:
        execute_insert(
            """INSERT INTO chart_analysis_reviews
               (stock_code, current_analysis_id, previous_analysis_id,
                previous_predictions_json, actual_stages_json,
                review_verdict, review_report, lessons_learned)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            [
                stock_code, current_analysis_id, prev_row["id"],
                json.dumps(prev_predictions, ensure_ascii=False),
                json.dumps(actual_stages[:3], ensure_ascii=False, default=str),
                verdict, report, lessons,
            ],
        )
        logger.info(f"[{stock_code}] 复盘记录已存储: verdict={verdict}")
    except Exception as e:
        logger.error(f"[{stock_code}] 复盘存储失败: {e}")

    return {"verdict": verdict, "report": report, "lessons": lessons}


def _extract_post_stages(all_stages: list, prev_analysis_date: str) -> list:
    """从本次 stages 中提取上次分析日期之后开始的阶段"""
    return [s for s in all_stages if str(s.get("start_date", "")) > prev_analysis_date]


def _match_predictions_to_actual(prev_predictions: list, actual_stages: list) -> list:
    """对比 predictions 与实际阶段"""
    from analysis.situation_constants import SITUATION_PHASES

    def _get_phase(sit_id):
        for phase, ids in SITUATION_PHASES.items():
            if sit_id in ids:
                return phase
        return "unknown"

    first_actual = actual_stages[0] if actual_stages else None
    results = []

    for pred in prev_predictions:
        # 新格式直接有situation_id，旧格式从文本提取
        predicted_sit_id = pred.get("situation_id") or _extract_situation_id_from_text(pred.get("scenario", ""))
        match_result = "unknown"

        if first_actual and predicted_sit_id:
            actual_sit_id = first_actual.get("situation_id")
            if actual_sit_id == predicted_sit_id:
                match_result = "exact_hit"
            elif _get_phase(actual_sit_id) == _get_phase(predicted_sit_id):
                match_result = "phase_hit"
            else:
                match_result = "miss"

        results.append({
            "prediction": pred,
            "predicted_situation_id": predicted_sit_id,
            "match_result": match_result,
            "actual_first_stage": first_actual,
        })

    return results


def _extract_situation_id_from_text(text: str) -> Optional[int]:
    """从预测文本中提取情形编号"""
    m = re.search(r'情形(\d+)', text)
    if m:
        return int(m.group(1))
    # 尝试匹配情形名称
    from analysis.situation_constants import SITUATION_NAMES
    for sit_id, name in SITUATION_NAMES.items():
        if name in text:
            return sit_id
    return None


def _register_prediction_monitors(stock_code: str, analysis_id: int,
                                   predictions: list, indicators: dict):
    """在分析完成后，将Top3预测的量化触发条件注册为活跃监控项"""
    from utils.db_utils import execute_insert
    from analysis.kline_trigger_quantifier import quantify_triggers

    # 1. 旧活跃monitors → superseded
    execute_insert(
        "UPDATE prediction_monitors SET status='superseded' WHERE stock_code=%s AND status='active'",
        [stock_code]
    )

    # 2. 对Top3 predictions注册monitors
    top_preds = predictions[:3]
    for pred in top_preds:
        sit_id = pred.get("situation_id")
        if not sit_id:
            continue

        triggers = quantify_triggers(stock_code, indicators, sit_id)
        if not triggers:
            continue

        # 计算已满足的条件数
        satisfied = sum(1 for t in triggers if t.get("satisfied"))
        total = len(triggers)

        execute_insert(
            """INSERT INTO prediction_monitors
               (stock_code, analysis_id, situation_id, scenario_name,
                probability, trigger_logic, triggers_json,
                status, satisfied_count, total_count)
               VALUES (%s, %s, %s, %s, %s, %s, %s, 'active', %s, %s)""",
            [
                stock_code, analysis_id, sit_id,
                pred.get("scenario", ""),
                pred.get("probability", 0),
                "priority_1_all",
                json.dumps(triggers, ensure_ascii=False),
                satisfied, total,
            ],
        )

    logger.info(f"[{stock_code}] 已注册 {len(top_preds)} 个预测监控项")


def _generate_review_report(stock_code: str, prev_predictions: list,
                            actual_stages: list, match_results: list) -> dict:
    """调用 DeepSeek 生成复盘报告"""
    from analysis.kline_deepseek import _get_api_key
    import openai

    api_key = _get_api_key()
    if not api_key:
        return {"report": "无API Key，跳过AI复盘", "lessons": ""}

    prompt = f"""请对以下股票阶段预测进行复盘分析：

## 上次预测（Top3场景）
{json.dumps(prev_predictions, ensure_ascii=False, indent=2)}

## 实际发生的阶段
{json.dumps(actual_stages[:3], ensure_ascii=False, indent=2, default=str)}

## 匹配结果
{json.dumps([{"scenario": m["prediction"].get("scenario"),"predicted_id": m["predicted_situation_id"],"result": m["match_result"]} for m in match_results], ensure_ascii=False, indent=2)}

请输出：
1. 复盘报告（200字以内，分析预测对错原因）
2. 教训总结（100字以内，简明扼要，供后续分析参考）

输出JSON格式：
{{"report":"复盘报告内容","lessons":"教训总结"}}"""

    try:
        client = openai.OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com/v1",
            timeout=60,
        )
        resp = client.chat.completions.create(
            model="deepseek-v4-pro",
            messages=[
                {"role": "system", "content": "你是股票技术分析复盘专家。只输出JSON，不要有其他文字。"},
                {"role": "user", "content": prompt},
            ],
            max_tokens=1024,
        )
        content = resp.choices[0].message.content or "{}"
        # 提取JSON
        text = content.strip()
        if "```" in text:
            parts = text.split("```")
            for part in parts:
                p = part.strip()
                if p.startswith("json"):
                    p = p[4:].strip()
                if p.startswith("{"):
                    text = p
                    break
        brace_idx = text.find("{")
        if brace_idx > 0:
            text = text[brace_idx:]
        result = json.loads(text.strip())
        return {
            "report": result.get("report", ""),
            "lessons": result.get("lessons", ""),
        }
    except Exception as e:
        logger.error(f"[{stock_code}] AI复盘报告生成失败: {e}")
        return {"report": f"复盘报告生成失败: {e}", "lessons": ""}

"""Pipeline D — 行业指标结构化抽取

研报/行业分析类：精细模式（deepseek-reasoner，通过 cleaning_deep stage）
其他类型：轻量模式（由 Pipeline A prompt 顺带，不在此文件）
"""
import json
import logging
from typing import Optional

from utils.db_utils import execute_cloud_query, upsert_industry_indicator

logger = logging.getLogger(__name__)

# ── Prompt ────────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """你是行业数据提取专家。从研报/行业分析文章中提取所有行业量化指标。

规则：
1. 只提取有明确数值的指标，禁止推测或捏造
2. metric_type 必须从枚举中选：growth_rate / output / price / market_size / penetration / capacity / inventory
3. 必须识别分子/分母定义（如同比=当期÷上年同期-1，环比=当期÷上期-1）
4. period_label 必须精确到粒度（写 2024Q3 而非"今年三季度"）
5. period_end_date：year→12-31，half→H1=06-30/H2=12-31，quarter→Q1=03-31/Q2=06-30/Q3=09-30/Q4=12-31，month→该月最后一天
6. forecast_target_label 只在 data_type=forecast 时填，填预测目标期（如 2025E）
7. forecast_target_date：同 period_end_date 规则推算
8. confidence：有明确出处=high；行业共识/研报预测=medium；模糊表述=low
9. source_snippet：原文中包含该数值的句子，不超过60字
10. 同一数值有歧义时，填 confidence=low 并在 metric_definition 中说明

输出严格 JSON（不要 markdown 代码块）：
{
  "indicators": [
    {
      "industry_l1": "有色金属",
      "industry_l2": "工业金属",
      "industry_l3": null,
      "metric_type": "growth_rate",
      "metric_name": "动力电池出货量同比增速",
      "metric_definition": "当月国内动力电池出货量÷上年同月出货量-1，来源：中国汽车动力电池产业创新联盟",
      "metric_numerator": "当月动力电池出货量(GWh)",
      "metric_denominator": "上年同月动力电池出货量(GWh)",
      "value": 25.3,
      "value_raw": "同比增长约25%",
      "period_type": "month",
      "period_label": "2024-09",
      "period_year": 2024,
      "period_end_date": "2024-09-30",
      "data_type": "actual",
      "confidence": "high",
      "source_snippet": "2024年9月动力电池出货量同比增长约25%，创年内新高"
    }
  ]
}"""

# ── 行业名 → 申万一级映射（快速兜底）────────────────────────────────────────

_L2_TO_L1 = {
    "动力电池": "电力设备", "锂电池": "电力设备", "储能电池": "电力设备",
    "工业金属": "有色金属", "铜": "有色金属", "铝": "有色金属", "锌": "有色金属",
    "新能源汽车": "汽车", "乘用车": "汽车",
    "光伏": "电力设备", "风电": "电力设备", "特高压": "电力设备",
    "钢铁": "钢铁", "特种钢": "钢铁",
    "半导体": "电子", "芯片": "电子",
}


def _guess_l1(l2: str) -> str:
    for key, l1 in _L2_TO_L1.items():
        if key in l2:
            return l1
    return "其他"


# ── LLM 调用（走 model_router cleaning_deep stage）────────────────────────────

def _call_llm(full_text: str) -> str:
    """调用 deepseek-reasoner（cleaning_deep stage）提取行业指标。

    降级链：cleaning_deep → cleaning → 直接 DeepSeek chat fallback
    """
    # 截断保护：reasoner 输入不超过 30000 字符
    text = full_text[:30000] if len(full_text) > 30000 else full_text
    user_message = f"请从以下文章中提取行业量化指标：\n\n{text}"

    try:
        from utils.model_router import call_model
        return call_model(
            "cleaning_deep",
            _SYSTEM_PROMPT,
            user_message,
            max_tokens=8000,
            timeout=300,
            retries=2,
        )
    except Exception as e:
        logger.warning(f"[Pipeline D] cleaning_deep 调用失败: {e}，降级到 cleaning stage")
        try:
            from utils.model_router import call_model
            return call_model(
                "cleaning",
                _SYSTEM_PROMPT,
                user_message,
                max_tokens=4096,
                timeout=120,
                retries=2,
            )
        except Exception as e2:
            logger.warning(f"[Pipeline D] cleaning 调用失败: {e2}，降级到 DeepSeek chat 直连")
            return _call_ds_fallback(user_message)


def _call_ds_fallback(user_message: str) -> str:
    """最终降级：直接调 DeepSeek chat（openai 兼容接口）"""
    try:
        import openai
    except ImportError:
        logger.error("[Pipeline D] openai 包未安装，无法降级")
        return ""

    try:
        from utils.db_utils import execute_query
        rows = execute_query(
            "SELECT value FROM system_config WHERE config_key='deepseek_api_key'",
            [],
        )
        api_key = rows[0]["value"] if rows else ""
    except Exception:
        api_key = ""

    if not api_key:
        import os
        api_key = os.environ.get("DEEPSEEK_API_KEY", "")

    if not api_key:
        logger.error("[Pipeline D] 未找到 deepseek_api_key，降级失败")
        return ""

    try:
        client = openai.OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com/v1",
            timeout=120,
            max_retries=1,
        )
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            max_tokens=4096,
        )
        return resp.choices[0].message.content or ""
    except Exception as e:
        logger.error(f"[Pipeline D] DeepSeek fallback 失败: {e}")
        return ""


# ── 解析与归一化 ──────────────────────────────────────────────────────────────

def _parse_indicators(raw: str) -> list:
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        data = json.loads(raw.strip())
        return data.get("indicators", []) if isinstance(data, dict) else []
    except Exception:
        return []


def _normalize_indicator(ind: dict, source_doc_id: int, publish_date_str: str) -> dict:
    """归一化单条指标，填充缺省值"""
    row = {}

    l1 = (ind.get("industry_l1") or "").strip()
    l2 = (ind.get("industry_l2") or "").strip()
    l3 = (ind.get("industry_l3") or None)
    row["industry_l1"] = l1 or _guess_l1(l2)
    row["industry_l2"] = l2
    row["industry_l3"] = l3 or None

    row["metric_type"] = (ind.get("metric_type") or "").strip()
    row["metric_name"] = (ind.get("metric_name") or "").strip()
    row["metric_definition"] = ind.get("metric_definition") or None
    row["metric_numerator"] = ind.get("metric_numerator") or None
    row["metric_denominator"] = ind.get("metric_denominator") or None

    try:
        row["value"] = float(ind["value"]) if ind.get("value") is not None else None
    except (TypeError, ValueError):
        row["value"] = None
    row["value_raw"] = ind.get("value_raw") or None

    row["period_type"] = ind.get("period_type") or None
    row["period_label"] = ind.get("period_label") or None
    row["period_year"] = ind.get("period_year") or None
    row["period_end_date"] = ind.get("period_end_date") or None

    try:
        row["publish_date"] = str(publish_date_str)[:10] if publish_date_str else None
    except Exception:
        row["publish_date"] = None

    row["forecast_target_label"] = ind.get("forecast_target_label") or None
    row["forecast_target_date"] = ind.get("forecast_target_date") or None

    row["data_type"] = ind.get("data_type") or "actual"
    row["confidence"] = ind.get("confidence") or "medium"
    row["source_type"] = "pipeline_d"
    row["source_doc_id"] = source_doc_id
    row["source_snippet"] = ind.get("source_snippet") or None

    return row


# ── 主入口 ────────────────────────────────────────────────────────────────────

def run_pipeline_d(extracted_text_id: int, full_text: str) -> int:
    """主入口：精细模式，用于研报/行业分析类文档。

    Returns: 写入的指标条数
    """
    try:
        meta = execute_cloud_query(
            """SELECT et.publish_time, sd.id as doc_id
               FROM extracted_texts et
               LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
               WHERE et.id = %s""",
            [extracted_text_id],
        )
        publish_date_str = (meta[0].get("publish_time") or "") if meta else ""
        source_doc_id = meta[0].get("doc_id") or extracted_text_id if meta else extracted_text_id
    except Exception as e:
        logger.warning(f"[Pipeline D] 获取元数据失败: {e}")
        publish_date_str = ""
        source_doc_id = extracted_text_id

    # Call LLM — cleaning_deep stage (deepseek-reasoner)
    raw = _call_llm(full_text)
    if not raw:
        return 0

    indicators = _parse_indicators(raw)
    if not indicators:
        logger.info(f"[Pipeline D] id={extracted_text_id} 未抽取到指标")
        return 0

    count = 0
    for ind in indicators:
        if not ind.get("industry_l2") or not ind.get("metric_name"):
            continue
        try:
            row = _normalize_indicator(ind, source_doc_id, publish_date_str)
            upsert_industry_indicator(row)
            count += 1
        except Exception as e:
            logger.warning(f"[Pipeline D] 写入失败: {e} | {ind.get('metric_name')}")

    logger.info(f"[Pipeline D] id={extracted_text_id} 写入 {count} 条指标")
    return count

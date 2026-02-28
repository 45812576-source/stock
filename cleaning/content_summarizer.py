"""Pipeline A — 分族摘要处理器

按 doc_type 分 4 族，每族使用不同的 prompt：
  族1 (structured): announcement/financial_report/data_release/policy_doc/xlsx_data
  族2 (analysis):   research_report/strategy_report/feature_news/roadshow_notes
  族3 (informal):   social_post/chat_record
  族4 (brief):      flash_news/market_commentary/digest_news拆条/other

digest_news 先拆条，每条独立走对应族处理。

存储：所有结果写 content_summaries（通用层），族特有字段写 type_fields JSON 列。
      不再依赖 4 张 detail 表（summary_structured/analysis/informal/brief）。

doc_type 解析优先级：
  1. source_documents.doc_type（入库时关键词初分类）
  2. classify_doc_type() 关键词匹配（doc_type=other 时）
  3. AI 兜底分类（关键词无命中时）
"""
import json
import logging
from typing import Optional

from config.doc_types import FAMILY_MAP, classify_doc_type
from cleaning.summary_prompts import (
    DOC_TYPE_CLASSIFY_PROMPT,
    DIGEST_SPLIT_PROMPT,
    get_summary_prompt,
)
from utils.db_utils import execute_cloud_query, execute_cloud_insert, sync_summary_to_local

logger = logging.getLogger(__name__)


# ── 模型调用（走 model_router）────────────────────────────────────────────────
# 族2（深度FOE）用 cleaning_deep stage → deepseek-reasoner
# 其他族用 cleaning stage → deepseek-chat

def _call_model(system_prompt: str, text: str, family: int, max_tokens: int = 2048) -> str:
    """调用模型，族2用 cleaning_deep（reasoner），其他用 cleaning（chat）"""
    stage = "cleaning_deep" if family == 2 else "cleaning"
    try:
        from utils.model_router import call_model
        return call_model(stage, system_prompt, text, max_tokens=max_tokens, timeout=120)
    except Exception as e:
        logger.warning(f"model_router 调用失败 stage={stage}: {e}，降级 DeepSeek chat")
        return _call_ds_fallback(system_prompt, text, max_tokens)


def _call_ds_fallback(system_prompt: str, text: str, max_tokens: int = 2048) -> str:
    """降级：直接调 DeepSeek chat"""
    from openai import OpenAI
    rows = execute_cloud_query("SELECT value FROM system_config WHERE config_key='deepseek_api_key'")
    if not rows:
        raise RuntimeError("system_config 中未找到 deepseek_api_key")
    client = OpenAI(api_key=rows[0]["value"], base_url="https://api.deepseek.com/v1")
    resp = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text},
        ],
        max_tokens=max_tokens,
        temperature=0.3,
        timeout=90,
    )
    return resp.choices[0].message.content or ""


def _parse_json(raw: str):
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        return json.loads(raw.strip())
    except Exception:
        return None


# ── doc_type 解析（前置，三级优先级）────────────────────────────────────────

def _resolve_doc_type(extracted_text_id: int, text: str) -> str:
    """从 source_documents 读已有 doc_type；仅 other 时才升级到关键词/AI"""
    rows = execute_cloud_query(
        """SELECT sd.doc_type, sd.title
           FROM extracted_texts et
           LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
           WHERE et.id = %s""",
        [extracted_text_id],
    )
    if rows:
        sd_doc_type = rows[0].get("doc_type") or "other"
        sd_title = rows[0].get("title") or ""
        # 级别1：source_documents 已有非 other 的 doc_type，直接用
        if sd_doc_type and sd_doc_type != "other":
            return sd_doc_type
        # 级别2：关键词分类
        kw_type = classify_doc_type(sd_title, text[:200])
        if kw_type != "other":
            return kw_type

    # 级别3：AI 兜底（成本最高，仅最终兜底）
    return _classify_doc_type_ai(text)


def _classify_doc_type_ai(text: str) -> str:
    """AI 判断 doc_type（兜底）"""
    try:
        raw = _call_model(DOC_TYPE_CLASSIFY_PROMPT, text[:3000], family=4, max_tokens=100)
        result = _parse_json(raw)
        if isinstance(result, dict) and result.get("doc_type"):
            dt = result["doc_type"]
            if dt in FAMILY_MAP:
                return dt
    except Exception as e:
        logger.warning(f"AI 分类 doc_type 失败: {e}")
    return "other"


# ── digest_news 拆条 ──────────────────────────────────────────────────────────

def _split_digest(text: str) -> list[dict]:
    """将拼盘快讯拆成独立条目"""
    try:
        raw = _call_model(DIGEST_SPLIT_PROMPT, text[:8000], family=4, max_tokens=3000)
        items = _parse_json(raw)
        if isinstance(items, list) and items:
            return [i for i in items if isinstance(i, dict) and i.get("text")]
    except Exception as e:
        logger.warning(f"digest 拆条失败: {e}")
    return [{"title": "", "text": text}]


# ── 单条处理：写 content_summaries（type_fields 存族特有字段）────────────────

def _process_one(
    extracted_text_id: int,
    text: str,
    doc_type: str,
) -> Optional[int]:
    """对单条文本（已知 doc_type）走分族摘要，写入 content_summaries

    Returns: content_summaries.id，失败返回 None
    """
    prompt, family = get_summary_prompt(doc_type)

    try:
        max_tok = 3000 if family == 2 else 2048
        raw = _call_model(prompt, text, family=family, max_tokens=max_tok)
        result = _parse_json(raw)
    except Exception as e:
        logger.error(f"[A] 模型调用失败 doc_type={doc_type} id={extracted_text_id}: {e}")
        return None

    if not isinstance(result, dict):
        logger.warning(f"[A] 结果格式异常 id={extracted_text_id}: {raw[:200]}")
        return None

    summary_text = result.get("summary", "")
    if not summary_text:
        logger.warning(f"[A] summary 为空 id={extracted_text_id}")
        return None

    fact_summary = _str(result.get("fact_summary") or _flatten_facts(result))
    opinion_summary = _str(result.get("opinion_summary") or _flatten_opinions(result))
    evidence_assessment = _str(result.get("evidence_assessment"))
    info_gaps = _str(result.get("info_gaps"))

    # 族特有字段存 type_fields JSON（不再写 detail 表）
    type_fields = _extract_type_fields(result, family)

    cs_id = execute_cloud_insert(
        """INSERT INTO content_summaries
           (extracted_text_id, doc_type, summary, fact_summary, opinion_summary,
            evidence_assessment, info_gaps, family, type_fields)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        [
            extracted_text_id,
            doc_type,
            _str(summary_text),
            fact_summary,
            opinion_summary,
            evidence_assessment,
            info_gaps,
            family,
            json.dumps(type_fields, ensure_ascii=False) if type_fields else None,
        ],
    )
    return cs_id


def _extract_type_fields(result: dict, family: int) -> dict:
    """提取族特有字段合并为 type_fields dict"""
    tf = {}
    if family == 1:
        for k in ("subject_entities", "key_facts", "key_data", "effective_date", "impact_scope"):
            if result.get(k) is not None:
                tf[k] = result[k]
    elif family == 2:
        for k in ("key_arguments", "type_fields"):
            v = result.get(k)
            if v is not None:
                # 族2 type_fields 已经是 dict，直接合并展开
                if k == "type_fields" and isinstance(v, dict):
                    tf.update(v)
                else:
                    tf[k] = v
    elif family == 3:
        for k in ("speaker", "speaker_type", "key_claims", "opinions", "sentiment"):
            if result.get(k) is not None:
                tf[k] = result[k]
    else:  # family 4
        for k in ("event_what", "event_who", "impact_target", "sentiment"):
            if result.get(k) is not None:
                tf[k] = result[k]
    return tf


def _str(val, default="") -> str:
    if val is None:
        return default
    if isinstance(val, (list, dict)):
        return json.dumps(val, ensure_ascii=False)
    return str(val)


def _flatten_facts(r: dict) -> str:
    parts = []
    if r.get("subject_entities"):
        parts.append(f"主体：{r['subject_entities']}")
    if r.get("key_facts"):
        kf = r["key_facts"]
        if isinstance(kf, dict):
            for k, v in kf.items():
                if v and k not in ("segments",):
                    parts.append(f"{k}：{v}")
        elif isinstance(kf, list):
            parts.extend(str(i) for i in kf)
    if r.get("event_what"):
        parts.append(r["event_what"])
    return "\n".join(parts)


def _flatten_opinions(r: dict) -> str:
    parts = []
    for op in (r.get("opinions") or []):
        if isinstance(op, dict) and op.get("opinion"):
            parts.append(op["opinion"])
    tf = r.get("type_fields") or {}
    if isinstance(tf, dict):
        if tf.get("management_guidance"):
            parts.extend(tf["management_guidance"])
        if tf.get("market_view"):
            parts.append(tf["market_view"])
    return "\n".join(parts)


# ── 主入口 ────────────────────────────────────────────────────────────────────

def summarize_single(extracted_text_id: int) -> Optional[int]:
    """对单条 extracted_text 生成分族摘要

    流程：
      1. 读取文本
      2. 解析 doc_type（source_doc → 关键词 → AI 三级）
      3. digest_news → 拆条 → 每条独立处理
      4. 其余 → 直接按族处理
      5. 标记 summary_status='done'，同步本地

    Returns: content_summaries.id（digest 返回第一条），失败返回 None
    """
    rows = execute_cloud_query(
        "SELECT id, full_text FROM extracted_texts WHERE id=%s",
        [extracted_text_id],
    )
    if not rows:
        return None

    full_text = rows[0]["full_text"] or ""
    if not full_text.strip():
        return None

    if len(full_text) > 12000:
        full_text = full_text[:12000] + "\n\n[文本已截断]"

    doc_type = _resolve_doc_type(extracted_text_id, full_text)
    logger.info(f"[A] id={extracted_text_id} doc_type={doc_type}")

    if doc_type == "digest_news":
        items = _split_digest(full_text)
        logger.info(f"[A] digest 拆出 {len(items)} 条 id={extracted_text_id}")
        first_cs_id = None
        inserted_cs_ids = []
        for item in items:
            item_text = item.get("text", "")
            if not item_text.strip():
                continue
            item_doc_type = classify_doc_type("", item_text[:200]) if len(item_text) > 100 else "flash_news"
            if item_doc_type == "other":
                item_doc_type = "flash_news"
            cs_id = _process_one(extracted_text_id, item_text, item_doc_type)
            if cs_id:
                if first_cs_id is None:
                    first_cs_id = cs_id
                inserted_cs_ids.append(cs_id)
        _finalize(extracted_text_id, inserted_cs_ids)
        return first_cs_id

    cs_id = _process_one(extracted_text_id, full_text, doc_type)
    _finalize(extracted_text_id, [cs_id] if cs_id else [])
    return cs_id


def _finalize(extracted_text_id: int, cs_ids: list):
    execute_cloud_insert(
        "UPDATE extracted_texts SET summary_status='done' WHERE id=%s",
        [extracted_text_id],
    )
    for cs_id in cs_ids:
        try:
            sync_summary_to_local(cs_id)
        except Exception as e:
            logger.warning(f"[A] 同步本地失败 cs_id={cs_id}: {e}")

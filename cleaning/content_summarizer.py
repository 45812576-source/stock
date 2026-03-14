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
    SOCIAL_POST_SPLIT_PROMPT,
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


# ── social_post 拆条 ─────────────────────────────────────────────────────────

def _split_social_post(text: str) -> list[dict]:
    """将社媒帖子按主题/公司拆分为独立条目

    Returns:
        [{"topic": str, "topics": list, "text": str, "has_data": bool, "stocks": list}, ...]
    """
    try:
        raw = _call_model(SOCIAL_POST_SPLIT_PROMPT, text[:8000], family=3, max_tokens=8000)
        items = _parse_json(raw)
        # 截断修复：如果 JSON 不完整，尝试修复
        if items is None and raw and raw.strip().startswith("["):
            items = _repair_truncated_json_array(raw)
        if isinstance(items, list) and items:
            valid = []
            for item in items:
                if isinstance(item, dict) and item.get("text"):
                    valid.append(item)
            if valid:
                return valid
    except Exception as e:
        logger.warning(f"social_post 拆条失败: {e}")
    return [{"topic": "", "topics": [], "text": text, "has_data": False, "stocks": []}]


def _repair_truncated_json_array(raw: str) -> Optional[list]:
    """修复被截断的 JSON 数组：尝试截取到最后一个完整的 } 并关闭数组"""
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    # 找最后一个完整的 },
    last_brace = raw.rfind("}")
    if last_brace > 0:
        candidate = raw[:last_brace + 1]
        # 确保以 ] 结束
        if not candidate.strip().endswith("]"):
            candidate = candidate.rstrip().rstrip(",") + "\n]"
        try:
            result = json.loads(candidate)
            if isinstance(result, list):
                logger.info(f"[A] 截断JSON修复成功，恢复 {len(result)} 条")
                return result
        except Exception:
            pass
    return None


# ── 单条处理：写 content_summaries（type_fields 存族特有字段）────────────────

def _process_one(
    extracted_text_id: int,
    text: str,
    doc_type: str,
    publish_time=None,
    split_meta: Optional[dict] = None,
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

    # 拆条 social_post：把拆条元数据（topic/commodities/has_data）存入 type_fields
    if split_meta:
        type_fields["split_topic"] = split_meta.get("topic", "")
        type_fields["split_topics"] = split_meta.get("topics", [])
        type_fields["split_has_data"] = split_meta.get("has_data", False)
        type_fields["split_stocks"] = split_meta.get("stocks", [])

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

    # Pipeline A Lite: 轻量指标抽取（顺带）
    indicators_lite = result.get("indicators") or []
    if indicators_lite:
        try:
            from utils.db_utils import upsert_industry_indicator
            publish_date_str = str(publish_time or "")[:10]
            for ind in indicators_lite:
                if not ind.get("industry_l2") or not ind.get("metric_name"):
                    continue
                lite_row = {
                    "industry_l1": ind.get("industry_l1") or "",
                    "industry_l2": ind.get("industry_l2", ""),
                    "industry_l3": None,
                    "metric_type": ind.get("metric_type") or "growth_rate",
                    "metric_name": ind.get("metric_name", ""),
                    "metric_definition": None,
                    "metric_numerator": None,
                    "metric_denominator": None,
                    "value": float(ind["value"]) if ind.get("value") is not None else None,
                    "value_raw": ind.get("value_raw"),
                    "period_type": None,
                    "period_label": ind.get("period_label"),
                    "period_year": ind.get("period_year"),
                    "period_end_date": None,
                    "publish_date": publish_date_str or None,
                    "forecast_target_label": None,
                    "forecast_target_date": None,
                    "data_type": ind.get("data_type", "actual"),
                    "confidence": ind.get("confidence", "medium"),
                    "source_type": "pipeline_a_lite",
                    "source_doc_id": extracted_text_id,
                    "source_snippet": ind.get("source_snippet"),
                }
                upsert_industry_indicator(lite_row)
        except Exception as e:
            logger.warning(f"[A Lite] 指标写入失败: {e}")

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


# ── social_post 拆条 → daily_intel_stocks 写入 ─────────────────────────────

def _write_daily_intel_stocks(extracted_text_id: int, stocks: list[dict], publish_time):
    """将拆条中提取的 stocks 写入 daily_intel_stocks（复用 daily_intel 表结构）"""
    try:
        from utils.db_utils import execute_query, execute_cloud_insert
        from datetime import date

        # 查 source_documents 标题
        sd_rows = execute_cloud_query(
            """SELECT sd.title FROM extracted_texts et
               LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
               WHERE et.id=%s""",
            [extracted_text_id],
        )
        source_title = sd_rows[0].get("title", "") if sd_rows else ""

        # name→code 映射
        name_code_rows = execute_query(
            "SELECT stock_code, stock_name FROM stock_info"
        )
        name_code_map = {r["stock_name"]: r["stock_code"] for r in (name_code_rows or []) if r.get("stock_name")}

        scan_date = str(publish_time)[:10] if publish_time else str(date.today())

        count = 0
        for s in stocks:
            if not isinstance(s, dict):
                continue
            sname = (s.get("stock_name") or "").strip()
            if not sname:
                continue
            scode = (s.get("stock_code") or "").strip() or name_code_map.get(sname) or None

            execute_cloud_insert(
                """INSERT INTO daily_intel_stocks
                   (scan_date, source_type, source_id, source_title,
                    stock_name, stock_code, industry, business_desc,
                    event_type, event_summary)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                [
                    scan_date, "zsxq", extracted_text_id,
                    (source_title or "")[:500],
                    sname[:50],
                    scode[:20] if scode else None,
                    (s.get("industry") or "")[:100],
                    "",  # business_desc 拆条 prompt 未提取
                    (s.get("event_type") or "")[:50],
                    (s.get("event_summary") or ""),
                ],
            )
            count += 1
        logger.info(f"[A] social_post 写入 daily_intel_stocks {count} 条 id={extracted_text_id}")
        return count
    except Exception as e:
        logger.warning(f"[A] daily_intel_stocks 写入失败 id={extracted_text_id}: {e}")
        return 0


# ── 公共入口：social_post 按需拆条（幂等）──────────────────────────────────────

def ensure_social_post_split(extracted_text_id: int) -> dict:
    """确保 social_post 已完成拆条+摘要+stocks提取。幂等：已done则直接读缓存。

    触发方：daily_intel scanner / deep_researcher / unified_pipeline
    返回:
        {
            "items": [{"topic", "topics", "has_data", "stocks"}, ...],
            "stocks": [{stock_name, stock_code, ...}, ...],  # 所有拆条的 stocks 汇总
            "cs_ids": [int, ...],  # content_summaries.id 列表
            "from_cache": bool,
        }
    """
    # 检查是否已处理过
    et_rows = execute_cloud_query(
        "SELECT id, full_text, publish_time, summary_status FROM extracted_texts WHERE id=%s",
        [extracted_text_id],
    )
    if not et_rows:
        return {"items": [], "stocks": [], "cs_ids": [], "from_cache": False}

    row = et_rows[0]
    full_text = row["full_text"] or ""
    publish_time = row.get("publish_time")

    # 已 done → 从 content_summaries 读缓存
    if row.get("summary_status") == "done":
        return _read_split_cache(extracted_text_id)

    if not full_text.strip():
        return {"items": [], "stocks": [], "cs_ids": [], "from_cache": False}

    if len(full_text) > 12000:
        full_text = full_text[:12000] + "\n\n[文本已截断]"

    # 执行拆条
    items = _split_social_post(full_text)
    logger.info(f"[split] social_post 拆出 {len(items)} 条 id={extracted_text_id}")

    inserted_cs_ids = []
    all_stocks = []
    for item in items:
        item_text = item.get("text", "")
        if not item_text.strip():
            continue
        item_stocks = item.get("stocks") or []
        if item_stocks:
            all_stocks.extend(item_stocks)
        item_topics = item.get("topics") or []
        item_stock_codes = [s.get("stock_code") for s in item_stocks if isinstance(s, dict) and s.get("stock_code")]
        cs_id = _process_one(
            extracted_text_id, item_text, "social_post",
            publish_time=publish_time,
            split_meta={
                "topic": item.get("topic", ""),
                "topics": item_topics,
                "has_data": item.get("has_data", False),
                "stocks": item_stocks,
            },
        )
        if cs_id:
            inserted_cs_ids.append(cs_id)

        # 拆条结果写入 text_chunks + Milvus（细粒度向量检索）
        try:
            from retrieval.chunker import chunk_and_index
            chunk_and_index(
                extracted_text_id=extracted_text_id,
                full_text=item_text,
                doc_type="social_post_split",
                publish_time=publish_time,
                extra_tags={
                    "topics": item_topics,
                    "stock_codes": item_stock_codes,
                },
            )
        except Exception as _chunk_err:
            logger.warning(f"[A] 拆条入Milvus失败 et_id={extracted_text_id}: {_chunk_err}")

    if all_stocks:
        _write_daily_intel_stocks(extracted_text_id, all_stocks, publish_time)

    _finalize(extracted_text_id, inserted_cs_ids)

    return {
        "items": items,
        "stocks": all_stocks,
        "cs_ids": inserted_cs_ids,
        "from_cache": False,
    }


def _read_split_cache(extracted_text_id: int) -> dict:
    """从 content_summaries 读取已有的拆条结果"""
    cs_rows = execute_cloud_query(
        """SELECT id, type_fields FROM content_summaries
           WHERE extracted_text_id=%s AND doc_type='social_post'
           ORDER BY id""",
        [extracted_text_id],
    )
    items = []
    all_stocks = []
    cs_ids = []
    for cs_row in (cs_rows or []):
        cs_ids.append(cs_row["id"])
        tf_raw = cs_row.get("type_fields") or "{}"
        try:
            tf = json.loads(tf_raw) if isinstance(tf_raw, str) else (tf_raw or {})
        except Exception:
            tf = {}
        item = {
            "topic": tf.get("split_topic", ""),
            "topics": tf.get("split_topics", []),
            "has_data": tf.get("split_has_data", False),
            "stocks": tf.get("split_stocks", []),
        }
        items.append(item)
        all_stocks.extend(item["stocks"])

    return {
        "items": items,
        "stocks": all_stocks,
        "cs_ids": cs_ids,
        "from_cache": True,
    }


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
        "SELECT id, full_text, source_format, publish_time FROM extracted_texts WHERE id=%s",
        [extracted_text_id],
    )
    if not rows:
        return None

    full_text = rows[0]["full_text"] or ""
    source_format = rows[0].get("source_format") or ""
    publish_time = rows[0].get("publish_time")
    if not full_text.strip():
        return None

    # txt 原文直接存 summary，不调 LLM
    if source_format == "text":
        doc_type = _resolve_doc_type(extracted_text_id, full_text)
        logger.info(f"[A] txt 原样存储 id={extracted_text_id} doc_type={doc_type}")
        cs_id = execute_cloud_insert(
            """INSERT INTO content_summaries
               (extracted_text_id, doc_type, summary, fact_summary, opinion_summary,
                evidence_assessment, info_gaps, family, type_fields)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            [extracted_text_id, doc_type, full_text, "", "", "", "", 3, None],
        )
        _finalize(extracted_text_id, [cs_id] if cs_id else [])
        return cs_id

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
            cs_id = _process_one(extracted_text_id, item_text, item_doc_type, publish_time=publish_time)
            if cs_id:
                if first_cs_id is None:
                    first_cs_id = cs_id
                inserted_cs_ids.append(cs_id)
        _finalize(extracted_text_id, inserted_cs_ids)
        return first_cs_id

    if doc_type == "social_post":
        result = ensure_social_post_split(extracted_text_id)
        return result["cs_ids"][0] if result["cs_ids"] else None

    cs_id = _process_one(extracted_text_id, full_text, doc_type, publish_time=publish_time)
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

"""统一清洗管线 — 对同一条 extracted_text 并发执行三条管线

Pipeline S:  semantic_clean      (DeepSeek, 前置语义清洗)
Pipeline A:  content_summaries   (Claude via call_model_json)
Pipeline B2: stock_mentions      (DeepSeek)
Pipeline C:  KG triples          (DeepSeek)

用法：
    from cleaning.unified_pipeline import process_single, process_pending
    process_single(extracted_text_id)
    process_pending(batch_size=50)
"""
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

from utils.db_utils import (
    execute_cloud_query, execute_cloud_insert,
    execute_query, execute_insert,
    sync_summary_to_local, sync_mentions_to_local,
)

logger = logging.getLogger(__name__)

# ── DeepSeek client (lazy singleton, thread-safe) ────────────────────────────

_deepseek_client = None
_deepseek_lock = threading.Lock()

def _get_deepseek():
    global _deepseek_client
    if _deepseek_client is None:
        with _deepseek_lock:
            if _deepseek_client is None:
                from openai import OpenAI
                rows = execute_cloud_query(
                    "SELECT value FROM system_config WHERE config_key='deepseek_api_key'"
                )
                if not rows:
                    raise RuntimeError("system_config 中未找到 deepseek_api_key")
                _deepseek_client = OpenAI(
                    api_key=rows[0]["value"], base_url="https://api.deepseek.com/v1"
                )
    return _deepseek_client


def _call_deepseek(system_prompt: str, text: str, max_tokens=2048, timeout=90) -> str:
    if len(text) > 12000:
        text = text[:12000] + "\n\n[文本已截断]"
    client = _get_deepseek()
    resp = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text},
        ],
        max_tokens=max_tokens,
        temperature=0.3,
        timeout=timeout,
    )
    return resp.choices[0].message.content


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

# ── Pipeline S: 语义清洗（前置步骤）─────────────────────────────────────────

_CLEAN_PROMPTS = {
    "pdf": """你是金融研报文本清洗专家。以下文本从PDF提取，由于多栏布局，侧边栏信息（分析师信息、股价评级、表现数据、免责声明等）可能被混入正文中间，打断了原本连贯的句子。

请执行：
1. 找到所有打断正文语义的异物文字（通常出现在句子中间，与前后文不连贯）
2. 删除这些异物文字
3. 修复被打断的句子，使其恢复连贯
4. 删除页眉页脚、页码、免责声明等重复出现的模板文字
5. 保留所有正文内容、数据、表格、图表描述、目录

如果文本已经很干净，直接原样返回。只做删除和修复，不添加任何新内容。直接输出清洗后的文本。""",

    "audio": """你是金融电话会议/音频转写文本清洗专家。以下文本由语音识别转写而来，包含口语噪音。

请执行：
1. 删除无意义的口语填充词和重复（"嗯"、"啊"、"那个"、"就是说"、连续重复的词句）
2. 修复被打断的句子——电话会中常有人插话打断，导致一句话被切成两段夹着别人的话
3. 合并说话人的断续表达，使其成为完整连贯的句子
4. 删除主持人的程序性话术（"下面有请XX回答"、"感谢XX的提问"等）
5. 保留所有实质性内容：观点、数据、问答、业务讨论
6. 保留说话人标识（如有）

不要改变原意，不要添加新内容，不要总结。直接输出清洗后的文本。""",

    "image": """你是OCR文本校对专家。以下文本由图片OCR识别而来，可能存在识别错误。

请执行：
1. 修复明显的OCR识别错字（形近字混淆、偏旁部首错误）
2. 修复断行导致的词语拆分和句子断裂
3. 修复表格数据错位（数字与标签对应关系）
4. 删除OCR产生的乱码和无意义字符
5. 修复标点符号识别错误（如"。"识别为"o"、"，"识别为","等）
6. 保留所有实质性内容

不要改变原意，不要添加新内容。直接输出清洗后的文本。""",

    "default": """你是金融文本清洗专家。以下文本从网页或文档提取，可能包含格式残留噪音。

请执行：
1. 删除混入正文的导航栏、广告、版权声明等网页/文档模板文字
2. 修复被格式残留打断的句子
3. 保留所有实质性内容

如果文本已经很干净，直接原样返回。只做删除和修复，不添加任何新内容。直接输出清洗后的文本。""",
}

# mp3 复用 audio prompt
_CLEAN_PROMPTS["mp3"] = _CLEAN_PROMPTS["audio"]
# mixed 复用 default
_CLEAN_PROMPTS["mixed"] = _CLEAN_PROMPTS["default"]
_CLEAN_PROMPTS["txt"] = _CLEAN_PROMPTS["default"]


def _split_long_text(text: str, chunk_size: int = 5000, overlap: int = 300) -> list:
    """长文本分段，优先按自然段落切分"""
    if len(text) <= chunk_size:
        return [text]
    try:
        from langchain_text_splitters import RecursiveCharacterTextSplitter
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size, chunk_overlap=overlap,
            separators=["\n\n", "\n", "。", "；", " "],
        )
        return splitter.split_text(text)
    except ImportError:
        chunks = []
        for i in range(0, len(text), chunk_size - overlap):
            chunks.append(text[i:i + chunk_size])
        return chunks


def _get_file_type(extracted_text_id: int) -> str:
    """查询 source_documents 获取 file_type"""
    rows = execute_cloud_query(
        """SELECT sd.file_type FROM extracted_texts et
           JOIN source_documents sd ON et.source_doc_id = sd.id
           WHERE et.id = %s""",
        [extracted_text_id],
    )
    return rows[0]["file_type"] if rows else "txt"


def _run_semantic_clean(extracted_text_id: int, full_text: str, file_type: str) -> str:
    """Pipeline S: DeepSeek 语义清洗，返回清洗后文本"""
    prompt = _CLEAN_PROMPTS.get(file_type, _CLEAN_PROMPTS["default"])
    chunks = _split_long_text(full_text)
    cleaned_parts = []
    for i, chunk in enumerate(chunks):
        try:
            result = _call_deepseek(prompt, chunk, max_tokens=4096, timeout=60)
            cleaned_parts.append(result if result else chunk)
        except Exception as e:
            logger.warning(f"[S] 语义清洗第{i+1}/{len(chunks)}段失败 id={extracted_text_id}: {e}")
            cleaned_parts.append(chunk)

    cleaned = "\n\n".join(cleaned_parts)

    # 写回 extracted_texts
    if cleaned and len(cleaned) > 100:
        execute_cloud_insert(
            "UPDATE extracted_texts SET full_text=%s, semantic_clean_status='done' WHERE id=%s",
            [cleaned, extracted_text_id],
        )
        logger.info(
            f"[S] 语义清洗完成 id={extracted_text_id} type={file_type} "
            f"({len(full_text)}→{len(cleaned)}字, {len(chunks)}段)"
        )
    else:
        execute_cloud_insert(
            "UPDATE extracted_texts SET semantic_clean_status='done' WHERE id=%s",
            [extracted_text_id],
        )
        logger.info(f"[S] 语义清洗结果过短，保留原文 id={extracted_text_id}")

    return cleaned or full_text


# ── Pipeline A: content_summaries ────────────────────────────────────────────

def _run_pipeline_a(extracted_text_id: int, full_text: str) -> Optional[int]:
    """返回 content_summaries.id（digest 返回第一条），失败返回 None"""
    from cleaning.content_summarizer import summarize_single
    try:
        return summarize_single(extracted_text_id)
    except Exception as e:
        logger.error(f"[A] 摘要失败 id={extracted_text_id}: {e}")
        return None


# ── Pipeline B2: stock_mentions ───────────────────────────────────────────────

_STOCK_MENTIONS_PROMPT = """你是专业的金融信息分析专家。请从以下文本中提取所有被**明确提及**的股票/上市公司标的。

严格规则：
1. 只提取文本中**直接出现**的具体股票名称或代码，不推断、不联想相关公司
2. 股票名称必须是具体的上市公司名称（如"宁德时代"、"比亚迪"），不接受模糊描述
3. 数据提供商、指数编制机构（如万得资讯、中证指数）不算投资标的
4. 如果文本中没有明确提及任何具体股票，直接返回空数组 []

输出 JSON 数组，每个元素：
{
  "stock_name": "股票/标的名称（具体公司名）",
  "stock_code": "代码（如有，如 600519.SH）",
  "related_themes": "相关题材/概念（逗号分隔）",
  "related_events": "文本中提及的相关事件",
  "theme_logic": "为什么这个股票和这个题材相关（基于文本内容）",
  "mention_time": "报道发布时间（如有，格式 YYYY-MM-DD HH:MM:SS，否则留空）"
}"""


def _run_pipeline_b2(extracted_text_id: int, full_text: str, publish_time) -> int:
    """返回写入的 stock_mentions 条数"""
    try:
        raw = _call_deepseek(_STOCK_MENTIONS_PROMPT, full_text)
        mentions = _parse_json(raw)
    except Exception as e:
        logger.error(f"[B2] DeepSeek 失败 id={extracted_text_id}: {e}")
        return 0
    if not isinstance(mentions, list) or not mentions:
        execute_cloud_insert(
            "UPDATE extracted_texts SET mentions_status='done' WHERE id=%s",
            [extracted_text_id],
        )
        return 0

    inserted_ids = []
    for m in mentions:
        if not m.get("stock_name"):
            continue
        mt = m.get("mention_time") or ""
        if not mt and publish_time:
            mt = str(publish_time)
        if not mt:
            mt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mid = execute_cloud_insert(
            """INSERT INTO stock_mentions
               (extracted_text_id, stock_name, stock_code,
                related_themes, related_events, theme_logic, mention_time)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            [extracted_text_id,
             m.get("stock_name", ""),
             m.get("stock_code") or None,
             m.get("related_themes") or None,
             m.get("related_events") or None,
             m.get("theme_logic") or None,
             mt],
        )
        if mid:
            inserted_ids.append(mid)
    
    execute_cloud_insert(
        "UPDATE extracted_texts SET mentions_status='done' WHERE id=%s",
        [extracted_text_id],
    )

    if inserted_ids:
        try:
            sync_mentions_to_local(inserted_ids)
        except Exception as e:
            logger.warning(f"[B2] 同步本地失败: {e}")
    return len(inserted_ids)

# ── Pipeline C: KG triples ────────────────────────────────────────────────────

def _run_pipeline_c(extracted_text_id: int, full_text: str) -> int:
    """提取 KG 三元组并写入本地 kg_entities / kg_relationships，返回写入关系数"""
    from knowledge_graph.kg_updater import KG_EXTRACTION_PROMPT, VALID_ENTITY_TYPES, VALID_RELATION_TYPES, RELATION_TO_CATEGORY
    from knowledge_graph.kg_manager import add_entity, add_relationship

    try:
        raw = _call_deepseek(KG_EXTRACTION_PROMPT, full_text, max_tokens=2048)
        triples = _parse_json(raw)
    except Exception as e:
        logger.error(f"[C] DeepSeek 失败 id={extracted_text_id}: {e}")
        return 0

    if not isinstance(triples, list) or not triples:
        execute_cloud_insert(
            "UPDATE extracted_texts SET kg_status='done' WHERE id=%s",
            [extracted_text_id],
        )
        return 0

    added = 0
    for triple in triples:
        src = triple.get("source") or {}
        tgt = triple.get("target") or {}
        src_name = src.get("name", "").strip()
        tgt_name = tgt.get("name", "").strip()
        if not src_name or not tgt_name or len(src_name) < 2 or len(tgt_name) < 2:
            continue

        src_type = src.get("type", "theme")
        tgt_type = tgt.get("type", "theme")
        if src_type not in VALID_ENTITY_TYPES:
            src_type = "theme"
        if tgt_type not in VALID_ENTITY_TYPES:
            tgt_type = "theme"

        src_id = add_entity(src_type, src_name, data_source="cleaning")
        tgt_id = add_entity(tgt_type, tgt_name, data_source="cleaning")
        if not src_id or not tgt_id:
            continue

        rel_type = triple.get("relation", "related")
        if rel_type not in VALID_RELATION_TYPES:
            rel_type = "related"
        strength = min(max(float(triple.get("strength", 0.5)), 0.1), 1.0)
        direction = triple.get("direction", "neutral")
        if direction not in ("positive", "negative", "neutral"):
            direction = "neutral"
        category = RELATION_TO_CATEGORY.get(rel_type, "structural")

        rid = add_relationship(
            src_id, tgt_id, rel_type,
            strength=strength, direction=direction,
            evidence=triple.get("evidence", ""),
            confidence=strength,
            relation_category=category,
            source_text=full_text[:200],
        )
        if rid:
            added += 1
            # 关联到 chunk：找与 full_text 对应的最相关 chunk
            try:
                _link_relationship_to_chunks(rid, extracted_text_id, src_name, tgt_name)
            except Exception:
                pass  # 关联失败不影响主流程

    # 标记 kg_status = done，避免重复处理
    execute_cloud_insert(
        "UPDATE extracted_texts SET kg_status='done' WHERE id=%s",
        [extracted_text_id],
    )
    return added


def _link_relationship_to_chunks(
    relationship_id: int,
    extracted_text_id: int,
    src_name: str,
    tgt_name: str,
):
    """将 KG 关系关联到包含该实体对的 text_chunks（写 kg_triple_chunks）"""
    rows = execute_query(
        """SELECT id FROM text_chunks
           WHERE extracted_text_id = %s
             AND (chunk_text LIKE %s OR chunk_text LIKE %s)
           ORDER BY chunk_index
           LIMIT 3""",
        [extracted_text_id, f"%{src_name}%", f"%{tgt_name}%"],
    )
    for row in rows:
        execute_insert(
            """INSERT IGNORE INTO kg_triple_chunks
               (relationship_id, chunk_id, confidence)
               VALUES (%s, %s, %s)""",
            [relationship_id, row["id"], 0.7],
        )


# ── 主入口 ────────────────────────────────────────────────────────────────────

def process_single(extracted_text_id: int, need_a=True, need_b=True, need_c=True,
                   on_status=None, rerun_a=False) -> dict:
    """对单条 extracted_text 先做语义清洗(S)，再并发执行三条管线(A/B2/C)

    on_status(stage, msg): 可选回调，用于向调用方汇报当前阶段
    rerun_a: 为 True 时清除旧 content_summaries 记录并重跑 Pipeline A
    Returns:
        {"summary_id": int|None, "mentions": int, "kg_rels": int, "semantic_cleaned": bool, "chunks": int}
    """
    # 重跑：清除该 extracted_text 的旧摘要记录，重置 summary_status
    if rerun_a:
        execute_cloud_insert(
            "DELETE FROM content_summaries WHERE extracted_text_id=%s",
            [extracted_text_id],
        )
        execute_cloud_insert(
            "UPDATE extracted_texts SET summary_status=NULL WHERE id=%s",
            [extracted_text_id],
        )
        logger.info(f"[A] 重跑：已清除旧摘要 id={extracted_text_id}")

    rows = execute_cloud_query(
        "SELECT id, full_text, publish_time, semantic_clean_status FROM extracted_texts WHERE id=%s",
        [extracted_text_id],
    )
    if not rows:
        return {"summary_id": None, "mentions": 0, "kg_rels": 0, "semantic_cleaned": False}

    row = rows[0]
    full_text = row["full_text"] or ""
    if not full_text.strip():
        return {"summary_id": None, "mentions": 0, "kg_rels": 0, "semantic_cleaned": False}

    # ★ Pipeline S: 语义清洗（前置步骤，所有文件类型）
    semantic_cleaned = False
    if row.get("semantic_clean_status") != "done":
        try:
            file_type = _get_file_type(extracted_text_id)
            if on_status:
                on_status("S", f"语义清洗 id={extracted_text_id} ({file_type})")
            # 用独立线程执行，设置总超时（避免长文本多段清洗阻塞整个管线）
            with ThreadPoolExecutor(max_workers=1) as _s_pool:
                _s_fut = _s_pool.submit(_run_semantic_clean, extracted_text_id, full_text, file_type)
                try:
                    full_text = _s_fut.result(timeout=180)  # 最多等3分钟
                    semantic_cleaned = True
                except Exception as _s_err:
                    logger.warning(f"[S] 语义清洗超时/失败 id={extracted_text_id}: {_s_err}，跳过继续执行A/B2/C")
        except Exception as e:
            logger.error(f"[S] 语义清洗异常 id={extracted_text_id}: {e}")

    # ★ 切片 + 向量索引（S完成后、A/B2/C之前）
    chunks_count = 0
    try:
        from retrieval.chunker import chunk_and_index
        # 查询文档元数据（含 doc_type）
        meta_rows = execute_cloud_query(
            """SELECT sd.file_type, sd.title, sd.doc_type, et.publish_time
               FROM extracted_texts et
               LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
               WHERE et.id = %s""",
            [extracted_text_id],
        )
        meta = meta_rows[0] if meta_rows else {}
        doc_type_hint = meta.get("doc_type") or ""
        file_type_hint = meta.get("file_type") or ""
        publish_time_hint = meta.get("publish_time")
        title_hint = meta.get("title") or ""
        chunks_count = chunk_and_index(
            extracted_text_id=extracted_text_id,
            full_text=full_text,
            doc_type=doc_type_hint,
            file_type=file_type_hint,
            publish_time=publish_time_hint,
            source_doc_title=title_hint,
        )
        if on_status:
            on_status("CHUNK", f"切片完成 {chunks_count} chunks")
    except Exception as e:
        logger.warning(f"切片失败 id={extracted_text_id}: {e}")

    if len(full_text) > 12000:
        full_text = full_text[:12000] + "\n\n[文本已截断]"

    results = {"summary_id": None, "mentions": 0, "kg_rels": 0, "semantic_cleaned": semantic_cleaned,
               "chunks": chunks_count}
    futures = {}

    with ThreadPoolExecutor(max_workers=3) as pool:
        if need_a:
            futures["a"] = pool.submit(_run_pipeline_a, extracted_text_id, full_text)
        if need_b:
            futures["b"] = pool.submit(_run_pipeline_b2, extracted_text_id, full_text, row.get("publish_time"))
        if need_c:
            futures["c"] = pool.submit(_run_pipeline_c, extracted_text_id, full_text)

        for key, fut in futures.items():
            try:
                val = fut.result()
                if key == "a":
                    results["summary_id"] = val
                elif key == "b":
                    results["mentions"] = val
                else:
                    results["kg_rels"] = val
            except Exception as e:
                logger.error(f"管线异常 id={extracted_text_id} pipeline={key}: {e}")

    # 摘要 chunk：族2文档的 content_summaries 展开写向量库
    if results.get("summary_id"):
        try:
            from retrieval.summary_chunker import index_summary_chunk
            index_summary_chunk(results["summary_id"])
        except Exception as e:
            logger.warning(f"摘要 chunk 写入失败 id={extracted_text_id}: {e}")

    return results


def process_pending(batch_size: int = 50, sleep: float = 0.5, should_cancel=None,
                    max_workers: int = 5, on_progress=None, rerun_a=False) -> dict:
    """处理所有 A/B/C 任意一条管线缺失的 extracted_texts，并发处理多条

    rerun_a: 为 True 时对所有记录强制重跑 Pipeline A（用于 prompt 升级后全量重跑）
    on_progress(done, total, et_id, result): 每完成一条时回调
    """
    if rerun_a:
        # 重跑模式：取所有有 full_text 的记录
        pending = execute_cloud_query(
            """SELECT id,
                      1 as need_a,
                      (mentions_status IS NULL OR mentions_status != 'done') as need_b,
                      (kg_status IS NULL OR kg_status != 'done') as need_c
               FROM extracted_texts
               WHERE full_text IS NOT NULL AND full_text != ''
               ORDER BY id
               LIMIT %s""",
            [batch_size],
        )
    else:
        pending = execute_cloud_query(
            """SELECT DISTINCT et.id,
                      (cs.id IS NULL) as need_a,
                      (et.mentions_status IS NULL OR et.mentions_status != 'done') as need_b,
                      (et.kg_status IS NULL OR et.kg_status != 'done') as need_c
               FROM extracted_texts et
               LEFT JOIN content_summaries cs ON et.id = cs.extracted_text_id
               WHERE cs.id IS NULL
                  OR (et.mentions_status IS NULL OR et.mentions_status != 'done')
                  OR (et.kg_status IS NULL OR et.kg_status != 'done')
               ORDER BY et.id
               LIMIT %s""",
            [batch_size],
        )
    total_a, total_b2, total_c = 0, 0, 0
    ok, fail = 0, 0

    # 回调通知实际待处理数
    if on_progress:
        on_progress(0, len(pending), None, None)

    def _run(row):
        return process_single(
            row["id"],
            need_a=bool(row["need_a"]),
            need_b=bool(row["need_b"]),
            need_c=bool(row["need_c"]),
            rerun_a=rerun_a,
        ), row

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = []
        for row in pending:
            if should_cancel and should_cancel():
                break
            futures.append(pool.submit(_run, row))

        for fut in as_completed(futures):
            if should_cancel and should_cancel():
                break
            try:
                r, row = fut.result()
                if r["summary_id"]:
                    total_a += 1
                total_b2 += r["mentions"]
                total_c  += r["kg_rels"]
                ok += 1
                logger.info(
                    f"[{ok+fail}/{len(pending)}] id={row['id']} "
                    f"need=({'A' if row['need_a'] else ''})({'B' if row['need_b'] else ''})({'C' if row['need_c'] else ''}) "
                    f"summary={r['summary_id']} mentions={r['mentions']} kg={r['kg_rels']}"
                )
                if on_progress:
                    on_progress(ok + fail, len(pending), row["id"], r)
            except Exception as e:
                fail += 1
                logger.error(f"process_single 失败: {e}")
                if on_progress:
                    on_progress(ok + fail, len(pending), None, None)

    return {"processed": ok + fail, "ok": ok, "fail": fail,
            "summaries": total_a, "mentions": total_b2, "kg_rels": total_c}

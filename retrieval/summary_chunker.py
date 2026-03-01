"""摘要 Chunk — 将族2 content_summaries 展开为自然语言，写入 MySQL + Milvus summary_chunks"""
import json
import logging
from typing import Optional

from config import MILVUS_HOST, MILVUS_PORT, EMBEDDING_DIM
from utils.db_utils import execute_cloud_query, execute_insert, execute_query
from retrieval.embedding import embed_texts

logger = logging.getLogger(__name__)

# Milvus collection 名
SUMMARY_COLLECTION_NAME = "summary_chunks"

# chunk_index 偏移，避免与 text_chunks 的 UNIQUE KEY(extracted_text_id, chunk_index) 冲突
SUMMARY_CHUNK_INDEX_OFFSET = 100000

_summary_connection_alias = "default"


def _ensure_connected():
    """确保已连接 Milvus（复用 default alias）。

    使用 connections.has_connection() 检查是否已有连接，避免与 vector_store.py
    共用同一 alias="default" 时重复 connect 导致冲突。
    老版本 pymilvus 若不支持 has_connection，降级为 try/except。
    """
    from pymilvus import connections
    try:
        if connections.has_connection("default"):
            return
    except AttributeError:
        # pymilvus 老版本不支持 has_connection，尝试直接连接，已连接时幂等
        try:
            connections.connect(alias=_summary_connection_alias, host=MILVUS_HOST, port=MILVUS_PORT)
            logger.info(f"summary_chunker Milvus 已连接（降级路径）: {MILVUS_HOST}:{MILVUS_PORT}")
        except Exception:
            pass
        return
    connections.connect(alias=_summary_connection_alias, host=MILVUS_HOST, port=MILVUS_PORT)
    logger.info(f"summary_chunker Milvus 已连接: {MILVUS_HOST}:{MILVUS_PORT}")


def _parse_type_fields(type_fields) -> dict:
    """兼容 type_fields 为 JSON 字符串或 dict 两种情况"""
    if type_fields is None:
        return {}
    if isinstance(type_fields, dict):
        return type_fields
    if isinstance(type_fields, str):
        try:
            parsed = json.loads(type_fields)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, ValueError):
            logger.warning(f"type_fields JSON 解析失败: {type_fields[:100]}")
            return {}
    return {}


def render_summary_text(cs_row: dict) -> str:
    """按 doc_type 将 content_summaries 一行展开为可读自然语言段落

    Args:
        cs_row: content_summaries 表的一行（dict），需含 doc_type/summary/fact_summary/
                opinion_summary/evidence_assessment/info_gaps/type_fields 字段
    Returns:
        多行拼接的自然语言字符串
    """
    doc_type = (cs_row.get("doc_type") or "").strip()
    summary = (cs_row.get("summary") or "").strip()
    fact_summary = (cs_row.get("fact_summary") or "").strip()
    opinion_summary = (cs_row.get("opinion_summary") or "").strip()
    evidence_assessment = (cs_row.get("evidence_assessment") or "").strip()
    info_gaps = (cs_row.get("info_gaps") or "").strip()
    tf = _parse_type_fields(cs_row.get("type_fields"))

    lines = []

    if doc_type == "research_report":
        institution = tf.get("institution", "")
        analyst = tf.get("analyst", "")
        rating = tf.get("rating", "")
        target_price = tf.get("target_price", "")
        current_price = tf.get("current_price", "")
        valuation_method = tf.get("valuation_method", "")
        risk_factors = tf.get("risk_factors", "")
        key_arguments = tf.get("key_arguments", [])

        header_parts = []
        if institution:
            header_parts.append(f"机构：{institution}")
        if analyst:
            header_parts.append(f"分析师：{analyst}")
        if rating:
            header_parts.append(f"评级：{rating}")
        if target_price:
            header_parts.append(f"目标价：{target_price}")
        if current_price:
            header_parts.append(f"当前价：{current_price}")
        if header_parts:
            lines.append("【研究报告】" + "，".join(header_parts))

        if summary:
            lines.append(f"总结：{summary}")
        if fact_summary:
            lines.append(f"事实摘要：{fact_summary}")
        if opinion_summary:
            lines.append(f"观点摘要：{opinion_summary}")

        if key_arguments:
            args_text_parts = []
            if isinstance(key_arguments, list):
                for arg in key_arguments:
                    if isinstance(arg, dict):
                        claim = arg.get("claim", "")
                        evidence = arg.get("evidence", "")
                        strength = arg.get("strength", "")
                        part = claim
                        if evidence:
                            part += f"（依据：{evidence}"
                            if strength:
                                part += f"，强度：{strength}"
                            part += "）"
                        if part:
                            args_text_parts.append(part)
                    elif isinstance(arg, str) and arg:
                        args_text_parts.append(arg)
            elif isinstance(key_arguments, str) and key_arguments:
                args_text_parts.append(key_arguments)
            if args_text_parts:
                lines.append("核心论点：" + "；".join(args_text_parts))

        if valuation_method:
            lines.append(f"估值方法：{valuation_method}")
        if risk_factors:
            if isinstance(risk_factors, list):
                lines.append("风险因素：" + "；".join(str(r) for r in risk_factors if r))
            else:
                lines.append(f"风险因素：{risk_factors}")

    elif doc_type == "strategy_report":
        market_view = tf.get("market_view", "")
        sector_allocation = tf.get("sector_allocation", "")
        key_themes = tf.get("key_themes", "")
        key_arguments = tf.get("key_arguments", [])
        time_horizon = tf.get("time_horizon", "")
        risk_factors = tf.get("risk_factors", "")

        lines.append("【策略报告】")

        if market_view:
            lines.append(f"市场观点：{market_view}")
        if sector_allocation:
            if isinstance(sector_allocation, list):
                lines.append("行业配置：" + "；".join(str(s) for s in sector_allocation if s))
            else:
                lines.append(f"行业配置：{sector_allocation}")
        if key_themes:
            if isinstance(key_themes, list):
                lines.append("核心主题：" + "；".join(str(t) for t in key_themes if t))
            else:
                lines.append(f"核心主题：{key_themes}")
        if time_horizon:
            lines.append(f"投资期限：{time_horizon}")

        if summary:
            lines.append(f"总结：{summary}")
        if fact_summary:
            lines.append(f"事实摘要：{fact_summary}")
        if opinion_summary:
            lines.append(f"观点摘要：{opinion_summary}")

        if key_arguments:
            args_text_parts = []
            if isinstance(key_arguments, list):
                for arg in key_arguments:
                    if isinstance(arg, dict):
                        claim = arg.get("claim", "")
                        evidence = arg.get("evidence", "")
                        part = claim
                        if evidence:
                            part += f"（{evidence}）"
                        if part:
                            args_text_parts.append(part)
                    elif isinstance(arg, str) and arg:
                        args_text_parts.append(arg)
            elif isinstance(key_arguments, str) and key_arguments:
                args_text_parts.append(key_arguments)
            if args_text_parts:
                lines.append("核心论点：" + "；".join(args_text_parts))

        if risk_factors:
            if isinstance(risk_factors, list):
                lines.append("风险因素：" + "；".join(str(r) for r in risk_factors if r))
            else:
                lines.append(f"风险因素：{risk_factors}")

    elif doc_type == "roadshow_notes":
        company = tf.get("company", "")
        management_guidance = tf.get("management_guidance", "")
        new_disclosures = tf.get("new_disclosures", "")
        key_qa = tf.get("key_qa", [])

        header = "【路演纪要】"
        if company:
            header += f" {company}"
        lines.append(header)

        if summary:
            lines.append(f"总结：{summary}")
        if fact_summary:
            lines.append(f"事实摘要：{fact_summary}")
        if opinion_summary:
            lines.append(f"观点摘要：{opinion_summary}")

        if management_guidance:
            if isinstance(management_guidance, list):
                lines.append("管理层指引：" + "；".join(str(g) for g in management_guidance if g))
            else:
                lines.append(f"管理层指引：{management_guidance}")
        if new_disclosures:
            if isinstance(new_disclosures, list):
                lines.append("新披露信息：" + "；".join(str(d) for d in new_disclosures if d))
            else:
                lines.append(f"新披露信息：{new_disclosures}")

        if key_qa:
            if isinstance(key_qa, list):
                qa_parts = []
                for qa in key_qa:
                    if isinstance(qa, dict):
                        q = qa.get("q", qa.get("question", ""))
                        a = qa.get("a", qa.get("answer", ""))
                        if q and a:
                            qa_parts.append(f"Q：{q} A：{a}")
                        elif q:
                            qa_parts.append(f"Q：{q}")
                if qa_parts:
                    lines.append("核心问答：" + " | ".join(qa_parts))

    elif doc_type == "feature_news":
        news_level = tf.get("news_level", "")
        industry_chain = tf.get("industry_chain", "")
        multiple_perspectives = tf.get("multiple_perspectives", "")
        background = tf.get("background", "")

        header_parts = ["【深度资讯】"]
        if news_level:
            header_parts.append(f"级别：{news_level}")
        lines.append("".join(header_parts))

        if background:
            lines.append(f"背景：{background}")
        if industry_chain:
            if isinstance(industry_chain, list):
                lines.append("产业链：" + "→".join(str(c) for c in industry_chain if c))
            else:
                lines.append(f"产业链：{industry_chain}")

        if summary:
            lines.append(f"总结：{summary}")
        if fact_summary:
            lines.append(f"事实摘要：{fact_summary}")
        if opinion_summary:
            lines.append(f"观点摘要：{opinion_summary}")

        if multiple_perspectives:
            if isinstance(multiple_perspectives, list):
                lines.append("多角度观点：" + "；".join(str(p) for p in multiple_perspectives if p))
            else:
                lines.append(f"多角度观点：{multiple_perspectives}")

    else:
        # 未知 doc_type，降级输出通用格式
        if doc_type:
            lines.append(f"【{doc_type}】")
        if summary:
            lines.append(f"总结：{summary}")
        if fact_summary:
            lines.append(f"事实摘要：{fact_summary}")
        if opinion_summary:
            lines.append(f"观点摘要：{opinion_summary}")

    # 共有字段（所有类型都追加）
    if evidence_assessment:
        lines.append(f"证据评估：{evidence_assessment}")
    if info_gaps:
        lines.append(f"信息缺口：{info_gaps}")

    return "\n".join(lines)


def ensure_summary_collection():
    """新建 Milvus collection 'summary_chunks'（不存在则创建，存在则 load 后返回）

    Returns:
        pymilvus.Collection 对象
    """
    from pymilvus import (
        Collection, CollectionSchema, FieldSchema, DataType,
        utility,
    )

    try:
        _ensure_connected()
    except Exception as e:
        logger.warning(f"Milvus 连接失败，无法创建 summary_chunks collection: {e}")
        raise

    if utility.has_collection(SUMMARY_COLLECTION_NAME):
        col = Collection(SUMMARY_COLLECTION_NAME)
        col.load()
        logger.info(f"Collection '{SUMMARY_COLLECTION_NAME}' 已存在，已加载")
        return col

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=EMBEDDING_DIM),
        FieldSchema(name="content_summary_id", dtype=DataType.INT64),
        FieldSchema(name="extracted_text_id", dtype=DataType.INT64),
        FieldSchema(name="doc_type", dtype=DataType.VARCHAR, max_length=50),
        FieldSchema(name="publish_time", dtype=DataType.VARCHAR, max_length=20),
    ]
    schema = CollectionSchema(fields, description="summary_chunks 摘要向量索引")
    col = Collection(SUMMARY_COLLECTION_NAME, schema)

    col.create_index(
        field_name="embedding",
        index_params={
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 16, "efConstruction": 256},
        },
    )
    col.load()
    logger.info(f"Collection '{SUMMARY_COLLECTION_NAME}' 创建完成，HNSW 索引已建立")
    return col


def index_summary_chunk(content_summary_id: int) -> bool:
    """将单条 family=2 的 content_summary 向量化并写入 MySQL + Milvus

    Args:
        content_summary_id: content_summaries.id
    Returns:
        True 成功写入，False 跳过或失败
    """
    # 读云端 content_summaries + extracted_texts.publish_time
    rows = execute_cloud_query(
        """
        SELECT cs.id, cs.extracted_text_id, cs.doc_type, cs.family,
               cs.summary, cs.fact_summary, cs.opinion_summary,
               cs.evidence_assessment, cs.info_gaps, cs.type_fields,
               et.publish_time
        FROM content_summaries cs
        LEFT JOIN extracted_texts et ON et.id = cs.extracted_text_id
        WHERE cs.id = %s
        """,
        [content_summary_id],
    )

    if not rows:
        logger.warning(f"content_summary_id={content_summary_id} 不存在")
        return False

    cs = rows[0]

    # 仅处理 family=2
    if int(cs.get("family") or 0) != 2:
        logger.debug(f"content_summary_id={content_summary_id} family={cs.get('family')} 非族2，跳过")
        return False

    # 展开文本
    text = render_summary_text(cs)
    if len(text.strip()) < 20:
        logger.info(f"content_summary_id={content_summary_id} 展开文本过短（<20字），跳过")
        return False

    extracted_text_id = int(cs.get("extracted_text_id") or 0)
    doc_type = (cs.get("doc_type") or "")[:50]
    publish_time = str(cs.get("publish_time") or "")[:20]
    chunk_index = SUMMARY_CHUNK_INDEX_OFFSET + content_summary_id

    # 写本地 MySQL text_chunks（ON DUPLICATE KEY UPDATE）
    # PyMySQL 在 ON DUPLICATE KEY UPDATE 命中已有行时返回 0，需要 fallback 查询
    try:
        chunk_db_id = execute_insert(
            """
            INSERT INTO text_chunks
                (extracted_text_id, chunk_index, chunk_text, chunk_type, doc_type, publish_time)
            VALUES (%s, %s, %s, 'summary', %s, %s)
            ON DUPLICATE KEY UPDATE
                chunk_text = VALUES(chunk_text),
                chunk_type = VALUES(chunk_type),
                doc_type = VALUES(doc_type),
                publish_time = VALUES(publish_time)
            """,
            [extracted_text_id, chunk_index, text, doc_type, publish_time],
        )
    except Exception as e:
        logger.warning(f"写 text_chunks 失败 content_summary_id={content_summary_id}: {e}")
        return False

    # ON DUPLICATE KEY UPDATE 时 execute_insert 返回 0，需要回查真实 id
    if not chunk_db_id:
        existing = execute_query(
            "SELECT id FROM text_chunks WHERE extracted_text_id=%s AND chunk_index=%s",
            [extracted_text_id, chunk_index],
        )
        if existing:
            chunk_db_id = existing[0]["id"]

    if not chunk_db_id:
        logger.error(f"写 text_chunks 失败 cs_id={content_summary_id}")
        return False

    chunk_db_id = int(chunk_db_id)

    # 生成 embedding
    try:
        embeddings = embed_texts([text])
        if not embeddings:
            logger.warning(f"embed_texts 返回空，content_summary_id={content_summary_id}")
            return False
        embedding = embeddings[0]
    except Exception as e:
        logger.warning(f"embed_texts 失败 content_summary_id={content_summary_id}: {e}")
        return False

    # 写 Milvus summary_chunks
    # 使用字典格式显式指定字段，避免位置匹配错位
    try:
        col = ensure_summary_collection()
        pt_str = str(cs.get("publish_time") or "")[:20]
        col.upsert([
            {
                "id": chunk_db_id,
                "embedding": embedding,
                "content_summary_id": content_summary_id,
                "extracted_text_id": extracted_text_id,
                "doc_type": (doc_type or "")[:50],
                "publish_time": pt_str,
            }
        ])
        logger.debug(f"Milvus upsert summary_chunk: content_summary_id={content_summary_id}, chunk_db_id={chunk_db_id}")
    except Exception as e:
        logger.warning(f"Milvus 写入失败 content_summary_id={content_summary_id}: {e}")
        return False

    return True


def search_summary_chunks(query_embedding: list[float], top_k: int = 5) -> list[dict]:
    """在 summary_chunks collection 中向量相似度搜索

    Args:
        query_embedding: 查询向量（1024维）
        top_k: 返回数量
    Returns:
        [{"chunk_id", "score", "content_summary_id", "extracted_text_id", "doc_type", "publish_time"}]
        异常时返回 []
    """
    try:
        _ensure_connected()
        from pymilvus import Collection, utility
        if not utility.has_collection(SUMMARY_COLLECTION_NAME):
            logger.warning(f"Collection '{SUMMARY_COLLECTION_NAME}' 不存在，请先运行 ensure_summary_collection()")
            return []

        col = Collection(SUMMARY_COLLECTION_NAME)
        col.load()

        search_params = {"metric_type": "COSINE", "params": {"ef": 128}}
        results = col.search(
            data=[query_embedding],
            anns_field="embedding",
            param=search_params,
            limit=top_k,
            output_fields=["content_summary_id", "extracted_text_id", "doc_type", "publish_time"],
        )

        hits = []
        for hit in results[0]:
            hits.append({
                "chunk_id": hit.id,
                "score": hit.score,
                "content_summary_id": hit.entity.get("content_summary_id"),
                "extracted_text_id": hit.entity.get("extracted_text_id"),
                "doc_type": hit.entity.get("doc_type"),
                "publish_time": hit.entity.get("publish_time"),
            })
        return hits

    except Exception as e:
        logger.warning(f"search_summary_chunks 失败: {e}")
        return []


def backfill_family2(batch_size: int = 100, dry_run: bool = False) -> dict:
    """批量回填族2 content_summaries 到 summary_chunks

    Args:
        batch_size: 分批迭代时每批处理的条数上限。注意：查询 family=2 的 id 列表是一次
                    性全量加载到内存（无分页游标），batch_size 仅控制内存中逐批迭代写入
                    的步长，不影响初始查询的数据量。如记录数极多，可考虑改为游标分页查询。
        dry_run: True 时只统计不写入
    Returns:
        {"total": int, "ok": int, "skip": int, "fail": int}
    """
    stats = {"total": 0, "ok": 0, "skip": 0, "fail": 0}

    # 查云端所有 family=2 的 content_summaries id
    try:
        id_rows = execute_cloud_query(
            "SELECT id FROM content_summaries WHERE family = 2 ORDER BY id",
        )
    except Exception as e:
        logger.error(f"查询 family=2 content_summaries 失败: {e}")
        return stats

    all_ids = [row["id"] for row in id_rows]
    stats["total"] = len(all_ids)
    logger.info(f"backfill_family2: 共 {stats['total']} 条族2摘要，dry_run={dry_run}")

    if dry_run:
        logger.info("dry_run=True，跳过实际写入")
        return stats

    for i in range(0, len(all_ids), batch_size):
        batch = all_ids[i: i + batch_size]
        for cs_id in batch:
            try:
                result = index_summary_chunk(cs_id)
                if result:
                    stats["ok"] += 1
                else:
                    stats["skip"] += 1
            except Exception as e:
                logger.warning(f"index_summary_chunk({cs_id}) 异常: {e}")
                stats["fail"] += 1

        logger.info(
            f"backfill_family2 进度: {min(i + batch_size, len(all_ids))}/{stats['total']} "
            f"ok={stats['ok']} skip={stats['skip']} fail={stats['fail']}"
        )

    logger.info(f"backfill_family2 完成: {stats}")
    return stats

"""将研究结果存入 raw_items + cleaned_items，使其像新闻一样可被检索和关联"""
import json
import logging
from utils.db_utils import execute_query, execute_insert, execute_cloud_insert, sync_summary_to_local

logger = logging.getLogger(__name__)


def _get_source_doc_id():
    """获取 source_doc 数据源 ID"""
    rows = execute_query("SELECT id FROM data_sources WHERE name='source_doc'")
    if rows:
        return rows[0]["id"]
    # 兜底：插入一条
    return execute_insert(
        "INSERT INTO data_sources (name, source_type) VALUES ('source_doc', 'source_doc')"
    )


def store_as_news(title, summary, tags, event_type="research_report",
                  sentiment="neutral", importance=3, impact_analysis="",
                  extra_meta=None):
    """将一条研究结果存为 raw_item + cleaned_item（旧管线，保留兼容）

    Args:
        title: 标题（存入 raw_items.title）
        summary: 摘要（存入 cleaned_items.summary）
        tags: 标签列表 ["AI", "算力"]
        event_type: 事件类型
        sentiment: positive/negative/neutral
        importance: 1-5
        impact_analysis: 影响分析文本
        extra_meta: 额外元数据 dict
    Returns:
        cleaned_item_id
    """
    source_id = _get_source_doc_id()

    # 1. 插入 raw_item
    meta = {"origin": "system_research"}
    if extra_meta:
        meta.update(extra_meta)

    raw_id = execute_insert(
        """INSERT INTO raw_items
           (source_id, external_id, title, content, item_type,
            processing_status, meta_json)
           VALUES (?, ?, ?, ?, 'report', 'cleaned', ?)""",
        [source_id, f"research_{event_type}_{id(title)}",
         title, summary, json.dumps(meta, ensure_ascii=False)],
    )

    # 2. 插入 cleaned_item
    cleaned_id = execute_insert(
        """INSERT INTO cleaned_items
           (raw_item_id, event_type, sentiment, importance,
            summary, tags_json, impact_analysis)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [raw_id, event_type, sentiment, importance,
         summary, json.dumps(tags, ensure_ascii=False),
         impact_analysis],
    )

    logger.info(f"研究结果已存为新闻: cleaned_id={cleaned_id}, title={title[:30]}")
    return cleaned_id


def store_as_extracted_text(title, full_text, summary=None,
                             source="system_report", source_format="markdown",
                             publish_time=None, extra_meta=None):
    """将系统报告存入新管线（extracted_texts + content_summaries）

    报告本身即总结，summary_status='skipped'，kg_status='pending'。

    Args:
        title: 报告标题（存入 source_ref）
        full_text: 报告全文
        summary: 简短摘要（写入 content_summaries.summary），None 则截取 full_text 前 500 字
        source: 数据源标识，默认 'system_report'
        source_format: 格式，默认 'markdown'
        publish_time: 发布时间
        extra_meta: 保留字段（暂未使用）
    Returns:
        {"extracted_text_id": int, "summary_id": int}
    """
    import hashlib
    source_ref = f"report_{hashlib.md5(title.encode()).hexdigest()[:12]}"

    # 去重检查
    from utils.db_utils import execute_cloud_query
    dup = execute_cloud_query(
        "SELECT id FROM extracted_texts WHERE source=%s AND source_ref=%s",
        [source, source_ref],
    )
    if dup:
        logger.info(f"报告已存在，跳过: {title[:40]}")
        return {"extracted_text_id": dup[0]["id"], "summary_id": None}

    # 写入 extracted_texts（云端）
    et_id = execute_cloud_insert(
        """INSERT INTO extracted_texts
           (source, source_format, publish_time, full_text,
            source_ref, extract_quality, summary_status, kg_status)
           VALUES (%s, %s, %s, %s, %s, 'pass', 'skipped', 'pending')""",
        [source, source_format, publish_time, full_text, source_ref],
    )

    # 写入 content_summaries（报告本身即总结）
    summary_text = summary or full_text[:500]
    cs_id = execute_cloud_insert(
        """INSERT INTO content_summaries
           (extracted_text_id, summary, fact_summary)
           VALUES (%s, %s, %s)""",
        [et_id, summary_text, full_text[:2000]],
    )

    # 同步到本地
    try:
        sync_summary_to_local(cs_id)
    except Exception as e:
        logger.warning(f"同步报告总结到本地失败: {e}")

    logger.info(f"报告已存入新管线: et_id={et_id}, cs_id={cs_id}, title={title[:40]}")
    return {"extracted_text_id": et_id, "summary_id": cs_id}


"""检索质量打分 — 基于 KG 实体/关系的人工审核状态，计算 chunk 质量乘法系数

quality_boost 不存储，运行时从 review_status 实时映射：
  approved       → 1.2  (人工确认正确, +20%)
  unreviewed     → 1.0  (未审核, 不变)
  pending_approval → 1.0 (待审批, 暂不变)
  rejected       → 0.3  (人工确认错误, 降权至30%)
"""
import logging
import math
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# review_status → 质量乘法系数
BOOST_MAP: dict[str, float] = {
    'approved':         1.2,
    'unreviewed':       1.0,
    'pending_approval': 1.0,
    'rejected':         0.3,
}


def get_chunk_quality_boost(chunk_ids: list[int]) -> dict[int, float]:
    """
    对每个 chunk，通过 chunk_entities JOIN kg_entities 获取关联实体的 review_status，
    取关联实体 boost 值的加权平均作为该 chunk 的最终乘法系数。

    规则:
    - chunk 关联 3 个 approved 实体 → boost = 1.2
    - chunk 关联 2 approved + 1 rejected → boost = (1.2+1.2+0.3)/3 = 0.9
    - chunk 无关联实体 → boost = 1.0（不变）
    - chunk 全关联 rejected → boost = 0.3

    Args:
        chunk_ids: text_chunks.id 列表

    Returns:
        {chunk_id: boost_factor}，未出现的 chunk_id 默认 1.0
    """
    if not chunk_ids:
        return {}

    try:
        from utils.db_utils import execute_query

        placeholders = ','.join(['%s'] * len(chunk_ids))
        rows = execute_query(f"""
            SELECT ce.chunk_id, ke.review_status
            FROM chunk_entities ce
            JOIN kg_entities ke ON ce.entity_id = ke.id
            WHERE ce.chunk_id IN ({placeholders})
        """, chunk_ids)

        if not rows:
            return {cid: 1.0 for cid in chunk_ids}

        # 按 chunk_id 分组，计算加权平均
        chunk_boost_sum: dict[int, float] = {}
        chunk_boost_cnt: dict[int, int] = {}

        for r in rows:
            cid = r['chunk_id']
            status = r['review_status'] or 'unreviewed'
            boost = BOOST_MAP.get(status, 1.0)
            chunk_boost_sum[cid] = chunk_boost_sum.get(cid, 0.0) + boost
            chunk_boost_cnt[cid] = chunk_boost_cnt.get(cid, 0) + 1

        result = {}
        for cid in chunk_ids:
            if cid in chunk_boost_sum:
                avg_boost = chunk_boost_sum[cid] / chunk_boost_cnt[cid]
                result[cid] = round(avg_boost, 4)
            else:
                result[cid] = 1.0  # 无关联实体 → 不变

        return result

    except Exception as e:
        logger.warning(f"get_chunk_quality_boost 失败 (chunk_ids={chunk_ids[:5]}...): {e}")
        return {cid: 1.0 for cid in chunk_ids}


def get_relationship_quality_boost(relationship_id: int) -> float:
    """获取单条 KG 关系的综合质量系数

    综合考虑：
    1. 关系自身的 review_status（权重 0.6）
    2. 关系的三元组佐证来源的 review_status 加权平均（权重 0.4）
       - 如果所有佐证来源都被 rejected，额外惩罚（乘以 0.5）
       - 如果没有佐证来源，不参与计算
    """
    try:
        from utils.db_utils import execute_query

        # 关系自身状态
        rows = execute_query(
            "SELECT review_status FROM kg_relationships WHERE id = %s",
            [relationship_id]
        )
        if not rows:
            return 1.0
        rel_status = rows[0]['review_status'] or 'unreviewed'
        rel_boost = BOOST_MAP.get(rel_status, 1.0)

        # 三元组佐证来源状态
        source_rows = execute_query(
            "SELECT review_status FROM kg_triple_sources WHERE relationship_id = %s",
            [relationship_id]
        )
        if not source_rows:
            # 没有佐证来源，只用关系自身状态
            return rel_boost

        source_boosts = [BOOST_MAP.get(r['review_status'] or 'unreviewed', 1.0) for r in source_rows]
        avg_source_boost = sum(source_boosts) / len(source_boosts)

        # 综合得分
        combined = rel_boost * 0.6 + avg_source_boost * 0.4

        # 额外惩罚：所有佐证来源都是 rejected
        if all(r['review_status'] == 'rejected' for r in source_rows):
            combined *= 0.5

        return round(combined, 4)

    except Exception as e:
        logger.warning(f"get_relationship_quality_boost 失败 (rel_id={relationship_id}): {e}")
        return 1.0


def get_entity_quality_boost(entity_id: int) -> float:
    """获取单个 KG 实体的质量系数"""
    try:
        from utils.db_utils import execute_query
        rows = execute_query(
            "SELECT review_status FROM kg_entities WHERE id = %s",
            [entity_id]
        )
        if not rows:
            return 1.0
        status = rows[0]['review_status'] or 'unreviewed'
        return BOOST_MAP.get(status, 1.0)
    except Exception as e:
        logger.warning(f"get_entity_quality_boost 失败 (entity_id={entity_id}): {e}")
        return 1.0


def is_relationship_rejected(relationship_id: int) -> bool:
    """判断某条关系是否被人工标记为 rejected（用于 hybrid 过滤）"""
    try:
        from utils.db_utils import execute_query
        rows = execute_query(
            "SELECT review_status FROM kg_relationships WHERE id = %s",
            [relationship_id]
        )
        if not rows:
            return False
        return rows[0]['review_status'] == 'rejected'
    except Exception:
        return False


# ── P1: 时效性衰减 ────────────────────────────────────────────────────────────

HALF_LIFE_BY_DOC_TYPE: dict[str, int] = {
    "flash_news":        7,
    "market_commentary": 7,
    "social_post":       14,
    "chat_record":       14,
    "feature_news":      30,
    "research_report":   45,
    "strategy_report":   30,
    "roadshow_notes":    30,
    "announcement":      60,
    "financial_report":  90,
    "policy_doc":        90,
    "data_release":      30,
}
DEFAULT_HALF_LIFE = 30


def time_decay(publish_time_str: str, doc_type: str = "") -> float:
    """指数衰减系数：发布后 half_life_days 天衰减到 0.5

    - 今天发布 → 1.0
    - half_life_days 天前 → 0.5
    - 无日期 → 0.5（中间值）

    Args:
        publish_time_str: "YYYY-MM-DD" 或 "YYYY-MM-DD HH:MM:SS"
        doc_type: 文档类型，用于选取半衰期
    Returns:
        0.0 ~ 1.0 之间的衰减系数
    """
    if not publish_time_str or len(str(publish_time_str)) < 10:
        return 0.5
    try:
        pub_date = datetime.strptime(str(publish_time_str)[:10], "%Y-%m-%d")
        days_ago = (datetime.now() - pub_date).days
        if days_ago < 0:
            days_ago = 0
        half_life = HALF_LIFE_BY_DOC_TYPE.get(doc_type, DEFAULT_HALF_LIFE)
        return math.pow(0.5, days_ago / half_life)
    except Exception:
        return 0.5


# ── P2: 摘要 chunk 质量系数 ───────────────────────────────────────────────────

def get_summary_chunk_quality_boost(content_summary_id: int) -> float:
    """获取摘要 chunk 的质量系数（基于 content_summaries.review_status）

    复用 BOOST_MAP：approved→1.2, unreviewed→1.0, rejected→0.3
    """
    try:
        from utils.db_utils import execute_cloud_query
        rows = execute_cloud_query(
            "SELECT review_status FROM content_summaries WHERE id = %s",
            [content_summary_id],
        )
        if not rows:
            return 1.0
        status = rows[0]["review_status"] or "unreviewed"
        return BOOST_MAP.get(status, 1.0)
    except Exception as e:
        logger.warning(f"get_summary_chunk_quality_boost 失败 cs_id={content_summary_id}: {e}")
        return 1.0


# 按 doc_type 使用不同半衰期（天）
HALF_LIFE_BY_DOC_TYPE: dict[str, int] = {
    "flash_news":        7,
    "market_commentary": 7,
    "social_post":       14,
    "chat_record":       14,
    "feature_news":      30,
    "research_report":   45,
    "strategy_report":   30,
    "roadshow_notes":    30,
    "announcement":      60,
    "financial_report":  90,
    "policy_doc":        90,
    "data_release":      30,
}
DEFAULT_HALF_LIFE = 30


def time_decay(publish_time_str: str, doc_type: str = "") -> float:
    """指数衰减系数：half_life_days 内衰减到 0.5

    - 今天发布 → 1.0
    - half_life_days 天前 → 0.5
    - 无日期 → 0.5（中间值）

    Args:
        publish_time_str: "YYYY-MM-DD" 或 "YYYY-MM-DD HH:MM:SS" 格式
        doc_type: 文档类型，用于选取半衰期
    Returns:
        0.0 ~ 1.0 之间的衰减系数
    """
    if not publish_time_str or len(str(publish_time_str)) < 10:
        return 0.5
    try:
        pub_date = datetime.strptime(str(publish_time_str)[:10], "%Y-%m-%d")
        days_ago = (datetime.now() - pub_date).days
        if days_ago < 0:
            days_ago = 0
        half_life = HALF_LIFE_BY_DOC_TYPE.get(doc_type, DEFAULT_HALF_LIFE)
        return math.pow(0.5, days_ago / half_life)
    except Exception:
        return 0.5

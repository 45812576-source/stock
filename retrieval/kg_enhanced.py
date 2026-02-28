"""L2 KG 增强检索 — 现有 KG 查询 + chunk 原文溯源"""
import logging
from typing import Optional

from retrieval.models import KGResult, ChunkResult

logger = logging.getLogger(__name__)


def kg_enhanced_search(
    entity_or_query: str,
    mode: str = "context",
    depth: int = 2,
    with_chunks: bool = True,
    limit: int = 3,
) -> KGResult:
    """L2 KG 增强检索

    Args:
        entity_or_query: 实体名称或查询字符串
        mode: 查询模式
            - "context"  子图（extract_context_subgraph）
            - "causal"   因果链（trace_causal_chain）
            - "impact"   影响分析（impact_analysis）
            - "company"  公司上下文（get_company_context）
        depth: 遍历深度
        with_chunks: 是否附加 chunk 原文佐证
        limit: 每条 KG 关系最多关联几个 chunk
    Returns:
        KGResult
    """
    from knowledge_graph import kg_query

    result = KGResult()

    if mode == "company":
        ctx = kg_query.get_company_context(entity_or_query)
        if ctx:
            result.text = str(ctx)

    elif mode == "causal":
        entities = kg_query.search_entities(entity_or_query, limit=1)
        if entities:
            eid = entities[0]["id"]
            chain = kg_query.trace_causal_chain(eid, max_depth=depth)
            if isinstance(chain, str):
                result.text = chain
            elif isinstance(chain, dict):
                result.text = str(chain)

    elif mode == "impact":
        entities = kg_query.search_entities(entity_or_query, limit=1)
        if entities:
            eid = entities[0]["id"]
            impact = kg_query.impact_analysis(eid)
            if isinstance(impact, str):
                result.text = impact
            elif isinstance(impact, dict):
                result.text = str(impact)

    else:  # context (default)
        entities = kg_query.search_entities(entity_or_query, limit=3)
        if entities:
            entity_ids = [e["id"] for e in entities]
            subgraph_text = kg_query.extract_context_subgraph(entity_ids, depth=depth)
            if isinstance(subgraph_text, str):
                result.text = subgraph_text
            elif isinstance(subgraph_text, dict):
                result.text = str(subgraph_text)
            result.nodes = entities

    # 附加 chunk 原文佐证
    if with_chunks and result.text:
        evidence = _get_evidence_chunks(entity_or_query, limit=limit)
        result.evidence_chunks = evidence

    return result


def _get_evidence_chunks(entity_name: str, limit: int = 3) -> list[ChunkResult]:
    """通过 chunk_entities 找到提及某实体的 chunk"""
    from utils.db_utils import execute_query

    rows = execute_query(
        """SELECT tc.id, tc.chunk_text, tc.extracted_text_id, tc.doc_type,
                  tc.file_type, tc.publish_time, tc.source_doc_title
           FROM text_chunks tc
           JOIN chunk_entities ce ON tc.id = ce.chunk_id
           JOIN kg_entities ke ON ce.entity_id = ke.id
           WHERE ke.entity_name = %s
           ORDER BY tc.publish_time DESC
           LIMIT %s""",
        [entity_name, limit],
    )

    chunks = []
    for r in rows:
        chunks.append(ChunkResult(
            chunk_id=r["id"],
            text=r["chunk_text"],
            score=1.0,
            extracted_text_id=r["extracted_text_id"],
            doc_type=r.get("doc_type") or "",
            file_type=r.get("file_type") or "",
            publish_time=str(r.get("publish_time") or ""),
            source_doc_title=r.get("source_doc_title") or "",
        ))
    return chunks


def get_evidence_for_relationship(relationship_id: int, limit: int = 3) -> list[ChunkResult]:
    """获取支撑某条 KG 关系的原文 chunks（通过 kg_triple_chunks）"""
    from utils.db_utils import execute_query

    rows = execute_query(
        """SELECT tc.id, tc.chunk_text, tc.extracted_text_id, tc.doc_type,
                  tc.file_type, tc.publish_time, tc.source_doc_title,
                  ktc.confidence
           FROM text_chunks tc
           JOIN kg_triple_chunks ktc ON tc.id = ktc.chunk_id
           WHERE ktc.relationship_id = %s
           ORDER BY ktc.confidence DESC
           LIMIT %s""",
        [relationship_id, limit],
    )

    chunks = []
    for r in rows:
        chunks.append(ChunkResult(
            chunk_id=r["id"],
            text=r["chunk_text"],
            score=r.get("confidence") or 0.5,
            extracted_text_id=r["extracted_text_id"],
            doc_type=r.get("doc_type") or "",
            file_type=r.get("file_type") or "",
            publish_time=str(r.get("publish_time") or ""),
            source_doc_title=r.get("source_doc_title") or "",
        ))
    return chunks

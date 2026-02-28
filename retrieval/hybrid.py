"""L3 混合检索 — 组合 L1 向量 + L2 KG，分区序列化为 merged_context"""
import logging
from typing import Optional

from retrieval.models import HybridResult, ChunkResult, KGResult

logger = logging.getLogger(__name__)


def hybrid_search(
    query: str,
    context: Optional[dict] = None,
    strategy: str = "auto",
    max_context_chars: int = 4000,
    top_k: int = 10,
) -> HybridResult:
    """L3 混合检索

    Args:
        query: 查询文本
        context: 检索范围上下文
            - stock_codes: list[str]
            - theme_tags: list[str]
            - entity_names: list[str]
        strategy: 检索策略
            - "auto"         自动（默认：向量优先，context 中有实体则加 KG）
            - "vector_first" 纯向量，不调 KG
            - "kg_first"     KG 优先，向量补充
            - "parallel"     KG + 向量同时查，结果融合
        max_context_chars: merged_context 字符数上限
        top_k: 向量检索返回数量
    Returns:
        HybridResult
    """
    from retrieval.semantic import semantic_search
    from retrieval.kg_enhanced import kg_enhanced_search

    result = HybridResult()

    # 向量检索
    filters = _build_filters(context)
    chunks = semantic_search(query, top_k=top_k, filters=filters)
    result.chunks = chunks

    # KG 查询
    kg_result = None
    entity_name = _extract_entity(query, context)

    if strategy == "vector_first":
        pass  # 不调 KG

    elif strategy == "kg_first":
        if entity_name:
            kg_result = kg_enhanced_search(entity_name, mode="context", with_chunks=False)
        result.kg = kg_result

    elif strategy == "parallel" or (strategy == "auto" and entity_name):
        if entity_name:
            try:
                kg_result = kg_enhanced_search(entity_name, mode="context", with_chunks=False)
                result.kg = kg_result
            except Exception as e:
                logger.warning(f"KG 查询失败: {e}")

    # 组装 merged_context
    result.merged_context = _merge_context(
        kg_result=kg_result,
        chunks=chunks,
        max_chars=max_context_chars,
    )

    return result


def _build_filters(context: Optional[dict]) -> Optional[dict]:
    """将 context 转为向量检索 filters"""
    if not context:
        return None
    filters = {}

    doc_types = context.get("doc_types")
    if doc_types:
        filters["doc_types"] = doc_types

    date_range = context.get("date_range")
    if date_range:
        filters["date_range"] = date_range

    return filters if filters else None


def _extract_entity(query: str, context: Optional[dict]) -> Optional[str]:
    """从 query 或 context 中提取实体名用于 KG 查询"""
    if context:
        entity_names = context.get("entity_names")
        if entity_names:
            return entity_names[0]
        theme_tags = context.get("theme_tags")
        if theme_tags:
            return theme_tags[0]

    # 简单启发：query 中有 2-6 字连续中文词可能是实体
    import re
    matches = re.findall(r'[\u4e00-\u9fff]{2,8}', query)
    return matches[0] if matches else None


def _merge_context(
    kg_result: Optional[KGResult],
    chunks: list[ChunkResult],
    max_chars: int = 4000,
) -> str:
    """分区序列化：KG 结构 40% + 向量 chunks 50% + KG 佐证 10%"""
    sections = []
    budget = max_chars

    kg_budget = int(max_chars * 0.40)
    chunk_budget = int(max_chars * 0.50)
    evidence_budget = int(max_chars * 0.10)

    # 区域1: KG 结构信息
    if kg_result and kg_result.text:
        kg_text = kg_result.text[:kg_budget]
        sections.append(kg_text)
        budget -= len(kg_text)

    # 区域2: 向量检索原文 chunks
    if chunks:
        sections.append("\n=== 相关原文 ===")
        used = 0
        for c in chunks:
            if used >= chunk_budget:
                break
            line = f"[{c.doc_type or '文档'}][{c.publish_time[:10] if c.publish_time else ''}] {c.text[:300]}"
            sections.append(line)
            used += len(line)

    # 区域3: KG 关系原文佐证
    if kg_result and kg_result.evidence_chunks:
        sections.append("\n=== 关系佐证 ===")
        used = 0
        for ec in kg_result.evidence_chunks[:3]:
            if used >= evidence_budget:
                break
            title = ec.source_doc_title or "来源文档"
            line = f"- {ec.text[:200]}（来源: {title}）"
            sections.append(line)
            used += len(line)

    return "\n".join(sections)

"""retrieval 模块 — 三层检索 API

L1: semantic_search   — 纯向量检索
L2: kg_enhanced_search — KG + chunk 溯源
L3: hybrid_search     — 混合检索，产出 merged_context
"""
from retrieval.models import ChunkResult, KGResult, HybridResult
from retrieval.semantic import semantic_search
from retrieval.kg_enhanced import kg_enhanced_search, get_evidence_for_relationship
from retrieval.hybrid import hybrid_search

__all__ = [
    "ChunkResult",
    "KGResult",
    "HybridResult",
    "semantic_search",
    "kg_enhanced_search",
    "get_evidence_for_relationship",
    "hybrid_search",
]

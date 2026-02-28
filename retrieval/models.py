"""检索结果数据结构"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ChunkResult:
    """单个 chunk 检索结果"""
    chunk_id: int
    text: str
    score: float = 0.0
    extracted_text_id: int = 0
    doc_type: str = ""
    file_type: str = ""
    publish_time: str = ""
    source_doc_title: str = ""
    metadata: dict = field(default_factory=dict)


@dataclass
class KGResult:
    """KG 检索结果"""
    nodes: list = field(default_factory=list)       # [{id, name, type}]
    edges: list = field(default_factory=list)        # [{src, tgt, relation, strength, evidence}]
    causal_chains: list = field(default_factory=list) # [chain_str, ...]
    text: str = ""                                    # LLM 可读文本
    evidence_chunks: list = field(default_factory=list)  # [ChunkResult, ...]


@dataclass
class HybridResult:
    """混合检索结果"""
    chunks: list = field(default_factory=list)       # [ChunkResult, ...]
    kg: Optional[KGResult] = None
    merged_context: str = ""                          # 融合后的 LLM 可读文本

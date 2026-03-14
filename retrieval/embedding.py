"""bge-m3 embedding 服务 — 线程安全懒加载单例"""
import logging
import threading
from typing import Union

from config import EMBEDDING_MODEL, EMBEDDING_DIM

logger = logging.getLogger(__name__)

_model = None
_lock = threading.Lock()


def _get_model():
    """懒加载 bge-m3 模型（首次调用 ~5s，之后复用）"""
    global _model
    if _model is None:
        with _lock:
            if _model is None:
                import os
                os.environ.setdefault("HF_HUB_OFFLINE", "1")
                from FlagEmbedding import BGEM3FlagModel
                logger.info(f"加载 embedding 模型: {EMBEDDING_MODEL}")
                _model = BGEM3FlagModel(EMBEDDING_MODEL, use_fp16=True)
                logger.info("embedding 模型加载完成")
    return _model


def embed_texts(texts: list[str], batch_size: int = 64) -> list[list[float]]:
    """批量编码文本 → 1024 维向量列表

    Args:
        texts: 文本列表
        batch_size: 批量大小
    Returns:
        list[list[float]]，长度 = len(texts)，每个元素 1024 维
    """
    if not texts:
        return []
    model = _get_model()
    output = model.encode(texts, batch_size=batch_size, max_length=512)
    # BGEM3FlagModel.encode 返回 dict: {'dense_vecs': np.array}
    dense = output["dense_vecs"]
    return dense.tolist()


def embed_query(query: str) -> list[float]:
    """单条查询编码 → 1024 维向量"""
    results = embed_texts([query])
    return results[0] if results else [0.0] * EMBEDDING_DIM

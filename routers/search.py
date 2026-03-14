"""语义搜索路由 — Google 风格搜索框 + Chunk 结果展示"""
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/search", tags=["search"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("", response_class=HTMLResponse)
async def search_page(request: Request):
    """语义搜索页面"""
    return templates.TemplateResponse("semantic_search.html", {"request": request})


@router.get("/api/search", response_class=JSONResponse)
async def api_semantic_search(
    q: str = Query(..., min_length=1, description="搜索查询"),
    top_k: int = Query(10, ge=1, le=100, description="返回数量"),
    doc_type: str = Query("", description="文档类型过滤"),
    date_start: str = Query("", description="开始日期"),
    date_end: str = Query("", description="结束日期"),
):
    """语义搜索 API

    使用 bge-m3 向量检索 + Milvus ANN + MySQL text_chunks 补全
    """
    from retrieval.semantic import semantic_search

    # 构建过滤条件
    filters = {}
    if doc_type:
        filters["doc_types"] = [doc_type]
    if date_start or date_end:
        filters["date_range"] = (date_start or "2000-01-01", date_end or "2099-12-31")

    try:
        results = semantic_search(q, top_k=top_k, filters=filters if filters else None)

        # 格式化结果
        items = []
        for r in results:
            items.append({
                "chunk_id": r.chunk_id,
                "text": r.text,
                "score": round(r.score, 4),
                "extracted_text_id": r.extracted_text_id,
                "doc_type": r.doc_type,
                "file_type": r.file_type,
                "publish_time": r.publish_time,
                "source_doc_title": r.source_doc_title,
            })

        return {
            "query": q,
            "total": len(items),
            "items": items,
        }
    except Exception as e:
        logger.error(f"semantic_search error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/chunk/{chunk_id}", response_class=JSONResponse)
async def api_get_chunk_detail(chunk_id: int):
    """获取 chunk 详情（包含上下文）"""
    from retrieval.chunker import get_chunks_by_ids

    try:
        rows = get_chunks_by_ids([chunk_id])
        if not rows:
            return JSONResponse({"error": "Chunk not found"}, status_code=404)

        row = rows[0]

        # 获取相邻 chunks 作为上下文
        extracted_text_id = row.get("extracted_text_id")
        chunk_index = row.get("chunk_index", 0)

        context_before = []
        context_after = []

        if extracted_text_id:
            # 获取前后的 chunks
            from utils.db_utils import execute_query
            before_rows = execute_query(
                """SELECT id, chunk_text, chunk_index FROM text_chunks
                   WHERE extracted_text_id = %s AND chunk_index < %s
                   ORDER BY chunk_index DESC LIMIT 2""",
                [extracted_text_id, chunk_index]
            )
            context_before = [{"chunk_id": r["id"], "text": r["chunk_text"][:200]} for r in reversed(before_rows or [])]

            after_rows = execute_query(
                """SELECT id, chunk_text, chunk_index FROM text_chunks
                   WHERE extracted_text_id = %s AND chunk_index > %s
                   ORDER BY chunk_index ASC LIMIT 2""",
                [extracted_text_id, chunk_index]
            )
            context_after = [{"chunk_id": r["id"], "text": r["chunk_text"][:200]} for r in (after_rows or [])]

        return {
            "chunk_id": row["id"],
            "text": row.get("chunk_text", ""),
            "extracted_text_id": extracted_text_id,
            "chunk_index": chunk_index,
            "doc_type": row.get("doc_type"),
            "file_type": row.get("file_type"),
            "publish_time": str(row.get("publish_time") or ""),
            "source_doc_title": row.get("source_doc_title"),
            "context_before": context_before,
            "context_after": context_after,
        }
    except Exception as e:
        logger.error(f"get_chunk_detail error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
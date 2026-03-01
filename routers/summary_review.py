"""摘要质量审核 API — 复用 KG 审核的状态枚举（P2）

端点：
  GET  /api/summary-review/queue          — 获取审核队列
  POST /api/summary-review/{cs_id}/approve — 批准
  POST /api/summary-review/{cs_id}/reject  — 驳回
  POST /api/summary-review/{cs_id}/rerun   — 重新生成摘要（清除旧摘要重跑 Pipeline A）
"""
import logging
from datetime import datetime
from fastapi import APIRouter

from utils.db_utils import execute_cloud_query, execute_cloud_insert

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["summary-review"])


@router.get("/api/summary-review/queue")
async def get_summary_review_queue(
    status: str = "all",
    family: int = 2,
    keyword: str = "",
    limit: int = 50,
):
    """获取摘要审核队列

    Args:
        status: all / unreviewed / approved / rejected / pending_approval
        family: 1/2/3/4，默认2（族2研报/策略最需要审核）
        keyword: 关键词过滤（匹配 summary 字段）
        limit: 返回条数上限
    """
    conditions = []
    params = []

    if status != "all":
        conditions.append("cs.review_status = %s")
        params.append(status)

    if family:
        conditions.append("cs.family = %s")
        params.append(family)

    if keyword:
        conditions.append("cs.summary LIKE %s")
        params.append(f"%{keyword}%")

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)

    rows = execute_cloud_query(f"""
        SELECT
            cs.id, cs.extracted_text_id, cs.doc_type, cs.family,
            cs.summary, cs.fact_summary, cs.opinion_summary,
            cs.review_status, cs.review_note, cs.reviewed_at,
            cs.type_fields,
            LEFT(et.full_text, 300) AS source_preview,
            sd.title AS source_title,
            sm.stock_name, sm.stock_code
        FROM content_summaries cs
        LEFT JOIN extracted_texts et ON cs.extracted_text_id = et.id
        LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
        LEFT JOIN stock_mentions sm ON cs.extracted_text_id = sm.extracted_text_id
        {where_clause}
        GROUP BY cs.id
        ORDER BY cs.created_at DESC
        LIMIT %s
    """, params)

    return {"status": "ok", "total": len(rows), "items": rows}


@router.post("/api/summary-review/{cs_id}/approve")
async def approve_summary(cs_id: int, note: str = ""):
    """批准摘要（review_status → approved）"""
    try:
        execute_cloud_insert(
            """UPDATE content_summaries
               SET review_status='approved', review_note=%s, reviewed_at=%s
               WHERE id=%s""",
            [note or None, datetime.now(), cs_id],
        )
        logger.info(f"摘要审核 approved cs_id={cs_id}")
        return {"status": "ok", "cs_id": cs_id, "review_status": "approved"}
    except Exception as e:
        logger.error(f"approve 失败 cs_id={cs_id}: {e}")
        return {"status": "error", "message": str(e)}


@router.post("/api/summary-review/{cs_id}/reject")
async def reject_summary(cs_id: int, note: str = ""):
    """驳回摘要（review_status → rejected，对应 chunk score ×0.3 自动压制）"""
    try:
        execute_cloud_insert(
            """UPDATE content_summaries
               SET review_status='rejected', review_note=%s, reviewed_at=%s
               WHERE id=%s""",
            [note or None, datetime.now(), cs_id],
        )
        logger.info(f"摘要审核 rejected cs_id={cs_id} note={note}")
        return {"status": "ok", "cs_id": cs_id, "review_status": "rejected"}
    except Exception as e:
        logger.error(f"reject 失败 cs_id={cs_id}: {e}")
        return {"status": "error", "message": str(e)}


@router.post("/api/summary-review/{cs_id}/rerun")
async def rerun_summary(cs_id: int):
    """重新生成摘要（清除旧记录，重跑 Pipeline A）

    适用于驳回后想重新生成更准确摘要的场景。
    """
    try:
        # 查出对应的 extracted_text_id
        rows = execute_cloud_query(
            "SELECT extracted_text_id FROM content_summaries WHERE id=%s",
            [cs_id],
        )
        if not rows:
            return {"status": "error", "message": f"cs_id={cs_id} 不存在"}

        et_id = rows[0]["extracted_text_id"]

        # 删除旧摘要，重置 summary_status
        execute_cloud_insert(
            "DELETE FROM content_summaries WHERE id=%s",
            [cs_id],
        )
        execute_cloud_insert(
            "UPDATE extracted_texts SET summary_status=NULL WHERE id=%s",
            [et_id],
        )

        # 重跑 Pipeline A（在后台线程执行，不阻塞请求）
        import threading
        def _rerun():
            try:
                from cleaning.unified_pipeline import process_single
                process_single(et_id, need_a=True, need_b=False, need_c=False)
            except Exception as ex:
                logger.error(f"rerun Pipeline A 失败 et_id={et_id}: {ex}")

        t = threading.Thread(target=_rerun, daemon=True)
        t.start()

        logger.info(f"摘要重新生成 cs_id={cs_id} et_id={et_id}")
        return {"status": "ok", "message": f"已触发重新生成，et_id={et_id}"}
    except Exception as e:
        logger.error(f"rerun 失败 cs_id={cs_id}: {e}")
        return {"status": "error", "message": str(e)}

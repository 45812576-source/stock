"""选股机器人 API 路由"""
import logging
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/selector", tags=["selector"])


class SelectorRequest(BaseModel):
    message: str
    need_summary: bool = False


@router.post("/api/run", response_class=JSONResponse)
def api_run_selector(req: SelectorRequest):
    """运行选股机器人

    Body: {"message": "用户输入", "need_summary": false}
    Returns: {candidates_count, filtered_count, stocks: [...], summary, debug}
    """
    if not req.message.strip():
        return JSONResponse({"error": "message is empty"}, status_code=400)
    try:
        from stock_selector.selector_engine import run_selector
        result = run_selector(req.message, need_summary=req.need_summary)
        return result
    except Exception as e:
        logger.exception(f"selector api error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

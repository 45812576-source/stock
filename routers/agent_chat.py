"""Agent 对话 — FastAPI 路由

提供 DeepSeek function calling Agent 的 HTTP 接口：
  POST /agent/chat        — 发送消息，返回 Agent 回答（同步）
  POST /agent/chat/stream — 发送消息，流式返回最终回答
  GET  /agent/            — Agent 对话页面
"""
import json
import logging
from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from pydantic import BaseModel

from utils.auth_deps import get_current_user, TokenData
from utils.quota_service import check_quota, consume_quota

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/agent", tags=["agent"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# 简单的内存会话存储（进程级别，重启清空）
_sessions: dict[str, list] = {}


class ChatRequest(BaseModel):
    message: str
    session_id: str = "default"


@router.get("/", response_class=HTMLResponse)
async def agent_page(request: Request):
    return templates.TemplateResponse("agent_chat.html", {"request": request})


@router.post("/chat")
async def agent_chat(req: ChatRequest, user: TokenData = Depends(get_current_user)):
    """同步 Agent 对话接口"""
    from agent.executor import run_agent

    user_id = user.user_id

    # 检查AI对话配额
    can_chat, msg = check_quota(user_id, 'ai_chat')
    if not can_chat:
        return JSONResponse({"ok": False, "error": msg}, status_code=403)

    # 消耗配额
    consume_quota(user_id, 'ai_chat', 1)

    session_id = req.session_id
    history = _sessions.get(session_id, [])

    try:
        reply = run_agent(req.message, history=history)
    except Exception as e:
        logger.error(f"Agent 执行失败: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    # 更新会话历史
    history = history + [
        {"role": "user", "content": req.message},
        {"role": "assistant", "content": reply},
    ]
    _sessions[session_id] = history[-20:]  # 最多保留20轮

    return JSONResponse({"ok": True, "reply": reply, "session_id": session_id})


@router.post("/chat/stream")
async def agent_chat_stream(req: ChatRequest, user: TokenData = Depends(get_current_user)):
    """流式 Agent 对话接口（SSE）"""
    from agent.executor import run_agent_stream

    user_id = user.user_id

    # 检查AI对话配额
    can_chat, msg = check_quota(user_id, 'ai_chat')
    if not can_chat:
        return JSONResponse({"ok": False, "error": msg}, status_code=403)

    # 消耗配额（预先扣除，流式完成后不做回滚）
    consume_quota(user_id, 'ai_chat', 1)

    session_id = req.session_id
    history = _sessions.get(session_id, [])
    collected = []

    def generate():
        try:
            for chunk in run_agent_stream(req.message, history=history):
                collected.append(chunk)
                yield f"data: {json.dumps({'chunk': chunk}, ensure_ascii=False)}\n\n"
        except Exception as e:
            logger.error(f"Agent 流式执行失败: {e}")
            yield f"data: {json.dumps({'error': str(e)}, ensure_ascii=False)}\n\n"
        finally:
            # 更新会话历史
            full_reply = "".join(collected)
            if full_reply:
                new_history = history + [
                    {"role": "user", "content": req.message},
                    {"role": "assistant", "content": full_reply},
                ]
                _sessions[session_id] = new_history[-20:]
            yield "data: [DONE]\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@router.get("/history/{session_id}")
async def get_history(session_id: str):
    """获取会话历史"""
    history = _sessions.get(session_id, [])
    return JSONResponse({"session_id": session_id, "history": history})


@router.delete("/history/{session_id}")
async def clear_history(session_id: str):
    """清空会话历史"""
    _sessions.pop(session_id, None)
    return JSONResponse({"ok": True})

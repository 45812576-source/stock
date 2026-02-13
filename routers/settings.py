"""系统设置路由 — API配置 / 非结构化信息源 / 结构化信息源"""
import json
import os
import logging
import threading
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from utils.db_utils import execute_query, execute_insert
from utils.sys_config import get_config, set_config
from utils.fetch_config import (
    load_fetch_settings, save_fetch_settings,
    add_source, add_custom_source, delete_source, get_available_sources,
    SOURCE_GROUPS, SOURCE_CATALOG,
)
from utils.skill_registry import get_analysis_registry, get_skill_content

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/settings", tags=["settings"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


# ==================== 结构化数据批量任务 ====================
_struct_tasks = {}


# ==================== 公共上下文 ====================

def _db_stats():
    stats = {}
    for table in ["raw_items", "cleaned_items", "stock_info", "stock_daily",
                   "capital_flow", "deep_research", "kg_entities", "kg_relations"]:
        try:
            rows = execute_query(f"SELECT count(*) as cnt FROM {table}")
            stats[table] = rows[0]["cnt"]
        except Exception:
            stats[table] = 0
    return stats


def _common_ctx(tab: str):
    return {
        "active_page": "settings",
        "tab": tab,
        "db_stats": _db_stats(),
    }


# ==================== 页面路由 ====================

@router.get("", response_class=HTMLResponse)
async def settings_api_page(request: Request):
    """Tab 1: API 配置"""
    ctx = _common_ctx("api")
    ctx["request"] = request

    ctx["api_usage"] = execute_query(
        """SELECT api_name, call_date, call_count, input_tokens, output_tokens, cost_usd
           FROM api_usage ORDER BY call_date DESC LIMIT 30"""
    )
    ctx["pipeline_logs"] = execute_query(
        "SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 20"
    )
    ctx["config"] = {
        "claude_api_key": get_config("claude_api_key") or os.getenv("ANTHROPIC_API_KEY", "") or os.getenv("ANTHROPIC_AUTH_TOKEN", ""),
        "claude_base_url": get_config("claude_base_url") or os.getenv("ANTHROPIC_BASE_URL", ""),
        "claude_model": get_config("claude_model") or "claude-sonnet-4-20250514",
    }
    return templates.TemplateResponse("settings.html", ctx)


@router.get("/sources", response_class=HTMLResponse)
async def settings_sources_page(request: Request):
    """Tab 2: 非结构化信息源"""
    ctx = _common_ctx("sources")
    ctx["request"] = request

    fetch_settings = load_fetch_settings()
    source_groups = []
    for gkey, gcfg in SOURCE_GROUPS.items():
        sources = []
        for skey, scfg in fetch_settings["sources"].items():
            if scfg["group"] == gkey:
                sources.append({
                    "key": skey,
                    "label": scfg["label"],
                    "enabled": scfg.get("enabled", False),
                    "desc": scfg.get("desc", ""),
                    "icon": scfg.get("icon", "article"),
                    "limit": scfg.get("limit"),
                    "max_pages": scfg.get("max_pages"),
                })
        source_groups.append({
            "key": gkey,
            "label": gcfg["label"],
            "icon": gcfg["icon"],
            "color": gcfg["color"],
            "sources": sources,
        })

    ctx["source_groups"] = source_groups
    ctx["fetch_settings"] = fetch_settings
    ctx["zsxq_cookie"] = get_config("zsxq_cookie") or os.getenv("ZSXQ_COOKIE", "")
    return templates.TemplateResponse("settings.html", ctx)


@router.get("/structured", response_class=HTMLResponse)
async def settings_structured_page(request: Request):
    """Tab 3: 结构化信息源"""
    ctx = _common_ctx("structured")
    ctx["request"] = request

    try:
        ctx["watchlist"] = execute_query(
            """SELECT w.stock_code, COALESCE(s.stock_name, '') as stock_name
               FROM watchlist w LEFT JOIN stock_info s ON w.stock_code=s.stock_code
               ORDER BY w.added_at DESC"""
        )
    except Exception:
        ctx["watchlist"] = []

    try:
        rows = execute_query("SELECT MAX(trade_date) as last_date FROM stock_daily")
        ctx["last_daily_date"] = rows[0]["last_date"] if rows and rows[0]["last_date"] else None
    except Exception:
        ctx["last_daily_date"] = None

    ctx["now_date"] = datetime.now().strftime("%Y-%m-%d")
    return templates.TemplateResponse("settings.html", ctx)


@router.get("/skills", response_class=HTMLResponse)
async def settings_skills_page(request: Request):
    """Tab 4: Skill 配置"""
    ctx = _common_ctx("skills")
    ctx["request"] = request
    ctx["registry"] = get_analysis_registry()
    return templates.TemplateResponse("settings.html", ctx)


# ==================== API 配置 ====================

@router.post("/api/save-config")
async def save_config(request: Request):
    """保存 Claude API 配置"""
    data = await request.json()
    saved = []
    for key in ["claude_api_key", "claude_base_url", "claude_model"]:
        if key in data:
            set_config(key, data[key])
            saved.append(key)
    # 重置 client 缓存
    import utils.claude_client as cc
    cc.client = None
    cc._last_key = None
    return JSONResponse({"ok": True, "saved": saved})


@router.post("/api/test-claude")
async def test_claude(request: Request):
    """测试 Claude API 连通性"""
    try:
        from utils.claude_client import call_claude
        result = call_claude("回复OK即可", "测试连通性", max_tokens=20)
        return JSONResponse({"ok": True, "response": result[:100]})
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {str(e)[:200]}"})


@router.get("/api/skill-content/{skill_name}")
async def api_skill_content(skill_name: str):
    """返回 Skill 文件内容（用于预览）"""
    content = get_skill_content(skill_name)
    if content is None:
        return JSONResponse({"ok": False, "error": "Skill 文件不存在"}, status_code=404)
    return JSONResponse({"ok": True, "content": content})

# ==================== 非结构化源配置 ====================

@router.post("/api/save-sources")
async def save_sources(request: Request):
    """保存非结构化信息源配置"""
    data = await request.json()
    settings = load_fetch_settings()

    # 更新 news_hours
    if "news_hours" in data:
        settings["news_hours"] = int(data["news_hours"])

    # 更新各源的 enabled / limit / max_pages
    if "sources" in data:
        for skey, updates in data["sources"].items():
            if skey in settings["sources"]:
                for field in ["enabled", "limit", "max_pages"]:
                    if field in updates:
                        settings["sources"][skey][field] = updates[field]

    save_fetch_settings(settings)

    # ZSXQ cookie 存 DB
    if "zsxq_cookie" in data:
        set_config("zsxq_cookie", data["zsxq_cookie"])

    return JSONResponse({"ok": True})


@router.post("/api/add-source")
async def api_add_source(request: Request):
    """添加信息源（从预置目录）"""
    data = await request.json()
    key = data.get("key", "")
    ok, msg = add_source(key)
    if not ok:
        return JSONResponse({"ok": False, "error": msg}, status_code=400)
    return JSONResponse({"ok": True})


@router.post("/api/add-custom-source")
async def api_add_custom_source(request: Request):
    """添加自定义信息源"""
    data = await request.json()
    ok, msg = add_custom_source(
        key=data.get("key", "").strip(),
        label=data.get("label", "").strip(),
        group=data.get("group", "news"),
        desc=data.get("desc", "").strip(),
        icon=data.get("icon", "article").strip(),
        fetcher_type=data.get("fetcher_type", "jasper"),
        limit=data.get("limit"),
        max_pages=data.get("max_pages"),
    )
    if not ok:
        return JSONResponse({"ok": False, "error": msg}, status_code=400)
    return JSONResponse({"ok": True})


@router.delete("/api/delete-source/{key}")
async def api_delete_source(key: str):
    """删除信息源"""
    ok, msg = delete_source(key)
    if not ok:
        return JSONResponse({"ok": False, "error": msg}, status_code=400)
    return JSONResponse({"ok": True})


@router.get("/api/available-sources")
async def api_available_sources():
    """返回可添加的源列表"""
    available = get_available_sources()
    return JSONResponse({"sources": available})


# ==================== 结构化数据批量下载 ====================

@router.post("/api/struct-batch")
async def struct_batch_download(request: Request, background_tasks: BackgroundTasks):
    """触发结构化数据批量下载"""
    data = await request.json()
    pool = data.get("pool", "watchlist")  # watchlist / all / custom
    custom_codes = data.get("custom_codes", [])
    start_date = data.get("start_date", "20240101")
    end_date = data.get("end_date", datetime.now().strftime("%Y%m%d"))
    data_types = data.get("data_types", ["daily", "capital", "financial"])

    # 确定股票列表
    if pool == "all":
        try:
            rows = execute_query("SELECT stock_code FROM stock_info LIMIT 5000")
            codes = [r["stock_code"] for r in rows]
        except Exception:
            codes = []
    elif pool == "custom":
        codes = [c.strip() for c in custom_codes if c.strip()]
    else:  # watchlist
        try:
            rows = execute_query("SELECT stock_code FROM watchlist")
            codes = [r["stock_code"] for r in rows]
        except Exception:
            codes = []

    if not codes:
        return JSONResponse({"ok": False, "error": "没有可下载的股票"}, status_code=400)

    task_id = f"struct_{int(datetime.now().timestamp())}"
    total_steps = len(codes) * len(data_types)
    _struct_tasks[task_id] = {
        "status": "running", "progress": 0, "total": total_steps,
        "current": "", "results": {}, "started_at": datetime.now().isoformat(),
    }

    def _run():
        task = _struct_tasks[task_id]
        step = 0
        for code in codes:
            for dtype in data_types:
                task["current"] = f"{code} - {dtype}"
                task["progress"] = step
                try:
                    from ingestion.akshare_source import (
                        fetch_stock_daily, fetch_capital_flow, fetch_financial_data
                    )
                    if dtype == "daily":
                        cnt = fetch_stock_daily(code, start_date=start_date, end_date=end_date)
                    elif dtype == "capital":
                        cnt = fetch_capital_flow(code)
                    elif dtype == "financial":
                        cnt = fetch_financial_data(code)
                    else:
                        cnt = 0
                    task["results"][f"{code}_{dtype}"] = {"ok": True, "count": cnt}
                except Exception as e:
                    task["results"][f"{code}_{dtype}"] = {"ok": False, "error": str(e)[:100]}
                step += 1
        task["status"] = "done"
        task["progress"] = total_steps

    background_tasks.add_task(_run)
    return JSONResponse({"ok": True, "task_id": task_id, "total": total_steps, "codes_count": len(codes)})


@router.get("/api/struct-task/{task_id}")
async def struct_task_status(task_id: str):
    """查询结构化数据下载任务状态"""
    task = _struct_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return JSONResponse({
        "status": task["status"],
        "progress": task["progress"],
        "total": task["total"],
        "current": task["current"],
    })

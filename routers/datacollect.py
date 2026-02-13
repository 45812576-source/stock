"""数据采集与清洗路由 — 采集管理 / 清洗管理 / 数据浏览"""
import json
import logging
import threading
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Request, BackgroundTasks, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from utils.db_utils import execute_query, execute_insert
from utils.fetch_config import load_fetch_settings, save_fetch_settings, SOURCE_GROUPS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/data", tags=["data"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# ==================== 采集配置 ====================
# 源分组 — 仅本模块用于 DB 统计查询的 db_names 映射
_SOURCE_GROUP_DB = {
    "news":      {"db_names": ["jasper"]},
    "report":    {"db_names": ["djyanbao", "fxbaogao", "eastmoney_report"]},
    "community": {"db_names": ["zsxq"]},
}

# ==================== 辅助查询 ====================

def _get_cleaning_stats():
    """获取清洗统计"""
    try:
        from cleaning.batch_cleaner import get_cleaning_stats
        return get_cleaning_stats()
    except Exception:
        return {"pending": 0, "processing": 0, "cleaned": 0, "failed": 0, "today_cleaned": 0, "today_fetched": 0}


def _get_source_group_stats():
    """按分组获取数据源状态"""
    groups = []
    for gkey, gcfg in SOURCE_GROUPS.items():
        db_names = _SOURCE_GROUP_DB.get(gkey, {}).get("db_names", [])
        placeholders = ",".join(["?"] * len(db_names)) if db_names else "''"

        # 今日采集数
        today = datetime.now().strftime("%Y-%m-%d")
        try:
            rows = execute_query(
                f"""SELECT COUNT(*) as cnt FROM raw_items r
                    JOIN data_sources d ON r.source_id=d.id
                    WHERE d.name IN ({placeholders}) AND date(r.fetched_at)=?""",
                db_names + [today])
            today_count = rows[0]["cnt"] if rows else 0
        except Exception:
            today_count = 0

        # 最近采集时间
        try:
            rows = execute_query(
                f"""SELECT MAX(r.fetched_at) as last_fetch FROM raw_items r
                    JOIN data_sources d ON r.source_id=d.id
                    WHERE d.name IN ({placeholders})""",
                db_names)
            last_fetch = rows[0]["last_fetch"] if rows and rows[0]["last_fetch"] else None
        except Exception:
            last_fetch = None

        # 计算相对时间
        last_sync_text = "从未同步"
        status = "offline"
        if last_fetch:
            try:
                dt = datetime.strptime(last_fetch[:19], "%Y-%m-%d %H:%M:%S")
                diff = (datetime.now() - dt).total_seconds()
                if diff < 60:
                    last_sync_text = "刚刚"
                elif diff < 3600:
                    last_sync_text = f"{int(diff / 60)} 分钟前"
                elif diff < 86400:
                    last_sync_text = f"{int(diff / 3600)} 小时前"
                else:
                    last_sync_text = f"{int(diff / 86400)} 天前"
                status = "online" if diff < 86400 else "idle"
            except Exception:
                last_sync_text = last_fetch[:16]

        # 子源列表
        settings = load_fetch_settings()
        sub_sources = []
        for skey, scfg in settings["sources"].items():
            if scfg["group"] == gkey:
                sub_sources.append({
                    "key": skey,
                    "label": scfg["label"],
                    "enabled": scfg.get("enabled", False),
                    "desc": scfg.get("desc", ""),
                })

        groups.append({
            "key": gkey,
            "label": gcfg["label"],
            "icon": gcfg["icon"],
            "color": gcfg["color"],
            "status": status,
            "today_count": today_count,
            "last_sync_text": last_sync_text,
            "sub_sources": sub_sources,
        })
    return groups


def _get_recent_items(limit=30):
    """获取最近采集的条目（活动流）"""
    try:
        rows = execute_query(
            """SELECT r.id, r.title, r.content, r.processing_status, r.fetched_at,
                      r.item_type, r.meta_json, d.name as source_name
               FROM raw_items r JOIN data_sources d ON r.source_id=d.id
               ORDER BY r.fetched_at DESC LIMIT ?""",
            [limit])
        items = []
        for r in (rows or []):
            d = dict(r)
            # 确定显示的源标签
            source_label = d["source_name"]
            meta = {}
            try:
                meta = json.loads(d.get("meta_json") or "{}")
            except Exception:
                pass
            sub_source = meta.get("sub_source", "")
            if sub_source:
                source_label = sub_source

            # 确定分组颜色
            group_color = "blue"
            for gkey, gdb in _SOURCE_GROUP_DB.items():
                if d["source_name"] in gdb["db_names"]:
                    group_color = SOURCE_GROUPS.get(gkey, {}).get("color", "blue")
                    break

            # 时间格式化
            time_str = ""
            if d.get("fetched_at"):
                try:
                    time_str = d["fetched_at"][11:19]  # HH:MM:SS
                except Exception:
                    time_str = ""

            items.append({
                "id": d["id"],
                "title": d.get("title") or "",
                "content_preview": (d.get("content") or "")[:200],
                "status": d["processing_status"],
                "time": time_str,
                "source_label": source_label,
                "group_color": group_color,
                "item_type": d.get("item_type") or "",
            })
        return items
    except Exception as e:
        logger.error(f"获取最近条目失败: {e}")
        return []


# ==================== 后台任务管理 ====================
# 简单的内存任务状态（单进程足够）
_bg_tasks = {}


def _get_task_status(task_id):
    return _bg_tasks.get(task_id)


def _build_fetch_steps(src_cfg, hours, source_filter=None):
    """构建采集步骤列表 — 遍历所有 enabled 源，按 group 排序"""
    group_order = list(SOURCE_GROUPS.keys())
    steps = []
    for key, cfg in src_cfg.items():
        if source_filter and key not in source_filter:
            continue
        if cfg.get("enabled"):
            steps.append((key, cfg["label"], cfg.get("group", "news")))
    steps.sort(key=lambda s: group_order.index(s[2]) if s[2] in group_order else 99)
    return steps


def _fetch_zsxq(cfg, hours):
    """知识星球采集辅助"""
    import os
    from utils.sys_config import get_config
    cookie = get_config("zsxq_cookie") or os.environ.get("ZSXQ_COOKIE", "")
    if not cookie:
        return 0
    from config import ZSXQ_GROUP_ID
    from ingestion.zsxq_source import fetch_zsxq_data
    result = fetch_zsxq_data(ZSXQ_GROUP_ID, cookie, hours=hours,
                             max_pages=cfg.get("max_pages", 5))
    return result.get("saved", 0)


# fetcher_type → 执行函数 的注册表
_FETCHER_REGISTRY = {
    "jasper":    lambda key, cfg, hours: __import__("ingestion.jasper_source", fromlist=["JasperSource"]).JasperSource().fetch(hours=hours, sources=[key]),
    "djyanbao":  lambda key, cfg, hours: __import__("ingestion.djyanbao_source", fromlist=["DjyanbaoSource"]).DjyanbaoSource().fetch(limit=cfg.get("limit", 100)),
    "fxbaogao":  lambda key, cfg, hours: __import__("ingestion.fxbaogao_source", fromlist=["FxbaogaoSource"]).FxbaogaoSource().fetch(limit=cfg.get("limit", 100)),
    "em_report": lambda key, cfg, hours: __import__("ingestion.eastmoney_report_source", fromlist=["EastmoneyReportSource"]).EastmoneyReportSource().fetch(limit=cfg.get("limit", 10)),
    "zsxq":      lambda key, cfg, hours: _fetch_zsxq(cfg, hours),
}


def _execute_fetch_step(key, settings):
    """执行单个采集步骤，基于 fetcher_type dispatch"""
    src_cfg = settings["sources"]
    hours = settings["news_hours"]
    cfg = src_cfg.get(key, {})
    fetcher_type = cfg.get("fetcher_type", "")
    fetcher = _FETCHER_REGISTRY.get(fetcher_type)
    if fetcher:
        return fetcher(key, cfg, hours)
    return 0


# ==================== 页面路由 ====================

@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def data_collection(request: Request):
    """数据采集管理页（Collection tab）"""
    stats = _get_cleaning_stats()
    groups = _get_source_group_stats()
    recent = _get_recent_items(30)

    return templates.TemplateResponse("datacollect.html", {
        "request": request,
        "active_page": "data",
        "tab": "collection",
        "stats": stats,
        "groups": groups,
        "recent_items": recent,
    })


@router.get("/cleaning", response_class=HTMLResponse)
async def data_cleaning(request: Request):
    """清洗管理页（Cleaning tab）"""
    stats = _get_cleaning_stats()

    # 最近 pipeline runs
    try:
        runs = execute_query(
            "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 10")
    except Exception:
        runs = []

    return templates.TemplateResponse("datacollect.html", {
        "request": request,
        "active_page": "data",
        "tab": "cleaning",
        "stats": stats,
        "pipeline_runs": runs or [],
    })


@router.get("/browse", response_class=HTMLResponse)
async def data_browse(request: Request, status: str = "", source: str = "", q: str = ""):
    """数据浏览页（Browse tab）"""
    stats = _get_cleaning_stats()

    # 数据源列表
    try:
        source_names = [s["name"] for s in execute_query("SELECT DISTINCT name FROM data_sources")]
    except Exception:
        source_names = []

    # 查询条目
    sql = """SELECT r.id, r.title, r.content, r.processing_status, r.fetched_at,
                    r.item_type, r.url, d.name as source_name
             FROM raw_items r JOIN data_sources d ON r.source_id=d.id"""
    params = []
    conditions = []

    if status:
        conditions.append("r.processing_status=?")
        params.append(status)
    if source:
        conditions.append("d.name=?")
        params.append(source)
    if q:
        conditions.append("(r.title LIKE ? OR r.content LIKE ?)")
        params.extend([f"%{q}%", f"%{q}%"])

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY r.fetched_at DESC LIMIT 100"

    try:
        items = execute_query(sql, params) or []
    except Exception:
        items = []

    return templates.TemplateResponse("datacollect.html", {
        "request": request,
        "active_page": "data",
        "tab": "browse",
        "stats": stats,
        "source_names": source_names,
        "browse_items": [dict(i) for i in items],
        "filter_status": status,
        "filter_source": source,
        "filter_q": q,
    })


# ==================== API 操作 ====================

@router.post("/sync-all", response_class=JSONResponse)
async def sync_all(background_tasks: BackgroundTasks):
    """一键全量采集"""
    task_id = f"sync_all_{int(datetime.now().timestamp())}"
    settings = load_fetch_settings()
    steps = _build_fetch_steps(settings["sources"], settings["news_hours"])

    if not steps:
        return JSONResponse({"error": "没有启用任何采集源"}, status_code=400)

    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": len(steps),
        "current": "", "results": [], "started_at": datetime.now().isoformat(),
    }

    def _run():
        task = _bg_tasks[task_id]
        for i, (key, label, group) in enumerate(steps):
            task["current"] = label
            task["progress"] = i
            try:
                count = _execute_fetch_step(key, settings)
                task["results"].append({"source": label, "count": count, "ok": True})
            except Exception as e:
                task["results"].append({"source": label, "error": str(e), "ok": False})
                logger.error(f"采集 {label} 失败: {e}")
        task["status"] = "done"
        task["progress"] = task["total"]

    background_tasks.add_task(_run)
    return {"task_id": task_id}


@router.post("/sync-group/{group_key}", response_class=JSONResponse)
async def sync_group(group_key: str, background_tasks: BackgroundTasks):
    """按分组采集"""
    settings = load_fetch_settings()
    # 找出该分组下的源
    group_sources = [k for k, v in settings["sources"].items() if v["group"] == group_key]
    steps = _build_fetch_steps(settings["sources"], settings["news_hours"], source_filter=group_sources)

    if not steps:
        return JSONResponse({"error": "该分组没有启用的采集源"}, status_code=400)

    task_id = f"sync_{group_key}_{int(datetime.now().timestamp())}"
    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": len(steps),
        "current": "", "results": [], "started_at": datetime.now().isoformat(),
    }

    def _run():
        task = _bg_tasks[task_id]
        for i, (key, label, group) in enumerate(steps):
            task["current"] = label
            task["progress"] = i
            try:
                count = _execute_fetch_step(key, settings)
                task["results"].append({"source": label, "count": count, "ok": True})
            except Exception as e:
                task["results"].append({"source": label, "error": str(e), "ok": False})
        task["status"] = "done"
        task["progress"] = task["total"]

    background_tasks.add_task(_run)
    return {"task_id": task_id}


@router.get("/task-status/{task_id}", response_class=JSONResponse)
async def task_status(task_id: str):
    """查询后台任务状态"""
    task = _bg_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return task


@router.post("/clean", response_class=JSONResponse)
async def run_clean(background_tasks: BackgroundTasks, limit: int = 20, deep: bool = False):
    """批量清洗"""
    task_id = f"clean_{int(datetime.now().timestamp())}"
    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": 1,
        "current": "批量清洗", "results": [],
    }

    def _run():
        task = _bg_tasks[task_id]
        try:
            from cleaning.batch_cleaner import batch_clean
            r = batch_clean(limit=limit, deep_analysis=deep)
            task["results"].append({
                "source": "批量清洗", "ok": True,
                "count": f"成功{r['success']}, 失败{r['failed']}, 总计{r['total']}",
            })
        except Exception as e:
            task["results"].append({"source": "批量清洗", "error": str(e), "ok": False})
        task["status"] = "done"
        task["progress"] = 1

    background_tasks.add_task(_run)
    return {"task_id": task_id}


@router.post("/retry-failed", response_class=JSONResponse)
async def retry_failed(background_tasks: BackgroundTasks):
    """重试失败条目"""
    task_id = f"retry_{int(datetime.now().timestamp())}"
    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": 1,
        "current": "重试失败", "results": [],
    }

    def _run():
        task = _bg_tasks[task_id]
        try:
            from cleaning.batch_cleaner import retry_failed as _retry
            r = _retry(limit=20)
            task["results"].append({
                "source": "重试失败", "ok": True,
                "count": f"成功{r['success']}, 失败{r['failed']}",
            })
        except Exception as e:
            task["results"].append({"source": "重试失败", "error": str(e), "ok": False})
        task["status"] = "done"
        task["progress"] = 1

    background_tasks.add_task(_run)
    return {"task_id": task_id}


@router.post("/generate-dashboard", response_class=JSONResponse)
async def generate_dashboard(background_tasks: BackgroundTasks):
    """生成 Dashboard 榜单"""
    task_id = f"dashboard_{int(datetime.now().timestamp())}"
    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": 1,
        "current": "榜单生成", "results": [],
    }

    def _run():
        task = _bg_tasks[task_id]
        try:
            from dashboards.pipeline import generate_all_dashboards
            result = generate_all_dashboards()
            task["results"].append({"source": "榜单生成", "ok": True, "count": str(result)})
        except Exception as e:
            task["results"].append({"source": "榜单生成", "error": str(e), "ok": False})
        task["status"] = "done"
        task["progress"] = 1

    background_tasks.add_task(_run)
    return {"task_id": task_id}


@router.get("/recent-items", response_class=JSONResponse)
async def api_recent_items(limit: int = 20, offset: int = 0):
    """获取最近采集条目（HTMX 分页加载）"""
    try:
        rows = execute_query(
            """SELECT r.id, r.title, r.content, r.processing_status, r.fetched_at,
                      r.item_type, r.meta_json, d.name as source_name
               FROM raw_items r JOIN data_sources d ON r.source_id=d.id
               ORDER BY r.fetched_at DESC LIMIT ? OFFSET ?""",
            [limit, offset])
        items = []
        for r in (rows or []):
            d = dict(r)
            meta = {}
            try:
                meta = json.loads(d.get("meta_json") or "{}")
            except Exception:
                pass
            items.append({
                "id": d["id"],
                "title": d.get("title") or "",
                "content_preview": (d.get("content") or "")[:200],
                "status": d["processing_status"],
                "time": (d.get("fetched_at") or "")[:19],
                "source_label": meta.get("sub_source", d["source_name"]),
                "source_name": d["source_name"],
            })
        return items
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

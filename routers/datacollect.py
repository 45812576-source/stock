"""数据采集与清洗路由 — 采集管理 / 清洗管理 / 数据浏览"""
import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import List

from fastapi import APIRouter, Request, BackgroundTasks, Form, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from utils.db_utils import execute_query, execute_insert
from utils.fetch_config import load_fetch_settings, save_fetch_settings, SOURCE_GROUPS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/data", tags=["data"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# ==================== 采集配置 ====================
# 源分组 — 仅本模块用于 DB 统计查询的 db_names 映射
_SOURCE_GROUP_DB = {
    "news":      {"db_names": ["jasper", "cninfo_notice", "earnings"]},
    "report":    {"db_names": ["djyanbao", "fxbaogao", "eastmoney_report"]},
    "community": {"db_names": ["zsxq"]},
    "market":    {"db_names": []},
}

# ==================== 辅助查询 ====================

def _get_cleaning_stats():
    """获取清洗统计"""
    try:
        from cleaning.batch_cleaner import get_cleaning_stats
        return get_cleaning_stats()
    except Exception:
        return {"pending": 0, "processing": 0, "cleaned": 0, "failed": 0, "today_cleaned": 0, "today_fetched": 0}


def _get_source_context():
    """构建信息源配置上下文（供所有 tab 共用）"""
    try:
        from utils.sys_config import get_config
        fetch_settings = load_fetch_settings()
        source_groups = []
        for gkey, gcfg in SOURCE_GROUPS.items():
            sources = []
            for skey, scfg in fetch_settings["sources"].items():
                if scfg["group"] == gkey:
                    sources.append({
                        "key": skey, "label": scfg["label"],
                        "enabled": scfg.get("enabled", False),
                        "desc": scfg.get("desc", ""),
                        "icon": scfg.get("icon", "article"),
                        "limit": scfg.get("limit"),
                        "max_pages": scfg.get("max_pages"),
                    })
            source_groups.append({
                "key": gkey, "label": gcfg["label"],
                "icon": gcfg["icon"], "color": gcfg["color"],
                "sources": sources,
            })
        return {
            "fetch_settings": fetch_settings,
            "source_groups": source_groups,
            "zsxq_cookie": get_config("zsxq_cookie") or "",
            "zsxq_group_ids": get_config("zsxq_group_ids") or ",".join(__import__("config").ZSXQ_GROUP_IDS),
        }
    except Exception:
        return {"fetch_settings": {"news_hours": 24, "sources": {}}, "source_groups": [], "zsxq_cookie": "", "zsxq_group_ids": ""}


def _get_source_doc_summary():
    """获取文档库概况 — 云端 5 个 COUNT 并发执行 + 本地 text_chunks 统计"""
    from utils.db_utils import execute_cloud_query
    from concurrent.futures import ThreadPoolExecutor, as_completed as _asc
    queries = {
        "et_total":    "SELECT COUNT(*) as n FROM extracted_texts",
        "pipeline_a":  "SELECT COUNT(DISTINCT extracted_text_id) as n FROM content_summaries",
        "pipeline_b":  "SELECT COUNT(DISTINCT extracted_text_id) as n FROM stock_mentions",
        "pipeline_c":  "SELECT COUNT(*) as n FROM extracted_texts WHERE kg_status='done'",
        "doc_stats":   "SELECT source, COUNT(*) as doc_count, SUM(CASE WHEN extract_status IN ('extracted','ready_to_pipe','done') THEN 1 ELSE 0 END) as extracted_count FROM source_documents GROUP BY source",
    }
    results = {}
    try:
        with ThreadPoolExecutor(max_workers=5) as pool:
            futs = {pool.submit(execute_cloud_query, sql): key for key, sql in queries.items()}
            for fut in _asc(futs):
                key = futs[fut]
                try:
                    results[key] = fut.result()
                except Exception:
                    results[key] = None
        doc_stats = results.get("doc_stats") or []

        # 本地 text_chunks 统计（本地 MySQL）
        chunks_total = 0
        try:
            r = execute_query("SELECT COUNT(*) as n FROM text_chunks")
            chunks_total = (r or [{}])[0].get("n", 0)
        except Exception:
            pass

        return {
            "total":        sum(r["doc_count"] for r in doc_stats),
            "extracted":    sum(r["extracted_count"] or 0 for r in doc_stats),
            "source_count": len(doc_stats),
            "et_total":     (results.get("et_total") or [{}])[0].get("n", 0),
            "pipeline_a":   (results.get("pipeline_a") or [{}])[0].get("n", 0),
            "pipeline_b":   (results.get("pipeline_b") or [{}])[0].get("n", 0),
            "pipeline_c":   (results.get("pipeline_c") or [{}])[0].get("n", 0),
            "chunks_total": chunks_total,
        }
    except Exception:
        return {"total": 0, "extracted": 0, "source_count": 0, "et_total": 0,
                "pipeline_a": 0, "pipeline_b": 0, "pipeline_c": 0}


def _get_source_group_stats():
    """按分组获取数据源状态"""
    groups = []
    for gkey, gcfg in SOURCE_GROUPS.items():
        db_names = _SOURCE_GROUP_DB.get(gkey, {}).get("db_names", [])
        placeholders = ",".join(["?"] * len(db_names)) if db_names else "''"

        # 数据总量（raw_items + source_documents）
        try:
            rows = execute_query(
                f"""SELECT COUNT(*) as cnt FROM raw_items r
                    JOIN data_sources d ON r.source_id=d.id
                    WHERE d.name IN ({placeholders})""",
                db_names)
            total_count = rows[0]["cnt"] if rows else 0
        except Exception:
            total_count = 0

        try:
            rows = execute_query(
                f"""SELECT COUNT(*) as cnt FROM source_documents
                    WHERE source IN ({placeholders})""",
                db_names)
            total_count += rows[0]["cnt"] if rows else 0
        except Exception:
            pass

        # 最近24小时采集数
        try:
            rows = execute_query(
                f"""SELECT COUNT(*) as cnt FROM raw_items r
                    JOIN data_sources d ON r.source_id=d.id
                    WHERE d.name IN ({placeholders})
                      AND r.fetched_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)""",
                db_names)
            recent_count = rows[0]["cnt"] if rows else 0
        except Exception:
            recent_count = 0

        try:
            rows = execute_query(
                f"""SELECT COUNT(*) as cnt FROM source_documents
                    WHERE source IN ({placeholders})
                      AND created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)""",
                db_names)
            recent_count += rows[0]["cnt"] if rows else 0
        except Exception:
            pass

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

        # 也检查 source_documents 的最近时间
        try:
            rows = execute_query(
                f"""SELECT MAX(created_at) as last_fetch FROM source_documents
                    WHERE source IN ({placeholders})""",
                db_names)
            sd_last = rows[0]["last_fetch"] if rows and rows[0]["last_fetch"] else None
            if sd_last and (not last_fetch or sd_last > last_fetch):
                last_fetch = sd_last
        except Exception:
            pass

        # 计算相对时间
        last_sync_text = "从未同步"
        status = "offline"
        if last_fetch:
            try:
                if isinstance(last_fetch, datetime):
                    dt = last_fetch
                else:
                    dt = datetime.strptime(str(last_fetch)[:19], "%Y-%m-%d %H:%M:%S")
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
                last_sync_text = str(last_fetch)[:16]

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
            "total_count": total_count,
            "recent_count": recent_count,
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
                    if isinstance(d["fetched_at"], datetime):
                        time_str = d["fetched_at"].strftime("%H:%M:%S")
                    else:
                        time_str = str(d["fetched_at"])[11:19]
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


def _fetch_zsxq(cfg, hours=None, start_date=None, end_date=None):
    """知识星球采集辅助"""
    import os
    from utils.sys_config import get_config
    from config import ZSXQ_COOKIE, ZSXQ_GROUP_IDS
    cookie = get_config("zsxq_cookie") or os.environ.get("ZSXQ_COOKIE", "") or ZSXQ_COOKIE
    if not cookie:
        return 0
    group_ids_str = get_config("zsxq_group_ids") or ""
    group_ids = [g.strip() for g in group_ids_str.split(",") if g.strip()] or ZSXQ_GROUP_IDS
    from ingestion.zsxq_source import fetch_zsxq_data
    result = fetch_zsxq_data(
        group_ids=group_ids,
        token=cookie,
        hours=hours,
        start_date=start_date,
        end_date=end_date,
        max_pages=cfg.get("max_pages", 50),
    )
    return result.get("saved", 0)


# fetcher_type → 执行函数 的注册表
_FETCHER_REGISTRY = {
    "jasper":    lambda key, cfg, hours: __import__("ingestion.jasper_source", fromlist=["JasperSource"]).JasperSource().fetch(hours=hours, sources=[key]),
    "djyanbao":  lambda key, cfg, hours: __import__("ingestion.djyanbao_source", fromlist=["DjyanbaoSource"]).DjyanbaoSource().fetch(limit=cfg.get("limit", 100)),
    "fxbaogao":  lambda key, cfg, hours: __import__("ingestion.fxbaogao_source", fromlist=["FxbaogaoSource"]).FxbaogaoSource().fetch(limit=cfg.get("limit", 100)),
    "em_report": lambda key, cfg, hours: __import__("ingestion.eastmoney_report_source", fromlist=["EastmoneyReportSource"]).EastmoneyReportSource().fetch(limit=cfg.get("limit", 10)),
    "cninfo_notice": lambda key, cfg, hours: __import__("ingestion.cninfo_notice_source", fromlist=["CninfoNoticeSource"]).CninfoNoticeSource().fetch(days=max(1, hours // 24), limit=cfg.get("limit", 200)),
    "earnings": lambda key, cfg, hours: __import__("ingestion.earnings_source", fromlist=["EarningsSource"]).EarningsSource().fetch(limit=cfg.get("limit", 500)),
    "zsxq":      lambda key, cfg, hours: _fetch_zsxq(cfg, hours=hours),
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
def data_collection(request: Request):
    """重定向到非结构化信息源页面"""
    return RedirectResponse(url="/data/cleaning", status_code=302)


@router.get("/cleaning", response_class=HTMLResponse)
def data_cleaning(request: Request):
    """清洗管理页（Cleaning tab）— 只渲染骨架，统计数据由 JS 异步加载"""
    from config.doc_types import DOC_TYPES
    return templates.TemplateResponse("datacollect.html", {
        "request": request,
        "active_page": "data",
        "tab": "cleaning",
        "doc_types": DOC_TYPES,
        **_get_source_context(),
    })


@router.get("/input", response_class=HTMLResponse)
def data_input(request: Request):
    """手动录入页"""
    from config.doc_types import DOC_TYPES
    from utils.db_utils import execute_cloud_query
    try:
        recent_docs = execute_cloud_query(
            """SELECT id, doc_type, file_type, title, source, publish_date,
                      text_content, extract_status, created_at
               FROM source_documents WHERE source='manual'
               ORDER BY id DESC LIMIT 20"""
        )
    except Exception:
        recent_docs = []
    return templates.TemplateResponse("data_input.html", {
        "request": request,
        "active_page": "data",
        "doc_types": DOC_TYPES,
        "recent_docs": recent_docs or [],
    })


@router.get("/browse", response_class=HTMLResponse)
def data_browse(request: Request, status: str = "", source: str = "",
                q: str = "", item_type: str = ""):
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
    if item_type:
        conditions.append("r.item_type=?")
        params.append(item_type)
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
        "filter_item_type": item_type,
        "filter_q": q,
        "source_doc_summary": _get_source_doc_summary(),
        **_get_source_context(),
    })


# ==================== API 操作 ====================

@router.post("/sync-all", response_class=JSONResponse)
def sync_all():
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

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@router.post("/sync-group/{group_key}", response_class=JSONResponse)
def sync_group(group_key: str):
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

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@router.get("/task-status/{task_id}", response_class=JSONResponse)
def task_status(task_id: str):
    """查询后台任务状态"""
    task = _bg_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return task


@router.post("/clean", response_class=JSONResponse)
def run_clean(limit: int = 20, deep: bool = False):
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

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@router.post("/retry-failed", response_class=JSONResponse)
def retry_failed():
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

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@router.post("/generate-dashboard", response_class=JSONResponse)
def generate_dashboard():
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

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@router.get("/recent-items", response_class=JSONResponse)
def api_recent_items(limit: int = 20, offset: int = 0):
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
                "time": str(d.get("fetched_at") or "")[:19],
                "source_label": meta.get("sub_source", d["source_name"]),
                "source_name": d["source_name"],
            })
        return items
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ==================== 源文档导入与提取 API ====================

@router.post("/api/import-source-sql", response_class=JSONResponse)
def import_source_sql(sql_path: str = Form(...)):
    """导入 SQL dump 文件到 source_documents"""
    from pathlib import Path
    if not Path(sql_path).exists():
        return JSONResponse({"error": f"文件不存在: {sql_path}"}, status_code=400)

    task_id = f"import_sql_{int(datetime.now().timestamp())}"
    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": 1,
        "current": "导入SQL", "results": [],
    }

    def _run():
        task = _bg_tasks[task_id]
        try:
            from ingestion.source_extractor import import_sql_dump
            result = import_sql_dump(sql_path)
            task["results"].append({
                "source": "SQL导入", "ok": True,
                "count": f"总计{result['total']}, 导入{result['imported']}, 跳过{result['skipped']}",
            })
        except Exception as e:
            task["results"].append({"source": "SQL导入", "error": str(e), "ok": False})
        task["status"] = "done"
        task["progress"] = 1

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@router.post("/api/extract-batch", response_class=JSONResponse)
def extract_batch(
                  file_type: str = Form(None), limit: int = Form(50)):
    """批量提取源文档并灌入 extracted_texts
    file_type: 逗号分隔的类型列表，如 'pdf,image'，None=全部
    """
    ft_list = [ft.strip() for ft in file_type.split(",") if ft.strip()] if file_type else []

    task_id = f"extract_{int(datetime.now().timestamp())}"
    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": 0,
        "current": "初始化", "results": [],
    }

    def _run():
        task = _bg_tasks[task_id]
        try:
            from ingestion.source_extractor import extract_batch as _extract, push_to_extracted_texts

            # 先查出总数，设置 total 让前端进度条有意义
            from utils.db_utils import execute_cloud_query
            count_sql = "SELECT COUNT(*) as n FROM source_documents WHERE extract_status='pending'"
            count_params = []
            if ft_list:
                placeholders = ",".join(["%s"] * len(ft_list))
                count_sql += f" AND file_type IN ({placeholders})"
                count_params = ft_list
            elif file_type:
                count_sql += " AND file_type=%s"
                count_params = [file_type]
            cnt = execute_cloud_query(count_sql, count_params)
            pending_total = min(cnt[0]["n"] if cnt else 0, limit * max(len(ft_list), 1))
            task["total"] = pending_total + 1  # +1 for 推入管线步骤
            task["current"] = f"待提取 {pending_total} 条"

            done_count = 0

            def on_progress(done, total, row_id):
                nonlocal done_count
                done_count = done
                task["progress"] = done_count
                task["current"] = f"提取中 {done}/{total}"

            if ft_list:
                for ft in ft_list:
                    task["current"] = f"提取 {ft}"
                    r1 = _extract(file_type=ft, limit=limit, on_progress=on_progress)
                    task["results"].append({
                        "source": f"提取·{ft}", "ok": True,
                        "count": f"成功{r1['success']}, 失败{r1['failed']}, 总计{r1['total']}",
                    })
            else:
                task["current"] = "提取全部类型"
                r1 = _extract(file_type=None, limit=limit, on_progress=on_progress)
                task["results"].append({
                    "source": "文本提取", "ok": True,
                    "count": f"成功{r1['success']}, 失败{r1['failed']}, 总计{r1['total']}",
                })

            # 推入 extracted_texts 管线
            task["current"] = "灌入extracted_texts"
            push_offset = done_count  # 提取阶段已完成的数量

            def on_push_progress(done, total, row_id):
                task["progress"] = push_offset + done
                task["current"] = f"灌入管线 {done}/{total}"

            r2 = push_to_extracted_texts(limit=limit, on_progress=on_push_progress)
            task["results"].append({
                "source": "灌入管线", "ok": True,
                "count": f"推送{r2['pushed']}, 跳过{r2['skipped']}, 总计{r2['total']}",
            })
        except Exception as e:
            task["results"].append({"source": task.get("current", "提取"), "error": str(e), "ok": False})
        task["status"] = "done"
        task["progress"] = task["total"]

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@router.get("/api/source-docs", response_class=JSONResponse)
def list_source_docs(file_type: str = None, status: str = None,
                     doc_type: str = None, q: str = None,
                     limit: int = 50, offset: int = 0):
    """查看源文档列表"""
    sql = "SELECT id, doc_type, file_type, title, author, publish_date, source, extract_status, raw_item_id, created_at FROM source_documents"
    params = []
    conditions = []
    if file_type:
        conditions.append("file_type=%s")
        params.append(file_type)
    if status:
        conditions.append("extract_status=%s")
        params.append(status)
    if doc_type:
        conditions.append("doc_type=%s")
        params.append(doc_type)
    if q:
        conditions.append("(title LIKE %s OR text_content LIKE %s)")
        params.extend([f"%{q}%", f"%{q}%"])
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY id DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    try:
        rows = execute_query(sql, params)
        count_sql = "SELECT doc_type, file_type, extract_status, COUNT(*) as cnt FROM source_documents GROUP BY doc_type, file_type, extract_status"
        stats = execute_query(count_sql)
        return {
            "items": [dict(r) for r in (rows or [])],
            "stats": [dict(s) for s in (stats or [])],
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ==================== 新管线触发 API ====================

@router.post("/run-summarize", response_class=JSONResponse)
def run_summarize(limit: int = 50, workers: int = 3):
    """触发 Pipeline A：批量内容总结"""
    task_id = f"summarize_{int(datetime.now().timestamp())}"
    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": 1,
        "current": "内容总结", "results": [],
    }

    def _run():
        task = _bg_tasks[task_id]
        try:
            from cleaning.batch_cleaner import batch_summarize
            r = batch_summarize(limit=limit, workers=workers)
            task["results"].append({
                "source": "内容总结", "ok": True,
                "count": f"成功{r['success']}, 失败{r['failed']}, 总计{r['total']}",
            })
        except Exception as e:
            task["results"].append({"source": "内容总结", "error": str(e), "ok": False})
        task["status"] = "done"
        task["progress"] = 1

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@router.post("/run-kg-extract", response_class=JSONResponse)
def run_kg_extract(limit: int = 30, workers: int = 2):
    """触发 Pipeline B：批量 KG 提取"""
    task_id = f"kg_extract_{int(datetime.now().timestamp())}"
    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": 1,
        "current": "KG提取", "results": [],
    }

    def _run():
        task = _bg_tasks[task_id]
        try:
            from knowledge_graph.kg_extractor_pipeline import batch_extract_kg
            r = batch_extract_kg(limit=limit, workers=workers)
            task["results"].append({
                "source": "KG提取", "ok": True,
                "count": f"实体+{r['entities']}, 关系+{r['relationships']}, 成功{r['done']}, 失败{r['failed']}",
            })
        except Exception as e:
            task["results"].append({"source": "KG提取", "error": str(e), "ok": False})
        task["status"] = "done"
        task["progress"] = 1

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@router.post("/api/sync-stockdb", response_class=JSONResponse)
def sync_stockdb(limit: int = Form(500)):
    """将 stock_db.stock_analysis 未同步记录拷贝到 source_documents"""
    task_id = f"syncdb_{int(datetime.now().timestamp())}"
    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": 1,
        "current": "同步中", "results": [],
    }

    def _run():
        task = _bg_tasks[task_id]
        try:
            from utils.db_utils import execute_cloud_query, execute_cloud_insert
            rows = execute_cloud_query(
                """SELECT id, doc_type, file_type, title, author, publish_date,
                          source, oss_url, text_content
                   FROM stock_db.stock_analysis
                   WHERE CONCAT('stockdb_', id) NOT IN (
                       SELECT source_ref FROM stock_analysis.source_documents
                       WHERE source_ref LIKE 'stockdb_%%'
                   )
                   ORDER BY id DESC
                   LIMIT %s""",
                [limit],
            )
            total = len(rows or [])
            task["total"] = total if total > 0 else 1
            task["current"] = f"0/{total}"
            pushed = 0
            skipped = 0
            failed = 0
            for i, r in enumerate(rows or []):
                source_ref = f"stockdb_{r['id']}"
                try:
                    execute_cloud_insert(
                        """INSERT INTO stock_analysis.source_documents
                           (doc_type, file_type, title, author, publish_date,
                            source, oss_url, text_content, extract_status, source_ref)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s)""",
                        [r.get("doc_type") or "news",
                         r.get("file_type") or "txt",
                         r.get("title") or "",
                         r.get("author") or "",
                         r.get("publish_date"),
                         r.get("source") or "stock_db",
                         r.get("oss_url"),
                         r.get("text_content") or "",
                         source_ref],
                    )
                    pushed += 1
                except Exception as e:
                    if "Duplicate" in str(e):
                        skipped += 1
                    else:
                        logger.error(f"sync_stockdb insert failed id={r['id']}: {e}")
                        failed += 1
                task["progress"] = i + 1
                task["current"] = f"{i+1}/{total}"

            task["results"].append({
                "source": "stock_db同步", "ok": True,
                "count": f"新增{pushed}, 跳过{skipped}, 失败{failed}, 总计{total}",
            })
        except Exception as e:
            task["results"].append({"source": "stock_db同步", "error": str(e), "ok": False})
        task["status"] = "done"
        task["progress"] = 1

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


# ==================== 定向提取 API ====================

@router.get("/api/cleaning-page-stats", response_class=JSONResponse)
def cleaning_page_stats():
    """页面加载时异步获取清洗页所有统计 — 本地 DB 三个查询并发执行"""
    from concurrent.futures import ThreadPoolExecutor, as_completed as _asc

    def _get_sd_stats():
        sd = {"pending": 0, "done_not_pushed": 0, "pushed": 0, "failed": 0}
        try:
            rows = execute_query(
                """SELECT extract_status,
                          SUM(CASE WHEN raw_item_id IS NULL THEN 1 ELSE 0 END) as not_pushed,
                          SUM(CASE WHEN raw_item_id IS NOT NULL THEN 1 ELSE 0 END) as pushed
                   FROM source_documents GROUP BY extract_status""")
            for r in (rows or []):
                st = r["extract_status"]
                if st == "pending":
                    sd["pending"] = (r["not_pushed"] or 0) + (r["pushed"] or 0)
                elif st == "done":
                    sd["done_not_pushed"] = r["not_pushed"] or 0
                    sd["pushed"] = r["pushed"] or 0
                elif st == "failed":
                    sd["failed"] = (r["not_pushed"] or 0) + (r["pushed"] or 0)
        except Exception:
            pass
        return sd

    def _get_runs():
        try:
            rows = execute_query(
                "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 10")
            return [dict(r) for r in (rows or [])]
        except Exception:
            return []

    with ThreadPoolExecutor(max_workers=3) as pool:
        f_cleaning = pool.submit(_get_cleaning_stats)
        f_sd       = pool.submit(_get_sd_stats)
        f_runs     = pool.submit(_get_runs)
        cleaning_stats = f_cleaning.result()
        sd_stats       = f_sd.result()
        pipeline_runs  = f_runs.result()

    return {
        "cleaning_stats": cleaning_stats,
        "sd_stats": sd_stats,
        "pipeline_runs": pipeline_runs,
    }


@router.get("/api/doc-stats", response_class=JSONResponse)
def get_doc_stats(doc_type: str = ""):
    """异步加载云端文档库统计（避免页面加载阻塞）。doc_type 可选过滤。"""
    from utils.db_utils import execute_cloud_query
    from config.doc_types import DOC_TYPES
    try:
        summary = _get_source_doc_summary()
        where = "WHERE doc_type=%s" if doc_type else ""
        params = [doc_type] if doc_type else []
        source_doc_stats = execute_cloud_query(
            f"""SELECT source, COUNT(*) as doc_count,
                      SUM(CASE WHEN extract_status IN ('extracted','ready_to_pipe','done') THEN 1 ELSE 0 END) as extracted_count,
                      SUM(CASE WHEN extract_status='pending' THEN 1 ELSE 0 END) as pending_count,
                      MAX(publish_date) as latest_date,
                      GROUP_CONCAT(DISTINCT file_type ORDER BY file_type SEPARATOR '/') as file_types
               FROM source_documents {where} GROUP BY source ORDER BY doc_count DESC""",
            params or None
        )
        doc_type_stats = execute_cloud_query(
            "SELECT doc_type, COUNT(*) as cnt FROM source_documents GROUP BY doc_type ORDER BY cnt DESC"
        )
        doc_type_label = {k: l for k, l, _ in DOC_TYPES}
        return {
            "summary": summary,
            "doc_type_filter": doc_type,
            "source_doc_stats": [dict(r) for r in (source_doc_stats or [])],
            "doc_type_stats": [
                {"doc_type": r["doc_type"], "label": doc_type_label.get(r["doc_type"], r["doc_type"] or "未分类"), "cnt": r["cnt"]}
                for r in (doc_type_stats or [])
            ],
        }
    except Exception as e:
        logger.error(f"get_doc_stats error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/keyword-search", response_class=JSONResponse)
def keyword_search(
    keywords: str = Form(...),
    file_types: str = Form(""),
):
    """关键词搜索 source_documents，返回匹配列表供定向提取"""
    kw_list = [k.strip() for k in keywords.replace("，", ",").split(",") if k.strip()]
    if not kw_list:
        return {"items": [], "total": 0}

    ft_list = [f.strip() for f in file_types.split(",") if f.strip()]

    conditions = []
    params = []

    kw_clauses = []
    for kw in kw_list:
        kw_clauses.append(
            "(title LIKE %s OR extracted_text LIKE %s OR text_content LIKE %s)"
        )
        params.extend([f"%{kw}%", f"%{kw}%", f"%{kw}%"])
    conditions.append("(" + " OR ".join(kw_clauses) + ")")

    if ft_list:
        placeholders = ",".join(["%s"] * len(ft_list))
        conditions.append(f"file_type IN ({placeholders})")
        params.extend(ft_list)

    where = " AND ".join(conditions)
    sql = f"""SELECT id, title, source, file_type, doc_type, extract_status,
                     publish_date, extracted_text, text_content
              FROM source_documents
              WHERE {where}
              ORDER BY id DESC
              LIMIT 200"""

    def _snippet(text, kw_list, size=300):
        """提取第一个关键词命中位置前后共 size 字的上下文"""
        if not text:
            return ""
        for kw in kw_list:
            idx = text.lower().find(kw.lower())
            if idx >= 0:
                start = max(0, idx - size // 3)
                end = min(len(text), start + size)
                snippet = text[start:end]
                if start > 0:
                    snippet = "…" + snippet
                if end < len(text):
                    snippet = snippet + "…"
                return snippet
        return text[:size]

    try:
        from utils.db_utils import execute_cloud_query
        rows = execute_cloud_query(sql, params)
        items = []
        for r in (rows or []):
            body = r.get("extracted_text") or r.get("text_content") or ""
            items.append({
                "id": r["id"],
                "title": r.get("title") or "",
                "source": r.get("source") or "",
                "file_type": r.get("file_type") or "",
                "doc_type": r.get("doc_type") or "",
                "extract_status": r.get("extract_status") or "pending",
                "publish_date": str(r["publish_date"]) if r.get("publish_date") else "",
                "snippet": _snippet(body, kw_list),
            })
        return {"items": items, "total": len(items)}
    except Exception as e:
        logger.error(f"keyword_search error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/targeted-extract", response_class=JSONResponse)
def targeted_extract(doc_ids: str = Form(...)):
    """对指定 doc_ids 做定向提取 + 推入 extracted_texts"""
    id_list = [int(x.strip()) for x in doc_ids.split(",") if x.strip().isdigit()]
    if not id_list:
        return JSONResponse({"error": "无有效 doc_ids"}, status_code=400)

    task_id = f"targeted_{int(datetime.now().timestamp())}"
    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": 2,
        "current": "定向提取", "results": [],
    }

    def _run():
        task = _bg_tasks[task_id]
        try:
            from ingestion.source_extractor import extract_by_ids, push_to_extracted_texts_by_ids
            task["current"] = "文本提取"
            r1 = extract_by_ids(id_list)
            task["results"].append({
                "source": "文本提取", "ok": True,
                "count": f"成功{r1['success']}, 跳过{r1['skipped']}, 失败{r1['failed']}, 总计{r1['total']}",
            })
            task["progress"] = 1

            task["current"] = "推入管线"
            r2 = push_to_extracted_texts_by_ids(id_list)
            task["results"].append({
                "source": "推入管线", "ok": True,
                "count": f"推送{r2['pushed']}, 跳过{r2['skipped']}, 失败{r2['failed']}, 总计{r2['total']}",
            })
        except Exception as e:
            task["results"].append({"source": task["current"], "error": str(e), "ok": False})
        task["status"] = "done"
        task["progress"] = 2

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


# ==================== 信息源管理 API ====================

@router.post("/api/save-sources", response_class=JSONResponse)
async def api_save_sources(request: Request):
    """保存信息源配置（enabled/limit/pages/newsHours/zsxqCookie）"""
    body = await request.json()
    settings = load_fetch_settings()
    for key, cfg in body.get("sources", {}).items():
        if key in settings["sources"]:
            if "enabled" in cfg:
                settings["sources"][key]["enabled"] = cfg["enabled"]
            if "limit" in cfg and cfg["limit"] is not None:
                settings["sources"][key]["limit"] = int(cfg["limit"])
            if "max_pages" in cfg and cfg["max_pages"] is not None:
                settings["sources"][key]["max_pages"] = int(cfg["max_pages"])
    if "news_hours" in body:
        settings["news_hours"] = int(body["news_hours"])
    if "zsxq_cookie" in body:
        from utils.sys_config import set_config
        set_config("zsxq_cookie", body["zsxq_cookie"])
    if "zsxq_group_ids" in body:
        from utils.sys_config import set_config
        set_config("zsxq_group_ids", body["zsxq_group_ids"])
    save_fetch_settings(settings)
    return {"ok": True}


@router.post("/api/fetch-zsxq", response_class=JSONResponse)
async def api_fetch_zsxq(request: Request):
    """知识星球专项采集（支持日期范围 + 多星球）"""
    import os
    from utils.sys_config import get_config
    from config import ZSXQ_COOKIE, ZSXQ_GROUP_IDS
    from ingestion.zsxq_source import fetch_zsxq_data

    body = await request.json()
    start_date = body.get("start_date") or None
    end_date = body.get("end_date") or None
    max_pages = int(body.get("max_pages", 50))

    cookie = get_config("zsxq_cookie") or os.environ.get("ZSXQ_COOKIE", "") or ZSXQ_COOKIE
    if not cookie:
        return JSONResponse({"error": "未配置 zsxq_access_token"}, status_code=400)

    group_ids_str = get_config("zsxq_group_ids") or ""
    group_ids = [g.strip() for g in group_ids_str.split(",") if g.strip()] or ZSXQ_GROUP_IDS

    task_id = f"zsxq_{int(datetime.now().timestamp())}"
    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": 1,
        "current": "知识星球", "results": [],
        "started_at": datetime.now().isoformat(),
    }

    def _run():
        task = _bg_tasks[task_id]
        total_saved = total_fetched = total_skipped = 0

        def _progress(page, saved, msg):
            nonlocal total_saved
            task["current"] = msg

        try:
            result = fetch_zsxq_data(
                group_ids=group_ids,
                token=cookie,
                start_date=start_date,
                end_date=end_date,
                max_pages=max_pages,
                progress_callback=_progress,
            )
            task["results"].append({
                "source": "知识星球", "ok": True,
                "count": result.get("saved", 0),
                "fetched": result.get("total_fetched", 0),
                "skipped": result.get("skipped", 0),
            })
        except Exception as e:
            logger.error(f"知识星球采集失败: {e}")
            task["results"].append({"source": "知识星球", "error": str(e), "ok": False})
        task["status"] = "done"
        task["progress"] = 1

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@router.post("/api/add-source", response_class=JSONResponse)
async def api_add_source(request: Request):
    """从 CATALOG 添加信息源"""
    body = await request.json()
    from utils.fetch_config import add_source
    ok, msg = add_source(body.get("key", ""))
    if not ok:
        return JSONResponse({"error": msg}, status_code=400)
    return {"ok": True}


@router.post("/api/add-custom-source", response_class=JSONResponse)
async def api_add_custom_source(request: Request):
    """自定义添加信息源"""
    body = await request.json()
    from utils.fetch_config import add_custom_source
    ok, msg = add_custom_source(
        key=body.get("key", ""), label=body.get("label", ""),
        group=body.get("group", "news"), desc=body.get("desc", ""),
        fetcher_type=body.get("fetcher_type", "jasper"),
    )
    if not ok:
        return JSONResponse({"error": msg}, status_code=400)
    return {"ok": True}


@router.post("/api/delete-source", response_class=JSONResponse)
async def api_delete_source(request: Request):
    """删除信息源"""
    body = await request.json()
    from utils.fetch_config import delete_source
    ok, msg = delete_source(body.get("key", ""))
    if not ok:
        return JSONResponse({"error": msg}, status_code=400)
    return {"ok": True}


@router.get("/api/available-sources", response_class=JSONResponse)
def api_available_sources():
    """获取可添加的信息源列表"""
    from utils.fetch_config import get_available_sources
    avail = get_available_sources()
    return {"sources": [{"key": k, "label": v["label"], "group": v.get("group", "news"),
                         "desc": v.get("desc", "")} for k, v in avail.items()]}


@router.post("/api/cancel-task", response_class=JSONResponse)
async def api_cancel_task(request: Request):
    """取消后台任务"""
    body = await request.json()
    task_id = body.get("task_id", "")
    task = _bg_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    task["cancelled"] = True
    task["status"] = "done"
    task["current"] = "已取消"
    return {"ok": True}


@router.post("/api/push-to-pipeline", response_class=JSONResponse)
def api_push_to_pipeline(limit: int = Form(500)):
    """将已提取但未入管线的文档推入 extracted_texts"""
    task_id = f"push_pipeline_{int(datetime.now().timestamp())}"
    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": 1,
        "current": "推入管线", "results": [],
    }

    def _run():
        task = _bg_tasks[task_id]
        try:
            from ingestion.source_extractor import push_to_extracted_texts
            r = push_to_extracted_texts(limit=limit)
            task["results"].append({
                "source": "推入管线", "ok": True,
                "count": f"推送{r['pushed']}, 跳过{r['skipped']}, 总计{r['total']}",
            })
        except Exception as e:
            task["results"].append({"source": "推入管线", "error": str(e), "ok": False})
        task["status"] = "done"
        task["progress"] = 1

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@router.post("/api/run-pipeline", response_class=JSONResponse)
def api_run_pipeline(
                     pipeline: str = Form("abc"), limit: int = Form(20)):
    """触发清洗管线 a/b2/c/abc"""
    task_id = f"pipeline_{pipeline}_{int(datetime.now().timestamp())}"
    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": 0,
        "current": "初始化", "results": [], "paused": False,
    }

    def _run():
        task = _bg_tasks[task_id]
        try:
            from cleaning.unified_pipeline import process_single
            from utils.db_utils import execute_cloud_query
            from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed

            need_a = pipeline in ("a", "abc")
            need_b = pipeline in ("b2", "abc")
            need_c = pipeline in ("c", "abc")

            # 构建 WHERE 条件，只查该管线需要处理的记录
            conditions = []
            if need_a:
                conditions.append("cs.id IS NULL")
            if need_b:
                conditions.append("(et.mentions_status IS NULL OR et.mentions_status != 'done')")
            if need_c:
                conditions.append("(et.kg_status IS NULL OR et.kg_status != 'done')")
            where = " OR ".join(conditions) if conditions else "1=0"

            pending = execute_cloud_query(
                f"""SELECT DISTINCT et.id,
                          (cs.id IS NULL) as need_a,
                          (et.mentions_status IS NULL OR et.mentions_status != 'done') as need_b,
                          (et.kg_status IS NULL OR et.kg_status != 'done') as need_c
                   FROM extracted_texts et
                   LEFT JOIN content_summaries cs ON et.id = cs.extracted_text_id
                   WHERE {where}
                   ORDER BY et.id
                   LIMIT %s""",
                [limit],
            ) or []

            total = len(pending)
            task["total"] = total
            task["current"] = f"待处理 {total} 条"
            ok, fail = 0, 0
            total_a, total_b2, total_c, total_chunks = 0, 0, 0, 0

            def should_cancel():
                while task.get("paused"):
                    import time; time.sleep(0.5)
                return task.get("cancelled", False)

            def _run_one(row):
                def _on_status(stage, msg):
                    task["current"] = msg
                return process_single(
                    row["id"],
                    need_a=need_a and bool(row["need_a"]),
                    need_b=need_b and bool(row["need_b"]),
                    need_c=need_c and bool(row["need_c"]),
                    on_status=_on_status,
                ), row

            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = []
                for row in pending:
                    if should_cancel():
                        break
                    futures.append(pool.submit(_run_one, row))
                for fut in _as_completed(futures):
                    if should_cancel():
                        break
                    try:
                        r, row = fut.result()
                        if r.get("summary_id"):
                            total_a += 1
                        total_b2 += r.get("mentions", 0)
                        total_c += r.get("kg_rels", 0)
                        total_chunks += r.get("chunks", 0)
                        ok += 1
                    except Exception as e2:
                        fail += 1
                        logger.error(f"pipeline {pipeline} id={row.get('id')} error: {e2}")
                    task["progress"] = ok + fail
                    task["current"] = f"{ok+fail}/{total}"

            task["results"].append({
                "source": f"管线{pipeline.upper()}", "ok": True,
                "count": f"成功{ok}, 失败{fail}, A={total_a}, B={total_b2}, C={total_c}, 切片={total_chunks}",
            })
        except Exception as e:
            logger.error(f"run_pipeline {pipeline} error: {e}")
            task["results"].append({"source": f"管线{pipeline.upper()}", "error": str(e), "ok": False})
        task["status"] = "done"

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@router.get("/api/active-tasks", response_class=JSONResponse)
def api_active_tasks():
    """返回所有活跃（running）的后台任务，供侧边栏全局进度组件轮询"""
    tasks = []
    for tid, t in list(_bg_tasks.items()):
        if t.get("status") != "running":
            continue
        total = t.get("total", 0) or 1
        progress = t.get("progress", 0)
        pct = min(int(progress / total * 100), 99) if total else 0
        # 从 task_id 前缀推断任务类型标签
        label = "任务"
        if tid.startswith("extract_"):         label = "文本提取"
        elif tid.startswith("pipeline_"):      label = "清洗管线"
        elif tid.startswith("push_pipeline_"): label = "推入管线"
        elif tid.startswith("clean_"):         label = "批量清洗"
        elif tid.startswith("sync_"):          label = "数据同步"
        elif tid.startswith("import_"):        label = "导入SQL"
        elif tid.startswith("backfill_chunks_"): label = "向量回填"
        tasks.append({
            "task_id": tid,
            "label": label,
            "current": t.get("current", ""),
            "progress": pct,
            "done": progress,
            "total": total,
        })
    return tasks


@router.get("/api/cleaning-logs", response_class=JSONResponse)
def api_cleaning_logs():
    """获取最近清洗日志"""
    try:
        from utils.db_utils import execute_cloud_query
        rows = execute_cloud_query(
            """SELECT id, pipeline, status, total_count, success_count, fail_count,
                      started_at, finished_at
               FROM pipeline_runs ORDER BY started_at DESC LIMIT 20"""
        )
        return {"logs": [dict(r) for r in (rows or [])]}
    except Exception:
        recent = []
        for tid, t in sorted(_bg_tasks.items(), key=lambda x: x[0], reverse=True)[:20]:
            recent.append({
                "task_id": tid, "status": t.get("status"),
                "current": t.get("current"), "results": t.get("results", []),
            })
        return {"logs": recent, "source": "memory"}


@router.post("/api/backfill-chunks", response_class=JSONResponse)
def api_backfill_chunks(limit: int = Form(500)):
    """触发存量 text_chunks 向量回填（backfill_chunks.py 逻辑）"""
    task_id = f"backfill_chunks_{int(datetime.now().timestamp())}"
    _bg_tasks[task_id] = {
        "status": "running", "progress": 0, "total": 0,
        "current": "初始化回填", "results": [],
    }

    def _run():
        task = _bg_tasks[task_id]
        try:
            from utils.db_utils import execute_cloud_query
            from retrieval.chunker import chunk_and_index
            from retrieval.vector_store import ensure_collection

            ensure_collection()

            rows = execute_cloud_query(
                """SELECT et.id, et.full_text, et.publish_time,
                          sd.file_type, sd.title
                   FROM extracted_texts et
                   LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
                   WHERE et.full_text IS NOT NULL
                     AND et.full_text != ''
                     AND et.id NOT IN (
                         SELECT DISTINCT extracted_text_id FROM text_chunks
                     )
                   ORDER BY et.id
                   LIMIT %s""",
                [limit],
            ) or []

            total = len(rows)
            task["total"] = total if total > 0 else 1
            task["current"] = f"待处理 {total} 条"

            done = 0
            chunks_total = 0
            errors = 0

            for row in rows:
                if task.get("cancelled"):
                    break
                et_id = row["id"]
                full_text = row["full_text"] or ""
                if not full_text.strip():
                    done += 1
                    continue
                try:
                    n = chunk_and_index(
                        extracted_text_id=et_id,
                        full_text=full_text,
                        file_type=row.get("file_type") or "",
                        publish_time=row.get("publish_time"),
                        source_doc_title=row.get("title") or "",
                    )
                    chunks_total += n
                except Exception as e:
                    logger.error(f"backfill chunk id={et_id}: {e}")
                    errors += 1
                done += 1
                task["progress"] = done
                task["current"] = f"切片进度 {done}/{total} | +{chunks_total} chunks"

            task["results"].append({
                "source": "向量回填", "ok": True,
                "count": f"处理{done}条, 生成{chunks_total}chunks, 失败{errors}",
            })
        except Exception as e:
            logger.error(f"backfill_chunks error: {e}")
            task["results"].append({"source": "向量回填", "error": str(e), "ok": False})
        task["status"] = "done"
        task["progress"] = task["total"]

    threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}



@router.post("/api/manual-add-doc", response_class=JSONResponse)
async def api_manual_add_doc(request: Request):
    """手动录入 source_document，直接写云端"""
    body = await request.json()
    title = (body.get("title") or "").strip()
    text_content = (body.get("text_content") or "").strip()
    if not text_content:
        return JSONResponse({"error": "正文内容不能为空"}, status_code=400)

    doc_type = body.get("doc_type") or "other"
    source = body.get("source") or "manual"
    author = body.get("author") or ""
    publish_date = body.get("publish_date") or None

    # 自动分类 doc_type（如果用户选了 auto）
    if doc_type == "auto":
        from config.doc_types import classify_doc_type
        doc_type = classify_doc_type(title, text_content[:200])

    from utils.db_utils import execute_cloud_insert
    try:
        execute_cloud_insert(
            """INSERT INTO source_documents
               (doc_type, file_type, title, author, publish_date, source,
                text_content, extracted_text, extract_status)
               VALUES (%s, 'txt', %s, %s, %s, %s, %s, %s, 'done')""",
            [doc_type, title or "(手动录入)", author, publish_date,
             source, text_content, text_content],
        )
        return {"ok": True, "doc_type": doc_type}
    except Exception as e:
        logger.error(f"manual_add_doc error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


# ==================== 提取审核台 API ====================

@router.get("/review", response_class=HTMLResponse)
def data_review(request: Request):
    """提取审核台页面"""
    from config.doc_types import DOC_TYPES
    from utils.db_utils import execute_cloud_query
    # 从数据库读取实际存在的 doc_type，作为下拉选项
    try:
        rows = execute_cloud_query(
            "SELECT doc_type, COUNT(*) as cnt FROM source_documents "
            "WHERE doc_type IS NOT NULL AND doc_type != '' "
            "GROUP BY doc_type ORDER BY cnt DESC"
        ) or []
        # 构建标签映射（精细类型用中文，粗类型直接用英文）
        label_map = {k: l for k, l, _ in DOC_TYPES}
        actual_doc_types = [
            (r["doc_type"], label_map.get(r["doc_type"], r["doc_type"]), r["cnt"])
            for r in rows
        ]
    except Exception:
        actual_doc_types = [(k, l, 0) for k, l, _ in DOC_TYPES]
    return templates.TemplateResponse("data_review.html", {
        "request": request,
        "active_page": "data",
        "doc_types": actual_doc_types,
    })


@router.get("/api/review-list", response_class=JSONResponse)
def api_review_list(
    file_type: str = "",
    source: str = "",
    status: str = "",
    doc_type: str = "",
    q: str = "",
    limit: int = 50,
    offset: int = 0,
):
    """提取审核台文档列表"""
    from utils.db_utils import execute_cloud_query

    conditions = []
    params = []

    # 默认排除 txt（txt 自动入管线，不需要人工审核）
    if not file_type:
        conditions.append("file_type != 'txt'")
    else:
        conditions.append("file_type = %s")
        params.append(file_type)

    if status:
        conditions.append("extract_status = %s")
        params.append(status)

    if source:
        conditions.append("source = %s")
        params.append(source)

    if doc_type:
        conditions.append("doc_type = %s")
        params.append(doc_type)

    if q:
        conditions.append("(title LIKE %s OR text_content LIKE %s)")
        params.extend([f"%{q}%", f"%{q}%"])

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"""SELECT id, title, file_type, doc_type, source, extract_status,
                     oss_url, text_content, publish_date, created_at, extracted_text
              FROM source_documents{where}
              ORDER BY id DESC LIMIT %s OFFSET %s"""
    params.extend([limit, offset])

    count_sql = f"SELECT COUNT(*) as n FROM source_documents{where}"
    count_params = params[:-2]  # exclude limit/offset

    try:
        rows = execute_cloud_query(sql, params) or []
        cnt_rows = execute_cloud_query(count_sql, count_params) or [{"n": 0}]
        total = cnt_rows[0]["n"]
        items = []
        for r in rows:
            d = dict(r)
            d["publish_date"] = str(d["publish_date"]) if d.get("publish_date") else ""
            d["created_at"] = str(d["created_at"])[:16] if d.get("created_at") else ""
            items.append(d)
        return {"items": items, "total": total}
    except Exception as e:
        logger.error(f"review_list error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/upload-file", response_class=JSONResponse)
async def api_upload_file(
    file: UploadFile,
    title: str = Form(""),
    doc_type: str = Form("auto"),
    source: str = Form("upload"),
    publish_date: str = Form(""),
):
    """上传文件并即时提取"""
    import time
    from utils.db_utils import execute_cloud_query, execute_cloud_insert

    # 推断 file_type
    suffix = Path(file.filename or "").suffix.lower()
    ft_map = {
        ".pdf": "pdf",
        ".png": "image", ".jpg": "image", ".jpeg": "image", ".gif": "image",
        ".mp3": "mp3", ".wav": "audio", ".m4a": "audio",
        ".xlsx": "xlsx", ".xls": "xlsx",
        ".txt": "txt",
    }
    file_type = ft_map.get(suffix, "txt")

    # 保存文件
    uploads_dir = Path(__file__).parent.parent / "static" / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time() * 1000)
    safe_name = f"{ts}_{Path(file.filename or 'file').name}"
    save_path = uploads_dir / safe_name
    content = await file.read()
    save_path.write_bytes(content)
    preview_url = f"/static/uploads/{safe_name}"

    # 写入 source_documents
    text_content = content.decode("utf-8", errors="replace") if file_type == "txt" else ""
    doc_title = title.strip() or Path(file.filename or "").stem or "(上传文件)"

    pd_val = publish_date.strip() or None

    try:
        execute_cloud_insert(
            """INSERT INTO source_documents
               (doc_type, file_type, title, source, oss_url, text_content, publish_date, extract_status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')""",
            [doc_type if doc_type != "auto" else "other",
             file_type, doc_title, source, preview_url, text_content, pd_val],
        )
        rows = execute_cloud_query(
            "SELECT LAST_INSERT_ID() as id"
        )
        doc_id = rows[0]["id"] if rows else None
    except Exception as e:
        return JSONResponse({"error": f"写入数据库失败: {e}"}, status_code=500)

    # 提取
    extracted_text = ""
    auto_pushed = False
    needs_reextract = False
    try:
        row = execute_cloud_query(
            "SELECT id, doc_type, file_type, title, text_content, oss_url FROM source_documents WHERE id=%s",
            [doc_id],
        )
        if row:
            extracted_text, needs_reextract = _do_extract_and_save(row[0])

        # txt 自动入管线
        if file_type == "txt" and doc_id:
            from ingestion.source_extractor import push_to_extracted_texts_by_ids
            push_to_extracted_texts_by_ids([doc_id])
            auto_pushed = True
    except Exception as e:
        logger.error(f"upload_file extract error: {e}")

    return {
        "doc_id": doc_id,
        "file_type": file_type,
        "title": doc_title,
        "extracted_text": extracted_text,
        "preview_url": preview_url,
        "auto_pushed": auto_pushed,
        "needs_reextract": needs_reextract,
    }


def _do_extract_and_save(row: dict) -> tuple:
    """提取+清洗单条文档并回写云端，返回 (extracted_text, needs_reextract)

    needs_reextract=True 表示有 OCR 碎片图表需要人工上传截图二次提取
    """
    from ingestion.source_extractor import _extract_single_with_meta, _semantic_clean
    from config.doc_types import classify_doc_type
    from utils.db_utils import execute_cloud_insert

    text, needs_understanding = _extract_single_with_meta(row)
    if text and len(text.strip()) >= 20:
        text = _semantic_clean(text, row["file_type"], row["id"], needs_understanding)
    new_doc_type = classify_doc_type(row.get("title") or "", (text or "")[:200])

    # needs_reextract: 扫描件PDF或纯OCR图片，含碎片图表需要人工确认
    needs_reextract = needs_understanding and row.get("file_type") in ("pdf", "image", "mixed")

    execute_cloud_insert(
        "UPDATE source_documents SET extracted_text=%s, extract_status='extracted', doc_type=%s WHERE id=%s",
        [text, new_doc_type, row["id"]],
    )
    return text or "", needs_reextract


@router.post("/api/extract-preview", response_class=JSONResponse)
async def api_extract_preview(request: Request):
    """批量提取预览（不入管线），返回提取结果供对照审核"""
    from utils.db_utils import execute_cloud_query

    body = await request.json()
    doc_ids = [int(x) for x in (body.get("doc_ids") or []) if str(x).isdigit()]
    if not doc_ids:
        return JSONResponse({"error": "doc_ids 不能为空"}, status_code=400)

    placeholders = ",".join(["%s"] * len(doc_ids))
    rows = execute_cloud_query(
        f"""SELECT id, doc_type, file_type, title, text_content, oss_url, extracted_text, extract_status
            FROM source_documents WHERE id IN ({placeholders})""",
        doc_ids,
    ) or []

    def _resolve_oss_url(d):
        """mp3/audio 的播放 URL 存在 text_content，其他类型用 oss_url"""
        if d.get("file_type") in ("mp3", "audio"):
            return d.get("text_content") or d.get("oss_url") or ""
        return d.get("oss_url") or ""

    results = []
    for row in rows:
        d = dict(row)
        if d.get("extract_status") in ("extracted", "ready_to_pipe", "done") and d.get("extracted_text"):
            # 已提取，直接返回
            results.append({
                "id": d["id"],
                "title": d.get("title") or "",
                "file_type": d.get("file_type") or "",
                "doc_type": d.get("doc_type") or "",
                "oss_url": _resolve_oss_url(d),
                "extracted_text": d.get("extracted_text") or "",
                "needs_reextract": False,
            })
        else:
            # 未提取，执行提取+清洗
            try:
                extracted, needs_reextract = _do_extract_and_save(d)
                from config.doc_types import classify_doc_type
                detected_type = classify_doc_type(d.get("title") or "", extracted[:200])
                results.append({
                    "id": d["id"],
                    "title": d.get("title") or "",
                    "file_type": d.get("file_type") or "",
                    "doc_type": detected_type,
                    "oss_url": _resolve_oss_url(d),
                    "extracted_text": extracted,
                    "needs_reextract": needs_reextract,
                })
            except Exception as e:
                results.append({
                    "id": d["id"],
                    "title": d.get("title") or "",
                    "file_type": d.get("file_type") or "",
                    "doc_type": d.get("doc_type") or "",
                    "oss_url": _resolve_oss_url(d),
                    "extracted_text": "",
                    "needs_reextract": False,
                    "error": str(e),
                })

    return {"items": results}


@router.post("/api/approve-docs", response_class=JSONResponse)
async def api_approve_docs(request: Request):
    """保存编辑后的提取文本，更新状态为 extracted（不自动入管线）"""
    from utils.db_utils import execute_cloud_insert

    body = await request.json()
    docs = body.get("docs") or []
    if not docs:
        return JSONResponse({"error": "docs 不能为空"}, status_code=400)

    saved = skipped = failed = 0

    for doc in docs:
        doc_id = doc.get("id")
        if not doc_id:
            skipped += 1
            continue
        edited_text = doc.get("extracted_text")
        try:
            if edited_text is not None:
                execute_cloud_insert(
                    "UPDATE source_documents SET extracted_text=%s, extract_status='extracted' WHERE id=%s",
                    [edited_text, doc_id],
                )
            else:
                execute_cloud_insert(
                    "UPDATE source_documents SET extract_status='extracted' WHERE id=%s",
                    [doc_id],
                )
            saved += 1
        except Exception as e:
            logger.error(f"approve_docs update id={doc_id}: {e}")
            failed += 1

    return {"saved": saved, "skipped": skipped, "failed": failed}


@router.get("/api/proxy-file")
async def api_proxy_file(url: str):
    """代理 OSS 文件，强制 Content-Disposition: inline，让浏览器内联渲染而非下载"""
    import httpx
    from fastapi.responses import StreamingResponse
    from urllib.parse import urlparse
    if not url:
        return JSONResponse({"error": "url 不能为空"}, status_code=400)
    try:
        # 知识星球域名需要带 cookie + 特定请求头才能访问文件
        cookies = {}
        extra_headers = {}
        host = urlparse(url).hostname or ""
        if "zsxq.com" in host or "zqimg.com" in host:
            from utils.sys_config import get_config
            from config import ZSXQ_COOKIE
            token = get_config("zsxq_cookie") or os.environ.get("ZSXQ_COOKIE", "") or ZSXQ_COOKIE
            if token:
                cookies["zsxq_access_token"] = token
            extra_headers = {
                "origin": "https://wx.zsxq.com",
                "referer": "https://wx.zsxq.com/",
                "user-agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/144.0.0.0 Safari/537.36"
                ),
            }

        async with httpx.AsyncClient(timeout=60, follow_redirects=True, cookies=cookies, headers=extra_headers) as client:
            resp = await client.get(url)
        content_type = resp.headers.get("content-type", "application/octet-stream")
        headers = {
            "Content-Disposition": "inline",
            "Content-Type": content_type,
            "Cache-Control": "no-cache",
        }
        return StreamingResponse(iter([resp.content]), media_type=content_type, headers=headers)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/reject-docs", response_class=JSONResponse)
async def api_reject_docs(request: Request):
    """丢弃文档（标记为 rejected）"""
    from utils.db_utils import execute_cloud_insert

    body = await request.json()
    doc_ids = [int(x) for x in (body.get("doc_ids") or []) if str(x).isdigit()]
    if not doc_ids:
        return JSONResponse({"error": "doc_ids 不能为空"}, status_code=400)

    placeholders = ",".join(["%s"] * len(doc_ids))
    try:
        execute_cloud_insert(
            f"UPDATE source_documents SET extract_status='rejected' WHERE id IN ({placeholders})",
            doc_ids,
        )
        return {"ok": True, "rejected": len(doc_ids)}
    except Exception as e:
        logger.error(f"reject_docs error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/upload-chart-images", response_class=JSONResponse)
async def api_upload_chart_images(
    files: List[UploadFile] = [],
    doc_id: int = Form(0),
):
    """上传图表截图 → Qwen 视觉模型理解 → 返回结构化描述文本

    用于扫描件 PDF / 纯 OCR 图片的二次提取：
    用户选取碎片图表区域截图上传，Qwen 理解后替换原 extracted_text 中的碎片文字
    """
    import base64
    from pathlib import Path

    if not files:
        return JSONResponse({"error": "请上传至少一张图片"}, status_code=400)

    # 保存图片到 static/uploads/ 并收集路径
    static_dir = Path(__file__).resolve().parent.parent / "static" / "uploads"
    static_dir.mkdir(parents=True, exist_ok=True)

    saved_paths = []
    for f in files:
        data = await f.read()
        if not data:
            continue
        import time
        fname = f"{int(time.time()*1000)}_{f.filename}"
        fpath = static_dir / fname
        fpath.write_bytes(data)
        saved_paths.append(str(fpath))

    if not saved_paths:
        return JSONResponse({"error": "无有效图片"}, status_code=400)

    # 调用 Qwen 视觉模型理解
    try:
        from utils.model_router import call_model_vision

        # 获取文档标题用于 context
        title = ""
        if doc_id:
            rows = execute_cloud_query(
                "SELECT title FROM source_documents WHERE id = %s", [doc_id]
            )
            title = rows[0]["title"] if rows else ""

        prompt = (
            f"这是从一份金融文档「{title or '研报'}」中截取的图表截图（共{len(saved_paths)}张）。"
            "请详细描述每张图表中的所有内容，包括：图表类型、数据标签、关键数字、趋势方向等。"
            "如果包含表格，请用 Markdown 表格格式输出。"
            "用结构清晰的中文回复。"
        )
        vision_text = call_model_vision("vision", prompt, saved_paths,
                                        max_tokens=4096, timeout=120)
        return {
            "ok": True,
            "vision_text": vision_text or "",
            "image_count": len(saved_paths),
        }
    except Exception as e:
        logger.error(f"upload-chart-images Qwen 失败: {e}")
        return JSONResponse({"error": f"Qwen 理解失败: {e}"}, status_code=500)
    finally:
        # 清理临时图片
        import os
        for p in saved_paths:
            try:
                os.unlink(p)
            except OSError:
                pass


# ==================== Source Documents 状态流转 API ====================

# 状态颜色映射
STATUS_COLORS = {
    "failed": "red",
    "pending": "gray",
    "extracted": "yellow",
    "ready_to_pipe": "blue",
    "processing": "purple",
    "done": "green",
    "rejected": "red",
    "url_expired": "orange",
    "skipped": "gray",
    "cleaning": "indigo",
    "remix": "cyan",
}

STATUS_LABELS = {
    "failed": "提取失败",
    "pending": "待提取",
    "extracted": "已提取",
    "ready_to_pipe": "待入管线",
    "processing": "管线处理中",
    "done": "已完成",
    "rejected": "已丢弃",
    "url_expired": "链接失效",
    "skipped": "已跳过",
    "cleaning": "清洗中",
    "remix": "已混编",
}


@router.get("/api/source-documents/{doc_id}/detail", response_class=JSONResponse)
async def api_get_source_document_detail(doc_id: int):
    """获取单条文档详情（含 oss_url + extracted_text，不触发提取）"""
    from utils.db_utils import execute_cloud_query
    rows = execute_cloud_query(
        """SELECT id, doc_type, file_type, title, source, oss_url, text_content,
                  extracted_text, extract_status, publish_date, created_at
           FROM source_documents WHERE id = %s""",
        [doc_id],
    )
    if not rows:
        return JSONResponse({"error": "not found"}, status_code=404)
    r = rows[0]
    ft = r.get("file_type") or ""
    oss_url = r.get("oss_url") or ""
    # audio: 播放链接在 text_content
    if ft in ("mp3", "audio"):
        oss_url = r.get("text_content") or oss_url
    return {
        "id": str(r["id"]),  # 字符串，避免 JS Number 精度丢失
        "title": r.get("title") or "",
        "file_type": ft,
        "doc_type": r.get("doc_type") or "",
        "source": r.get("source") or "",
        "oss_url": oss_url,
        "text_content": r.get("text_content") or "",
        "extracted_text": r.get("extracted_text") or "",
        "extract_status": r.get("extract_status") or "pending",
        "publish_date": str(r.get("publish_date") or "")[:10],
        "created_at": str(r.get("created_at") or "")[:16],
    }


@router.get("/api/source-documents", response_class=JSONResponse)
async def api_get_source_documents(
    page: int = 1,
    page_size: int = 20,
    status: str = "",
    source: str = "",
    doc_type: str = "",
    file_type: str = "",
    search: str = "",
):
    """获取 source_documents 列表（分页 + 筛选）

    状态流转：failed → pending → extracted → ready_to_pipe → processing → done
    """
    from utils.db_utils import execute_cloud_query

    # 构建查询条件
    conditions = []
    params = []

    if status:
        conditions.append("extract_status = %s")
        params.append(status)

    if source:
        conditions.append("source = %s")
        params.append(source)

    if doc_type:
        conditions.append("doc_type = %s")
        params.append(doc_type)

    if file_type:
        conditions.append("file_type = %s")
        params.append(file_type)

    if search:
        conditions.append("(title LIKE %s OR text_content LIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    # 查询总数
    count_sql = f"SELECT COUNT(*) as total FROM source_documents WHERE {where_clause}"
    total_rows = execute_cloud_query(count_sql, params)
    total = total_rows[0]["total"] if total_rows else 0

    # 分页查询
    offset = (page - 1) * page_size
    data_sql = f"""
        SELECT id, doc_type, file_type, title, author, source, publish_date,
               extract_status, reviewed_at, reviewed_by, review_notes,
               created_at, updated_at
        FROM source_documents
        WHERE {where_clause}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """
    rows = execute_cloud_query(data_sql, params + [page_size, offset])

    # 格式化结果
    items = []
    for r in rows or []:
        items.append({
            "id": str(r["id"]),  # 字符串，避免 JS Number 精度丢失
            "doc_type": r.get("doc_type") or "",
            "file_type": r.get("file_type") or "",
            "title": r.get("title") or "",
            "author": r.get("author") or "",
            "source": r.get("source") or "",
            "publish_date": str(r.get("publish_date") or "")[:10],
            "extract_status": r.get("extract_status") or "pending",
            "status_label": STATUS_LABELS.get(r.get("extract_status"), r.get("extract_status", "")),
            "status_color": STATUS_COLORS.get(r.get("extract_status"), "gray"),
            "reviewed_at": str(r.get("reviewed_at") or "")[:16] if r.get("reviewed_at") else "",
            "reviewed_by": r.get("reviewed_by") or "",
            "review_notes": r.get("review_notes") or "",
            "created_at": str(r.get("created_at") or "")[:16],
        })

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
        "items": items,
    }


@router.get("/api/source-documents/stats", response_class=JSONResponse)
async def api_get_source_documents_stats():
    """获取 source_documents 状态统计"""
    from utils.db_utils import execute_cloud_query

    sql = """
        SELECT extract_status, COUNT(*) as cnt
        FROM source_documents
        GROUP BY extract_status
    """
    rows = execute_cloud_query(sql)

    stats = {
        "total": 0,
        "failed": 0,
        "pending": 0,
        "extracted": 0,
        "ready_to_pipe": 0,
        "processing": 0,
        "done": 0,
        "rejected": 0,
        "remix": 0,
    }

    for r in rows or []:
        status = r.get("extract_status") or "pending"
        cnt = r.get("cnt", 0)
        stats["total"] += cnt
        if status in stats:
            stats[status] = cnt

    return stats


@router.post("/api/source-documents/review", response_class=JSONResponse)
async def api_review_source_documents(request: Request):
    """批量审核文档

    审核后状态从 extracted 变为 ready_to_pipe
    """
    from utils.db_utils import execute_cloud_insert

    body = await request.json()
    doc_ids = body.get("doc_ids", [])
    review_notes = body.get("review_notes", "")
    reviewer = body.get("reviewer", "admin")

    if not doc_ids:
        return JSONResponse({"error": "doc_ids 不能为空"}, status_code=400)

    placeholders = ",".join(["%s"] * len(doc_ids))
    try:
        execute_cloud_insert(
            f"""
            UPDATE source_documents
            SET extract_status = 'ready_to_pipe',
                reviewed_at = NOW(),
                reviewed_by = %s,
                review_notes = %s
            WHERE id IN ({placeholders}) AND extract_status = 'extracted'
            """,
            [reviewer, review_notes] + doc_ids,
        )
        return {"ok": True, "reviewed": len(doc_ids)}
    except Exception as e:
        logger.error(f"review_source_documents error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/remix-save", response_class=JSONResponse)
async def api_remix_save(request: Request):
    """保存 Remix 编辑结果，将文档状态置为 remix

    body: { doc_id: int, extracted_text: str }
    """
    from utils.db_utils import execute_cloud_insert

    body = await request.json()
    doc_id = body.get("doc_id")
    extracted_text = body.get("extracted_text", "")

    if not doc_id:
        return JSONResponse({"error": "doc_id 不能为空"}, status_code=400)

    try:
        execute_cloud_insert(
            "UPDATE source_documents SET extracted_text=%s, extract_status='remix' WHERE id=%s",
            [extracted_text, doc_id],
        )
        return {"ok": True}
    except Exception as e:
        logger.error(f"remix-save error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/source-documents/pipe", response_class=JSONResponse)
async def api_pipe_source_documents(request: Request, background_tasks: BackgroundTasks):
    """执行管线处理

    将 ready_to_pipe 状态的文档推入 extracted_texts 并启动清洗管线
    """
    from utils.db_utils import execute_cloud_insert
    from ingestion.source_extractor import push_to_extracted_texts_by_ids

    body = await request.json()
    doc_ids = body.get("doc_ids", [])

    if not doc_ids:
        # 如果未指定 ID，处理所有 ready_to_pipe / remix 状态的文档
        from utils.db_utils import execute_cloud_query
        rows = execute_cloud_query(
            "SELECT id FROM source_documents WHERE extract_status IN ('ready_to_pipe', 'remix') LIMIT 100"
        )
        doc_ids = [r["id"] for r in rows or []]

    if not doc_ids:
        return {"pushed": 0, "message": "没有待处理的文档"}

    # 更新状态为 processing
    placeholders = ",".join(["%s"] * len(doc_ids))
    try:
        execute_cloud_insert(
            f"UPDATE source_documents SET extract_status = 'processing' WHERE id IN ({placeholders})",
            doc_ids,
        )
    except Exception as e:
        logger.error(f"更新状态为 processing 失败: {e}")

    # 推入 extracted_texts
    try:
        result = push_to_extracted_texts_by_ids(doc_ids)
        pushed = result.get("pushed", 0)
        skipped = result.get("skipped", 0)
        failed = result.get("failed", 0)

        # 更新成功的文档状态为 done
        if pushed > 0:
            execute_cloud_insert(
                f"""
                UPDATE source_documents SET extract_status = 'done'
                WHERE id IN ({placeholders}) AND extract_status = 'processing'
                """,
                doc_ids,
            )

        return {
            "pushed": pushed,
            "skipped": skipped,
            "failed": failed,
            "message": f"成功推入 {pushed} 条，跳过 {skipped} 条，失败 {failed} 条",
        }
    except Exception as e:
        logger.error(f"pipe_source_documents error: {e}")
        # 回滚状态
        execute_cloud_insert(
            f"UPDATE source_documents SET extract_status = 'ready_to_pipe' WHERE id IN ({placeholders})",
            doc_ids,
        )
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/source-documents/retry", response_class=JSONResponse)
async def api_retry_source_documents(request: Request):
    """重试失败的文档

    将 failed 状态重置为 pending，重新触发提取
    """
    from utils.db_utils import execute_cloud_insert

    body = await request.json()
    doc_ids = body.get("doc_ids", [])

    if not doc_ids:
        return JSONResponse({"error": "doc_ids 不能为空"}, status_code=400)

    placeholders = ",".join(["%s"] * len(doc_ids))
    try:
        execute_cloud_insert(
            f"UPDATE source_documents SET extract_status = 'pending' WHERE id IN ({placeholders}) AND extract_status = 'failed'",
            doc_ids,
        )
        return {"ok": True, "retried": len(doc_ids)}
    except Exception as e:
        logger.error(f"retry_source_documents error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/backfill-summary-chunks")
async def backfill_summary_chunks(
    batch_size: int = 100,
    dry_run: bool = False,
):
    """回填族2摘要 chunk（research_report/strategy_report/roadshow_notes/feature_news）"""
    try:
        from retrieval.summary_chunker import backfill_family2
        result = backfill_family2(batch_size=batch_size, dry_run=dry_run)
        return {"status": "ok", "result": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}

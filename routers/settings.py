"""系统设置路由 — API配置 / 非结构化信息源 / 结构化数据监控"""
import json
import os
import logging
import threading
from datetime import datetime, date
from decimal import Decimal


def _safe_json(v):
    """递归将任意值转为 JSON 可序列化类型。"""
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, dict):
        return {k: _safe_json(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [_safe_json(i) for i in v]
    return v


def _safe_task(task: dict) -> dict:
    return {k: _safe_json(v) for k, v in task.items()}
from pathlib import Path

from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from utils.db_utils import execute_query, execute_insert, execute_cloud_query
from utils.sys_config import get_config, set_config
from utils.fetch_config import (
    load_fetch_settings, save_fetch_settings,
    SOURCE_GROUPS,
)
from utils.skill_registry import get_analysis_registry, get_skill_content, save_skill_content

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/settings", tags=["settings"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


# ==================== 结构化数据批量任务 ====================
_struct_tasks = {}


# ==================== 公共上下文 ====================

def _db_stats():
    stats = {}
    for table in ["raw_items", "cleaned_items", "stock_info", "stock_daily",
                   "capital_flow", "deep_research", "kg_entities", "kg_relationships"]:
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
def settings_api_page(request: Request):
    """Tab 1: API 配置 — 多模型配置"""
    ctx = _common_ctx("api")
    ctx["request"] = request

    ctx["api_usage"] = execute_query(
        """SELECT api_name, call_date, call_count, input_tokens, output_tokens, cost_usd
           FROM api_usage ORDER BY call_date DESC LIMIT 30"""
    )
    ctx["pipeline_logs"] = execute_query(
        "SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 20"
    )
    # 多模型配置
    try:
        model_configs = execute_query("SELECT * FROM model_configs ORDER BY id")
        ctx["model_configs"] = [dict(r) for r in (model_configs or [])]
    except Exception:
        ctx["model_configs"] = []

    # 保留旧 claude 配置（向后兼容）
    ctx["config"] = {
        "claude_api_key": get_config("claude_api_key") or os.getenv("ANTHROPIC_API_KEY", "") or os.getenv("ANTHROPIC_AUTH_TOKEN", ""),
        "claude_base_url": get_config("claude_base_url") or os.getenv("ANTHROPIC_BASE_URL", ""),
        "claude_model": get_config("claude_model") or "claude-sonnet-4-20250514",
    }

    # 已存储的 API Key（脱敏展示）
    stored_keys = {}
    try:
        key_rows = execute_query("SELECT config_key, value FROM system_config")
        for r in key_rows:
            if "_api_key" in r["config_key"] or r["config_key"] == "claude_api_key":
                v = r["value"] or ""
                if v:
                    stored_keys[r["config_key"]] = v[:8] + "..." + v[-4:] if len(v) > 12 else v[:4] + "..."
                else:
                    stored_keys[r["config_key"]] = ""
    except Exception:
        pass
    ctx["stored_keys"] = stored_keys

    return templates.TemplateResponse("settings.html", ctx)



@router.get("/structured", response_class=HTMLResponse)
def settings_structured_page(request: Request):
    """Tab 3: 结构化数据监控"""
    ctx = _common_ctx("structured")
    ctx["request"] = request

    # 监控规则
    try:
        ctx["monitor_rules"] = [dict(r) for r in execute_query(
            "SELECT * FROM data_monitor_rules ORDER BY module_name, data_type"
        ) or []]
    except Exception:
        ctx["monitor_rules"] = []

    # 数据新鲜度
    freshness = {}
    checks = [
        ("daily",      "SELECT MAX(trade_date) as latest FROM stock_daily"),
        ("capital",    "SELECT MAX(trade_date) as latest FROM capital_flow"),
        ("financial",  "SELECT MAX(report_period) as latest FROM financial_reports"),
        ("northbound", "SELECT MAX(trade_date) as latest FROM northbound_flow"),
    ]
    for key, sql in checks:
        try:
            rows = execute_query(sql)
            freshness[key] = str(rows[0]["latest"]) if rows and rows[0]["latest"] else None
        except Exception:
            freshness[key] = None
    ctx["data_freshness"] = freshness

    # 保留原有 watchlist / last_daily_date 供折叠批量下载区域用
    try:
        ctx["watchlist"] = execute_query(
            """SELECT w.stock_code, COALESCE(s.stock_name, '') as stock_name
               FROM watchlist w LEFT JOIN stock_info s ON w.stock_code=s.stock_code
               ORDER BY w.added_at DESC"""
        )
    except Exception:
        ctx["watchlist"] = []

    ctx["last_daily_date"] = freshness.get("daily")
    ctx["now_date"] = datetime.now().strftime("%Y-%m-%d")
    return templates.TemplateResponse("settings.html", ctx)


@router.get("/skills", response_class=HTMLResponse)
def settings_skills_page(request: Request):
    """Tab 4: Skill 配置"""
    ctx = _common_ctx("skills")
    ctx["request"] = request
    ctx["registry"] = get_analysis_registry()
    return templates.TemplateResponse("settings.html", ctx)


@router.get("/strategy", response_class=HTMLResponse)
def settings_strategy_page(request: Request):
    """Tab 5: 选股策略规则库"""
    from config.stock_selection_presets import RULE_CATEGORIES
    ctx = _common_ctx("strategy")
    ctx["request"] = request
    ctx["rule_categories"] = RULE_CATEGORIES
    try:
        rows = execute_query(
            "SELECT * FROM stock_selection_rules WHERE is_active=1 ORDER BY is_system DESC, sort_order, id"
        ) or []
        ctx["rules"] = [dict(r) for r in rows]
        ctx["system_count"] = sum(1 for r in ctx["rules"] if r["is_system"])
        ctx["custom_count"] = sum(1 for r in ctx["rules"] if not r["is_system"])
    except Exception:
        ctx["rules"] = []
        ctx["system_count"] = 0
        ctx["custom_count"] = 0
    return templates.TemplateResponse("settings.html", ctx)


# ==================== 多模型配置 API ====================

@router.get("/api/model-configs")
def api_get_model_configs():
    """返回所有 stage 的模型配置"""
    try:
        rows = execute_query("SELECT * FROM model_configs ORDER BY id")
        return JSONResponse({"ok": True, "configs": [dict(r) for r in (rows or [])]})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]})


@router.post("/api/save-model-config")
async def api_save_model_config(request: Request):
    """保存单个 stage 的模型配置"""
    data = await request.json()
    stage = (data.get("stage") or "").strip()
    if not stage:
        return JSONResponse({"ok": False, "error": "stage 不能为空"}, status_code=400)

    provider = (data.get("provider") or "claude_cli").strip()
    model_name = (data.get("model_name") or "sonnet").strip()
    api_key_value = (data.get("api_key") or "").strip()
    base_url = (data.get("base_url") or "").strip()
    extra_json = (data.get("extra_json") or "").strip()
    explicit_key_ref = (data.get("api_key_ref") or "").strip()

    # 如果提供了 api_key，存到 system_config
    # 优先使用用户指定的 key_ref，否则自动用 {provider}_api_key
    api_key_ref = None
    if api_key_value:
        api_key_ref = explicit_key_ref if explicit_key_ref else f"{provider}_api_key"
        set_config(api_key_ref, api_key_value)
    elif explicit_key_ref:
        # 没有新 key 值，但用户指定了 key_ref，直接更新引用
        api_key_ref = explicit_key_ref

    execute_insert(
        """INSERT INTO model_configs (stage, provider, model_name, api_key_ref, base_url, extra_json)
           VALUES (%s, %s, %s, %s, %s, %s)
           ON DUPLICATE KEY UPDATE
             provider=VALUES(provider), model_name=VALUES(model_name),
             api_key_ref=COALESCE(VALUES(api_key_ref), api_key_ref),
             base_url=VALUES(base_url), extra_json=VALUES(extra_json),
             updated_at=NOW()""",
        [stage, provider, model_name, api_key_ref, base_url or None, extra_json or None],
    )

    # 清除缓存
    try:
        from utils.model_router import invalidate_config_cache
        invalidate_config_cache(stage)
    except Exception:
        pass

    return JSONResponse({"ok": True})


@router.post("/api/save-key")
async def api_save_key(request: Request):
    """直接保存一个 API Key 到 system_config"""
    data = await request.json()
    key_name = (data.get("key_name") or "").strip()
    key_value = (data.get("key_value") or "").strip()
    if not key_name or not key_value:
        return JSONResponse({"ok": False, "error": "key_name 和 key_value 不能为空"}, status_code=400)
    try:
        set_config(key_name, key_value)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]})



def api_test_model(stage: str):
    """测试指定 stage 的模型连通性"""
    try:
        from utils.model_router import call_model
        result = call_model(stage, "回复OK即可", "测试连通性", max_tokens=20, timeout=30)
        return JSONResponse({"ok": True, "response": result[:100]})
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {str(e)[:200]}"})


# ==================== API 配置（保留旧接口向后兼容）====================

@router.post("/api/save-config")
async def save_config(request: Request):
    """保存 Claude API 配置（兼容旧接口）"""
    data = await request.json()
    saved = []
    for key in ["claude_api_key", "claude_base_url", "claude_model"]:
        if key in data:
            set_config(key, data[key])
            saved.append(key)
    return JSONResponse({"ok": True, "saved": saved})


@router.post("/api/test-claude")
def test_claude(request: Request):
    """测试 Claude API 连通性（兼容旧接口）"""
    try:
        from utils.model_router import call_model
        result = call_model("chat", "回复OK即可", "测试连通性", max_tokens=20, timeout=30)
        return JSONResponse({"ok": True, "response": result[:100]})
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"{type(e).__name__}: {str(e)[:200]}"})


@router.get("/api/skill-content/{skill_name}")
def api_skill_content(skill_name: str):
    """返回 Skill 文件内容（用于预览）"""
    content = get_skill_content(skill_name)
    if content is None:
        return JSONResponse({"ok": False, "error": "Skill 文件不存在"}, status_code=404)
    return JSONResponse({"ok": True, "content": content})


@router.post("/api/skill-save/{skill_name}")
async def api_skill_save(skill_name: str, request: Request):
    """保存 Skill 文件内容"""
    data = await request.json()
    content = data.get("content", "")
    if not skill_name or "/" in skill_name or ".." in skill_name:
        return JSONResponse({"ok": False, "error": "非法 skill_name"}, status_code=400)
    try:
        path = save_skill_content(skill_name, content)
        return JSONResponse({"ok": True, "path": path, "chars": len(content)})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


# ==================== 选股策略规则库 API ====================

@router.get("/api/selection-rules")
def api_list_selection_rules():
    """返回所有活跃规则（含分类元数据）"""
    from config.stock_selection_presets import RULE_CATEGORIES
    try:
        rows = execute_query(
            "SELECT * FROM stock_selection_rules WHERE is_active=1 ORDER BY is_system DESC, sort_order, id"
        ) or []
        return JSONResponse({
            "ok": True,
            "rules": [dict(r) for r in rows],
            "categories": RULE_CATEGORIES,
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


@router.post("/api/selection-rules/seed")
def api_seed_selection_rules():
    """初始化/补充系统预置规则（INSERT IGNORE），写入 layer 字段"""
    from config.stock_selection_presets import PRESET_RULES
    added = 0
    for i, rule in enumerate(PRESET_RULES):
        try:
            execute_insert(
                """INSERT INTO stock_selection_rules
                   (category, rule_name, definition, layer, is_system, sort_order)
                   VALUES (%s, %s, %s, %s, 1, %s)
                   ON DUPLICATE KEY UPDATE
                     layer=VALUES(layer), definition=VALUES(definition)""",
                [rule["category"], rule["rule_name"], rule["definition"],
                 rule.get("layer", 0), i],
            )
            added += 1
        except Exception:
            pass
    return JSONResponse({"ok": True, "added": added, "total": len(PRESET_RULES)})


@router.post("/api/selection-rules")
async def api_add_selection_rule(request: Request):
    """新增自定义规则"""
    data = await request.json()
    category = (data.get("category") or "").strip()
    rule_name = (data.get("rule_name") or "").strip()
    definition = (data.get("definition") or "").strip()
    if not rule_name or not definition:
        return JSONResponse({"ok": False, "error": "规则名称和定义不能为空"}, status_code=400)
    if not category:
        category = "custom"
    try:
        new_id = execute_insert(
            """INSERT INTO stock_selection_rules (category, rule_name, definition, is_system)
               VALUES (%s, %s, %s, 0)""",
            [category, rule_name, definition],
        )
        return JSONResponse({"ok": True, "id": new_id})
    except Exception as e:
        msg = str(e)
        if "Duplicate" in msg:
            return JSONResponse({"ok": False, "error": "规则名称已存在"}, status_code=400)
        return JSONResponse({"ok": False, "error": msg[:200]}, status_code=500)


@router.put("/api/selection-rules/{rule_id}")
async def api_update_selection_rule(rule_id: int, request: Request):
    """更新规则（仅自定义规则可改）"""
    data = await request.json()
    rule_name = (data.get("rule_name") or "").strip()
    definition = (data.get("definition") or "").strip()
    category = (data.get("category") or "").strip()
    if not rule_name or not definition:
        return JSONResponse({"ok": False, "error": "规则名称和定义不能为空"}, status_code=400)
    try:
        execute_insert(
            """UPDATE stock_selection_rules
               SET rule_name=%s, definition=%s, category=%s, updated_at=NOW()
               WHERE id=%s AND is_system=0""",
            [rule_name, definition, category or "custom", rule_id],
        )
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


@router.delete("/api/selection-rules/{rule_id}")
def api_delete_selection_rule(rule_id: int):
    """删除规则（仅自定义规则可删）"""
    try:
        execute_insert(
            "UPDATE stock_selection_rules SET is_active=0 WHERE id=%s AND is_system=0",
            [rule_id],
        )
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


# ==================== 数据监控规则 API ====================

@router.post("/api/save-monitor-rule")
async def save_monitor_rule(request: Request):
    """创建或更新监控规则"""
    data = await request.json()
    rule_id = data.get("id")
    module_name = (data.get("module_name") or "").strip()
    data_type = (data.get("data_type") or "").strip()
    if not module_name or not data_type:
        return JSONResponse({"ok": False, "error": "module_name 和 data_type 不能为空"}, status_code=400)

    stock_pool = data.get("stock_pool", "watchlist")
    custom_codes_json = json.dumps(data.get("custom_codes", []), ensure_ascii=False) if data.get("custom_codes") else None
    lookback_days = int(data.get("lookback_days", 7))
    schedule_cron = (data.get("schedule_cron") or "").strip() or None
    enabled = 1 if data.get("enabled", True) else 0

    if rule_id:
        execute_insert(
            """UPDATE data_monitor_rules
               SET module_name=%s, data_type=%s, stock_pool=%s, custom_codes_json=%s,
                   lookback_days=%s, schedule_cron=%s, enabled=%s, updated_at=NOW()
               WHERE id=%s""",
            [module_name, data_type, stock_pool, custom_codes_json,
             lookback_days, schedule_cron, enabled, rule_id],
        )
        return JSONResponse({"ok": True, "id": rule_id})
    else:
        new_id = execute_insert(
            """INSERT INTO data_monitor_rules
               (module_name, data_type, stock_pool, custom_codes_json, lookback_days, schedule_cron, enabled)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
                 stock_pool=VALUES(stock_pool), lookback_days=VALUES(lookback_days),
                 schedule_cron=VALUES(schedule_cron), enabled=VALUES(enabled), updated_at=NOW()""",
            [module_name, data_type, stock_pool, custom_codes_json,
             lookback_days, schedule_cron, enabled],
        )
        return JSONResponse({"ok": True, "id": new_id})


@router.post("/api/trigger-monitor/{rule_id}")
def trigger_monitor(rule_id: int, background_tasks: BackgroundTasks):
    """手动触发一条监控规则的数据拉取"""
    rule = execute_query("SELECT * FROM data_monitor_rules WHERE id=%s", [rule_id])
    if not rule:
        return JSONResponse({"ok": False, "error": "规则不存在"}, status_code=404)
    rule = dict(rule[0])

    def _run():
        try:
            # 获取股票列表
            pool = rule.get("stock_pool", "watchlist")
            if pool == "all":
                rows = execute_query("SELECT stock_code FROM stock_info LIMIT 5000")
                codes = [r["stock_code"] for r in rows]
            elif pool == "custom":
                custom = json.loads(rule.get("custom_codes_json") or "[]")
                codes = [c for c in custom if c]
            else:
                rows = execute_query(
                    "SELECT DISTINCT stock_code FROM watchlist_list_stocks WHERE status='active'"
                )
                codes = [r["stock_code"] for r in rows]

            if not codes:
                execute_insert(
                    "UPDATE data_monitor_rules SET last_run_at=NOW(), last_status=%s WHERE id=%s",
                    ["no_stocks", rule_id],
                )
                return

            from ingestion.akshare_source import (
                fetch_stock_daily, fetch_capital_flow, fetch_financial_data
            )
            dtype = rule.get("data_type", "daily")
            ok_count = 0
            for code in codes:
                try:
                    if dtype == "daily":
                        fetch_stock_daily(code)
                    elif dtype == "capital":
                        fetch_capital_flow(code)
                    elif dtype == "financial":
                        fetch_financial_data(code)
                    ok_count += 1
                except Exception:
                    pass

            execute_insert(
                "UPDATE data_monitor_rules SET last_run_at=NOW(), last_status=%s WHERE id=%s",
                [f"ok:{ok_count}", rule_id],
            )
        except Exception as e:
            execute_insert(
                "UPDATE data_monitor_rules SET last_run_at=NOW(), last_status=%s WHERE id=%s",
                [f"error:{str(e)[:50]}", rule_id],
            )

    background_tasks.add_task(_run)
    return JSONResponse({"ok": True, "message": f"已触发规则 #{rule_id} 后台执行"})


@router.delete("/api/delete-monitor/{rule_id}")
def delete_monitor_rule(rule_id: int):
    """删除监控规则"""
    execute_insert("DELETE FROM data_monitor_rules WHERE id=%s", [rule_id])
    return JSONResponse({"ok": True})



# ==================== 结构化数据批量下载（折叠保留）====================

@router.post("/api/struct-batch")
async def struct_batch_download(request: Request, background_tasks: BackgroundTasks):
    """触发结构化数据批量下载"""
    data = await request.json()
    pool = data.get("pool", "watchlist")
    custom_codes = data.get("custom_codes", [])
    start_date = data.get("start_date", "20240101")
    end_date = data.get("end_date", datetime.now().strftime("%Y%m%d"))
    data_types = data.get("data_types", ["daily", "capital", "financial"])

    if pool == "all":
        try:
            rows = execute_query("SELECT stock_code FROM stock_info LIMIT 5000")
            codes = [r["stock_code"] for r in rows]
        except Exception:
            codes = []
    elif pool == "custom":
        codes = [c.strip() for c in custom_codes if c.strip()]
    else:
        try:
            rows = execute_query(
                "SELECT DISTINCT stock_code FROM watchlist_list_stocks WHERE status='active'"
            )
            codes = [r["stock_code"] for r in rows]
            if not codes:
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
def struct_task_status(task_id: str):
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


# ==================== 清洗管线手动触发 ====================
_pipeline_tasks: dict = {}
_extract_tasks: dict = {}


def _pipeline_task_run(task_id: str, mode: str, batch_size: int):
    task = _pipeline_tasks[task_id]
    try:
        if mode == "a":
            from cleaning.content_summarizer import summarize_single
            from utils.db_utils import execute_cloud_query as _cq
            pending = _cq(
                """SELECT et.id FROM extracted_texts et
                   LEFT JOIN content_summaries cs ON et.id = cs.extracted_text_id
                   WHERE cs.id IS NULL ORDER BY et.id LIMIT %s""",
                [batch_size],
            )
            task["total"] = len(pending)
            ok = fail = 0
            for i, row in enumerate(pending):
                import time
                while task.get("paused"):
                    if task.get("cancel"): break
                    time.sleep(1)
                if task.get("cancel"):
                    task["status"] = "cancelled"
                    break
                task["progress"] = i + 1
                task["current"] = f"id={row['id']}"
                try:
                    summarize_single(row["id"])
                    ok += 1
                except Exception as e:
                    fail += 1
                    logger.warning(f"Pipeline A id={row['id']}: {e}")
            task["result"] = {"ok": ok, "fail": fail}

        elif mode == "b2":
            from cleaning.stock_mentions_extractor import extract_mentions_single, _get_deepseek_client
            from utils.db_utils import execute_cloud_query as _cq
            pending = _cq(
                """SELECT et.id FROM extracted_texts et
                   LEFT JOIN stock_mentions sm ON et.id = sm.extracted_text_id
                   WHERE sm.id IS NULL ORDER BY et.id LIMIT %s""",
                [batch_size],
            )
            task["total"] = len(pending)
            client = _get_deepseek_client()
            total = 0
            for i, row in enumerate(pending):
                import time
                while task.get("paused"):
                    if task.get("cancel"): break
                    time.sleep(1)
                if task.get("cancel"):
                    task["status"] = "cancelled"
                    break
                task["progress"] = i + 1
                task["current"] = f"id={row['id']}"
                try:
                    n = extract_mentions_single(row["id"], client=client)
                    total += n
                except Exception as e:
                    logger.warning(f"Pipeline B2 id={row['id']}: {e}")
            task["result"] = {"mentions": total}

        elif mode == "c":
            from cleaning.unified_pipeline import _run_pipeline_c, _get_deepseek
            from utils.db_utils import execute_cloud_query as _cq
            pending = _cq(
                """SELECT id, full_text FROM extracted_texts
                   WHERE kg_status != 'done' OR kg_status IS NULL
                   ORDER BY id LIMIT %s""",
                [batch_size],
            )
            task["total"] = len(pending)
            _get_deepseek()  # warm up client
            total = 0
            for i, row in enumerate(pending):
                import time
                while task.get("paused"):
                    if task.get("cancel"): break
                    time.sleep(1)
                if task.get("cancel"):
                    task["status"] = "cancelled"
                    break
                task["progress"] = i + 1
                task["current"] = f"id={row['id']}"
                try:
                    n = _run_pipeline_c(row["id"], row["full_text"] or "")
                    total += n
                except Exception as e:
                    logger.warning(f"Pipeline C id={row['id']}: {e}")
            task["result"] = {"kg_rels": total}

        elif mode == "abc":
            from cleaning.unified_pipeline import process_pending
            task["current"] = "查询待处理..."

            def _on_progress(done, total, et_id, r):
                task["total"] = total
                task["progress"] = done
                if et_id and r:
                    a_ok = "✓" if r.get("summary_id") else "–"
                    task["current"] = f"id={et_id}  A:{a_ok}  B:{r.get('mentions',0)}  C:{r.get('kg_rels',0)}"

            
            def _check_cancel():
                import time
                while task.get("paused"):
                    if task.get("cancel"): return True
                    time.sleep(1)
                return task.get("cancel", False)

            result = process_pending(
                batch_size=batch_size,
                should_cancel=_check_cancel,
                on_progress=_on_progress,
            )
            task["progress"] = result["processed"]
            task["result"] = result
            import time
            while task.get("paused"):
                if task.get("cancel"): break
                time.sleep(1)
            if task.get("cancel"):
                task["status"] = "cancelled"
                return

        if task.get("status") != "cancelled":
            task["status"] = "done"
    except Exception as e:
        task["status"] = "error"
        task["result"] = {"error": str(e)}
        logger.error(f"管线任务失败 mode={mode}: {e}")


@router.post("/api/pipeline-run", response_class=JSONResponse)
async def run_pipeline(request: Request):
    """手动触发清洗管线"""
    data = await request.json()
    mode = data.get("mode", "abc")          # a / b2 / c / abc
    batch_size = int(data.get("batch_size", 20))
    task_id = f"pipeline_{mode}_{int(datetime.now().timestamp())}"
    _pipeline_tasks[task_id] = {
        "status": "running", "mode": mode,
        "progress": 0, "total": batch_size, "current": "初始化...",
        "result": None, "cancel": False, "started_at": datetime.now().isoformat(),
    }
    import threading
    threading.Thread(target=_pipeline_task_run, args=(task_id, mode, batch_size), daemon=True).start()
    return JSONResponse({"ok": True, "task_id": task_id})


@router.get("/api/pipeline-run/{task_id}", response_class=JSONResponse)
def pipeline_run_status(task_id: str):
    task = _pipeline_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return JSONResponse(_safe_task(task))

@router.post("/api/extract-run", response_class=JSONResponse)
async def run_extract(request: Request):
    """手动触发 source_documents → extracted_texts 提取"""
    data = await request.json()
    batch_size = int(data.get("batch_size", 20))
    file_types = data.get("file_types") or ["txt"]   # 默认只处理 txt
    sources = data.get("sources")                     # None = 不过滤信息源
    doc_type = data.get("doc_type") or None           # None = 不过滤文档类型
    task_id = f"extract_{int(datetime.now().timestamp())}"
    _extract_tasks[task_id] = {
        "status": "running", "progress": 0, "total": batch_size,
        "current": "初始化...", "result": None, "cancel": False,
        "started_at": datetime.now().isoformat(),
    }

    def _run():
        task = _extract_tasks[task_id]
        try:
            from ingestion.source_extractor import _extract_single, push_to_extracted_texts
            from utils.db_utils import execute_cloud_query as _cq, execute_cloud_insert as _ci
            placeholders = ",".join(["%s"] * len(file_types))
            sql = f"SELECT id, doc_type, file_type, title, text_content, oss_url FROM source_documents WHERE extract_status='pending' AND file_type IN ({placeholders})"
            params = list(file_types)
            if sources:
                src_ph = ",".join(["%s"] * len(sources))
                sql += f" AND source IN ({src_ph})"
                params.extend(sources)
            if doc_type:
                sql += " AND doc_type=%s"
                params.append(doc_type)
            sql += " LIMIT %s"
            params.append(batch_size)
            pending = _cq(sql, params)
            task["total"] = len(pending)
            ok = fail = skip = pushed = 0
            for i, row in enumerate(pending):
                import time
                while task.get("paused"):
                    if task.get("cancel"): break
                    time.sleep(1)
                if task.get("cancel"):
                    task["status"] = "cancelled"
                    break
                task["progress"] = i + 1
                task["current"] = f"id={row['id']} ({row.get('file_type','')})"
                try:
                    extracted = _extract_single(row)
                    from config.doc_types import classify_doc_type
                    new_doc_type = classify_doc_type(
                        row.get("title") or "",
                        (extracted or "")[:200],
                    )
                    _ci("UPDATE source_documents SET extracted_text=%s, extract_status='extracted', doc_type=%s WHERE id=%s",
                        [extracted, new_doc_type, row["id"]])
                    ok += 1
                    # 提取成功后立即推入 extracted_texts
                    try:
                        r = push_to_extracted_texts(limit=1)
                        pushed += r.get("pushed", 0)
                    except Exception:
                        pass
                except Exception as e:
                    err = str(e)
                    if "401" in err or "403" in err or "Unauthorized" in err:
                        _ci("UPDATE source_documents SET extract_status='skipped' WHERE id=%s", [row["id"]])
                        skip += 1
                    else:
                        _ci("UPDATE source_documents SET extract_status='failed' WHERE id=%s", [row["id"]])
                        fail += 1
            if task["status"] != "cancelled":
                task["status"] = "done"
            task["result"] = {"ok": ok, "fail": fail, "skip": skip, "pushed": pushed}
        except Exception as e:
            task["status"] = "error"
            task["result"] = {"error": str(e)}

    import threading
    threading.Thread(target=_run, daemon=True).start()
    return JSONResponse({"ok": True, "task_id": task_id})


@router.post("/api/push-to-pipeline", response_class=JSONResponse)
async def push_to_pipeline(request: Request):
    """将 extract_status='done' 但未入 extracted_texts 的文档批量推入管线"""
    data = await request.json()
    batch_size = int(data.get("batch_size", 500))
    task_id = f"push_{int(datetime.now().timestamp())}"
    _extract_tasks[task_id] = {
        "status": "running", "progress": 0, "total": batch_size,
        "current": "初始化...", "result": None, "cancel": False,
        "started_at": datetime.now().isoformat(),
    }

    def _run():
        task = _extract_tasks[task_id]
        try:
            from ingestion.source_extractor import push_to_extracted_texts
            result = push_to_extracted_texts(limit=batch_size)
            task["total"] = result.get("total", 0)
            task["progress"] = result.get("pushed", 0)
            task["status"] = "done"
            task["result"] = result
        except Exception as e:
            task["status"] = "error"
            task["result"] = {"error": str(e)}
            logger.error(f"推入管线失败: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return JSONResponse({"ok": True, "task_id": task_id})


@router.post("/api/extract-cancel/{task_id}", response_class=JSONResponse)
def cancel_extract(task_id: str):
    task = _extract_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    task["cancel"] = True
    return JSONResponse({"ok": True})


@router.get("/api/extract-run/{task_id}", response_class=JSONResponse)
def extract_run_status(task_id: str):
    task = _extract_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return JSONResponse(_safe_task(task))



@router.post("/api/pipeline-pause/{task_id}", response_class=JSONResponse)
def pause_pipeline(task_id: str):
    task = _pipeline_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    task["paused"] = True
    return JSONResponse({"ok": True})


@router.post("/api/pipeline-resume/{task_id}", response_class=JSONResponse)
def resume_pipeline(task_id: str):
    task = _pipeline_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    task["paused"] = False
    return JSONResponse({"ok": True})


@router.post("/api/pipeline-cancel/{task_id}", response_class=JSONResponse)
def cancel_pipeline(task_id: str):
    task = _pipeline_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    task["cancel"] = True
    return JSONResponse({"ok": True})


@router.get("/api/cleaning-logs", response_class=JSONResponse)
def get_cleaning_logs():
    """获取清洗管线日志：最近处理记录"""
    try:
        from utils.db_utils import execute_cloud_query as _cq, execute_query as _q
        cs = _cq("""SELECT cs.id, cs.doc_type, cs.summary, cs.created_at,
                          et.source, et.publish_time
                   FROM content_summaries cs
                   JOIN extracted_texts et ON cs.extracted_text_id = et.id
                   ORDER BY cs.created_at DESC LIMIT 20""")
        sm = _cq("""SELECT sm.id, sm.stock_name, sm.stock_code, sm.related_themes,
                          sm.mention_time, et.source
                   FROM stock_mentions sm
                   JOIN extracted_texts et ON sm.extracted_text_id = et.id
                   ORDER BY sm.id DESC LIMIT 20""")
        kg = _q("""SELECT r.id, e_s.entity_name as src, r.relation_type, e_t.entity_name as tgt,
                         r.created_at
                  FROM kg_relationships r
                  JOIN kg_entities e_s ON r.source_entity_id = e_s.id
                  JOIN kg_entities e_t ON r.target_entity_id = e_t.id
                  WHERE e_s.data_source = 'cleaning' OR e_t.data_source = 'cleaning'
                  ORDER BY r.created_at DESC LIMIT 20""")
        return JSONResponse(_safe_json({
            "content_summaries": [dict(r) for r in (cs or [])],
            "stock_mentions": [dict(r) for r in (sm or [])],
            "kg_relationships": [dict(r) for r in (kg or [])],
        }))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/source-doc-summary", response_class=JSONResponse)
def get_source_doc_summary():
    """返回云端文档库概况统计（供前端轮询刷新）"""
    try:
        from utils.db_utils import execute_cloud_query as _cq
        doc_stats = _cq("""
            SELECT source, COUNT(*) as doc_count,
                   SUM(CASE WHEN extract_status IN ('extracted','ready_to_pipe','done') THEN 1 ELSE 0 END) as extracted_count
            FROM source_documents
            GROUP BY source
        """)
        total_docs = sum(r["doc_count"] for r in (doc_stats or []))
        total_extracted = sum(r["extracted_count"] or 0 for r in (doc_stats or []))
        et_total = _cq("SELECT COUNT(*) as n FROM extracted_texts")
        cs_done  = _cq("SELECT COUNT(DISTINCT extracted_text_id) as n FROM content_summaries")
        sm_done  = _cq("SELECT COUNT(DISTINCT extracted_text_id) as n FROM stock_mentions")
        kg_done  = _cq("SELECT COUNT(*) as n FROM extracted_texts WHERE kg_status='done'")
        return JSONResponse({
            "total": total_docs,
            "extracted": total_extracted,
            "source_count": len(doc_stats or []),
            "et_total": et_total[0]["n"] if et_total else 0,
            "pipeline_a": cs_done[0]["n"] if cs_done else 0,
            "pipeline_b": sm_done[0]["n"] if sm_done else 0,
            "pipeline_c": kg_done[0]["n"] if kg_done else 0,
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ==================== 标签计算引擎 API ====================

_tagging_tasks: dict = {}


@router.post("/api/run-tagging", response_class=JSONResponse)
async def run_tagging(request: Request, background_tasks: BackgroundTasks):
    """触发标签计算
    body: {layer: 1|2|3, stock_code?: "600519", mode: "test"|"full"}
    """
    data = await request.json()
    layer = int(data.get("layer", 1))
    stock_code = (data.get("stock_code") or "").strip() or None
    mode = data.get("mode", "test")

    task_id = f"tagging_L{layer}_{int(datetime.now().timestamp())}"
    _tagging_tasks[task_id] = {
        "status": "running", "layer": layer, "mode": mode,
        "stock_code": stock_code, "result": None,
        "started_at": datetime.now().isoformat(),
    }

    def _run():
        task = _tagging_tasks[task_id]
        try:
            if layer == 1:
                from tagging.l1_quant_engine import run_l1_for_stock, run_l1_batch
                if stock_code:
                    result = run_l1_for_stock(stock_code)
                else:
                    result = run_l1_batch()
            elif layer == 2:
                from tagging.l2_ai_engine import run_l2_batch
                limit = 10 if mode == "test" else 100
                result = run_l2_batch(limit=limit)
            elif layer == 3:
                from tagging.l3_deep_engine import run_l3_for_stock, run_l3_batch
                if stock_code:
                    result = run_l3_for_stock(stock_code)
                else:
                    result = run_l3_batch()
            else:
                result = {"error": f"未知 layer: {layer}"}
            task["result"] = result
            task["status"] = "done"
        except Exception as e:
            task["status"] = "error"
            task["result"] = {"error": str(e)[:300]}
            logger.error(f"标签计算失败 layer={layer}: {e}")

    import threading
    threading.Thread(target=_run, daemon=True).start()
    return JSONResponse({"ok": True, "task_id": task_id})


@router.get("/api/tagging-status", response_class=JSONResponse)
def tagging_status():
    """返回各层标签计算状态统计"""
    try:
        stats = {}
        for layer in [1, 2, 3]:
            rows = execute_query(
                """SELECT COUNT(DISTINCT stock_code) as stocks,
                          COUNT(DISTINCT rule_id) as rules,
                          MAX(computed_at) as last_run
                   FROM stock_rule_tags WHERE layer=%s AND matched=1""",
                [layer],
            ) or []
            if rows:
                r = dict(rows[0])
                stats[f"L{layer}"] = {
                    "stocks": r.get("stocks", 0),
                    "rules": r.get("rules", 0),
                    "last_run": str(r.get("last_run", "")) if r.get("last_run") else None,
                }
            else:
                stats[f"L{layer}"] = {"stocks": 0, "rules": 0, "last_run": None}
        return JSONResponse({"ok": True, "stats": stats})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


@router.get("/api/tagging-task/{task_id}", response_class=JSONResponse)
def tagging_task_status(task_id: str):
    task = _tagging_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return JSONResponse(_safe_task(task))


# ==================== 股票标签批量更新 API ====================

@router.post("/api/run-batch-tag-update", response_class=JSONResponse)
async def run_batch_tag_update(request: Request):
    """触发分层批量标签更新
    body: {tiers?: ["A","B","C"], stock_code?: "600519"}
    Tier A = 全量L1量化, Tier B = 有新闻提及L2 AI, Tier C = 重点股票L3 AI深度
    """
    data = await request.json()
    tiers = data.get("tiers") or ["A", "B", "C"]
    stock_code = (data.get("stock_code") or "").strip() or None

    from tagging.batch_updater import start_batch_update_task
    task_id = start_batch_update_task(tiers=tiers, stock_code=stock_code)
    return JSONResponse({"ok": True, "task_id": task_id})


@router.get("/api/batch-tag-status", response_class=JSONResponse)
def batch_tag_status():
    """返回批量标签更新任务列表（最近10条）"""
    from tagging.batch_updater import get_all_batch_tasks
    tasks = get_all_batch_tasks()
    return JSONResponse({"ok": True, "tasks": [_safe_task(t) for t in tasks]})


@router.get("/api/batch-tag-task/{task_id}", response_class=JSONResponse)
def batch_tag_task_status(task_id: str):
    """查询单个批量标签更新任务状态"""
    from tagging.batch_updater import get_batch_task_status
    task = get_batch_task_status(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return JSONResponse(_safe_task(task))


@router.get("/api/stock-tag-stats", response_class=JSONResponse)
def stock_tag_stats():
    """返回三类标签的覆盖统计"""
    try:
        # 选股标签覆盖
        sel_rows = execute_query(
            """SELECT layer, COUNT(DISTINCT stock_code) as stocks, COUNT(*) as tags
               FROM stock_rule_tags WHERE matched=1 GROUP BY layer"""
        ) or []
        # KG行业标签覆盖
        ind_rows = execute_query(
            """SELECT COUNT(DISTINCT ke_src.external_id) as stocks
               FROM kg_entities ke_src
               JOIN kg_relationships kr ON kr.source_entity_id = ke_src.id
               JOIN kg_entities ke_tgt ON kr.target_entity_id = ke_tgt.id
               WHERE ke_src.entity_type='company' AND kr.relation_type='belongs_to_industry'
                 AND ke_src.external_id IS NOT NULL"""
        ) or []
        # KG主题标签覆盖
        theme_rows = execute_query(
            """SELECT COUNT(DISTINCT ke_src.external_id) as stocks
               FROM kg_entities ke_src
               JOIN kg_relationships kr ON kr.source_entity_id = ke_src.id
               JOIN kg_entities ke_tgt ON kr.target_entity_id = ke_tgt.id
               WHERE ke_src.entity_type='company' AND ke_tgt.entity_type='theme'
                 AND ke_src.external_id IS NOT NULL"""
        ) or []
        return JSONResponse({
            "ok": True,
            "selection": {f"L{r['layer']}": {"stocks": r["stocks"], "tags": r["tags"]} for r in sel_rows},
            "industry_kg_stocks": ind_rows[0]["stocks"] if ind_rows else 0,
            "theme_kg_stocks": theme_rows[0]["stocks"] if theme_rows else 0,
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)

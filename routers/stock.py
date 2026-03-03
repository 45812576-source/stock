"""个股研究路由 — 列表/详情/报告/追踪研究"""
import json
import logging
import time
import threading
import uuid
from datetime import datetime
from fastapi import APIRouter, Request, BackgroundTasks, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from utils.db_utils import execute_query, execute_insert
from utils.auth_deps import get_current_user, TokenData
from utils.quota_service import check_quota, consume_quota
from tracking.watchlist_manager import (
    add_to_watchlist, remove_from_watchlist,
    update_watch_type,
)
from research.report_generator import get_research_report, list_research_records
from research.universal_db import get_sector_heat_detail

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/stock", tags=["stock"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# ==================== 研究任务进度追踪 ====================

_research_tasks: dict = {}
_tasks_lock = threading.Lock()

_PROGRESS_MAP = {
    "正在检查数据充分性": 5, "正在获取个股数据": 10,
    "[1/6]": 15, "[2/6]": 28, "[3/6]": 42,
    "[4/6]": 55, "[5/6]": 68, "[6/6]": 80,
    "正在综合": 90, "正在保存": 95,
    # 单步重跑时的子步骤进度（估值4步、其他步骤通用）
    "[4a]": 15, "[4b]": 35, "[4c]": 60, "[4d]": 85,
    "[1]": 20, "[2]": 50, "[3]": 80,
    "读取已有报告": 5, "重跑完成": 100,
}

def _create_research_task(stock_code, stock_name=""):
    task_id = uuid.uuid4().hex[:8]
    with _tasks_lock:
        _research_tasks[task_id] = {
            "id": task_id, "stock_code": stock_code,
            "stock_name": stock_name, "status": "running",
            "progress": 0, "message": "准备中...",
            "created_at": time.time(), "finished_at": None,
            "research_id": None, "step_results": {},
        }
    return task_id

def _update_research_task(task_id, **kw):
    with _tasks_lock:
        if task_id in _research_tasks:
            _research_tasks[task_id].update(kw)

def _make_progress_cb(task_id):
    def cb(msg):
        pct = 0
        for key, val in _PROGRESS_MAP.items():
            if key in msg:
                pct = val
                break
        _update_research_task(task_id, message=msg, progress=pct)
    return cb


def _make_step_cb(task_id):
    """创建步骤结果回调，每完成一步就存入 task"""
    _STEP_LABELS = {
        "business_model": "商业模式画布",
        "value_chain": "产业链地图",
        "financial": "财务分析",
        "valuation": "估值分析",
        "sector_heat": "板块热度",
        "research_data": "研究数据",
    }
    def cb(step_name, result_text):
        with _tasks_lock:
            task = _research_tasks.get(task_id)
            # 处理 dict 或 string 两种结果类型
            result_str = result_text if isinstance(result_text, str) else str(result_text)
            if task and result_text and not result_str.startswith("分析失败"):
                # 对 dict 结果进行截断处理
                content = result_text[:3000] if isinstance(result_text, str) else str(result_text)[:3000]
                task["step_results"][step_name] = {
                    "label": _STEP_LABELS.get(step_name, step_name),
                    "content": content,
                }
    return cb


# ==================== 辅助函数 ====================

def _get_stock_info(stock_code):
    """获取个股基本信息"""
    rows = execute_query(
        "SELECT * FROM stock_info WHERE stock_code=?", [stock_code]
    )
    if rows:
        info = dict(rows[0])
        # 合并 industry_l1 为 industry 供模板使用
        info.setdefault("industry", info.get("industry_l1") or info.get("industry_l2") or "")
        return info
    return {"stock_code": stock_code, "stock_name": "", "industry": ""}


def _get_latest_price(stock_code):
    """获取最新行情（全部字段）"""
    rows = execute_query(
        """SELECT * FROM stock_daily WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT 1""",
        [stock_code],
    )
    return dict(rows[0]) if rows else None


def _get_kline_data(stock_code, days=120):
    """获取K线数据"""
    rows = execute_query(
        """SELECT trade_date, open, high, low, close, volume, amount
           FROM stock_daily WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT ?""",
        [stock_code, days],
    )
    return [dict(r) for r in reversed(rows)] if rows else []


def _get_knowledge_tags(stock_code):
    """获取个股关联的知识标签（三源聚合：行业/主题/选股）

    Returns:
        {
            'core': [  # 行业标签 + 投资主题标签（全显示）
                {'name': '半导体', 'type': 'industry'},
                {'name': 'AI芯片', 'type': 'theme'},
            ],
            'more': [  # 选股标签（matched=1 的规则）
                {'name': '均线多头排列', 'type': 'selection', 'category': '技术形态', 'layer': 1},
            ],
            'structured': {  # 结构化分类（供新模板使用）
                'industry': [...], 'themes': [...], 'selection': [...]
            }
        }
    """
    try:
        from tagging.stock_tag_service import get_stock_tags
        result = get_stock_tags(stock_code)
        display = result.to_display_dict()
        core = (
            [{"name": t, "type": "industry"} for t in result.industry_tags] +
            [{"name": t, "type": "theme"} for t in result.theme_tags]
        )
        more = [
            {
                "name": t.name, "type": "selection",
                "category": t.category, "layer": t.layer,
                "confidence": t.confidence,
            }
            for t in result.selection_tags
        ]
        return {"core": core, "more": more, "structured": display}
    except Exception as e:
        logger.warning(f"标签聚合失败，降级到旧逻辑: {e}")
        rows = execute_query(
            """SELECT DISTINCT e.entity_name, e.entity_type
               FROM kg_entities e
               JOIN kg_relationships r ON e.id=r.source_entity_id OR e.id=r.target_entity_id
               WHERE e.entity_name LIKE %s OR e.entity_type='stock'
               LIMIT 20""",
            [f"%{stock_code}%"],
        )
        if not rows:
            rows2 = execute_query(
                """SELECT ci.tags_json
                   FROM item_companies ic JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
                   WHERE ic.stock_code=%s
                   ORDER BY ci.cleaned_at DESC LIMIT 20""",
                [stock_code],
            )
            tags = set()
            for r in (rows2 or []):
                try:
                    for t in json.loads(r["tags_json"] or "[]"):
                        tags.add(t)
                except (json.JSONDecodeError, TypeError):
                    pass
            return {
                "core": [{"name": t, "type": "tag"} for t in list(tags)[:5]],
                "more": [{"name": t, "type": "tag"} for t in list(tags)[5:10]],
                "structured": {},
            }
        core = [{"name": r["entity_name"], "type": r["entity_type"]} for r in rows[:5]]
        more = [{"name": r["entity_name"], "type": r["entity_type"]} for r in rows[5:10]]
        return {"core": core, "more": more, "structured": {}}


def _get_latest_research(stock_code):
    """获取最新深度研究"""
    rows = execute_query(
        """SELECT * FROM deep_research
           WHERE research_type='stock' AND target=?
           ORDER BY created_at DESC LIMIT 1""",
        [stock_code],
    )
    if not rows:
        return None
    r = rows[0]
    report = json.loads(r["report_json"]) if r.get("report_json") else {}
    return {
        "id": r["id"],
        "date": r["research_date"],
        "scores": {
            "overall": r.get("overall_score", 0) or 0,
        },
        "recommendation": r.get("recommendation", ""),
        "report": report.get("report", report) if isinstance(report, dict) else report,
        "created_at": r.get("created_at", ""),
    }


def _get_research_history(stock_code, limit=10):
    """获取研究历史"""
    return execute_query(
        """SELECT id, research_date, overall_score, recommendation, created_at
           FROM deep_research
           WHERE research_type='stock' AND target=?
           ORDER BY created_at DESC LIMIT ?""",
        [stock_code, limit],
    )


def _get_related_news(stock_code, limit=10):
    """获取关联新闻"""
    return execute_query(
        """SELECT ci.id, ci.summary, ci.sentiment, ci.importance,
                  ci.event_type, ci.cleaned_at
           FROM item_companies ic JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
           WHERE ic.stock_code=?
           ORDER BY ci.cleaned_at DESC LIMIT ?""",
        [stock_code, limit],
    )


def _get_capital_flow(stock_code, limit=250):
    """获取资金流向"""
    return execute_query(
        """SELECT trade_date, main_net_inflow, super_large_net, large_net, medium_net, small_net
           FROM capital_flow WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT ?""",
        [stock_code, limit],
    )


def _get_data_sources():
    """获取可用数据源列表"""
    try:
        rows = execute_query("SELECT * FROM data_sources ORDER BY source_name")
        if rows:
            return rows
    except Exception:
        pass
    # 默认数据源
    return [
        {"id": 1, "source_name": "新闻资讯", "source_type": "news"},
        {"id": 2, "source_name": "研报数据", "source_type": "research"},
        {"id": 3, "source_name": "行情数据", "source_type": "market"},
        {"id": 4, "source_name": "资金流向", "source_type": "capital"},
        {"id": 5, "source_name": "知识图谱", "source_type": "knowledge"},
    ]


def _get_all_stock_tags(stock_code):
    """聚合个股的所有标签：watchlist + 新闻标签 + 知识图谱 + 行业"""
    tags = set()

    # 1. watchlist.related_tags
    wl = execute_query("SELECT related_tags FROM watchlist WHERE stock_code=?", [stock_code])
    for w in (wl or []):
        try:
            for t in json.loads(w["related_tags"] or "[]"):
                tags.add(t)
        except (json.JSONDecodeError, TypeError):
            pass

    # 2. item_companies → cleaned_items.tags_json（最近半年）
    news_tags = execute_query(
        """SELECT DISTINCT ci.tags_json
           FROM item_companies ic JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
           WHERE ic.stock_code=? AND ci.cleaned_at >= date('now', '-180 days')""",
        [stock_code],
    )
    for r in (news_tags or []):
        try:
            for t in json.loads(r["tags_json"] or "[]"):
                tags.add(t)
        except (json.JSONDecodeError, TypeError):
            pass

    # 3. 知识图谱实体
    kg = execute_query(
        """SELECT DISTINCT e.entity_name
           FROM kg_entities e
           JOIN kg_relationships r ON e.id=r.source_entity_id OR e.id=r.target_entity_id
           WHERE e.entity_name LIKE ? AND e.entity_type != 'stock'
           LIMIT 30""",
        [f"%{stock_code}%"],
    )
    for r in (kg or []):
        tags.add(r["entity_name"])

    # 4. 行业标签
    info = execute_query("SELECT industry_l1, industry_l2 FROM stock_info WHERE stock_code=?", [stock_code])
    if info:
        for col in ["industry_l1", "industry_l2"]:
            if info[0].get(col):
                tags.add(info[0][col])

    # 5. 股票名称本身也作为搜索词
    name_row = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=?", [stock_code])
    if name_row and name_row[0].get("stock_name"):
        tags.add(name_row[0]["stock_name"])

    return sorted(tags)


def _search_source_preview(source_type, stock_code, tags, months=6):
    """按标签搜索各信息源，返回命中数量和最新摘要"""
    date_cutoff = f"-{months * 30} days"
    tag_conditions = " OR ".join(["ci.tags_json LIKE ?"] * len(tags))
    tag_params = [f"%{t}%" for t in tags]

    if source_type == "news":
        # 新闻资讯：cleaned_items 中非研报类
        if not tags:
            return {"count": 0, "preview_items": []}
        sql = f"""SELECT ci.id, ci.summary, ci.sentiment, ci.importance, ci.cleaned_at
                  FROM cleaned_items ci
                  WHERE ({tag_conditions})
                    AND ci.cleaned_at >= date('now', ?)
                    AND ci.event_type != 'research_report'
                  ORDER BY ci.cleaned_at DESC"""
        rows = execute_query(sql, tag_params + [date_cutoff])
        items = [r["summary"][:80] if r.get("summary") else "" for r in (rows or [])[:5]]
        return {"count": len(rows or []), "preview_items": items}

    elif source_type == "research":
        # 研报数据
        rows = execute_query(
            """SELECT id, broker_name, report_type, rating, target_price, report_date
               FROM research_reports
               WHERE stock_code=? AND report_date >= date('now', ?)
               ORDER BY report_date DESC""",
            [stock_code, date_cutoff],
        )
        items = [
            f"{r.get('broker_name','')} {r.get('report_type','')} {r.get('rating','')}"
            for r in (rows or [])[:5]
        ]
        return {"count": len(rows or []), "preview_items": items}

    elif source_type == "market":
        # 行情数据
        rows = execute_query(
            """SELECT COUNT(*) as cnt FROM stock_daily
               WHERE stock_code=? AND trade_date >= date('now', ?)""",
            [stock_code, date_cutoff],
        )
        cnt = rows[0]["cnt"] if rows else 0
        return {"count": cnt, "preview_items": []}

    elif source_type == "capital":
        # 资金流向
        rows = execute_query(
            """SELECT COUNT(*) as cnt FROM capital_flow
               WHERE stock_code=? AND trade_date >= date('now', ?)""",
            [stock_code, date_cutoff],
        )
        cnt = rows[0]["cnt"] if rows else 0
        # 也查行业资金流
        if tags:
            ind_conditions = " OR ".join(["industry_name LIKE ?"] * len(tags))
            ind_params = [f"%{t}%" for t in tags]
            ind_rows = execute_query(
                f"""SELECT COUNT(*) as cnt FROM industry_capital_flow
                    WHERE ({ind_conditions}) AND trade_date >= date('now', ?)""",
                ind_params + [date_cutoff],
            )
            cnt += (ind_rows[0]["cnt"] if ind_rows else 0)
        return {"count": cnt, "preview_items": []}

    elif source_type == "knowledge":
        # 知识图谱
        rows = execute_query(
            """SELECT COUNT(DISTINCT r.id) as cnt
               FROM kg_relationships r
               JOIN kg_entities e1 ON r.source_entity_id=e1.id
               JOIN kg_entities e2 ON r.target_entity_id=e2.id
               WHERE e1.entity_name LIKE ? OR e2.entity_name LIKE ?""",
            [f"%{stock_code}%", f"%{stock_code}%"],
        )
        cnt = rows[0]["cnt"] if rows else 0
        return {"count": cnt, "preview_items": []}

    return {"count": 0, "preview_items": []}


def _collect_followup_context(stock_code, tags, selected_sources, months=6):
    """收集追踪研究的完整上下文数据"""
    date_cutoff = f"-{months * 30} days"
    context_parts = [f"股票: {stock_code}", f"搜索标签: {', '.join(tags)}", f"时间范围: 近{months}个月", ""]

    if "news" in selected_sources and tags:
        tag_conditions = " OR ".join(["ci.tags_json LIKE ?"] * len(tags))
        tag_params = [f"%{t}%" for t in tags]
        news = execute_query(
            f"""SELECT ci.summary, ci.sentiment, ci.importance, ci.event_type, ci.cleaned_at
                FROM cleaned_items ci
                WHERE ({tag_conditions}) AND ci.cleaned_at >= date('now', ?)
                  AND ci.event_type != 'research_report'
                ORDER BY ci.importance DESC, ci.cleaned_at DESC LIMIT 50""",
            tag_params + [date_cutoff],
        )
        if news:
            context_parts.append(f"=== 关联新闻 ({len(news)}条) ===")
            for n in news:
                context_parts.append(
                    f"  [{n.get('sentiment','')}][{n.get('importance',0)}⭐] "
                    f"{n.get('summary','')} ({str(n.get('cleaned_at',''))[:10]})"
                )
            context_parts.append("")

    if "research" in selected_sources:
        reports = execute_query(
            """SELECT broker_name, title, rating, target_price, report_date
               FROM research_reports
               WHERE stock_code=? AND report_date >= date('now', ?)
               ORDER BY report_date DESC LIMIT 20""",
            [stock_code, date_cutoff],
        )
        if reports:
            context_parts.append(f"=== 研报数据 ({len(reports)}条) ===")
            for r in reports:
                context_parts.append(
                    f"  {r.get('broker_name','')}: {r.get('title','')} "
                    f"评级:{r.get('rating','')} 目标价:{r.get('target_price','')}"
                )
            context_parts.append("")

    if "capital" in selected_sources:
        capital = execute_query(
            """SELECT trade_date, main_net_inflow, super_large_net, large_net
               FROM capital_flow WHERE stock_code=? AND trade_date >= date('now', ?)
               ORDER BY trade_date DESC LIMIT 30""",
            [stock_code, date_cutoff],
        )
        if capital:
            context_parts.append(f"=== 资金流向 ({len(capital)}条) ===")
            for c in capital:
                context_parts.append(
                    f"  {c['trade_date']}: 主力净流入{c.get('main_net_inflow',0)} "
                    f"超大单{c.get('super_large_net',0)} 大单{c.get('large_net',0)}"
                )
            context_parts.append("")

    return "\n".join(context_parts)


# ==================== 路由 ====================

@router.get("", response_class=HTMLResponse)
def stock_list(request: Request):
    """个股研究首页 — 重定向到自选页"""
    return RedirectResponse(url="/portfolio", status_code=302)


@router.get("/{stock_code}", response_class=HTMLResponse)
def stock_detail(request: Request, stock_code: str, user: TokenData = Depends(get_current_user)):
    """个股详情页"""
    user_id = user.user_id

    # 检查K线分析权限
    can_chart, chart_msg = check_quota(user_id, 'chart_analysis')

    # 先检查本地是否有数据，没有则从云端同步
    from utils.db_utils import ensure_stock_data
    sync_result = ensure_stock_data(stock_code, days=250)
    if sync_result.get('synced'):
        logger.info(f"已从云端同步 {stock_code} 数据: K线={sync_result.get('kline')}")

    stock = _get_stock_info(stock_code)
    price = _get_latest_price(stock_code)
    kline = _get_kline_data(stock_code, 250)
    layered_tags = _get_knowledge_tags(stock_code)
    research = _get_latest_research(stock_code)
    history = _get_research_history(stock_code)
    news = _get_related_news(stock_code)
    capital = _get_capital_flow(stock_code, 250)

    # 是否已在 watchlist
    wl = execute_query("SELECT watch_type FROM watchlist WHERE stock_code=%s", [stock_code])
    in_watchlist = wl[0]["watch_type"] if wl else None

    # 为模板准备标签数据
    core_tags = layered_tags.get('core', [])
    more_tags = layered_tags.get('more', [])

    return templates.TemplateResponse("stock_detail.html", {
        "request": request,
        "active_page": "stock",
        "stock": stock,
        "stock_code": stock_code,
        "price": price,
        "kline": kline,
        "kline_json": json.dumps(kline, ensure_ascii=False, default=str),
        "capital_json": json.dumps([dict(c) for c in reversed(capital)] if capital else [], ensure_ascii=False, default=str),
        "tags": core_tags + more_tags,  # 兼容旧模板
        "core_tags": core_tags,
        "more_tags": more_tags,
        "tags_json": json.dumps(core_tags + more_tags, ensure_ascii=False),
        "research": research,
        "history": history,
        "news": news,
        "capital": capital,
        "in_watchlist": in_watchlist,
        "can_chart_analysis": can_chart,
        "chart_analysis_msg": chart_msg,
    })


@router.get("/{stock_code}/report/{report_id}", response_class=HTMLResponse)
def stock_report(request: Request, stock_code: str, report_id: int):
    """深度研究报告页"""
    stock = _get_stock_info(stock_code)
    report = get_research_report(report_id)
    news = _get_related_news(stock_code, 8)

    if not report:
        return RedirectResponse(url=f"/stock/{stock_code}", status_code=302)

    # 板块热度详情（15日资金流时序）
    try:
        sector_heat_detail = get_sector_heat_detail(stock_code)
    except Exception:
        sector_heat_detail = {"company_flow_15d": [], "sub_industries": [], "investment_themes": []}

    return templates.TemplateResponse("stock_report.html", {
        "request": request,
        "active_page": "stock",
        "stock": stock,
        "stock_code": stock_code,
        "report": report,
        "news": news,
        "sector_heat_detail": sector_heat_detail,
    })


@router.get("/{stock_code}/followup", response_class=HTMLResponse)
def stock_followup(request: Request, stock_code: str):
    """追踪研究配置页"""
    stock = _get_stock_info(stock_code)
    research = _get_latest_research(stock_code)
    tags = _get_all_stock_tags(stock_code)

    # 各信息源按标签预检索，显示命中数量
    sources = [
        {"key": "news", "name": "新闻资讯", "icon": "newspaper", "desc": "按标签搜索关联新闻、舆情、公告"},
        {"key": "research", "name": "研报数据", "icon": "analytics", "desc": "券商研报、评级、目标价"},
        {"key": "market", "name": "行情数据", "icon": "candlestick_chart", "desc": "日K线、成交量、换手率"},
        {"key": "capital", "name": "资金流向", "icon": "account_balance", "desc": "主力资金、行业资金净流入"},
        {"key": "knowledge", "name": "知识图谱", "icon": "hub", "desc": "实体关系、事件关联网络"},
    ]
    for src in sources:
        preview = _search_source_preview(src["key"], stock_code, tags)
        src["count"] = preview["count"]
        src["preview_items"] = preview.get("preview_items", [])

    return templates.TemplateResponse("stock_followup.html", {
        "request": request,
        "active_page": "stock",
        "stock": stock,
        "stock_code": stock_code,
        "research": research,
        "tags": tags,
        "sources": sources,
    })


# ==================== API 操作 ====================

@router.get("/api/research-tasks")
def get_research_tasks():
    """获取所有研究任务状态"""
    now = time.time()
    with _tasks_lock:
        # 清理1小时前已完成的任务
        stale = [k for k, v in _research_tasks.items()
                 if v["finished_at"] and now - v["finished_at"] > 3600]
        for k in stale:
            del _research_tasks[k]
        # 返回摘要（不含 step_results 内容，减少传输量）
        result = []
        for t in _research_tasks.values():
            item = {k: v for k, v in t.items() if k != "step_results"}
            item["completed_steps"] = list(t.get("step_results", {}).keys())
            result.append(item)
        return result


@router.get("/api/research-task/{task_id}")
def get_research_task_detail(task_id: str):
    """获取单个研究任务详情（含步骤结果）"""
    with _tasks_lock:
        task = _research_tasks.get(task_id)
        if not task:
            return JSONResponse({"error": "任务不存在"}, status_code=404)
        return dict(task)

@router.delete("/{stock_code}/report/{report_id}")
def delete_report(stock_code: str, report_id: int):
    """删除一条深度研究报告"""
    row = execute_query(
        "SELECT id FROM deep_research WHERE id=? AND target=?",
        [report_id, stock_code],
    )
    if not row:
        return JSONResponse({"error": "报告不存在"}, status_code=404)
    execute_insert("DELETE FROM deep_research WHERE id=?", [report_id])
    return JSONResponse({"ok": True})


@router.post("/{stock_code}/run-research")
async def run_research(request: Request, stock_code: str,
                       background_tasks: BackgroundTasks):
    """整体重新研究（后台执行全部6步）"""
    from research.deep_researcher import deep_research_stock

    info = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=?", [stock_code])
    stock_name = info[0]["stock_name"] if info else stock_code
    task_id = _create_research_task(stock_code, stock_name)

    def _run():
        try:
            result = deep_research_stock(
                stock_code,
                progress_callback=_make_progress_cb(task_id),
                step_callback=_make_step_cb(task_id),
            )
            rid = result.get("research_id")
            if result.get("error"):
                _update_research_task(task_id, status="error", progress=100,
                                     message=f"失败: {result['error']}",
                                     finished_at=time.time(), research_id=rid)
            else:
                _update_research_task(task_id, status="done", progress=100,
                                     message="研究完成", finished_at=time.time(),
                                     research_id=rid)
        except Exception as e:
            logger.error(f"整体研究失败 {stock_code}: {e}")
            _update_research_task(task_id, status="error",
                                 message=f"异常: {e}", finished_at=time.time())

    background_tasks.add_task(_run)
    return JSONResponse({"ok": True, "task_id": task_id, "message": "研究已启动"})


@router.post("/{stock_code}/run-research-step")
def run_research_step(stock_code: str, background_tasks: BackgroundTasks,
                      request: Request = None):
    """单独重跑某个板块（后台执行）

    Body JSON: {"step": "valuation", "report_id": 43}
    """
    import asyncio

    async def _get_body():
        if request:
            try:
                return await request.json()
            except Exception:
                return {}
        return {}

    # FastAPI 同步路由中无法直接 await，改用 BackgroundTasks 传参
    # 通过 query params 接收
    return JSONResponse({"error": "请使用 POST JSON body"}, status_code=400)


@router.post("/{stock_code}/api/run-research-step")
async def api_run_research_step(request: Request, stock_code: str,
                                 background_tasks: BackgroundTasks):
    """单独重跑某个板块（后台执行）

    Body JSON: {"step": "valuation", "report_id": 43}
    step 可选值: business_model, value_chain, financial, valuation, sector_heat, research_data
    """
    from research.deep_researcher import deep_research_stock, ALL_STEPS

    body = await request.json()
    step = body.get("step", "")
    report_id = body.get("report_id")

    if step not in ALL_STEPS:
        return JSONResponse(
            {"error": f"无效步骤: {step}，可选: {ALL_STEPS}"}, status_code=400
        )

    info = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=?", [stock_code])
    stock_name = info[0]["stock_name"] if info else stock_code
    task_id = _create_research_task(stock_code, stock_name)

    def _run():
        try:
            result = deep_research_stock(
                stock_code,
                steps=[step],
                existing_report_id=report_id,
                progress_callback=_make_progress_cb(task_id),
                step_callback=_make_step_cb(task_id),
            )
            rid = result.get("research_id")
            if result.get("error"):
                _update_research_task(task_id, status="error", progress=100,
                                     message=f"失败: {result['error']}",
                                     finished_at=time.time(), research_id=rid)
            else:
                _update_research_task(task_id, status="done", progress=100,
                                     message=f"{step} 重跑完成",
                                     finished_at=time.time(), research_id=rid)
        except Exception as e:
            logger.error(f"单步重跑失败 {stock_code}/{step}: {e}")
            _update_research_task(task_id, status="error",
                                 message=f"异常: {e}", finished_at=time.time())

    background_tasks.add_task(_run)
    return JSONResponse({"ok": True, "task_id": task_id,
                         "message": f"正在重跑 {step} 板块"})



    """触发深度研究（后台执行）"""
    from research.deep_researcher import deep_research_stock

    info = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=?", [stock_code])
    stock_name = info[0]["stock_name"] if info else stock_code
    task_id = _create_research_task(stock_code, stock_name)

    def _run():
        try:
            result = deep_research_stock(
                stock_code,
                progress_callback=_make_progress_cb(task_id),
                step_callback=_make_step_cb(task_id),
            )
            rid = result.get("research_id")
            if result.get("error"):
                _update_research_task(task_id, status="error", progress=100,
                                     message=f"失败: {result['error']}",
                                     finished_at=time.time(), research_id=rid)
            else:
                _update_research_task(task_id, status="done", progress=100,
                                     message="研究完成", finished_at=time.time(),
                                     research_id=rid)
        except Exception as e:
            logger.error(f"深度研究失败: {e}")
            _update_research_task(task_id, status="error",
                                 message=f"异常: {e}", finished_at=time.time())

    background_tasks.add_task(_run)
    return JSONResponse({"ok": True, "task_id": task_id, "message": "深度研究已启动"})


@router.post("/{stock_code}/run-followup")
async def run_followup(request: Request, stock_code: str,
                       background_tasks: BackgroundTasks):
    """触发追踪研究 — 按标签搜索各信息源半年数据"""
    form = await request.form()
    context = form.get("context", "")
    constraints = form.get("constraints", "")
    selected_sources = form.getlist("sources")

    tags = _get_all_stock_tags(stock_code)

    from research.deep_researcher import deep_research_stock

    info = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=?", [stock_code])
    stock_name = info[0]["stock_name"] if info else stock_code
    task_id = _create_research_task(stock_code, stock_name)

    def _run():
        try:
            _update_research_task(task_id, message="正在收集标签关联数据...", progress=3)
            # 收集标签搜索的完整上下文
            extra_context = _collect_followup_context(
                stock_code, tags, selected_sources, months=6
            )
            if context:
                extra_context += f"\n\n=== 用户研究指令 ===\n{context}"
            if constraints:
                extra_context += f"\n\n=== 约束条件 ===\n{constraints}"

            # 调用深度研究，附加额外上下文
            deep_research_stock(stock_code, progress_callback=_make_progress_cb(task_id))
            _update_research_task(task_id, status="done", progress=100,
                                 message="追踪研究完成", finished_at=time.time())
            logger.info(f"追踪研究完成: {stock_code}, 标签数: {len(tags)}, 信息源: {selected_sources}")
        except Exception as e:
            logger.error(f"追踪研究失败: {e}")
            _update_research_task(task_id, status="error",
                                 message=f"失败: {e}", finished_at=time.time())

    background_tasks.add_task(_run)
    return JSONResponse({"ok": True, "task_id": task_id, "message": "追踪研究已启动"})


@router.post("/add")
async def add_stock(request: Request):
    """添加股票到跟踪列表（支持代码或名称搜索）"""
    form = await request.form()
    raw_input = form.get("stock_code", "").strip()
    name = form.get("stock_name", "").strip()
    wtype = form.get("watch_type", "interested")

    code = raw_input
    # 如果输入包含中文，按名称搜索 stock_info 查找对应代码
    if raw_input and any('\u4e00' <= c <= '\u9fff' for c in raw_input):
        rows = execute_query(
            "SELECT stock_code, stock_name FROM stock_info WHERE stock_name LIKE ? LIMIT 5",
            [f"%{raw_input}%"],
        )
        if rows:
            code = rows[0]["stock_code"]
            name = name or rows[0]["stock_name"]

    if code:
        add_to_watchlist(code, name or None, wtype)
    return RedirectResponse(url=f"/stock/{code}" if code else "/stock", status_code=303)


@router.post("/{stock_code}/remove")
def remove_stock(stock_code: str):
    """从跟踪列表移除"""
    remove_from_watchlist(stock_code)
    return RedirectResponse(url="/stock", status_code=303)


@router.post("/{stock_code}/update-type")
async def update_type(request: Request, stock_code: str):
    """更新标记类型"""
    form = await request.form()
    wtype = form.get("watch_type", "interested")
    update_watch_type(stock_code, wtype)
    return RedirectResponse(url=f"/stock/{stock_code}", status_code=303)


# ==================== 自动采集 ====================

def _auto_fetch_stock_data(stock_code: str) -> dict:
    """自动从 AKShare 采集个股 K 线 + 资金流 + 基本信息"""
    import akshare as ak
    from utils.db_utils import get_db
    from datetime import timedelta

    result = {"kline": 0, "capital": 0, "info": False, "errors": []}
    start_date = (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")

    # 判断市场
    is_hk = stock_code.startswith("HK.") or (len(stock_code) == 5 and stock_code.isdigit())
    clean_code = stock_code.replace("HK.", "")

    # 1. K 线
    try:
        if is_hk:
            df = ak.stock_hk_hist(symbol=clean_code, period="daily",
                                  start_date=start_date, adjust="qfq")
        else:
            df = ak.stock_zh_a_hist(symbol=clean_code, period="daily",
                                    start_date=start_date, adjust="qfq")
        if df is not None and not df.empty:
            with get_db() as conn:
                for _, row in df.iterrows():
                    td = str(row.get("日期", ""))[:10]
                    conn.execute(
                        """REPLACE INTO stock_daily
                           (stock_code, trade_date, open, high, low, close,
                            volume, amount, turnover_rate, amplitude,
                            change_pct, change_amount)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [clean_code, td, row.get("开盘"), row.get("最高"),
                         row.get("最低"), row.get("收盘"), row.get("成交量"),
                         row.get("成交额"), row.get("换手率"), row.get("振幅"),
                         row.get("涨跌幅"), row.get("涨跌额")],
                    )
                    result["kline"] += 1
    except Exception as e:
        result["errors"].append(f"K线: {e}")

    # 2. 资金流（仅 A 股）
    if not is_hk:
        try:
            time.sleep(0.5)
            market = "sh" if clean_code.startswith("6") else "sz"
            df2 = ak.stock_individual_fund_flow(stock=clean_code, market=market)
            if df2 is not None and not df2.empty:
                with get_db() as conn:
                    for _, row in df2.iterrows():
                        td = str(row.get("日期", ""))[:10]
                        conn.execute(
                            """REPLACE INTO capital_flow
                               (stock_code, trade_date, main_net_inflow,
                                super_large_net, large_net, medium_net, small_net)
                               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                            [clean_code, td,
                             row.get("主力净流入-净额"), row.get("超大单净流入-净额"),
                             row.get("大单净流入-净额"), row.get("中单净流入-净额"),
                             row.get("小单净流入-净额")],
                        )
                        result["capital"] += 1
        except Exception as e:
            result["errors"].append(f"资金流: {e}")

    # 3. 基本信息
    if not is_hk:
        try:
            time.sleep(0.5)
            df3 = ak.stock_individual_info_em(symbol=clean_code)
            info = dict(zip(df3["item"], df3["value"]))
            with get_db() as conn:
                conn.execute(
                    """UPDATE stock_info SET industry_l1=%s, market_cap=%s,
                              total_shares=%s, float_shares=%s
                       WHERE stock_code=%s""",
                    [info.get("行业"), info.get("总市值"),
                     info.get("总股本"), info.get("流通股"), clean_code],
                )
            result["info"] = True
        except Exception:
            pass  # 非关键

    logger.info(f"自动采集 {stock_code}: K线{result['kline']} 资金流{result['capital']}")
    return result


@router.get("/{stock_code}/api/auto-fetch")
def api_auto_fetch(stock_code: str):
    """自动采集个股数据（前端检测无数据时调用）"""
    # 先检查是否已有数据，避免重复采集
    rows = execute_query(
        "SELECT COUNT(*) as cnt FROM stock_daily WHERE stock_code=?",
        [stock_code],
    )
    if rows and rows[0]["cnt"] > 0:
        return JSONResponse({"status": "exists", "kline": rows[0]["cnt"]})

    result = _auto_fetch_stock_data(stock_code)
    return JSONResponse({
        "status": "ok" if result["kline"] > 0 else "no_data",
        **result,
    })


@router.post("/{stock_code}/api/toggle-watch")
def api_toggle_watch(stock_code: str):
    """切换关注状态"""
    wl = execute_query("SELECT id FROM watchlist WHERE stock_code=?", [stock_code])
    if wl:
        remove_from_watchlist(stock_code)
        return JSONResponse({"watched": False})
    else:
        info = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=?", [stock_code])
        name = info[0]["stock_name"] if info else ""
        add_to_watchlist(stock_code, name, "interested")
        return JSONResponse({"watched": True, "type": "interested"})


_tag_gen_tasks: dict = {}


@router.post("/{stock_code}/api/generate-tags")
def api_generate_tags(stock_code: str, background_tasks: BackgroundTasks):
    """触发 L1+L2+L3 全量标签生成（后台任务）"""
    task_id = f"tag_{stock_code}_{int(time.time())}"
    _tag_gen_tasks[task_id] = {"status": "running", "phase": "L1", "result": None}

    def _run():
        task = _tag_gen_tasks[task_id]
        try:
            # L1 量化
            task["phase"] = "L1"
            from tagging.l1_quant_engine import run_l1_for_stock
            run_l1_for_stock(stock_code)

            # L2 AI 轻量
            task["phase"] = "L2"
            from tagging.l2_ai_engine import run_l2_for_stock
            run_l2_for_stock(stock_code)

            # L3 AI 深度
            task["phase"] = "L3"
            from tagging.l3_deep_engine import run_l3_for_stock
            run_l3_for_stock(stock_code)

            # 读取最终结果
            task["phase"] = "done"
            from tagging.stock_tag_service import get_stock_tags
            result = get_stock_tags(stock_code)
            core = (
                [{"name": t, "type": "industry"} for t in result.industry_tags] +
                [{"name": t, "type": "theme"} for t in result.theme_tags]
            )
            more = [
                {"name": t.name, "type": "selection", "category": t.category, "layer": t.layer}
                for t in result.selection_tags
            ]
            task["status"] = "done"
            task["result"] = {"core": core, "more": more}
        except Exception as e:
            logger.warning(f"generate-tags 失败 {stock_code}: {e}")
            task["status"] = "failed"
            task["result"] = {"core": [], "more": [], "error": str(e)}

    background_tasks.add_task(_run)
    return JSONResponse({"task_id": task_id})


@router.get("/{stock_code}/api/generate-tags/{task_id}")
def api_generate_tags_status(stock_code: str, task_id: str):
    """查询标签生成任务状态"""
    task = _tag_gen_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return JSONResponse(task)


@router.post("/{stock_code}/api/save-tags")
async def api_save_tags(request: Request, stock_code: str):
    """保存标签到 watchlist.related_tags"""
    body = await request.json()
    tags = body.get("tags", [])
    wl = execute_query("SELECT id FROM watchlist WHERE stock_code=?", [stock_code])
    if wl:
        execute_insert(
            "UPDATE watchlist SET related_tags=? WHERE stock_code=?",
            [json.dumps(tags, ensure_ascii=False), stock_code],
        )
    else:
        info = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=?", [stock_code])
        name = info[0]["stock_name"] if info else ""
        add_to_watchlist(stock_code, name, "interested")
        execute_insert(
            "UPDATE watchlist SET related_tags=? WHERE stock_code=?",
            [json.dumps(tags, ensure_ascii=False), stock_code],
        )
    return JSONResponse({"ok": True, "count": len(tags)})

# ==================== K线阶段分析 API ====================

_chart_analysis_tasks: dict = {}
_ca_tasks_lock = threading.Lock()


def _create_ca_task(stock_code: str) -> str:
    task_id = uuid.uuid4().hex[:8]
    with _ca_tasks_lock:
        _chart_analysis_tasks[task_id] = {
            "id": task_id, "stock_code": stock_code,
            "status": "running", "result": None,
            "created_at": time.time(),
        }
    return task_id


@router.post("/{stock_code}/api/chart-analysis")
async def api_start_chart_analysis(stock_code: str, background_tasks: BackgroundTasks, user: TokenData = Depends(get_current_user)):
    """触发K线阶段分析（后台任务）"""
    user_id = user.user_id

    # 检查K线分析权限
    can_run, msg = check_quota(user_id, 'chart_analysis')
    if not can_run:
        return JSONResponse({"ok": False, "error": msg}, status_code=403)

    # 消耗配额
    consume_quota(user_id, 'chart_analysis', 1)

    task_id = _create_ca_task(stock_code)

    def _run():
        try:
            from analysis.kline_analyzer import run_full_analysis
            result = run_full_analysis(stock_code, days=180)
            with _ca_tasks_lock:
                if task_id in _chart_analysis_tasks:
                    _chart_analysis_tasks[task_id]["status"] = "done"
                    _chart_analysis_tasks[task_id]["result"] = result
        except Exception as e:
            logger.error(f"K线阶段分析失败 {stock_code}: {e}")
            with _ca_tasks_lock:
                if task_id in _chart_analysis_tasks:
                    _chart_analysis_tasks[task_id]["status"] = "error"
                    _chart_analysis_tasks[task_id]["result"] = {"ok": False, "error": str(e)}

    background_tasks.add_task(_run)
    return JSONResponse({"ok": True, "task_id": task_id})


@router.get("/{stock_code}/api/chart-analysis/latest")
def api_get_latest_chart_analysis(stock_code: str):
    """获取最新阶段分析结果"""
    try:
        from analysis.kline_analyzer import get_latest_analysis
        data = get_latest_analysis(stock_code)
        if data:
            # 序列化 date/datetime 为字符串
            payload = {
                "ok": True,
                "analysis_date": str(data.get("analysis_date") or ""),
                "created_at": str(data.get("created_at") or ""),
                "stages": data.get("stages") or [],
                "current_stage": data.get("current_stage") or {},
                "predictions": data.get("predictions") or [],
            }
            return JSONResponse(payload)
        return JSONResponse({"ok": False, "message": "暂无分析数据"})
    except Exception as e:
        logger.error(f"获取阶段分析失败 {stock_code}: {e}")
        return JSONResponse({"ok": False, "error": str(e)})


@router.get("/{stock_code}/api/chart-analysis/{task_id}")
def api_get_chart_analysis_task(stock_code: str, task_id: str):
    """查询阶段分析任务状态"""
    with _ca_tasks_lock:
        task = _chart_analysis_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return JSONResponse(task)


# ==================== 个股报告 Chatbot API ====================

@router.get("/{stock_code}/api/chat/history")
def api_get_chat_history(stock_code: str):
    """获取个股对话历史（最近30轮）"""
    rows = execute_query(
        """SELECT id, role, content, metadata_json, created_at
           FROM stock_chat_messages WHERE stock_code=%s
           ORDER BY created_at DESC LIMIT 60""",
        [stock_code],
    )
    msgs = list(reversed([dict(r) for r in (rows or [])]))
    return JSONResponse({"ok": True, "messages": msgs})


@router.delete("/{stock_code}/api/chat/history")
def api_clear_chat_history(stock_code: str):
    """清空个股对话历史"""
    execute_insert("DELETE FROM stock_chat_messages WHERE stock_code=%s", [stock_code])
    return JSONResponse({"ok": True})


@router.post("/{stock_code}/api/chat")
async def api_stock_chat(request: Request, stock_code: str):
    """个股报告Chatbot — SSE流式返回"""
    from fastapi.responses import StreamingResponse

    body = await request.json()
    user_message = body.get("message", "").strip()
    drag_context = body.get("drag_context", "")
    current_tab  = body.get("current_tab", "")

    if not user_message:
        return JSONResponse({"error": "消息不能为空"}, status_code=400)

    # 获取股票信息
    info = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=%s", [stock_code])
    stock_name = info[0]["stock_name"] if info else stock_code

    # 获取历史（最近30轮=60条）
    history_rows = execute_query(
        """SELECT role, content FROM stock_chat_messages WHERE stock_code=%s
           ORDER BY created_at DESC LIMIT 60""",
        [stock_code],
    )
    history = [{"role": r["role"], "content": r["content"]} for r in reversed(history_rows or [])]

    # 从 DB 读取当前报告章节数据，注入 system prompt 防幻觉
    _TAB_TO_SECTION = {
        "bm": "business_model", "vc": "value_chain", "fin": "financial",
        "val": "valuation", "sh": "sector_heat", "rd": "research_data",
        "pi": "policy_impact",
    }
    report_section_data = ""
    if current_tab:
        section_key = _TAB_TO_SECTION.get(current_tab, "")
        if section_key:
            rpt_rows = execute_query(
                "SELECT report_json FROM deep_research WHERE research_type='stock' AND target=%s ORDER BY created_at DESC LIMIT 1",
                [stock_code],
            )
            if rpt_rows and rpt_rows[0].get("report_json"):
                try:
                    rpt_full = json.loads(rpt_rows[0]["report_json"])
                    rpt_body = rpt_full.get("report", rpt_full) if isinstance(rpt_full, dict) else {}
                    section_data = rpt_body.get(section_key, {})
                    if section_data:
                        report_section_data = json.dumps(section_data, ensure_ascii=False)[:4000]
                except (json.JSONDecodeError, TypeError):
                    pass

    # 构建extra_context
    extra_context = f"当前分析股票: {stock_code} {stock_name}"
    if current_tab:
        extra_context += f"\n当前查看的报告章节: {current_tab}"
    if report_section_data:
        extra_context += f'\n\n=== 当前报告数据（必须基于此回答，不得编造）===\n{report_section_data}\n如果用户问的内容超出以上数据范围，明确告知当前报告中没有这方面的数据。'
    if drag_context:
        extra_context += f"\n用户拖入的讨论内容:\n{drag_context}"

    # 保存用户消息
    execute_insert(
        "INSERT INTO stock_chat_messages (stock_code, role, content) VALUES (%s, %s, %s)",
        [stock_code, "user", user_message],
    )

    # 清理超过30轮的历史
    _trim_chat_history(stock_code)

    def _stream():
        from agent.executor import run_agent_stream
        full_response = []
        try:
            for chunk in run_agent_stream(user_message, history=history, extra_context=extra_context):
                full_response.append(chunk)
                yield f"data: {json.dumps({'chunk': chunk}, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)}, ensure_ascii=False)}\n\n"
            return

        # 保存助手回复
        assistant_reply = "".join(full_response)
        if assistant_reply:
            execute_insert(
                "INSERT INTO stock_chat_messages (stock_code, role, content) VALUES (%s, %s, %s)",
                [stock_code, "assistant", assistant_reply],
            )
            _trim_chat_history(stock_code)
        yield "data: [DONE]\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")


def _trim_chat_history(stock_code: str):
    """保留最近30轮（60条）对话"""
    rows = execute_query(
        "SELECT id FROM stock_chat_messages WHERE stock_code=%s ORDER BY created_at DESC LIMIT 10000",
        [stock_code],
    )
    if rows and len(rows) > 60:
        ids_to_delete = [r["id"] for r in rows[60:]]
        if ids_to_delete:
            placeholders = ",".join(["%s"] * len(ids_to_delete))
            execute_insert(
                f"DELETE FROM stock_chat_messages WHERE id IN ({placeholders})",
                ids_to_delete,
            )


# ==================== KG 新闻关联 API ====================

@router.get("/{stock_code}/api/news-detail")
def api_news_detail(stock_code: str, q: str = ""):
    """根据新闻标题关键词检索展开详情，供产业链面板新闻展开用"""
    if not q:
        return JSONResponse({"ok": False, "detail": None, "error": "缺少查询关键词"})
    try:
        from research.rag_context import search_news_detail
        detail = search_news_detail(q, stock_code=stock_code)
        if detail:
            return JSONResponse({"ok": True, "detail": detail})
        return JSONResponse({"ok": False, "detail": None, "error": "未找到相关内容"})
    except Exception as e:
        logger.error(f"news-detail 检索失败: {e}")
        return JSONResponse({"ok": False, "detail": None, "error": str(e)})


@router.get("/{stock_code}/api/kg-related-news")
def api_kg_related_news(stock_code: str, entity_name: str = ""):
    """根据实体名搜索最新关联新闻（D3 tooltip 用）"""
    if not entity_name:
        return JSONResponse({"ok": False, "news": []})

    like = f"%{entity_name}%"
    # 优先从 content_summaries 搜（新管线）
    rows = execute_query(
        """SELECT cs.summary AS title, cs.summary, cs.created_at AS date
           FROM content_summaries cs
           WHERE cs.summary LIKE %s
           ORDER BY cs.created_at DESC LIMIT 3""",
        [like],
    )
    # 降级到 cleaned_items
    if not rows:
        rows = execute_query(
            """SELECT ci.summary AS title, ci.summary, ci.cleaned_at AS date
               FROM cleaned_items ci
               WHERE ci.summary LIKE %s OR ci.tags_json LIKE %s
               ORDER BY ci.cleaned_at DESC LIMIT 3""",
            [like, like],
        )
    news = [
        {
            "title": (r.get("title") or "")[:60],
            "summary": (r.get("summary") or "")[:120],
            "date": str(r.get("date") or ""),
        }
        for r in (rows or [])
    ]
    return JSONResponse({"ok": True, "news": news})


@router.get("/{stock_code}/api/kg-graph")
def api_kg_graph(stock_code: str):
    """返回与该股票相关的 KG 节点和边（D3 力导向图用）"""
    # 先找股票名称
    info = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=%s", [stock_code])
    stock_name = info[0]["stock_name"] if info else ""

    # 找与该股票直接相关的实体（通过关系表）
    entity_rows = execute_query(
        """SELECT DISTINCT e.id, e.entity_name, e.entity_type
           FROM kg_entities e
           JOIN kg_relationships r ON e.id = r.source_entity_id OR e.id = r.target_entity_id
           WHERE r.source_entity_id IN (
               SELECT id FROM kg_entities WHERE entity_name LIKE %s OR entity_name LIKE %s
           ) OR r.target_entity_id IN (
               SELECT id FROM kg_entities WHERE entity_name LIKE %s OR entity_name LIKE %s
           )
           LIMIT 30""",
        [f"%{stock_code}%", f"%{stock_name}%", f"%{stock_code}%", f"%{stock_name}%"],
    )

    if not entity_rows:
        return JSONResponse({"nodes": [], "links": []})

    node_ids = {r["id"] for r in entity_rows}
    id_set_ph = ",".join(["%s"] * len(node_ids))
    rel_rows = execute_query(
        f"""SELECT source_entity_id, target_entity_id, relation_type
            FROM kg_relationships
            WHERE source_entity_id IN ({id_set_ph}) AND target_entity_id IN ({id_set_ph})
            LIMIT 60""",
        list(node_ids) * 2,
    )

    nodes = [{"id": r["id"], "name": r["entity_name"], "type": r["entity_type"]} for r in entity_rows]
    links = [{"source": r["source_entity_id"], "target": r["target_entity_id"], "relation": r["relation_type"] or ""} for r in (rel_rows or [])]
    return JSONResponse({"nodes": nodes, "links": links})


@router.get("/{stock_code}/api/chips")
def api_chips(stock_code: str, date: str = None):
    """获取筹码分布数据，返回 price/percent 列表及 90%/70% 成本线。

    Query params:
        date: YYYY-MM-DD，不传则取最新一期
    """
    from utils.db_utils import get_cyq_chips
    chips = get_cyq_chips(stock_code, date)
    if not chips:
        return JSONResponse({"date": date, "chips": [], "cost_90": None, "cost_70": None})

    # 计算累计持仓成本分位（90% / 70%）
    total_pct = sum(c["percent"] for c in chips)
    if total_pct <= 0:
        return JSONResponse({"date": date, "chips": chips, "cost_90": None, "cost_70": None})

    cumulative = 0.0
    cost_90 = cost_70 = None
    for c in chips:
        cumulative += c["percent"]
        frac = cumulative / total_pct
        if cost_70 is None and frac >= 0.70:
            cost_70 = c["price"]
        if cost_90 is None and frac >= 0.90:
            cost_90 = c["price"]
            break

    return JSONResponse({
        "date": date,
        "chips": chips,
        "cost_90": cost_90,
        "cost_70": cost_70,
    })

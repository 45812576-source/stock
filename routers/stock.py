"""个股研究路由 — 列表/详情/报告/追踪研究"""
import json
import logging
import time
import threading
import uuid
from datetime import datetime
from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from utils.db_utils import execute_query, execute_insert
from tracking.watchlist_manager import (
    get_watchlist, get_stock_today_news, get_open_positions,
    get_position_summary, add_to_watchlist, remove_from_watchlist,
    update_watch_type,
)
from research.report_generator import get_research_report, list_research_records

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/stock", tags=["stock"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# ==================== 研究任务进度追踪 ====================

_research_tasks: dict = {}
_tasks_lock = threading.Lock()

_PROGRESS_MAP = {
    "正在检查数据充分性": 5, "正在获取个股数据": 10,
    "[1/5]": 20, "[2/5]": 35, "[3/5]": 50,
    "[4/5]": 65, "[5/5]": 80,
    "正在综合": 90, "正在保存": 95,
}

def _create_research_task(stock_code, stock_name=""):
    task_id = uuid.uuid4().hex[:8]
    with _tasks_lock:
        _research_tasks[task_id] = {
            "id": task_id, "stock_code": stock_code,
            "stock_name": stock_name, "status": "running",
            "progress": 0, "message": "准备中...",
            "created_at": time.time(), "finished_at": None,
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


# ==================== 辅助函数 ====================

def _get_sidebar_stocks():
    """获取侧边栏股票列表：感兴趣 + 已持仓"""
    interested = get_watchlist("interested") or []
    holding = get_watchlist("holding") or []
    return {"interested": interested, "holding": holding}


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
    """获取最新行情"""
    rows = execute_query(
        """SELECT close, change_pct, volume, amount, trade_date
           FROM stock_daily WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT 1""",
        [stock_code],
    )
    return rows[0] if rows else None


def _get_kline_data(stock_code, days=120):
    """获取K线数据"""
    rows = execute_query(
        """SELECT trade_date, open, high, low, close, volume
           FROM stock_daily WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT ?""",
        [stock_code, days],
    )
    return list(reversed(rows)) if rows else []


def _get_knowledge_tags(stock_code):
    """获取个股关联的知识标签"""
    rows = execute_query(
        """SELECT DISTINCT e.entity_name, e.entity_type
           FROM kg_entities e
           JOIN kg_relationships r ON e.id=r.source_entity_id OR e.id=r.target_entity_id
           WHERE e.entity_name LIKE ? OR e.entity_type='stock'
           LIMIT 20""",
        [f"%{stock_code}%"],
    )
    if not rows:
        # fallback: 从 item_companies 获取关联标签
        rows2 = execute_query(
            """SELECT DISTINCT ci.tags_json
               FROM item_companies ic JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
               WHERE ic.stock_code=?
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
        return [{"name": t, "type": "tag"} for t in list(tags)[:15]]
    return [{"name": r["entity_name"], "type": r["entity_type"]} for r in rows]


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
            "financial": r.get("financial_score", 0) or 0,
            "valuation": r.get("valuation_score", 0) or 0,
            "technical": r.get("technical_score", 0) or 0,
            "sentiment": r.get("sentiment_score", 0) or 0,
            "catalyst": r.get("catalyst_score", 0) or 0,
            "risk": r.get("risk_score", 0) or 0,
            "overall": r.get("overall_score", 0) or 0,
        },
        "recommendation": r.get("recommendation", ""),
        "report": report.get("report", report),
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


def _get_capital_flow(stock_code, limit=10):
    """获取资金流向"""
    return execute_query(
        """SELECT trade_date, main_net_inflow, super_large_net, large_net
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
                    f"{n.get('summary','')} ({n.get('cleaned_at','')[:10]})"
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
async def stock_list(request: Request):
    """个股研究首页 — 重定向到第一个持仓/关注股票，或显示空状态"""
    sidebar = _get_sidebar_stocks()
    first = None
    if sidebar["holding"]:
        first = sidebar["holding"][0]["stock_code"]
    elif sidebar["interested"]:
        first = sidebar["interested"][0]["stock_code"]
    if first:
        return RedirectResponse(url=f"/stock/{first}", status_code=302)
    return templates.TemplateResponse("stock_detail.html", {
        "request": request,
        "active_page": "stock",
        "sidebar": sidebar,
        "stock": None,
        "price": None,
        "kline": [],
        "tags": [],
        "research": None,
        "history": [],
        "news": [],
        "capital": [],
    })


@router.get("/{stock_code}", response_class=HTMLResponse)
async def stock_detail(request: Request, stock_code: str):
    """个股详情页"""
    sidebar = _get_sidebar_stocks()
    stock = _get_stock_info(stock_code)
    price = _get_latest_price(stock_code)
    kline = _get_kline_data(stock_code)
    tags = _get_knowledge_tags(stock_code)
    research = _get_latest_research(stock_code)
    history = _get_research_history(stock_code)
    news = _get_related_news(stock_code)
    capital = _get_capital_flow(stock_code, 5)

    return templates.TemplateResponse("stock_detail.html", {
        "request": request,
        "active_page": "stock",
        "sidebar": sidebar,
        "stock": stock,
        "stock_code": stock_code,
        "price": price,
        "kline": kline,
        "kline_json": json.dumps(kline, ensure_ascii=False, default=str),
        "tags": tags,
        "research": research,
        "history": history,
        "news": news,
        "capital": capital,
    })


@router.get("/{stock_code}/report/{report_id}", response_class=HTMLResponse)
async def stock_report(request: Request, stock_code: str, report_id: int):
    """深度研究报告页"""
    sidebar = _get_sidebar_stocks()
    stock = _get_stock_info(stock_code)
    report = get_research_report(report_id)
    news = _get_related_news(stock_code, 8)

    if not report:
        return RedirectResponse(url=f"/stock/{stock_code}", status_code=302)

    return templates.TemplateResponse("stock_report.html", {
        "request": request,
        "active_page": "stock",
        "sidebar": sidebar,
        "stock": stock,
        "stock_code": stock_code,
        "report": report,
        "news": news,
    })


@router.get("/{stock_code}/followup", response_class=HTMLResponse)
async def stock_followup(request: Request, stock_code: str):
    """追踪研究配置页"""
    sidebar = _get_sidebar_stocks()
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
        "sidebar": sidebar,
        "stock": stock,
        "stock_code": stock_code,
        "research": research,
        "tags": tags,
        "sources": sources,
    })


# ==================== API 操作 ====================

@router.get("/api/research-tasks")
async def get_research_tasks():
    """获取所有研究任务状态"""
    now = time.time()
    with _tasks_lock:
        # 清理1小时前已完成的任务
        stale = [k for k, v in _research_tasks.items()
                 if v["finished_at"] and now - v["finished_at"] > 3600]
        for k in stale:
            del _research_tasks[k]
        return list(_research_tasks.values())

@router.post("/{stock_code}/run-research")
async def run_research(stock_code: str, background_tasks: BackgroundTasks):
    """触发深度研究（后台执行）"""
    from research.deep_researcher import deep_research_stock

    info = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=?", [stock_code])
    stock_name = info[0]["stock_name"] if info else stock_code
    task_id = _create_research_task(stock_code, stock_name)

    # 记录开始
    research_id = execute_insert(
        """INSERT INTO deep_research
           (research_type, target, research_date, overall_score, recommendation)
           VALUES ('stock', ?, date('now'), 0, '研究中...')""",
        [stock_code],
    )

    def _run():
        try:
            result = deep_research_stock(stock_code, progress_callback=_make_progress_cb(task_id))
            _update_research_task(task_id, status="done", progress=100,
                                 message="研究完成", finished_at=time.time())
            if result.get("error"):
                execute_insert(
                    "UPDATE deep_research SET recommendation=? WHERE id=?",
                    [f"失败: {result['error']}", research_id],
                )
                _update_research_task(task_id, status="error",
                                     message=f"失败: {result['error']}", finished_at=time.time())
        except Exception as e:
            logger.error(f"深度研究失败: {e}")
            execute_insert(
                "UPDATE deep_research SET recommendation=? WHERE id=?",
                [f"异常: {e}", research_id],
            )
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
    """添加股票到跟踪列表"""
    form = await request.form()
    code = form.get("stock_code", "").strip()
    name = form.get("stock_name", "").strip()
    wtype = form.get("watch_type", "interested")
    if code:
        add_to_watchlist(code, name or None, wtype)
    return RedirectResponse(url=f"/stock/{code}" if code else "/stock", status_code=303)


@router.post("/{stock_code}/remove")
async def remove_stock(stock_code: str):
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

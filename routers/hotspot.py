"""热点研究 — FastAPI 路由"""
import json
from datetime import datetime
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from utils.db_utils import execute_query
from hotspot.tag_recommender import (
    get_top_tags, get_tag_dashboard_distribution,
    recommend_tag_groups, save_tag_group, get_saved_groups, delete_tag_group,
    get_group_related_news,
)
from hotspot.tag_group_analyzer import analyze_tag_group
from hotspot.tag_group_research import research_tag_group, get_group_research_history
from knowledge_graph.kg_manager import add_entity, update_entity, get_entity_by_id

router = APIRouter(prefix="/hotspot", tags=["hotspot"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


# ── 数据查询 ──────────────────────────────────────────────────

def get_top_hotspots(days: int = 7, limit: int = 10) -> list:
    try:
        tags = get_top_tags(days, limit)
        for i, t in enumerate(tags):
            t["rank"] = i + 1
            t["momentum"] = round(t.get("total_freq", 0) * 10 / max(days, 1), 1)
        return tags
    except Exception:
        return []


def get_frequency_chart_data(days: int = 30) -> dict:
    """多维综合热度：新闻提及×重要性加权 + 资金流入标准化"""
    try:
        top10 = get_top_tags(days, 10)
        series = {}
        # 收集所有标签的资金流数据，用于标准化
        all_capital = []

        for tag in top10:
            name = tag["tag_name"]

            # 维度1: 新闻提及 × 重要性加权
            news_rows = execute_query(
                """SELECT date(ci.cleaned_at) as day,
                          COUNT(*) as mention_count,
                          SUM(ci.importance) as weighted_mentions
                   FROM cleaned_items ci
                   WHERE ci.tags_json LIKE ? AND ci.cleaned_at >= date('now', ?)
                   GROUP BY date(ci.cleaned_at) ORDER BY day""",
                [f"%{name}%", f"-{days} days"],
            )

            # 维度2: 关联行业资金净流入
            capital_rows = execute_query(
                """SELECT trade_date as day, SUM(net_inflow) as net_flow
                   FROM industry_capital_flow
                   WHERE industry_name LIKE ? AND trade_date >= date('now', ?)
                   GROUP BY trade_date""",
                [f"%{name}%", f"-{days} days"],
            )
            capital_map = {r["day"]: r["net_flow"] or 0 for r in capital_rows}
            all_capital.extend(abs(v) for v in capital_map.values() if v)

            # 合并日期
            day_data = {}
            for r in news_rows:
                d = r["day"]
                day_data[d] = {"news_heat": r["weighted_mentions"] or 0, "capital": capital_map.get(d, 0)}
            for d, v in capital_map.items():
                if d not in day_data:
                    day_data[d] = {"news_heat": 0, "capital": v}

            series[name] = {"days": day_data}

        # 标准化资金流：映射到 0-max_news_heat 的范围
        max_capital = max(all_capital) if all_capital else 1
        max_news = max(
            (d["news_heat"] for s in series.values() for d in s["days"].values()),
            default=1,
        ) or 1

        result = {}
        for name, data in series.items():
            points = []
            for day in sorted(data["days"].keys()):
                d = data["days"][day]
                capital_normalized = abs(d["capital"]) / max_capital * max_news * 0.3 if max_capital else 0
                heat = d["news_heat"] + capital_normalized
                points.append({"date": day, "heat": round(heat, 1), "news": d["news_heat"], "capital": round(d["capital"], 0)})
            result[name] = points

        return result
    except Exception:
        return {}


def get_tag_clusters(days: int = 7) -> list:
    """获取已保存的标签组 + 颜色分配"""
    colors = ["#135bec", "#10b981", "#f97316", "#a855f7", "#ef4444", "#06b6d4"]
    try:
        groups = get_saved_groups()
        for i, g in enumerate(groups):
            g["tags"] = json.loads(g.get("tags_json") or "[]")
            g["color"] = colors[i % len(colors)]
            # 最近研究摘要
            history = get_group_research_history(g["id"], 1)
            g["has_research"] = bool(history)
            if history:
                g["last_research_date"] = str(history[0].get("created_at", ""))[:10]
        return groups
    except Exception:
        return []


def get_research_data(group_id: int) -> dict:
    """获取标签组的完整研究数据"""
    try:
        groups = get_saved_groups()
        group = next((g for g in groups if g["id"] == group_id), None)
        if not group:
            return None

        tags = json.loads(group.get("tags_json") or "[]")
        group["tags"] = tags

        # 加载最近研究结果
        history = get_group_research_history(group_id, 1)
        research = {}
        if history:
            h = history[0]
            research = {
                "macro_report": h.get("macro_report"),
                "industry_report": h.get("industry_report"),
                "news": json.loads(h["news_summary_json"]) if h.get("news_summary_json") else [],
                "sector_heat": json.loads(h["sector_heat_json"]) if h.get("sector_heat_json") else [],
                "top10_stocks": json.loads(h["top10_stocks_json"]) if h.get("top10_stocks_json") else [],
                "research_date": str(h.get("created_at", ""))[:10],
            }

        return {"group": group, "research": research}
    except Exception:
        return None


# ── 页面路由 ──────────────────────────────────────────────────

def _calc_days(start: str = None, end: str = None, days: int = 7) -> tuple:
    """计算日期范围，返回 (days, start_date, end_date, custom_range)"""
    if start and end:
        from datetime import datetime as dt
        try:
            s = dt.strptime(start, "%Y-%m-%d")
            e = dt.strptime(end, "%Y-%m-%d")
            delta = (e - s).days
            return max(delta, 1), start, end, True
        except ValueError:
            pass
    end_date = datetime.now().strftime("%Y-%m-%d")
    from datetime import timedelta
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    return days, start_date, end_date, False


@router.get("", response_class=HTMLResponse)
async def hotspot_overview(request: Request, days: int = 7, start: str = None, end: str = None):
    d, start_date, end_date, custom_range = _calc_days(start, end, days)
    ctx = {
        "request": request,
        "active_page": "hotspot",
        "days": d,
        "start_date": start_date,
        "end_date": end_date,
        "custom_range": custom_range,
        "hotspots": get_top_hotspots(d, 10),
        "chart_data": get_frequency_chart_data(d),
        "clusters": get_tag_clusters(d),
    }
    return templates.TemplateResponse("hotspot.html", ctx)


@router.get("/research/{group_id}", response_class=HTMLResponse)
async def hotspot_research(request: Request, group_id: int, tab: str = "logic"):
    data = get_research_data(group_id)
    if not data:
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/hotspot")

    ctx = {
        "request": request,
        "active_page": "hotspot",
        "group": data["group"],
        "research": data["research"],
        "active_tab": tab,
    }
    return templates.TemplateResponse("hotspot_research.html", ctx)


@router.post("/recommend", response_class=HTMLResponse)
async def trigger_recommend(request: Request, days: int = 7):
    """触发 AI 标签组推荐"""
    error = None
    try:
        groups = recommend_tag_groups(days, top_n=5)
        # 自动保存推荐结果
        for g in groups:
            save_tag_group(
                g.get("group_name", " + ".join(g["tags"][:3])),
                g["tags"],
                group_logic=g.get("group_logic"),
                time_range=days,
            )
    except Exception as e:
        error = str(e)

    ctx = {
        "request": request,
        "active_page": "hotspot",
        "days": days,
        "hotspots": get_top_hotspots(days, 10),
        "chart_data": get_frequency_chart_data(days),
        "clusters": get_tag_clusters(days),
        "recommend_error": error,
    }
    return templates.TemplateResponse("hotspot.html", ctx)


@router.post("/research/{group_id}/run", response_class=HTMLResponse)
async def run_deep_research(request: Request, group_id: int):
    """触发深度研究"""
    error = None
    try:
        research_tag_group(group_id)
    except Exception as e:
        error = str(e)

    data = get_research_data(group_id)
    ctx = {
        "request": request,
        "active_page": "hotspot",
        "group": data["group"] if data else {},
        "research": data["research"] if data else {},
        "active_tab": "logic",
        "research_error": error,
    }
    return templates.TemplateResponse("hotspot_research.html", ctx)


@router.post("/api/save-to-kg", response_class=JSONResponse)
async def save_to_kg(request: Request):
    """将标签组保存到知识图谱（theme 类型实体）"""
    body = await request.json()
    group_id = body.get("group_id")
    if not group_id:
        return JSONResponse({"error": "缺少 group_id"}, status_code=400)

    groups = get_saved_groups()
    group = next((g for g in groups if g["id"] == int(group_id)), None)
    if not group:
        return JSONResponse({"error": "标签组不存在"}, status_code=404)

    group_name = group["group_name"]
    group_logic = group.get("group_logic") or ""
    tags = json.loads(group.get("tags_json") or "[]")

    properties = {"tags": tags, "source": "hotspot_tag_group", "source_id": group_id}

    # 检查是否已存在同名 theme 实体
    existing = execute_query(
        "SELECT id FROM kg_entities WHERE entity_type='theme' AND entity_name=?",
        [group_name],
    )
    if existing:
        eid = existing[0]["id"]
        update_entity(eid, properties=properties, investment_logic=group_logic)
    else:
        eid = add_entity(
            entity_type="theme",
            entity_name=group_name,
            investment_logic=group_logic,
            properties=properties,
        )

    return {"ok": True, "entity_id": eid, "message": f"已保存到知识图谱: {group_name}"}


@router.post("/group/{group_id}/delete")
async def delete_group(group_id: int):
    delete_tag_group(group_id)
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/hotspot", status_code=303)


@router.get("/research/{group_id}/followup", response_class=HTMLResponse)
async def hotspot_followup(request: Request, group_id: int):
    """追踪研究配置页"""
    data = get_research_data(group_id)
    if not data:
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/hotspot")

    ctx = {
        "request": request,
        "active_page": "hotspot",
        "group": data["group"],
        "research": data["research"],
    }
    return templates.TemplateResponse("hotspot_followup.html", ctx)


@router.post("/research/{group_id}/run-followup", response_class=HTMLResponse)
async def run_followup_research(request: Request, group_id: int):
    """触发追踪研究（带配置）"""
    form = await request.form()
    # 读取配置（后续可传入 research_tag_group）
    context = form.get("context", "")
    constraints = form.get("constraints", "")
    dimensions = form.getlist("dimensions")

    error = None
    try:
        research_tag_group(group_id)
    except Exception as e:
        error = str(e)

    data = get_research_data(group_id)
    ctx = {
        "request": request,
        "active_page": "hotspot",
        "group": data["group"] if data else {},
        "research": data["research"] if data else {},
        "active_tab": "logic",
        "research_error": error,
    }
    return templates.TemplateResponse("hotspot_research.html", ctx)

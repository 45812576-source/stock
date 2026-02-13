"""知识图谱路由 — Schema / 可视化 / 实体管理 / 推理引擎"""
import json
import logging
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Request, BackgroundTasks, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from utils.db_utils import execute_query, execute_insert
from knowledge_graph.kg_manager import (
    get_kg_stats, get_all_entities, get_entity_by_id,
    get_entity_relations, get_subgraph, get_entity_count,
    add_entity, add_relationship, delete_entity, delete_relationship,
    update_entity, update_relationship, get_update_log,
)
from knowledge_graph.kg_query import search_entities, find_path, get_related_stocks

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/kg", tags=["knowledge_graph"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# 实体类型配色
ENTITY_COLORS = {
    "company":      {"bg": "#10b981", "label": "公司",     "icon": "business"},
    "industry":     {"bg": "#f59e0b", "label": "行业",     "icon": "category"},
    "theme":        {"bg": "#8b5cf6", "label": "主题",     "icon": "lightbulb"},
    "macro_factor": {"bg": "#ef4444", "label": "宏观因素", "icon": "public"},
    "indicator":    {"bg": "#06b6d4", "label": "指标",     "icon": "show_chart"},
}

RELATION_LABELS = {
    "impacts":    {"label": "影响", "color": "#f59e0b"},
    "belongs_to": {"label": "属于", "color": "#8b5cf6"},
    "competes":   {"label": "竞争", "color": "#ef4444"},
    "supplies":   {"label": "供应", "color": "#10b981"},
    "benefits":   {"label": "受益", "color": "#06b6d4"},
    "related":    {"label": "相关", "color": "#64748b"},
}

# 实体类型默认属性定义
ENTITY_SCHEMA = {
    "company":      ["stock_code", "market", "sector", "market_cap"],
    "industry":     ["level", "parent_industry"],
    "theme":        ["hot_score", "source"],
    "macro_factor": ["region", "frequency", "impact_direction"],
    "indicator":    ["unit", "data_source", "update_frequency"],
}


def _common_ctx(tab: str):
    """各 tab 共用的模板上下文"""
    stats = get_kg_stats()
    return {
        "active_page": "kg",
        "tab": tab,
        "stats": stats,
        "entity_colors": ENTITY_COLORS,
        "relation_labels": RELATION_LABELS,
        "entity_schema": ENTITY_SCHEMA,
    }


# ==================== 页面路由 ====================

@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def kg_schema_page(request: Request):
    """Schema Designer（默认页）"""
    ctx = _common_ctx("schema")
    ctx["request"] = request
    return templates.TemplateResponse("knowledge_graph.html", ctx)


@router.get("/visualization", response_class=HTMLResponse)
async def kg_visualization(request: Request):
    """知识图谱可视化"""
    ctx = _common_ctx("visualization")
    ctx["request"] = request
    return templates.TemplateResponse("knowledge_graph.html", ctx)


@router.get("/entities", response_class=HTMLResponse)
async def kg_entities_page(request: Request, entity_type: str = "", q: str = "", page: int = 1):
    """实体管理"""
    ctx = _common_ctx("entities")
    per_page = 50
    offset = (page - 1) * per_page

    if q:
        entities = search_entities(q, entity_type=entity_type or None, limit=per_page)
        total = len(entities)
    else:
        entities = get_all_entities(entity_type=entity_type or None, limit=per_page, offset=offset)
        total = get_entity_count(entity_type=entity_type or None)

    ctx.update({
        "request": request,
        "entities": [dict(e) for e in (entities or [])],
        "filter_type": entity_type,
        "filter_q": q,
        "page": page,
        "total": total,
        "per_page": per_page,
    })
    return templates.TemplateResponse("knowledge_graph.html", ctx)


@router.get("/inference", response_class=HTMLResponse)
async def kg_inference_page(request: Request):
    """推理引擎"""
    ctx = _common_ctx("inference")
    ctx["request"] = request
    # 最近更新日志
    try:
        logs = get_update_log(limit=20)
    except Exception:
        logs = []
    ctx["update_logs"] = [dict(l) for l in (logs or [])]
    return templates.TemplateResponse("knowledge_graph.html", ctx)


# ==================== API 路由 ====================

@router.get("/api/graph-data", response_class=JSONResponse)
async def api_graph_data(center_id: int = 0, depth: int = 2):
    """获取图谱数据（vis-network 格式）"""
    if center_id > 0:
        subgraph = get_subgraph(center_id, depth=depth)
        raw_nodes = subgraph["nodes"]
        raw_edges = subgraph["edges"]
    else:
        raw_nodes = execute_query("SELECT * FROM kg_entities LIMIT 200") or []
        raw_edges = execute_query(
            """SELECT r.*, e1.entity_name as source_name, e2.entity_name as target_name
               FROM kg_relationships r
               JOIN kg_entities e1 ON r.source_entity_id=e1.id
               JOIN kg_entities e2 ON r.target_entity_id=e2.id
               LIMIT 500"""
        ) or []

    nodes = []
    seen_ids = set()
    for n in raw_nodes:
        nd = dict(n)
        nid = nd["id"]
        if nid in seen_ids:
            continue
        seen_ids.add(nid)
        etype = nd.get("entity_type", "")
        color = ENTITY_COLORS.get(etype, {}).get("bg", "#64748b")
        nodes.append({
            "id": nid,
            "label": nd["entity_name"],
            "group": etype,
            "color": {"background": color, "border": color, "highlight": {"background": color, "border": "#fff"}},
            "font": {"color": "#e2e8f0", "size": 12},
            "shape": "dot" if etype != "company" else "diamond",
            "size": 20 if etype == "company" else 15,
            "title": f"{ENTITY_COLORS.get(etype, {}).get('label', etype)}: {nd['entity_name']}",
        })

    edges = []
    seen_edges = set()
    for e in raw_edges:
        ed = dict(e)
        eid = ed["id"]
        if eid in seen_edges:
            continue
        seen_edges.add(eid)
        rtype = ed.get("relation_type", "")
        direction_color = {"positive": "#10b981", "negative": "#ef4444", "neutral": "#64748b"}.get(
            ed.get("direction", "neutral"), "#64748b")
        edges.append({
            "id": eid,
            "from": ed["source_entity_id"],
            "to": ed["target_entity_id"],
            "label": RELATION_LABELS.get(rtype, {}).get("label", rtype),
            "color": {"color": direction_color, "opacity": 0.6},
            "width": max(1, (ed.get("strength") or 0.5) * 3),
            "arrows": "to",
            "font": {"color": "#475569", "size": 9, "strokeWidth": 0},
            "title": f"{rtype} (强度: {ed.get('strength', 0.5):.1f}, 置信度: {ed.get('confidence', 0.5):.1f})",
        })

    return {"nodes": nodes, "edges": edges}


@router.get("/api/entity/{entity_id}", response_class=JSONResponse)
async def api_entity_detail(entity_id: int):
    """获取实体详情 + 关系"""
    entity = get_entity_by_id(entity_id)
    if not entity:
        return JSONResponse({"error": "实体不存在"}, status_code=404)

    ed = dict(entity)
    props = {}
    try:
        props = json.loads(ed.get("properties_json") or "{}")
    except Exception:
        pass

    relations = get_entity_relations(entity_id)
    outgoing = [dict(r) for r in (relations.get("outgoing") or [])]
    incoming = [dict(r) for r in (relations.get("incoming") or [])]

    related_news = []
    if ed.get("entity_type") == "company":
        try:
            news = execute_query(
                """SELECT ci.summary, ci.sentiment, ci.importance, ci.cleaned_at
                   FROM item_companies ic JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
                   WHERE ic.stock_name LIKE ?
                   ORDER BY ci.cleaned_at DESC LIMIT 5""",
                [f"%{ed['entity_name']}%"])
            related_news = [dict(n) for n in (news or [])]
        except Exception:
            pass

    return {
        "entity": ed,
        "properties": props,
        "outgoing": outgoing,
        "incoming": incoming,
        "related_news": related_news,
        "degree": len(outgoing) + len(incoming),
    }


@router.get("/api/search", response_class=JSONResponse)
async def api_search(q: str = "", entity_type: str = ""):
    """搜索实体"""
    if not q:
        return []
    results = search_entities(q, entity_type=entity_type or None, limit=20)
    return [dict(r) for r in (results or [])]


@router.get("/api/path", response_class=JSONResponse)
async def api_find_path(source_id: int = 0, target_id: int = 0):
    """查找路径"""
    if not source_id or not target_id:
        return {"error": "需要指定起点和终点"}
    path = find_path(source_id, target_id)
    if not path:
        return {"path": None, "message": "未找到路径"}
    path_entities = []
    for eid in path:
        e = get_entity_by_id(eid)
        if e:
            path_entities.append(dict(e))
    return {"path": path, "entities": path_entities}


# --- 实体 CRUD API ---

@router.post("/api/entity", response_class=JSONResponse)
async def api_add_entity(request: Request):
    """新增实体"""
    body = await request.json()
    eid = add_entity(
        entity_type=body["entity_type"],
        entity_name=body["entity_name"],
        description=body.get("description"),
        properties=body.get("properties"),
        investment_logic=body.get("investment_logic"),
    )
    return {"id": eid, "ok": True}


@router.put("/api/entity/{entity_id}", response_class=JSONResponse)
async def api_update_entity(entity_id: int, request: Request):
    """更新实体"""
    body = await request.json()
    ok = update_entity(
        entity_id,
        description=body.get("description"),
        properties=body.get("properties"),
        investment_logic=body.get("investment_logic"),
    )
    return {"ok": ok}


@router.delete("/api/entity/{entity_id}", response_class=JSONResponse)
async def api_delete_entity(entity_id: int):
    """删除实体"""
    ok = delete_entity(entity_id)
    return {"ok": ok}


# --- 关系 CRUD API ---

@router.post("/api/relationship", response_class=JSONResponse)
async def api_add_relationship(request: Request):
    """新增关系"""
    body = await request.json()
    rid = add_relationship(
        source_id=body["source_id"],
        target_id=body["target_id"],
        relation_type=body["relation_type"],
        strength=body.get("strength", 0.5),
        direction=body.get("direction", "positive"),
        evidence=body.get("evidence"),
        confidence=body.get("confidence", 0.5),
    )
    return {"id": rid, "ok": True}


@router.delete("/api/relationship/{rel_id}", response_class=JSONResponse)
async def api_delete_rel(rel_id: int):
    """删除关系"""
    ok = delete_relationship(rel_id)
    return {"ok": ok}


# --- 推理 / 构建 API ---

_kg_tasks = {}

@router.post("/api/update-kg", response_class=JSONResponse)
async def api_update_kg(background_tasks: BackgroundTasks, use_claude: bool = False):
    """触发知识图谱更新"""
    task_id = f"kg_update_{int(datetime.now().timestamp())}"
    _kg_tasks[task_id] = {"status": "running", "progress": "", "result": None}

    def _run():
        task = _kg_tasks[task_id]
        try:
            from knowledge_graph.kg_updater import update_from_cleaned_items
            task["progress"] = "正在从清洗数据中提取实体和关系..."
            result = update_from_cleaned_items(use_claude=use_claude)
            task["result"] = result
            task["status"] = "done"
        except Exception as e:
            task["result"] = {"error": str(e)}
            task["status"] = "failed"

    background_tasks.add_task(_run)
    return {"task_id": task_id}


@router.post("/api/inference", response_class=JSONResponse)
async def api_run_inference(background_tasks: BackgroundTasks, request: Request):
    """运行推理引擎 — 发现隐含关系"""
    body = await request.json()
    rule_type = body.get("rule_type", "all")  # all / path / similarity / chain
    task_id = f"kg_infer_{int(datetime.now().timestamp())}"
    _kg_tasks[task_id] = {"status": "running", "progress": "", "result": None}

    def _run():
        task = _kg_tasks[task_id]
        try:
            discovered = []
            task["progress"] = "正在分析实体关系模式..."

            # 1) 路径推理：如果 A→B, B→C 且无 A→C，推荐 A→C
            if rule_type in ("all", "chain"):
                task["progress"] = "链式推理：发现间接关系..."
                rels = execute_query(
                    """SELECT r1.source_entity_id as a, r1.target_entity_id as b,
                              r2.target_entity_id as c, r1.relation_type as r1_type,
                              r2.relation_type as r2_type,
                              e1.entity_name as a_name, e2.entity_name as b_name, e3.entity_name as c_name,
                              e1.entity_type as a_type, e3.entity_type as c_type
                       FROM kg_relationships r1
                       JOIN kg_relationships r2 ON r1.target_entity_id = r2.source_entity_id
                       JOIN kg_entities e1 ON r1.source_entity_id = e1.id
                       JOIN kg_entities e2 ON r1.target_entity_id = e2.id
                       JOIN kg_entities e3 ON r2.target_entity_id = e3.id
                       WHERE r1.source_entity_id != r2.target_entity_id
                       AND NOT EXISTS (
                           SELECT 1 FROM kg_relationships r3
                           WHERE r3.source_entity_id = r1.source_entity_id
                           AND r3.target_entity_id = r2.target_entity_id
                       )
                       LIMIT 50""") or []
                for r in rels:
                    rd = dict(r)
                    discovered.append({
                        "type": "chain",
                        "source_name": rd["a_name"], "source_type": rd["a_type"],
                        "target_name": rd["c_name"], "target_type": rd["c_type"],
                        "source_id": rd["a"], "target_id": rd["c"],
                        "via": rd["b_name"],
                        "logic": f"{rd['a_name']} —[{rd['r1_type']}]→ {rd['b_name']} —[{rd['r2_type']}]→ {rd['c_name']}",
                        "confidence": 0.4,
                    })

            # 2) 同类推理：同行业公司共享关系
            if rule_type in ("all", "similarity"):
                task["progress"] = "相似性推理：分析同类实体..."
                same_industry = execute_query(
                    """SELECT DISTINCT e1.id as id1, e1.entity_name as name1,
                              e2.id as id2, e2.entity_name as name2,
                              e3.entity_name as industry_name
                       FROM kg_relationships r1
                       JOIN kg_relationships r2 ON r1.target_entity_id = r2.target_entity_id
                       JOIN kg_entities e1 ON r1.source_entity_id = e1.id
                       JOIN kg_entities e2 ON r2.source_entity_id = e2.id
                       JOIN kg_entities e3 ON r1.target_entity_id = e3.id
                       WHERE e1.entity_type = 'company' AND e2.entity_type = 'company'
                       AND e3.entity_type = 'industry'
                       AND r1.relation_type = 'belongs_to' AND r2.relation_type = 'belongs_to'
                       AND e1.id < e2.id
                       AND NOT EXISTS (
                           SELECT 1 FROM kg_relationships r3
                           WHERE (r3.source_entity_id = e1.id AND r3.target_entity_id = e2.id)
                           OR (r3.source_entity_id = e2.id AND r3.target_entity_id = e1.id)
                       )
                       LIMIT 30""") or []
                for r in same_industry:
                    rd = dict(r)
                    discovered.append({
                        "type": "similarity",
                        "source_name": rd["name1"], "source_type": "company",
                        "target_name": rd["name2"], "target_type": "company",
                        "source_id": rd["id1"], "target_id": rd["id2"],
                        "via": rd["industry_name"],
                        "logic": f"{rd['name1']} 和 {rd['name2']} 同属 {rd['industry_name']}，可能存在竞争关系",
                        "confidence": 0.3,
                    })

            task["result"] = {"discovered": discovered[:100], "total": len(discovered)}
            task["status"] = "done"
        except Exception as e:
            logger.exception("推理引擎异常")
            task["result"] = {"error": str(e)}
            task["status"] = "failed"

    background_tasks.add_task(_run)
    return {"task_id": task_id}


@router.post("/api/accept-inference", response_class=JSONResponse)
async def api_accept_inference(request: Request):
    """接受推理结果，写入知识图谱"""
    body = await request.json()
    rid = add_relationship(
        source_id=body["source_id"],
        target_id=body["target_id"],
        relation_type=body.get("relation_type", "related"),
        strength=body.get("confidence", 0.3),
        direction=body.get("direction", "neutral"),
        evidence=body.get("logic", "AI推理"),
        confidence=body.get("confidence", 0.3),
    )
    return {"id": rid, "ok": True}


@router.get("/api/task-status/{task_id}", response_class=JSONResponse)
async def api_task_status(task_id: str):
    task = _kg_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return task

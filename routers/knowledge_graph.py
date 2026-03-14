"""知识图谱路由 — Schema / 可视化 / 实体管理 / 推理引擎"""
import json
import logging
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Request, BackgroundTasks, Query, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from utils.db_utils import execute_query, execute_insert
from utils.auth_deps import get_current_user, require_annotator, require_super_admin
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
    "market":          {"bg": "#3b82f6", "label": "市场",         "icon": "public"},
    "macro_indicator": {"bg": "#ef4444", "label": "宏观指标",     "icon": "show_chart"},
    "policy":          {"bg": "#6366f1", "label": "政策",         "icon": "gavel"},
    "industry":        {"bg": "#f59e0b", "label": "行业",         "icon": "category"},
    "industry_chain":  {"bg": "#d97706", "label": "产业链",       "icon": "link"},
    "theme":           {"bg": "#8b5cf6", "label": "投资主题",     "icon": "lightbulb"},
    "company":         {"bg": "#10b981", "label": "公司",         "icon": "business"},
    "strategy":        {"bg": "#14b8a6", "label": "战略/模式",    "icon": "strategy"},
    "commodity":       {"bg": "#78716c", "label": "大宗商品",     "icon": "inventory_2"},
    "energy":          {"bg": "#f97316", "label": "能源",         "icon": "local_fire_department"},
    "intermediate":    {"bg": "#06b6d4", "label": "半成品",       "icon": "precision_manufacturing"},
    "consumer_good":   {"bg": "#ec4899", "label": "消费品",       "icon": "shopping_cart"},
}

RELATION_LABELS = {
    # 因果影响
    "causes_positive":          {"label": "正向驱动",   "color": "#10b981"},
    "causes_negative":          {"label": "负向影响",   "color": "#ef4444"},
    "cost_transmission":        {"label": "成本传导",   "color": "#f97316"},
    "indicator_transmission":   {"label": "指标传导",   "color": "#f59e0b"},
    "demand_driven":            {"label": "需求驱动",   "color": "#3b82f6"},
    "supply_driven":            {"label": "供给驱动",   "color": "#06b6d4"},
    "demand_source_of":         {"label": "需求来源",   "color": "#8b5cf6"},
    "demand_substitute":        {"label": "需求替代",   "color": "#d97706"},
    # 结构归属
    "belongs_to_industry":      {"label": "属于行业",   "color": "#8b5cf6"},
    "belongs_to_chain":         {"label": "属于产业链", "color": "#d97706"},
    "policy_affects":           {"label": "政策影响",   "color": "#6366f1"},
    "supplier_of":              {"label": "供应商",     "color": "#10b981"},
    "customer_of":              {"label": "客户",       "color": "#3b82f6"},
    "competitor":               {"label": "竞争",       "color": "#ef4444"},
    "substitute_threat":        {"label": "替代威胁",   "color": "#f97316"},
    "controls":                 {"label": "控制",       "color": "#78716c"},
    "holds_stake":              {"label": "持股",       "color": "#14b8a6"},
    "subsidiary_of":            {"label": "子公司",     "color": "#06b6d4"},
    # 要素关联
    "major_cost_item":          {"label": "主要成本",   "color": "#ef4444"},
    "major_revenue_item":       {"label": "主要收入",   "color": "#10b981"},
    "cost_affected_by":         {"label": "成本受影响", "color": "#f97316"},
    "revenue_affected_by":      {"label": "收入受影响", "color": "#3b82f6"},
    # 政策关联
    "benefits":                 {"label": "受益",       "color": "#10b981"},
    "hurts":                    {"label": "受损",       "color": "#ef4444"},
    "risk_factor":              {"label": "风险因素",   "color": "#f59e0b"},
    "catalyst":                 {"label": "催化剂",     "color": "#8b5cf6"},
    # 指标关联
    "leading_indicator_of":     {"label": "领先指标",   "color": "#3b82f6"},
    "coincident_indicator_of":  {"label": "同步指标",   "color": "#06b6d4"},
    "lagging_indicator_of":     {"label": "滞后指标",   "color": "#78716c"},
    "correlated_with":          {"label": "相关性",     "color": "#d97706"},
    # 兜底
    "related":                  {"label": "相关",       "color": "#64748b"},
}

# 实体类型默认属性定义
ENTITY_SCHEMA = {
    "market":          ["region", "market_type"],
    "macro_indicator": ["region", "frequency", "unit", "data_source"],
    "policy":          ["region", "effective_date", "impact_scope"],
    "industry":        ["level", "parent_industry"],
    "industry_chain":  ["chain_level", "core_links"],
    "theme":           ["hot_score", "source"],
    "company":         ["stock_code", "market", "sector", "market_cap"],
    "strategy":        ["strategy_type", "target_market"],
    "commodity":       ["unit", "exchange", "category"],
    "energy":          ["unit", "exchange", "category"],
    "intermediate":    ["upstream_material", "downstream_product"],
    "consumer_good":   ["category", "target_market"],
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
def kg_schema_page(request: Request):
    """Schema Designer（默认页）"""
    ctx = _common_ctx("schema")
    ctx["request"] = request
    return templates.TemplateResponse("knowledge_graph.html", ctx)


@router.get("/visualization", response_class=HTMLResponse)
def kg_visualization(request: Request):
    """知识图谱可视化"""
    ctx = _common_ctx("visualization")
    ctx["request"] = request
    return templates.TemplateResponse("knowledge_graph.html", ctx)


@router.get("/entities", response_class=HTMLResponse)
def kg_entities_page(request: Request, entity_type: str = "", q: str = "", page: int = 1):
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
def kg_inference_page(request: Request):
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
def api_graph_data(center_id: int = 0, depth: int = 2):
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

    # 每种实体类型的形状和大小
    SHAPE_MAP = {
        "market": ("hexagon", 26),
        "theme": ("star", 26),
        "industry": ("dot", 24),
        "industry_chain": ("dot", 22),
        "company": ("diamond", 28),
        "strategy": ("square", 20),
        "macro_indicator": ("triangle", 24),
        "commodity": ("square", 22),
        "energy": ("triangleDown", 24),
        "intermediate": ("square", 20),
        "consumer_good": ("dot", 22),
        "policy": ("triangle", 22),
    }

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
        shape, size = SHAPE_MAP.get(etype, ("dot", 20))
        nodes.append({
            "id": nid,
            "label": nd["entity_name"],
            "group": etype,
            "font": {"color": "#e2e8f0", "size": 14},
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
            "font": {"color": "#64748b", "size": 10, "strokeWidth": 0},
            "title": f"{rtype} (强度: {ed.get('strength', 0.5):.1f}, 置信度: {ed.get('confidence', 0.5):.1f})",
        })

    return {"nodes": nodes, "edges": edges}


@router.get("/api/entity/{entity_id}", response_class=JSONResponse)
def api_entity_detail(entity_id: int):
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
def api_search(q: str = "", entity_type: str = ""):
    """搜索实体"""
    if not q:
        return []
    results = search_entities(q, entity_type=entity_type or None, limit=20)
    return [dict(r) for r in (results or [])]


@router.get("/api/path", response_class=JSONResponse)
def api_find_path(source_id: int = 0, target_id: int = 0):
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
def api_delete_entity(entity_id: int):
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
def api_delete_rel(rel_id: int):
    """删除关系"""
    ok = delete_relationship(rel_id)
    return {"ok": ok}


# --- 推理 / 构建 API ---

def _run_inference_sync(rule_type="all", auto_accept=False):
    """同步运行推理引擎（供调度器和构建后自动调用）

    Args:
        rule_type: all / chain / similarity
        auto_accept: 是否自动采纳置信度 >= 0.5 的结果写入KG

    Returns:
        list of discovered relations
    """
    discovered = []

    # 停用词：太泛的 theme 名，不应参与推理
    STOP_THEMES = {"市场", "策略", "综合", "其他", "热点", "概念", "板块", "行情",
                   "投资", "分析", "研究", "报告", "观点", "趋势", "机会", "风险"}

    # 预加载所有 industry 实体名，用于过滤混入 theme 的行业名
    _ind_names = execute_query(
        "SELECT DISTINCT entity_name FROM kg_entities WHERE entity_type='industry'"
    ) or []
    INDUSTRY_NAMES = {r["entity_name"] for r in _ind_names}

    # ── 1) 链式推理：A→B→C，置信度 = min(两条边strength) × 0.8 ──
    if rule_type in ("all", "chain"):
        rels = execute_query(
            """SELECT r1.source_entity_id as a, r1.target_entity_id as b,
                      r2.target_entity_id as c, r1.relation_type as r1_type,
                      r2.relation_type as r2_type,
                      r1.strength as s1, r2.strength as s2,
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
               ORDER BY LEAST(r1.strength, r2.strength) DESC
               LIMIT 50""") or []
        for r in rels:
            rd = dict(r)
            s1 = float(rd.get("s1") or 0.5)
            s2 = float(rd.get("s2") or 0.5)
            conf = round(min(s1, s2) * 0.8, 2)
            discovered.append({
                "type": "chain",
                "source_name": rd["a_name"], "source_type": rd["a_type"],
                "target_name": rd["c_name"], "target_type": rd["c_type"],
                "source_id": rd["a"], "target_id": rd["c"],
                "via": rd["b_name"],
                "logic": f"{rd['a_name']} —[{rd['r1_type']}]→ {rd['b_name']} —[{rd['r2_type']}]→ {rd['c_name']}",
                "confidence": conf,
            })

    # ── 2) 同行业竞争，置信度按行业粒度区分 ──
    if rule_type in ("all", "similarity"):
        same_industry = execute_query(
            """SELECT DISTINCT e1.id as id1, e1.entity_name as name1,
                      e2.id as id2, e2.entity_name as name2,
                      e3.entity_name as industry_name,
                      r1.strength as s1, r2.strength as s2
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
            ind = rd["industry_name"]
            # 细分行业（名称≥4字符或含"/"）置信度更高
            is_specific = len(ind) >= 4 or "/" in ind
            base = 0.45 if is_specific else 0.25
            conf = round(min(base, 0.6), 2)
            discovered.append({
                "type": "similarity",
                "source_name": rd["name1"], "source_type": "company",
                "target_name": rd["name2"], "target_type": "company",
                "source_id": rd["id1"], "target_id": rd["id2"],
                "via": ind,
                "logic": f"{rd['name1']} 和 {rd['name2']} 同属 {ind}，可能存在竞争关系",
                "confidence": conf,
            })

    # ── 3) — 共享主题规则已移除，主题由 kg_theme_identifier 独立识别 ──

    # ── 4) 行业传导，置信度 = 行业→主题边的 strength × 0.7 ──
    if rule_type in ("all", "chain"):
        ind_theme = execute_query(
            """SELECT DISTINCT c.id as comp_id, c.entity_name as comp_name,
                      t.id as theme_id, t.entity_name as theme_name,
                      ind.entity_name as ind_name,
                      r_it.strength as it_strength
               FROM kg_relationships r_bt
               JOIN kg_entities c ON r_bt.source_entity_id = c.id AND c.entity_type = 'company'
               JOIN kg_entities ind ON r_bt.target_entity_id = ind.id AND ind.entity_type = 'industry'
               JOIN kg_relationships r_it ON r_it.source_entity_id = ind.id
               JOIN kg_entities t ON r_it.target_entity_id = t.id AND t.entity_type = 'theme'
               WHERE r_bt.relation_type = 'belongs_to'
               AND NOT EXISTS (
                   SELECT 1 FROM kg_relationships r3
                   WHERE r3.source_entity_id = c.id AND r3.target_entity_id = t.id
               )
               ORDER BY r_it.strength DESC
               LIMIT 30""") or []
        for r in ind_theme:
            rd = dict(r)
            # 过滤掉停用词主题
            if rd["theme_name"] in STOP_THEMES or rd["theme_name"] in INDUSTRY_NAMES:
                continue
            it_s = float(rd.get("it_strength") or 0.5)
            conf = round(it_s * 0.7, 2)
            discovered.append({
                "type": "chain",
                "source_name": rd["comp_name"], "source_type": "company",
                "target_name": rd["theme_name"], "target_type": "theme",
                "source_id": rd["comp_id"], "target_id": rd["theme_id"],
                "via": rd["ind_name"],
                "logic": f"{rd['comp_name']} 属于 {rd['ind_name']}，{rd['ind_name']} 关联 {rd['theme_name']}",
                "confidence": conf,
            })

    # 自动采纳高置信度结果
    if auto_accept:
        from knowledge_graph.kg_manager import add_relationship as _add_rel
        accepted = 0
        for d in discovered:
            if d["confidence"] >= 0.5:
                try:
                    _add_rel(
                        source_id=d["source_id"], target_id=d["target_id"],
                        relation_type="related",
                        strength=d["confidence"], direction="neutral",
                        evidence=d["logic"], confidence=d["confidence"],
                    )
                    accepted += 1
                except Exception:
                    pass
        logger.info(f"[Inference] 自动采纳 {accepted}/{len(discovered)} 条推理结果 (置信度>=0.5)")

    return discovered[:100]


_kg_tasks = {}
_kg_inspect_tasks = {}


@router.get("/inspect", response_class=HTMLResponse)
def kg_inspect_page(request: Request):
    """KG 巡检"""
    ctx = _common_ctx("inspect")
    ctx["request"] = request
    return templates.TemplateResponse("knowledge_graph.html", ctx)


@router.post("/api/kg-inspect", response_class=JSONResponse)
def run_kg_inspect(background_tasks: BackgroundTasks,
                   days: int = 7, limit: int = 20, dry_run: bool = False):
    """触发 KG 巡检（冲突清理 + 交叉补全）"""
    task_id = f"kg_inspect_{int(datetime.now().timestamp())}"
    _kg_inspect_tasks[task_id] = {
        "status": "running",
        "phase": "schema_validate",   # schema_validate | name_cleanup | cleanup | cross_complete | done
        "phase_label": "正在扫描 Schema...",
        "scan": None,          # schema_validate 结果（发现了多少脏数据）
        "name_cleanup": None,  # name_cleanup 结果
        "cleanup": None,       # conflict_cleanup 结果
        "cross": None,         # cross_complete 进度
        "cross_progress": 0,
        "cross_total": 0,
        "result": None,
        "started_at": datetime.now().isoformat(),
    }

    def _run():
        task = _kg_inspect_tasks[task_id]
        try:
            from knowledge_graph.kg_inspector import schema_validate, conflict_cleanup, cross_complete, name_cleanup

            # Phase 1: Schema 扫描
            task["phase"] = "schema_validate"
            task["phase_label"] = "正在扫描 Schema..."
            scan = schema_validate(dry_run=dry_run)
            task["scan"] = {
                "invalid_entity_types": scan["invalid_entity_types"],
                "invalid_relation_types": scan["invalid_relation_types"],
                "invalid_combinations": scan["invalid_combinations"],
                "conflicting_relations": scan["conflicting_relations"],
                "invalid_entity_names": scan["invalid_entity_names"],
                "related_ratio": scan["related_ratio"],
                "details_count": len(scan.get("details", [])),
                "details": scan.get("details", [])[:50],  # 最多展示50条
            }

            # Phase 1.5: 实体名规范化清洗
            task["phase"] = "name_cleanup"
            task["phase_label"] = "正在清洗实体名..."
            name_result = name_cleanup(dry_run=dry_run)
            task["name_cleanup"] = name_result

            # Phase 2: 冲突清理
            task["phase"] = "cleanup"
            task["phase_label"] = "正在清理脏数据..."
            cleanup = conflict_cleanup(dry_run=dry_run)
            task["cleanup"] = cleanup

            # Phase 3: 交叉补全
            task["phase"] = "cross_complete"
            task["phase_label"] = "正在交叉补全..."

            def _progress(current, total, msg):
                task["cross_progress"] = current
                task["cross_total"] = total
                task["phase_label"] = f"交叉补全 {current}/{total}"

            cross = cross_complete(days=days, limit=limit, progress_callback=_progress)
            task["cross"] = cross

            task["phase"] = "done"
            task["phase_label"] = "完成"
            task["status"] = "done"
            task["result"] = {
                "schema_validation": scan,
                "name_cleanup": name_result,
                "cleanup": cleanup,
                "cross_complete": cross,
                "inspected_at": datetime.now().isoformat(),
            }
            execute_insert(
                "REPLACE INTO system_config (config_key, value) VALUES ('kg_last_inspect', %s)",
                [datetime.now().isoformat()],
            )
        except Exception as e:
            task["status"] = "failed"
            task["phase_label"] = f"失败: {str(e)[:100]}"
            task["result"] = {"error": str(e)}
            logger.error(f"KG巡检失败: {e}")

    background_tasks.add_task(_run)
    return {"task_id": task_id}


@router.get("/api/kg-inspect/{task_id}", response_class=JSONResponse)
def kg_inspect_status_kg(task_id: str):
    """查询 KG 巡检任务状态"""
    task = _kg_inspect_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return JSONResponse(task)


@router.get("/api/kg-inspect-info", response_class=JSONResponse)
def kg_inspect_info_kg():
    """获取 KG 巡检基本信息（上次时间、实体/关系数）"""
    try:
        last_inspect = execute_query("SELECT value FROM system_config WHERE config_key='kg_last_inspect'")
        entity_count = execute_query("SELECT COUNT(*) as cnt FROM kg_entities")
        rel_count = execute_query("SELECT COUNT(*) as cnt FROM kg_relationships")
        return {
            "last_inspect": last_inspect[0]["value"] if last_inspect else None,
            "entity_count": entity_count[0]["cnt"] if entity_count else 0,
            "relationship_count": rel_count[0]["cnt"] if rel_count else 0,
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/identify-themes", response_class=JSONResponse)
def api_identify_themes(background_tasks: BackgroundTasks,
                        days: int = 7, confidence_threshold: float = 0.6):
    """触发投资主题识别（纯数据驱动，不调用API）"""
    task_id = f"kg_themes_{int(datetime.now().timestamp())}"
    _kg_tasks[task_id] = {"status": "running", "progress": "", "result": None}

    def _run():
        task = _kg_tasks[task_id]
        try:
            from knowledge_graph.kg_theme_identifier import identify_themes
            def _cb(cur, total, msg):
                task["progress"] = msg
            result = identify_themes(
                days=days,
                confidence_threshold=confidence_threshold,
                progress_callback=_cb,
            )
            task["result"] = result
            task["status"] = "done"
        except Exception as e:
            logger.exception("主题识别异常")
            task["result"] = {"error": str(e)}
            task["status"] = "failed"

    background_tasks.add_task(_run)
    return {"task_id": task_id}


@router.post("/api/update-kg", response_class=JSONResponse)
def api_update_kg(background_tasks: BackgroundTasks, use_claude: bool = False):
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
            task["progress"] = "构建完成，正在运行推理引擎..."
            # 构建完成 → 自动跑推理
            try:
                inferred = _run_inference_sync(rule_type="all", auto_accept=True)
                task["result"]["inferred"] = len(inferred)
            except Exception as ie:
                logger.warning(f"构建后自动推理失败: {ie}")
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
    rule_type = body.get("rule_type", "all")
    task_id = f"kg_infer_{int(datetime.now().timestamp())}"
    _kg_tasks[task_id] = {"status": "running", "progress": "", "result": None}

    def _run():
        task = _kg_tasks[task_id]
        try:
            task["progress"] = "正在分析实体关系模式..."
            discovered = _run_inference_sync(rule_type=rule_type, auto_accept=False)
            task["result"] = {"discovered": discovered, "total": len(discovered)}
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
def api_task_status(task_id: str):
    task = _kg_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return task


# ── 深度语义校验 ──────────────────────────────────────────────────────────────

_kg_semantic_tasks = {}


@router.post("/api/kg-semantic-validate", response_class=JSONResponse)
def run_kg_semantic_validate(
    background_tasks: BackgroundTasks,
    dry_run: bool = False,
    entity_types: str = "",   # 逗号分隔，空则全量
):
    """触发 KG 深度语义校验"""
    task_id = f"kg_semantic_{int(datetime.now().timestamp())}"
    types_list = [t.strip() for t in entity_types.split(",") if t.strip()] or None
    _kg_semantic_tasks[task_id] = {
        "status": "running",
        "current_type": "",
        "type_idx": 0,
        "total_types": len(types_list) if types_list else 12,
        "batch_msg": "",
        "by_type": {},
        "result": None,
        "started_at": datetime.now().isoformat(),
    }

    def _run():
        task = _kg_semantic_tasks[task_id]
        try:
            from knowledge_graph.kg_semantic_validator import (
                validate_entities_by_type, VALIDATION_ORDER
            )
            types_to_check = types_list or VALIDATION_ORDER
            task["total_types"] = len(types_to_check)
            total_checked = total_invalid = total_deleted = 0

            for type_idx, entity_type in enumerate(types_to_check):
                task["type_idx"] = type_idx + 1
                task["current_type"] = entity_type
                task["batch_msg"] = f"开始校验 {entity_type}..."

                def _batch_cb(cur, tot, msg):
                    task["batch_msg"] = msg

                result = validate_entities_by_type(
                    entity_type,
                    dry_run=dry_run,
                    progress_callback=_batch_cb,
                )
                # 每种类型完成后立即写入，前端实时可见
                task["by_type"][entity_type] = {
                    "total": result["total"],
                    "invalid_count": result["invalid_count"],
                    "deleted_count": result["deleted_count"],
                    "invalid_names": result["invalid_names"][:30],
                }
                total_checked += result["total"]
                total_invalid += result["invalid_count"]
                total_deleted += result["deleted_count"]

            task["result"] = {
                "total_checked": total_checked,
                "total_invalid": total_invalid,
                "total_deleted": total_deleted,
            }
            task["status"] = "done"
        except Exception as e:
            task["status"] = "failed"
            task["result"] = {"error": str(e)}
            logger.error(f"KG语义校验失败: {e}")

    background_tasks.add_task(_run)
    return {"task_id": task_id}


@router.get("/api/kg-semantic-validate/{task_id}", response_class=JSONResponse)
def kg_semantic_validate_status(task_id: str):
    """查询语义校验任务状态"""
    task = _kg_semantic_tasks.get(task_id)
    if not task:
        return JSONResponse({"error": "任务不存在"}, status_code=404)
    return JSONResponse(task)


@router.get("/api/kg-semantic-latest", response_class=JSONResponse)
def kg_semantic_latest():
    """返回最近一次语义校验任务（running 或 done），供页面恢复用"""
    if not _kg_semantic_tasks:
        return JSONResponse({"task_id": None})
    # 按 started_at 倒序取最新
    latest_id = max(_kg_semantic_tasks, key=lambda k: _kg_semantic_tasks[k].get("started_at", ""))
    task = _kg_semantic_tasks[latest_id]
    return JSONResponse({"task_id": latest_id, "dry_run": False, **task})


@router.post("/api/extract-from-summary/{summary_id}", response_class=JSONResponse)
def api_extract_from_summary(summary_id: int):
    """从单条 content_summary 增补实体/关系到 KG（同步执行）"""
    try:
        from knowledge_graph.kg_inspector import extract_from_summary
        result = extract_from_summary(summary_id)
        return result
    except Exception as e:
        logger.error(f"extract_from_summary 异常: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


# ==================== KG 审核工作台 ====================

@router.get("/annotate", response_class=HTMLResponse)
def kg_annotate_page(request: Request, user: dict = Depends(get_current_user)):
    """KG 审核工作台页面"""
    ctx = _common_ctx("annotate")
    ctx["request"] = request
    ctx["current_user"] = {
        "user_id": user.user_id,
        "username": user.username,
        "role": user.role,
    }
    return templates.TemplateResponse("knowledge_graph.html", ctx)


@router.get("/api/review/stats", response_class=JSONResponse)
def api_review_stats(user: dict = Depends(require_annotator)):
    """审核统计卡片数据"""
    try:
        from knowledge_graph.kg_reviewer import get_review_stats
        return get_review_stats()
    except Exception as e:
        logger.error(f"review stats 失败: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/review/queue", response_class=JSONResponse)
def api_review_queue(
    target_type: str = "all",
    status: str = "all",
    entity_type: str = "",
    relation_type: str = "",
    keyword: str = "",
    limit: int = 50,
    offset: int = 0,
    user: dict = Depends(require_annotator),
):
    """获取审核队列（分页、筛选）"""
    try:
        from knowledge_graph.kg_reviewer import get_review_queue
        result = get_review_queue(
            target_type=target_type,
            status_filter=status,
            entity_type_filter=entity_type,
            relation_type_filter=relation_type,
            keyword=keyword,
            limit=limit,
            offset=offset,
        )
        # datetime 转字符串
        for item in result['items']:
            for k, v in list(item.items()):
                if hasattr(v, 'isoformat'):
                    item[k] = v.isoformat()
        return result
    except Exception as e:
        logger.error(f"review queue 失败: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/review/detail/{target_type}/{target_id}", response_class=JSONResponse)
def api_review_detail(target_type: str, target_id: int, user: dict = Depends(require_annotator)):
    """获取单条审核详情（包含关联 chunks + 审核历史）"""
    try:
        from knowledge_graph.kg_reviewer import (
            get_entity_chunks, get_relationship_chunks, get_review_log
        )
        detail: dict = {}

        if target_type == "entity":
            rows = execute_query(
                """SELECT id, entity_name, entity_type, review_status, review_note,
                          reviewed_by, reviewed_at, approved_by, approved_at,
                          description, properties_json, investment_logic, created_at
                   FROM kg_entities WHERE id = %s""",
                [target_id]
            )
            if rows:
                d = dict(rows[0])
                for k, v in d.items():
                    if hasattr(v, 'isoformat'):
                        d[k] = v.isoformat()
                detail['item'] = d
            detail['chunks'] = [_serialize_row(r) for r in get_entity_chunks(target_id)]
            # 关联关系
            rels = execute_query("""
                SELECT r.id, r.relation_type, r.review_status,
                       e1.entity_name AS src_name, e2.entity_name AS tgt_name
                FROM kg_relationships r
                JOIN kg_entities e1 ON r.source_entity_id = e1.id
                JOIN kg_entities e2 ON r.target_entity_id = e2.id
                WHERE r.source_entity_id = %s OR r.target_entity_id = %s
                LIMIT 20
            """, [target_id, target_id])
            detail['relations'] = [dict(r) for r in (rels or [])]
        else:
            rows = execute_query(
                """SELECT r.id, r.relation_type, r.review_status, r.review_note,
                          r.strength, r.confidence, r.direction, r.evidence,
                          r.reviewed_by, r.reviewed_at, r.approved_by, r.approved_at,
                          e1.entity_name AS src_name, e1.entity_type AS src_type,
                          e2.entity_name AS tgt_name, e2.entity_type AS tgt_type,
                          r.created_at
                   FROM kg_relationships r
                   JOIN kg_entities e1 ON r.source_entity_id = e1.id
                   JOIN kg_entities e2 ON r.target_entity_id = e2.id
                   WHERE r.id = %s""",
                [target_id]
            )
            if rows:
                d = dict(rows[0])
                for k, v in d.items():
                    if hasattr(v, 'isoformat'):
                        d[k] = v.isoformat()
                detail['item'] = d
            detail['chunks'] = [_serialize_row(r) for r in get_relationship_chunks(target_id)]

        detail['log'] = [_serialize_row(r) for r in get_review_log(target_type, target_id, limit=20)]
        return detail
    except Exception as e:
        logger.error(f"review detail 失败: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/review/mark-pending", response_class=JSONResponse)
async def api_mark_pending(request: Request, user: dict = Depends(require_annotator)):
    """data_admin 标记为 pending_approval"""
    try:
        from knowledge_graph.kg_reviewer import mark_pending
        body = await request.json()
        ok = mark_pending(
            target_type=body["target_type"],
            target_id=body["target_id"],
            user_id=user.user_id,
            user_role=user.role,
            note=body.get("note", ""),
        )
        return {"ok": ok}
    except Exception as e:
        logger.error(f"mark pending 失败: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/review/approve", response_class=JSONResponse)
async def api_approve(request: Request, user: dict = Depends(require_super_admin)):
    """super_admin 批准"""
    try:
        from knowledge_graph.kg_reviewer import approve
        body = await request.json()
        ok = approve(
            target_type=body["target_type"],
            target_id=body["target_id"],
            user_id=user.user_id,
            user_role=user.role,
            note=body.get("note", ""),
        )
        return {"ok": ok}
    except Exception as e:
        logger.error(f"approve 失败: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/review/reject", response_class=JSONResponse)
async def api_reject(request: Request, user: dict = Depends(require_super_admin)):
    """super_admin 驳回"""
    try:
        from knowledge_graph.kg_reviewer import reject
        body = await request.json()
        ok = reject(
            target_type=body["target_type"],
            target_id=body["target_id"],
            user_id=user.user_id,
            user_role=user.role,
            note=body.get("note", ""),
        )
        return {"ok": ok}
    except Exception as e:
        logger.error(f"reject 失败: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/review/revert", response_class=JSONResponse)
async def api_revert(request: Request, user: dict = Depends(require_super_admin)):
    """super_admin revert 到历史快照"""
    try:
        from knowledge_graph.kg_reviewer import revert
        body = await request.json()
        ok = revert(
            target_type=body["target_type"],
            target_id=body["target_id"],
            user_id=user.user_id,
            user_role=user.role,
            log_id=body.get("log_id"),
        )
        return {"ok": ok}
    except Exception as e:
        logger.error(f"revert 失败: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/review/batch", response_class=JSONResponse)
async def api_batch_review(request: Request, user: dict = Depends(require_super_admin)):
    """super_admin 批量审批/驳回"""
    try:
        from knowledge_graph.kg_reviewer import batch_approve
        body = await request.json()
        result = batch_approve(
            items=body["items"],
            user_id=user.user_id,
            user_role=user.role,
            action=body.get("action", "approve"),
            note=body.get("note", ""),
        )
        return result
    except Exception as e:
        logger.error(f"batch review 失败: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/review/edit-entity", response_class=JSONResponse)
async def api_edit_entity_review(request: Request, user: dict = Depends(require_annotator)):
    """编辑实体（改名/改类型/改描述），status → pending_approval"""
    try:
        from knowledge_graph.kg_reviewer import edit_entity
        body = await request.json()
        ok = edit_entity(
            entity_id=body["entity_id"],
            user_id=user.user_id,
            user_role=user.role,
            new_name=body.get("new_name"),
            new_type=body.get("new_type"),
            new_description=body.get("new_description"),
            note=body.get("note", ""),
        )
        return {"ok": ok}
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=422)
    except Exception as e:
        logger.error(f"edit entity 失败: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@router.post("/api/review/edit-relationship", response_class=JSONResponse)
async def api_edit_relationship_review(request: Request, user: dict = Depends(require_annotator)):
    """编辑关系属性，status → pending_approval"""
    try:
        from knowledge_graph.kg_reviewer import edit_relationship
        body = await request.json()
        ok = edit_relationship(
            rel_id=body["rel_id"],
            user_id=user.user_id,
            user_role=user.role,
            new_relation_type=body.get("new_relation_type"),
            new_strength=body.get("new_strength"),
            new_confidence=body.get("new_confidence"),
            new_direction=body.get("new_direction"),
            new_evidence=body.get("new_evidence"),
            note=body.get("note", ""),
        )
        return {"ok": ok}
    except Exception as e:
        logger.error(f"edit relationship 失败: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/review/log", response_class=JSONResponse)
def api_review_log(
    target_type: str = "",
    target_id: int = 0,
    limit: int = 50,
    user: dict = Depends(require_annotator),
):
    """获取审核历史日志"""
    try:
        from knowledge_graph.kg_reviewer import get_review_log
        logs = get_review_log(
            target_type=target_type or None,
            target_id=target_id or None,
            limit=limit,
        )
        return {"logs": [_serialize_row(r) for r in logs]}
    except Exception as e:
        logger.error(f"review log 失败: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


def _serialize_row(row: dict) -> dict:
    """将 DB 行中的 datetime/Decimal 转为 JSON 可序列化类型"""
    d = dict(row)
    for k, v in d.items():
        if hasattr(v, 'isoformat'):
            d[k] = v.isoformat()
        elif hasattr(v, '__float__'):
            d[k] = float(v)
    return d

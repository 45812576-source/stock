"""知识图谱查询 — 支持因果链遍历、子图提取、影响分析"""
import logging
from collections import deque
from utils.db_utils import execute_query

logger = logging.getLogger(__name__)


# ==================== 基础查询 ====================

def search_entities(keyword, entity_type=None, limit=20):
    """搜索实体"""
    sql = "SELECT * FROM kg_entities WHERE entity_name LIKE ?"
    params = [f"%{keyword}%"]
    if entity_type:
        sql += " AND entity_type=?"
        params.append(entity_type)
    sql += f" LIMIT {limit}"
    return execute_query(sql, params)


def explore_kg_graph(entity_name: str, depth: int = 2, relation_types: list = None,
                     max_nodes: int = 40) -> dict:
    """从指定实体出发，BFS遍历KG图，返回关系网络

    Args:
        entity_name: 起始实体名称（模糊匹配）
        depth: 遍历深度（1=直接邻居，2=邻居的邻居）
        relation_types: 只遍历这些关系类型（None=全部）
        max_nodes: 最大返回节点数

    Returns:
        {
            "root": {"id": ..., "name": ..., "type": ...},
            "nodes": [{"id", "name", "type"}],
            "edges": [{"source_name", "relation", "target_name", "strength", "direction"}],
            "by_relation": {"demand_source_of": [...], "supply_driven": [...], ...}
        }
    """
    # 找到起始实体
    roots = execute_query(
        "SELECT id, entity_name, entity_type FROM kg_entities WHERE entity_name LIKE ? LIMIT 10",
        [f"%{entity_name}%"],
    )
    if not roots:
        return {"root": None, "nodes": [], "edges": [], "by_relation": {},
                "message": f"未找到实体: {entity_name}"}

    # 优先级：精确匹配 > commodity/industry/theme 类型 > 其他
    _TYPE_PRIORITY = {"commodity": 0, "industry": 1, "industry_chain": 2,
                      "theme": 3, "macro_indicator": 4, "intermediate": 5}
    root = None
    for r in roots:
        if r["entity_name"] == entity_name:
            root = r
            break
    if not root:
        # 按类型优先级排序，commodity/industry 优先于 company
        roots.sort(key=lambda r: _TYPE_PRIORITY.get(r["entity_type"], 99))
        root = roots[0]

    # BFS 遍历
    visited_ids = {root["id"]}
    queue = deque([(root["id"], 0)])
    nodes = {root["id"]: {"id": root["id"], "name": root["entity_name"], "type": root["entity_type"]}}
    edges = []

    # 构建关系类型过滤
    rel_filter = ""
    rel_params = []
    if relation_types:
        placeholders = ",".join(["?" for _ in relation_types])
        rel_filter = f" AND r.relation_type IN ({placeholders})"
        rel_params = list(relation_types)

    while queue and len(nodes) < max_nodes:
        current_id, current_depth = queue.popleft()
        if current_depth >= depth:
            continue

        # 正向关系
        sql_fwd = f"""SELECT r.relation_type, r.strength, r.direction,
                             e.id as tid, e.entity_name as tname, e.entity_type as ttype
                      FROM kg_relationships r
                      JOIN kg_entities e ON r.target_entity_id = e.id
                      WHERE r.source_entity_id = ?{rel_filter}
                      LIMIT 30"""
        fwd = execute_query(sql_fwd, [current_id] + rel_params)

        # 反向关系
        sql_rev = f"""SELECT r.relation_type, r.strength, r.direction,
                             e.id as tid, e.entity_name as tname, e.entity_type as ttype
                      FROM kg_relationships r
                      JOIN kg_entities e ON r.source_entity_id = e.id
                      WHERE r.target_entity_id = ?{rel_filter}
                      LIMIT 30"""
        rev = execute_query(sql_rev, [current_id] + rel_params)

        src_name = nodes[current_id]["name"]

        for row in (fwd or []):
            tid = row["tid"]
            edges.append({
                "source_name": src_name,
                "relation": row["relation_type"],
                "target_name": row["tname"],
                "target_type": row["ttype"],
                "strength": row.get("strength"),
                "direction": row.get("direction"),
            })
            if tid not in visited_ids and len(nodes) < max_nodes:
                visited_ids.add(tid)
                nodes[tid] = {"id": tid, "name": row["tname"], "type": row["ttype"]}
                queue.append((tid, current_depth + 1))

        for row in (rev or []):
            tid = row["tid"]
            edges.append({
                "source_name": row["tname"],
                "relation": row["relation_type"],
                "target_name": src_name,
                "target_type": nodes[current_id]["type"],
                "strength": row.get("strength"),
                "direction": row.get("direction"),
            })
            if tid not in visited_ids and len(nodes) < max_nodes:
                visited_ids.add(tid)
                nodes[tid] = {"id": tid, "name": row["tname"], "type": row["ttype"]}
                queue.append((tid, current_depth + 1))

    # 按关系类型分组（方便 LLM 快速理解结构）
    by_relation = {}
    for e in edges:
        rt = e["relation"]
        if rt not in by_relation:
            by_relation[rt] = []
        by_relation[rt].append(f"{e['source_name']} → {e['target_name']}")

    return {
        "root": {"id": root["id"], "name": root["entity_name"], "type": root["entity_type"]},
        "nodes": list(nodes.values()),
        "edges": edges,
        "by_relation": by_relation,
        "node_count": len(nodes),
        "edge_count": len(edges),
    }


def find_path(source_id, target_id, max_depth=6):
    """查找两个实体之间的路径（BFS）"""
    queue = deque([(source_id, [source_id])])
    visited = {source_id}

    while queue:
        current, path = queue.popleft()
        if current == target_id:
            return path
        if len(path) >= max_depth:
            continue

        neighbors = execute_query(
            """SELECT target_entity_id as neighbor FROM kg_relationships WHERE source_entity_id=?
               UNION
               SELECT source_entity_id as neighbor FROM kg_relationships WHERE target_entity_id=?""",
            [current, current],
        )
        for n in neighbors:
            nid = n["neighbor"]
            if nid not in visited:
                visited.add(nid)
                queue.append((nid, path + [nid]))

    return None


def get_related_stocks(entity_name):
    """获取与某实体相关的所有股票"""
    entities = execute_query(
        "SELECT id FROM kg_entities WHERE entity_name LIKE ?", [f"%{entity_name}%"]
    )
    stocks = []
    for ent in entities:
        rels = execute_query(
            """SELECT ke.entity_name, ke.external_id as stock_code,
                      kr.strength, kr.direction, kr.relation_type
               FROM kg_relationships kr JOIN kg_entities ke ON kr.target_entity_id=ke.id
               WHERE kr.source_entity_id=? AND ke.entity_type='company'
               UNION
               SELECT ke.entity_name, ke.external_id as stock_code,
                      kr.strength, kr.direction, kr.relation_type
               FROM kg_relationships kr JOIN kg_entities ke ON kr.source_entity_id=ke.id
               WHERE kr.target_entity_id=? AND ke.entity_type='company'""",
            [ent["id"], ent["id"]],
        )
        stocks.extend(rels)
    return stocks


def get_theme_industries(tag_names):
    """从KG查找与标签关联的行业（轻量级，纯DB查询，不调Claude）

    路径: theme → (any relation) → industry
          industry → (any relation) → theme

    Returns:
        list of {"industry_name": str, "relation_type": str,
                 "strength": float, "direction": str}
    """
    if not tag_names:
        return []

    # 找到匹配的 theme 实体 ID（精确匹配优先，LIKE 兜底）
    theme_ids = []
    for tag in tag_names:
        rows = execute_query(
            "SELECT id FROM kg_entities WHERE entity_type='theme' AND entity_name=?",
            [tag],
        )
        if not rows:
            rows = execute_query(
                "SELECT id FROM kg_entities WHERE entity_type='theme' AND entity_name LIKE ? LIMIT 3",
                [f"%{tag}%"],
            )
        theme_ids.extend(r["id"] for r in rows)

    if not theme_ids:
        return []

    theme_ids = list(set(theme_ids))
    ph = ",".join(["?"] * len(theme_ids))

    industries = execute_query(
        f"""SELECT DISTINCT ke.entity_name AS industry_name,
                   kr.relation_type, kr.strength, kr.direction
            FROM kg_relationships kr
            JOIN kg_entities ke ON ke.entity_type='industry' AND (
                (kr.target_entity_id = ke.id AND kr.source_entity_id IN ({ph}))
                OR
                (kr.source_entity_id = ke.id AND kr.target_entity_id IN ({ph}))
            )
            ORDER BY kr.strength DESC""",
        theme_ids + theme_ids,
    )
    return [dict(r) for r in industries] if industries else []


# ==================== 因果链遍历 ====================

CAUSAL_TYPES = {
    "causes_positive", "causes_negative", "cost_transmission",
    "indicator_transmission", "demand_driven", "supply_driven",
    "demand_source_of", "demand_substitute",
    "benefits", "hurts", "risk_factor", "catalyst",
    "cost_affected_by", "revenue_affected_by",
}


def trace_causal_chain(entity_id, max_depth=4, direction="downstream"):
    """沿因果关系链遍历，返回传导路径

    Args:
        entity_id: 起始实体 ID
        max_depth: 最大遍历深度
        direction: downstream=沿因果方向往下游走, upstream=往上游追溯

    Returns:
        list of chains, 每条 chain 是 [{"entity": {...}, "relation": {...}}, ...]
    """
    chains = []
    visited = set()

    def _dfs(eid, current_chain, depth):
        if depth >= max_depth or eid in visited:
            if len(current_chain) > 1:
                chains.append(list(current_chain))
            return

        visited.add(eid)

        if direction == "downstream":
            rels = execute_query(
                """SELECT kr.*, ke.id as tgt_id, ke.entity_name as tgt_name,
                          ke.entity_type as tgt_type
                   FROM kg_relationships kr
                   JOIN kg_entities ke ON kr.target_entity_id = ke.id
                   WHERE kr.source_entity_id=? AND kr.relation_type IN ({})
                   ORDER BY kr.strength DESC""".format(
                    ",".join(["?"] * len(CAUSAL_TYPES))
                ),
                [eid] + list(CAUSAL_TYPES),
            )
            next_field = "tgt_id"
        else:
            rels = execute_query(
                """SELECT kr.*, ke.id as src_id, ke.entity_name as src_name,
                          ke.entity_type as src_type
                   FROM kg_relationships kr
                   JOIN kg_entities ke ON kr.source_entity_id = ke.id
                   WHERE kr.target_entity_id=? AND kr.relation_type IN ({})
                   ORDER BY kr.strength DESC""".format(
                    ",".join(["?"] * len(CAUSAL_TYPES))
                ),
                [eid] + list(CAUSAL_TYPES),
            )
            next_field = "src_id"

        if not rels:
            if len(current_chain) > 1:
                chains.append(list(current_chain))
            visited.discard(eid)
            return

        for rel in rels[:5]:  # 每层最多展开 5 条
            next_id = rel[next_field]
            current_chain.append(rel)
            _dfs(next_id, current_chain, depth + 1)
            current_chain.pop()

        visited.discard(eid)

    entity = execute_query("SELECT * FROM kg_entities WHERE id=?", [entity_id])
    if not entity:
        return []

    _dfs(entity_id, [{"entity": entity[0]}], 0)
    return chains


# ==================== 子图提取（给 LLM 用） ====================

def extract_context_subgraph(entity_ids, depth=2, categories=None, max_nodes=50):
    """提取多个实体周围的子图，序列化为 LLM 可读文本

    Args:
        entity_ids: 实体 ID 列表
        depth: 遍历深度
        categories: 关系类别过滤 (causal/structural/element/policy/indicator)
        max_nodes: 最大节点数

    Returns:
        dict with "nodes", "edges", "text" (LLM-readable)
    """
    visited_nodes = set()
    nodes = {}
    edges = []

    def _collect(eid, d):
        if d <= 0 or eid in visited_nodes or len(visited_nodes) >= max_nodes:
            return
        visited_nodes.add(eid)

        ent = execute_query("SELECT * FROM kg_entities WHERE id=?", [eid])
        if not ent:
            return
        nodes[eid] = ent[0]

        # 查询关系
        cat_filter = ""
        params = [eid, eid]
        if categories:
            placeholders = ",".join(["?"] * len(categories))
            cat_filter = f" AND kr.relation_category IN ({placeholders})"
            params = [eid] + list(categories) + [eid] + list(categories)

        rels = execute_query(
            f"""SELECT kr.*, 'outgoing' as dir,
                       ke.id as other_id, ke.entity_name as other_name, ke.entity_type as other_type
                FROM kg_relationships kr
                JOIN kg_entities ke ON kr.target_entity_id = ke.id
                WHERE kr.source_entity_id=? {cat_filter}
                UNION ALL
                SELECT kr.*, 'incoming' as dir,
                       ke.id as other_id, ke.entity_name as other_name, ke.entity_type as other_type
                FROM kg_relationships kr
                JOIN kg_entities ke ON kr.source_entity_id = ke.id
                WHERE kr.target_entity_id=? {cat_filter}
                ORDER BY strength DESC LIMIT 20""",
            params,
        )

        for rel in rels:
            edges.append(rel)
            _collect(rel["other_id"], d - 1)

    for eid in entity_ids:
        _collect(eid, depth)

    # 序列化为 LLM 可读文本
    text_lines = ["## 知识图谱上下文\n"]

    # 节点
    text_lines.append("### 实体")
    for nid, n in nodes.items():
        desc = f" — {n['description'][:80]}" if n.get("description") else ""
        text_lines.append(f"- [{n['entity_type']}] {n['entity_name']}{desc}")

    # 边
    text_lines.append("\n### 关系")
    seen_edges = set()
    for e in edges:
        src_name = nodes.get(e.get("source_entity_id"), {}).get("entity_name", "?")
        tgt_name = e.get("other_name", "?")
        if e["dir"] == "incoming":
            src_name, tgt_name = tgt_name, nodes.get(e.get("target_entity_id"), {}).get("entity_name", "?")

        edge_key = (src_name, e["relation_type"], tgt_name)
        if edge_key in seen_edges:
            continue
        seen_edges.add(edge_key)

        extras = []
        if e.get("direction") and e["direction"] != "neutral":
            extras.append(e["direction"])
        if e.get("percentage"):
            extras.append(f"占比{e['percentage']}%")
        if e.get("time_lag"):
            extras.append(f"时滞:{e['time_lag']}")
        extra_str = f" [{', '.join(extras)}]" if extras else ""

        text_lines.append(f"- {src_name} --[{e['relation_type']}]--> {tgt_name}{extra_str}")

    return {
        "nodes": list(nodes.values()),
        "edges": edges,
        "text": "\n".join(text_lines),
    }


# ==================== 影响分析 ====================

def impact_analysis(event_entity_id, max_depth=3):
    """给定事件/政策实体，追踪所有受影响的行业和公司

    Returns:
        dict with "affected_industries", "affected_companies", "chains"
    """
    chains = trace_causal_chain(event_entity_id, max_depth=max_depth, direction="downstream")

    affected_industries = {}
    affected_companies = {}

    # 也查直接关系（benefits/hurts/risk_factor/catalyst）
    direct_rels = execute_query(
        """SELECT kr.*, ke.entity_name, ke.entity_type, ke.external_id
           FROM kg_relationships kr
           JOIN kg_entities ke ON kr.target_entity_id = ke.id
           WHERE kr.source_entity_id=?
           AND kr.relation_type IN ('benefits','hurts','risk_factor','catalyst',
                                     'causes_positive','causes_negative')""",
        [event_entity_id],
    )

    for rel in direct_rels:
        entry = {
            "name": rel["entity_name"],
            "relation": rel["relation_type"],
            "direction": rel.get("direction", "neutral"),
            "strength": rel.get("strength", 0.5),
            "evidence": rel.get("evidence", ""),
        }
        if rel["entity_type"] == "industry":
            affected_industries[rel["entity_name"]] = entry
        elif rel["entity_type"] == "company":
            entry["stock_code"] = rel.get("external_id")
            affected_companies[rel["entity_name"]] = entry

    # 从因果链中提取
    for chain in chains:
        for step in chain:
            if isinstance(step, dict) and "entity" in step:
                continue
            etype = step.get("tgt_type", "")
            ename = step.get("tgt_name", "")
            if etype == "industry" and ename not in affected_industries:
                affected_industries[ename] = {
                    "name": ename,
                    "relation": step.get("relation_type", ""),
                    "direction": step.get("direction", "neutral"),
                    "strength": step.get("strength", 0.5),
                    "via_chain": True,
                }
            elif etype == "company" and ename not in affected_companies:
                affected_companies[ename] = {
                    "name": ename,
                    "relation": step.get("relation_type", ""),
                    "direction": step.get("direction", "neutral"),
                    "strength": step.get("strength", 0.5),
                    "via_chain": True,
                }

    # 对于受影响的行业，查找其下属公司
    for ind_name in list(affected_industries.keys()):
        ind_entities = execute_query(
            "SELECT id FROM kg_entities WHERE entity_type='industry' AND entity_name=%s",
            [ind_name],
        )
        if not ind_entities:
            continue
        ind_stocks = execute_query(
            """SELECT ke.entity_name, ke.external_id
               FROM kg_relationships kr
               JOIN kg_entities ke ON kr.source_entity_id = ke.id
               WHERE kr.target_entity_id=%s AND kr.relation_type='belongs_to_industry'
               AND ke.entity_type='company'
               ORDER BY kr.strength DESC LIMIT 20""",
            [ind_entities[0]["id"]],
        )
        affected_industries[ind_name]["top_stocks"] = [
            {"name": s["entity_name"], "stock_code": s.get("external_id")}
            for s in ind_stocks
        ]

    return {
        "affected_industries": list(affected_industries.values()),
        "affected_companies": list(affected_companies.values()),
        "chains": chains,
    }


# ==================== 公司上下文查询 ====================

def search_content_summaries(keywords: list, periods: list = None, limit: int = 10) -> list:
    """在 content_summaries 表中搜索包含关键词的内容摘要

    同时搜索 summary 和 fact_summary 列，提高召回率。

    Args:
        keywords: 搜索关键词列表，多个关键词取 OR
        periods: 时间期间过滤列表，如 ['2026', '2027']（匹配文本）
        limit: 返回结果数量上限

    Returns:
        list of dicts with keys: id, summary_title, summary, fact_summary, family, created_at
    """
    if not keywords:
        return []

    # 构建 LIKE 条件（关键词之间 OR，同时搜 summary 和 fact_summary）
    keyword_clauses = " OR ".join(
        ["summary LIKE ?" for _ in keywords] +
        ["fact_summary LIKE ?" for _ in keywords]
    )
    params = [f"%{kw}%" for kw in keywords] * 2

    # 如果有 periods，追加 AND (period OR period)（同时匹配两列）
    period_sql = ""
    if periods:
        period_clauses = " OR ".join(
            ["summary LIKE ?" for _ in periods] +
            ["fact_summary LIKE ?" for _ in periods]
        )
        period_sql = f" AND ({period_clauses})"
        params += [f"%{p}%" for p in periods] * 2

    sql = f"""SELECT id, LEFT(summary, 80) AS summary_title, summary,
                     fact_summary, family, created_at
              FROM content_summaries
              WHERE ({keyword_clauses}){period_sql}
              ORDER BY created_at DESC
              LIMIT {limit}"""
    try:
        rows = execute_query(sql, params)
        return [dict(r) for r in rows] if rows else []
    except Exception as e:
        logger.warning(f"search_content_summaries 查询失败: {e}")
        return []


def get_company_context(stock_code_or_name):
    """获取公司的完整 KG 上下文（行业归属、成本结构、关联主题）

    Returns:
        dict with "entity", "industries", "cost_elements", "revenue_elements",
                  "themes", "supply_chain", "text"
    """
    # 查找公司实体
    entities = execute_query(
        """SELECT * FROM kg_entities
           WHERE entity_type='company'
           AND (entity_name LIKE ? OR external_id=?)""",
        [f"%{stock_code_or_name}%", stock_code_or_name],
    )
    if not entities:
        return None

    ent = entities[0]
    eid = ent["id"]

    # 行业归属
    industries = execute_query(
        """SELECT ke.entity_name, kr.strength
           FROM kg_relationships kr
           JOIN kg_entities ke ON kr.target_entity_id = ke.id
           WHERE kr.source_entity_id=? AND kr.relation_type='belongs_to_industry'""",
        [eid],
    )

    # 成本要素
    cost_elements = execute_query(
        """SELECT ke.entity_name, kr.percentage, kr.evidence
           FROM kg_relationships kr
           JOIN kg_entities ke ON kr.target_entity_id = ke.id
           WHERE kr.source_entity_id=? AND kr.relation_type='major_cost_item'
           ORDER BY kr.percentage DESC""",
        [eid],
    )

    # 收入要素
    revenue_elements = execute_query(
        """SELECT ke.entity_name, kr.percentage, kr.evidence
           FROM kg_relationships kr
           JOIN kg_entities ke ON kr.target_entity_id = ke.id
           WHERE kr.source_entity_id=? AND kr.relation_type='major_revenue_item'
           ORDER BY kr.percentage DESC""",
        [eid],
    )

    # 关联主题
    themes = execute_query(
        """SELECT ke.entity_name, kr.direction, kr.strength
           FROM kg_relationships kr
           JOIN kg_entities ke ON kr.target_entity_id = ke.id
           WHERE kr.source_entity_id=? AND ke.entity_type='theme'
           UNION
           SELECT ke.entity_name, kr.direction, kr.strength
           FROM kg_relationships kr
           JOIN kg_entities ke ON kr.source_entity_id = ke.id
           WHERE kr.target_entity_id=? AND ke.entity_type='theme'""",
        [eid, eid],
    )

    # 供应链关系
    supply_chain = execute_query(
        """SELECT ke.entity_name, kr.relation_type, kr.direction, kr.strength, 'downstream' as chain_dir
           FROM kg_relationships kr
           JOIN kg_entities ke ON kr.target_entity_id = ke.id
           WHERE kr.source_entity_id=?
           AND kr.relation_type IN ('supplier_of','customer_of','competitor','substitute_threat')
           UNION
           SELECT ke.entity_name, kr.relation_type, kr.direction, kr.strength, 'upstream' as chain_dir
           FROM kg_relationships kr
           JOIN kg_entities ke ON kr.source_entity_id = ke.id
           WHERE kr.target_entity_id=?
           AND kr.relation_type IN ('supplier_of','customer_of','competitor','substitute_threat')""",
        [eid, eid],
    )

    # 序列化为文本
    lines = [f"## {ent['entity_name']} 知识图谱上下文\n"]
    if ent.get("description"):
        lines.append(f"主营业务: {ent['description'][:200]}\n")

    if industries:
        lines.append("所属行业: " + ", ".join(i["entity_name"] for i in industries))

    if cost_elements:
        lines.append("\n成本结构:")
        for c in cost_elements:
            pct = f" ({c['percentage']}%)" if c.get("percentage") else ""
            lines.append(f"  - {c['entity_name']}{pct}")

    if revenue_elements:
        lines.append("\n收入结构:")
        for r in revenue_elements:
            pct = f" ({r['percentage']}%)" if r.get("percentage") else ""
            lines.append(f"  - {r['entity_name']}{pct}")

    if themes:
        lines.append("\n关联主题: " + ", ".join(
            f"{t['entity_name']}({'利好' if t.get('direction')=='positive' else '利空' if t.get('direction')=='negative' else '中性'})"
            for t in themes
        ))

    if supply_chain:
        lines.append("\n供应链关系:")
        for s in supply_chain:
            lines.append(f"  - {s['relation_type']}: {s['entity_name']}")

    return {
        "entity": ent,
        "industries": industries,
        "cost_elements": cost_elements,
        "revenue_elements": revenue_elements,
        "themes": themes,
        "supply_chain": supply_chain,
        "text": "\n".join(lines),
    }

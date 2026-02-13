"""知识图谱CRUD操作 — 完整版：增删改查 + 变更日志 + 统计"""
import json
import logging
from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)


# ========== 实体操作 ==========

def add_entity(entity_type, entity_name, description=None, properties=None, investment_logic=None):
    """添加实体（去重），返回实体ID"""
    # 先检查是否已存在
    existing = execute_query(
        "SELECT id FROM kg_entities WHERE entity_type=? AND entity_name=?",
        [entity_type, entity_name],
    )
    if existing:
        return existing[0]["id"]

    eid = execute_insert(
        """INSERT OR IGNORE INTO kg_entities (entity_type, entity_name, description, properties_json, investment_logic)
           VALUES (?, ?, ?, ?, ?)""",
        [entity_type, entity_name, description,
         json.dumps(properties, ensure_ascii=False) if properties else None,
         investment_logic],
    )
    if eid:
        _log_change(entity_id=eid, action="create",
                    new_value={"type": entity_type, "name": entity_name},
                    source="system")
    return eid


def get_entity_by_id(entity_id):
    """按ID获取实体"""
    rows = execute_query("SELECT * FROM kg_entities WHERE id=?", [entity_id])
    return rows[0] if rows else None


def update_entity(entity_id, description=None, properties=None, investment_logic=None):
    """更新实体信息"""
    old = get_entity_by_id(entity_id)
    if not old:
        return False
    updates, params = [], []
    if description is not None:
        updates.append("description=?")
        params.append(description)
    if properties is not None:
        updates.append("properties_json=?")
        params.append(json.dumps(properties, ensure_ascii=False))
    if investment_logic is not None:
        updates.append("investment_logic=?")
        params.append(investment_logic)
    if not updates:
        return False
    updates.append("updated_at=CURRENT_TIMESTAMP")
    params.append(entity_id)
    execute_insert(f"UPDATE kg_entities SET {', '.join(updates)} WHERE id=?", params)
    _log_change(entity_id=entity_id, action="update",
                old_value={"description": old.get("description")},
                new_value={"description": description},
                source="manual")
    return True


def delete_entity(entity_id):
    """删除实体及其所有关系"""
    old = get_entity_by_id(entity_id)
    if not old:
        return False
    execute_insert("DELETE FROM kg_relationships WHERE source_entity_id=? OR target_entity_id=?",
                   [entity_id, entity_id])
    execute_insert("DELETE FROM kg_entities WHERE id=?", [entity_id])
    _log_change(entity_id=entity_id, action="delete",
                old_value={"type": old["entity_type"], "name": old["entity_name"]},
                source="manual")
    return True


def find_entity(name):
    """按名称模糊查找实体"""
    return execute_query(
        "SELECT * FROM kg_entities WHERE entity_name LIKE ?", [f"%{name}%"]
    )


def get_all_entities(entity_type=None, limit=100, offset=0):
    """获取实体列表（分页+类型过滤）"""
    sql = "SELECT * FROM kg_entities"
    params = []
    if entity_type:
        sql += " WHERE entity_type=?"
        params.append(entity_type)
    sql += " ORDER BY updated_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    return execute_query(sql, params)


def get_entity_count(entity_type=None):
    """获取实体数量"""
    sql = "SELECT COUNT(*) as cnt FROM kg_entities"
    params = []
    if entity_type:
        sql += " WHERE entity_type=?"
        params.append(entity_type)
    rows = execute_query(sql, params)
    return rows[0]["cnt"] if rows else 0


# ========== 关系操作 ==========

def add_relationship(source_id, target_id, relation_type, strength=0.5,
                     direction="positive", evidence=None, confidence=0.5):
    """添加关系（检查重复）"""
    existing = execute_query(
        """SELECT id FROM kg_relationships
           WHERE source_entity_id=? AND target_entity_id=? AND relation_type=?""",
        [source_id, target_id, relation_type],
    )
    if existing:
        # 更新已有关系的强度（取较大值）
        execute_insert(
            """UPDATE kg_relationships SET strength=MAX(strength, ?),
               confidence=MAX(confidence, ?), updated_at=CURRENT_TIMESTAMP
               WHERE id=?""",
            [strength, confidence, existing[0]["id"]],
        )
        return existing[0]["id"]

    rid = execute_insert(
        """INSERT INTO kg_relationships
           (source_entity_id, target_entity_id, relation_type, strength, direction, evidence, confidence)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [source_id, target_id, relation_type, strength, direction, evidence, confidence],
    )
    if rid:
        _log_change(relationship_id=rid, action="create",
                    new_value={"source": source_id, "target": target_id, "type": relation_type},
                    source="system")
    return rid


def update_relationship(rel_id, strength=None, direction=None, confidence=None):
    """更新关系属性"""
    updates, params = [], []
    if strength is not None:
        updates.append("strength=?")
        params.append(strength)
    if direction is not None:
        updates.append("direction=?")
        params.append(direction)
    if confidence is not None:
        updates.append("confidence=?")
        params.append(confidence)
    if not updates:
        return False
    updates.append("updated_at=CURRENT_TIMESTAMP")
    params.append(rel_id)
    execute_insert(f"UPDATE kg_relationships SET {', '.join(updates)} WHERE id=?", params)
    return True


def delete_relationship(rel_id):
    """删除关系"""
    execute_insert("DELETE FROM kg_relationships WHERE id=?", [rel_id])
    _log_change(relationship_id=rel_id, action="delete", source="manual")
    return True


def get_entity_relations(entity_id):
    """获取实体的所有关系"""
    outgoing = execute_query(
        """SELECT kr.*, ke.entity_name as target_name, ke.entity_type as target_type
           FROM kg_relationships kr JOIN kg_entities ke ON kr.target_entity_id=ke.id
           WHERE kr.source_entity_id=?""",
        [entity_id],
    )
    incoming = execute_query(
        """SELECT kr.*, ke.entity_name as source_name, ke.entity_type as source_type
           FROM kg_relationships kr JOIN kg_entities ke ON kr.source_entity_id=ke.id
           WHERE kr.target_entity_id=?""",
        [entity_id],
    )
    return {"outgoing": outgoing, "incoming": incoming}


def get_subgraph(entity_id, depth=2):
    """获取以entity_id为中心的子图"""
    visited = set()
    nodes = []
    edges = []

    def _traverse(eid, d):
        if d <= 0 or eid in visited:
            return
        visited.add(eid)
        ent = get_entity_by_id(eid)
        if ent:
            nodes.append(ent)
        rels = get_entity_relations(eid)
        for r in rels["outgoing"]:
            edges.append(r)
            _traverse(r["target_entity_id"], d - 1)
        for r in rels["incoming"]:
            edges.append(r)
            _traverse(r["source_entity_id"], d - 1)

    _traverse(entity_id, depth)
    return {"nodes": nodes, "edges": edges}


# ========== 统计 ==========

def get_kg_stats():
    """获取知识图谱统计信息"""
    entity_counts = execute_query(
        "SELECT entity_type, COUNT(*) as cnt FROM kg_entities GROUP BY entity_type"
    )
    rel_counts = execute_query(
        "SELECT relation_type, COUNT(*) as cnt FROM kg_relationships GROUP BY relation_type"
    )
    total_entities = execute_query("SELECT COUNT(*) as cnt FROM kg_entities")
    total_rels = execute_query("SELECT COUNT(*) as cnt FROM kg_relationships")
    recent_updates = execute_query(
        "SELECT * FROM kg_update_log ORDER BY updated_at DESC LIMIT 10"
    )
    return {
        "total_entities": total_entities[0]["cnt"] if total_entities else 0,
        "total_relationships": total_rels[0]["cnt"] if total_rels else 0,
        "entity_by_type": {r["entity_type"]: r["cnt"] for r in entity_counts},
        "rel_by_type": {r["relation_type"]: r["cnt"] for r in rel_counts},
        "recent_updates": recent_updates,
    }


# ========== 变更日志 ==========

def _log_change(entity_id=None, relationship_id=None, action="create",
                old_value=None, new_value=None, source="system"):
    """记录变更日志"""
    try:
        execute_insert(
            """INSERT INTO kg_update_log (entity_id, relationship_id, action,
               old_value_json, new_value_json, source)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [entity_id, relationship_id, action,
             json.dumps(old_value, ensure_ascii=False) if old_value else None,
             json.dumps(new_value, ensure_ascii=False) if new_value else None,
             source],
        )
    except Exception as e:
        logger.warning(f"记录变更日志失败: {e}")


def get_update_log(limit=50):
    """获取变更日志"""
    return execute_query(
        "SELECT * FROM kg_update_log ORDER BY updated_at DESC LIMIT ?", [limit]
    )

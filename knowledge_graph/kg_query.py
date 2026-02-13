"""知识图谱查询"""
from utils.db_utils import execute_query


def search_entities(keyword, entity_type=None, limit=20):
    """搜索实体"""
    sql = "SELECT * FROM kg_entities WHERE entity_name LIKE ?"
    params = [f"%{keyword}%"]
    if entity_type:
        sql += " AND entity_type=?"
        params.append(entity_type)
    sql += f" LIMIT {limit}"
    return execute_query(sql, params)


def find_path(source_id, target_id, max_depth=4):
    """查找两个实体之间的路径（BFS）"""
    from collections import deque
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
            """SELECT ke.entity_name, kr.strength, kr.direction
               FROM kg_relationships kr JOIN kg_entities ke ON kr.target_entity_id=ke.id
               WHERE kr.source_entity_id=? AND ke.entity_type='company'
               UNION
               SELECT ke.entity_name, kr.strength, kr.direction
               FROM kg_relationships kr JOIN kg_entities ke ON kr.source_entity_id=ke.id
               WHERE kr.target_entity_id=? AND ke.entity_type='company'""",
            [ent["id"], ent["id"]],
        )
        stocks.extend(rels)
    return stocks

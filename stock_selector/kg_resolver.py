"""kg_resolver — KG 多跳查询，从关键词解析出候选股票池

Phase 1: LIKE 匹配锚点实体
Phase 2: 1跳直接关联 company + 2跳 via 中间实体
Phase 3: 打分去重，多路径命中得分更高
"""
import logging
from utils.db_utils import execute_query

logger = logging.getLogger(__name__)

# 1跳直接受益关系
DIRECT_BENEFIT_RELATIONS = (
    "benefits", "catalyst", "causes_positive", "revenue_affected_by",
    "demand_driven", "demand_source_of",
)
# 2跳中间实体类型
MID_ENTITY_TYPES = ("industry", "theme", "industry_chain")
# 2跳到 company 的关系
COMPANY_ATTACH_RELATIONS = ("belongs_to_industry", "benefits", "catalyst", "causes_positive")


def _match_anchors(keyword: str, entity_types: list[str]) -> list[dict]:
    """LIKE 匹配锚点实体，精确匹配优先"""
    kw4 = keyword[:4]
    # 精确匹配
    if entity_types:
        ph = ",".join(["%s"] * len(entity_types))
        rows = execute_query(
            f"SELECT id, entity_name, entity_type FROM kg_entities WHERE entity_name=%s AND entity_type IN ({ph}) LIMIT 5",
            [keyword] + entity_types,
        )
        if not rows:
            rows = execute_query(
                f"SELECT id, entity_name, entity_type FROM kg_entities WHERE entity_name LIKE %s AND entity_type IN ({ph}) LIMIT 10",
                [f"%{kw4}%"] + entity_types,
            )
    else:
        rows = execute_query(
            "SELECT id, entity_name, entity_type FROM kg_entities WHERE entity_name=%s LIMIT 5",
            [keyword],
        )
        if not rows:
            rows = execute_query(
                "SELECT id, entity_name, entity_type FROM kg_entities WHERE entity_name LIKE %s LIMIT 10",
                [f"%{kw4}%"],
            )
    return [dict(r) for r in (rows or [])]


def _get_direct_companies(anchor_id: int, relation_hint: str) -> list[dict]:
    """1跳：锚点 → company（直接关系）"""
    relations = list(DIRECT_BENEFIT_RELATIONS)
    if relation_hint and relation_hint not in relations:
        relations.insert(0, relation_hint)
    ph = ",".join(["%s"] * len(relations))

    # source→target 方向
    rows1 = execute_query(
        f"""SELECT ke.external_id, ke.entity_name, kr.relation_type, kr.strength
            FROM kg_relationships kr
            JOIN kg_entities ke ON kr.target_entity_id = ke.id
            WHERE kr.source_entity_id = %s
              AND kr.relation_type IN ({ph})
              AND ke.entity_type = 'company'""",
        [anchor_id] + relations,
    )
    # target→source 方向（company benefits from anchor）
    rows2 = execute_query(
        f"""SELECT ke.external_id, ke.entity_name, kr.relation_type, kr.strength
            FROM kg_relationships kr
            JOIN kg_entities ke ON kr.source_entity_id = ke.id
            WHERE kr.target_entity_id = %s
              AND kr.relation_type IN ({ph})
              AND ke.entity_type = 'company'""",
        [anchor_id] + relations,
    )
    return [dict(r) for r in (rows1 or [])] + [dict(r) for r in (rows2 or [])]


def _get_mid_entities(anchor_id: int) -> list[dict]:
    """获取锚点的中间实体（industry/theme/industry_chain）"""
    ph = ",".join(["%s"] * len(MID_ENTITY_TYPES))
    rows = execute_query(
        f"""SELECT DISTINCT ke.id, ke.entity_name, ke.entity_type
            FROM kg_relationships kr
            JOIN kg_entities ke ON (
                (kr.target_entity_id = ke.id AND kr.source_entity_id = %s)
                OR (kr.source_entity_id = ke.id AND kr.target_entity_id = %s)
            )
            WHERE ke.entity_type IN ({ph}) AND ke.id != %s
            LIMIT 10""",
        [anchor_id, anchor_id] + list(MID_ENTITY_TYPES) + [anchor_id],
    )
    return [dict(r) for r in (rows or [])]


def _get_companies_via_mid(mid_id: int) -> list[dict]:
    """2跳：中间实体 → company"""
    ph = ",".join(["%s"] * len(COMPANY_ATTACH_RELATIONS))
    rows = execute_query(
        f"""SELECT ke.external_id, ke.entity_name, kr.relation_type, kr.strength
            FROM kg_relationships kr
            JOIN kg_entities ke ON kr.source_entity_id = ke.id
            WHERE kr.target_entity_id = %s
              AND kr.relation_type IN ({ph})
              AND ke.entity_type = 'company'
            LIMIT 50""",
        [mid_id] + list(COMPANY_ATTACH_RELATIONS),
    )
    return [dict(r) for r in (rows or [])]


def kg_resolve(keywords: list[str], entity_types: list[str], relation_hint: str) -> list[dict]:
    """
    多跳查询，返回候选股票列表，按命中分数降序。

    Returns:
        list of {
            "code": str,
            "name": str,
            "score": float,       # 命中路径数 × 强度加权
            "paths": list[str],   # 命中路径描述
        }
    """
    company_scores: dict[str, dict] = {}  # code → {score, name, paths}

    for kw in keywords:
        anchors = _match_anchors(kw, entity_types)
        if not anchors:
            logger.debug(f"kg_resolve: no anchor for keyword '{kw}'")
            continue

        for anchor in anchors:
            aid = anchor["id"]
            aname = anchor["entity_name"]

            # 1跳
            for comp in _get_direct_companies(aid, relation_hint):
                code = comp.get("external_id") or comp.get("entity_name")
                if not code:
                    continue
                path = f"{kw}→{aname}→[{comp['relation_type']}]→{comp['entity_name']}"
                strength = comp.get("strength") or 0.5
                _add_score(company_scores, code, comp["entity_name"], strength * 1.0, path)

            # 2跳
            for mid in _get_mid_entities(aid):
                mid_name = mid["entity_name"]
                for comp in _get_companies_via_mid(mid["id"]):
                    code = comp.get("external_id") or comp.get("entity_name")
                    if not code:
                        continue
                    path = f"{kw}→{aname}→{mid_name}→[{comp['relation_type']}]→{comp['entity_name']}"
                    strength = comp.get("strength") or 0.3
                    _add_score(company_scores, code, comp["entity_name"], strength * 0.6, path)

    # 排序
    result = sorted(company_scores.values(), key=lambda x: x["score"], reverse=True)
    logger.info(f"kg_resolve: {len(keywords)} keywords → {len(result)} candidates")
    return result


def _add_score(scores: dict, code: str, name: str, delta: float, path: str):
    if code not in scores:
        scores[code] = {"code": code, "name": name, "score": 0.0, "paths": []}
    scores[code]["score"] += delta
    if len(scores[code]["paths"]) < 5:
        scores[code]["paths"].append(path)

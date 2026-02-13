"""从清洗数据更新知识图谱 — Claude智能提取实体和关系"""
import json
import logging
from datetime import datetime
from utils.db_utils import execute_query, execute_insert
from utils.claude_client import call_claude_json
from knowledge_graph.kg_manager import (
    add_entity, add_relationship, find_entity, get_entity_count,
)

logger = logging.getLogger(__name__)

KG_EXTRACTION_PROMPT = """你是知识图谱构建专家。请从以下金融资讯中提取实体和关系。

实体类型：
- macro_factor: 宏观因素（如：GDP增速、CPI、利率、汇率、政策）
- industry: 行业（如：半导体、新能源、医药）
- company: 公司（如：宁德时代、比亚迪）
- theme: 主题/概念（如：AI、碳中和、国产替代）
- indicator: 指标（如：PMI、社融、M2）

关系类型：
- impacts: A影响B（如：利率上升影响银行股）
- belongs_to: A属于B（如：宁德时代属于新能源行业）
- competes: A与B竞争
- supplies: A供应B
- benefits: A受益于B
- related: A与B相关

请输出JSON格式：
{
  "entities": [
    {"type": "实体类型", "name": "实体名称", "description": "简短描述"}
  ],
  "relationships": [
    {"source": "源实体名", "target": "目标实体名",
     "type": "关系类型", "strength": 0.1-1.0,
     "direction": "positive/negative/neutral",
     "evidence": "关系依据（一句话）"}
  ]
}

注意：
- 只提取明确的、有价值的实体和关系
- 公司名用全称
- strength表示关系强度，重大影响用0.8+，一般关联用0.3-0.5
- 每条资讯提取不超过5个实体、5个关系"""


def update_from_cleaned_items(since_date=None, use_claude=False, progress_callback=None):
    """从cleaned_items中提取实体和关系更新图谱

    Args:
        since_date: 起始日期，None则处理所有
        use_claude: 是否使用Claude智能提取（消耗API额度）
        progress_callback: 进度回调 fn(current, total, msg)
    """
    sql = "SELECT ci.* FROM cleaned_items ci"
    params = []
    if since_date:
        sql += " WHERE date(ci.cleaned_at) >= ?"
        params.append(since_date)
    sql += " ORDER BY ci.cleaned_at"
    items = execute_query(sql, params)

    total = len(items)
    added_entities = 0
    added_rels = 0

    for idx, item in enumerate(items):
        if progress_callback:
            progress_callback(idx + 1, total, f"处理第{idx+1}/{total}条")

        if use_claude and item.get("importance", 0) >= 3:
            e, r = _extract_with_claude(item)
        else:
            e, r = _extract_from_structured(item)
        added_entities += e
        added_rels += r

    logger.info(f"图谱更新: 新增实体{added_entities}, 新增关系{added_rels}")
    return {"entities": added_entities, "relationships": added_rels, "processed": total}


def _extract_from_structured(item):
    """从已有结构化字段提取实体和关系（不消耗API）"""
    added_e, added_r = 0, 0

    # 从tags提取主题实体
    tags = []
    if item.get("tags_json"):
        try:
            tags = json.loads(item["tags_json"])
        except (json.JSONDecodeError, TypeError):
            pass

    tag_ids = {}
    for tag in tags:
        if not tag or len(tag) < 2:
            continue
        eid = add_entity("theme", tag)
        if eid:
            tag_ids[tag] = eid
            added_e += 1

    # 从关联公司提取公司实体
    companies = execute_query(
        "SELECT * FROM item_companies WHERE cleaned_item_id=?", [item["id"]]
    )
    for comp in companies:
        comp_name = comp.get("stock_name") or comp.get("stock_code", "")
        if not comp_name:
            continue
        comp_id = add_entity("company", comp_name,
                             properties={"stock_code": comp.get("stock_code")})
        if comp_id:
            added_e += 1

        # 公司与标签的关系
        for tag, tid in tag_ids.items():
            if comp_id and tid:
                direction = comp.get("impact", "neutral")
                if direction not in ("positive", "negative", "neutral"):
                    direction = "neutral"
                rid = add_relationship(comp_id, tid, "related",
                                       strength=0.4, direction=direction,
                                       evidence=item.get("summary", "")[:100])
                if rid:
                    added_r += 1

    # 从关联行业提取行业实体
    industries = execute_query(
        "SELECT * FROM item_industries WHERE cleaned_item_id=?", [item["id"]]
    )
    for ind in industries:
        ind_name = ind.get("industry_name", "")
        if not ind_name:
            continue
        ind_id = add_entity("industry", ind_name)
        if ind_id:
            added_e += 1

        # 行业与标签的关系
        for tag, tid in tag_ids.items():
            if ind_id and tid:
                rid = add_relationship(ind_id, tid, "related", strength=0.3)
                if rid:
                    added_r += 1

        # 公司与行业的关系
        for comp in companies:
            comp_name = comp.get("stock_name") or comp.get("stock_code", "")
            comp_ents = find_entity(comp_name)
            if comp_ents and ind_id:
                rid = add_relationship(comp_ents[0]["id"], ind_id, "belongs_to",
                                       strength=0.7, direction="neutral")
                if rid:
                    added_r += 1

    return added_e, added_r


def _extract_with_claude(item):
    """使用Claude智能提取实体和关系"""
    added_e, added_r = 0, 0

    content = f"标题: {item.get('summary', '')}\n"
    if item.get("event_type"):
        content += f"事件类型: {item['event_type']}\n"
    if item.get("impact_analysis"):
        content += f"影响分析: {item['impact_analysis']}\n"
    if item.get("tags_json"):
        content += f"标签: {item['tags_json']}\n"

    try:
        result = call_claude_json(KG_EXTRACTION_PROMPT, content, max_tokens=1024)
    except Exception as e:
        logger.warning(f"Claude提取失败: {e}")
        return _extract_from_structured(item)

    if not isinstance(result, dict):
        return _extract_from_structured(item)

    # 创建实体
    name_to_id = {}
    for ent in result.get("entities", []):
        etype = ent.get("type", "theme")
        ename = ent.get("name", "")
        if not ename or len(ename) < 2:
            continue
        valid_types = {"macro_factor", "industry", "company", "theme", "indicator"}
        if etype not in valid_types:
            etype = "theme"
        eid = add_entity(etype, ename, description=ent.get("description"))
        if eid:
            name_to_id[ename] = eid
            added_e += 1

    # 创建关系
    for rel in result.get("relationships", []):
        src_name = rel.get("source", "")
        tgt_name = rel.get("target", "")
        # 查找实体ID
        src_id = name_to_id.get(src_name)
        tgt_id = name_to_id.get(tgt_name)
        if not src_id:
            found = find_entity(src_name)
            src_id = found[0]["id"] if found else None
        if not tgt_id:
            found = find_entity(tgt_name)
            tgt_id = found[0]["id"] if found else None
        if not src_id or not tgt_id:
            continue

        rel_type = rel.get("type", "related")
        valid_rels = {"impacts", "belongs_to", "competes", "supplies", "benefits", "related"}
        if rel_type not in valid_rels:
            rel_type = "related"

        strength = min(max(float(rel.get("strength", 0.5)), 0.1), 1.0)
        direction = rel.get("direction", "neutral")
        if direction not in ("positive", "negative", "neutral"):
            direction = "neutral"

        rid = add_relationship(src_id, tgt_id, rel_type,
                               strength=strength, direction=direction,
                               evidence=rel.get("evidence", ""),
                               confidence=0.7)
        if rid:
            added_r += 1

    return added_e, added_r


def get_update_summary(since_date=None):
    """获取图谱更新摘要"""
    sql = "SELECT action, COUNT(*) as cnt FROM kg_update_log"
    params = []
    if since_date:
        sql += " WHERE date(updated_at) >= ?"
        params.append(since_date)
    sql += " GROUP BY action"
    return execute_query(sql, params)

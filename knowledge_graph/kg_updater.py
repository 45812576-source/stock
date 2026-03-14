"""从清洗数据更新知识图谱 — Claude智能提取实体和关系（v2: 28种关系类型）

DEPRECATED: update_from_cleaned_items() 已被 kg_extractor_pipeline.batch_extract_kg() 替代。
本文件保留 KG_EXTRACTION_PROMPT 供新管线复用。
VALID_ENTITY_TYPES / VALID_RELATION_TYPES / RELATION_TO_CATEGORY 已迁移至 kg_schema.py。
"""
import json
import logging
from datetime import datetime
from utils.db_utils import execute_query, execute_insert
from utils.model_router import call_model_json

def call_claude_json(system_prompt, user_message, max_tokens=4096, timeout=900):
    return call_model_json('kg', system_prompt, user_message, max_tokens=max_tokens, timeout=timeout)
from knowledge_graph.kg_manager import (
    add_entity, add_relationship, find_entity, get_entity_count,
)
from knowledge_graph.kg_schema import (
    VALID_ENTITY_TYPES, VALID_RELATION_TYPES, RELATION_TO_CATEGORY,
)

logger = logging.getLogger(__name__)

# ==================== 新版抽取 Prompt ====================

KG_EXTRACTION_PROMPT = """你是金融知识图谱构建专家。请从以下金融资讯中提取实体-关系三元组。

## 核心原则：只输出三元组，没有孤立实体

**输出格式是三元组数组，每条记录必须同时包含 source、relation、target 三个字段。**
- ❌ 禁止：单独列出实体（没有对应关系）
- ❌ 禁止：单独列出关系（没有对应实体）
- ✅ 只允许：完整三元组 (实体A) -[关系]-> (实体B)

如果一个实体无法与另一个实体构成有意义的关系，则丢弃该实体，不输出。

## 实体类型（12种）
- market: 市场（如：中国A股市场、美国股市）
- industry: 行业（如：动力电池、半导体、新能源汽车、企业服务/SaaS）
- industry_chain: 产业链（如：锂电池产业链、光伏产业链）
- company: 上市公司（如：宁德时代、比亚迪）
- strategy: 企业战略/商业模式（如：低价策略、订阅制、出海战略、垂直整合）
- macro_indicator: 宏观经济指标（如：PMI、CPI、GDP增速、美联储利率）
- commodity: 大宗商品/原材料（如：碳酸锂、铜、铁矿石）
- energy: 能源（如：原油、天然气、煤炭、电力）
- intermediate: 半成品/中间品（如：电池级碳酸锂、多晶硅、钢材）
- consumer_good: 消费品，含虚拟产品（如：智能手机、新能源汽车、白酒、手游、爱奇艺会员）
- policy: 政策/政治影响，含政治人物态度（如：碳中和政策、反垄断法、特朗普关税威胁表态）
- theme: 投资主题（如：碳中和、AI、国产替代）。"XX概念股"→提取"XX"为 theme

## 实体边界严格规则

**以下情况一律丢弃，不作为实体：**
- ❌ 动词短语/事件描述：如"批准星链卫星部署"、"火箭海上回收作业"
- ❌ 完整句子：如"美国联邦通信委员会批准星链卫星部署"
- ❌ 纯地理位置：如"南海海域"、"北京"（除非明确代表市场，如"中国A股市场"）
- ❌ 模糊机构：如"XX集团相关公司"（company 必须是具体上市公司名）

## 关系类型
因果: causes_positive, causes_negative, cost_transmission, indicator_transmission, demand_driven, supply_driven, demand_source_of, demand_substitute
结构: belongs_to_industry, belongs_to_chain, supplier_of, customer_of, competitor, substitute_threat
要素: major_cost_item, major_revenue_item, cost_affected_by, revenue_affected_by
政策: benefits, hurts, risk_factor, catalyst
指标: leading_indicator_of, coincident_indicator_of, correlated_with

## 输出格式
JSON 数组，每个元素是一个完整三元组：
[
  {
    "source": {"name": "实体名", "type": "实体类型"},
    "relation": "关系类型英文标识",
    "target": {"name": "实体名", "type": "实体类型"},
    "strength": 0.1-1.0,
    "direction": "positive/negative/neutral",
    "evidence": "关系依据（一句话）"
  }
]

## 实体命名规范（必须严格遵守）

### 通用规则
- 实体名必须是**名词或名词短语**，不能包含动词、事件描述、形容词修饰
- 去掉所有动词/事件后缀：❌"AI超级入口争夺" → ✅"AI超级入口"；❌"Robotaxi商业模式跑通" → ✅"Robotaxi"
- 去掉模糊后缀：❌"化妆品制造及其他行业" → ✅"化妆品制造"；❌"半导体等行业" → ✅"半导体"

### 按类型的特殊规则
- **commodity/energy**: 只保留商品名本身，去掉"价格""走势""行情"等后缀，去掉地理定语前缀
  ❌"铜价格" → ✅"铜"；❌"国内煤矿" → ✅"煤矿"；❌"哈萨克斯坦铅银矿" → ✅"铅银矿"
- **macro_indicator**: 只保留指标缩写或标准名称，去掉"主要经济体""同比""环比"等定语
  ❌"主要经济体M2同比" → ✅"M2"；❌"中国CPI同比" → ✅"CPI"
- **theme**: 提取核心投资概念，去掉事件描述和动词，去掉地理前缀
  ❌"AI超级入口之争" → ✅"AI超级入口"；❌"国内AI应用" → ✅"AI应用"
- **industry_chain**: 必须以"产业链"或"供应链"结尾，简称要补全
  ❌"火箭链" → ✅"火箭产业链"；❌"算力链" → ✅"算力产业链"
- **industry**: 保留证券行业分类中的标准名称，按类型细分的服务业（如"商务服务""信息服务"）是合法分类不要丢弃

## 注意
- 每条资讯提取不超过 8 个三元组
- strength: 重大影响 0.8+，一般关联 0.3-0.5
- 优先提取跨层级的因果传导（如：宏观→行业→公司）"""


# ==================== 主入口 ====================

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
    """从已有结构化字段提取实体和关系（不消耗API）
    注意：theme 实体由 kg_theme_identifier 模块独立识别，此处不再提取
    """
    added_e, added_r = 0, 0

    # 从关联公司提取公司实体
    companies = execute_query(
        "SELECT * FROM item_companies WHERE cleaned_item_id=?", [item["id"]]
    )
    for comp in companies:
        comp_name = comp.get("stock_name") or comp.get("stock_code", "")
        if not comp_name:
            continue
        comp_id = add_entity("company", comp_name,
                             properties={"stock_code": comp.get("stock_code")},
                             external_id=comp.get("stock_code"))
        if comp_id:
            added_e += 1

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

        # 公司与行业的关系
        for comp in companies:
            comp_name = comp.get("stock_name") or comp.get("stock_code", "")
            comp_ents = find_entity(comp_name)
            if comp_ents and ind_id:
                rid = add_relationship(comp_ents[0]["id"], ind_id,
                                       "belongs_to_industry",
                                       relation_category="structural",
                                       strength=0.7, direction="neutral")
                if rid:
                    added_r += 1

    return added_e, added_r


def _extract_with_claude(item):
    """使用Claude智能提取实体和关系（v2: 支持28种关系类型）"""
    added_e, added_r = 0, 0

    content = f"标题: {item.get('summary', '')}\n"
    if item.get("event_type"):
        content += f"事件类型: {item['event_type']}\n"
    if item.get("impact_analysis"):
        content += f"影响分析: {item['impact_analysis']}\n"
    if item.get("tags_json"):
        content += f"标签: {item['tags_json']}\n"

    try:
        result = call_claude_json(KG_EXTRACTION_PROMPT, content, max_tokens=2048)
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
        if etype not in VALID_ENTITY_TYPES:
            etype = "industry"
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
        if rel_type not in VALID_RELATION_TYPES:
            rel_type = "related"

        category = rel.get("category") or RELATION_TO_CATEGORY.get(rel_type, "structural")
        strength = min(max(float(rel.get("strength", 0.5)), 0.1), 1.0)
        direction = rel.get("direction", "neutral")
        if direction not in ("positive", "negative", "neutral"):
            direction = "neutral"

        time_lag = rel.get("time_lag")
        if time_lag not in ("immediate", "short", "medium", "long", None):
            time_lag = None

        certainty = rel.get("certainty")
        if certainty not in ("deterministic", "probabilistic", None):
            certainty = None

        rid = add_relationship(
            src_id, tgt_id, rel_type,
            strength=strength, direction=direction,
            evidence=rel.get("evidence", ""),
            confidence=0.7,
            relation_category=category,
            time_lag=time_lag,
            certainty=certainty,
            conditions=rel.get("conditions"),
            source_text=item.get("summary", "")[:200],
        )
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

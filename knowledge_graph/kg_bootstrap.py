"""KG 骨架填充 — 从已有 DB 数据批量导入实体和关系

用法:
    python knowledge_graph/kg_bootstrap.py --step industry   # 行业→公司映射
    python knowledge_graph/kg_bootstrap.py --step composition # 主营构成(成本/收入要素)
    python knowledge_graph/kg_bootstrap.py --step news        # 新闻→主题/行业/公司关系
    python knowledge_graph/kg_bootstrap.py --step all         # 全部
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import argparse
import pymysql
from config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_conn():
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PASSWORD, database=MYSQL_DB,
        charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor,
    )


# ---------- 实体缓存（避免重复查询） ----------

_entity_cache = {}  # (type, name) -> id


def _ensure_entity(cur, conn, entity_type, entity_name, sub_type=None,
                   external_id=None, data_source=None, description=None):
    """确保实体存在，返回 ID（带缓存）"""
    key = (entity_type, entity_name)
    if key in _entity_cache:
        return _entity_cache[key]

    cur.execute(
        "SELECT id FROM kg_entities WHERE entity_type=%s AND entity_name=%s",
        [entity_type, entity_name],
    )
    row = cur.fetchone()
    if row:
        _entity_cache[key] = row["id"]
        return row["id"]

    cur.execute(
        """INSERT INTO kg_entities
           (entity_type, sub_type, entity_name, external_id, data_source, description)
           VALUES (%s, %s, %s, %s, %s, %s)
           ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)""",
        [entity_type, sub_type, entity_name, external_id, data_source, description],
    )
    conn.commit()
    eid = cur.lastrowid
    _entity_cache[key] = eid
    return eid


def _ensure_relationship(cur, conn, src_id, tgt_id, relation_type,
                         relation_category=None, strength=0.5, direction="neutral",
                         confidence=0.7, percentage=None, evidence=None):
    """确保关系存在（去重）"""
    cur.execute(
        """SELECT id FROM kg_relationships
           WHERE source_entity_id=%s AND target_entity_id=%s AND relation_type=%s""",
        [src_id, tgt_id, relation_type],
    )
    if cur.fetchone():
        return  # 已存在，跳过

    cur.execute(
        """INSERT INTO kg_relationships
           (source_entity_id, target_entity_id, relation_type, relation_category,
            strength, direction, confidence, percentage, evidence)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        [src_id, tgt_id, relation_type, relation_category,
         strength, direction, confidence, percentage, evidence],
    )


# ==================== Step 1: 行业→公司映射 ====================

def bootstrap_industry(conn):
    """从 industry_stock_mapping + stock_info 导入行业和公司实体及关系"""
    cur = conn.cursor()

    # 1a. 创建行业实体
    cur.execute("SELECT DISTINCT industry_name, industry_code FROM industry_stock_mapping")
    industries = cur.fetchall()
    logger.info(f"创建行业实体: {len(industries)} 个")

    for ind in industries:
        _ensure_entity(cur, conn, "industry", ind["industry_name"],
                       external_id=ind["industry_code"], data_source="eastmoney")

    # 1b. 创建公司实体（从 stock_info 取完整信息）
    cur.execute("""
        SELECT si.stock_code, si.stock_name, si.industry_l1, si.industry_l2,
               si.main_business
        FROM stock_info si
    """)
    stocks = cur.fetchall()
    logger.info(f"创建公司实体: {len(stocks)} 只")

    for i, s in enumerate(stocks):
        desc = s.get("main_business") or None
        _ensure_entity(cur, conn, "company", s["stock_name"],
                       external_id=s["stock_code"], data_source="stock_info",
                       description=desc)
        if (i + 1) % 500 == 0:
            logger.info(f"  公司实体进度: {i+1}/{len(stocks)}")

    conn.commit()

    # 1c. 创建 belongs_to_industry 关系
    cur.execute("""
        SELECT ism.industry_name, ism.stock_code COLLATE utf8mb4_0900_ai_ci as stock_code, si.stock_name
        FROM industry_stock_mapping ism
        JOIN stock_info si ON ism.stock_code COLLATE utf8mb4_0900_ai_ci = si.stock_code
    """)
    mappings = cur.fetchall()
    logger.info(f"创建行业归属关系: {len(mappings)} 条")

    count = 0
    for i, m in enumerate(mappings):
        ind_id = _entity_cache.get(("industry", m["industry_name"]))
        comp_id = _entity_cache.get(("company", m["stock_name"]))
        if not ind_id or not comp_id:
            continue

        _ensure_relationship(cur, conn, comp_id, ind_id, "belongs_to_industry",
                             relation_category="structural", strength=0.9,
                             confidence=0.95)
        count += 1

        if (i + 1) % 2000 == 0:
            conn.commit()
            logger.info(f"  关系进度: {i+1}/{len(mappings)}")

    conn.commit()
    logger.info(f"行业映射完成: {len(industries)} 行业, {len(stocks)} 公司, {count} 条关系")
    return count


# ==================== Step 2: 主营构成 → 成本/收入要素 ====================

def bootstrap_composition(conn):
    """从 stock_business_composition 导入成本/收入要素实体和关系"""
    cur = conn.cursor()

    cur.execute("""
        SELECT sbc.*, si.stock_name
        FROM stock_business_composition sbc
        JOIN stock_info si ON sbc.stock_code COLLATE utf8mb4_0900_ai_ci = si.stock_code
        WHERE sbc.classify_type IN ('按产品', '按行业', '按地区')
    """)
    rows = cur.fetchall()
    if not rows:
        logger.info("stock_business_composition 为空，跳过")
        return 0

    logger.info(f"处理主营构成: {len(rows)} 条")
    count = 0

    for i, r in enumerate(rows):
        stock_name = r["stock_name"]
        item_name = r["item_name"]
        classify = r["classify_type"]

        comp_id = _entity_cache.get(("company", stock_name))
        if not comp_id:
            comp_id = _ensure_entity(cur, conn, "company", stock_name,
                                     external_id=r["stock_code"])

        # 按产品分类 → 收入要素
        if classify == "按产品":
            elem_type = "revenue_element"
            rel_type = "major_revenue_item"
        elif classify == "按行业":
            elem_type = "revenue_element"
            rel_type = "major_revenue_item"
        else:  # 按地区
            elem_type = "revenue_element"
            rel_type = "major_revenue_item"

        elem_id = _ensure_entity(cur, conn, elem_type, item_name,
                                 sub_type=classify, data_source="eastmoney")

        pct = r.get("revenue_pct")
        _ensure_relationship(cur, conn, comp_id, elem_id, rel_type,
                             relation_category="element", strength=0.8,
                             percentage=pct,
                             evidence=f"报告期: {r.get('report_date', '')}")
        count += 1

        # 如果有成本数据，也建立成本关系
        if r.get("cost") and float(r.get("cost") or 0) > 0:
            cost_pct = r.get("cost_pct")
            _ensure_relationship(cur, conn, comp_id, elem_id, "major_cost_item",
                                 relation_category="element", strength=0.8,
                                 percentage=cost_pct,
                                 evidence=f"报告期: {r.get('report_date', '')}")
            count += 1

        if (i + 1) % 1000 == 0:
            conn.commit()
            logger.info(f"  主营构成进度: {i+1}/{len(rows)}")

    conn.commit()
    logger.info(f"主营构成完成: {count} 条关系")
    return count


# ==================== Step 3: 新闻 → 主题/行业/公司关系 ====================

def bootstrap_news(conn):
    """从 cleaned_items + item_companies + item_industries 导入关系"""
    cur = conn.cursor()

    # 3a. 从 cleaned_items 提取 theme 实体（tags）
    cur.execute("SELECT id, tags_json, summary, event_type FROM cleaned_items WHERE tags_json IS NOT NULL")
    items = cur.fetchall()
    logger.info(f"处理 cleaned_items: {len(items)} 条")

    tag_count = 0
    for item in items:
        try:
            tags = json.loads(item["tags_json"])
        except (json.JSONDecodeError, TypeError):
            continue
        for tag in tags:
            if tag and len(tag) >= 2:
                _ensure_entity(cur, conn, "theme", tag, data_source="cleaning")
                tag_count += 1

    conn.commit()
    logger.info(f"  创建主题实体: {tag_count} 个（去重后 {len([k for k in _entity_cache if k[0]=='theme'])}）")

    # 3b. 公司↔主题关系（从 item_companies）
    cur.execute("""
        SELECT ic.stock_code, ic.stock_name, ic.impact,
               ci.tags_json, ci.summary
        FROM item_companies ic
        JOIN cleaned_items ci ON ic.cleaned_item_id = ci.id
        WHERE ci.tags_json IS NOT NULL
    """)
    comp_rels = cur.fetchall()
    logger.info(f"处理公司-主题关系: {len(comp_rels)} 条")

    rel_count = 0
    for cr in comp_rels:
        comp_name = cr.get("stock_name") or ""
        if not comp_name:
            continue
        comp_id = _entity_cache.get(("company", comp_name))
        if not comp_id:
            comp_id = _ensure_entity(cur, conn, "company", comp_name,
                                     external_id=cr.get("stock_code"))

        try:
            tags = json.loads(cr["tags_json"])
        except (json.JSONDecodeError, TypeError):
            continue

        impact = cr.get("impact", "neutral")
        if impact not in ("positive", "negative", "neutral"):
            impact = "neutral"

        for tag in tags:
            if not tag or len(tag) < 2:
                continue
            tag_id = _entity_cache.get(("theme", tag))
            if not tag_id:
                continue
            _ensure_relationship(cur, conn, comp_id, tag_id, "related",
                                 relation_category="structural", strength=0.4,
                                 direction=impact,
                                 evidence=(cr.get("summary") or "")[:100])
            rel_count += 1

    conn.commit()

    # 3c. 行业↔主题关系（从 item_industries）
    cur.execute("""
        SELECT ii.industry_name,
               ci.tags_json, ci.summary
        FROM item_industries ii
        JOIN cleaned_items ci ON ii.cleaned_item_id = ci.id
        WHERE ci.tags_json IS NOT NULL
    """)
    ind_rels = cur.fetchall()
    logger.info(f"处理行业-主题关系: {len(ind_rels)} 条")

    for ir in ind_rels:
        ind_name = ir.get("industry_name", "")
        if not ind_name:
            continue
        ind_id = _entity_cache.get(("industry", ind_name))
        if not ind_id:
            ind_id = _ensure_entity(cur, conn, "industry", ind_name, data_source="cleaning")

        try:
            tags = json.loads(ir["tags_json"])
        except (json.JSONDecodeError, TypeError):
            continue

        for tag in tags:
            if not tag or len(tag) < 2:
                continue
            tag_id = _entity_cache.get(("theme", tag))
            if not tag_id:
                continue
            _ensure_relationship(cur, conn, ind_id, tag_id, "related",
                                 relation_category="structural", strength=0.3,
                                 evidence=(ir.get("summary") or "")[:100])
            rel_count += 1

    conn.commit()
    logger.info(f"新闻关系完成: {rel_count} 条关系")
    return rel_count


# ==================== Main ====================

def print_stats(conn):
    """打印 KG 统计"""
    cur = conn.cursor()
    cur.execute("SELECT entity_type, COUNT(*) as cnt FROM kg_entities GROUP BY entity_type ORDER BY cnt DESC")
    print("\n实体类型分布:")
    for r in cur.fetchall():
        print(f"  {r['entity_type']}: {r['cnt']}")
    cur.execute("SELECT relation_type, COUNT(*) as cnt FROM kg_relationships GROUP BY relation_type ORDER BY cnt DESC")
    print("\n关系类型分布:")
    for r in cur.fetchall():
        print(f"  {r['relation_type']}: {r['cnt']}")
    cur.execute("SELECT COUNT(*) as cnt FROM kg_entities")
    print(f"\n实体总数: {cur.fetchone()['cnt']}")
    cur.execute("SELECT COUNT(*) as cnt FROM kg_relationships")
    print(f"关系总数: {cur.fetchone()['cnt']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KG 骨架填充")
    parser.add_argument("--step", choices=["industry", "composition", "news", "all"],
                        default="all", help="填充步骤")
    args = parser.parse_args()

    conn = get_conn()

    if args.step in ("industry", "all"):
        bootstrap_industry(conn)

    if args.step in ("composition", "all"):
        bootstrap_composition(conn)

    if args.step in ("news", "all"):
        bootstrap_news(conn)

    print_stats(conn)
    conn.close()

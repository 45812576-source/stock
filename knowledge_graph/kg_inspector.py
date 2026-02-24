"""KG 巡检程序 — 冲突清理 + 总结↔KG 交叉补全

两个核心功能：
1. conflict_cleanup(): 清理矛盾关系和孤立实体
2. cross_complete(): 遍历近期 content_summaries，补全缺失的实体/关系
3. name_cleanup(): 存量实体名规范化（重命名/合并/丢弃）
"""
import logging
from datetime import datetime, timedelta
from utils.db_utils import execute_query, execute_insert
from utils.model_router import call_model_json
from knowledge_graph.kg_manager import (
    add_entity, add_relationship, find_entity, delete_relationship, delete_entity,
)
from knowledge_graph.kg_updater import KG_EXTRACTION_PROMPT
from knowledge_graph.kg_name_normalizer import normalize_entity_name
from knowledge_graph.kg_schema import (
    VALID_ENTITY_TYPES, VALID_RELATION_TYPES, RELATION_TO_CATEGORY,
    VALID_COMBINATIONS, CONFLICTING_PAIRS, ENTITY_LAYER_MAP,
)

logger = logging.getLogger(__name__)


def schema_validate(dry_run: bool = False) -> dict:
    """校验 KG 中所有实体和关系是否符合增强 schema

    Args:
        dry_run: True 则只统计不修改
    Returns:
        {"invalid_entity_types": int, "invalid_relation_types": int,
         "invalid_combinations": int, "conflicting_relations": int,
         "invalid_entity_names": int, "related_ratio": float,
         "details": [...]}
    """
    result = {
        "invalid_entity_types": 0,
        "invalid_relation_types": 0,
        "invalid_combinations": 0,
        "conflicting_relations": 0,
        "invalid_entity_names": 0,
        "related_ratio": 0.0,
        "details": [],
    }

    # 1. 实体类型校验
    bad_entities = execute_query(
        "SELECT id, entity_name, entity_type FROM kg_entities WHERE entity_type NOT IN (%s)"
        % ",".join(["%s"] * len(VALID_ENTITY_TYPES)),
        list(VALID_ENTITY_TYPES),
    )
    result["invalid_entity_types"] = len(bad_entities)
    for e in bad_entities:
        result["details"].append(
            f"非法实体类型: id={e['id']} name={e['entity_name']} type={e['entity_type']}"
        )
        logger.warning(f"Schema校验-非法实体类型: {e['entity_name']} ({e['entity_type']})")
    if bad_entities and not dry_run:
        ids = [e["id"] for e in bad_entities]
        batch = 200
        for i in range(0, len(ids), batch):
            chunk = ids[i:i+batch]
            ph = ",".join(["%s"] * len(chunk))
            execute_insert(f"DELETE FROM kg_relationships WHERE source_entity_id IN ({ph}) OR target_entity_id IN ({ph})", chunk + chunk)
            execute_insert(f"DELETE FROM kg_entities WHERE id IN ({ph})", chunk)

    # 2. 关系类型校验
    bad_rels = execute_query(
        "SELECT id, relation_type FROM kg_relationships WHERE relation_type NOT IN (%s)"
        % ",".join(["%s"] * len(VALID_RELATION_TYPES)),
        list(VALID_RELATION_TYPES),
    )
    result["invalid_relation_types"] = len(bad_rels)
    for r in bad_rels:
        result["details"].append(
            f"非法关系类型: rel_id={r['id']} type={r['relation_type']}"
        )
    if bad_rels and not dry_run:
        ids = [r["id"] for r in bad_rels]
        batch = 200
        for i in range(0, len(ids), batch):
            chunk = ids[i:i+batch]
            ph = ",".join(["%s"] * len(chunk))
            execute_insert(f"DELETE FROM kg_relationships WHERE id IN ({ph})", chunk)

    # 3. 组合合法性校验 (source_type, relation_type, target_type)
    all_rels = execute_query(
        """SELECT r.id, r.relation_type,
                  s.entity_type as src_type, s.entity_name as src_name,
                  t.entity_type as tgt_type, t.entity_name as tgt_name
           FROM kg_relationships r
           JOIN kg_entities s ON r.source_entity_id = s.id
           JOIN kg_entities t ON r.target_entity_id = t.id"""
    )
    bad_combo_ids = []
    for rel in all_rels:
        combo = (rel["src_type"], rel["relation_type"], rel["tgt_type"])
        if combo not in VALID_COMBINATIONS:
            result["invalid_combinations"] += 1
            result["details"].append(
                f"非法组合: {rel['src_name']}({rel['src_type']}) "
                f"-[{rel['relation_type']}]-> "
                f"{rel['tgt_name']}({rel['tgt_type']}) rel_id={rel['id']}"
            )
            bad_combo_ids.append(rel["id"])
    if bad_combo_ids and not dry_run:
        batch = 200
        for i in range(0, len(bad_combo_ids), batch):
            chunk = bad_combo_ids[i:i+batch]
            ph = ",".join(["%s"] * len(chunk))
            execute_insert(f"DELETE FROM kg_relationships WHERE id IN ({ph})", chunk)

    # 4. 互斥关系校验（使用 kg_schema 的完整 CONFLICTING_PAIRS）
    for type_a, type_b in CONFLICTING_PAIRS:
        conflicts = execute_query(
            """SELECT a.id as id_a, b.id as id_b
               FROM kg_relationships a
               JOIN kg_relationships b
                 ON a.source_entity_id = b.source_entity_id
                AND a.target_entity_id = b.target_entity_id
               WHERE a.relation_type=%s AND b.relation_type=%s""",
            [type_a, type_b],
        )
        result["conflicting_relations"] += len(conflicts)
        for c in conflicts:
            result["details"].append(
                f"互斥冲突: {type_a}(id={c['id_a']}) vs {type_b}(id={c['id_b']})"
            )

    # 5. 实体名合法性（长度检测 + 非名词检测）
    short_names = execute_query(
        "SELECT id, entity_name, entity_type FROM kg_entities WHERE CHAR_LENGTH(entity_name) < 2"
    )
    result["invalid_entity_names"] = len(short_names)
    for e in short_names:
        result["details"].append(
            f"实体名过短: id={e['id']} name='{e['entity_name']}' type={e['entity_type']}"
        )

    # 6. related 兜底关系占比
    total_rels = execute_query("SELECT COUNT(*) as cnt FROM kg_relationships")
    related_rels = execute_query(
        "SELECT COUNT(*) as cnt FROM kg_relationships WHERE relation_type='related'"
    )
    total_cnt = total_rels[0]["cnt"] if total_rels else 0
    related_cnt = related_rels[0]["cnt"] if related_rels else 0
    result["related_ratio"] = round(related_cnt / total_cnt, 4) if total_cnt > 0 else 0.0
    if result["related_ratio"] > 0.05:
        result["details"].append(
            f"related 兜底关系占比过高: {related_cnt}/{total_cnt} = {result['related_ratio']:.1%}（应 < 5%）"
        )

    total_issues = (result["invalid_entity_types"] + result["invalid_relation_types"]
                    + result["invalid_combinations"] + result["conflicting_relations"]
                    + result["invalid_entity_names"])
    logger.info(
        f"KG Schema校验完成: 总问题={total_issues}, "
        f"非法实体类型={result['invalid_entity_types']}, "
        f"非法关系类型={result['invalid_relation_types']}, "
        f"非法组合={result['invalid_combinations']}, "
        f"互斥冲突={result['conflicting_relations']}, "
        f"实体名异常={result['invalid_entity_names']}, "
        f"related占比={result['related_ratio']:.1%}"
    )
    return result


def conflict_cleanup(dry_run: bool = False) -> dict:
    """清理 KG 中的冲突关系和孤立实体

    Args:
        dry_run: True 则只统计不删除
    Returns:
        {"conflicts_removed": int, "orphans_removed": int}
    """
    conflicts_removed = 0
    orphans_removed = 0

    # 1. 清理矛盾关系
    for type_a, type_b in CONFLICTING_PAIRS:
        # 找出同一对实体间同时存在 type_a 和 type_b 的情况
        rows = execute_query(
            """SELECT a.id as id_a, b.id as id_b,
                      a.confidence as conf_a, b.confidence as conf_b,
                      a.created_at as time_a, b.created_at as time_b
               FROM kg_relationships a
               JOIN kg_relationships b
                 ON a.source_entity_id = b.source_entity_id
                AND a.target_entity_id = b.target_entity_id
               WHERE a.relation_type=%s AND b.relation_type=%s""",
            [type_a, type_b],
        )
        for row in rows:
            # 保留 confidence 更高的，时间更新的优先
            keep_a = (row["conf_a"] or 0) >= (row["conf_b"] or 0)
            remove_id = row["id_b"] if keep_a else row["id_a"]
            logger.info(f"冲突关系: {type_a} vs {type_b}, 删除 rel_id={remove_id}")
            if not dry_run:
                delete_relationship(remove_id)
            conflicts_removed += 1

    # 2. 清理孤立实体（无任何关系）— 批量删除，避免逐条连接
    orphans = execute_query(
        """SELECT e.id FROM kg_entities e
           WHERE NOT EXISTS (
               SELECT 1 FROM kg_relationships r
               WHERE r.source_entity_id = e.id OR r.target_entity_id = e.id
           )"""
    )
    orphans_removed = len(orphans)
    if orphans and not dry_run:
        ids = [row["id"] for row in orphans]
        # 分批删除，每批 200 个，避免 IN 子句过长
        batch = 200
        for i in range(0, len(ids), batch):
            chunk = ids[i:i+batch]
            ph = ",".join(["%s"] * len(chunk))
            execute_insert(f"DELETE FROM kg_entities WHERE id IN ({ph})", chunk)
        logger.info(f"批量删除孤立实体: {orphans_removed} 个")

    logger.info(f"KG巡检-冲突清理: 冲突={conflicts_removed}, 孤立={orphans_removed}, dry_run={dry_run}")
    return {"conflicts_removed": conflicts_removed, "orphans_removed": orphans_removed}


def cross_complete(days: int = 7, limit: int = 20, progress_callback=None) -> dict:
    """总结↔KG 交叉补全：从近期 content_summaries 提取缺失的实体/关系

    Args:
        days: 处理最近 N 天的总结
        limit: 最多处理条数
        progress_callback: callback(current, total, msg)
    Returns:
        {"entities_added": int, "relationships_added": int, "processed": int}
    """
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

    # 从本地读取近期总结
    rows = execute_query(
        """SELECT id, extracted_text_id, summary, fact_summary, opinion_summary
           FROM content_summaries
           WHERE created_at >= %s
           ORDER BY created_at DESC
           LIMIT %s""",
        [cutoff, limit],
    )

    total = len(rows)
    entities_added = 0
    relationships_added = 0

    for idx, row in enumerate(rows):
        if progress_callback:
            progress_callback(idx + 1, total, f"交叉补全 summary_id={row['id']}")

        text = "\n\n".join(filter(None, [
            row.get("summary", ""),
            row.get("fact_summary", ""),
            row.get("opinion_summary", ""),
        ]))
        if not text.strip():
            continue

        try:
            result = call_model_json(
                'kg',
                KG_EXTRACTION_PROMPT,
                text,
                max_tokens=2048,
                timeout=120,
            )
        except Exception as e:
            logger.warning(f"交叉补全提取失败 summary_id={row['id']}: {e}")
            continue

        if not result:
            continue

        # 兼容两种格式：
        # 新格式（三元组数组）: [{source: {name, type}, relation, target: {name, type}, ...}]
        # 旧格式（分离）: {entities: [...], relationships: [...]}
        triples = []
        if isinstance(result, list):
            triples = result
        elif isinstance(result, dict) and result.get("entities"):
            # 旧格式 — 转换为三元组处理
            triples = []
            _ent_map = {(e.get("name") or "").strip(): e for e in result.get("entities", [])}
            for rel in result.get("relationships", []):
                src_name = (rel.get("source") or "").strip()
                tgt_name = (rel.get("target") or "").strip()
                src_ent = _ent_map.get(src_name, {"name": src_name, "type": "industry"})
                tgt_ent = _ent_map.get(tgt_name, {"name": tgt_name, "type": "industry"})
                triples.append({
                    "source": src_ent, "target": tgt_ent,
                    "relation": rel.get("type", "related"),
                    "strength": rel.get("strength", 0.5),
                    "direction": rel.get("direction", "neutral"),
                    "evidence": rel.get("evidence", ""),
                })

        # 写入实体和关系
        name_to_id = {}
        for triple in triples:
            src_info = triple.get("source") or {}
            tgt_info = triple.get("target") or {}
            rel_type = triple.get("relation", "related")

            if isinstance(src_info, str):
                src_info = {"name": src_info, "type": "industry"}
            if isinstance(tgt_info, str):
                tgt_info = {"name": tgt_info, "type": "industry"}

            src_name = (src_info.get("name") or "").strip()
            tgt_name = (tgt_info.get("name") or "").strip()
            src_type_raw = src_info.get("type", "industry")
            tgt_type_raw = tgt_info.get("type", "industry")

            # 规范化实体名
            src_name_norm = normalize_entity_name(src_type_raw, src_name)
            tgt_name_norm = normalize_entity_name(tgt_type_raw, tgt_name)
            if not src_name_norm or not tgt_name_norm:
                continue
            src_name = src_name_norm
            tgt_name = tgt_name_norm

            if not src_name or not tgt_name or len(src_name) < 2 or len(tgt_name) < 2:
                continue
            if rel_type not in VALID_RELATION_TYPES:
                rel_type = "related"

            # 确保实体存在
            for ename, etype_raw in [(src_name, src_info.get("type", "industry")),
                                      (tgt_name, tgt_info.get("type", "industry"))]:
                if ename in name_to_id:
                    continue
                etype = etype_raw if etype_raw in VALID_ENTITY_TYPES else "industry"
                existing = find_entity(ename)
                if existing:
                    name_to_id[ename] = existing[0]["id"]
                else:
                    eid = add_entity(etype, ename)
                    if eid:
                        name_to_id[ename] = eid
                        entities_added += 1

            src_id = name_to_id.get(src_name)
            tgt_id = name_to_id.get(tgt_name)
            if not src_id or not tgt_id:
                continue

            category = RELATION_TO_CATEGORY.get(rel_type, "structural")
            strength = min(max(float(triple.get("strength", 0.5)), 0.1), 1.0)
            direction = triple.get("direction", "neutral")
            if direction not in ("positive", "negative", "neutral"):
                direction = "neutral"

            rid = add_relationship(
                src_id, tgt_id, rel_type,
                strength=strength, direction=direction,
                evidence=triple.get("evidence", ""),
                confidence=0.6,
                relation_category=category,
            )
            if rid:
                relationships_added += 1
                from knowledge_graph.kg_manager import write_triple_source
                write_triple_source(
                    relationship_id=rid,
                    source_entity_id=src_id,
                    target_entity_id=tgt_id,
                    source_type='content_summary',
                    source_id=row['id'],
                    source_title=(text[:100] if text else None),
                )

    logger.info(f"KG交叉补全: 新增实体={entities_added}, 新增关系={relationships_added}, 处理={total}")
    return {"entities_added": entities_added, "relationships_added": relationships_added,
            "processed": total}


def name_cleanup(dry_run: bool = False) -> dict:
    """存量实体名规范化清洗

    遍历所有 kg_entities，对每个实体调用 normalize_entity_name()：
    - 清洗后名字 == 原名 → 跳过
    - 清洗后名字 != 原名 且目标名已存在 → 合并（迁移关系到目标实体，删除当前实体）
    - 清洗后名字 != 原名 且目标名不存在 → 重命名
    - 清洗后返回 None → 删除实体及其关系

    Returns:
        {"renamed": int, "merged": int, "deleted": int, "skipped": int}
    """
    result = {"renamed": 0, "merged": 0, "deleted": 0, "skipped": 0}

    entities = execute_query(
        "SELECT id, entity_type, entity_name FROM kg_entities ORDER BY id"
    ) or []

    for ent in entities:
        eid = ent["id"]
        etype = ent["entity_type"]
        ename = ent["entity_name"]

        cleaned = normalize_entity_name(etype, ename)

        if cleaned is None:
            # 应丢弃
            logger.info(f"name_cleanup: 删除实体 id={eid} name='{ename}' type={etype}")
            if not dry_run:
                execute_insert(
                    "DELETE FROM kg_relationships WHERE source_entity_id=%s OR target_entity_id=%s",
                    [eid, eid]
                )
                execute_insert("DELETE FROM kg_entities WHERE id=%s", [eid])
            result["deleted"] += 1

        elif cleaned != ename:
            # 检查目标名是否已存在
            existing = execute_query(
                "SELECT id FROM kg_entities WHERE entity_type=%s AND entity_name=%s",
                [etype, cleaned]
            )
            if existing:
                target_id = existing[0]["id"]
                logger.info(
                    f"name_cleanup: 合并 id={eid} '{ename}' → id={target_id} '{cleaned}'"
                )
                if not dry_run:
                    # 迁移关系
                    execute_insert(
                        "UPDATE kg_relationships SET source_entity_id=%s WHERE source_entity_id=%s",
                        [target_id, eid]
                    )
                    execute_insert(
                        "UPDATE kg_relationships SET target_entity_id=%s WHERE target_entity_id=%s",
                        [target_id, eid]
                    )
                    # 删除重复关系（合并后可能产生）
                    execute_insert(
                        """DELETE r1 FROM kg_relationships r1
                           INNER JOIN kg_relationships r2
                           ON r1.source_entity_id = r2.source_entity_id
                              AND r1.target_entity_id = r2.target_entity_id
                              AND r1.relation_type = r2.relation_type
                              AND r1.id > r2.id""",
                        []
                    )
                    execute_insert("DELETE FROM kg_entities WHERE id=%s", [eid])
                result["merged"] += 1
            else:
                logger.info(
                    f"name_cleanup: 重命名 id={eid} '{ename}' → '{cleaned}'"
                )
                if not dry_run:
                    execute_insert(
                        "UPDATE kg_entities SET entity_name=%s, updated_at=NOW() WHERE id=%s",
                        [cleaned, eid]
                    )
                result["renamed"] += 1
        else:
            result["skipped"] += 1

    logger.info(
        f"KG名称清洗: 重命名={result['renamed']}, 合并={result['merged']}, "
        f"删除={result['deleted']}, 跳过={result['skipped']}, dry_run={dry_run}"
    )
    return result


def run_inspection(days: int = 7, limit: int = 20,
                   dry_run: bool = False, progress_callback=None) -> dict:
    """运行完整巡检（schema校验 + 名称清洗 + 冲突清理 + 交叉补全）"""
    schema_result = schema_validate(dry_run=dry_run)
    name_result = name_cleanup(dry_run=dry_run)
    cleanup_result = conflict_cleanup(dry_run=dry_run)
    complete_result = cross_complete(days=days, limit=limit,
                                     progress_callback=progress_callback)
    return {
        "schema_validation": schema_result,
        "name_cleanup": name_result,
        "cleanup": cleanup_result,
        "cross_complete": complete_result,
        "inspected_at": datetime.now().isoformat(),
    }


def extract_from_summary(summary_id: int) -> dict:
    """从单条 content_summary 提取实体/关系并写入 KG

    Returns:
        {"entities_added": int, "relationships_added": int, "summary_id": int, "status": "done"|"failed"}
    """
    rows = execute_query(
        "SELECT id, summary, fact_summary, opinion_summary FROM content_summaries WHERE id=%s",
        [summary_id],
    )
    if not rows:
        return {"entities_added": 0, "relationships_added": 0, "summary_id": summary_id, "status": "failed"}

    row = rows[0]
    text = "\n\n".join(filter(None, [
        row.get("summary", ""),
        row.get("fact_summary", ""),
        row.get("opinion_summary", ""),
    ]))
    if not text.strip():
        return {"entities_added": 0, "relationships_added": 0, "summary_id": summary_id, "status": "failed"}

    try:
        result = call_model_json(
            'kg',
            KG_EXTRACTION_PROMPT,
            text,
            max_tokens=2048,
            timeout=120,
        )
    except Exception as e:
        logger.warning(f"extract_from_summary 提取失败 summary_id={summary_id}: {e}")
        return {"entities_added": 0, "relationships_added": 0, "summary_id": summary_id, "status": "failed"}

    if not result:
        return {"entities_added": 0, "relationships_added": 0, "summary_id": summary_id, "status": "failed"}

    # 兼容两种格式（复用 cross_complete 的逻辑）
    triples = []
    if isinstance(result, list):
        triples = result
    elif isinstance(result, dict) and result.get("entities"):
        _ent_map = {(e.get("name") or "").strip(): e for e in result.get("entities", [])}
        for rel in result.get("relationships", []):
            src_name = (rel.get("source") or "").strip()
            tgt_name = (rel.get("target") or "").strip()
            src_ent = _ent_map.get(src_name, {"name": src_name, "type": "industry"})
            tgt_ent = _ent_map.get(tgt_name, {"name": tgt_name, "type": "industry"})
            triples.append({
                "source": src_ent, "target": tgt_ent,
                "relation": rel.get("type", "related"),
                "strength": rel.get("strength", 0.5),
                "direction": rel.get("direction", "neutral"),
                "evidence": rel.get("evidence", ""),
            })

    entities_added = 0
    relationships_added = 0
    name_to_id = {}

    for triple in triples:
        src_info = triple.get("source") or {}
        tgt_info = triple.get("target") or {}
        rel_type = triple.get("relation", "related")

        if isinstance(src_info, str):
            src_info = {"name": src_info, "type": "industry"}
        if isinstance(tgt_info, str):
            tgt_info = {"name": tgt_info, "type": "industry"}

        src_name = (src_info.get("name") or "").strip()
        tgt_name = (tgt_info.get("name") or "").strip()
        src_type_raw = src_info.get("type", "industry")
        tgt_type_raw = tgt_info.get("type", "industry")

        src_name_norm = normalize_entity_name(src_type_raw, src_name)
        tgt_name_norm = normalize_entity_name(tgt_type_raw, tgt_name)
        if not src_name_norm or not tgt_name_norm:
            continue
        src_name = src_name_norm
        tgt_name = tgt_name_norm

        if not src_name or not tgt_name or len(src_name) < 2 or len(tgt_name) < 2:
            continue
        if rel_type not in VALID_RELATION_TYPES:
            rel_type = "related"

        for ename, etype_raw in [(src_name, src_info.get("type", "industry")),
                                  (tgt_name, tgt_info.get("type", "industry"))]:
            if ename in name_to_id:
                continue
            etype = etype_raw if etype_raw in VALID_ENTITY_TYPES else "industry"
            existing = find_entity(ename)
            if existing:
                name_to_id[ename] = existing[0]["id"]
            else:
                eid = add_entity(etype, ename)
                if eid:
                    name_to_id[ename] = eid
                    entities_added += 1

        src_id = name_to_id.get(src_name)
        tgt_id = name_to_id.get(tgt_name)
        if not src_id or not tgt_id:
            continue

        category = RELATION_TO_CATEGORY.get(rel_type, "structural")
        strength = min(max(float(triple.get("strength", 0.5)), 0.1), 1.0)
        direction = triple.get("direction", "neutral")
        if direction not in ("positive", "negative", "neutral"):
            direction = "neutral"

        rid = add_relationship(
            src_id, tgt_id, rel_type,
            strength=strength, direction=direction,
            evidence=triple.get("evidence", ""),
            confidence=0.6,
            relation_category=category,
        )
        if rid:
            relationships_added += 1
            from knowledge_graph.kg_manager import write_triple_source
            write_triple_source(
                relationship_id=rid,
                source_entity_id=src_id,
                target_entity_id=tgt_id,
                source_type='content_summary',
                source_id=summary_id,
                source_title=(text[:100] if text else None),
            )

    logger.info(f"extract_from_summary: summary_id={summary_id}, 新增实体={entities_added}, 新增关系={relationships_added}")
    return {
        "entities_added": entities_added,
        "relationships_added": relationships_added,
        "summary_id": summary_id,
        "status": "done",
    }

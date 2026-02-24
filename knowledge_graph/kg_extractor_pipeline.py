"""KG 提取管线 — Pipeline B
从 extracted_texts 读取文本，通过 Split→Map→Reduce 管线提取实体和关系，
写入本地 kg_entities/kg_relationships，更新 extracted_texts.kg_status。
"""
import logging
from utils.db_utils import execute_cloud_query, execute_cloud_insert
from utils.model_router import call_model_json
from knowledge_graph.kg_manager import add_entity, add_relationship, find_entity
from knowledge_graph.kg_updater import KG_EXTRACTION_PROMPT
from knowledge_graph.kg_name_normalizer import normalize_entity_name
from knowledge_graph.kg_schema import (
    VALID_ENTITY_TYPES, VALID_RELATION_TYPES, RELATION_TO_CATEGORY,
)

logger = logging.getLogger(__name__)

CHUNK_SIZE = 3000  # 每个分片的最大字符数


def extract_kg_from_text(extracted_text_id: int) -> dict:
    """对单条 extracted_text 执行 KG 提取（Split→Map→Reduce）

    Returns:
        {"entities": int, "relationships": int, "status": "done"|"failed"}
    """
    rows = execute_cloud_query(
        "SELECT id, full_text, source FROM extracted_texts WHERE id=%s",
        [extracted_text_id],
    )
    if not rows:
        return {"entities": 0, "relationships": 0, "status": "failed"}

    full_text = rows[0]["full_text"]

    # Split
    chunks = _split_text(full_text, CHUNK_SIZE)

    # Map: 每个分片独立提取
    all_entities = []
    all_relationships = []
    all_triples = []
    for chunk in chunks:
        try:
            result = call_model_json(
                'kg',
                KG_EXTRACTION_PROMPT,
                chunk,
                max_tokens=2048,
                timeout=120,
            )
            if isinstance(result, list):
                # 新格式：三元组数组
                all_triples.extend(result)
            elif isinstance(result, dict):
                # 旧格式：分离的 entities/relationships
                all_entities.extend(result.get("entities", []))
                all_relationships.extend(result.get("relationships", []))
        except Exception as e:
            logger.warning(f"KG 提取分片失败 id={extracted_text_id}: {e}")

    # Reduce: 去重合并，写入 KG
    added_e, added_r = _reduce_and_write(all_entities, all_relationships, all_triples, extracted_text_id=extracted_text_id)

    status = "done" if (added_e + added_r > 0 or len(chunks) > 0) else "failed"
    execute_cloud_insert(
        "UPDATE extracted_texts SET kg_status=%s WHERE id=%s",
        [status, extracted_text_id],
    )

    return {"entities": added_e, "relationships": added_r, "status": status}


def _split_text(text: str, chunk_size: int) -> list:
    """将长文本按段落分片"""
    if len(text) <= chunk_size:
        return [text]

    paragraphs = text.split("\n\n")
    chunks = []
    current = []
    current_len = 0

    for para in paragraphs:
        if current_len + len(para) > chunk_size and current:
            chunks.append("\n\n".join(current))
            current = [para]
            current_len = len(para)
        else:
            current.append(para)
            current_len += len(para)

    if current:
        chunks.append("\n\n".join(current))

    return chunks or [text[:chunk_size]]


def _reduce_and_write(entities: list, relationships: list, triples: list = None, extracted_text_id: int = None) -> tuple:
    """去重合并实体和关系，写入本地 KG

    支持两种格式：
    - 旧格式：entities + relationships 分离
    - 新格式：triples 三元组数组 [{source: {name, type}, relation, target: {name, type}, ...}]
    """
    added_e = 0
    added_r = 0
    name_to_id = {}

    # ── 处理新格式三元组 ──
    if triples:
        seen_names = set()
        seen_rels = set()
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
            src_type = src_info.get("type", "industry")
            tgt_type = tgt_info.get("type", "industry")

            # 规范化实体名（去掉定语/事件后缀/价格后缀等）
            src_name_norm = normalize_entity_name(src_type, src_name)
            tgt_name_norm = normalize_entity_name(tgt_type, tgt_name)
            if not src_name_norm or not tgt_name_norm:
                continue
            src_name = src_name_norm
            tgt_name = tgt_name_norm

            if not src_name or not tgt_name or len(src_name) < 2 or len(tgt_name) < 2:
                continue
            if rel_type not in VALID_RELATION_TYPES:
                rel_type = "related"

            rel_key = (src_name, tgt_name, rel_type)
            if rel_key in seen_rels:
                continue
            seen_rels.add(rel_key)

            # 确保实体存在
            for ename, etype_raw in [(src_name, src_info.get("type", "industry")),
                                      (tgt_name, tgt_info.get("type", "industry"))]:
                if ename in name_to_id:
                    continue
                etype = etype_raw if etype_raw in VALID_ENTITY_TYPES else "industry"
                if ename not in seen_names:
                    seen_names.add(ename)
                    eid = add_entity(etype, ename, description=src_info.get("description") if ename == src_name else tgt_info.get("description"))
                    if eid:
                        name_to_id[ename] = eid
                        added_e += 1
                    else:
                        found = find_entity(ename)
                        if found:
                            name_to_id[ename] = found[0]["id"]

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

            category = RELATION_TO_CATEGORY.get(rel_type, "structural")
            strength = min(max(float(triple.get("strength", 0.5)), 0.1), 1.0)
            direction = triple.get("direction", "neutral")
            if direction not in ("positive", "negative", "neutral"):
                direction = "neutral"

            time_lag = triple.get("time_lag")
            if time_lag not in ("immediate", "short", "medium", "long", None):
                time_lag = None

            certainty = triple.get("certainty")
            if certainty not in ("deterministic", "probabilistic", None):
                certainty = None

            rid = add_relationship(
                src_id, tgt_id, rel_type,
                strength=strength, direction=direction,
                evidence=triple.get("evidence", ""),
                confidence=0.7,
                relation_category=category,
                time_lag=time_lag,
                certainty=certainty,
            )
            if rid:
                added_r += 1
                if extracted_text_id:
                    from knowledge_graph.kg_manager import write_triple_source
                    write_triple_source(
                        relationship_id=rid,
                        source_entity_id=src_id,
                        target_entity_id=tgt_id,
                        source_type='extracted_text',
                        source_id=extracted_text_id,
                        extracted_text_id=extracted_text_id,
                    )

    # ── 处理旧格式（entities + relationships 分离）──
    seen_names = set()
    for ent in entities:
        ename = (ent.get("name") or "").strip()
        etype = ent.get("type", "industry")
        if not ename or len(ename) < 2 or ename in seen_names:
            continue
        if etype not in VALID_ENTITY_TYPES:
            etype = "industry"
        seen_names.add(ename)
        eid = add_entity(etype, ename, description=ent.get("description"))
        if eid:
            name_to_id[ename] = eid
            added_e += 1

    # 去重关系（按 source+target+type 去重）
    seen_rels = set()
    for rel in relationships:
        src_name = (rel.get("source") or "").strip()
        tgt_name = (rel.get("target") or "").strip()
        rel_type = rel.get("type", "related")

        if not src_name or not tgt_name:
            continue
        if rel_type not in VALID_RELATION_TYPES:
            rel_type = "related"

        rel_key = (src_name, tgt_name, rel_type)
        if rel_key in seen_rels:
            continue
        seen_rels.add(rel_key)

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
        )
        if rid:
            added_r += 1
            if extracted_text_id:
                from knowledge_graph.kg_manager import write_triple_source
                write_triple_source(
                    relationship_id=rid,
                    source_entity_id=src_id,
                    target_entity_id=tgt_id,
                    source_type='extracted_text',
                    source_id=extracted_text_id,
                    extracted_text_id=extracted_text_id,
                )

    return added_e, added_r


def batch_extract_kg(limit: int = 30, workers: int = 2, progress_callback=None) -> dict:
    """批量 KG 提取（Pipeline B）

    Args:
        limit: 最大处理条数
        workers: 并发 worker 数（建议 ≤3，避免 API 限流）
        progress_callback: callback(current, total, msg)
    Returns:
        {"entities": int, "relationships": int, "done": int, "failed": int, "run_id": int}
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    from utils.db_utils import execute_cloud_insert as cloud_insert

    run_id = cloud_insert(
        "INSERT INTO pipeline_runs (pipeline_name, stage) VALUES ('batch_extract_kg', 'kg_extraction')"
    )

    pending = execute_cloud_query(
        """SELECT id FROM extracted_texts
           WHERE kg_status='pending' AND extract_quality='pass'
           ORDER BY extract_time ASC LIMIT %s""",
        [limit],
    )

    total = len(pending)
    counter = {"entities": 0, "relationships": 0, "done": 0, "failed": 0}
    lock = threading.Lock()

    def _process(item):
        eid = item["id"]
        try:
            result = extract_kg_from_text(eid)
            with lock:
                counter["entities"] += result["entities"]
                counter["relationships"] += result["relationships"]
                if result["status"] == "done":
                    counter["done"] += 1
                else:
                    counter["failed"] += 1
                if progress_callback:
                    progress_callback(
                        counter["done"] + counter["failed"], total,
                        f"KG提取 id={eid}: +{result['entities']}实体 +{result['relationships']}关系"
                    )
        except Exception as e:
            logger.error(f"KG提取异常 id={eid}: {e}")
            execute_cloud_insert(
                "UPDATE extracted_texts SET kg_status='failed' WHERE id=%s", [eid]
            )
            with lock:
                counter["failed"] += 1

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_process, item) for item in pending]
        for f in as_completed(futures):
            f.result()

    status = "success" if counter["failed"] == 0 else (
        "failed" if counter["done"] == 0 else "partial")
    cloud_insert(
        """UPDATE pipeline_runs SET finished_at=NOW(),
           status=%s, items_processed=%s, details_json=%s
           WHERE id=%s""",
        [status, counter["done"] + counter["failed"],
         f'{{"entities": {counter["entities"]}, "relationships": {counter["relationships"]}, '
         f'"done": {counter["done"]}, "failed": {counter["failed"]}}}',
         run_id],
    )

    logger.info(f"批量KG提取完成: 实体+{counter['entities']}, 关系+{counter['relationships']}, "
                f"成功{counter['done']}, 失败{counter['failed']}")
    return {**counter, "total": total, "run_id": run_id}

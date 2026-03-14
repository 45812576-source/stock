"""KG 人工审核业务层

审核对象三层：
  - 实体（kg_entities）
  - 关系（kg_relationships）
  - 三元组佐证来源（kg_triple_sources）—— 支撑关系成立的原始文档

data_admin 可以标记任意层为 pending_approval，super_admin 可以确认/驳回。

状态流转:
  unreviewed → pending_approval (data_admin 标记)
  pending_approval → approved    (super_admin 确认)
  pending_approval → rejected    (super_admin 驳回)
  unreviewed      → approved     (super_admin 直接标记)
  unreviewed      → rejected     (super_admin 直接标记)
  rejected/approved → unreviewed (super_admin revert)

质量传导链:
  kg_triple_sources.review_status → 影响关系质量系数
  kg_entities.review_status       → 影响关系质量系数 + chunk boost
  kg_relationships.review_status  → 直接影响关系质量系数
"""
import json
import logging
from datetime import datetime
from typing import Optional

from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)

VALID_STATUSES = {'unreviewed', 'pending_approval', 'approved', 'rejected'}


# ──────────────────────────────────────────────────────────────────────────────
# 统计信息
# ──────────────────────────────────────────────────────────────────────────────

def get_review_stats() -> dict:
    """获取审核统计：待审/已审/驳回/今日审核量（三层合计）"""
    def _to_dict(rows):
        return {r['review_status']: r['cnt'] for r in (rows or [])}

    e = _to_dict(execute_query("SELECT review_status, COUNT(*) AS cnt FROM kg_entities GROUP BY review_status"))
    r = _to_dict(execute_query("SELECT review_status, COUNT(*) AS cnt FROM kg_relationships GROUP BY review_status"))
    t = _to_dict(execute_query("SELECT review_status, COUNT(*) AS cnt FROM kg_triple_sources GROUP BY review_status"))

    today = datetime.now().strftime('%Y-%m-%d')
    today_count = execute_query(
        "SELECT COUNT(*) AS cnt FROM kg_review_log WHERE DATE(created_at) = %s",
        [today]
    )
    today_cnt = today_count[0]['cnt'] if today_count else 0

    def _sum(d, key): return d.get(key, 0)

    return {
        'entity': e,
        'relationship': r,
        'triple_source': t,
        'pending':  _sum(e,'pending_approval') + _sum(r,'pending_approval') + _sum(t,'pending_approval'),
        'approved': _sum(e,'approved')         + _sum(r,'approved')         + _sum(t,'approved'),
        'rejected': _sum(e,'rejected')         + _sum(r,'rejected')         + _sum(t,'rejected'),
        'today_reviewed': today_cnt,
    }


# ──────────────────────────────────────────────────────────────────────────────
# 审核队列
# ──────────────────────────────────────────────────────────────────────────────

def get_review_queue(
    target_type: str = 'all',        # 'entity' | 'relationship' | 'triple_source' | 'all'
    status_filter: str = 'all',      # 'unreviewed' | 'pending_approval' | 'approved' | 'rejected' | 'all'
    entity_type_filter: str = '',
    relation_type_filter: str = '',
    keyword: str = '',
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """获取审核队列（实体 + 关系 + 三元组佐证来源混合列表）"""
    results = []
    total = 0

    def _status_clause(status, col='review_status'):
        if status != 'all' and status in VALID_STATUSES:
            return f"AND {col} = '{status}'"
        return ""

    # 实体
    if target_type in ('entity', 'all') and target_type != 'triple_source':
        entity_type_clause = ""
        if entity_type_filter:
            entity_type_clause = f"AND entity_type = '{entity_type_filter}'"
        kw_clause = ""
        params = []
        if keyword:
            kw_clause = "AND entity_name LIKE %s"
            params.append(f'%{keyword}%')

        count_sql = f"""
            SELECT COUNT(*) AS cnt FROM kg_entities
            WHERE 1=1 {_status_clause(status_filter)} {entity_type_clause} {kw_clause}
        """
        cnt = execute_query(count_sql, params or None)
        entity_total = cnt[0]['cnt'] if cnt else 0
        total += entity_total

        rows = execute_query(f"""
            SELECT id, entity_name, entity_type, review_status, review_note,
                   reviewed_by, reviewed_at, approved_by, approved_at,
                   description, created_at
            FROM kg_entities
            WHERE 1=1 {_status_clause(status_filter)} {entity_type_clause} {kw_clause}
            ORDER BY
              CASE review_status
                WHEN 'pending_approval' THEN 0
                WHEN 'unreviewed' THEN 1
                WHEN 'rejected' THEN 2
                WHEN 'approved' THEN 3
              END,
              created_at DESC
            LIMIT %s OFFSET %s
        """, (params or []) + [limit, offset])

        for r in (rows or []):
            d = dict(r)
            d['target_type'] = 'entity'
            results.append(d)

    # 关系
    if target_type in ('relationship', 'all') and target_type != 'triple_source':
        kw_clause = ""
        rtype_clause = ""
        params = []
        if keyword:
            kw_clause = "AND (e1.entity_name LIKE %s OR e2.entity_name LIKE %s OR r.relation_type LIKE %s)"
            params.extend([f'%{keyword}%', f'%{keyword}%', f'%{keyword}%'])
        if relation_type_filter:
            rtype_clause = f"AND r.relation_type = '{relation_type_filter}'"

        count_sql = f"""
            SELECT COUNT(*) AS cnt
            FROM kg_relationships r
            JOIN kg_entities e1 ON r.source_entity_id = e1.id
            JOIN kg_entities e2 ON r.target_entity_id = e2.id
            WHERE 1=1 {_status_clause(status_filter, 'r.review_status')} {rtype_clause} {kw_clause}
        """
        cnt = execute_query(count_sql, params or None)
        rel_total = cnt[0]['cnt'] if cnt else 0
        total += rel_total

        rows = execute_query(f"""
            SELECT r.id, r.relation_type, r.review_status, r.review_note,
                   r.reviewed_by, r.reviewed_at, r.approved_by, r.approved_at,
                   r.strength, r.confidence, r.direction, r.evidence,
                   e1.entity_name AS src_name, e1.entity_type AS src_type,
                   e2.entity_name AS tgt_name, e2.entity_type AS tgt_type,
                   r.source_entity_id, r.target_entity_id,
                   r.created_at
            FROM kg_relationships r
            JOIN kg_entities e1 ON r.source_entity_id = e1.id
            JOIN kg_entities e2 ON r.target_entity_id = e2.id
            WHERE 1=1 {_status_clause(status_filter, 'r.review_status')} {rtype_clause} {kw_clause}
            ORDER BY
              CASE r.review_status
                WHEN 'pending_approval' THEN 0
                WHEN 'unreviewed' THEN 1
                WHEN 'rejected' THEN 2
                WHEN 'approved' THEN 3
              END,
              r.created_at DESC
            LIMIT %s OFFSET %s
        """, (params or []) + [limit, offset])

        for r in (rows or []):
            d = dict(r)
            d['target_type'] = 'relationship'
            results.append(d)

    # 三元组佐证来源
    if target_type in ('triple_source', 'all'):
        kw_clause = ""
        params = []
        if keyword:
            kw_clause = "AND (ts.source_title LIKE %s OR e1.entity_name LIKE %s OR e2.entity_name LIKE %s)"
            params.extend([f'%{keyword}%', f'%{keyword}%', f'%{keyword}%'])

        count_sql = f"""
            SELECT COUNT(*) AS cnt
            FROM kg_triple_sources ts
            JOIN kg_entities e1 ON ts.source_entity_id = e1.id
            JOIN kg_entities e2 ON ts.target_entity_id = e2.id
            WHERE 1=1 {_status_clause(status_filter, 'ts.review_status')} {kw_clause}
        """
        cnt = execute_query(count_sql, params or None)
        triple_total = cnt[0]['cnt'] if cnt else 0
        total += triple_total

        rows = execute_query(f"""
            SELECT ts.id, ts.relationship_id, ts.source_type, ts.source_id,
                   ts.source_title, ts.source_time, ts.review_status, ts.review_note,
                   ts.reviewed_by, ts.reviewed_at, ts.created_at,
                   e1.entity_name AS src_name, e1.entity_type AS src_type,
                   e2.entity_name AS tgt_name, e2.entity_type AS tgt_type,
                   r.relation_type
            FROM kg_triple_sources ts
            JOIN kg_entities e1 ON ts.source_entity_id = e1.id
            JOIN kg_entities e2 ON ts.target_entity_id = e2.id
            JOIN kg_relationships r ON ts.relationship_id = r.id
            WHERE 1=1 {_status_clause(status_filter, 'ts.review_status')} {kw_clause}
            ORDER BY
              CASE ts.review_status
                WHEN 'pending_approval' THEN 0
                WHEN 'unreviewed' THEN 1
                WHEN 'rejected' THEN 2
                WHEN 'approved' THEN 3
              END,
              ts.created_at DESC
            LIMIT %s OFFSET %s
        """, (params or []) + [limit, offset])

        for r in (rows or []):
            d = dict(r)
            d['target_type'] = 'triple_source'
            results.append(d)

    return {'items': results, 'total': total}


# ──────────────────────────────────────────────────────────────────────────────
# 审核操作（data_admin）
# ──────────────────────────────────────────────────────────────────────────────

def mark_pending(
    target_type: str,
    target_id: int,
    user_id: int,
    user_role: str,
    note: str = '',
) -> bool:
    """data_admin 标记为 pending_approval，记录操作日志"""
    old = _get_current(target_type, target_id)
    if not old:
        return False

    table = _get_table(target_type)
    execute_insert(f"""
        UPDATE {table}
        SET review_status = 'pending_approval',
            review_note = %s,
            reviewed_by = %s,
            reviewed_at = %s
        WHERE id = %s
    """, [note or old.get('review_note', ''), user_id, datetime.now(), target_id])

    _write_log(
        target_type=target_type,
        target_id=target_id,
        action='approve',  # data_admin "approve" = 标记提交审批
        old_values=old,
        new_values={'review_status': 'pending_approval', 'review_note': note},
        note=note,
        user_id=user_id,
        user_role=user_role,
    )
    return True


# ──────────────────────────────────────────────────────────────────────────────
# 审批操作（super_admin）
# ──────────────────────────────────────────────────────────────────────────────

def approve(
    target_type: str,
    target_id: int,
    user_id: int,
    user_role: str,
    note: str = '',
) -> bool:
    """super_admin 批准"""
    old = _get_current(target_type, target_id)
    if not old:
        return False

    table = _get_table(target_type)
    now = datetime.now()
    if target_type == 'triple_source':
        # triple_source 没有 approved_by/approved_at，复用 reviewed_by/reviewed_at
        execute_insert(f"""
            UPDATE {table}
            SET review_status = 'approved',
                review_note = %s,
                reviewed_by = %s,
                reviewed_at = %s
            WHERE id = %s
        """, [note, user_id, now, target_id])
    else:
        execute_insert(f"""
            UPDATE {table}
            SET review_status = 'approved',
                review_note = %s,
                approved_by = %s,
                approved_at = %s
            WHERE id = %s
        """, [note, user_id, now, target_id])

    _write_log(
        target_type=target_type,
        target_id=target_id,
        action='approve',
        old_values=old,
        new_values={'review_status': 'approved', 'review_note': note},
        note=note,
        user_id=user_id,
        user_role=user_role,
    )
    return True


def reject(
    target_type: str,
    target_id: int,
    user_id: int,
    user_role: str,
    note: str = '',
) -> bool:
    """super_admin 驳回"""
    old = _get_current(target_type, target_id)
    if not old:
        return False

    table = _get_table(target_type)
    now = datetime.now()
    if target_type == 'triple_source':
        execute_insert(f"""
            UPDATE {table}
            SET review_status = 'rejected',
                review_note = %s,
                reviewed_by = %s,
                reviewed_at = %s
            WHERE id = %s
        """, [note, user_id, now, target_id])
    else:
        execute_insert(f"""
            UPDATE {table}
            SET review_status = 'rejected',
                review_note = %s,
                approved_by = %s,
                approved_at = %s
            WHERE id = %s
        """, [note, user_id, now, target_id])

    _write_log(
        target_type=target_type,
        target_id=target_id,
        action='reject',
        old_values=old,
        new_values={'review_status': 'rejected', 'review_note': note},
        note=note,
        user_id=user_id,
        user_role=user_role,
    )
    return True


def revert(
    target_type: str,
    target_id: int,
    user_id: int,
    user_role: str,
    log_id: Optional[int] = None,
) -> bool:
    """super_admin 驳回时可选 revert 到 old_values（从 kg_review_log 取快照）"""
    old = _get_current(target_type, target_id)
    if not old:
        return False

    old_values = None
    if log_id:
        log_row = execute_query(
            "SELECT old_values FROM kg_review_log WHERE id = %s AND target_type = %s AND target_id = %s",
            [log_id, target_type, target_id]
        )
        if log_row and log_row[0]['old_values']:
            try:
                old_values = json.loads(log_row[0]['old_values'])
            except Exception:
                pass

    table = _get_table(target_type)

    if target_type == 'entity' and old_values:
        execute_insert(f"""
            UPDATE {table}
            SET entity_name = %s,
                entity_type = %s,
                review_status = 'unreviewed',
                review_note = NULL,
                reviewed_by = NULL,
                reviewed_at = NULL,
                approved_by = NULL,
                approved_at = NULL
            WHERE id = %s
        """, [
            old_values.get('entity_name', old['entity_name']),
            old_values.get('entity_type', old['entity_type']),
            target_id,
        ])
    elif target_type == 'triple_source':
        execute_insert(f"""
            UPDATE {table}
            SET review_status = 'unreviewed',
                review_note = NULL,
                reviewed_by = NULL,
                reviewed_at = NULL
            WHERE id = %s
        """, [target_id])
    else:
        execute_insert(f"""
            UPDATE {table}
            SET review_status = 'unreviewed',
                review_note = NULL,
                reviewed_by = NULL,
                reviewed_at = NULL,
                approved_by = NULL,
                approved_at = NULL
            WHERE id = %s
        """, [target_id])

    _write_log(
        target_type=target_type,
        target_id=target_id,
        action='revert',
        old_values=old,
        new_values={'review_status': 'unreviewed'},
        note='revert',
        user_id=user_id,
        user_role=user_role,
    )
    return True


def batch_approve(
    items: list[dict],   # [{'target_type': 'entity'|'relationship', 'target_id': int}, ...]
    user_id: int,
    user_role: str,
    action: str = 'approve',  # 'approve' | 'reject'
    note: str = '',
) -> dict:
    """批量审批/驳回"""
    success = 0
    failed = 0
    for item in items:
        t = item.get('target_type')
        tid = item.get('target_id')
        if not t or not tid:
            failed += 1
            continue
        try:
            if action == 'approve':
                ok = approve(t, tid, user_id, user_role, note)
            else:
                ok = reject(t, tid, user_id, user_role, note)
            if ok:
                success += 1
            else:
                failed += 1
        except Exception as e:
            logger.warning(f"batch {action} 失败 {t}/{tid}: {e}")
            failed += 1
    return {'success': success, 'failed': failed}


# ──────────────────────────────────────────────────────────────────────────────
# 编辑操作（data_admin — 改名/改类型/改关系属性）
# ──────────────────────────────────────────────────────────────────────────────

def edit_entity(
    entity_id: int,
    user_id: int,
    user_role: str,
    new_name: Optional[str] = None,
    new_type: Optional[str] = None,
    new_description: Optional[str] = None,
    note: str = '',
) -> bool:
    """编辑实体 — 修改立即生效，status → pending_approval，记 log"""
    old = _get_current('entity', entity_id)
    if not old:
        return False

    updates = []
    params = []
    if new_name and new_name != old.get('entity_name'):
        updates.append('entity_name = %s')
        params.append(new_name)
    if new_type and new_type != old.get('entity_type'):
        updates.append('entity_type = %s')
        params.append(new_type)
    if new_description is not None:
        updates.append('description = %s')
        params.append(new_description)

    if not updates:
        return True  # nothing to change

    updates += ['review_status = %s', 'review_note = %s', 'reviewed_by = %s', 'reviewed_at = %s']
    params += ['pending_approval', note, user_id, datetime.now()]
    params.append(entity_id)

    try:
        execute_insert(
            f"UPDATE kg_entities SET {', '.join(updates)} WHERE id = %s",
            params
        )
    except Exception as e:
        err_str = str(e)
        if '1062' in err_str or 'Duplicate entry' in err_str:
            # 目标名称+类型已存在，给出友好提示
            target_name = new_name or old.get('entity_name', '')
            target_type = new_type or old.get('entity_type', '')
            raise ValueError(f"实体 [{target_type}] {target_name} 已存在，无法重命名。请先合并或使用其他名称。")
        raise

    new_snapshot = {k: v for k, v in [
        ('entity_name', new_name),
        ('entity_type', new_type),
        ('description', new_description),
        ('review_status', 'pending_approval'),
    ] if v is not None}

    _write_log(
        target_type='entity',
        target_id=entity_id,
        action='edit',
        old_values=old,
        new_values=new_snapshot,
        note=note,
        user_id=user_id,
        user_role=user_role,
    )
    return True


def edit_relationship(
    rel_id: int,
    user_id: int,
    user_role: str,
    new_relation_type: Optional[str] = None,
    new_strength: Optional[float] = None,
    new_confidence: Optional[float] = None,
    new_direction: Optional[str] = None,
    new_evidence: Optional[str] = None,
    note: str = '',
) -> bool:
    """编辑关系 — 修改立即生效，status → pending_approval，记 log"""
    old = _get_current('relationship', rel_id)
    if not old:
        return False

    updates = []
    params = []
    if new_relation_type and new_relation_type != old.get('relation_type'):
        updates.append('relation_type = %s')
        params.append(new_relation_type)
    if new_strength is not None:
        updates.append('strength = %s')
        params.append(new_strength)
    if new_confidence is not None:
        updates.append('confidence = %s')
        params.append(new_confidence)
    if new_direction is not None:
        updates.append('direction = %s')
        params.append(new_direction)
    if new_evidence is not None:
        updates.append('evidence = %s')
        params.append(new_evidence)

    if not updates:
        return True

    updates += ['review_status = %s', 'review_note = %s', 'reviewed_by = %s', 'reviewed_at = %s']
    params += ['pending_approval', note, user_id, datetime.now()]
    params.append(rel_id)

    execute_insert(
        f"UPDATE kg_relationships SET {', '.join(updates)} WHERE id = %s",
        params
    )

    _write_log(
        target_type='relationship',
        target_id=rel_id,
        action='edit',
        old_values=old,
        new_values={'review_status': 'pending_approval'},
        note=note,
        user_id=user_id,
        user_role=user_role,
    )
    return True


# ──────────────────────────────────────────────────────────────────────────────
# 审核历史
# ──────────────────────────────────────────────────────────────────────────────

def get_review_log(
    target_type: Optional[str] = None,
    target_id: Optional[int] = None,
    limit: int = 50,
) -> list[dict]:
    """获取审核历史日志"""
    conditions = []
    params = []
    if target_type:
        conditions.append('target_type = %s')
        params.append(target_type)
    if target_id:
        conditions.append('target_id = %s')
        params.append(target_id)

    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
    params.append(limit)

    rows = execute_query(f"""
        SELECT id, target_type, target_id, action, old_values, new_values,
               note, user_id, user_role, created_at
        FROM kg_review_log
        {where}
        ORDER BY created_at DESC
        LIMIT %s
    """, params or None)

    result = []
    for r in (rows or []):
        d = dict(r)
        # JSON 列反序列化
        for col in ('old_values', 'new_values'):
            if isinstance(d.get(col), str):
                try:
                    d[col] = json.loads(d[col])
                except Exception:
                    pass
        result.append(d)
    return result


# ──────────────────────────────────────────────────────────────────────────────
# 关联 chunks（审核详情展开用）
# ──────────────────────────────────────────────────────────────────────────────

def get_entity_chunks(entity_id: int, limit: int = 5) -> list[dict]:
    """获取某实体关联的 chunk 原文（用于审核详情展示）"""
    rows = execute_query("""
        SELECT tc.id, tc.chunk_text, tc.doc_type, tc.publish_time, tc.source_doc_title
        FROM text_chunks tc
        JOIN chunk_entities ce ON tc.id = ce.chunk_id
        WHERE ce.entity_id = %s
        ORDER BY tc.publish_time DESC
        LIMIT %s
    """, [entity_id, limit])
    return [dict(r) for r in (rows or [])]


def get_relationship_chunks(rel_id: int, limit: int = 5) -> list[dict]:
    """获取某关系关联的 chunk 原文（通过 kg_triple_chunks）"""
    rows = execute_query("""
        SELECT tc.id, tc.chunk_text, tc.doc_type, tc.publish_time, tc.source_doc_title,
               ktc.confidence
        FROM text_chunks tc
        JOIN kg_triple_chunks ktc ON tc.id = ktc.chunk_id
        WHERE ktc.relationship_id = %s
        ORDER BY ktc.confidence DESC
        LIMIT %s
    """, [rel_id, limit])
    return [dict(r) for r in (rows or [])]


# ──────────────────────────────────────────────────────────────────────────────
# 内部辅助
# ──────────────────────────────────────────────────────────────────────────────

def _get_table(target_type: str) -> str:
    return {'entity': 'kg_entities', 'relationship': 'kg_relationships', 'triple_source': 'kg_triple_sources'}[target_type]


def _get_current(target_type: str, target_id: int) -> Optional[dict]:
    """获取当前实体/关系/三元组来源的快照（用于写入 old_values）"""
    if target_type == 'entity':
        rows = execute_query(
            """SELECT id, entity_name, entity_type, review_status, review_note,
                      reviewed_by, reviewed_at, approved_by, approved_at, description
               FROM kg_entities WHERE id = %s""",
            [target_id]
        )
    elif target_type == 'relationship':
        rows = execute_query(
            """SELECT id, relation_type, review_status, review_note,
                      reviewed_by, reviewed_at, approved_by, approved_at,
                      strength, confidence, direction, evidence,
                      source_entity_id, target_entity_id
               FROM kg_relationships WHERE id = %s""",
            [target_id]
        )
    else:  # triple_source
        rows = execute_query(
            """SELECT id, relationship_id, source_type, source_id, source_title,
                      review_status, review_note, reviewed_by, reviewed_at
               FROM kg_triple_sources WHERE id = %s""",
            [target_id]
        )
    if not rows:
        return None
    d = dict(rows[0])
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
    return d


def _write_log(
    target_type: str,
    target_id: int,
    action: str,
    old_values: Optional[dict],
    new_values: Optional[dict],
    note: str,
    user_id: int,
    user_role: str,
) -> None:
    """写入审核历史日志"""
    try:
        execute_insert("""
            INSERT INTO kg_review_log
              (target_type, target_id, action, old_values, new_values, note, user_id, user_role)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, [
            target_type,
            target_id,
            action,
            json.dumps(old_values, ensure_ascii=False, default=str) if old_values else None,
            json.dumps(new_values, ensure_ascii=False, default=str) if new_values else None,
            note,
            user_id,
            user_role,
        ])
    except Exception as e:
        logger.warning(f"写入 kg_review_log 失败: {e}")

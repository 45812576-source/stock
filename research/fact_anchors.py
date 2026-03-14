"""P3: 事实锚点扩大 — 从族1摘要的 key_data 中提取硬数据

优先级：
  1. financial_reports 表 + stock_business_composition（已有，最权威）
  2. 族1摘要 key_data（本模块，AI 从公告/财报中提取的硬数据，覆盖面更广）
  3. 无锚点（AI 自由发挥）

使用方式：
    from research.fact_anchors import get_summary_fact_anchors
    extra = get_summary_fact_anchors(stock_code)
    if extra:
        prompt_input += extra
"""
import json
import logging

logger = logging.getLogger(__name__)

# doc_type → 中文标签
_DOC_TYPE_LABEL = {
    "financial_report": "财报",
    "announcement":     "公告",
    "data_release":     "数据",
    "policy_doc":       "政策",
}


def get_summary_fact_anchors(stock_code: str, limit: int = 5) -> str:
    """从族1摘要的 key_data / key_facts 提取事实锚点

    查 content_summaries（云端）JOIN stock_mentions，找到与该股票相关的族1摘要，
    解析 type_fields JSON 中的 key_data / key_facts 字段，格式化为事实锚点文本。

    Args:
        stock_code: 股票代码，如 "600519.SH"
        limit: 最多取最近几条（默认5条）
    Returns:
        格式化的事实锚点文本，如：
        === 事实锚点（来自公告/财报，不可违反）===
        [财报 2024-09-30] 营收: 1234亿, 净利润: 567亿
        空字符串表示无可用数据
    """
    if not stock_code:
        return ""

    try:
        from utils.db_utils import execute_query

        rows = execute_query("""
            SELECT cs.doc_type, cs.summary, cs.type_fields, cs.created_at
            FROM content_summaries cs
            JOIN stock_mentions sm ON cs.extracted_text_id = sm.extracted_text_id
            WHERE sm.stock_code = %s AND cs.family = 1
            ORDER BY cs.created_at DESC
            LIMIT %s
        """, [stock_code, limit])

        if not rows:
            return ""

        anchors = []
        for r in rows:
            raw_tf = r.get("type_fields")
            if isinstance(raw_tf, str):
                try:
                    tf = json.loads(raw_tf)
                except Exception:
                    tf = {}
            elif isinstance(raw_tf, dict):
                tf = raw_tf
            else:
                tf = {}

            # 优先取 key_data，再取 key_facts
            kd = tf.get("key_data") or tf.get("key_facts")
            if not kd:
                continue

            doc_label = _DOC_TYPE_LABEL.get(r.get("doc_type", ""), "文档")
            date_str = tf.get("effective_date") or str(r.get("created_at", ""))[:10]

            # 格式化 key_data（可能是 dict 或 list）
            data_parts = []
            if isinstance(kd, dict):
                for k, v in kd.items():
                    if v is not None and str(v).strip():
                        data_parts.append(f"{k}: {v}")
            elif isinstance(kd, list):
                for item in kd:
                    if isinstance(item, dict):
                        k = item.get("name") or item.get("key") or ""
                        v = item.get("value") or item.get("val") or ""
                        if k and v:
                            data_parts.append(f"{k}: {v}")
                    elif isinstance(item, str) and item.strip():
                        data_parts.append(item)
            elif isinstance(kd, str) and kd.strip():
                data_parts.append(kd)

            if data_parts:
                anchors.append(f"[{doc_label} {date_str}] {', '.join(data_parts[:6])}")

        if not anchors:
            return ""

        return "=== 事实锚点（来自公告/财报，不可违反）===\n" + "\n".join(anchors)

    except Exception as e:
        logger.warning(f"get_summary_fact_anchors 失败 stock_code={stock_code}: {e}")
        return ""

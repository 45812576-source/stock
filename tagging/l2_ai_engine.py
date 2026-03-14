"""L2 AI 轻量标注引擎 — 基于 stock_mentions/content_summaries 已有数据

反转视角：遍历 stock_mentions，让 AI 判断每条信息匹配哪些 L2 规则
"""
import json
import logging

logger = logging.getLogger(__name__)


def _q(sql, params=None):
    from utils.db_utils import execute_cloud_query
    return execute_cloud_query(sql, params or []) or []


def _lq(sql, params=None):
    from utils.db_utils import execute_query
    return execute_query(sql, params or []) or []


def _get_l2_rules():
    """获取所有 L2 规则"""
    rows = _lq(
        "SELECT id, rule_name, category, definition FROM stock_selection_rules WHERE layer=2 AND is_active=1"
    )
    return [dict(r) for r in rows]


def _build_l2_prompt(rules: list, mentions_batch: list) -> str:
    rules_text = "\n".join(
        f"- {r['rule_name']}（{r['category']}）: {r['definition'][:100]}..."
        for r in rules
    )
    mentions_text = "\n\n".join(
        f"[{i+1}] 股票:{m.get('stock_name','?')}({m.get('stock_code','?')}) "
        f"主题:{m.get('related_themes','')} 事件:{m.get('related_events','')} "
        f"逻辑:{m.get('theme_logic','')}"
        for i, m in enumerate(mentions_batch)
    )
    return f"""你是一个量化选股规则标注专家。根据以下股票提及信息，判断每条信息匹配哪些选股规则。

## 选股规则列表
{rules_text}

## 股票提及信息
{mentions_text}

## 输出要求
返回 JSON 数组，每个元素格式：
{{"stock_code": "600519", "rule_name": "规则名称", "confidence": 0.8, "evidence": "匹配依据简述"}}

只返回 confidence >= 0.6 的匹配项。如果没有匹配，返回空数组 []。
只返回 JSON，不要其他文字。"""


def run_l2_for_stock(stock_code: str) -> dict:
    """对单只股票执行 L2 AI 轻量标注（基于该股的 stock_mentions）"""
    from utils.db_utils import execute_insert
    from utils.model_router import call_model_json

    rules = _get_l2_rules()
    if not rules:
        return {"error": "没有 L2 规则", "tagged": 0}

    mentions = _q(
        """SELECT sm.id, sm.stock_name, sm.stock_code, sm.related_themes,
                  sm.related_events, sm.theme_logic, sm.mention_time
           FROM stock_mentions sm
           WHERE sm.stock_code=%s
           ORDER BY sm.id DESC LIMIT 20""",
        [stock_code],
    )
    if not mentions:
        return {"processed": 0, "tagged": 0, "message": "该股暂无 stock_mentions 数据"}

    rule_id_map = {r["rule_name"]: r for r in rules}
    batch = [dict(m) for m in mentions]
    prompt = _build_l2_prompt(rules, batch)

    try:
        result = call_model_json(
            "tagging",
            "你是选股规则标注专家，只返回 JSON。",
            prompt,
            max_tokens=2000,
            timeout=60,
        )
        if not isinstance(result, list):
            result = []
    except Exception as e:
        logger.warning(f"L2 AI call failed {stock_code}: {e}")
        return {"error": str(e), "tagged": 0}

    tagged = 0
    for item in result:
        rule_name = item.get("rule_name", "")
        confidence = float(item.get("confidence", 0.6))
        evidence = item.get("evidence", "")
        rule_info = rule_id_map.get(rule_name)
        if not rule_info or confidence < 0.6:
            continue
        try:
            execute_insert(
                """INSERT INTO stock_rule_tags
                   (stock_code, rule_id, rule_category, rule_name, matched, confidence, evidence, layer, computed_at)
                   VALUES (%s, %s, %s, %s, 1, %s, %s, 2, NOW())
                   ON DUPLICATE KEY UPDATE
                     matched=1, confidence=GREATEST(confidence, VALUES(confidence)),
                     evidence=VALUES(evidence), computed_at=NOW()""",
                [stock_code, rule_info["id"], rule_info["category"],
                 rule_name, confidence, evidence[:500]],
            )
            tagged += 1
        except Exception as e:
            logger.warning(f"L2 insert error {stock_code}/{rule_name}: {e}")

    return {"processed": len(batch), "tagged": tagged}


def run_l2_batch(limit: int = 50) -> dict:
    """处理最近 N 条未标注的 stock_mentions"""
    from utils.db_utils import execute_insert
    from utils.model_router import call_model_json

    rules = _get_l2_rules()
    if not rules:
        return {"error": "没有 L2 规则，请先种子初始化"}

    # 获取未处理的 stock_mentions（有 stock_code 的）
    mentions = _q(
        """SELECT sm.id, sm.stock_name, sm.stock_code, sm.related_themes,
                  sm.related_events, sm.theme_logic, sm.mention_time
           FROM stock_mentions sm
           WHERE sm.stock_code IS NOT NULL AND sm.stock_code != ''
           ORDER BY sm.id DESC LIMIT %s""",
        [limit],
    )
    if not mentions:
        return {"processed": 0, "tagged": 0, "message": "没有可处理的 stock_mentions"}

    # 获取规则 ID 映射
    rule_id_map = {r["rule_name"]: r for r in rules}

    # 分批处理（每批 10 条）
    batch_size = 10
    total_tagged = 0
    processed = 0

    for i in range(0, len(mentions), batch_size):
        batch = [dict(m) for m in mentions[i:i+batch_size]]
        prompt = _build_l2_prompt(rules, batch)
        try:
            result = call_model_json(
                "tagging",
                "你是选股规则标注专家，只返回 JSON。",
                prompt,
                max_tokens=2000,
                timeout=60,
            )
            if not isinstance(result, list):
                result = []
        except Exception as e:
            logger.warning(f"L2 AI call failed batch {i}: {e}")
            result = []

        for item in result:
            rule_name = item.get("rule_name", "")
            stock_code = item.get("stock_code", "")
            confidence = float(item.get("confidence", 0.6))
            evidence = item.get("evidence", "")
            rule_info = rule_id_map.get(rule_name)
            if not rule_info or not stock_code or confidence < 0.6:
                continue
            try:
                execute_insert(
                    """INSERT INTO stock_rule_tags
                       (stock_code, rule_id, rule_category, rule_name, matched, confidence, evidence, layer, computed_at)
                       VALUES (%s, %s, %s, %s, 1, %s, %s, 2, NOW())
                       ON DUPLICATE KEY UPDATE
                         matched=1, confidence=GREATEST(confidence, VALUES(confidence)),
                         evidence=VALUES(evidence), computed_at=NOW()""",
                    [stock_code, rule_info["id"], rule_info["category"],
                     rule_name, confidence, evidence[:500]],
                )
                total_tagged += 1
            except Exception as e:
                logger.warning(f"L2 insert error {stock_code}/{rule_name}: {e}")

        processed += len(batch)

    return {"processed": processed, "tagged": total_tagged}

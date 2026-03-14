"""L3 AI 深度分析引擎 — 仅对已有≥2个标签或在 watchlist/portfolio 中的股票执行

每只股票一次 AI 调用，prompt 包含所有 L3 规则定义 + 该股票的已有信息
"""
import json
import logging

logger = logging.getLogger(__name__)


def _lq(sql, params=None):
    from utils.db_utils import execute_query
    return execute_query(sql, params or []) or []


def _cq(sql, params=None):
    from utils.db_utils import execute_cloud_query
    return execute_cloud_query(sql, params or []) or []


def _get_l3_rules():
    rows = _lq(
        "SELECT id, rule_name, category, definition FROM stock_selection_rules WHERE layer=3 AND is_active=1"
    )
    return [dict(r) for r in rows]


def _get_stock_context(stock_code: str) -> str:
    """收集股票的已有信息作为 AI 分析上下文"""
    parts = []

    # 基本信息
    info = _lq("SELECT * FROM stock_info WHERE stock_code=%s", [stock_code])
    if info:
        i = dict(info[0])
        parts.append(f"基本信息: {i.get('stock_name','')} 行业={i.get('industry_l1','')} 市值={i.get('market_cap','')}亿")

    # 最新财报
    fin = _lq(
        "SELECT * FROM financial_reports WHERE stock_code=%s ORDER BY report_period DESC LIMIT 2",
        [stock_code],
    )
    if fin:
        f = dict(fin[0])
        parts.append(
            f"最新财报({f.get('report_period','')}): 营收增速={f.get('revenue_yoy','')}% "
            f"净利润增速={f.get('profit_yoy','')}% ROE={f.get('roe','')}%"
        )

    # 已有 L1/L2 标签
    tags = _lq(
        "SELECT rule_name, matched, confidence FROM stock_rule_tags WHERE stock_code=%s AND matched=1",
        [stock_code],
    )
    if tags:
        tag_names = [t["rule_name"] for t in tags]
        parts.append(f"已匹配规则: {', '.join(tag_names)}")

    # 最近 stock_mentions
    mentions = _cq(
        """SELECT related_themes, related_events, theme_logic
           FROM stock_mentions WHERE stock_code=%s
           ORDER BY id DESC LIMIT 5""",
        [stock_code],
    )
    if mentions:
        m_texts = [
            f"主题:{m.get('related_themes','')} 事件:{m.get('related_events','')} 逻辑:{m.get('theme_logic','')}"
            for m in mentions
        ]
        parts.append("近期提及:\n" + "\n".join(m_texts))

    return "\n\n".join(parts) if parts else f"股票代码: {stock_code}（暂无详细信息）"


def _build_l3_prompt(rules: list, stock_code: str, context: str) -> str:
    rules_text = "\n".join(
        f"- {r['rule_name']}（{r['category']}）: {r['definition'][:150]}..."
        for r in rules
    )
    return f"""你是一个深度股票分析专家。请根据以下股票信息，判断该股票是否符合各项深度分析规则。

## 股票信息
{context}

## 深度分析规则
{rules_text}

## 输出要求
返回 JSON 数组，每个匹配的规则格式：
{{"rule_name": "规则名称", "matched": true/false, "confidence": 0.0-1.0, "evidence": "分析依据（50字以内）"}}

只返回 confidence >= 0.5 的项目。只返回 JSON，不要其他文字。"""


def run_l3_for_stock(stock_code: str) -> dict:
    """对单只股票执行 L3 深度分析"""
    from utils.db_utils import execute_insert
    from utils.model_router import call_model_json

    rules = _get_l3_rules()
    if not rules:
        return {"error": "没有 L3 规则"}

    rule_id_map = {r["rule_name"]: r for r in rules}
    context = _get_stock_context(stock_code)
    prompt = _build_l3_prompt(rules, stock_code, context)

    try:
        result = call_model_json(
            "tagging",
            "你是深度股票分析专家，只返回 JSON。",
            prompt,
            max_tokens=3000,
            timeout=90,
        )
        if not isinstance(result, list):
            result = []
    except Exception as e:
        logger.warning(f"L3 AI call failed {stock_code}: {e}")
        return {"error": str(e), "tagged": 0}

    tagged = 0
    for item in result:
        rule_name = item.get("rule_name", "")
        matched = bool(item.get("matched", False))
        confidence = float(item.get("confidence", 0.5))
        evidence = item.get("evidence", "")
        rule_info = rule_id_map.get(rule_name)
        if not rule_info or confidence < 0.5:
            continue
        try:
            execute_insert(
                """INSERT INTO stock_rule_tags
                   (stock_code, rule_id, rule_category, rule_name, matched, confidence, evidence, layer, computed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, 3, NOW())
                   ON DUPLICATE KEY UPDATE
                     matched=VALUES(matched), confidence=VALUES(confidence),
                     evidence=VALUES(evidence), computed_at=NOW()""",
                [stock_code, rule_info["id"], rule_info["category"],
                 rule_name, 1 if matched else 0, confidence, evidence[:500]],
            )
            tagged += 1
        except Exception as e:
            logger.warning(f"L3 insert error {stock_code}/{rule_name}: {e}")

    return {"stock_code": stock_code, "tagged": tagged, "rules_evaluated": len(result)}


def run_l3_batch() -> dict:
    """筛选符合条件的股票批量执行 L3"""
    # 条件：已有≥2个标签 OR 在 watchlist/portfolio 中
    tagged_stocks = _lq(
        """SELECT stock_code, COUNT(*) as tag_count
           FROM stock_rule_tags WHERE matched=1
           GROUP BY stock_code HAVING tag_count >= 2"""
    )
    watchlist = _lq("SELECT stock_code FROM watchlist")
    portfolio = _lq("SELECT DISTINCT stock_code FROM holding_positions WHERE status='open'")

    codes = set(r["stock_code"] for r in tagged_stocks)
    codes |= set(r["stock_code"] for r in watchlist)
    codes |= set(r["stock_code"] for r in portfolio)

    summary = {"total": len(codes), "done": 0, "errors": 0, "details": {}}
    for code in codes:
        try:
            r = run_l3_for_stock(code)
            summary["done"] += 1
            summary["details"][code] = r
        except Exception as e:
            summary["errors"] += 1
            logger.error(f"L3 batch error {code}: {e}")

    return summary

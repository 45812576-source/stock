"""共享内容查询工具 — content_summaries / stock_mentions / KG 推荐"""
import re
import logging
from datetime import datetime, timedelta
from utils.db_utils import execute_query

logger = logging.getLogger(__name__)


def query_content_summaries(doc_types: list, date_str: str = None,
                             limit: int = 10, fallback_days: int = 3) -> list:
    """查询 content_summaries JOIN extracted_texts，按 doc_type 过滤。

    优先返回 date_str 当天（按 et.publish_time）数据，无数据时回退到最近 fallback_days 天。
    返回字段：id, extracted_text_id, doc_type, summary, fact_summary,
              opinion_summary, evidence_assessment, info_gaps, publish_time
    """
    if not doc_types:
        return []
    placeholders = ",".join(["%s"] * len(doc_types))
    select_join = f"""SELECT cs.id, cs.extracted_text_id, cs.doc_type,
                        cs.summary, cs.fact_summary, cs.opinion_summary,
                        cs.evidence_assessment, cs.info_gaps,
                        cs.created_at,
                        COALESCE(et.publish_time, cs.created_at) as publish_time
                 FROM content_summaries cs
                 LEFT JOIN extracted_texts et ON cs.extracted_text_id = et.id"""
    try:
        if date_str:
            rows = execute_query(
                f"""{select_join}
                    WHERE cs.doc_type IN ({placeholders})
                      AND DATE(COALESCE(et.publish_time, cs.created_at)) = %s
                    ORDER BY cs.created_at DESC LIMIT %s""",
                doc_types + [date_str, limit],
            )
            if rows:
                return [dict(r) for r in rows]
        # 回退：最近 fallback_days 天
        rows = execute_query(
            f"""{select_join}
                WHERE cs.doc_type IN ({placeholders})
                  AND cs.created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                ORDER BY cs.created_at DESC LIMIT %s""",
            doc_types + [fallback_days, limit],
        )
        return [dict(r) for r in (rows or [])]
    except Exception as e:
        logger.warning(f"query_content_summaries 失败: {e}")
        return []


def query_stock_mentions(limit: int = 30, min_confidence: float = 0.5,
                          days: int = 7) -> list:
    """查询最新 stock_mentions，按 mention_time 降序。

    返回字段：id, extracted_text_id, stock_name, stock_code,
              related_themes, related_events, theme_logic, mention_time
    """
    try:
        rows = execute_query(
            """SELECT id, extracted_text_id, stock_name, stock_code,
                      related_themes, related_events, theme_logic, mention_time
               FROM stock_mentions
               WHERE mention_time >= DATE_SUB(NOW(), INTERVAL %s DAY)
               ORDER BY mention_time DESC LIMIT %s""",
            [days, limit],
        )
        return [dict(r) for r in (rows or [])]
    except Exception as e:
        logger.warning(f"query_stock_mentions 失败: {e}")
        return []


def query_stock_mentions_for_codes(stock_codes: list, days: int = 7) -> list:
    """按股票代码查询 stock_mentions。"""
    if not stock_codes:
        return []
    placeholders = ",".join(["%s"] * len(stock_codes))
    try:
        rows = execute_query(
            f"""SELECT id, extracted_text_id, stock_name, stock_code,
                       related_themes, related_events, theme_logic, mention_time
                FROM stock_mentions
                WHERE stock_code IN ({placeholders})
                  AND mention_time >= DATE_SUB(NOW(), INTERVAL %s DAY)
                ORDER BY mention_time DESC""",
            stock_codes + [days],
        )
        return [dict(r) for r in (rows or [])]
    except Exception as e:
        logger.warning(f"query_stock_mentions_for_codes 失败: {e}")
        return []


def aggregate_mentions_by_theme(days: int = 7, limit: int = 30) -> list:
    """按 related_themes 聚合 stock_mentions 为篮子。

    返回：[{theme, stocks:[{stock_name,stock_code,theme_logic}],
            mention_count, latest_time}]
    """
    try:
        rows = execute_query(
            """SELECT stock_name, stock_code, related_themes, theme_logic, mention_time
               FROM stock_mentions
               WHERE mention_time >= DATE_SUB(NOW(), INTERVAL %s DAY)
                 AND related_themes IS NOT NULL AND related_themes != ''
               ORDER BY mention_time DESC LIMIT 500""",
            [days],
        )
        if not rows:
            return []

        theme_map = {}
        for r in rows:
            themes_raw = r.get("related_themes") or ""
            # related_themes 可能是逗号分隔或 JSON 数组
            if themes_raw.startswith("["):
                import json
                try:
                    themes = json.loads(themes_raw)
                except Exception:
                    themes = [themes_raw]
            else:
                themes = [t.strip() for t in themes_raw.split(",") if t.strip()]

            for theme in themes:
                if theme not in theme_map:
                    theme_map[theme] = {
                        "theme": theme,
                        "stocks": [],
                        "mention_count": 0,
                        "latest_time": None,
                    }
                bucket = theme_map[theme]
                # 去重：优先用 stock_code，无 code 则用 stock_name
                dedup_key = r.get("stock_code") or r.get("stock_name")
                existing_keys = {
                    s.get("stock_code") or s.get("stock_name")
                    for s in bucket["stocks"]
                }
                if dedup_key and dedup_key not in existing_keys:
                    bucket["stocks"].append({
                        "stock_name": r.get("stock_name", ""),
                        "stock_code": r.get("stock_code") or "",
                        "theme_logic": r.get("theme_logic", ""),
                    })
                bucket["mention_count"] += 1
                mt = str(r.get("mention_time", ""))
                if not bucket["latest_time"] or mt > bucket["latest_time"]:
                    bucket["latest_time"] = mt

        result = sorted(theme_map.values(), key=lambda x: x["mention_count"], reverse=True)
        result = result[:limit]

        # 补充投资逻辑：优先用 tag_groups.group_logic，fallback 从 theme_logic 提炼
        theme_names = [r["theme"] for r in result]
        if theme_names:
            placeholders = ",".join(["%s"] * len(theme_names))
            tg_rows = execute_query(
                f"SELECT group_name, group_logic FROM tag_groups WHERE group_name IN ({placeholders}) AND group_logic IS NOT NULL AND group_logic != ''",
                theme_names,
            )
            tg_map = {r["group_name"]: r["group_logic"] for r in tg_rows}

        for r in result:
            if r["theme"] in tg_map:
                # tag_groups 里有现成逻辑，截取前60字作为一句话
                r["investment_logic"] = tg_map[r["theme"]][:60]
            else:
                r["investment_logic"] = ""

        return result
    except Exception as e:
        logger.warning(f"aggregate_mentions_by_theme 失败: {e}")
        return []


def get_kg_recommended_stocks(keywords: list, limit: int = 5) -> list:
    """从 KG 查找与关键词关联的股票。

    返回：[{stock_code, stock_name, relation_type, strength}]
    """
    if not keywords:
        return []
    try:
        from knowledge_graph.kg_query import get_related_stocks
        seen = set()
        results = []
        for kw in keywords[:5]:
            stocks = get_related_stocks(kw)
            for s in stocks or []:
                code = s.get("stock_code") or s.get("external_id")
                if code and code not in seen:
                    seen.add(code)
                    results.append({
                        "stock_code": code,
                        "stock_name": s.get("entity_name", ""),
                        "relation_type": s.get("relation_type", ""),
                        "strength": float(s.get("strength") or 0),
                    })
        results.sort(key=lambda x: x["strength"], reverse=True)
        return results[:limit]
    except Exception as e:
        logger.warning(f"get_kg_recommended_stocks 失败: {e}")
        return []


def extract_keywords_from_summary(text: str, max_kw: int = 5) -> list:
    """从摘要文本提取关键词（简单分词，过滤停用词）。"""
    if not text:
        return []
    stopwords = {
        "的", "了", "在", "是", "和", "与", "对", "将", "为", "以", "及",
        "等", "中", "上", "下", "有", "其", "该", "此", "这", "那", "但",
        "而", "或", "并", "也", "都", "已", "被", "由", "从", "到", "于",
        "不", "无", "未", "非", "可", "能", "会", "应", "需", "要", "如",
    }
    # 提取2-6字的中文词组
    words = re.findall(r'[\u4e00-\u9fff]{2,6}', text)
    seen = set()
    result = []
    for w in words:
        if w not in stopwords and w not in seen:
            seen.add(w)
            result.append(w)
        if len(result) >= max_kw:
            break
    return result

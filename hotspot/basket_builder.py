"""股票篮子构建器 — 向量语义聚类 + Claude 主题归并"""
import json
import logging
import math
from utils.db_utils import execute_query
from utils.model_router import call_model_json as _call_model_json

logger = logging.getLogger(__name__)

BASKET_CLUSTER_PROMPT = """你是A股投资研究主编。以下文本片段是各个股票提及记录的"related_themes"字段，
已按语义相似度分成若干候选桶（每桶一行，逗号分隔关键词）。

请将这些候选桶归纳为不超过12个投资主题，每个主题输出：
{"group_name":"主题名（4-8字）","group_logic":"逻辑（50字以内）","source_indices":[候选桶序号]}

只输出JSON数组，不要其他文字。"""


def _cosine_similarity(a: list, b: list) -> float:
    """计算两个向量的余弦相似度"""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _greedy_cluster(texts: list, vectors: list, threshold: float = 0.75) -> list:
    """贪心聚类：余弦相似度 >= threshold 合并到同一桶
    返回：[{indices:[int], centroid:[float]}]
    """
    used = [False] * len(texts)
    clusters = []
    for i in range(len(texts)):
        if used[i]:
            continue
        cluster_indices = [i]
        used[i] = True
        for j in range(i + 1, len(texts)):
            if used[j]:
                continue
            sim = _cosine_similarity(vectors[i], vectors[j])
            if sim >= threshold:
                cluster_indices.append(j)
                used[j] = True
        # 计算质心
        dim = len(vectors[i])
        centroid = [0.0] * dim
        for idx in cluster_indices:
            for k in range(dim):
                centroid[k] += vectors[idx][k]
        n = len(cluster_indices)
        centroid = [v / n for v in centroid]
        clusters.append({"indices": cluster_indices, "centroid": centroid})
    return clusters


def _extract_theme_texts(row: dict) -> list:
    """从 stock_mention 行提取主题文本列表"""
    themes_raw = row.get("related_themes") or ""
    if themes_raw.startswith("["):
        try:
            themes = json.loads(themes_raw)
        except Exception:
            themes = [themes_raw]
    else:
        themes = [t.strip() for t in themes_raw.split(",") if t.strip()]
    return [t for t in themes if t]


def _build_baskets_from_clusters(clusters, all_items, all_themes) -> list:
    """将聚类结果转化为篮子列表（不调 Claude）
    返回 ≤ cluster 数量的原始桶，供后续 Claude 归并。
    """
    buckets = []
    for cl in clusters:
        theme_set = set()
        stock_map = {}  # code -> {name, theme_logic}
        ext_ids = set()
        for idx in cl["indices"]:
            item = all_items[idx]
            themes = all_themes[idx]
            for t in themes:
                theme_set.add(t)
            code = item.get("stock_code") or ""
            name = item.get("stock_name") or ""
            if code and code not in stock_map:
                stock_map[code] = {
                    "stock_code": code,
                    "stock_name": name,
                    "theme_logic": item.get("theme_logic") or "",
                }
            et_id = item.get("extracted_text_id")
            if et_id:
                ext_ids.add(et_id)
        buckets.append({
            "theme_keywords": list(theme_set)[:8],
            "stocks": list(stock_map.values()),
            "chunk_count": len(ext_ids),
        })
    return buckets


def _count_chunks_for_themes(theme_keywords: list, days: int) -> int:
    """统计与主题关键词关联的 chunk 数（COUNT DISTINCT extracted_text_id）"""
    if not theme_keywords:
        return 0
    conditions = " OR ".join(["sm.related_themes LIKE %s"] * len(theme_keywords))
    params = [f"%{t}%" for t in theme_keywords] + [days]
    try:
        rows = execute_query(
            f"""SELECT COUNT(DISTINCT sm.extracted_text_id) AS cnt
                FROM stock_mentions sm
                WHERE ({conditions})
                  AND sm.mention_time >= DATE_SUB(NOW(), INTERVAL %s DAY)""",
            params,
        )
        return int(rows[0]["cnt"]) if rows else 0
    except Exception as e:
        logger.warning(f"chunk count 查询失败: {e}")
        return 0


def build_stock_baskets(days: int = 3) -> list:
    """构建向量语义聚类股票篮子

    Step 1: 拉 stock_mentions（days 天内，LIMIT 500）
    Step 2: 对每条 related_themes embed
    Step 3: 余弦相似度聚类（threshold=0.75），合并相似主题 → 候选桶 ≤ 20
    Step 4: 若 > 20 桶，调 Claude 归并 → ≤ 12 主题
    Step 5: 每桶：去重股票，统计 chunk_count
    降级：向量服务不可用时 fallback 到 aggregate_mentions_by_theme()

    返回：[{theme, group_logic, stocks:[{stock_code,stock_name,theme_logic}],
             mention_count, chunk_count}]
    """
    # Step 1: 拉数据
    try:
        rows = execute_query(
            """SELECT stock_name, stock_code, related_themes, theme_logic,
                      extracted_text_id, mention_time
               FROM stock_mentions
               WHERE mention_time >= DATE_SUB(NOW(), INTERVAL %s DAY)
                 AND related_themes IS NOT NULL AND related_themes != ''
               ORDER BY mention_time DESC LIMIT 500""",
            [days],
        )
    except Exception as e:
        logger.warning(f"stock_mentions 查询失败: {e}")
        rows = []

    if not rows:
        return _fallback_baskets(days)

    items = [dict(r) for r in rows]
    all_themes = [_extract_theme_texts(r) for r in items]

    # 去掉空主题的行
    valid_pairs = [(item, themes) for item, themes in zip(items, all_themes) if themes]
    if not valid_pairs:
        return _fallback_baskets(days)

    items = [p[0] for p in valid_pairs]
    all_themes = [p[1] for p in valid_pairs]

    # Step 2: 向量化主题文本（每行取第一个主题词）
    theme_texts_for_embed = [t[0] if t else "" for t in all_themes]

    try:
        from retrieval.embedding import embed_texts
        vectors = embed_texts(theme_texts_for_embed)
    except Exception as e:
        logger.warning(f"向量化失败，降级: {e}")
        return _fallback_baskets(days)

    if not vectors or len(vectors) != len(items):
        return _fallback_baskets(days)

    # Step 3: 聚类
    clusters = _greedy_cluster(theme_texts_for_embed, vectors, threshold=0.75)
    logger.info(f"basket_builder: 聚类结果 {len(clusters)} 桶（输入 {len(items)} 条）")

    # 构建原始桶
    raw_buckets = _build_baskets_from_clusters(clusters, items, all_themes)

    # Step 4: 超过 20 桶时调 Claude 归并
    if len(raw_buckets) > 20:
        raw_buckets = _merge_buckets_with_claude(raw_buckets)

    # Step 5: 填充 mention_count 和 chunk_count
    result = []
    for bkt in raw_buckets[:12]:
        theme_name = " · ".join(bkt["theme_keywords"][:3]) if bkt["theme_keywords"] else "未知主题"
        group_logic = bkt.get("group_logic", "")
        chunk_count = _count_chunks_for_themes(bkt["theme_keywords"], days)
        mention_count = sum(
            1 for item, themes in zip(items, all_themes)
            if any(kw in " ".join(themes) for kw in bkt["theme_keywords"])
        )
        result.append({
            "theme": bkt.get("theme_name") or theme_name,
            "group_logic": group_logic,
            "stocks": bkt["stocks"],
            "mention_count": mention_count,
            "chunk_count": chunk_count,
        })

    return result


def _merge_buckets_with_claude(raw_buckets: list) -> list:
    """调 Claude 将 >20 桶归并为 ≤12 主题"""
    desc = ""
    for i, bkt in enumerate(raw_buckets[:30]):
        kws = ", ".join(bkt["theme_keywords"][:5])
        desc += f"{i}: {kws}\n"

    try:
        results = _call_model_json(
            "hotspot",
            BASKET_CLUSTER_PROMPT,
            desc,
            max_tokens=2000,
        )
        if not isinstance(results, list):
            return raw_buckets[:20]

        merged = []
        for item in results[:12]:
            src_indices = item.get("source_indices") or []
            combined_keywords = set()
            combined_stocks = {}
            combined_ext_ids = set()
            for idx in src_indices:
                if 0 <= idx < len(raw_buckets):
                    bkt = raw_buckets[idx]
                    combined_keywords.update(bkt.get("theme_keywords", []))
                    for s in bkt.get("stocks", []):
                        code = s.get("stock_code") or s.get("stock_name")
                        if code and code not in combined_stocks:
                            combined_stocks[code] = s
                    combined_ext_ids.update(range(bkt.get("chunk_count", 0)))

            merged.append({
                "theme_name": item.get("group_name", ""),
                "group_logic": item.get("group_logic", ""),
                "theme_keywords": list(combined_keywords)[:8],
                "stocks": list(combined_stocks.values()),
                "chunk_count": len(combined_ext_ids),
            })
        return merged if merged else raw_buckets[:20]
    except Exception as e:
        logger.warning(f"Claude 归并桶失败: {e}")
        return raw_buckets[:20]


def _fallback_baskets(days: int) -> list:
    """降级：使用 aggregate_mentions_by_theme()"""
    try:
        from utils.content_query import aggregate_mentions_by_theme
        baskets = aggregate_mentions_by_theme(days=days, limit=12)
        # 补充 chunk_count
        for bkt in baskets:
            theme_keywords = [bkt["theme"]]
            bkt["chunk_count"] = _count_chunks_for_themes(theme_keywords, days)
            bkt["group_logic"] = bkt.get("investment_logic", "")
        return baskets
    except Exception as e:
        logger.warning(f"fallback_baskets 失败: {e}")
        return []

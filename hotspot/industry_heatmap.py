"""细分行业 chunk 热力图 — 向量聚类 + SQL 统计"""
import json
import logging
import math
from datetime import datetime, timedelta
from utils.db_utils import execute_query

logger = logging.getLogger(__name__)

# 利好/利空关键词规则
_POSITIVE_KWS = [
    "利好", "上涨", "增长", "突破", "扩张", "政策支持", "获批", "爆发", "放量",
    "涨停", "新高", "超预期", "订单", "中标", "受益", "景气", "复苏",
]
_NEGATIVE_KWS = [
    "利空", "下跌", "下滑", "风险", "亏损", "减少", "压力", "下调", "违规",
    "处罚", "退市", "跌停", "负增长", "萎缩", "受损", "受压",
]


def _assess_direction(text: str) -> str:
    """根据文本关键词判断情绪方向"""
    pos = sum(1 for kw in _POSITIVE_KWS if kw in text)
    neg = sum(1 for kw in _NEGATIVE_KWS if kw in text)
    if pos > neg:
        return "positive"
    if neg > pos:
        return "negative"
    return "neutral"


def _cosine_sim(a: list, b: list) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _get_industry_chunk_heatmap_sql(days: int) -> dict:
    """降级 SQL 路径：直接 JOIN text_chunks + stock_mentions + stock_info"""
    try:
        rows = execute_query(
            """SELECT si.industry_l2 AS industry,
                      DATE(tc.publish_time) AS day,
                      COUNT(DISTINCT tc.extracted_text_id) AS chunk_count
               FROM text_chunks tc
               JOIN stock_mentions sm ON tc.extracted_text_id = sm.extracted_text_id
               JOIN stock_info si ON sm.stock_code = si.stock_code
               WHERE tc.publish_time >= DATE_SUB(NOW(), INTERVAL %s DAY)
                 AND si.industry_l2 IS NOT NULL AND si.industry_l2 != ''
               GROUP BY si.industry_l2, DATE(tc.publish_time)
               ORDER BY day, industry""",
            [days],
        )
    except Exception as e:
        logger.warning(f"industry_heatmap SQL 路径失败: {e}")
        rows = []

    if not rows:
        # 再尝试不依赖 text_chunks 的路径
        try:
            rows = execute_query(
                """SELECT si.industry_l2 AS industry,
                          DATE(sm.mention_time) AS day,
                          COUNT(DISTINCT sm.extracted_text_id) AS chunk_count
                   FROM stock_mentions sm
                   JOIN stock_info si ON sm.stock_code = si.stock_code
                   WHERE sm.mention_time >= DATE_SUB(NOW(), INTERVAL %s DAY)
                     AND si.industry_l2 IS NOT NULL AND si.industry_l2 != ''
                   GROUP BY si.industry_l2, DATE(sm.mention_time)
                   ORDER BY day, industry""",
                [days],
            )
        except Exception as e2:
            logger.warning(f"industry_heatmap fallback SQL 失败: {e2}")
            rows = []

    # 生成日期列表
    today = datetime.now().date()
    dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]

    # 构建 industry -> {daily: {day: count}, total: int}
    industry_map = {}
    for r in (rows or []):
        ind = r["industry"]
        day = str(r["day"])[:10]
        cnt = int(r["chunk_count"] or 0)
        if ind not in industry_map:
            industry_map[ind] = {"daily": {}, "total": 0, "text_samples": []}
        industry_map[ind]["daily"][day] = cnt
        industry_map[ind]["total"] += cnt

    # 排序取 Top 30
    sorted_industries = sorted(industry_map.items(), key=lambda x: x[1]["total"], reverse=True)[:30]

    industries = []
    for ind_name, data in sorted_industries:
        industries.append({
            "name": ind_name,
            "total": data["total"],
            "daily": data["daily"],
            "ai_direction": "neutral",
        })

    return {"dates": dates, "industries": industries}


def get_industry_chunk_heatmap(days: int = 7) -> dict:
    """获取细分行业 by 天 chunk 出现次数热力图

    Step 1: 拉 content_summaries + stock_mentions
    Step 2: embed + 余弦相似度聚类（threshold=0.72）
    Step 3: 每桶关联 stock_codes → JOIN stock_info.industry_l2
    Step 4: 按 (industry_l2, DATE) 统计 COUNT DISTINCT extracted_text_id
    Step 5: 利好/利空方向判断
    降级：Milvus 不可用时直接用 SQL JOIN

    返回：{dates:[str], industries:[{name, total, daily:{day:cnt}, ai_direction}]}
    """
    try:
        return _get_heatmap_with_embedding(days)
    except Exception as e:
        logger.warning(f"向量路径失败，降级SQL: {e}")
        return _get_industry_chunk_heatmap_sql(days)


def _get_heatmap_with_embedding(days: int) -> dict:
    """向量聚类路径"""
    from retrieval.embedding import embed_texts

    # Step 1: 拉数据
    try:
        from utils.content_query import query_content_summaries, query_stock_mentions
        cs_rows = query_content_summaries(
            doc_types=["policy_doc", "data_release", "strategy_report",
                       "market_commentary", "research_report",
                       "announcement", "feature_news", "flash_news"],
            date_str=None, limit=200, fallback_days=days,
        )
        sm_rows = query_stock_mentions(limit=500, days=days)
    except Exception as e:
        logger.warning(f"数据拉取失败: {e}")
        raise

    # 合并文本：cs.summary + sm.related_themes
    texts = []
    meta = []  # {type, stock_codes, extracted_text_id, publish_time, text_sample}

    for cs in cs_rows:
        t = ((cs.get("summary") or "") + " " + (cs.get("fact_summary") or "")).strip()
        if t:
            texts.append(t)
            meta.append({
                "type": "cs",
                "extracted_text_id": cs.get("extracted_text_id"),
                "publish_time": str(cs.get("publish_time", ""))[:10],
                "stock_codes": [],
                "text_sample": t[:100],
            })

    for sm in sm_rows:
        t = ((sm.get("related_themes") or "") + " " + (sm.get("theme_logic") or "")).strip()
        if t:
            texts.append(t)
            meta.append({
                "type": "sm",
                "extracted_text_id": sm.get("extracted_text_id"),
                "publish_time": str(sm.get("mention_time", ""))[:10],
                "stock_codes": [sm.get("stock_code")] if sm.get("stock_code") else [],
                "text_sample": t[:100],
            })

    if not texts:
        raise ValueError("无可用文本")

    # Step 2: 向量化 + 聚类
    vectors = embed_texts(texts)
    if not vectors:
        raise ValueError("向量化结果为空")

    # 简单聚类：threshold=0.72
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
            if _cosine_sim(vectors[i], vectors[j]) >= 0.72:
                cluster_indices.append(j)
                used[j] = True
        clusters.append(cluster_indices)

    # Step 3 & 4: 每桶 → 关联 stock_codes → industry_l2 → 按天统计
    # 收集所有涉及的 stock_codes 批量查 industry
    stock_codes_all = set()
    for cl_indices in clusters:
        for idx in cl_indices:
            for code in meta[idx]["stock_codes"]:
                if code:
                    stock_codes_all.add(code)

    # 补充：从 sm 里的 stock_codes 查行业
    code_industry_map = {}
    if stock_codes_all:
        ph = ",".join(["%s"] * len(stock_codes_all))
        try:
            si_rows = execute_query(
                f"SELECT stock_code, industry_l2 FROM stock_info WHERE stock_code IN ({ph})",
                list(stock_codes_all),
            )
            for r in (si_rows or []):
                if r.get("industry_l2"):
                    code_industry_map[r["stock_code"]] = r["industry_l2"]
        except Exception as e:
            logger.warning(f"stock_info 查询失败: {e}")

    # 构建 industry → day → set(extracted_text_id)
    industry_day_ids = {}  # {industry: {day: set(ext_id)}}
    industry_text_samples = {}  # {industry: [text]}

    for cl_indices in clusters:
        # 收集桶内所有文本样本（用于方向判断）
        sample_texts = " ".join(meta[idx]["text_sample"] for idx in cl_indices[:3])
        direction = _assess_direction(sample_texts)

        # 收集桶内 industries
        bucket_industries = set()
        for idx in cl_indices:
            for code in meta[idx]["stock_codes"]:
                ind = code_industry_map.get(code)
                if ind:
                    bucket_industries.add(ind)

        # 按 (industry, day) 统计 ext_ids
        for idx in cl_indices:
            m = meta[idx]
            day = m["publish_time"]
            ext_id = m["extracted_text_id"]
            if not day or not ext_id:
                continue
            for ind in bucket_industries:
                if ind not in industry_day_ids:
                    industry_day_ids[ind] = {}
                    industry_text_samples[ind] = []
                if day not in industry_day_ids[ind]:
                    industry_day_ids[ind][day] = set()
                industry_day_ids[ind][day].add(ext_id)
                if len(industry_text_samples[ind]) < 5:
                    industry_text_samples[ind].append(sample_texts)

    if not industry_day_ids:
        # 向量路径无结果，回退 SQL
        raise ValueError("向量路径无 industry 匹配")

    # 生成日期列表
    today = datetime.now().date()
    dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]

    # 汇总结果
    industries = []
    for ind_name, day_ids in industry_day_ids.items():
        total = sum(len(ids) for ids in day_ids.values())
        daily = {day: len(ids) for day, ids in day_ids.items()}
        sample_text = " ".join(industry_text_samples.get(ind_name, []))
        direction = _assess_direction(sample_text)
        industries.append({
            "name": ind_name,
            "total": total,
            "daily": daily,
            "ai_direction": direction,
        })

    industries.sort(key=lambda x: x["total"], reverse=True)
    industries = industries[:30]

    return {"dates": dates, "industries": industries}

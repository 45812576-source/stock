"""AI 投资主题挖掘 — 三源文本聚类（新主流程）+ 共现聚类 fallback"""
import json
import logging
import math
from collections import Counter, defaultdict
from utils.db_utils import execute_query, execute_insert
from utils.model_router import call_model_json as _call_model_json

def call_claude_json(system_prompt, user_message, max_tokens=4096, timeout=900):
    return _call_model_json('hotspot', system_prompt, user_message, max_tokens=max_tokens, timeout=timeout)

logger = logging.getLogger(__name__)

# ── 三源文本聚类 Prompt ───────────────────────────────────────

THEME_MINING_PROMPT = """你是A股投资研究主编。以下文本片段来自待研究新闻、股票提及和研报摘要，
请归纳出不超过12个投资主题，每个主题输出：
{"group_name":"主题名（4-8字）","group_logic":"逻辑（50字以内）","tags":["词1","词2","词3","词4","词5"]}

只输出JSON数组，不要其他文字。"""


def get_top_tags(days=7, limit=30):
    """获取指定时间段内出现频次最高的标签"""
    return execute_query(
        """SELECT tag_name, MAX(tag_type) as tag_type,
                  COUNT(*) as total_freq,
                  GROUP_CONCAT(DISTINCT dashboard_type) as dashboards,
                  MIN(appear_date) as first_appear,
                  MAX(appear_date) as last_appear
           FROM dashboard_tag_frequency
           WHERE appear_date >= date('now', ?)
           GROUP BY tag_name
           ORDER BY total_freq DESC
           LIMIT ?""",
        [f"-{days} days", limit],
    )


def get_tag_dashboard_distribution(tag_name, days=7):
    """获取单个标签在各榜单中的分布"""
    return execute_query(
        """SELECT dashboard_type, COUNT(*) as freq,
                  GROUP_CONCAT(DISTINCT appear_date) as dates
           FROM dashboard_tag_frequency
           WHERE tag_name=? AND appear_date >= date('now', ?)
           GROUP BY dashboard_type
           ORDER BY freq DESC""",
        [tag_name, f"-{days} days"],
    )


def recommend_tag_groups(days=7, top_n=20):
    """推荐关联标签组 — 新主流程：三源文本聚类 + Claude投资主题归纳
    降级：向量服务不可用时 fallback 到 dashboard_tag_frequency 共现聚类
    """
    try:
        candidates = _build_theme_candidates_from_text(days)
        if candidates:
            logger.info(f"三源文本聚类完成，候选主题: {len(candidates)}")
            return candidates[:top_n]
    except Exception as e:
        logger.warning(f"三源文本聚类失败，降级共现聚类: {e}")

    return _recommend_by_cooccurrence(days, top_n)


# ── 新主流程：三源文本聚类 ────────────────────────────────────

def _cosine_sim(a: list, b: list) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _build_theme_candidates_from_text(days: int) -> list:
    """三源文本聚类主流程

    Step 1: discovery_pool.title + snapshot.summary（全量）
    Step 2: stock_mentions.related_themes（days 天内，LIMIT 300）
    Step 3: content_summaries.summary + fact_summary（days 天内，LIMIT 100）
    Step 4: 合并文本列表，embed，余弦相似度聚类（threshold=0.72）→ 候选桶
    Step 5: call_claude_json 归纳 → ≤12 主题
    """
    from retrieval.embedding import embed_texts

    texts = []
    text_meta = []  # {source, snippet}

    # Step 1: discovery_pool
    try:
        pool_rows = execute_query(
            "SELECT title, snapshot FROM discovery_pool ORDER BY added_at DESC LIMIT 100",
            [],
        )
        for r in (pool_rows or []):
            title = r.get("title") or ""
            snap = r.get("snapshot") or ""
            if isinstance(snap, str):
                try:
                    snap_d = json.loads(snap)
                    snap = snap_d.get("summary") or snap_d.get("fact") or ""
                except Exception:
                    pass
            t = (title + " " + (snap or "")).strip()
            if t:
                texts.append(t[:200])
                text_meta.append({"source": "pool", "snippet": t[:80]})
    except Exception as e:
        logger.warning(f"discovery_pool 读取失败: {e}")

    # Step 2: stock_mentions.related_themes
    try:
        from utils.content_query import query_stock_mentions
        sm_rows = query_stock_mentions(limit=300, days=days)
        for sm in sm_rows:
            themes_raw = sm.get("related_themes") or ""
            if themes_raw.startswith("["):
                try:
                    themes = json.loads(themes_raw)
                except Exception:
                    themes = [themes_raw]
            else:
                themes = [t.strip() for t in themes_raw.split(",") if t.strip()]
            theme_logic = sm.get("theme_logic") or ""
            t = " ".join(themes) + " " + theme_logic
            t = t.strip()
            if t:
                texts.append(t[:200])
                text_meta.append({"source": "sm", "snippet": t[:80]})
    except Exception as e:
        logger.warning(f"stock_mentions 读取失败: {e}")

    # Step 3: content_summaries
    try:
        from utils.content_query import query_content_summaries
        cs_rows = query_content_summaries(
            doc_types=["policy_doc", "data_release", "strategy_report",
                       "market_commentary", "research_report",
                       "announcement", "feature_news", "flash_news"],
            date_str=None, limit=100, fallback_days=days,
        )
        for cs in cs_rows:
            t = ((cs.get("summary") or "") + " " + (cs.get("fact_summary") or "")).strip()
            if t:
                texts.append(t[:200])
                text_meta.append({"source": "cs", "snippet": t[:80]})
    except Exception as e:
        logger.warning(f"content_summaries 读取失败: {e}")

    if not texts:
        raise ValueError("三源均无可用文本")

    # Step 4: embed + 聚类（threshold=0.72）
    vectors = embed_texts(texts)
    if not vectors or len(vectors) != len(texts):
        raise ValueError("向量化失败")

    used = [False] * len(texts)
    clusters = []
    for i in range(len(texts)):
        if used[i]:
            continue
        cluster_indices = [i]
        used[i] = True
        for j in range(i + 1, len(texts)):
            if not used[j] and _cosine_sim(vectors[i], vectors[j]) >= 0.72:
                cluster_indices.append(j)
                used[j] = True
        clusters.append(cluster_indices)

    logger.info(f"三源聚类: {len(texts)} 条文本 → {len(clusters)} 桶")

    # 每桶取代表性片段（取前 3 条拼接）
    cluster_snippets = []
    for cl_indices in clusters:
        snippets = [text_meta[idx]["snippet"] for idx in cl_indices[:3]]
        cluster_snippets.append("；".join(snippets))

    # Step 5: Claude 归纳 ≤12 主题
    batch_text = "\n".join(f"{i}: {s}" for i, s in enumerate(cluster_snippets[:40]))
    try:
        results = call_claude_json(THEME_MINING_PROMPT, batch_text, max_tokens=3000)
    except Exception as e:
        logger.warning(f"Claude 主题归纳失败: {e}")
        results = None

    if not isinstance(results, list) or not results:
        # 降级：直接从聚类桶构建基础候选
        return _build_fallback_from_clusters(clusters, texts, days)

    candidates = []
    for item in results[:12]:
        tags = item.get("tags") or []
        group_name = item.get("group_name") or " + ".join(tags[:3])
        group_logic = item.get("group_logic") or ""
        if not tags:
            continue
        # 频次：粗估（桶数量）
        freq = len(clusters) // max(len(results), 1)
        candidates.append({
            "tags": tags[:5],
            "group_name": group_name,
            "group_logic": group_logic,
            "frequency": freq,
        })

    return candidates


def _build_fallback_from_clusters(clusters, texts, days):
    """Claude 归纳失败时，从聚类桶直接构建基础候选（每桶提取关键词）"""
    import re
    stopwords = {"的", "了", "在", "是", "和", "与", "对", "将", "为", "以", "及",
                 "等", "中", "上", "下", "有", "其", "该", "此", "这", "那", "但",
                 "而", "或", "并", "也", "都", "已", "被", "由", "从", "到", "于"}
    candidates = []
    for cl_indices in clusters[:12]:
        all_text = " ".join(texts[i] for i in cl_indices[:5])
        words = re.findall(r'[\u4e00-\u9fff]{2,6}', all_text)
        freq_map = {}
        for w in words:
            if w not in stopwords:
                freq_map[w] = freq_map.get(w, 0) + 1
        top_words = sorted(freq_map.items(), key=lambda x: x[1], reverse=True)[:5]
        tags = [w for w, _ in top_words]
        if len(tags) < 2:
            continue
        candidates.append({
            "tags": tags,
            "group_name": " + ".join(tags[:3]),
            "group_logic": "（AI服务不可用）基于文本聚类的自动归纳结果",
            "frequency": len(cl_indices),
        })
    return candidates


# ── 旧共现聚类（保留为 fallback）────────────────────────────

def _recommend_by_cooccurrence(days=7, top_n=20):
    """旧主流程：dashboard_tag_frequency 共现聚类 + Claude验证（fallback）"""
    tags = get_top_tags(days, 40)
    if not tags:
        return []

    # 扩展标签来源：content_summaries 关键词 + stock_mentions themes
    try:
        from utils.content_query import query_content_summaries, extract_keywords_from_summary, query_stock_mentions
        import json as _json
        extra_tag_freq = {}
        cs_rows = query_content_summaries(
            doc_types=["policy_doc", "data_release", "strategy_report",
                       "market_commentary", "research_report",
                       "announcement", "feature_news", "flash_news"],
            date_str=None, limit=100, fallback_days=days,
        )
        for cs in cs_rows:
            kws = extract_keywords_from_summary(
                (cs.get("summary") or "") + " " + (cs.get("fact_summary") or ""), max_kw=3
            )
            for kw in kws:
                extra_tag_freq[kw] = extra_tag_freq.get(kw, 0) + 1
        sm_rows = query_stock_mentions(limit=300, days=days)
        for sm in sm_rows:
            themes_raw = sm.get("related_themes") or ""
            if themes_raw.startswith("["):
                try:
                    themes = _json.loads(themes_raw)
                except Exception:
                    themes = [themes_raw]
            else:
                themes = [t.strip() for t in themes_raw.split(",") if t.strip()]
            for theme in themes:
                if theme:
                    extra_tag_freq[theme] = extra_tag_freq.get(theme, 0) + 2
        existing_names = {t["tag_name"] for t in tags}
        for name, freq in extra_tag_freq.items():
            if name in existing_names:
                for t in tags:
                    if t["tag_name"] == name:
                        t["total_freq"] = t.get("total_freq", 0) + freq
                        break
            else:
                tags.append({"tag_name": name, "total_freq": freq})
        tags.sort(key=lambda x: x.get("total_freq", 0), reverse=True)
    except Exception:
        pass

    tag_freq_map = {t["tag_name"]: t["total_freq"] for t in tags}

    pair_counter = Counter()
    co_occur = defaultdict(list)
    for tag in tags:
        name = tag["tag_name"]
        records = execute_query(
            """SELECT appear_date, dashboard_type FROM dashboard_tag_frequency
               WHERE tag_name=? AND appear_date >= date('now', ?)""",
            [name, f"-{days} days"],
        )
        for r in records:
            key = f"{r['appear_date']}_{r['dashboard_type']}"
            co_occur[key].append(name)

    for key, tag_list in co_occur.items():
        unique_tags = list(set(tag_list))
        for i in range(len(unique_tags)):
            for j in range(i + 1, len(unique_tags)):
                pair = tuple(sorted([unique_tags[i], unique_tags[j]]))
                pair_counter[pair] += 1

    used = set()
    candidates = []

    for pair, freq in pair_counter.most_common(80):
        if freq < 1:
            break
        if pair[0] in used and pair[1] in used:
            continue
        group_tags = set(pair)
        for other_pair, other_freq in pair_counter.most_common(200):
            if other_freq < 2:
                break
            if other_pair[0] in group_tags or other_pair[1] in group_tags:
                group_tags.update(other_pair)
            if len(group_tags) >= 5:
                break

        group_tags -= used
        if len(group_tags) < 2:
            continue

        sorted_tags = sorted(group_tags, key=lambda t: tag_freq_map.get(t, 0), reverse=True)
        total_freq = sum(tag_freq_map.get(t, 0) for t in sorted_tags)

        dashboards = set()
        for t in sorted_tags:
            for tag_info in tags:
                if tag_info["tag_name"] == t and tag_info.get("dashboards"):
                    dashboards.update(tag_info["dashboards"].split(","))

        candidates.append({
            "tags": sorted_tags[:5],
            "frequency": total_freq,
            "dashboards": list(dashboards),
            "tag_count": len(sorted_tags),
        })
        used.update(sorted_tags)

        if len(candidates) >= top_n * 3:
            break

    if not candidates:
        return []

    validated = _validate_groups_with_claude(candidates)

    if not validated:
        logger.warning("Claude验证全部失败，降级使用原始候选组（无AI逻辑说明）")
        for c in candidates:
            c["group_name"] = " + ".join(c["tags"][:3])
            c["group_logic"] = "（AI服务不可用）基于共现频率的自动聚类结果"
            c["valid"] = True
        validated = candidates

    return sorted(validated, key=lambda g: g["frequency"], reverse=True)[:top_n]


TAG_VALIDATION_PROMPT = """你是A股投资策略分析师。以下是从市场热点数据中挖掘出的若干候选标签组合。
请判断每个组合是否构成一条有意义的投资主线/逻辑链。

判断标准：
- 标签之间是否有因果关系、产业链上下游关系、政策传导关系、或资金轮动关系
- 能否用一句话概括这组标签背后的投资逻辑
- "降准+消费电子+白酒"这种无逻辑关联的组合应该被淘汰

请对每个候选组输出JSON数组，每个元素格式：
{
    "index": 候选组序号(从0开始),
    "valid": true/false,
    "group_name": "投资主线名称（如：AI算力产业链、宽松货币利好地产链）",
    "logic": "一句话投资逻辑",
    "refined_tags": ["优化后的标签列表，可以删除不相关的标签"]
}

只输出JSON数组，不要其他文字。"""


SINGLE_LOGIC_PROMPT = """你是A股投资策略分析师。以下标签组合来自近期市场热点数据：
{tags}

请判断这组标签是否构成一条有意义的投资主线。如果是，输出JSON：
{{"valid": true, "group_name": "投资主线名称", "logic": "一段话说明投资逻辑（100-200字）"}}

如果这些标签之间没有明确的因果/产业链/政策传导关系，输出：
{{"valid": false}}

只输出JSON，不要其他文字。"""


def _generate_single_logic(tags):
    """为单个标签组生成投资逻辑，失败返回 None"""
    try:
        result = call_claude_json(
            SINGLE_LOGIC_PROMPT.format(tags=" + ".join(tags)),
            "", max_tokens=500,
        )
        if result.get("valid") and result.get("logic"):
            return {
                "group_name": result.get("group_name", " + ".join(tags[:3])),
                "group_logic": result["logic"],
            }
    except Exception as e:
        logger.warning(f"单组逻辑生成失败 {tags}: {e}")
    return None


def _validate_groups_with_claude(candidates):
    """用Claude验证候选标签组的投资逻辑"""
    desc = ""
    for i, c in enumerate(candidates):
        desc += f"候选组{i}: {' + '.join(c['tags'])} (频次:{c['frequency']})\n"

    try:
        results = call_claude_json(TAG_VALIDATION_PROMPT, desc, max_tokens=2000)
    except Exception as e:
        logger.warning(f"Claude标签组批量验证失败: {e}")
        return []

    if not isinstance(results, list):
        logger.warning("Claude返回格式异常")
        return []

    validated = []
    for item in results:
        idx = item.get("index", -1)
        if not item.get("valid") or idx < 0 or idx >= len(candidates):
            continue
        group = candidates[idx]
        logic = item.get("logic", "")
        if not logic:
            single = _generate_single_logic(group["tags"])
            if not single:
                continue
            logic = single["group_logic"]
            group["group_name"] = single["group_name"]
        else:
            group["group_name"] = item.get("group_name", " + ".join(group["tags"][:3]))
        group["group_logic"] = logic
        refined = item.get("refined_tags")
        if refined and len(refined) >= 2:
            group["tags"] = refined[:5]
            group["tag_count"] = len(group["tags"])
        validated.append(group)

    if not validated:
        logger.warning("Claude过滤后无有效标签组")
        return []

    return validated


def _fallback_validate(candidates):
    """逐个调 Claude 生成投资逻辑，生成不了的直接丢弃"""
    validated = []
    for c in candidates:
        single = _generate_single_logic(c["tags"])
        if single:
            c["group_name"] = single["group_name"]
            c["group_logic"] = single["group_logic"]
            validated.append(c)
    return validated


MERGE_GROUPS_PROMPT = """你是A股投资策略分析师。以下是若干已验证的标签组，请判断哪些组的投资逻辑高度相似，应该合并。

合并规则：
- 投资逻辑相似（同一产业链、同一政策受益方向）的组应合并
- 合并后从并集中选取最核心的标签，最多保留5个
- 不相似的组保持独立

请输出JSON数组，每个元素格式：
{
    "merged_tags": ["合并后的标签列表，最多5个"],
    "group_name": "合并后的投资主线名称",
    "group_logic": "合并后的投资逻辑",
    "source_indices": [原始组序号列表]
}

只输出JSON数组，不要其他文字。"""


def merge_and_filter_groups(groups, days=7):
    """相似合并 + 条件评分筛选 → Top 12"""
    if not groups:
        return []

    # ── 1. Claude 相似合并 ──
    merged = _merge_similar_groups(groups)

    # ── 2. 条件评分：龙头资金为必要条件，宏观/行业利好加分 ──
    scored = []
    for g in merged:
        tags = g.get("tags", [])
        leader_info = _check_leader_capital(tags, days)
        if not leader_info:
            continue  # 无龙头资金数据则跳过

        macro_ok = _check_macro_positive(tags, days)
        industry_ok = _check_industry_positive(tags, days)

        g["macro_positive"] = macro_ok
        g["industry_positive"] = industry_ok
        g["leader_stock"] = leader_info["stock_name"]
        g["leader_code"] = leader_info["stock_code"]
        g["leader_net_inflow"] = leader_info["net_inflow"]
        g["group_total_inflow"] = leader_info.get("group_total_inflow", 0)
        g["group_total_cap"] = leader_info.get("group_total_cap", 0)
        g["group_stock_count"] = leader_info.get("group_stock_count", 0)
        g["daily_inflow"] = leader_info.get("daily_inflow", [])
        g["kg_stock_groups"] = leader_info.get("kg_stock_groups", [])
        # 评分：宏观+行业各1分，龙头净流入作为排序权重
        g["_filter_score"] = int(macro_ok) + int(industry_ok)
        scored.append(g)

    # ── 3. 按评分降序 → 同分按龙头净流入降序，取 Top 12 ──
    scored.sort(key=lambda x: (x["_filter_score"], x.get("leader_net_inflow", 0)), reverse=True)
    return scored[:12]


def _merge_similar_groups(groups):
    """用 Claude 合并投资逻辑相似的标签组"""
    if len(groups) <= 1:
        return groups

    desc = ""
    for i, g in enumerate(groups):
        tags_str = " + ".join(g.get("tags", []))
        logic = g.get("group_logic", "")
        desc += f"组{i}: {tags_str} — {logic}\n"

    try:
        results = call_claude_json(MERGE_GROUPS_PROMPT, desc, max_tokens=3000)
    except Exception as e:
        logger.warning(f"Claude合并标签组失败，跳过合并: {e}")
        return groups

    if not isinstance(results, list):
        logger.warning("Claude合并返回格式异常，跳过合并")
        return groups

    merged = []
    for item in results:
        tags = item.get("merged_tags", [])[:5]
        name = item.get("group_name", "")
        logic = item.get("group_logic", "")
        if not logic:
            # 从原始 groups 中继承 logic
            for i in item.get("source_indices", []):
                if i < len(groups) and groups[i].get("group_logic"):
                    logic = groups[i]["group_logic"]
                    break
        if not logic:
            # 单独生成一次
            single = _generate_single_logic(tags)
            if single:
                logic = single["group_logic"]
                name = name or single["group_name"]
            else:
                continue  # 生成不了逻辑就丢弃这个组
        merged.append({
            "tags": tags,
            "group_name": name,
            "group_logic": logic,
            "frequency": sum(
                groups[i].get("frequency", 0)
                for i in item.get("source_indices", [])
                if i < len(groups)
            ),
        })
    return merged if merged else groups


def _check_macro_positive(tags, days):
    """检查标签是否出现在近N天宏观利好新闻中"""
    for tag in tags:
        rows = execute_query(
            """SELECT COUNT(*) as cnt FROM cleaned_items
               WHERE event_type IN ('macro', 'macro_policy') AND sentiment='positive'
               AND tags_json LIKE ?
               AND cleaned_at >= DATE_SUB(CURDATE(), INTERVAL ? DAY)""",
            [f"%{tag}%", days],
        )
        if rows and rows[0]["cnt"] > 0:
            return True
    return False


def _check_industry_positive(tags, days):
    """检查标签是否出现在近N天行业利好新闻中"""
    for tag in tags:
        rows = execute_query(
            """SELECT COUNT(*) as cnt FROM cleaned_items
               WHERE event_type IN ('industry', 'industry_news') AND sentiment='positive'
               AND tags_json LIKE ?
               AND cleaned_at >= DATE_SUB(CURDATE(), INTERVAL ? DAY)""",
            [f"%{tag}%", days],
        )
        if rows and rows[0]["cnt"] > 0:
            return True
    return False


def _check_leader_capital(tags, days):
    """KG驱动的个股发现 + 龙头识别

    流程:
    1. 从KG查 theme→industry 关联行业
    2. 从 stock_info 查行业内股票
    3. 合并新闻标签关联股票（item_companies）
    4. 按7日主力净流入排序，选龙头
    5. 返回行业分组的股票列表（存入 extra_json 供前端展示）

    如果KG无数据则 fallback 到纯新闻标签关联。
    """
    # ── A. KG路径：theme → industry → stocks ──
    kg_industry_stocks = {}  # {industry_name: {code: {name, inflow, source}}}
    try:
        from knowledge_graph.kg_query import get_theme_industries
        kg_industries = get_theme_industries(tags)
        if kg_industries:
            industry_names = list({ind["industry_name"] for ind in kg_industries})
            for ind_name in industry_names[:10]:
                rows = execute_query(
                    """SELECT stock_code, stock_name, industry_l2
                       FROM stock_info
                       WHERE (industry_l1=? OR industry_l2=? OR industry_l1 LIKE ? OR industry_l2 LIKE ?)
                       LIMIT 30""",
                    [ind_name, ind_name, f"%{ind_name}%", f"%{ind_name}%"],
                )
                for r in (rows or []):
                    if r["stock_code"]:
                        kg_industry_stocks.setdefault(ind_name, {})[r["stock_code"]] = {
                            "stock_name": r["stock_name"],
                            "industry_l2": r.get("industry_l2", ""),
                            "source": "kg",
                        }
    except Exception as e:
        logger.warning(f"KG行业查询失败，将使用新闻标签兜底: {e}")

    # ── B. 新闻标签路径：tags → item_companies ──
    news_stocks = {}  # {code: {name, match_tags}}
    for tag in tags:
        rows = execute_query(
            """SELECT DISTINCT ic.stock_code, ic.stock_name
               FROM item_companies ic
               JOIN cleaned_items ci ON ic.cleaned_item_id = ci.id
               WHERE ci.tags_json LIKE ?
               AND ci.cleaned_at >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
               AND ic.relevance = 'primary'""",
            [f"%{tag}%", days],
        )
        for r in (rows or []):
            if r["stock_code"]:
                if r["stock_code"] not in news_stocks:
                    news_stocks[r["stock_code"]] = {
                        "stock_name": r["stock_name"], "match_tags": 0
                    }
                news_stocks[r["stock_code"]]["match_tags"] += 1

    # ── C. 合并：KG股票 + 新闻股票 ──
    all_codes = set()
    for ind_stocks in kg_industry_stocks.values():
        all_codes.update(ind_stocks.keys())
    all_codes.update(news_stocks.keys())

    if not all_codes:
        return None

    # ── D. 批量查资金流（最近7个交易日） ──
    code_list = list(all_codes)
    flow_map = {}  # code -> total_inflow
    # 分批查询避免参数过多
    for i in range(0, len(code_list), 50):
        batch = code_list[i:i+50]
        ph = ",".join(["?"] * len(batch))
        rows = execute_query(
            f"""SELECT stock_code, SUM(main_net_inflow) as total_inflow
                FROM capital_flow
                WHERE stock_code IN ({ph})
                AND trade_date >= (SELECT DISTINCT trade_date FROM capital_flow ORDER BY trade_date DESC LIMIT 1 OFFSET 6)
                GROUP BY stock_code""",
            batch,
        )
        for r in (rows or []):
            if r["total_inflow"] is not None:
                flow_map[r["stock_code"]] = float(r["total_inflow"])

    # ── E. 构建行业分组结果（与研究层格式一致） ──
    kg_stock_groups = []
    used_codes = set()

    for ind_name, ind_stocks in kg_industry_stocks.items():
        group_stocks = []
        for code, info in ind_stocks.items():
            inflow = flow_map.get(code, 0)
            source = "both" if code in news_stocks else "kg"
            group_stocks.append({
                "stock_code": code,
                "stock_name": info["stock_name"],
                "industry_l2": info.get("industry_l2", ""),
                "total_inflow": inflow,
                "match_tags": news_stocks.get(code, {}).get("match_tags", 0),
                "source": source,
            })
            used_codes.add(code)
        group_stocks.sort(key=lambda x: x["total_inflow"], reverse=True)
        kg_stock_groups.append({
            "industry": ind_name,
            "stocks": group_stocks[:8],
        })

    # 新闻独有的股票归入"其他关联标的"
    other_stocks = []
    for code, info in news_stocks.items():
        if code not in used_codes:
            other_stocks.append({
                "stock_code": code,
                "stock_name": info["stock_name"],
                "industry_l2": "",
                "total_inflow": flow_map.get(code, 0),
                "match_tags": info["match_tags"],
                "source": "news",
            })
    if other_stocks:
        other_stocks.sort(key=lambda x: x["total_inflow"], reverse=True)
        kg_stock_groups.append({
            "industry": "其他关联标的",
            "stocks": other_stocks[:8],
        })

    # ── F. 选龙头：全部股票中资金流入最大且 > 0 ──
    best = None
    group_total_inflow = sum(flow_map.get(c, 0) for c in all_codes)
    for code in all_codes:
        inflow = flow_map.get(code, 0)
        name = news_stocks.get(code, {}).get("stock_name", "")
        if not name:
            for ind_stocks in kg_industry_stocks.values():
                if code in ind_stocks:
                    name = ind_stocks[code]["stock_name"]
                    break
        if best is None or inflow > best["net_inflow"]:
            best = {"stock_code": code, "stock_name": name, "net_inflow": inflow}

    if not best or best["net_inflow"] <= 0:
        return None

    # 组总体净流入必须为正
    if group_total_inflow <= 0:
        return None

    # 总市值：优先用 stock_info.market_cap（亿元单位），否则用最新收盘价 × 总股本
    ph = ",".join(["?"] * len(code_list))
    cap_rows = execute_query(
        f"SELECT SUM(market_cap) as total_cap FROM stock_info WHERE stock_code IN ({ph}) AND market_cap > 0",
        code_list,
    )
    group_total_cap = float(cap_rows[0]["total_cap"] or 0) * 1e8 if cap_rows and cap_rows[0]["total_cap"] else 0  # 转换为元

    # 如果 market_cap 为空，尝试用 close × total_shares 计算
    if group_total_cap == 0:
        cap_rows2 = execute_query(
            f"""SELECT SUM(sd.close * si.total_shares) as total_cap
                FROM stock_daily sd
                JOIN stock_info si ON sd.stock_code = si.stock_code
                WHERE sd.stock_code IN ({ph})
                AND si.total_shares > 0
                AND sd.trade_date >= DATE_SUB(CURDATE(), INTERVAL 10 DAY)""",
            code_list,
        )
        group_total_cap = float(cap_rows2[0]["total_cap"] or 0) if cap_rows2 and cap_rows2[0]["total_cap"] else 0

    # 按天汇总流入（柱状图）- 取最近7个交易日
    daily_inflow = []
    daily_rows = execute_query(
        f"""SELECT trade_date, SUM(main_net_inflow) as day_inflow
            FROM capital_flow
            WHERE stock_code IN ({ph})
            AND trade_date >= (SELECT DISTINCT trade_date FROM capital_flow ORDER BY trade_date DESC LIMIT 1 OFFSET 6)
            GROUP BY trade_date ORDER BY trade_date""",
        code_list,
    )
    for dr in (daily_rows or []):
        daily_inflow.append({
            "date": str(dr["trade_date"]),
            "inflow": float(dr["day_inflow"] or 0),
        })

    best["group_total_inflow"] = group_total_inflow
    best["group_total_cap"] = group_total_cap
    best["group_stock_count"] = len(all_codes)
    best["daily_inflow"] = daily_inflow
    best["kg_stock_groups"] = kg_stock_groups
    return best


def get_group_related_news(tags, days=7, limit=8):
    """获取标签组关联的清洗后新闻摘要，按关联度×重要性排序"""
    all_news = {}
    for tag in tags:
        items = execute_query(
            """SELECT ci.id, ci.summary, ci.sentiment, ci.importance,
                      ci.event_type, ci.cleaned_at
               FROM cleaned_items ci
               WHERE ci.tags_json LIKE ? AND ci.cleaned_at >= date('now', ?)
               ORDER BY ci.importance DESC LIMIT 20""",
            [f"%{tag}%", f"-{days} days"],
        )
        for item in items:
            nid = item["id"]
            if nid not in all_news:
                all_news[nid] = dict(item)
                all_news[nid]["match_count"] = 0
            all_news[nid]["match_count"] += 1

    for n in all_news.values():
        n["score"] = n["match_count"] * n.get("importance", 1)

    ranked = sorted(all_news.values(), key=lambda x: x["score"], reverse=True)
    return ranked[:limit]


def save_tag_group(group_name, tags, group_logic=None, time_range=7, extra=None):
    """保存标签组到数据库"""
    total_freq = 0
    for tag in tags:
        freq_rows = execute_query(
            """SELECT COUNT(*) as cnt FROM dashboard_tag_frequency
               WHERE tag_name=? AND appear_date >= date('now', ?)""",
            [tag, f"-{time_range} days"],
        )
        total_freq += freq_rows[0]["cnt"] if freq_rows else 0

    extra_json = json.dumps(extra, ensure_ascii=False) if extra else None
    group_id = execute_insert(
        """INSERT INTO tag_groups (group_name, tags_json, group_logic, time_range, total_frequency, extra_json)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [group_name, json.dumps(tags, ensure_ascii=False),
         group_logic, time_range, total_freq, extra_json],
    )

    # 不再将热点研究存入 content_summaries，避免污染 RAG 检索

    return group_id


def get_saved_groups():
    """获取已保存的标签组"""
    return execute_query(
        "SELECT * FROM tag_groups ORDER BY total_frequency DESC"
    )


def delete_tag_group(group_id):
    """删除标签组"""
    execute_insert("DELETE FROM tag_groups WHERE id=?", [group_id])


def clear_all_tag_groups():
    """清空所有标签组（推荐前调用）"""
    execute_insert("DELETE FROM group_chat_messages", [])
    execute_insert("DELETE FROM tag_group_research", [])
    execute_insert("DELETE FROM tag_groups", [])

"""从8类榜单提取高频标签组推荐 — 增强版：智能分组+持久化"""
import json
import logging
from collections import Counter, defaultdict
from utils.db_utils import execute_query, execute_insert
from utils.claude_client import call_claude_json

logger = logging.getLogger(__name__)


def get_top_tags(days=7, limit=30):
    """获取指定时间段内出现频次最高的标签"""
    return execute_query(
        """SELECT tag_name, tag_type,
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


def recommend_tag_groups(days=7, top_n=5):
    """推荐关联标签组 — 共现聚类 + Claude投资逻辑验证"""
    tags = get_top_tags(days, 40)
    if not tags:
        return []

    # 构建共现矩阵：同一天同一榜单出现的标签视为关联
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

    # 统计标签对共现次数
    pair_counter = Counter()
    for key, tag_list in co_occur.items():
        unique_tags = list(set(tag_list))
        for i in range(len(unique_tags)):
            for j in range(i + 1, len(unique_tags)):
                pair = tuple(sorted([unique_tags[i], unique_tags[j]]))
                pair_counter[pair] += 1

    # 贪心聚类：生成候选组（多生成一些，后面用Claude过滤）
    used = set()
    candidates = []
    tag_freq_map = {t["tag_name"]: t["total_freq"] for t in tags}

    for pair, freq in pair_counter.most_common(50):
        if pair[0] in used and pair[1] in used:
            continue
        group_tags = set(pair)
        for other_pair, other_freq in pair_counter.most_common(100):
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

    # Claude语义验证：过滤无投资逻辑的组合
    validated = _validate_groups_with_claude(candidates)

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


def _validate_groups_with_claude(candidates):
    """用Claude验证候选标签组的投资逻辑"""
    desc = ""
    for i, c in enumerate(candidates):
        desc += f"候选组{i}: {' + '.join(c['tags'])} (频次:{c['frequency']})\n"

    try:
        results = call_claude_json(TAG_VALIDATION_PROMPT, desc, max_tokens=2000)
    except Exception as e:
        logger.warning(f"Claude标签组验证失败，回退到原始结果: {e}")
        for c in candidates:
            c["group_name"] = " + ".join(c["tags"][:3])
        return candidates

    if not isinstance(results, list):
        logger.warning("Claude返回格式异常，回退到原始结果")
        for c in candidates:
            c["group_name"] = " + ".join(c["tags"][:3])
        return candidates

    validated = []
    for item in results:
        idx = item.get("index", -1)
        if not item.get("valid") or idx < 0 or idx >= len(candidates):
            continue
        group = candidates[idx]
        group["group_name"] = item.get("group_name", " + ".join(group["tags"][:3]))
        group["group_logic"] = item.get("logic", "")
        refined = item.get("refined_tags")
        if refined and len(refined) >= 2:
            group["tags"] = refined
            group["tag_count"] = len(refined)
        validated.append(group)

    if not validated:
        logger.warning("Claude过滤后无有效标签组，回退频次最高的候选")
        top = candidates[0]
        top["group_name"] = " + ".join(top["tags"][:3])
        return [top]

    return validated


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


def save_tag_group(group_name, tags, group_logic=None, time_range=7):
    """保存标签组到数据库"""
    total_freq = 0
    for tag in tags:
        freq_rows = execute_query(
            """SELECT COUNT(*) as cnt FROM dashboard_tag_frequency
               WHERE tag_name=? AND appear_date >= date('now', ?)""",
            [tag, f"-{time_range} days"],
        )
        total_freq += freq_rows[0]["cnt"] if freq_rows else 0

    return execute_insert(
        """INSERT INTO tag_groups (group_name, tags_json, group_logic, time_range, total_frequency)
           VALUES (?, ?, ?, ?, ?)""",
        [group_name, json.dumps(tags, ensure_ascii=False),
         group_logic, time_range, total_freq],
    )


def get_saved_groups():
    """获取已保存的标签组"""
    return execute_query(
        "SELECT * FROM tag_groups ORDER BY total_frequency DESC"
    )


def delete_tag_group(group_id):
    """删除标签组"""
    execute_insert("DELETE FROM tag_groups WHERE id=?", [group_id])

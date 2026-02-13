"""标签组深度分析 — 成组逻辑 + 关联新闻排序 + 板块热度"""
import json
import logging
from utils.db_utils import execute_query
from utils.claude_client import call_claude

logger = logging.getLogger(__name__)

GROUP_LOGIC_PROMPT = """你是金融市场分析专家。请分析以下标签组为什么会关联出现，解释它们之间的逻辑关系。

要求：
1. 分析这些标签之间的内在联系（产业链、政策驱动、资金联动等）
2. 解释为什么它们会在同一时期成为市场热点
3. 判断这个组合代表的投资主线是什么
4. 评估这个主线的持续性（短期炒作/中期趋势/长期逻辑）

输出简洁的中文分析（不超过400字）。"""


def analyze_tag_group(tags, days=7):
    """分析标签组：成组逻辑 + 关联新闻 + 板块热度"""
    result = {
        "tags": tags,
        "group_logic": None,
        "news_ranked": [],
        "sector_heat": [],
    }

    # 1. 成组逻辑分析（Claude）
    result["group_logic"] = _analyze_group_logic(tags, days)

    # 2. 关联新闻排序（按关联度×重要性）
    result["news_ranked"] = _get_ranked_news(tags, days)

    # 3. 板块热度（行业资金流向时序）
    result["sector_heat"] = _get_sector_heat(tags, days)

    return result


def _analyze_group_logic(tags, days):
    """使用Claude分析成组逻辑"""
    tags_str = ", ".join(tags)

    # 收集上下文：这些标签出现在哪些榜单
    context = f"标签组: {tags_str}\n\n各标签出现情况:\n"
    for tag in tags:
        freq = execute_query(
            """SELECT dashboard_type, COUNT(*) as cnt
               FROM dashboard_tag_frequency
               WHERE tag_name=? AND appear_date >= date('now', ?)
               GROUP BY dashboard_type""",
            [tag, f"-{days} days"],
        )
        if freq:
            dists = ", ".join(f"榜单{f['dashboard_type']}({f['cnt']}次)" for f in freq)
            context += f"- {tag}: {dists}\n"

    # 收集关联新闻摘要
    news = _get_ranked_news(tags, days)
    if news:
        context += "\n近期关联新闻:\n"
        for n in news[:10]:
            context += f"- [{n.get('sentiment', '')}] {n.get('summary', '')}\n"

    try:
        return call_claude(GROUP_LOGIC_PROMPT, context, max_tokens=800)
    except Exception as e:
        logger.warning(f"成组逻辑分析失败: {e}")
        return f"标签组「{tags_str}」在近{days}天内频繁共现，可能存在产业链或政策关联。"


def _get_ranked_news(tags, days):
    """获取关联新闻，按（关联标签数×重要性）排序"""
    all_news = {}
    for tag in tags:
        items = execute_query(
            """SELECT ci.id, ci.summary, ci.sentiment, ci.importance,
                      ci.event_type, ci.tags_json, ci.cleaned_at
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

    # 计算排序分数：关联标签数 × 重要性
    for n in all_news.values():
        n["score"] = n["match_count"] * n.get("importance", 1)

    ranked = sorted(all_news.values(), key=lambda x: x["score"], reverse=True)
    return ranked[:30]


def _get_sector_heat(tags, days):
    """获取板块热度：行业资金流向时序数据"""
    sector_data = []
    for tag in tags:
        flows = execute_query(
            """SELECT trade_date, industry_name, net_inflow
               FROM industry_capital_flow
               WHERE industry_name LIKE ? AND trade_date >= date('now', ?)
               ORDER BY trade_date""",
            [f"%{tag}%", f"-{days} days"],
        )
        if flows:
            sector_data.append({
                "tag": tag,
                "flows": [dict(f) for f in flows],
                "total_inflow": sum(f.get("net_inflow", 0) for f in flows),
            })

    # 也查个股资金流向汇总
    for tag in tags:
        stock_flows = execute_query(
            """SELECT cf.trade_date, SUM(cf.main_net_inflow) as total_main_inflow
               FROM capital_flow cf
               JOIN item_companies ic ON cf.stock_code=ic.stock_code
               JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
               WHERE ci.tags_json LIKE ? AND cf.trade_date >= date('now', ?)
               GROUP BY cf.trade_date
               ORDER BY cf.trade_date""",
            [f"%{tag}%", f"-{days} days"],
        )
        if stock_flows:
            sector_data.append({
                "tag": f"{tag}(个股汇总)",
                "flows": [dict(f) for f in stock_flows],
                "total_inflow": sum(f.get("total_main_inflow", 0) for f in stock_flows),
            })

    return sorted(sector_data, key=lambda x: abs(x.get("total_inflow", 0)), reverse=True)

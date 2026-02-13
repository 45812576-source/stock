"""标签组宏观/行业深度分析 + 个股推荐Top10"""
import json
import logging
from utils.db_utils import execute_query, execute_insert
from utils.claude_client import call_claude

logger = logging.getLogger(__name__)

MACRO_ANALYSIS_PROMPT = """你是宏观经济分析师。请基于以下标签组和关联信息，生成宏观分析报告。

分析框架：
1. 宏观背景：当前经济周期阶段、政策环境
2. 驱动因素：推动这组标签成为热点的宏观因素
3. 资金面分析：流动性、利率、汇率对该主线的影响
4. 风险因素：可能的宏观风险和政策转向
5. 投资启示：基于宏观判断的配置建议

输出结构化的中文报告（不超过600字）。"""

INDUSTRY_ANALYSIS_PROMPT = """你是行业分析师。请基于以下标签组和关联信息，生成行业分析报告。

分析框架：
1. 行业格局：竞争格局、市场规模、增长趋势
2. 产业链分析：上中下游关系、利润分配
3. 催化剂：近期推动行业热度的事件/政策
4. 估值水平：行业整体估值是否合理
5. 投资机会：细分赛道推荐、龙头vs弹性标的

输出结构化的中文报告（不超过600字）。"""


def research_tag_group(group_id):
    """对标签组进行完整深度研究（6维分析）"""
    group = execute_query("SELECT * FROM tag_groups WHERE id=?", [group_id])
    if not group:
        return None
    group = group[0]
    tags = json.loads(group["tags_json"])
    tags_str = ", ".join(tags)

    # 收集关联数据
    news_data = _collect_news(tags)
    stock_flows = _collect_stock_flows(tags)
    sector_heat = _collect_sector_heat(tags)

    # 构建分析上下文
    context = _build_context(tags_str, news_data, stock_flows, sector_heat)

    # 生成宏观报告
    try:
        macro_report = call_claude(MACRO_ANALYSIS_PROMPT, context, max_tokens=1200)
    except Exception as e:
        logger.warning(f"宏观分析失败: {e}")
        macro_report = "宏观分析生成失败，请稍后重试。"

    # 生成行业报告
    try:
        industry_report = call_claude(INDUSTRY_ANALYSIS_PROMPT, context, max_tokens=1200)
    except Exception as e:
        logger.warning(f"行业分析失败: {e}")
        industry_report = "行业分析生成失败，请稍后重试。"

    # 推荐个股Top10
    top10 = _rank_stocks(tags, stock_flows)

    # 保存研究结果
    research_id = execute_insert(
        """INSERT INTO tag_group_research
           (group_id, research_date, macro_report, industry_report,
            news_summary_json, sector_heat_json, top10_stocks_json)
           VALUES (?, date('now'), ?, ?, ?, ?, ?)""",
        [group_id, macro_report, industry_report,
         json.dumps([dict(n) for n in news_data[:20]], ensure_ascii=False, default=str),
         json.dumps(sector_heat, ensure_ascii=False, default=str),
         json.dumps(top10, ensure_ascii=False, default=str)],
    )

    return {
        "research_id": research_id,
        "macro_report": macro_report,
        "industry_report": industry_report,
        "news": news_data[:20],
        "sector_heat": sector_heat,
        "top10_stocks": top10,
    }


def get_group_research_history(group_id, limit=5):
    """获取标签组的研究历史"""
    return execute_query(
        """SELECT * FROM tag_group_research
           WHERE group_id=? ORDER BY research_date DESC LIMIT ?""",
        [group_id, limit],
    )


def _collect_news(tags):
    """收集标签关联新闻"""
    all_news = {}
    for tag in tags:
        items = execute_query(
            """SELECT ci.id, ci.summary, ci.sentiment, ci.importance,
                      ci.event_type, ci.tags_json, ci.cleaned_at
               FROM cleaned_items ci
               WHERE ci.tags_json LIKE ?
               ORDER BY ci.cleaned_at DESC LIMIT 15""",
            [f"%{tag}%"],
        )
        for item in items:
            nid = item["id"]
            if nid not in all_news:
                all_news[nid] = dict(item)
                all_news[nid]["match_tags"] = 0
            all_news[nid]["match_tags"] += 1

    ranked = sorted(all_news.values(),
                    key=lambda x: x["match_tags"] * x.get("importance", 1),
                    reverse=True)
    return ranked


def _collect_stock_flows(tags):
    """收集标签关联个股的资金流向"""
    stock_map = {}
    for tag in tags:
        flows = execute_query(
            """SELECT cf.stock_code, si.stock_name,
                      SUM(cf.main_net_inflow) as total_inflow,
                      COUNT(*) as days
               FROM capital_flow cf
               LEFT JOIN stock_info si ON cf.stock_code=si.stock_code
               JOIN item_companies ic ON cf.stock_code=ic.stock_code
               JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
               WHERE ci.tags_json LIKE ?
               GROUP BY cf.stock_code
               ORDER BY total_inflow DESC LIMIT 20""",
            [f"%{tag}%"],
        )
        for f in flows:
            code = f["stock_code"]
            if code not in stock_map:
                stock_map[code] = {
                    "stock_code": code,
                    "stock_name": f.get("stock_name") or code,
                    "total_inflow": 0,
                    "match_tags": 0,
                }
            stock_map[code]["total_inflow"] += f.get("total_inflow", 0) or 0
            stock_map[code]["match_tags"] += 1

    return sorted(stock_map.values(), key=lambda x: x["total_inflow"], reverse=True)


def _collect_sector_heat(tags):
    """收集板块热度数据"""
    heat = []
    for tag in tags:
        flows = execute_query(
            """SELECT trade_date, industry_name, net_inflow
               FROM industry_capital_flow
               WHERE industry_name LIKE ?
               ORDER BY trade_date DESC LIMIT 30""",
            [f"%{tag}%"],
        )
        if flows:
            total = sum(f.get("net_inflow", 0) or 0 for f in flows)
            heat.append({
                "tag": tag,
                "total_inflow": total,
                "daily_flows": [{"date": f["trade_date"],
                                 "inflow": f.get("net_inflow", 0)} for f in flows],
            })
    return heat


def _rank_stocks(tags, stock_flows):
    """推荐个股Top10：按（个股净流入 / 标签下全部个股净流入之和）排序"""
    if not stock_flows:
        return []

    total_inflow = sum(abs(s.get("total_inflow", 0)) for s in stock_flows)
    if total_inflow == 0:
        return stock_flows[:10]

    top10 = []
    for s in stock_flows[:10]:
        inflow = s.get("total_inflow", 0)
        ratio = inflow / total_inflow if total_inflow > 0 else 0
        top10.append({
            "stock_code": s["stock_code"],
            "stock_name": s["stock_name"],
            "total_inflow": round(inflow, 2),
            "inflow_ratio": round(ratio * 100, 2),
            "match_tags": s.get("match_tags", 0),
        })
    return top10


def _build_context(tags_str, news_data, stock_flows, sector_heat):
    """构建Claude分析上下文"""
    context = f"标签组: {tags_str}\n\n"

    context += "=== 关联新闻摘要 ===\n"
    for n in news_data[:15]:
        icon = {"positive": "利好", "negative": "利空"}.get(n.get("sentiment"), "中性")
        context += f"- [{icon}][重要性{n.get('importance', 0)}] {n.get('summary', '')}\n"

    if sector_heat:
        context += "\n=== 板块资金流向 ===\n"
        for sh in sector_heat[:5]:
            context += f"- {sh['tag']}: 累计净流入 {sh.get('total_inflow', 0)/1e8:.2f}亿\n"

    if stock_flows:
        context += "\n=== 关联个股资金Top5 ===\n"
        for sf in stock_flows[:5]:
            context += (f"- {sf['stock_code']} {sf['stock_name']}: "
                        f"净流入 {sf.get('total_inflow', 0)/1e4:.0f}万\n")

    return context

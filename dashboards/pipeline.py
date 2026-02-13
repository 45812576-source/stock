"""Dashboard统一生成流水线 — 运行8类榜单+标签频次统计"""
import json
import logging
from datetime import datetime
from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)


def generate_all_dashboards(date_str=None):
    """生成所有8类Dashboard榜单

    Returns:
        dict: 各榜单生成结果
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    run_id = execute_insert(
        "INSERT INTO pipeline_runs (pipeline_name, details_json) VALUES ('dashboard_generate', ?)",
        [json.dumps({"date": date_str})],
    )

    results = {}

    # 榜单1: 宏观利好利空
    try:
        from dashboards.macro_highlights import generate_macro_highlights
        data = generate_macro_highlights(date_str)
        results["macro_highlights"] = {
            "positives": len(data.get("positives", [])),
            "negatives": len(data.get("negatives", [])),
        }
        _extract_tags_from_cleaned(data.get("positives", []) + data.get("negatives", []),
                                   dashboard_type=1, date_str=date_str)
    except Exception as e:
        logger.error(f"榜单1生成失败: {e}")
        results["macro_highlights"] = {"error": str(e)}

    # 榜单2: 行业重大利好Top10
    try:
        from dashboards.industry_news import generate_industry_news
        data = generate_industry_news(date_str)
        results["industry_news"] = {"count": len(data)}
        _extract_tags_from_cleaned(data, dashboard_type=2, date_str=date_str)
    except Exception as e:
        logger.error(f"榜单2生成失败: {e}")
        results["industry_news"] = {"error": str(e)}

    # 榜单3: 行业资金净流入Top10
    try:
        from dashboards.industry_capital import generate_industry_capital
        data = generate_industry_capital(date_str)
        results["industry_capital"] = {"count": len(data)}
        _extract_industry_tags(data, dashboard_type=3, date_str=date_str)
    except Exception as e:
        logger.error(f"榜单3生成失败: {e}")

    # 榜单4: 财报超预期Top10
    try:
        from dashboards.earnings_beat import generate_earnings_beat
        data = generate_earnings_beat()
        results["earnings_beat"] = {"count": len(data)}
        _extract_stock_tags(data, dashboard_type=4, date_str=date_str)
    except Exception as e:
        logger.error(f"榜单4生成失败: {e}")

    # 榜单5: 3月券商覆盖Top10
    try:
        from dashboards.broker_coverage_3m import generate_broker_coverage_3m
        data = generate_broker_coverage_3m(date_str)
        results["broker_3m"] = {"count": len(data)}
        _extract_stock_tags(data, dashboard_type=5, date_str=date_str)
    except Exception as e:
        logger.error(f"榜单5生成失败: {e}")

    # 榜单6: 1月券商覆盖Top10
    try:
        from dashboards.broker_coverage_1m import generate_broker_coverage_1m
        data = generate_broker_coverage_1m(date_str)
        results["broker_1m"] = {"count": len(data)}
        _extract_stock_tags(data, dashboard_type=6, date_str=date_str)
    except Exception as e:
        logger.error(f"榜单6生成失败: {e}")

    # 榜单7: 个股资金/市值Top10
    try:
        from dashboards.stock_capital import generate_stock_capital
        data = generate_stock_capital(date_str)
        results["stock_capital"] = {"count": len(data)}
        _extract_stock_tags(data, dashboard_type=7, date_str=date_str)
    except Exception as e:
        logger.error(f"榜单7生成失败: {e}")

    # 榜单8: 宏观资金面指标
    try:
        from dashboards.macro_indicators import generate_macro_indicators
        data = generate_macro_indicators()
        ind_count = len(data.get("indicators", []))
        nb_count = len(data.get("northbound", []))
        results["macro_indicators"] = {"indicators": ind_count, "northbound": nb_count}
        _extract_macro_tags(data, dashboard_type=8, date_str=date_str)
    except Exception as e:
        logger.error(f"榜单8生成失败: {e}")

    # 更新流水线记录
    execute_insert(
        """UPDATE pipeline_runs SET finished_at=CURRENT_TIMESTAMP,
           status='success', details_json=? WHERE id=?""",
        [json.dumps(results, ensure_ascii=False, default=str), run_id],
    )

    logger.info(f"Dashboard生成完成: {results}")
    return results


# ========== 标签频次提取 ==========

def _extract_tags_from_cleaned(items, dashboard_type, date_str):
    """从cleaned_items结果中提取标签频次"""
    for rank, item in enumerate(items, 1):
        tags_raw = item.get("tags_json", "[]")
        try:
            tags = json.loads(tags_raw) if isinstance(tags_raw, str) else tags_raw
        except (json.JSONDecodeError, TypeError):
            tags = []

        for tag in tags:
            if not tag:
                continue
            # 判断标签类型
            tag_type = _classify_tag(tag)
            execute_insert(
                """INSERT OR REPLACE INTO dashboard_tag_frequency
                   (tag_name, tag_type, dashboard_type, appear_date, rank_position, context_json)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [tag, tag_type, dashboard_type, date_str, rank,
                 json.dumps({"summary": item.get("summary", "")}, ensure_ascii=False)],
            )


def _extract_industry_tags(items, dashboard_type, date_str):
    """从行业资金流向中提取标签"""
    for rank, item in enumerate(items, 1):
        name = item.get("industry_name", "")
        if not name:
            continue
        execute_insert(
            """INSERT OR REPLACE INTO dashboard_tag_frequency
               (tag_name, tag_type, dashboard_type, appear_date, rank_position, context_json)
               VALUES (?, 'industry', ?, ?, ?, ?)""",
            [name, dashboard_type, date_str, rank,
             json.dumps({
                 "net_inflow": item.get("net_inflow"),
                 "change_pct": item.get("change_pct"),
                 "leading_stock": item.get("leading_stock"),
             }, ensure_ascii=False, default=str)],
        )


def _extract_stock_tags(items, dashboard_type, date_str):
    """从个股相关榜单中提取标签"""
    for rank, item in enumerate(items, 1):
        code = item.get("stock_code", "")
        name = item.get("stock_name", "")
        if not code:
            continue
        execute_insert(
            """INSERT OR REPLACE INTO dashboard_tag_frequency
               (tag_name, tag_type, dashboard_type, appear_date, rank_position, context_json)
               VALUES (?, 'stock', ?, ?, ?, ?)""",
            [f"{code} {name}".strip(), dashboard_type, date_str, rank,
             json.dumps(dict(item), ensure_ascii=False, default=str)],
        )


def _extract_macro_tags(data, dashboard_type, date_str):
    """从宏观指标中提取标签"""
    for rank, item in enumerate(data.get("indicators", []), 1):
        name = item.get("indicator_name", "")
        if not name:
            continue
        execute_insert(
            """INSERT OR REPLACE INTO dashboard_tag_frequency
               (tag_name, tag_type, dashboard_type, appear_date, rank_position, context_json)
               VALUES (?, 'macro', ?, ?, ?, ?)""",
            [name, dashboard_type, date_str, rank,
             json.dumps({
                 "value": item.get("value"),
                 "unit": item.get("unit"),
             }, ensure_ascii=False, default=str)],
        )


def _classify_tag(tag):
    """简单分类标签类型"""
    macro_keywords = ["降息", "降准", "加息", "GDP", "CPI", "PPI", "PMI",
                      "财政", "货币", "央行", "美联储", "汇率", "利率",
                      "通胀", "就业", "贸易", "关税"]
    industry_keywords = ["半导体", "新能源", "AI", "人工智能", "医药", "消费",
                         "汽车", "地产", "银行", "保险", "券商", "军工",
                         "光伏", "锂电", "芯片", "5G", "云计算", "白酒"]

    for kw in macro_keywords:
        if kw in tag:
            return "macro"
    for kw in industry_keywords:
        if kw in tag:
            return "industry"
    return "theme"


def get_tag_frequency_summary(days=7):
    """获取标签频次汇总（供热点动向使用）"""
    return execute_query(
        """SELECT tag_name, tag_type,
                  COUNT(*) as appear_count,
                  SUM(CASE WHEN dashboard_type <= 2 THEN 1 ELSE 0 END) as macro_count,
                  SUM(CASE WHEN dashboard_type BETWEEN 3 AND 6 THEN 1 ELSE 0 END) as industry_count,
                  SUM(CASE WHEN dashboard_type >= 7 THEN 1 ELSE 0 END) as stock_count,
                  GROUP_CONCAT(DISTINCT dashboard_type) as dashboards,
                  MIN(rank_position) as best_rank
           FROM dashboard_tag_frequency
           WHERE appear_date >= date('now', ?)
           GROUP BY tag_name
           ORDER BY appear_count DESC
           LIMIT 50""",
        [f"-{days} days"],
    )

"""驱动因子监控 — 从深研报告提取 topline/bottomline drivers，匹配近期新闻，AI 评估影响"""
import json
import logging
from utils.db_utils import execute_query

logger = logging.getLogger(__name__)


def get_portfolio_drivers(stock_codes: list) -> list:
    """从 deep_research 表获取持仓股票的 topline/bottomline drivers

    Returns:
        [{stock_code, stock_name, drivers: [{name, direction, type}]}]
    """
    if not stock_codes:
        return []

    placeholders = ",".join(["%s"] * len(stock_codes))
    rows = execute_query(
        f"""SELECT target, report_json, research_date
            FROM deep_research
            WHERE target IN ({placeholders}) AND research_type='stock'
            ORDER BY id DESC""",
        stock_codes,
    ) or []

    # 每个股票只取最新一条报告
    seen = set()
    result = []
    for r in rows:
        code = r.get("target", "")
        if code in seen:
            continue
        seen.add(code)

        try:
            report = json.loads(r.get("report_json") or "{}")
        except Exception:
            continue

        drivers = []
        for d in report.get("topline_drivers", []):
            name = d.get("driver_name") or d.get("name", "")
            if name:
                drivers.append({
                    "name": name,
                    "direction": d.get("direction", ""),
                    "type": "topline",
                })
        for d in report.get("bottomline_drivers", []):
            name = d.get("driver_name") or d.get("name", "")
            if name:
                drivers.append({
                    "name": name,
                    "direction": d.get("direction", ""),
                    "type": "bottomline",
                })

        if not drivers:
            continue

        # 查股票名
        info = execute_query(
            "SELECT stock_name FROM stock_info WHERE stock_code=%s", [code]
        )
        stock_name = info[0]["stock_name"] if info else code

        result.append({
            "stock_code": code,
            "stock_name": stock_name,
            "drivers": drivers,
        })

    return result


def match_drivers_to_news(drivers: list, days: int = 3) -> list:
    """用 hybrid_search 匹配每个 driver 是否有近期新闻命中

    Args:
        drivers: get_portfolio_drivers 返回的列表
        days: 匹配最近多少天的新闻（通过 chunk 时间过滤）

    Returns:
        [{stock_code, stock_name, driver_name, direction, type, has_news, news_summary, chunks}]
    """
    from retrieval.hybrid import hybrid_search

    alerts = []
    for stock in drivers:
        code = stock["stock_code"]
        stock_name = stock["stock_name"]
        for drv in stock["drivers"]:
            driver_name = drv["name"]
            try:
                hr = hybrid_search(
                    driver_name,
                    context={"stock_codes": [code]},
                    top_k=3,
                )
                chunks = hr.chunks or []
                has_news = len(chunks) > 0
                news_summary = ""
                if has_news and hr.merged_context:
                    news_summary = hr.merged_context[:300]
                alerts.append({
                    "stock_code": code,
                    "stock_name": stock_name,
                    "driver_name": driver_name,
                    "direction": drv.get("direction", ""),
                    "type": drv.get("type", ""),
                    "has_news": has_news,
                    "news_summary": news_summary,
                    "chunks": [
                        {"text": c.text[:200], "score": c.score, "source": c.source_doc_title}
                        for c in chunks[:2]
                    ],
                })
            except Exception as e:
                logger.warning(f"Driver match failed for {code}/{driver_name}: {e}")

    return alerts


def evaluate_driver_impact(driver_name: str, news_context: str, stock_code: str) -> dict:
    """AI 评估新闻对驱动因子的影响

    Returns:
        {impact_level: "高/中/低", direction: "正面/负面/中性", summary: "..."}
    """
    from utils.model_router import call_model

    system_prompt = f"""你是专业的股票研究员。请分析以下新闻对该股票驱动因子的影响。

驱动因子：{driver_name}
股票代码：{stock_code}

请以 JSON 格式输出（不要加 markdown 代码块）：
{{"impact_level": "高/中/低", "direction": "正面/负面/中性", "summary": "50字内简述影响"}}"""

    user_msg = f"相关新闻：\n{news_context[:1500]}"

    try:
        raw = call_model("chat", system_prompt, user_msg, max_tokens=300)
        # 尝试解析 JSON
        raw = raw.strip()
        result = json.loads(raw)
        return result
    except Exception:
        return {"impact_level": "中", "direction": "中性", "summary": "AI评估失败"}

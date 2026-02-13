"""持仓个股变化分析 — 自动deep research触发+变化highlight报告"""
import json
import logging
from datetime import datetime
from utils.db_utils import execute_query, execute_insert
from utils.claude_client import call_claude

logger = logging.getLogger(__name__)

CHANGE_HIGHLIGHT_PROMPT = """你是持仓股票变化分析专家。请基于以下新增信息，分析对持仓股票的影响变化。

对比维度：
1. 基本面变化（业绩、财务、经营）
2. 市场情绪变化（资金流向、机构动向）
3. 行业/政策变化
4. 技术面变化
5. 风险提示

请输出简洁的变化highlight报告（中文，不超过500字），重点标注：
- 🟢 利好变化
- 🔴 利空变化
- ⚠️ 需要关注的风险

最后给出操作建议：加仓/持有/减仓/清仓"""


def check_holding_updates(date_str=None):
    """检查持仓个股是否有新的关联信息，触发自动研究

    Returns:
        list: 触发的研究记录列表
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    holdings = execute_query("SELECT * FROM watchlist WHERE watch_type='holding'")
    triggered = []

    for h in holdings:
        code = h["stock_code"]
        name = h["stock_name"] or code

        # 检查今日是否有新的关联新闻/研报（重要性>=3）
        new_items = execute_query(
            """SELECT ci.id, ci.event_type, ci.summary, ci.importance,
                      ci.sentiment, ci.impact_analysis, ci.tags_json
               FROM item_companies ic JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
               WHERE ic.stock_code=? AND date(ci.cleaned_at)=? AND ci.importance >= 3
               ORDER BY ci.importance DESC""",
            [code, date_str],
        )

        if not new_items:
            continue

        # 检查是否已经触发过
        existing = execute_query(
            "SELECT id FROM holding_research_log WHERE stock_code=? AND trigger_date=?",
            [code, date_str],
        )
        if existing:
            continue

        # 生成变化highlight报告
        report = _generate_change_highlight(code, name, new_items)

        # 记录触发日志
        trigger_types = list(set(item["event_type"] for item in new_items))
        log_id = execute_insert(
            """INSERT INTO holding_research_log
               (stock_code, trigger_date, trigger_type, trigger_item_id,
                change_highlights_json, report_pushed)
               VALUES (?, ?, ?, ?, ?, 1)""",
            [code, date_str,
             ",".join(trigger_types),
             new_items[0]["id"],
             json.dumps({
                 "report": report,
                 "trigger_count": len(new_items),
                 "triggers": [{"summary": i["summary"], "importance": i["importance"],
                               "sentiment": i["sentiment"]} for i in new_items[:5]],
             }, ensure_ascii=False)],
        )

        triggered.append({
            "stock_code": code,
            "stock_name": name,
            "log_id": log_id,
            "trigger_count": len(new_items),
            "report": report,
        })

        logger.info(f"持仓{code}触发变化分析: {len(new_items)}条新信息")

    return triggered


def _generate_change_highlight(stock_code, stock_name, new_items):
    """生成变化highlight报告"""
    # 构建上下文
    context = f"持仓股票: {stock_code} {stock_name}\n\n"
    context += f"今日新增{len(new_items)}条关联信息:\n\n"

    for i, item in enumerate(new_items[:10], 1):
        sentiment_icon = {"positive": "🟢", "negative": "🔴"}.get(item["sentiment"], "⚪")
        context += f"{i}. {sentiment_icon} [{item['event_type']}] {item['summary']}\n"
        if item.get("impact_analysis"):
            context += f"   影响: {item['impact_analysis'][:100]}\n"

    # 获取最近的持仓信息
    positions = execute_query(
        "SELECT * FROM holding_positions WHERE stock_code=? AND status='open'",
        [stock_code],
    )
    if positions:
        p = positions[0]
        context += f"\n持仓信息: 买入价{p['buy_price']}, 数量{p['quantity']}, "
        context += f"买入日期{p['buy_date']}\n"

    # 获取最近行情
    recent = execute_query(
        """SELECT trade_date, close, change_pct FROM stock_daily
           WHERE stock_code=? ORDER BY trade_date DESC LIMIT 5""",
        [stock_code],
    )
    if recent:
        context += "\n近5日行情:\n"
        for r in recent:
            context += f"  {r['trade_date']}: {r['close']} ({r['change_pct']}%)\n"

    try:
        report = call_claude(CHANGE_HIGHLIGHT_PROMPT, context, max_tokens=1024)
        return report
    except Exception as e:
        logger.error(f"生成变化报告失败 {stock_code}: {e}")
        # 降级：生成简单摘要
        lines = [f"## {stock_code} {stock_name} 变化摘要\n"]
        for item in new_items[:5]:
            icon = {"positive": "🟢", "negative": "🔴"}.get(item["sentiment"], "⚪")
            lines.append(f"- {icon} {item['summary']}")
        return "\n".join(lines)


def get_today_push_notifications(date_str=None):
    """获取今日所有推送通知"""
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    logs = execute_query(
        """SELECT hrl.*, w.stock_name
           FROM holding_research_log hrl
           LEFT JOIN watchlist w ON hrl.stock_code=w.stock_code
           WHERE hrl.trigger_date=? AND hrl.report_pushed=1
           ORDER BY hrl.created_at DESC""",
        [date_str],
    )
    return logs


def get_stock_research_history(stock_code, limit=10):
    """获取个股研究历史"""
    return execute_query(
        """SELECT * FROM holding_research_log
           WHERE stock_code=? ORDER BY trigger_date DESC LIMIT ?""",
        [stock_code, limit],
    )

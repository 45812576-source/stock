"""深度研究主流程 — 集成Skills的多步分析"""
import json
import logging
from pathlib import Path
from utils.claude_client import call_claude, call_claude_json
from utils.db_utils import execute_insert, execute_query
from research.universal_db import (
    get_stock_profile, get_stock_technical_summary,
    get_peer_comparison, get_industry_data, get_macro_data,
)
from config import SKILLS_DIR

logger = logging.getLogger(__name__)


def _load_skill(skill_name):
    """从Skills目录加载SKILL.md内容作为system prompt"""
    path = SKILLS_DIR / skill_name / "SKILL.md"
    if path.exists():
        content = path.read_text(encoding="utf-8")
        logger.info(f"已加载Skill: {skill_name} ({len(content)}字符)")
        return content
    logger.warning(f"Skill未找到: {path}")
    return ""


# ==================== 个股深度研究（5步分析） ====================

STOCK_SYNTHESIS_PROMPT = """你是资深A股投资分析师。现在你已经完成了对一只股票的多维度深度分析，
请基于以下各维度的分析结果，给出最终的综合评估。

请输出严格的JSON格式：
{
    "financial_score": 0-100,
    "valuation_score": 0-100,
    "technical_score": 0-100,
    "sentiment_score": 0-100,
    "catalyst_score": 0-100,
    "risk_score": 0-100,
    "overall_score": 0-100,
    "recommendation": "强烈推荐|推荐|中性|谨慎|回避",
    "target_price": null或数字,
    "report": {
        "executive_summary": "一段话总结核心观点（150字内）",
        "financial_analysis": "财务分析要点（300字）",
        "valuation_analysis": "估值分析要点（300字）",
        "technical_analysis": "技术分析要点（300字）",
        "sentiment_analysis": "市场情绪与资金面分析（200字）",
        "catalyst_analysis": "催化剂与事件驱动分析（200字）",
        "risk_analysis": "风险评估与应对（200字）",
        "conclusion": "投资结论、操作建议和买卖点位（300字）"
    }
}

评分标准：
- 80-100: 该维度表现优秀
- 60-79: 该维度表现良好
- 40-59: 该维度表现一般
- 20-39: 该维度表现较差
- 0-19: 该维度存在严重问题
- risk_score: 分数越高代表风险越低（80分=低风险，20分=高风险）
- overall_score: 综合加权，不是简单平均"""


def deep_research_stock(stock_code, progress_callback=None):
    """个股深度研究 — 5步Skill驱动分析"""

    # === 数据充分性检查 + 自动补数据 ===
    if progress_callback:
        progress_callback("正在检查数据充分性...")

    from research.data_readiness import ensure_stock_data_ready
    readiness = ensure_stock_data_ready(
        stock_code, max_rounds=2, progress_callback=progress_callback
    )

    if not readiness["ready"]:
        logger.warning(f"{stock_code} 数据不充分: {readiness['missing']}")

    # === 原有逻辑 ===
    if progress_callback:
        progress_callback("正在获取个股数据...")

    profile = get_stock_profile(stock_code)
    if not profile.get("info"):
        return {"error": f"未找到股票信息: {stock_code}"}

    tech = get_stock_technical_summary(stock_code)
    peers = get_peer_comparison(stock_code)
    context = _build_stock_context(profile, tech, peers)

    # 附加数据充分性信息到context
    context += f"\n\n=== 数据充分性 ===\n置信度: {readiness['confidence']:.0%}\n"
    for dim, info in readiness["dimensions"].items():
        status = "✅" if info["ok"] else "⚠️不足"
        context += f"  {dim}: {status} ({info['count']}/{info['min']})\n"

    stock_name = profile["info"].get("stock_name", stock_code)

    step_results = {}

    # Step 1: 财务建模分析
    if progress_callback:
        progress_callback(f"[1/5] 财务建模分析 ({stock_name})...")
    skill_prompt = _load_skill("financial-model-construction")
    if skill_prompt:
        try:
            step_results["financial"] = call_claude(
                skill_prompt,
                f"请对以下股票进行财务分析和业务拆解：\n\n{context}",
                max_tokens=4096,
            )
        except Exception as e:
            logger.error(f"财务建模分析失败: {e}")
            step_results["financial"] = f"分析失败: {e}"

    # Step 2: 估值分析
    if progress_callback:
        progress_callback(f"[2/5] 估值分析 ({stock_name})...")
    skill_prompt = _load_skill("company-valuation")
    if skill_prompt:
        try:
            valuation_ctx = context
            if step_results.get("financial"):
                valuation_ctx += f"\n\n=== 财务建模分析结果 ===\n{step_results['financial'][:3000]}"
            step_results["valuation"] = call_claude(
                skill_prompt,
                f"请对以下股票进行估值分析：\n\n{valuation_ctx}",
                max_tokens=4096,
            )
        except Exception as e:
            logger.error(f"估值分析失败: {e}")
            step_results["valuation"] = f"分析失败: {e}"

    # Step 3: 技术分析
    if progress_callback:
        progress_callback(f"[3/5] 技术面分析 ({stock_name})...")
    skill_prompt = _load_skill("stock-chart-analysis")
    if skill_prompt:
        try:
            step_results["technical"] = call_claude(
                skill_prompt,
                f"请对以下股票进行技术分析：\n\n{context}",
                max_tokens=3000,
            )
        except Exception as e:
            logger.error(f"技术分析失败: {e}")
            step_results["technical"] = f"分析失败: {e}"

    # Step 4: 买点预测
    if progress_callback:
        progress_callback(f"[4/5] 买卖点预测 ({stock_name})...")
    skill_prompt = _load_skill("dynamic-stock-chart-predict")
    if skill_prompt:
        try:
            predict_ctx = context
            if step_results.get("technical"):
                predict_ctx += f"\n\n=== 技术分析结果 ===\n{step_results['technical'][:2000]}"
            step_results["prediction"] = call_claude(
                skill_prompt,
                f"请预测以下股票的阶段转换和买卖点：\n\n{predict_ctx}",
                max_tokens=3000,
            )
        except Exception as e:
            logger.error(f"买点预测失败: {e}")
            step_results["prediction"] = f"分析失败: {e}"

    # Step 5: 事件与情绪分析
    if progress_callback:
        progress_callback(f"[5/5] 事件与情绪分析 ({stock_name})...")
    skill_prompt = _load_skill("stock-event-analysis")
    if skill_prompt:
        try:
            step_results["event"] = call_claude(
                skill_prompt,
                f"请分析以下股票的近期事件和市场情绪：\n\n{context}",
                max_tokens=3000,
            )
        except Exception as e:
            logger.error(f"事件分析失败: {e}")
            step_results["event"] = f"分析失败: {e}"

    # 最终综合：汇总所有分析结果，生成6维评分
    if progress_callback:
        progress_callback(f"正在综合所有分析结果...")

    synthesis_input = f"股票: {stock_code} {stock_name}\n\n"
    synthesis_input += f"=== 基础数据 ===\n{context[:2000]}\n\n"
    for key, label in [
        ("financial", "财务建模分析"), ("valuation", "估值分析"),
        ("technical", "技术面分析"), ("prediction", "买卖点预测"),
        ("event", "事件与情绪分析"),
    ]:
        if step_results.get(key):
            synthesis_input += f"=== {label}结果 ===\n{step_results[key][:2500]}\n\n"

    try:
        result = call_claude_json(STOCK_SYNTHESIS_PROMPT, synthesis_input, max_tokens=4096)
    except Exception as e:
        logger.error(f"综合分析失败: {e}")
        return {"error": str(e)}

    if not isinstance(result, dict):
        return {"error": "分析结果格式异常"}

    if progress_callback:
        progress_callback("正在保存研究结果...")

    # 保存
    research_id = execute_insert(
        """INSERT INTO deep_research
           (research_type, target, research_date, financial_score, valuation_score,
            technical_score, sentiment_score, catalyst_score, risk_score,
            overall_score, report_json, recommendation)
           VALUES ('stock', ?, date('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [stock_code, result.get("financial_score"), result.get("valuation_score"),
         result.get("technical_score"), result.get("sentiment_score"),
         result.get("catalyst_score"), result.get("risk_score"),
         result.get("overall_score"),
         json.dumps(result.get("report", {}), ensure_ascii=False),
         result.get("recommendation")],
    )

    if result.get("overall_score", 0) >= 70:
        _create_opportunity(stock_code, profile["info"], result, research_id)

    logger.info(f"个股研究完成: {stock_code}, 综合评分: {result.get('overall_score')}")
    return {"research_id": research_id, "scores": {
        "financial": result.get("financial_score"),
        "valuation": result.get("valuation_score"),
        "technical": result.get("technical_score"),
        "sentiment": result.get("sentiment_score"),
        "catalyst": result.get("catalyst_score"),
        "risk": result.get("risk_score"),
        "overall": result.get("overall_score"),
    }, **result}


# ==================== 行业深度研究（2步分析） ====================

INDUSTRY_SYNTHESIS_PROMPT = """你是资深行业分析师。基于以下行业多维度分析结果，给出综合评估。

请输出严格的JSON格式：
{
    "industry_score": 0-100,
    "growth_score": 0-100,
    "policy_score": 0-100,
    "recommendation": "超配|标配|低配",
    "report": {
        "overview": "行业概况（300字）",
        "competition": "竞争格局（300字）",
        "value_chain": "产业链分析（300字）",
        "drivers": "驱动因素（200字）",
        "opportunities": "投资机会（300字）",
        "risks": "风险提示（200字）"
    },
    "top_stocks": ["推荐个股代码列表，最多10只"]
}"""


def deep_research_industry(industry_name, progress_callback=None):
    """行业深度研究 — 2步Skill驱动分析"""
    if progress_callback:
        progress_callback("正在获取行业数据...")

    data = get_industry_data(industry_name)
    context = _build_industry_context(industry_name, data)

    step_results = {}

    # Step 1: 行业影响分析（event-industry-impact）
    if progress_callback:
        progress_callback("[1/2] 行业结构与竞争分析...")
    skill_prompt = _load_skill("event-industry-impact")
    if skill_prompt:
        try:
            step_results["industry"] = call_claude(
                skill_prompt,
                f"请对以下行业进行深度分析（竞争格局、产业链、五力模型）：\n\n{context}",
                max_tokens=4096,
            )
        except Exception as e:
            logger.error(f"行业分析失败: {e}")
            step_results["industry"] = f"分析失败: {e}"

    # Step 2: 宏观环境对行业的影响（macro-stock-analysis）
    if progress_callback:
        progress_callback("[2/2] 宏观环境与行业关联分析...")
    skill_prompt = _load_skill("macro-stock-analysis")
    if skill_prompt:
        try:
            macro_data = get_macro_data()
            macro_ctx = _build_macro_context(industry_name, macro_data)
            step_results["macro"] = call_claude(
                skill_prompt,
                f"请分析宏观环境对{industry_name}行业的影响：\n\n{macro_ctx}\n\n行业数据：\n{context[:2000]}",
                max_tokens=3000,
            )
        except Exception as e:
            logger.error(f"宏观关联分析失败: {e}")
            step_results["macro"] = f"分析失败: {e}"

    # 综合
    if progress_callback:
        progress_callback("正在综合行业分析结果...")

    synthesis_input = f"行业: {industry_name}\n\n=== 基础数据 ===\n{context[:2000]}\n\n"
    if step_results.get("industry"):
        synthesis_input += f"=== 行业结构分析 ===\n{step_results['industry'][:3000]}\n\n"
    if step_results.get("macro"):
        synthesis_input += f"=== 宏观关联分析 ===\n{step_results['macro'][:2000]}\n\n"

    try:
        result = call_claude_json(INDUSTRY_SYNTHESIS_PROMPT, synthesis_input, max_tokens=4096)
    except Exception as e:
        logger.error(f"行业综合分析失败: {e}")
        return {"error": str(e)}

    if not isinstance(result, dict):
        return {"error": "分析结果格式异常"}

    if progress_callback:
        progress_callback("正在保存研究结果...")

    research_id = execute_insert(
        """INSERT INTO deep_research
           (research_type, target, research_date, overall_score, report_json, recommendation)
           VALUES ('industry', ?, date('now'), ?, ?, ?)""",
        [industry_name, result.get("industry_score"),
         json.dumps(result, ensure_ascii=False),
         result.get("recommendation")],
    )

    return {"research_id": research_id, **result}


# ==================== 宏观深度研究（2步分析） ====================

MACRO_SYNTHESIS_PROMPT = """你是宏观经济分析师。基于以下多维度宏观分析结果，给出综合评估。

请输出严格的JSON格式：
{
    "cycle_stage": "复苏|扩张|滞胀|衰退",
    "market_outlook": "看多|中性|看空",
    "confidence": 0-100,
    "report": {
        "cycle_analysis": "经济周期分析（300字）",
        "policy_analysis": "政策环境分析（300字）",
        "liquidity_analysis": "流动性分析（200字）",
        "external_analysis": "外部环境分析（200字）",
        "market_impact": "市场影响判断（300字）",
        "allocation_advice": "大类资产配置建议（300字）"
    },
    "sector_recommendations": ["推荐行业列表"]
}"""


def deep_research_macro(topic="整体宏观", progress_callback=None):
    """宏观深度研究 — 2步Skill驱动分析"""
    if progress_callback:
        progress_callback("正在获取宏观数据...")

    data = get_macro_data()
    context = _build_macro_context(topic, data)

    step_results = {}

    # Step 1: 宏观经济分析（macro-stock-analysis）
    if progress_callback:
        progress_callback("[1/2] 宏观经济指标分析...")
    skill_prompt = _load_skill("macro-stock-analysis")
    if skill_prompt:
        try:
            step_results["macro"] = call_claude(
                skill_prompt,
                f"请对当前宏观经济进行深度分析：\n\n{context}",
                max_tokens=4096,
            )
        except Exception as e:
            logger.error(f"宏观分析失败: {e}")
            step_results["macro"] = f"分析失败: {e}"

    # Step 2: 宏观洞察生成（macro-insight-generation）
    if progress_callback:
        progress_callback("[2/2] 宏观洞察与投资启示...")
    skill_prompt = _load_skill("macro-insight-generation")
    if skill_prompt:
        try:
            insight_ctx = context
            if step_results.get("macro"):
                insight_ctx += f"\n\n=== 宏观分析结果 ===\n{step_results['macro'][:3000]}"
            step_results["insight"] = call_claude(
                skill_prompt,
                f"请基于宏观分析生成投资洞察和配置建议：\n\n{insight_ctx}",
                max_tokens=4096,
            )
        except Exception as e:
            logger.error(f"宏观洞察生成失败: {e}")
            step_results["insight"] = f"分析失败: {e}"

    # 综合
    if progress_callback:
        progress_callback("正在综合宏观分析结果...")

    synthesis_input = f"研究主题: {topic}\n\n=== 基础数据 ===\n{context[:2000]}\n\n"
    if step_results.get("macro"):
        synthesis_input += f"=== 宏观经济分析 ===\n{step_results['macro'][:3000]}\n\n"
    if step_results.get("insight"):
        synthesis_input += f"=== 宏观洞察 ===\n{step_results['insight'][:3000]}\n\n"

    try:
        result = call_claude_json(MACRO_SYNTHESIS_PROMPT, synthesis_input, max_tokens=4096)
    except Exception as e:
        logger.error(f"宏观综合分析失败: {e}")
        return {"error": str(e)}

    if not isinstance(result, dict):
        return {"error": "分析结果格式异常"}

    if progress_callback:
        progress_callback("正在保存研究结果...")

    research_id = execute_insert(
        """INSERT INTO deep_research
           (research_type, target, research_date, overall_score, report_json, recommendation)
           VALUES ('macro', ?, date('now'), ?, ?, ?)""",
        [topic, result.get("confidence"),
         json.dumps(result, ensure_ascii=False),
         result.get("market_outlook")],
    )

    return {"research_id": research_id, **result}


# ==================== 上下文构建 ====================

def _build_stock_context(profile, tech, peers):
    """构建个股研究上下文"""
    info = profile["info"]
    parts = [f"股票: {info['stock_code']} {info['stock_name']}"]
    parts.append(f"行业: {info.get('industry_l1', '')} > {info.get('industry_l2', '')}")
    if info.get("market_cap"):
        parts.append(f"市值: {info['market_cap']}亿")

    if tech:
        parts.append("\n=== 技术面 ===")
        parts.append(f"最新价: {tech.get('latest_price')}")
        for ma in ["ma5", "ma10", "ma20", "ma60"]:
            if tech.get(ma):
                parts.append(f"  {ma.upper()}: {tech[ma]}")
        if tech.get("change_5d") is not None:
            parts.append(f"  5日涨跌: {tech['change_5d']}%")
        if tech.get("change_20d") is not None:
            parts.append(f"  20日涨跌: {tech['change_20d']}%")
        parts.append(f"  60日最高: {tech.get('high_60d')} 最低: {tech.get('low_60d')}")

    if profile["daily"]:
        parts.append("\n=== 近10日行情 ===")
        for d in profile["daily"][:10]:
            parts.append(f"  {d['trade_date']}: 开{d.get('open','')} 高{d.get('high','')} "
                         f"低{d.get('low','')} 收{d['close']} 涨跌{d.get('change_pct','')}% "
                         f"量{d.get('volume','')} 额{d.get('amount','')} "
                         f"换手{d.get('turnover_rate','')}%")

    if profile["financials"]:
        parts.append("\n=== 财务数据 ===")
        for f in profile["financials"][:4]:
            parts.append(
                f"  {f.get('report_period','')}: 营收{f.get('revenue','')} "
                f"净利{f.get('net_profit','')} "
                f"营收YoY{f.get('revenue_yoy','')}% 利润YoY{f.get('profit_yoy','')}%"
            )

    if profile["capital"]:
        parts.append("\n=== 近10日资金流向 ===")
        for c in profile["capital"][:10]:
            parts.append(f"  {c['trade_date']}: 主力净流入{c.get('main_net_inflow','')} "
                         f"超大单{c.get('super_large_net','')} 大单{c.get('large_net','')} "
                         f"中单{c.get('medium_net','')} 小单{c.get('small_net','')}")

    if peers:
        parts.append("\n=== 同行对比 ===")
        for p in peers[:8]:
            parts.append(f"  {p.get('stock_code','')} {p.get('stock_name','')}: "
                         f"市值{p.get('market_cap','')}亿 涨跌{p.get('change_pct','')}%")

    if profile["reports"]:
        parts.append("\n=== 近期研报 ===")
        for r in profile["reports"][:5]:
            parts.append(f"  {r.get('broker_name','')}: {r.get('rating','')} "
                         f"目标价{r.get('target_price','')}")

    if profile["news"]:
        parts.append("\n=== 近期新闻 ===")
        for n in profile["news"][:15]:
            parts.append(f"  [{n.get('sentiment','')}][{n.get('importance','')}⭐] "
                         f"{n.get('summary','')}")

    return "\n".join(parts)


def _build_industry_context(industry_name, data):
    """构建行业研究上下文"""
    parts = [f"行业: {industry_name}"]

    if data.get("flows"):
        parts.append("\n=== 行业资金流向 ===")
        for f in data["flows"][:10]:
            parts.append(f"  {f.get('trade_date','')}: 净流入{f.get('net_inflow','')} "
                         f"涨跌{f.get('change_pct','')}% 领涨{f.get('leading_stock','')}")

    if data.get("stocks"):
        parts.append("\n=== 行业内主要个股 ===")
        for s in data["stocks"][:15]:
            parts.append(f"  {s.get('stock_code','')} {s.get('stock_name','')}: "
                         f"市值{s.get('market_cap','')}亿")

    if data.get("news"):
        parts.append("\n=== 行业新闻 ===")
        for n in data["news"][:15]:
            parts.append(f"  [{n.get('sentiment','')}] {n.get('summary','')}")

    return "\n".join(parts)


def _build_macro_context(topic, data):
    """构建宏观研究上下文"""
    parts = [f"研究主题: {topic}"]

    if data.get("indicators"):
        parts.append("\n=== 宏观指标 ===")
        for ind in data["indicators"][:20]:
            parts.append(f"  {ind.get('indicator_name','')}: "
                         f"{ind.get('indicator_value','')} ({ind.get('indicator_date','')})")

    if data.get("northbound"):
        parts.append("\n=== 北向资金 ===")
        for nb in data["northbound"][:10]:
            parts.append(f"  {nb.get('trade_date','')}: 净流入{nb.get('net_buy','')}")

    if data.get("news"):
        parts.append("\n=== 宏观新闻 ===")
        for n in data["news"][:15]:
            parts.append(f"  [{n.get('sentiment','')}] {n.get('summary','')}")

    return "\n".join(parts)


# ==================== 投资机会生成 ====================

def _create_opportunity(stock_code, info, result, research_id):
    """从研究结果自动生成投资机会"""
    score = result.get("overall_score", 0)
    rec = result.get("recommendation", "")
    report = result.get("report", {})

    rating = "A" if score >= 85 else "B" if score >= 70 else "C"
    summary = report.get("executive_summary", report.get("conclusion", ""))[:200]

    execute_insert(
        """INSERT INTO investment_opportunities
           (stock_code, stock_name, opportunity_type, rating, summary,
            source, tags_json, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'active')""",
        [stock_code, info.get("stock_name", ""),
         f"深度研究-{rec}", rating, summary,
         f"deep_research#{research_id}",
         json.dumps({"recommendation": rec, "score": score}, ensure_ascii=False)],
    )

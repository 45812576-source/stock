"""投资主题识别模块 — 纯数据驱动，综合5类信号识别投资主题

信号类型（按硬度排序）：
1. 数据信号：financial_reports 收入/利润加速, macro_indicators 拐点, stock_daily 价格异动
2. 政策信号：cleaned_items 政策类新闻, kg_entities 政策实体
3. 产业信号：cleaned_items 产业新闻, research_reports 龙头动向
4. 资金信号：northbound_flow, industry_capital_flow, capital_flow 板块聚合
5. 叙事信号：dashboard_tag_frequency 标签热度, research_reports 研报密度
"""
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta

from utils.db_utils import execute_query
from knowledge_graph.kg_manager import add_entity, add_relationship, find_entity

logger = logging.getLogger(__name__)

# 信号类型权重（硬信号权重高）
SIGNAL_WEIGHTS = {
    "data": 0.30,
    "policy": 0.25,
    "industry": 0.20,
    "capital": 0.15,
    "narrative": 0.10,
}


def identify_themes(days=7, confidence_threshold=0.6, progress_callback=None):
    """主入口：识别投资主题

    Args:
        days: 回看天数
        confidence_threshold: 置信度门槛，低于此值不创建
        progress_callback: fn(current, total, msg)

    Returns:
        dict with themes list and stats
    """
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    if progress_callback:
        progress_callback(1, 6, "采集数据信号...")
    data_signals = _gather_data_signals(since)

    if progress_callback:
        progress_callback(2, 6, "采集政策信号...")
    policy_signals = _gather_policy_signals(since)

    if progress_callback:
        progress_callback(3, 6, "采集产业信号...")
    industry_signals = _gather_industry_signals(since)

    if progress_callback:
        progress_callback(4, 6, "采集资金信号...")
    capital_signals = _gather_capital_signals(since)

    if progress_callback:
        progress_callback(5, 6, "采集叙事信号...")
    narrative_signals = _gather_narrative_signals(since)

    if progress_callback:
        progress_callback(6, 6, "综合评分，识别主题...")

    # 汇总：以行业名为 key 聚合各类信号
    industry_scores = defaultdict(lambda: {
        "data": 0.0, "policy": 0.0, "industry": 0.0,
        "capital": 0.0, "narrative": 0.0,
        "signal_types": set(), "companies": set(),
        "evidences": [],
    })

    _merge_signals(industry_scores, data_signals, "data")
    _merge_signals(industry_scores, policy_signals, "policy")
    _merge_signals(industry_scores, industry_signals, "industry")
    _merge_signals(industry_scores, capital_signals, "capital")
    _merge_signals(industry_scores, narrative_signals, "narrative")

    # 计算加权置信度
    themes = []
    for name, info in industry_scores.items():
        if not name or len(name) < 2:
            continue
        confidence = sum(
            info[st] * SIGNAL_WEIGHTS[st] for st in SIGNAL_WEIGHTS
        )
        confidence = round(min(confidence, 1.0), 3)
        if confidence < confidence_threshold:
            continue
        themes.append({
            "theme_name": name,
            "confidence": confidence,
            "signal_types": sorted(info["signal_types"]),
            "related_companies": sorted(info["companies"])[:10],
            "evidences": info["evidences"][:5],
        })

    themes.sort(key=lambda t: t["confidence"], reverse=True)
    themes = themes[:30]  # 最多30个主题

    # 写入 KG
    added_entities = 0
    added_rels = 0
    for t in themes:
        desc = f"置信度{t['confidence']:.0%}，信号来源：{'、'.join(t['signal_types'])}"
        if t["evidences"]:
            desc += f"。{t['evidences'][0]}"

        eid = add_entity(
            "theme", t["theme_name"],
            description=desc,
            properties={
                "confidence": t["confidence"],
                "signal_types": t["signal_types"],
                "identified_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            },
        )
        if eid:
            added_entities += 1
            t["entity_id"] = eid

        # 关联公司
        for comp_name in t["related_companies"]:
            found = find_entity(comp_name)
            if found and eid:
                rid = add_relationship(
                    found[0]["id"], eid, "related",
                    relation_category="structural",
                    strength=t["confidence"] * 0.8,
                    direction="positive",
                    evidence=f"主题识别：{comp_name} 关联 {t['theme_name']}",
                    confidence=t["confidence"],
                )
                if rid:
                    added_rels += 1

        # 关联行业（如果行业实体存在）
        ind_found = find_entity(t["theme_name"])
        if ind_found:
            for ent in ind_found:
                if ent.get("entity_type") == "industry" and eid:
                    rid = add_relationship(
                        ent["id"], eid, "related",
                        relation_category="structural",
                        strength=t["confidence"],
                        direction="positive",
                        evidence=f"行业 {t['theme_name']} 形成投资主题",
                    )
                    if rid:
                        added_rels += 1

    logger.info(
        f"主题识别完成: 发现{len(themes)}个主题, "
        f"新增实体{added_entities}, 新增关系{added_rels}"
    )
    return {
        "themes": themes,
        "added_entities": added_entities,
        "added_relationships": added_rels,
    }


def _merge_signals(scores, signals, signal_type):
    """将某类信号合并到行业评分表"""
    for s in signals:
        name = s.get("industry") or s.get("name", "")
        if not name:
            continue
        entry = scores[name]
        entry[signal_type] = max(entry[signal_type], s.get("score", 0.0))
        entry["signal_types"].add(signal_type)
        if s.get("companies"):
            entry["companies"].update(s["companies"])
        if s.get("evidence"):
            entry["evidences"].append(s["evidence"])


# ==================== 5 类信号采集 ====================


def _gather_data_signals(since):
    """数据信号：财报加速 + 宏观拐点 + 价格异动"""
    signals = []

    # 1) 同行业多公司收入/利润加速
    rows = execute_query("""
        SELECT ii.industry_name,
               COUNT(DISTINCT fr.stock_code) as cnt,
               AVG(fr.revenue_yoy) as avg_rev_yoy,
               AVG(fr.profit_yoy) as avg_profit_yoy,
               GROUP_CONCAT(DISTINCT ic.stock_name SEPARATOR ',') as companies
        FROM financial_reports fr
        JOIN item_companies ic ON fr.stock_code = ic.stock_code
        JOIN item_industries ii ON ic.cleaned_item_id = ii.cleaned_item_id
        WHERE fr.report_date >= ?
        AND (fr.revenue_yoy > 20 OR fr.profit_yoy > 30)
        GROUP BY ii.industry_name
        HAVING cnt >= 2
        ORDER BY avg_profit_yoy DESC
        LIMIT 20
    """, [since]) or []

    for r in rows:
        rd = dict(r)
        score = min(1.0, (rd.get("cnt", 0) / 5) + (abs(rd.get("avg_profit_yoy", 0)) / 100))
        comps = (rd.get("companies") or "").split(",")[:5]
        signals.append({
            "industry": rd["industry_name"],
            "score": round(score, 3),
            "companies": set(c for c in comps if c),
            "evidence": f"{rd['industry_name']}有{rd['cnt']}家公司业绩加速，"
                        f"平均收入增速{rd.get('avg_rev_yoy', 0):.1f}%",
        })

    # 2) 价格异动：行业内多只股票涨幅显著
    rows = execute_query("""
        SELECT ii.industry_name,
               COUNT(DISTINCT sd.stock_code) as cnt,
               AVG(sd.change_pct) as avg_chg,
               GROUP_CONCAT(DISTINCT ic.stock_name SEPARATOR ',') as companies
        FROM stock_daily sd
        JOIN item_companies ic ON sd.stock_code = ic.stock_code
        JOIN item_industries ii ON ic.cleaned_item_id = ii.cleaned_item_id
        WHERE sd.trade_date >= ?
        AND sd.change_pct > 5
        GROUP BY ii.industry_name
        HAVING cnt >= 3
        ORDER BY avg_chg DESC
        LIMIT 15
    """, [since]) or []

    for r in rows:
        rd = dict(r)
        score = min(1.0, rd.get("cnt", 0) / 8 + rd.get("avg_chg", 0) / 20)
        comps = (rd.get("companies") or "").split(",")[:5]
        signals.append({
            "industry": rd["industry_name"],
            "score": round(score, 3),
            "companies": set(c for c in comps if c),
            "evidence": f"{rd['industry_name']}{rd['cnt']}只股票涨幅超5%，"
                        f"平均涨幅{rd.get('avg_chg', 0):.1f}%",
        })

    return signals


def _gather_policy_signals(since):
    """政策信号：政策类新闻 + KG 政策实体"""
    signals = []

    # 政策类新闻按行业聚合
    rows = execute_query("""
        SELECT ii.industry_name,
               COUNT(*) as cnt,
               AVG(ci.importance) as avg_imp,
               GROUP_CONCAT(DISTINCT ic.stock_name SEPARATOR ',') as companies
        FROM cleaned_items ci
        JOIN item_industries ii ON ci.id = ii.cleaned_item_id
        LEFT JOIN item_companies ic ON ci.id = ic.cleaned_item_id
        WHERE ci.event_type IN ('macro', 'macro_policy')
        AND ci.cleaned_at >= ?
        GROUP BY ii.industry_name
        HAVING cnt >= 1
        ORDER BY cnt DESC
        LIMIT 20
    """, [since]) or []

    for r in rows:
        rd = dict(r)
        score = min(1.0, rd.get("cnt", 0) / 5 + (rd.get("avg_imp", 3) - 3) * 0.2)
        comps = (rd.get("companies") or "").split(",")[:5]
        signals.append({
            "industry": rd["industry_name"],
            "score": round(max(score, 0.1), 3),
            "companies": set(c for c in comps if c),
            "evidence": f"{rd['industry_name']}有{rd['cnt']}条政策相关新闻",
        })

    # KG 中已有的 policy 实体关联的行业
    rows = execute_query("""
        SELECT e2.entity_name as industry_name,
               COUNT(*) as policy_cnt,
               GROUP_CONCAT(DISTINCT e1.entity_name SEPARATOR '、') as policies
        FROM kg_relationships r
        JOIN kg_entities e1 ON r.source_entity_id = e1.id AND e1.entity_type = 'policy'
        JOIN kg_entities e2 ON r.target_entity_id = e2.id AND e2.entity_type = 'industry'
        GROUP BY e2.entity_name
        HAVING policy_cnt >= 1
        ORDER BY policy_cnt DESC
        LIMIT 15
    """) or []

    for r in rows:
        rd = dict(r)
        score = min(1.0, rd.get("policy_cnt", 0) / 3)
        signals.append({
            "industry": rd["industry_name"],
            "score": round(score, 3),
            "companies": set(),
            "evidence": f"{rd['industry_name']}关联政策：{rd.get('policies', '')[:60]}",
        })

    return signals


def _gather_industry_signals(since):
    """产业信号：产业新闻密度 + 研报龙头动向"""
    signals = []

    # 产业新闻密度
    rows = execute_query("""
        SELECT ii.industry_name,
               COUNT(*) as cnt,
               AVG(ci.importance) as avg_imp,
               GROUP_CONCAT(DISTINCT ic.stock_name SEPARATOR ',') as companies
        FROM cleaned_items ci
        JOIN item_industries ii ON ci.id = ii.cleaned_item_id
        LEFT JOIN item_companies ic ON ci.id = ic.cleaned_item_id
        WHERE ci.event_type = 'industry_news'
        AND ci.cleaned_at >= ?
        GROUP BY ii.industry_name
        HAVING cnt >= 2
        ORDER BY cnt DESC
        LIMIT 20
    """, [since]) or []

    for r in rows:
        rd = dict(r)
        score = min(1.0, rd.get("cnt", 0) / 8 + (rd.get("avg_imp", 3) - 3) * 0.15)
        comps = (rd.get("companies") or "").split(",")[:5]
        signals.append({
            "industry": rd["industry_name"],
            "score": round(max(score, 0.1), 3),
            "companies": set(c for c in comps if c),
            "evidence": f"{rd['industry_name']}{rd['cnt']}条产业新闻",
        })

    # 研报覆盖的行业（龙头动向）
    rows = execute_query("""
        SELECT ii.industry_name,
               COUNT(DISTINCT rr.id) as report_cnt,
               GROUP_CONCAT(DISTINCT rr.stock_name SEPARATOR ',') as companies
        FROM research_reports rr
        JOIN item_companies ic ON rr.stock_code = ic.stock_code
        JOIN item_industries ii ON ic.cleaned_item_id = ii.cleaned_item_id
        WHERE rr.report_date >= ?
        GROUP BY ii.industry_name
        HAVING report_cnt >= 2
        ORDER BY report_cnt DESC
        LIMIT 15
    """, [since]) or []

    for r in rows:
        rd = dict(r)
        score = min(1.0, rd.get("report_cnt", 0) / 6)
        comps = (rd.get("companies") or "").split(",")[:5]
        signals.append({
            "industry": rd["industry_name"],
            "score": round(score, 3),
            "companies": set(c for c in comps if c),
            "evidence": f"{rd['industry_name']}{rd['report_cnt']}篇研报覆盖",
        })

    return signals


def _gather_capital_signals(since):
    """资金信号：行业资金流入 + 个股主力净流入聚合"""
    signals = []

    # 行业资金净流入
    rows = execute_query("""
        SELECT industry_name,
               SUM(net_inflow) as total_inflow,
               AVG(change_pct) as avg_chg,
               GROUP_CONCAT(DISTINCT leading_stock SEPARATOR ',') as companies
        FROM industry_capital_flow
        WHERE trade_date >= ?
        AND net_inflow > 0
        GROUP BY industry_name
        HAVING total_inflow > 0
        ORDER BY total_inflow DESC
        LIMIT 20
    """, [since]) or []

    for r in rows:
        rd = dict(r)
        inflow = rd.get("total_inflow", 0)
        # 归一化：假设 10 亿为满分
        score = min(1.0, abs(inflow) / 1e9) if inflow else 0
        comps = (rd.get("companies") or "").split(",")[:5]
        signals.append({
            "industry": rd["industry_name"],
            "score": round(max(score, 0.1), 3),
            "companies": set(c for c in comps if c),
            "evidence": f"{rd['industry_name']}资金净流入{inflow/1e8:.1f}亿",
        })

    # 个股主力净流入按行业聚合
    rows = execute_query("""
        SELECT ii.industry_name,
               SUM(cf.main_net_inflow) as total_main,
               COUNT(DISTINCT cf.stock_code) as stock_cnt,
               GROUP_CONCAT(DISTINCT ic.stock_name SEPARATOR ',') as companies
        FROM capital_flow cf
        JOIN item_companies ic ON cf.stock_code = ic.stock_code
        JOIN item_industries ii ON ic.cleaned_item_id = ii.cleaned_item_id
        WHERE cf.trade_date >= ?
        AND cf.main_net_inflow > 0
        GROUP BY ii.industry_name
        HAVING stock_cnt >= 2
        ORDER BY total_main DESC
        LIMIT 15
    """, [since]) or []

    for r in rows:
        rd = dict(r)
        total = rd.get("total_main", 0)
        score = min(1.0, abs(total) / 5e8) if total else 0
        comps = (rd.get("companies") or "").split(",")[:5]
        signals.append({
            "industry": rd["industry_name"],
            "score": round(max(score, 0.1), 3),
            "companies": set(c for c in comps if c),
            "evidence": f"{rd['industry_name']}{rd['stock_cnt']}只股票主力净流入",
        })

    return signals


def _gather_narrative_signals(since):
    """叙事信号：标签热度 + 研报密度"""
    signals = []

    # 标签热度（dashboard_tag_frequency）
    rows = execute_query("""
        SELECT tag_name,
               COUNT(*) as appear_cnt,
               MIN(rank_position) as best_rank
        FROM dashboard_tag_frequency
        WHERE appear_date >= ?
        GROUP BY tag_name
        HAVING appear_cnt >= 2
        ORDER BY appear_cnt DESC
        LIMIT 20
    """, [since]) or []

    for r in rows:
        rd = dict(r)
        cnt = rd.get("appear_cnt", 0)
        rank = rd.get("best_rank", 10) or 10
        score = min(1.0, cnt / 7 + (10 - min(rank, 10)) / 20)
        signals.append({
            "industry": rd["tag_name"],
            "score": round(max(score, 0.1), 3),
            "companies": set(),
            "evidence": f"标签「{rd['tag_name']}」出现{cnt}次，最高排名第{rank}",
        })

    # 研报密度（按行业）
    rows = execute_query("""
        SELECT ii.industry_name,
               COUNT(DISTINCT rr.id) as cnt
        FROM research_reports rr
        JOIN item_companies ic ON rr.stock_code = ic.stock_code
        JOIN item_industries ii ON ic.cleaned_item_id = ii.cleaned_item_id
        WHERE rr.report_date >= ?
        GROUP BY ii.industry_name
        HAVING cnt >= 3
        ORDER BY cnt DESC
        LIMIT 15
    """, [since]) or []

    for r in rows:
        rd = dict(r)
        score = min(1.0, rd.get("cnt", 0) / 10)
        signals.append({
            "industry": rd["industry_name"],
            "score": round(score, 3),
            "companies": set(),
            "evidence": f"{rd['industry_name']}近期{rd['cnt']}篇研报，关注度高",
        })

    return signals

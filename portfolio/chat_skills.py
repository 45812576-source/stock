"""选股机器人 Skill/SubSkill 架构

Intent 路由 → SubSkill → 取数 + 推荐逻辑

SubSkill 清单：
  capital_flow   资金流向分析  — 主力资金异动、行业轮动信号
  valuation      估值筛选      — PE/PB/市值/行业横向比价
  kg_theme       KG主题关联    — 从问题词关联到知识图谱主题/行业
  basket_diag    篮子诊断      — 对当前持仓/篮子的组合健康诊断
  lookalike      Lookalike推荐 — 找与篮子股票相似的候选标的
  general        兜底通用      — 以上都不匹配时，用完整上下文回答
"""

import logging
from utils.db_utils import execute_query

logger = logging.getLogger(__name__)


# ── Intent 路由 ──────────────────────────────────────────────────

INTENT_KEYWORDS = {
    "capital_flow": [
        "资金", "主力", "净流入", "净流出", "北向", "大单", "聪明钱",
        "抄底", "出逃", "流入", "流出", "资金面", "筹码",
    ],
    "valuation": [
        "估值", "PE", "PB", "市盈率", "市净率", "低估", "便宜", "贵",
        "性价比", "ps", "市值", "EV", "折价", "溢价", "历史分位",
    ],
    "kg_theme": [
        "主题", "赛道", "行业", "板块", "概念", "AI", "新能源", "半导体",
        "消费", "医药", "军工", "关联", "相关股", "产业链", "上下游",
    ],
    "basket_diag": [
        "诊断", "健康", "组合", "持仓", "篮子", "风险", "集中度",
        "分散", "回撤", "相关性", "持有", "我的", "组合风险",
    ],
    "lookalike": [
        "类似", "相似", "lookalike", "找同类", "备选", "替代",
        "同赛道", "同行业", "推荐", "还有哪些", "其他", "除了",
    ],
    "stock_selector": [
        "选股", "筛选", "找股票", "哪些股票", "哪只股票", "月线", "周线",
        "连阳", "均线多头", "放量突破", "回踩", "箱体", "MACD",
        "受益", "受损", "政策", "两会", "报告", "最受益",
    ],
}


def route_intent(message: str) -> str:
    """根据问题关键词路由到 SubSkill，返回 skill 名称"""
    msg_lower = message.lower()
    scores = {intent: 0 for intent in INTENT_KEYWORDS}
    for intent, keywords in INTENT_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in msg_lower:
                scores[intent] += 1
    # stock_selector 优先：只要命中1个技术面/选股词就优先
    if scores["stock_selector"] >= 1:
        return "stock_selector"
    best = max(scores, key=lambda k: scores[k])
    if scores[best] == 0:
        return "general"
    return best


# ── SubSkill: 资金流向 ────────────────────────────────────────────

def skill_capital_flow(message: str, basket_codes: list, call_model_fn) -> dict:
    """主力资金异动分析"""
    # 取数：近7日 + 近30日资金流，按股票
    codes_ph = ",".join(["%s"] * len(basket_codes)) if basket_codes else "''"

    cf_rows = []
    if basket_codes:
        cf_rows = execute_query(
            f"""SELECT cf.stock_code, si.stock_name,
                       SUM(CASE WHEN cf.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                                THEN cf.main_net_inflow ELSE 0 END) / 10000 AS net_7d_wan,
                       SUM(CASE WHEN cf.trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                                THEN cf.main_net_inflow ELSE 0 END) / 10000 AS net_30d_wan
                FROM capital_flow cf
                LEFT JOIN stock_info si ON cf.stock_code = si.stock_code
                WHERE cf.stock_code IN ({codes_ph})
                GROUP BY cf.stock_code, si.stock_name
                ORDER BY net_7d_wan DESC""",
            basket_codes,
        ) or []

    # 全市场资金流异动 Top5（非篮子内）
    top_inflow = execute_query(
        """SELECT cf.stock_code, si.stock_name, si.industry_l1,
                  SUM(cf.main_net_inflow)/10000 AS net_wan
           FROM capital_flow cf
           LEFT JOIN stock_info si ON cf.stock_code = si.stock_code
           WHERE cf.trade_date >= DATE_SUB(CURDATE(), INTERVAL 5 DAY)
             AND LENGTH(cf.stock_code) = 6
           GROUP BY cf.stock_code, si.stock_name, si.industry_l1
           ORDER BY net_wan DESC LIMIT 5""",
        [],
    ) or []

    # 组装 context
    lines = ["## 资金流数据"]
    if cf_rows:
        lines.append("### 篮子股票近期资金流")
        for r in cf_rows:
            n7 = float(r["net_7d_wan"] or 0)
            n30 = float(r["net_30d_wan"] or 0)
            sign7 = "▲" if n7 > 0 else "▼"
            lines.append(f"- {r['stock_code']} {r.get('stock_name','')}: 7日{sign7}{abs(n7):.0f}万, 30日{abs(n30):.0f}万")

    if top_inflow:
        lines.append("### 近5日全市场主力净流入Top5")
        for r in top_inflow:
            lines.append(f"- {r['stock_code']} {r.get('stock_name','')}({r.get('industry_l1','')}): {float(r['net_wan'] or 0):.0f}万")

    data_ctx = "\n".join(lines)

    system = f"""你是A股资金面分析专家。基于以下资金流数据，回答用户关于资金动向的问题。

{data_ctx}

## 回复规范
- 直接给结论，不废话
- 重点标注资金异动显著的个股（7日净流入绝对值大）
- 如推荐个股，用 ```推荐股票\\n[{{"stock_code":"","stock_name":"","reason":""}}]``` 格式
- 禁止编造不在数据中的数字"""

    reply = call_model_fn("chat", system, f"用户问：{message}", max_tokens=2048)
    return _parse_reply(reply)


# ── SubSkill: 估值筛选 ────────────────────────────────────────────

def skill_valuation(message: str, basket_codes: list, call_model_fn) -> dict:
    """估值横向比价"""
    # 篮子股票估值
    codes_ph = ",".join(["%s"] * len(basket_codes)) if basket_codes else "''"

    val_rows = []
    if basket_codes:
        val_rows = execute_query(
            f"""SELECT si.stock_code, si.stock_name, si.industry_l1,
                       si.market_cap / 1e8 AS cap_yi,
                       sd.close,
                       sd.change_pct
                FROM stock_info si
                LEFT JOIN (
                    SELECT sd1.stock_code, sd1.close, sd1.change_pct
                    FROM stock_daily sd1
                    JOIN (SELECT stock_code, MAX(trade_date) AS mx FROM stock_daily
                          WHERE stock_code IN ({codes_ph}) GROUP BY stock_code) t
                      ON sd1.stock_code=t.stock_code AND sd1.trade_date=t.mx
                ) sd ON si.stock_code = sd.stock_code
                WHERE si.stock_code IN ({codes_ph})""",
            basket_codes + basket_codes,
        ) or []

    # deep_research 评分（有就取）
    dr_rows = []
    if basket_codes:
        dr_rows = execute_query(
            f"""SELECT target, valuation_score, overall_score, recommendation
                FROM deep_research
                WHERE target IN ({codes_ph})
                ORDER BY research_date DESC""",
            basket_codes,
        ) or []
    dr_map = {r["target"]: r for r in dr_rows}

    lines = ["## 估值数据"]
    for r in val_rows:
        code = r["stock_code"]
        cap = float(r["cap_yi"] or 0)
        dr = dr_map.get(code, {})
        val_score = dr.get("valuation_score")
        parts = [f"- {code} {r.get('stock_name','')}({r.get('industry_l1','')})"]
        parts.append(f"市值:{cap:.0f}亿")
        if val_score:
            parts.append(f"估值评分:{val_score:.0f}/100")
        if dr.get("recommendation"):
            parts.append(f"建议:{dr['recommendation']}")
        lines.append(" | ".join(parts))

    data_ctx = "\n".join(lines)

    system = f"""你是A股估值分析专家。基于以下估值数据，回答用户的比价/低估筛选问题。

{data_ctx}

## 回复规范
- 直接给出估值判断（低估/合理/高估）及理由
- 如需在篮子外推荐低估个股，用 ```推荐股票\\n[{{"stock_code":"","stock_name":"","reason":""}}]``` 格式
- 禁止编造PE/PB具体数字（数据库无此字段时说明数据不足）"""

    reply = call_model_fn("chat", system, f"用户问：{message}", max_tokens=2048)
    return _parse_reply(reply)


# ── SubSkill: KG主题关联 ──────────────────────────────────────────

def skill_kg_theme(message: str, basket_codes: list, call_model_fn) -> dict:
    """从问题提取关键词，关联KG主题/行业，找相关股票"""
    # 从消息里提取可能的主题词（取前3个有意义词）
    import re
    # 简单分词：中文4字以内词块
    keywords = re.findall(r'[\u4e00-\u9fa5A-Za-z0-9]{2,8}', message)[:6]

    # KG关联查询
    kg_results = []
    for kw in keywords:
        rows = execute_query(
            """SELECT ke.entity_name, ke.entity_type, ke.investment_logic
               FROM kg_entities ke
               WHERE ke.entity_name LIKE %s
                 AND ke.entity_type IN ('industry', 'theme', 'event')
               LIMIT 5""",
            [f"%{kw[:4]}%"],
        ) or []
        kg_results.extend(rows)

    # 对匹配到的 KG industry/theme 找关联股票
    related_stocks = []
    seen_entities = set()
    for entity in kg_results[:5]:
        entity_name = entity["entity_name"]
        if entity_name in seen_entities:
            continue
        seen_entities.add(entity_name)
        # 找该实体下的公司
        stock_rows = execute_query(
            """SELECT ke.external_id AS code, ke.entity_name AS name
               FROM kg_relationships kr
               JOIN kg_entities ke ON kr.source_entity_id = ke.id
               JOIN kg_entities target ON kr.target_entity_id = target.id
               WHERE target.entity_name LIKE %s
                 AND ke.entity_type = 'company'
                 AND ke.external_id IS NOT NULL
               LIMIT 10""",
            [f"%{entity_name[:4]}%"],
        ) or []
        related_stocks.extend([dict(r) for r in stock_rows])

    # 去重 + 补股票名
    seen_codes = set()
    unique_stocks = []
    for s in related_stocks:
        if s["code"] and s["code"] not in seen_codes and len(s["code"]) == 6:
            seen_codes.add(s["code"])
            unique_stocks.append(s)

    lines = ["## KG主题关联结果"]
    if kg_results:
        lines.append(f"匹配到 {len(kg_results)} 个KG实体: " +
                     ", ".join(set(r["entity_name"] for r in kg_results)))
    if unique_stocks:
        lines.append(f"关联股票({len(unique_stocks)}只): " +
                     ", ".join(f"{s['code']}" for s in unique_stocks[:15]))
        # 这些股票的资金流（快速判断热度）
        codes = [s["code"] for s in unique_stocks[:15]]
        if codes:
            codes_ph = ",".join(["%s"] * len(codes))
            cf_quick = execute_query(
                f"""SELECT stock_code, SUM(main_net_inflow)/10000 AS net_wan
                    FROM capital_flow
                    WHERE stock_code IN ({codes_ph})
                      AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 5 DAY)
                    GROUP BY stock_code ORDER BY net_wan DESC""",
                codes,
            ) or []
            if cf_quick:
                cf_str = ", ".join([f"{r['stock_code']}:{float(r['net_wan'] or 0):.0f}万"
                                    for r in cf_quick[:10]])
                lines.append(f"近5日资金流: {cf_str}")
    else:
        lines.append("未找到关联股票，请换关键词")

    data_ctx = "\n".join(lines)

    system = f"""你是A股主题/行业关联分析专家。根据知识图谱检索结果回答用户问题。

{data_ctx}

## 回复规范
- 先说命中了哪些主题/行业
- 从关联股票中挑选最具代表性的（结合资金流热度），用 ```推荐股票\\n[{{"stock_code":"","stock_name":"","reason":""}}]``` 格式
- 最多推荐5只，只推荐A股（6位数字代码）"""

    reply = call_model_fn("chat", system, f"用户问：{message}", max_tokens=2048)
    return _parse_reply(reply)


# ── SubSkill: 篮子诊断 ────────────────────────────────────────────

def skill_basket_diag(message: str, basket_stocks: list, call_model_fn) -> dict:
    """对当前篮子/持仓做组合健康诊断"""
    if not basket_stocks:
        return {"reply": "篮子为空，无法诊断。请先添加股票。", "recommendations": []}

    codes = [s["stock_code"] for s in basket_stocks]
    codes_ph = ",".join(["%s"] * len(codes))

    # 行业集中度
    industry_count: dict = {}
    for s in basket_stocks:
        ind = s.get("industry_l1") or "未知"
        industry_count[ind] = industry_count.get(ind, 0) + 1

    # 近30日各股涨跌幅
    perf_rows = execute_query(
        f"""SELECT a.stock_code, si.stock_name,
                   ROUND((a.close / NULLIF(b.close, 0) - 1) * 100, 2) AS chg30
            FROM stock_daily a
            JOIN (SELECT stock_code, MAX(trade_date) AS mx FROM stock_daily
                  WHERE stock_code IN ({codes_ph}) GROUP BY stock_code) la
              ON a.stock_code=la.stock_code AND a.trade_date=la.mx
            JOIN stock_info si ON a.stock_code=si.stock_code
            JOIN (SELECT stock_code, close, trade_date FROM stock_daily) b
              ON b.stock_code=a.stock_code
            JOIN (SELECT stock_code, MAX(trade_date) AS mx30
                  FROM stock_daily WHERE stock_code IN ({codes_ph})
                    AND trade_date <= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                  GROUP BY stock_code) lb
              ON b.stock_code=lb.stock_code AND b.trade_date=lb.mx30""",
        codes * 3,
    ) or []

    # 5日资金流
    cf_rows = execute_query(
        f"""SELECT stock_code, SUM(main_net_inflow)/10000 AS net_wan
            FROM capital_flow WHERE stock_code IN ({codes_ph})
              AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 5 DAY)
            GROUP BY stock_code""",
        codes,
    ) or []
    cf_map = {r["stock_code"]: float(r["net_wan"] or 0) for r in cf_rows}

    lines = ["## 篮子诊断数据"]
    lines.append(f"股票数: {len(basket_stocks)}")
    lines.append("行业分布: " + ", ".join(f"{k}×{v}" for k, v in sorted(industry_count.items(), key=lambda x: -x[1])))
    lines.append("近30日表现:")
    for r in perf_rows:
        chg = float(r["chg30"] or 0)
        cf = cf_map.get(r["stock_code"], 0)
        lines.append(f"  {r['stock_code']} {r.get('stock_name','')}: 涨跌{chg:+.1f}%, 5日资金{cf:+.0f}万")

    data_ctx = "\n".join(lines)

    system = f"""你是A股组合诊断专家。根据以下组合数据，诊断组合健康状况并给出具体建议。

{data_ctx}

## 诊断维度（按需展开）
1. 行业集中度风险（单一行业超40%需预警）
2. 强弱分化（近30日涨跌差异大的找原因）
3. 资金背离（涨了但资金流出 / 跌了但资金流入 的股票）
4. 建议操作（减仓/加仓/替换具体哪只，要有理由）

## 回复规范
- 先给整体健康度（高/中/低风险）
- 具体问题直接说，不废话"""

    reply = call_model_fn("chat", system, f"用户问：{message}", max_tokens=2048)
    return _parse_reply(reply)


# ── SubSkill: Lookalike推荐 ───────────────────────────────────────

def skill_lookalike(message: str, basket_stocks: list, call_model_fn) -> dict:
    """找与篮子股票相似的候选标的"""
    if not basket_stocks:
        return {"reply": "篮子为空，无法找Lookalike。请先添加参考股票。", "recommendations": []}

    codes = [s["stock_code"] for s in basket_stocks]
    codes_ph = ",".join(["%s"] * len(codes))

    # 找篮子股票的行业 + KG关联主题
    industries = list(set(s.get("industry_l1") for s in basket_stocks if s.get("industry_l1")))
    ind_ph = ",".join(["%s"] * len(industries)) if industries else "''"

    # 同行业内未在篮子中的股票，按资金流排序
    candidates = []
    if industries:
        candidate_rows = execute_query(
            f"""SELECT si.stock_code, si.stock_name, si.industry_l1,
                       si.market_cap / 1e8 AS cap_yi,
                       COALESCE(cf.net_wan, 0) AS net_wan
                FROM stock_info si
                LEFT JOIN (
                    SELECT stock_code, SUM(main_net_inflow)/10000 AS net_wan
                    FROM capital_flow
                    WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 5 DAY)
                    GROUP BY stock_code
                ) cf ON si.stock_code = cf.stock_code
                WHERE si.industry_l1 IN ({ind_ph})
                  AND si.stock_code NOT IN ({codes_ph})
                  AND LENGTH(si.stock_code) = 6
                ORDER BY net_wan DESC
                LIMIT 20""",
            industries + codes,
        ) or []
        candidates = [dict(r) for r in candidate_rows]

    # KG关联：找与篮子有共同主题的股票
    kg_candidates = []
    kg_rows = execute_query(
        f"""SELECT DISTINCT ke2.external_id AS code
            FROM kg_entities ke1
            JOIN kg_relationships kr ON ke1.id = kr.source_entity_id OR ke1.id = kr.target_entity_id
            JOIN kg_entities ke2 ON (kr.source_entity_id = ke2.id OR kr.target_entity_id = ke2.id)
            WHERE ke1.external_id IN ({codes_ph})
              AND ke2.entity_type = 'company'
              AND ke2.external_id NOT IN ({codes_ph})
              AND ke2.external_id IS NOT NULL
              AND LENGTH(ke2.external_id) = 6
            LIMIT 20""",
        codes + codes,
    ) or []
    kg_codes = [r["code"] for r in kg_rows if r["code"]]

    if kg_codes:
        kgc_ph = ",".join(["%s"] * len(kg_codes))
        kgc_rows = execute_query(
            f"""SELECT si.stock_code, si.stock_name, si.industry_l1
                FROM stock_info si WHERE si.stock_code IN ({kgc_ph})""",
            kg_codes,
        ) or []
        kg_candidates = [dict(r) for r in kgc_rows]

    lines = ["## Lookalike候选"]
    if candidates:
        lines.append(f"同行业候选(按近5日资金流排序):")
        for c in candidates[:10]:
            lines.append(f"  {c['stock_code']} {c.get('stock_name','')}({c.get('industry_l1','')}) "
                         f"市值:{float(c.get('cap_yi') or 0):.0f}亿 5日资金:{float(c.get('net_wan') or 0):+.0f}万")
    if kg_candidates:
        codes_str = ", ".join(f"{c['stock_code']}({c.get('stock_name','')})" for c in kg_candidates[:10])
        lines.append(f"KG关联同主题候选: {codes_str}")

    ref_str = ", ".join(f"{s['stock_code']} {s.get('stock_name','')}" for s in basket_stocks[:5])
    lines.insert(0, f"参考股票: {ref_str}")

    data_ctx = "\n".join(lines)

    system = f"""你是A股选股专家，擅长寻找相似标的。根据以下候选数据，从中筛选出最值得关注的Lookalike股票。

{data_ctx}

## 筛选逻辑
1. 优先选同行业内资金流强、市值相近的（同量级赛道竞争对手）
2. KG关联候选优先选有主题共振的
3. 结合参考股票的特点推理"为什么这只相似"

## 回复规范
- 推荐3-5只，用 ```推荐股票\\n[{{"stock_code":"","stock_name":"","reason":"为什么与参考股票相似"}}]``` 格式
- reason字段说明相似点（同行业/同主题/资金共振等）"""

    reply = call_model_fn("chat", system, f"用户问：{message}", max_tokens=2048)
    return _parse_reply(reply)


# ── SubSkill: 选股机器人 ──────────────────────────────────────────

def skill_stock_selector(message: str, project_ctx: dict) -> dict:
    """接入 selector_engine，执行完整选股流程"""
    from stock_selector.selector_engine import run_selector

    # 判断是否需要整体总结（消息较长或含报告类词汇）
    need_summary = len(message) > 200 or any(
        kw in message for kw in ["报告", "两会", "政策", "总结", "整体"]
    )

    try:
        result = run_selector(message, need_summary=need_summary)
    except Exception as e:
        logger.error(f"skill_stock_selector error: {e}")
        return {"reply": f"选股引擎出错：{e}", "recommendations": []}

    stocks = result.get("stocks", [])
    candidates_count = result.get("candidates_count", 0)
    filtered_count = result.get("filtered_count", 0)
    summary = result.get("summary", "")

    if not stocks:
        return {
            "reply": f"KG候选池 {candidates_count} 只，筛选后无符合条件的股票。\n可能原因：筛选条件过严，或相关股票暂无技术面数据。",
            "recommendations": [],
        }

    # 组装回复文本
    lines = []
    if summary:
        lines.append(summary)
        lines.append("")

    lines.append(f"**KG候选池**: {candidates_count} 只 → 筛选后 {filtered_count} 只 → Top {len(stocks)}")
    lines.append("")

    recommendations = []
    for s in stocks:
        reason = s.get("reason", "")
        paths_short = s["kg_paths"][0] if s.get("kg_paths") else ""
        lines.append(f"**{s['name']}**（{s['code']}）")
        if reason:
            lines.append(f"  {reason}")
        if paths_short:
            lines.append(f"  KG路径: {paths_short}")
        lines.append("")
        recommendations.append({
            "stock_code": s["code"],
            "stock_name": s["name"],
            "reason": reason,
        })

    reply = "\n".join(lines).strip()
    return {"reply": reply, "recommendations": recommendations}


# ── SubSkill: 通用兜底 ────────────────────────────────────────────

def skill_general(message: str, basket_stocks: list, project_ctx: dict, call_model_fn) -> dict:
    """兜底通用回复，带完整上下文"""
    basket_text = "\n".join(
        f"- {s['stock_code']} {s.get('stock_name','')}"
        for s in basket_stocks
    ) or "（空）"

    project_name = project_ctx.get("project_name", "")
    investment_logic = project_ctx.get("investment_logic", "")

    system = f"""你是A股选股助手，服务于Portfolio实验室。

当前项目：{project_name}
投资逻辑：{investment_logic or "暂无"}
篮子股票：
{basket_text}

## 回复规范
- 直接给结论，不废话
- 不能确定的说明缺什么数据
- 如推荐个股，用 ```推荐股票\\n[{{"stock_code":"","stock_name":"","reason":""}}]``` 格式"""

    reply = call_model_fn("chat", system, f"用户问：{message}", max_tokens=2048)
    return _parse_reply(reply)


# ── 工具函数 ─────────────────────────────────────────────────────

def _parse_reply(text: str) -> dict:
    """从AI回复中解析推荐股票JSON"""
    import re, json
    recommendations = []
    pattern = r'```推荐股票\s*\n(.*?)```'
    matches = re.findall(pattern, text, re.DOTALL)
    for match in matches:
        try:
            recs = json.loads(match.strip())
            if isinstance(recs, list):
                recommendations.extend(recs)
        except json.JSONDecodeError:
            pass
    clean_text = re.sub(pattern, '', text, flags=re.DOTALL).strip()
    return {"reply": clean_text, "recommendations": recommendations}


# ── 主入口 ───────────────────────────────────────────────────────

def dispatch(message: str, basket_stocks: list, project_ctx: dict, call_model_fn) -> dict:
    """
    根据用户消息路由到对应 SubSkill。

    basket_stocks: list of {stock_code, stock_name, industry_l1, ...}
    project_ctx: {project_name, investment_logic}
    call_model_fn: callable(task, system, user, max_tokens) -> str
    """
    intent = route_intent(message)
    logger.info(f"[chat_skills] intent={intent} | message={message[:50]}")

    basket_codes = [s["stock_code"] for s in basket_stocks if s.get("stock_code")]

    if intent == "capital_flow":
        return skill_capital_flow(message, basket_codes, call_model_fn)
    elif intent == "valuation":
        return skill_valuation(message, basket_codes, call_model_fn)
    elif intent == "kg_theme":
        return skill_kg_theme(message, basket_codes, call_model_fn)
    elif intent == "basket_diag":
        if not basket_stocks:
            return skill_general(message, basket_stocks, project_ctx, call_model_fn)
        return skill_basket_diag(message, basket_stocks, call_model_fn)
    elif intent == "lookalike":
        if not basket_stocks:
            return skill_general(message, basket_stocks, project_ctx, call_model_fn)
        return skill_lookalike(message, basket_stocks, call_model_fn)
    elif intent == "stock_selector":
        return skill_stock_selector(message, project_ctx)
    else:
        return skill_general(message, basket_stocks, project_ctx, call_model_fn)

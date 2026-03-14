"""标签组研究层 — 投资逻辑导向的 6 维深度分析"""
import json
import logging
from utils.db_utils import execute_query, execute_insert
from utils.model_router import call_model as _call_model, call_model_json as _call_model_json

def call_claude(system_prompt, user_message, max_tokens=4096, timeout=900, **kwargs):
    return _call_model('hotspot', system_prompt, user_message, max_tokens=max_tokens, timeout=timeout)

def call_claude_json(system_prompt, user_message, max_tokens=4096, timeout=900):
    return _call_model_json('hotspot', system_prompt, user_message, max_tokens=max_tokens, timeout=timeout)
from utils.skill_registry import get_skill_content
from knowledge_graph.kg_query import extract_context_subgraph, trace_causal_chain

logger = logging.getLogger(__name__)

# ── Helper: 为股票添加 PE 和市值 ───────────────────────────────

def _enrich_stocks_with_pe_cap(stock_codes: list) -> dict:
    """批量查询股票的 PE 和市值，本地缺失时从云端 stock_db 同步"""
    if not stock_codes:
        return {}

    # 1. 从本地 stock_info 获取市值
    ph = ",".join(["?"] * len(stock_codes))
    local_rows = execute_query(
        f"SELECT stock_code, market_cap FROM stock_info WHERE stock_code IN ({ph})",
        stock_codes,
    )
    result = {}
    for r in (local_rows or []):
        cap = r.get("market_cap") or 0
        if cap > 0:
            cap = cap * 1e8  # 亿元转元
        result[r["stock_code"]] = {"market_cap": cap, "pe_ratio": None}

    # 补充未找到的代码
    for code in stock_codes:
        if code not in result:
            result[code] = {"market_cap": 0, "pe_ratio": None}

    # 2. 从云端 stock_db 获取 PE（本地没有这个字段）
    try:
        from config import CLOUD_MYSQL_HOST, CLOUD_MYSQL_PORT, CLOUD_MYSQL_USER, CLOUD_MYSQL_PASSWORD
        import pymysql
        conn = pymysql.connect(
            host=CLOUD_MYSQL_HOST, port=CLOUD_MYSQL_PORT,
            user=CLOUD_MYSQL_USER, password=CLOUD_MYSQL_PASSWORD,
            database='stock_db', charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
        )
        cur = conn.cursor()
        ph2 = ",".join(["%s"] * len(stock_codes))
        cur.execute(
            f"""SELECT symbol as stock_code, pe_ttm, total_mv
                FROM stock_data
                WHERE symbol IN ({ph2})
                ORDER BY trade_date DESC""",
            stock_codes,
        )
        cloud_rows = cur.fetchall()
        seen = set()
        for r in cloud_rows:
            code = r["stock_code"]
            if code in seen:
                continue  # 只取最新一条
            seen.add(code)
            # 更新 PE
            if r.get("pe_ttm"):
                result[code]["pe_ratio"] = float(r["pe_ttm"])
            # 如果本地市值缺失，用云端的
            if result[code]["market_cap"] == 0 and r.get("total_mv"):
                result[code]["market_cap"] = float(r["total_mv"])
        conn.close()
    except Exception as e:
        logger.warning(f"从云端获取PE/市值失败: {e}")

    return result

# ── Prompts ──────────────────────────────────────────────────

MACRO_ANALYSIS_PROMPT = """你是宏观经济分析师。请围绕给定的投资逻辑，全面分析宏观环境对该投资主题的支撑或压制。

{macro_skill_excerpt}

要求：
- 必须围绕投资逻辑展开，不要堆砌无关宏观数据
- 每个因素必须解释它如何影响这个投资主题
- macro_supporting 用于在"成组逻辑"页面展示，需简洁有力
- 对照提供的「数据库实际数据」，判断哪些因素已被数据验证（命中），哪些尚无数据

请严格输出以下 JSON 格式（不要输出其他内容）：
```json
{{
  "summary": "宏观环境总结（2-3句话）",
  "macro_supporting": "宏观环境如何支撑此投资逻辑（1段话，80-150字）",
  "factors": [
    {{
      "name": "宏观因素名称",
      "current_state": "当前状态描述",
      "logic_relation": "与投资逻辑的关系说明",
      "direction": "positive/negative/neutral",
      "confidence": "high/medium/low",
      "db_hit": true,
      "db_evidence": "数据库中命中的具体数据（如有）"
    }}
  ],
  "not_tracked": ["数据库中尚未跟踪但值得关注的宏观因素1", "..."]
}}
```"""

INDUSTRY_ANALYSIS_PROMPT = """你是行业分析师。请围绕给定的投资逻辑，全面分析所有受影响的行业和产业链。

{industry_skill_excerpt}

要求：
- 必须围绕投资逻辑展开
- industry_supporting 用于在"成组逻辑"页面展示，需简洁有力
- industries 必须按「直接影响 → 间接/传递影响」排序
- 每个行业必须说明影响类型（direct/indirect）和传导路径
- chain_position 必须是：上游/中游/下游/全产业链 之一

请严格输出以下 JSON 格式（不要输出其他内容）：
```json
{{
  "summary": "行业格局总结（2-3句话）",
  "industry_supporting": "行业趋势如何支撑此投资逻辑（1段话，80-150字）",
  "industries": [
    {{
      "name": "行业名称",
      "chain_position": "上游/中游/下游/全产业链",
      "impact_type": "direct/indirect",
      "transmission_path": "传导路径说明",
      "benefit_reason": "受益/受损原因",
      "supporting_evidence": "支撑证据（来自新闻、数据等）",
      "key_catalysts": ["催化剂1", "催化剂2"],
      "confidence": "high/medium/low",
      "impact_direction": "positive/negative/mixed"
    }}
  ]
}}
```"""

STOCK_RATIONALE_PROMPT = """你是基金经理。请为每只推荐个股提供投资逻辑——为什么你愿意投资并承担风险。

投资主题：{group_logic}

要求：
- 每只股票的 reason 字段写 1-2 句话，说明核心投资逻辑
- 角度：这只股票在当前主题下的独特优势、催化剂、竞争壁垒
- 不要泛泛而谈"受益于行业增长"，要具体到公司层面的逻辑
- risk 字段写 1 句话，说明最大风险点

请严格输出 JSON 数组（stock_code 对应输入中的代码，不要输出其他内容）：
```json
[
  {{
    "stock_code": "000001.SZ",
    "reason": "具体的投资逻辑...",
    "risk": "最大风险点..."
  }}
]
```"""

STOCK_RANKING_PROMPT = """你是基金经理。请对以下候选个股按 stock-recommendation Skill 的四维评分框架进行行业内相对排序。

投资主题：{group_logic}

对每只股票按以下四个维度评分（行业内相对排序，非硬性门槛）：

1. 龙头属性（最高权重）：满足任一即可——定价权/技术壁垒/定义产业进程/掌握受益资源
2. 受益直接性（高权重）：直接受益（0跳）> 间接受益（1跳）> 边际受益（≥2跳）
3. 估值合理性（中权重）：PE相对行业中位数偏离度，亏损公司用PS/PEG/EV/EBITDA替代
4. 流动性（基础权重）：结合资金流入数据和行业地位

输出要求：
- reason: 推荐标签，从以下选择1-3个（用逗号分隔）：
  龙头:定价权, 龙头:技术壁垒, 龙头:定义产业, 龙头:受益资源,
  受益:直接, 受益:间接, 受益:边际,
  估值:低估, 估值:合理, 估值:偏高,
  成长:高成长, 成长:转型中, 成长:稳定
- logic: 1-2句具体投资逻辑说明，含受益路径

请严格输出 JSON 数组，按综合得分从高到低排列（不要输出其他内容）：
[{{"stock_code": "...", "stock_name": "...", "reason": "龙头:定价权,受益:直接", "logic": "具体说明..."}}]
"""

LOGIC_SYNTHESIS_PROMPT = """你是投资研究主编。请基于以下材料，为这个投资主题撰写一份完整的投资逻辑综述。

投资主题：{group_logic}
标签：{tags}

要求：
- news_summary: 从关联新闻中总结，相关新闻如何支撑/影响此投资逻辑（3-5句话，引用具体新闻事实）
- industry_summary: 该投资逻辑影响哪些行业、如何影响（2-3句话，引用具体行业和传导路径）
- macro_hit_summary: 目前数据库中命中了哪些宏观因素在驱动/压制此投资逻辑（2-3句话，引用具体宏观数据）
- news_digest: 从关联新闻中提炼出 3-5 条最关键的证据链，每条说明「什么事实 → 如何支撑/削弱投资逻辑」
- macro_impact: 概括宏观环境对此投资主题的整体影响（2-3句话，需引用具体宏观因素）
- industry_impact: 概括受益行业格局和产业链传导逻辑（2-3句话，需引用具体行业）
- thesis_summary: 综合以上三方面，写一段 150-250 字的投资逻辑总结，要有论点、论据、结论
- investment_opportunity_points: 提炼 2-4 个可操作的投资机会点，每个包含触发条件、受益行业/标的、主要风险

请严格输出以下 JSON 格式（不要输出其他内容）：
```json
{{
  "news_summary": "新闻对投资逻辑的支撑总结...",
  "industry_summary": "影响行业总结...",
  "macro_hit_summary": "宏观命中情况总结...",
  "news_digest": [
    {{"fact": "事实描述", "implication": "对投资逻辑的含义", "direction": "positive/negative/neutral"}}
  ],
  "macro_impact": "宏观影响概述...",
  "industry_impact": "行业影响概述...",
  "thesis_summary": "综合投资逻辑总结...",
  "investment_opportunity_points": [
    {{"trigger": "触发条件", "beneficiary": "受益行业/标的", "risk": "主要风险"}}
  ]
}}
```"""

NEWS_PARSE_PROMPT = """你是投资研究助手。请将以下新闻逐条解析为结构化信息，围绕给定的投资逻辑进行分析。

投资逻辑：{group_logic}

对每条新闻输出：
- fact: 客观事实描述（1句话）
- opinion: 市场观点/分析师观点（1句话，如无明显观点则写"无明确观点"）
- highlight: 一句话总结该新闻跟投资逻辑的关联性（必须具体，说明什么事实→如何影响投资逻辑）
- supporting: 对本投资逻辑的支撑或削弱说明（1句话）
- direction: positive/negative/neutral

请严格输出 JSON 数组（不要输出其他内容）：
```json
[
  {{
    "news_id": 原始新闻ID,
    "fact": "...",
    "opinion": "...",
    "highlight": "...",
    "supporting": "...",
    "direction": "positive/negative/neutral"
  }}
]
```"""


# ── Main Research Flow ───────────────────────────────────────

def research_tag_group(group_id, progress_callback=None):
    """对标签组进行完整深度研究（6维分析）"""
    def _progress(msg, pct=None):
        if progress_callback:
            progress_callback(msg, pct)

    group = execute_query("SELECT * FROM tag_groups WHERE id=?", [group_id])
    if not group:
        return None
    group = group[0]
    tags = json.loads(group["tags_json"])
    tags_str = ", ".join(tags)
    group_logic = group.get("group_logic")

    # 如果没有成组逻辑，先生成一个
    if not group_logic:
        _progress("生成投资逻辑", 2)
        group_logic = _generate_group_logic(tags)
        if group_logic:
            execute_insert(
                "UPDATE tag_groups SET group_logic=? WHERE id=?",
                [group_logic, group_id],
            )
            logger.info(f"为标签组 {group_id} 生成投资逻辑: {group_logic[:60]}...")
        else:
            group_logic = f"围绕{tags_str}的投资主题"

    # 1. 收集关联新闻
    _progress("[1/6] 收集关联新闻", 5)
    news_data = _collect_news(tags)

    # 2. 解析新闻为结构化 fact/opinion/supporting
    _progress("[2/6] AI 解析新闻", 15)
    news_parsed = _parse_news_with_claude(news_data[:20], group_logic)

    # 3. 收集主题热度（15日热力图）
    _progress("[3/6] 收集主题热度", 30)
    theme_heat = _collect_theme_heat(tags)

    # 3.5 收集细分行业 chunk 热度（7/15/30天）
    industry_heat = {}
    try:
        industry_heat = _collect_industry_chunk_heat(tags, days_list=[7, 15, 30])
    except Exception as e:
        logger.warning(f"细分行业 chunk 热度收集失败: {e}")

    # 4. 收集资金流向（保留用于个股推荐）
    stock_flows = _collect_stock_flows(tags)

    # 5. 从 KG 收集受益行业
    kg_industries = _collect_kg_industries(tags)

    # 6. 构建分析上下文
    context = _build_context(tags_str, group_logic, news_data, stock_flows, kg_industries)

    # 6.5 收集宏观实际数据
    _progress("[4/8] 收集宏观数据", 40)
    try:
        macro_data = _collect_macro_data()
    except Exception as e:
        logger.warning(f"宏观数据收集失败: {e}")
        macro_data = {"macro_indicators": [], "northbound_flow": []}

    # 构建含宏观数据的上下文
    context_with_macro = context + "\n\n=== 数据库宏观指标（近30日） ===\n"
    for row in macro_data["macro_indicators"][:30]:
        context_with_macro += f"- {row.get('indicator_name', '')} ({row.get('indicator_date', '')}): {row.get('value', '')} {row.get('unit', '')}\n"
    context_with_macro += "\n=== 北向资金（近7日） ===\n"
    for row in macro_data["northbound_flow"][:7]:
        context_with_macro += f"- {row.get('trade_date', '')}: 净流入 {row.get('total_net', '')}亿\n"

    # 7. Claude 生成结构化宏观分析（注入 macro-stock-analysis Skill）
    _progress("[5/8] AI 宏观分析", 48)
    macro_json = {}
    macro_report = ""
    try:
        macro_skill = get_skill_content("macro-stock-analysis")
        macro_skill_excerpt = _extract_skill_key_sections(macro_skill, max_chars=3000)
        macro_prompt = MACRO_ANALYSIS_PROMPT.format(macro_skill_excerpt=macro_skill_excerpt)
        macro_prompt = _inject_critical_thinking(macro_prompt)
        macro_json = _call_json_with_retry(macro_prompt, context_with_macro, max_tokens=3000)
        macro_report = macro_json.get("summary", "")
    except Exception as e:
        logger.warning(f"宏观分析失败: {e}")
        macro_report = "宏观分析生成失败，请稍后重试。"

    # 8. Claude 生成结构化行业分析（注入 event-industry-impact Skill）
    _progress("[6/8] AI 行业分析", 58)
    industry_json = {}
    industry_report = ""
    try:
        industry_skill = get_skill_content("event-industry-impact")
        industry_skill_excerpt = _extract_skill_key_sections(industry_skill, max_chars=3000)
        industry_prompt = INDUSTRY_ANALYSIS_PROMPT.format(industry_skill_excerpt=industry_skill_excerpt)
        industry_prompt = _inject_critical_thinking(industry_prompt)
        industry_json = _call_json_with_retry(industry_prompt, context, max_tokens=2000)
        industry_report = industry_json.get("summary", "")
    except Exception as e:
        logger.warning(f"行业分析失败: {e}")
        industry_report = "行业分析生成失败，请稍后重试。"

    # 9. 构建个股推荐 — 每个受益产业链推荐股票
    _progress("[7/8] 构建个股推荐", 70)
    top10 = []
    all_codes_for_enrich = []
    try:
        # 获取所有产业链信息
        industries_list = industry_json.get("industries", []) if isinstance(industry_json, dict) else []

        # 为每个产业链匹配股票
        industry_stocks_map = {}  # {产业链名: [股票列表]}
        for ind in industries_list:
            ind_name = ind.get("name", "")
            if not ind_name:
                continue
            chain_pos = ind.get("chain_position", "")

            # 提取行业名主要部分
            import re
            main_name = re.sub(r'[（(].*[）)]', '', ind_name).strip()
            if not main_name:
                main_name = ind_name

            # 构建匹配关键词（包含别名）
            INDUSTRY_ALIAS = {
                "传感器": "电子", "电子元件": "电子", "连接器": "电子",
                "PCB": "电子", "被动元件": "电子", "AI算力": "计算机",
                "光模块": "通信", "服务器": "计算机", "消费电子": "消费电子",
                "汽车电子": "汽车", "半导体设备": "半导体", "半导体材料": "半导体",
                "数据中心": "通信", "IDC": "通信", "云计算": "计算机",
            }
            keywords = [main_name, ind_name[:4]]
            for alias, db_name in INDUSTRY_ALIAS.items():
                if alias in main_name or alias in ind_name:
                    keywords.append(db_name)

            # 从 stock_info 匹配
            conditions = ["(si.industry_l1 LIKE ? OR si.industry_l2 LIKE ?)"]
            params = [f"%{main_name}%", f"%{main_name}%"]
            for kw in set(keywords) - {main_name}:
                conditions.append("(si.industry_l1 LIKE ? OR si.industry_l2 LIKE ?)")
                params.extend([f"%{kw}%", f"%{kw}%"])

            stocks = execute_query(
                f"""SELECT si.stock_code, si.stock_name, si.industry_l1, si.industry_l2,
                           COALESCE(SUM(cf.main_net_inflow), 0) as total_inflow_7d
                    FROM stock_info si
                    LEFT JOIN capital_flow cf ON si.stock_code = cf.stock_code
                        AND cf.trade_date >= (SELECT DISTINCT trade_date FROM capital_flow ORDER BY trade_date DESC LIMIT 1 OFFSET 6)
                    WHERE {' OR '.join(conditions)}
                    GROUP BY si.stock_code, si.stock_name, si.industry_l1, si.industry_l2
                    ORDER BY total_inflow_7d DESC
                    LIMIT 5""",
                params,
            )
            if stocks:
                industry_stocks_map[ind_name] = {
                    "chain_position": chain_pos,
                    "impact_type": ind.get("impact_type", "direct"),
                    "stocks": [dict(s) for s in stocks],
                }

        # 补充：将新闻标签关联的股票归类到对应产业链
        for sf in stock_flows:
            code = sf["stock_code"]
            ind_l1 = sf.get("industry_l1", "")
            ind_l2 = sf.get("industry_l2", "")

            # 找到最匹配的产业链
            matched_industry = None
            for ind in industries_list:
                ind_name = ind.get("name", "")
                main_name = re.sub(r'[（(].*[）)]', '', ind_name).strip()
                if main_name in ind_l1 or main_name in ind_l2 or ind_l1 in main_name or ind_l2 in main_name:
                    matched_industry = ind_name
                    break

            if matched_industry and matched_industry in industry_stocks_map:
                # 检查是否已存在
                existing_codes = {s["stock_code"] for s in industry_stocks_map[matched_industry]["stocks"]}
                if code not in existing_codes:
                    industry_stocks_map[matched_industry]["stocks"].append({
                        "stock_code": code,
                        "stock_name": sf.get("stock_name", code),
                        "industry_l1": ind_l1,
                        "industry_l2": ind_l2,
                        "total_inflow_7d": sf.get("total_inflow", 0),
                    })
            elif matched_industry:
                # 产业链还没有股票，创建
                for ind in industries_list:
                    if ind.get("name") == matched_industry:
                        industry_stocks_map[matched_industry] = {
                            "chain_position": ind.get("chain_position", ""),
                            "impact_type": ind.get("impact_type", "direct"),
                            "stocks": [{
                                "stock_code": code,
                                "stock_name": sf.get("stock_name", code),
                                "industry_l1": ind_l1,
                                "industry_l2": ind_l2,
                                "total_inflow_7d": sf.get("total_inflow", 0),
                            }],
                        }
                        break

        # 构建最终推荐结构
        for ind in industries_list:
            ind_name = ind.get("name", "")
            if ind_name in industry_stocks_map:
                stocks = industry_stocks_map[ind_name]["stocks"]
                # 按资金流入排序，取 Top 3
                stocks.sort(key=lambda x: x.get("total_inflow_7d", 0), reverse=True)
                for s in stocks[:3]:
                    all_codes_for_enrich.append(s["stock_code"])
                top10.append({
                    "industry": ind_name,
                    "chain_position": industry_stocks_map[ind_name]["chain_position"],
                    "impact_type": industry_stocks_map[ind_name]["impact_type"],
                    "stocks": stocks[:3],
                })

        # Skill 级分析排序（为每只股票生成推荐理由）
        _progress("AI 个股排序", 78)
        all_candidates = []
        for grp in top10:
            for s in grp.get("stocks", []):
                s["chain_position"] = grp["chain_position"]
                s["from_industry"] = grp["industry"]
                all_candidates.append(s)

        if all_candidates:
            ranked_candidates = _rank_stocks_with_skill(all_candidates[:30], group_logic)

            # 重新按产业链分组
            ranked_map = {s["stock_code"]: s for s in ranked_candidates}
            for grp in top10:
                for s in grp.get("stocks", []):
                    r = ranked_map.get(s["stock_code"], {})
                    s["recommend_tags"] = r.get("recommend_tags", "")
                    s["recommend_logic"] = r.get("recommend_logic", "")

            # 批量添加 PE 和市值
            pe_cap_map = _enrich_stocks_with_pe_cap(all_codes_for_enrich)
            for grp in top10:
                for s in grp.get("stocks", []):
                    info = pe_cap_map.get(s["stock_code"], {})
                    s["pe_ratio"] = info.get("pe_ratio")
                    s["market_cap"] = info.get("market_cap")
    except Exception as e:
        logger.warning(f"个股推荐构建失败: {e}")
        # 降级：使用旧逻辑
        industry_stocks = _collect_industry_stocks(industry_json, tags)
        top10 = _build_stock_recommendations(industry_json, industry_stocks, stock_flows)

    # 10. AI 综合逻辑论证（汇总新闻论据 + 宏观/行业影响 + 3 个 summary）
    _progress("[8/8] 综合逻辑论证", 90)
    logic_synthesis = {}
    try:
        synthesis_context = context
        synthesis_context += f"\n\n=== 宏观分析结论 ===\n{macro_json.get('summary', '暂无')}"
        if macro_json.get("factors"):
            synthesis_context += "\n宏观因素命中情况:\n"
            for f in macro_json["factors"]:
                hit = "已命中" if f.get("db_hit") else "未命中"
                synthesis_context += f"- {f.get('name', '')}: {hit}"
                if f.get("db_evidence"):
                    synthesis_context += f" ({f['db_evidence']})"
                synthesis_context += "\n"
        synthesis_context += f"\n\n=== 行业分析结论 ===\n{industry_json.get('summary', '暂无')}"
        if news_parsed:
            synthesis_context += "\n\n=== 新闻结构化解析 ===\n"
            for np in news_parsed[:10]:
                synthesis_context += f"- 事实: {np.get('fact', '')} | 对投资逻辑: {np.get('supporting', '')} ({np.get('direction', '')})\n"
        prompt = LOGIC_SYNTHESIS_PROMPT.format(group_logic=group_logic, tags=tags_str)
        logic_synthesis = _call_json_with_retry(prompt, synthesis_context, max_tokens=2000)
    except Exception as e:
        logger.warning(f"逻辑综合生成失败: {e}")

    # 11. 保存到 DB
    _progress("保存研究结果", 95)
    research_id = execute_insert(
        """INSERT INTO tag_group_research
           (group_id, research_date, macro_report, macro_json,
            industry_report, industry_json, news_summary_json,
            news_parsed_json, sector_heat_json, theme_heat_json,
            top10_stocks_json, logic_synthesis_json, industry_heat_json)
           VALUES (?, date('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [group_id, macro_report,
         json.dumps(macro_json, ensure_ascii=False, default=str),
         industry_report,
         json.dumps(industry_json, ensure_ascii=False, default=str),
         json.dumps([dict(n) for n in news_data[:20]], ensure_ascii=False, default=str),
         json.dumps(news_parsed, ensure_ascii=False, default=str),
         json.dumps([], ensure_ascii=False),  # sector_heat_json 保留兼容
         json.dumps(theme_heat, ensure_ascii=False, default=str),
         json.dumps(top10, ensure_ascii=False, default=str),
         json.dumps(logic_synthesis, ensure_ascii=False, default=str),
         json.dumps(industry_heat, ensure_ascii=False, default=str)],
    )

    # 不再将热点标签组研究存入 content_summaries，避免污染 RAG 检索

    return {
        "research_id": research_id,
        "macro_json": macro_json,
        "industry_json": industry_json,
        "news_parsed": news_parsed,
        "theme_heat": theme_heat,
        "top10_stocks": top10,
    }


def get_group_research_history(group_id, limit=5):
    """获取标签组的研究历史"""
    return execute_query(
        """SELECT * FROM tag_group_research
           WHERE group_id=? ORDER BY research_date DESC LIMIT ?""",
        [group_id, limit],
    )


# ── Data Collection ──────────────────────────────────────────

def _collect_news(tags):
    """收集标签关联新闻 — 优先 hybrid_search，降级旧 LIKE 逻辑"""
    # 优先：hybrid_search 从 chunks 反查 content_summaries
    try:
        from retrieval.hybrid import hybrid_search
        query = " ".join(tags[:3])
        hr = hybrid_search(query, top_k=20)
        chunks = hr.chunks or []
        if chunks:
            # 从 chunks 反查 content_summaries 结构化摘要
            ext_ids = list({c.extracted_text_id for c in chunks if c.extracted_text_id})
            news_list = []
            if ext_ids:
                ph = ",".join(["%s"] * len(ext_ids))
                cs_rows = execute_query(
                    f"""SELECT cs.id, cs.extracted_text_id,
                               COALESCE(cs.fact_summary, cs.summary) AS summary,
                               cs.created_at AS cleaned_at,
                               'neutral' AS sentiment, 3 AS importance,
                               'research' AS event_type
                        FROM content_summaries cs
                        WHERE cs.extracted_text_id IN ({ph})
                        ORDER BY cs.created_at DESC""",
                    ext_ids,
                )
                seen = set()
                for r in (cs_rows or []):
                    key = r.get("extracted_text_id")
                    if key not in seen:
                        seen.add(key)
                        d = dict(r)
                        d["match_tags"] = 1
                        news_list.append(d)
            if news_list:
                return news_list
    except Exception as e:
        logger.warning(f"hybrid_search 收集新闻失败，降级 LIKE: {e}")

    # 降级：旧 LIKE 逻辑
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


def _parse_news_with_claude(news_list, group_logic):
    """批量解析新闻为 fact/opinion/supporting 结构"""
    if not news_list:
        return []

    news_text = "\n".join(
        f"[ID={n.get('id', i)}] {n.get('summary', '')}"
        for i, n in enumerate(news_list)
    )
    prompt = NEWS_PARSE_PROMPT.format(group_logic=group_logic)

    try:
        parsed = call_claude_json(prompt, news_text, max_tokens=3000)
        if isinstance(parsed, list):
            # 将解析结果与原始新闻合并
            parsed_map = {}
            for p in parsed:
                nid = p.get("news_id")
                if nid is not None:
                    parsed_map[nid] = p

            result = []
            for n in news_list:
                nid = n.get("id")
                p = parsed_map.get(nid, {})
                result.append({
                    "news_id": nid,
                    "summary": n.get("summary", ""),
                    "sentiment": n.get("sentiment"),
                    "importance": n.get("importance", 0),
                    "match_tags": n.get("match_tags", 0),
                    "fact": p.get("fact", n.get("summary", "")),
                    "opinion": p.get("opinion", "无明确观点"),
                    "highlight": p.get("highlight", ""),
                    "supporting": p.get("supporting", ""),
                    "direction": p.get("direction", "neutral"),
                })
            return result
        return []
    except Exception as e:
        logger.warning(f"新闻解析失败: {e}")
        # 降级：返回原始新闻，不含结构化字段
        return [{
            "news_id": n.get("id"),
            "summary": n.get("summary", ""),
            "sentiment": n.get("sentiment"),
            "importance": n.get("importance", 0),
            "match_tags": n.get("match_tags", 0),
            "fact": n.get("summary", ""),
            "opinion": "无明确观点",
            "highlight": "",
            "supporting": "",
            "direction": n.get("sentiment", "neutral"),
        } for n in news_list]


def _collect_theme_heat(tags):
    """从 dashboard_tag_frequency 查询 15 日热力图数据"""
    if not tags:
        return {"dates": [], "tags": [], "matrix": {}}

    # 获取最近 15 个交易日
    date_rows = execute_query(
        """SELECT DISTINCT appear_date FROM dashboard_tag_frequency
           ORDER BY appear_date DESC LIMIT 15""",
        [],
    )
    if not date_rows:
        return {"dates": [], "tags": [], "matrix": {}}

    dates = sorted([r["appear_date"] for r in date_rows])

    # 查询标签在这些日期的出现频次
    placeholders_tags = ",".join(["?"] * len(tags))
    placeholders_dates = ",".join(["?"] * len(dates))
    freq_rows = execute_query(
        f"""SELECT tag_name, appear_date, COUNT(*) as freq
            FROM dashboard_tag_frequency
            WHERE tag_name IN ({placeholders_tags})
            AND appear_date IN ({placeholders_dates})
            GROUP BY tag_name, appear_date""",
        tags + dates,
    )

    # 构建热力图矩阵
    matrix = {}
    tag_totals = {}
    for row in freq_rows:
        tname = row["tag_name"]
        if tname not in matrix:
            matrix[tname] = {}
            tag_totals[tname] = 0
        matrix[tname][row["appear_date"]] = row["freq"]
        tag_totals[tname] += row["freq"]

    # 按总频次排序标签
    sorted_tags = sorted(matrix.keys(), key=lambda t: tag_totals.get(t, 0), reverse=True)

    return {
        "dates": dates,
        "tags": sorted_tags,
        "matrix": matrix,
        "totals": tag_totals,
    }


def _collect_industry_chunk_heat(tags: list, days_list: list = None) -> dict:
    """统计与 tags 相关的细分行业 chunk 热度（多时间窗口）

    返回：{
      "days_list": [7, 15, 30],
      "industries": [
        {"name": str, "total_7": int, "total_15": int, "total_30": int,
         "capital_net_inflow": float, "market_cap": float, "flow_cap_ratio": float}
      ]
    }
    """
    if days_list is None:
        days_list = [7, 15, 30]

    # Step 1: 从 stock_mentions 找与 tags 相关的 stock_codes
    stock_codes = set()
    for tag in tags:
        rows = execute_query(
            """SELECT DISTINCT stock_code FROM stock_mentions
               WHERE (related_themes LIKE %s OR theme_logic LIKE %s)
                 AND stock_code IS NOT NULL AND stock_code != ''
               LIMIT 100""",
            [f"%{tag}%", f"%{tag}%"],
        )
        for r in (rows or []):
            if r.get("stock_code"):
                stock_codes.add(r["stock_code"])

    if not stock_codes:
        return {"days_list": days_list, "industries": []}

    # Step 2: stock_codes → industry_l2
    ph = ",".join(["%s"] * len(stock_codes))
    si_rows = execute_query(
        f"SELECT stock_code, industry_l2 FROM stock_info WHERE stock_code IN ({ph}) AND industry_l2 IS NOT NULL AND industry_l2 != ''",
        list(stock_codes),
    )
    code_industry = {r["stock_code"]: r["industry_l2"] for r in (si_rows or [])}
    industry_codes = {}  # industry_l2 -> set(stock_code)
    for code, ind in code_industry.items():
        industry_codes.setdefault(ind, set()).add(code)

    if not industry_codes:
        return {"days_list": days_list, "industries": []}

    # Step 3: 按 industry_l2 → 各时间窗口 chunk 数
    industry_data = {}
    for ind_name, codes in industry_codes.items():
        ind_ph = ",".join(["%s"] * len(codes))
        day_counts = {}
        for days in days_list:
            try:
                # 优先用 text_chunks
                cnt_rows = execute_query(
                    f"""SELECT COUNT(DISTINCT tc.extracted_text_id) AS cnt
                        FROM text_chunks tc
                        JOIN stock_mentions sm ON tc.extracted_text_id = sm.extracted_text_id
                        WHERE sm.stock_code IN ({ind_ph})
                          AND tc.publish_time >= DATE_SUB(NOW(), INTERVAL %s DAY)""",
                    list(codes) + [days],
                )
                cnt = int(cnt_rows[0]["cnt"]) if cnt_rows else 0
                if cnt == 0:
                    # fallback: stock_mentions 直接统计
                    cnt_rows2 = execute_query(
                        f"""SELECT COUNT(DISTINCT extracted_text_id) AS cnt
                            FROM stock_mentions
                            WHERE stock_code IN ({ind_ph})
                              AND mention_time >= DATE_SUB(NOW(), INTERVAL %s DAY)""",
                        list(codes) + [days],
                    )
                    cnt = int(cnt_rows2[0]["cnt"]) if cnt_rows2 else 0
            except Exception as e:
                logger.warning(f"industry chunk count 失败: {e}")
                cnt = 0
            day_counts[days] = cnt

        # Step 4: 资金净流入 + 市值（SUM over industry stocks）
        try:
            flow_rows = execute_query(
                f"""SELECT SUM(cf.main_net_inflow) AS net_inflow
                    FROM capital_flow cf
                    WHERE cf.stock_code IN ({ind_ph})
                      AND cf.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)""",
                list(codes),
            )
            net_inflow = float(flow_rows[0]["net_inflow"] or 0) if flow_rows else 0.0

            cap_rows = execute_query(
                f"SELECT SUM(market_cap) AS total_cap FROM stock_info WHERE stock_code IN ({ind_ph}) AND market_cap > 0",
                list(codes),
            )
            mkt_cap = float(cap_rows[0]["total_cap"] or 0) * 1e8 if cap_rows else 0.0
        except Exception as e:
            logger.warning(f"industry capital 查询失败: {e}")
            net_inflow = 0.0
            mkt_cap = 0.0

        flow_cap_ratio = net_inflow / mkt_cap if mkt_cap > 0 else 0.0
        entry = {"name": ind_name, "flow_cap_ratio": round(flow_cap_ratio * 100, 4),
                 "capital_net_inflow": net_inflow, "market_cap": mkt_cap}
        for days in days_list:
            entry[f"total_{days}"] = day_counts.get(days, 0)
        industry_data[ind_name] = entry

    # 按 total_{days_list[0]} 降序
    industries = sorted(industry_data.values(), key=lambda x: x.get(f"total_{days_list[0]}", 0), reverse=True)
    return {"days_list": days_list, "industries": industries}


def _collect_stock_flows(tags):
    """收集标签关联个股的资金流向"""
    stock_map = {}
    for tag in tags:
        flows = execute_query(
            """SELECT cf.stock_code, MAX(si.stock_name) as stock_name,
                      MAX(si.industry_l1) as industry_l1,
                      MAX(si.industry_l2) as industry_l2,
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
                    "industry_l1": f.get("industry_l1") or "",
                    "industry_l2": f.get("industry_l2") or "",
                    "total_inflow": 0,
                    "match_tags": 0,
                    "source": "news",
                }
            stock_map[code]["total_inflow"] += f.get("total_inflow", 0) or 0
            stock_map[code]["match_tags"] += 1

    return sorted(stock_map.values(), key=lambda x: x["total_inflow"], reverse=True)


def _collect_kg_industries(tags):
    """从知识图谱查询与标签相关的行业实体"""
    if not tags:
        return []

    industries = []
    for tag in tags:
        # 查找 tag 对应的 theme 实体
        theme_entities = execute_query(
            """SELECT id FROM kg_entities
               WHERE entity_type='theme' AND entity_name LIKE ?""",
            [f"%{tag}%"],
        )
        for te in theme_entities:
            # 查找 theme→industry 或 industry→theme 的关系
            rels = execute_query(
                """SELECT e.entity_name, e.id as entity_id,
                          r.relation_type, r.direction, r.strength, r.confidence
                   FROM kg_relationships r
                   JOIN kg_entities e ON (
                       (r.source_entity_id=? AND r.target_entity_id=e.id)
                       OR (r.target_entity_id=? AND r.source_entity_id=e.id)
                   )
                   WHERE e.entity_type='industry'""",
                [te["id"], te["id"]],
            )
            for rel in rels:
                industries.append({
                    "name": rel["entity_name"],
                    "relation": rel["relation_type"],
                    "direction": rel["direction"],
                    "strength": rel.get("strength", 0.5),
                })

    # 去重
    seen = set()
    unique = []
    for ind in industries:
        if ind["name"] not in seen:
            seen.add(ind["name"])
            unique.append(ind)
    return unique


def _collect_macro_data():
    """收集近期宏观指标数据，供 AI 分析哪些被命中"""
    macro_rows = execute_query(
        """SELECT indicator_name, indicator_date, value, unit
           FROM macro_indicators
           WHERE indicator_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
           ORDER BY indicator_date DESC""",
        [],
    )

    nb_rows = execute_query(
        """SELECT trade_date, total_net, cumulative
           FROM northbound_flow
           WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
           ORDER BY trade_date DESC""",
        [],
    )

    return {"macro_indicators": macro_rows or [], "northbound_flow": nb_rows or []}


def _extract_skill_key_sections(skill_content, max_chars=3000):
    """从 Skill markdown 中截取关键段落（Quick Reference、China Market 等），控制 token 量"""
    if not skill_content:
        return ""

    # 跳过 YAML front-matter
    lines = skill_content.split("\n")
    content_lines = []
    in_frontmatter = False
    for line in lines:
        if line.strip() == "---":
            in_frontmatter = not in_frontmatter
            continue
        if not in_frontmatter:
            content_lines.append(line)
    full_text = "\n".join(content_lines).strip()

    # 尝试提取关键 section
    key_headers = ["Quick Reference", "China Market", "关键框架", "核心指标", "Key Indicator",
                    "Value Chain", "Industry Mapping", "产业链", "行业映射"]
    sections = []
    current_section = []
    current_header = ""

    for line in content_lines:
        if line.startswith("## ") or line.startswith("### "):
            if current_section and any(kw.lower() in current_header.lower() for kw in key_headers):
                sections.append("\n".join(current_section))
            current_section = [line]
            current_header = line
        else:
            current_section.append(line)

    # 检查最后一个 section
    if current_section and any(kw.lower() in current_header.lower() for kw in key_headers):
        sections.append("\n".join(current_section))

    if sections:
        result = "\n\n".join(sections)
        return result[:max_chars]

    # 降级：截取前 max_chars
    return full_text[:max_chars]


def _collect_industry_stocks(industry_json, tags):
    """根据受益行业列表查询行业内股票"""
    if not isinstance(industry_json, dict):
        return {}
    benefiting = industry_json.get("industries") or industry_json.get("benefiting_industries", [])
    industry_names = [b["name"] for b in benefiting if b.get("name")]

    if not industry_names:
        return {}

    result = {}
    for ind_name in industry_names:
        stocks = execute_query(
            """SELECT si.stock_code, si.stock_name, si.industry_l1, si.industry_l2,
                      COALESCE(cf_agg.total_inflow, 0) as total_inflow
               FROM stock_info si
               LEFT JOIN (
                   SELECT stock_code, SUM(main_net_inflow) as total_inflow
                   FROM capital_flow
                   GROUP BY stock_code
               ) cf_agg ON si.stock_code = cf_agg.stock_code
               WHERE si.industry_l1 LIKE ? OR si.industry_l2 LIKE ?
               ORDER BY total_inflow DESC LIMIT 10""",
            [f"%{ind_name}%", f"%{ind_name}%"],
        )
        if stocks:
            result[ind_name] = [dict(s) for s in stocks]

    return result


def _build_stock_recommendations(industry_json, industry_stocks, stock_flows):
    """合并受益行业股票 + 标签关联股票，按行业分组"""
    if not isinstance(industry_json, dict):
        benefiting = []
    else:
        benefiting = industry_json.get("industries") or industry_json.get("benefiting_industries", [])

    # 已收录的股票代码（去重用）
    seen_codes = set()
    groups = []

    # 收集所有股票代码，用于批量查询 PE 和市值
    all_codes = []

    # 按受益行业分组
    for ind in benefiting:
        ind_name = ind.get("name", "")
        chain_pos = ind.get("chain_position", "")
        impact_type = ind.get("impact_type", "direct")
        ind_stocks = industry_stocks.get(ind_name, [])

        stock_list = []
        for s in ind_stocks:
            code = s["stock_code"]
            if code in seen_codes:
                continue
            seen_codes.add(code)
            all_codes.append(code)
            # 查找是否也在标签关联股票中
            tag_match = next((sf for sf in stock_flows if sf["stock_code"] == code), None)
            stock_list.append({
                "stock_code": code,
                "stock_name": s.get("stock_name", code),
                "industry_l2": s.get("industry_l2", ""),
                "total_inflow": s.get("total_inflow", 0),
                "match_tags": tag_match["match_tags"] if tag_match else 0,
                "source": "both" if tag_match else "kg",
            })

        if stock_list:
            groups.append({
                "industry": ind_name,
                "chain_position": chain_pos,
                "impact_type": impact_type,
                "stocks": stock_list[:8],
            })

    # 补充：标签关联但不在受益行业中的股票
    remaining = []
    for sf in stock_flows:
        if sf["stock_code"] not in seen_codes:
            seen_codes.add(sf["stock_code"])
            all_codes.append(sf["stock_code"])
            remaining.append({
                "stock_code": sf["stock_code"],
                "stock_name": sf["stock_name"],
                "industry_l2": sf.get("industry_l2", ""),
                "total_inflow": sf.get("total_inflow", 0),
                "match_tags": sf.get("match_tags", 0),
                "source": "news",
            })
    if remaining:
        groups.append({
            "industry": "其他关联标的",
            "chain_position": "",
            "impact_type": "indirect",
            "stocks": remaining[:10],
        })

    # 批量查询 PE 和市值，添加到每只股票
    pe_cap_map = _enrich_stocks_with_pe_cap(all_codes)
    for grp in groups:
        for s in grp.get("stocks", []):
            info = pe_cap_map.get(s["stock_code"], {})
            s["pe_ratio"] = info.get("pe_ratio")
            s["market_cap"] = info.get("market_cap")

    # stock_mentions 优先级加权：与标签匹配的股票排在前面
    try:
        from utils.content_query import query_stock_mentions
        sm_rows = query_stock_mentions(limit=200, days=14)
        mentioned_codes = set()
        for sm in sm_rows:
            if sm.get("stock_code"):
                mentioned_codes.add(sm["stock_code"])
        # 将 mentioned 股票标记，排序时优先
        for grp in groups:
            stocks = grp.get("stocks", [])
            for s in stocks:
                s["_mentioned"] = s.get("stock_code", "") in mentioned_codes
            stocks.sort(key=lambda x: (x.get("_mentioned", False), x.get("net_inflow", 0) or 0), reverse=True)
            for s in stocks:
                s.pop("_mentioned", None)
    except Exception:
        pass

    return groups


def _collect_industry_top3_by_flow(industries_json):
    """对每个受影响行业，查询近7日资金流入Top3个股"""
    # 行业别名映射（AI行业名 → 数据库行业名）
    INDUSTRY_ALIAS = {
        "传感器": "电子",
        "电子元件": "电子",
        "连接器": "电子",
        "PCB": "电子",
        "被动元件": "电子",
        "AI算力": "计算机",
        "光模块": "通信",
        "服务器": "计算机",
        "消费电子": "消费电子",
        "汽车电子": "汽车",
        "半导体设备": "半导体",
        "半导体材料": "半导体",
    }

    industries = industries_json.get("industries", []) if isinstance(industries_json, dict) else []
    result = {}
    for ind in industries:
        ind_name = ind.get("name", "")
        if not ind_name:
            continue
        # 提取行业名主要部分（去掉括号内容）
        import re
        main_name = re.sub(r'[（(].*[）)]', '', ind_name).strip()
        if not main_name:
            main_name = ind_name

        # 构建匹配关键词列表
        keywords = [main_name, ind_name[:4]]
        # 添加别名映射
        for alias, db_name in INDUSTRY_ALIAS.items():
            if alias in main_name or alias in ind_name:
                keywords.append(db_name)

        # 构建 OR 条件
        conditions = []
        params = []
        for kw in set(keywords):
            conditions.append("si.industry_l1 LIKE ?")
            conditions.append("si.industry_l2 LIKE ?")
            params.extend([f"%{kw}%", f"%{kw}%"])

        stocks = execute_query(
            f"""SELECT si.stock_code, si.stock_name, si.industry_l1, si.industry_l2,
                       COALESCE(SUM(cf.main_net_inflow), 0) as total_inflow_7d
                FROM stock_info si
                LEFT JOIN capital_flow cf ON si.stock_code = cf.stock_code
                    AND cf.trade_date >= (SELECT DISTINCT trade_date FROM capital_flow ORDER BY trade_date DESC LIMIT 1 OFFSET 6)
                WHERE {' OR '.join(conditions)}
                GROUP BY si.stock_code, si.stock_name, si.industry_l1, si.industry_l2
                ORDER BY total_inflow_7d DESC
                LIMIT 3""",
            params,
        )
        if stocks:
            result[ind_name] = {
                "chain_position": ind.get("chain_position", ""),
                "impact_type": ind.get("impact_type", "direct"),
                "stocks": [dict(s) for s in stocks],
            }
    return result


def _rank_stocks_with_skill(candidates, group_logic):
    """合并所有行业候选股，调用 Claude 按预期收益排序
    对每只候选股调 hybrid_search 注入近期事件作为 recent_events
    """
    if not candidates:
        return []

    # 为每只股票注入 recent_events（hybrid_search top_k=3）
    try:
        from retrieval.hybrid import hybrid_search
        for s in candidates[:30]:
            stock_name = s.get("stock_name") or s.get("stock_code", "")
            query = f"{stock_name} {group_logic[:20]}"
            try:
                hr = hybrid_search(query, top_k=3)
                recent_texts = []
                for chunk in (hr.chunks or [])[:3]:
                    if chunk.text:
                        recent_texts.append(chunk.text[:80])
                s["recent_events"] = "；".join(recent_texts) if recent_texts else ""
            except Exception:
                s["recent_events"] = ""
    except Exception as e:
        logger.warning(f"recent_events 注入失败（不影响主流程）: {e}")

    # 构建候选股文本（含 recent_events）
    lines = []
    for s in candidates:
        inflow = (s.get('total_inflow_7d', 0) or s.get('total_inflow', 0) or 0) / 1e4
        recent = s.get("recent_events", "")
        line = (f"- {s['stock_code']} {s.get('stock_name', '')}（{s.get('industry_l2', '')}）"
                f" 7日资金净流入: {inflow:.0f}万")
        if recent:
            line += f" 近期: {recent}"
        lines.append(line)
    stock_text = "\n".join(lines)

    prompt = STOCK_RANKING_PROMPT.format(group_logic=group_logic)

    try:
        ranked = call_claude_json(prompt, stock_text, max_tokens=3000)
        if isinstance(ranked, list):
            ranked_map = {r["stock_code"]: r for r in ranked if r.get("stock_code")}
            for s in candidates:
                r = ranked_map.get(s["stock_code"], {})
                s["recommend_tags"] = r.get("reason", "")
                s["recommend_logic"] = r.get("logic", "")
            return candidates
    except Exception as e:
        logger.warning(f"个股排序分析失败: {e}")

    return candidates


def _build_context(tags_str, group_logic, news_data, stock_flows, kg_industries):
    """构建 Claude 分析上下文（含 KG 子图 + 因果链）"""
    context = f"标签组: {tags_str}\n投资逻辑: {group_logic}\n\n"

    context += "=== 关联新闻摘要 ===\n"
    for n in news_data[:15]:
        icon = {"positive": "利好", "negative": "利空"}.get(n.get("sentiment"), "中性")
        context += f"- [{icon}][重要性{n.get('importance', 0)}] {n.get('summary', '')}\n"

    # KG 子图提取：找到 theme 实体，提取周围子图
    tags = [t.strip() for t in tags_str.split(",")]
    theme_ids = []
    for tag in tags:
        ents = execute_query(
            "SELECT id FROM kg_entities WHERE entity_type='theme' AND entity_name LIKE ?",
            [f"%{tag}%"],
        )
        theme_ids.extend(e["id"] for e in ents)

    if theme_ids:
        try:
            subgraph = extract_context_subgraph(
                theme_ids[:10], depth=2,
                categories=["causal", "structural", "element"],
                max_nodes=40,
            )
            if subgraph.get("text"):
                context += f"\n{subgraph['text']}\n"
        except Exception as e:
            logger.warning(f"KG子图提取失败: {e}")

        # 因果链：从每个 theme 实体追踪下游传导
        causal_lines = []
        for tid in theme_ids[:5]:
            try:
                chains = trace_causal_chain(tid, max_depth=3, direction="downstream")
                for chain in chains[:3]:
                    steps = []
                    for step in chain:
                        if isinstance(step, dict) and "entity" in step:
                            steps.append(step["entity"].get("entity_name", "?"))
                        else:
                            name = step.get("tgt_name", "?")
                            rel = step.get("relation_type", "?")
                            steps.append(f"--[{rel}]--> {name}")
                    if len(steps) > 1:
                        causal_lines.append(" ".join(steps))
            except Exception:
                pass

        if causal_lines:
            context += "\n=== 因果传导链 ===\n"
            for line in causal_lines[:8]:
                context += f"- {line}\n"
    elif kg_industries:
        # 降级：无 theme 实体时用简单行业列表
        context += "\n=== 知识图谱关联行业 ===\n"
        for ind in kg_industries[:10]:
            context += f"- {ind['name']}（关系: {ind['relation']}，方向: {ind['direction']}）\n"

    if stock_flows:
        context += "\n=== 关联个股资金Top5 ===\n"
        for sf in stock_flows[:5]:
            context += (f"- {sf['stock_code']} {sf['stock_name']}"
                        f"（{sf.get('industry_l1', '')}）: "
                        f"净流入 {sf.get('total_inflow', 0)/1e4:.0f}万\n")

    return context


def _generate_stock_rationales(top10: list, group_logic: str) -> list:
    """AI 生成每只推荐个股的投资逻辑和风险点"""
    # 收集所有股票
    all_stocks = []
    for grp in top10:
        for s in grp.get("stocks", []):
            all_stocks.append(s)

    if not all_stocks:
        return top10

    # 构建输入：股票列表
    stock_text = "\n".join(
        f"- {s['stock_code']} {s.get('stock_name', '')}（{s.get('industry_l2', '')}）"
        f" 资金净流入: {(s.get('total_inflow', 0) or 0)/1e4:.0f}万"
        for s in all_stocks
    )

    prompt = STOCK_RATIONALE_PROMPT.format(group_logic=group_logic)

    try:
        rationales = call_claude_json(prompt, stock_text, max_tokens=3000)
        if isinstance(rationales, list):
            rationale_map = {r["stock_code"]: r for r in rationales if r.get("stock_code")}
            for grp in top10:
                for s in grp.get("stocks", []):
                    r = rationale_map.get(s["stock_code"], {})
                    s["reason"] = r.get("reason", "")
                    s["risk"] = r.get("risk", "")
    except Exception as e:
        logger.warning(f"个股投资逻辑生成失败: {e}")

    return top10


def _generate_group_logic(tags):
    """为缺少成组逻辑的标签组生成投资逻辑"""
    tags_str = " + ".join(tags)
    prompt = f"""你是A股投资策略分析师。以下标签组合来自近期市场热点数据：
{tags_str}

请分析这组标签构成的投资主线，输出一段话（100-200字）说明投资逻辑——
为什么这些标签有关联？产业链/政策/技术上的传导关系是什么？投资机会在哪里？

直接输出逻辑文本，不要JSON，不要标题。"""
    try:
        result = call_claude("", prompt, max_tokens=500, timeout=60)
        if result and len(result) > 20:
            return result.strip()
    except Exception as e:
        logger.warning(f"生成标签组投资逻辑失败: {e}")
    return None


def _call_json_with_retry(system_prompt, user_message, max_tokens=2000, retries=1):
    """调用 Claude JSON 接口，失败时追加强调 JSON 指令重试"""
    try:
        return call_claude_json(system_prompt, user_message, max_tokens=max_tokens)
    except (ValueError, json.JSONDecodeError) as e:
        if retries <= 0:
            raise
        logger.warning(f"JSON 解析失败，重试并强调 JSON 输出: {e}")
        reinforced = system_prompt + "\n\n⚠️ 重要：你必须只输出 JSON，不要输出任何其他文字、解释或标题。直接以 { 开头。"
        return call_claude_json(reinforced, user_message, max_tokens=max_tokens)


def _inject_critical_thinking(prompt: str) -> str:
    """在 prompt 末尾注入批判性思维检查清单"""
    ct_skill = get_skill_content("stock-analysis-critical-thinking")
    if ct_skill:
        # 提取清单正文（跳过 YAML front-matter）
        lines = ct_skill.split("\n")
        content_lines = []
        in_frontmatter = False
        for line in lines:
            if line.strip() == "---":
                in_frontmatter = not in_frontmatter
                continue
            if not in_frontmatter:
                content_lines.append(line)
        content = "\n".join(content_lines).strip()
        if content and content != "## 分析时必须检查的问题清单":
            prompt += f"\n\n## 批判性思维检查清单\n{content}\n请确保你的分析已考虑以上所有检查点。"
    return prompt
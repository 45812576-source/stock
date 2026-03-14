"""估值工具 — DeepSeek tool_use 函数定义 + 实现"""
import json
import logging

logger = logging.getLogger(__name__)

# ── Tool 定义列表（OpenAI function calling 格式）────────────────────────────────

VALUATION_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_driver_expectation",
            "description": (
                "从知识库(content_summaries + kg)搜索某个驱动因素的未来预期值。"
                "返回按置信度排序的预期数据、来源引用原文。"
                "用于获取如'全球AI服务器出货量2026年预期'这类数据。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "driver_name": {
                        "type": "string",
                        "description": "驱动因素名称，如'AI服务器出货量'",
                    },
                    "periods": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "预期年份，如['2026','2027','2028']",
                    },
                    "search_keywords": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "辅助搜索关键词，用于扩大搜索范围",
                    },
                },
                "required": ["driver_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_kg_company_context",
            "description": (
                "从知识图谱获取公司的完整上下文："
                "行业归属、成本结构、收入结构、关联主题、供应链关系。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "stock_code": {
                        "type": "string",
                        "description": "股票代码，如'000001'",
                    },
                    "focus_areas": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "关注领域，如['cost_elements','revenue_elements','supply_chain']",
                    },
                },
                "required": ["stock_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "validate_capacity_growth",
            "description": (
                "验证公司产能是否能支撑预期的收入增长。"
                "搜索产能相关的新闻、公告、知识图谱数据。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "stock_code": {"type": "string", "description": "股票代码"},
                    "segment_name": {"type": "string", "description": "业务分部名称"},
                    "expected_revenue_growth_pct": {
                        "type": "number",
                        "description": "预期收入增速百分比，如50表示50%",
                    },
                    "growth_period": {
                        "type": "string",
                        "description": "增长期间，如'2025-2027'",
                    },
                },
                "required": ["stock_code", "segment_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_peer_multiples",
            "description": (
                "获取同行业可比公司的估值倍数(PE/PB/PS/EV-EBITDA)。"
                "用于确定分部估值的参考倍数区间。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "stock_code": {
                        "type": "string",
                        "description": "目标公司代码，用于确定行业",
                    },
                    "segment_type": {
                        "type": "string",
                        "description": "业务类型，如'AI服务器'，用于精确匹配",
                    },
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "需要的指标，如['PE','PS','EV_EBITDA']",
                    },
                },
                "required": ["stock_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_constraint_factors",
            "description": (
                "搜索影响估值的限制因素：技术限制、政策限制、产能瓶颈、竞争威胁等。"
                "从知识图谱和新闻中检索。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "stock_code": {"type": "string", "description": "股票代码"},
                    "segment_name": {
                        "type": "string",
                        "description": "业务分部名称（可选，不填则搜全公司）",
                    },
                    "factor_types": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "因素类型，如['technology','policy','capacity','competition']",
                    },
                },
                "required": ["stock_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "explore_kg_graph",
            "description": (
                "从指定实体出发遍历知识图谱，发现关联的上下游、需求来源、供给因素、"
                "成本传导等关系网络。用于理解驱动因素的完整传导链条。"
                "例如：搜索'铜'可发现 铜←demand_source_of←新能源车/光伏/电网，"
                "然后再用search_driver_expectation搜索各下游的具体预期数据。"
                "支持按关系类型过滤，如只看demand_source_of/supply_driven/cost_transmission。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_name": {
                        "type": "string",
                        "description": "起始实体名称，如'铜'、'AI服务器'、'光伏'",
                    },
                    "depth": {
                        "type": "integer",
                        "description": "遍历深度，1=直接邻居，2=两跳。默认2",
                    },
                    "relation_types": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "只遍历这些关系类型（可选）。常用类型："
                            "demand_source_of, demand_driven, supply_driven, "
                            "cost_affected_by, cost_transmission, revenue_affected_by, "
                            "causes_positive, causes_negative, supplier_of, customer_of, "
                            "belongs_to_chain, leading_indicator_of"
                        ),
                    },
                },
                "required": ["entity_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_peer_valuation",
            "description": (
                "通过RAG检索搜索可比公司估值信息、研报评级、目标价等数据。"
                "优先复用已检索的上游数据，补充未能覆盖的同行比较数据。"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "stock_code": {
                        "type": "string",
                        "description": "股票代码",
                    },
                    "query": {
                        "type": "string",
                        "description": "检索查询词，如'可比公司 估值 PE 研报评级'",
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "返回结果数量，默认8",
                    },
                },
                "required": ["stock_code"],
            },
        },
    },
]


# ── Tool 实现函数 ─────────────────────────────────────────────────────────────

def _search_driver_expectation(driver_name: str, periods: list = None,
                                search_keywords: list = None) -> dict:
    """搜索驱动因素预期值 — KG图遍历扩展搜索

    1. 先用 KG 图遍历找到驱动因素的关联实体（需求来源、供给因素等）
    2. 用扩展后的关键词搜索 content_summaries
    3. 优先返回含具体数字的 fact_summary
    """
    import re
    from knowledge_graph.kg_query import search_entities, search_content_summaries, explore_kg_graph

    results = []

    # 1. KG 图遍历：发现关联实体作为扩展搜索词
    expanded_keywords = set()
    expanded_keywords.add(driver_name)
    if search_keywords:
        expanded_keywords.update(search_keywords)

    try:
        # 用估值相关的关系类型遍历
        graph = explore_kg_graph(
            driver_name, depth=1,
            relation_types=[
                "demand_source_of", "demand_driven", "supply_driven",
                "cost_affected_by", "cost_transmission", "revenue_affected_by",
                "causes_positive", "causes_negative", "belongs_to_chain",
                "leading_indicator_of", "supplier_of", "customer_of",
            ],
            max_nodes=20,
        )
        if graph.get("edges"):
            for edge in graph["edges"]:
                # 收集关联实体名作为扩展搜索词
                for name in [edge.get("source_name", ""), edge.get("target_name", "")]:
                    if name and name != driver_name and len(name) >= 2:
                        expanded_keywords.add(name)
            # 把图结构本身也作为结果返回，让 LLM 看到传导链
            results.append({
                "value": "",
                "unit": "",
                "period": "结构",
                "confidence": "high",
                "source": "知识图谱关系网络",
                "source_quote": "; ".join(
                    f"{e['source_name']}--{e['relation']}-->{e['target_name']}"
                    for e in graph["edges"][:15]
                ),
                "data_type": "kg_graph",
            })
    except Exception as e:
        logger.debug(f"KG图遍历失败: {e}")

    # 2. KG 实体搜索（补充描述信息）
    try:
        kg_entities = search_entities(driver_name, limit=3)
        for ent in (kg_entities or []):
            desc = ent.get("description", "")
            if desc:
                results.append({
                    "value": desc,
                    "unit": "",
                    "period": "当前",
                    "confidence": "medium",
                    "source": f"知识图谱实体: {ent.get('entity_name', '')}",
                    "source_quote": desc[:200],
                    "data_type": "kg_entity",
                })
    except Exception as e:
        logger.debug(f"KG实体搜索失败: {e}")

    # 3. content_summaries 搜索（用扩展后的关键词）
    # 分批搜索：先搜原始驱动因素，再搜扩展关键词
    seen_ids = set()
    all_summaries = []

    try:
        # 第一批：原始关键词（高相关性）
        primary_kws = [driver_name]
        if search_keywords:
            primary_kws.extend(search_keywords[:2])
        s1 = search_content_summaries(keywords=primary_kws, periods=periods or [], limit=5)
        for s in (s1 or []):
            if s["id"] not in seen_ids:
                seen_ids.add(s["id"])
                all_summaries.append(s)

        # 第二批：KG扩展关键词（间接相关性）
        extra_kws = [kw for kw in expanded_keywords if kw != driver_name and kw not in (search_keywords or [])]
        if extra_kws:
            # 每次搜2-3个关键词，避免 OR 太宽
            for i in range(0, min(len(extra_kws), 6), 2):
                batch = extra_kws[i:i+2]
                s2 = search_content_summaries(keywords=batch, periods=periods or [], limit=3)
                for s in (s2 or []):
                    if s["id"] not in seen_ids:
                        seen_ids.add(s["id"])
                        all_summaries.append(s)
    except Exception as e:
        logger.debug(f"content_summaries搜索失败: {e}")

    # 4. 排序：有 fact_summary 且含数字的优先
    HAS_NUMBER = re.compile(r'\d+\.?\d*')

    def _score(s):
        fact = s.get("fact_summary") or ""
        summary = s.get("summary") or ""
        score = 0
        # fact_summary 非空 +2
        if fact:
            score += 2
        # 含数字 +3（有具体预测数据）
        text = fact or summary
        if HAS_NUMBER.search(text):
            score += 3
        # 含年份匹配 +2
        for p in (periods or []):
            if p in text:
                score += 2
                break
        # 含驱动因素名 +1
        if driver_name in text:
            score += 1
        return score

    all_summaries.sort(key=_score, reverse=True)

    for s in all_summaries[:8]:
        quote = (s.get("fact_summary") or s.get("summary") or "")[:300]
        results.append({
            "value": "",
            "unit": "",
            "period": ",".join(periods) if periods else "未指定",
            "confidence": "high" if _score(s) >= 5 else "medium" if _score(s) >= 2 else "low",
            "source": s.get("summary_title") or "知识库",
            "source_quote": quote,
            "data_type": "content_summary",
            "family": s.get("family", ""),
        })

    if not results:
        return {
            "driver_name": driver_name,
            "periods": periods,
            "data_available": False,
            "message": f"未找到关于'{driver_name}'的预期数据，请使用保守估计",
            "expanded_keywords": list(expanded_keywords),
            "results": [],
        }

    return {
        "driver_name": driver_name,
        "periods": periods,
        "data_available": True,
        "expanded_keywords": list(expanded_keywords)[:15],
        "results": results[:12],
    }


def _get_kg_company_context(stock_code: str, focus_areas: list = None) -> dict:
    """获取公司知识图谱上下文"""
    from knowledge_graph.kg_query import get_company_context
    try:
        ctx = get_company_context(stock_code)
        if not ctx:
            return {"error": f"未找到股票 {stock_code} 的知识图谱数据", "stock_code": stock_code}

        result = {
            "stock_code": stock_code,
            "entity": ctx.get("entity", {}),
            "industries": ctx.get("industries", []),
            "text_summary": ctx.get("text", ""),
        }

        focus = focus_areas or ["cost_elements", "revenue_elements", "supply_chain", "themes"]
        if "cost_elements" in focus:
            result["cost_elements"] = ctx.get("cost_elements", [])
        if "revenue_elements" in focus:
            result["revenue_elements"] = ctx.get("revenue_elements", [])
        if "supply_chain" in focus:
            result["supply_chain"] = ctx.get("supply_chain", [])
        if "themes" in focus:
            result["themes"] = ctx.get("themes", [])

        return result
    except Exception as e:
        return {"error": str(e), "stock_code": stock_code}


def _validate_capacity_growth(stock_code: str, segment_name: str,
                               expected_revenue_growth_pct: float = None,
                               growth_period: str = None) -> dict:
    """验证产能能否支撑增长"""
    from knowledge_graph.kg_query import get_company_context, search_content_summaries

    capacity_info = []
    can_support = None
    detail = ""

    # 1. 从KG获取产能关系
    try:
        ctx = get_company_context(stock_code)
        if ctx:
            for rel in (ctx.get("supply_chain") or []):
                if any(kw in str(rel.get("entity_name", "")) for kw in ["产能", "工厂", "产线", "扩产"]):
                    capacity_info.append({
                        "type": "kg_supply_chain",
                        "entity": rel.get("entity_name"),
                        "relation": rel.get("relation_type"),
                    })
    except Exception as e:
        logger.debug(f"产能KG查询失败: {e}")

    # 2. 搜索产能相关内容
    try:
        keywords = [stock_code, segment_name]
        cap_keywords = ["产能", "扩产", "新建", "投产"]
        summaries = search_content_summaries(
            keywords=keywords + cap_keywords,
            limit=5,
        )
        for s in (summaries or []):
            text = s.get("summary", "") or ""
            if any(kw in text for kw in cap_keywords):
                capacity_info.append({
                    "type": "news",
                    "title": s.get("summary_title", ""),
                    "quote": text[:200],
                })
    except Exception as e:
        logger.debug(f"产能新闻搜索失败: {e}")

    if capacity_info:
        detail = f"找到 {len(capacity_info)} 条产能相关信息"
        can_support = None  # 需要 LLM 判断
    else:
        detail = "未找到产能相关信息，无法验证"
        can_support = None

    return {
        "stock_code": stock_code,
        "segment_name": segment_name,
        "expected_growth_pct": expected_revenue_growth_pct,
        "growth_period": growth_period,
        "can_support": can_support,
        "detail": detail,
        "capacity_evidence": capacity_info[:5],
        "note": "需要结合具体数据判断，此处仅提供原始证据",
    }


def _get_peer_multiples(stock_code: str, segment_type: str = None,
                        metrics: list = None) -> dict:
    """获取同行估值倍数"""
    from research.universal_db import get_peer_comparison
    from utils.db_utils import execute_query

    try:
        peers_raw = get_peer_comparison(stock_code)
        if not peers_raw:
            return {
                "stock_code": stock_code,
                "error": "未找到同行数据",
                "peers": [],
                "statistics": {},
            }

        peers_data = []
        for peer in peers_raw[:10]:
            peer_code = peer.get("stock_code")
            if not peer_code:
                continue

            # 获取财务数据计算倍数
            fin = execute_query(
                """SELECT revenue, net_profit, eps FROM financial_reports
                   WHERE stock_code=%s ORDER BY report_period DESC LIMIT 1""",
                [peer_code],
            )
            price = peer.get("close") or 0
            market_cap = (peer.get("market_cap") or 0) * 1e8  # 亿→元

            pe = pb = ps = None
            if fin and price:
                eps = fin[0].get("eps") or 0
                if eps and eps > 0:
                    pe = round(price / eps, 1)
                revenue = fin[0].get("revenue") or 0
                if revenue and revenue > 0 and market_cap:
                    ps = round(market_cap / revenue, 2)

            peers_data.append({
                "stock_code": peer_code,
                "stock_name": peer.get("stock_name", ""),
                "market_cap_bn": peer.get("market_cap"),
                "pe": pe,
                "pb": pb,
                "ps": ps,
            })

        # 计算中位数
        pe_list = [p["pe"] for p in peers_data if p.get("pe") and p["pe"] > 0]
        ps_list = [p["ps"] for p in peers_data if p.get("ps") and p["ps"] > 0]

        def _median(lst):
            if not lst:
                return None
            s = sorted(lst)
            n = len(s)
            return round(s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2, 2)

        def _percentile75(lst):
            if not lst:
                return None
            s = sorted(lst)
            idx = int(len(s) * 0.75)
            return round(s[min(idx, len(s) - 1)], 2)

        return {
            "stock_code": stock_code,
            "segment_type": segment_type,
            "peers": peers_data,
            "statistics": {
                "pe_median": _median(pe_list),
                "pe_75th": _percentile75(pe_list),
                "pe_count": len(pe_list),
                "ps_median": _median(ps_list),
                "ps_75th": _percentile75(ps_list),
                "ps_count": len(ps_list),
            },
        }
    except Exception as e:
        return {"stock_code": stock_code, "error": str(e), "peers": [], "statistics": {}}


def _search_constraint_factors(stock_code: str, segment_name: str = None,
                                factor_types: list = None) -> dict:
    """搜索估值限制因素"""
    from knowledge_graph.kg_query import get_company_context, search_content_summaries

    factors = []

    # 1. 从KG获取风险关系（hurts / risk_factor）
    try:
        ctx = get_company_context(stock_code)
        if ctx:
            for rel in (ctx.get("supply_chain") or []):
                rt = rel.get("relation_type", "")
                if rt in ("hurts", "risk_factor", "substitute_threat"):
                    factors.append({
                        "factor": rel.get("entity_name", ""),
                        "type": rt,
                        "severity": "medium",
                        "source": "知识图谱",
                        "quote": "",
                    })
    except Exception as e:
        logger.debug(f"KG风险因素查询失败: {e}")

    # 2. 搜索风险相关内容
    try:
        risk_keywords = ["风险", "限制", "瓶颈", "挑战", "压力"]
        base_keywords = [stock_code]
        if segment_name:
            base_keywords.append(segment_name)
        summaries = search_content_summaries(
            keywords=base_keywords + risk_keywords,
            limit=6,
        )
        for s in (summaries or []):
            text = s.get("summary", "") or ""
            if any(kw in text for kw in risk_keywords):
                factors.append({
                    "factor": s.get("summary_title", ""),
                    "type": "news_risk",
                    "severity": "medium",
                    "source": s.get("summary_title", "知识库"),
                    "quote": text[:200],
                })
    except Exception as e:
        logger.debug(f"风险因素新闻搜索失败: {e}")

    return {
        "stock_code": stock_code,
        "segment_name": segment_name,
        "factors": factors[:10],
        "total_found": len(factors),
    }


def _explore_kg_graph(entity_name: str, depth: int = 2,
                       relation_types: list = None) -> dict:
    """遍历知识图谱关系网络"""
    from knowledge_graph.kg_query import explore_kg_graph
    try:
        result = explore_kg_graph(
            entity_name=entity_name,
            depth=min(depth or 2, 3),
            relation_types=relation_types,
            max_nodes=40,
        )
        # 精简输出：去掉 id，只保留 LLM 需要的信息
        if result.get("nodes"):
            result["nodes"] = [
                {"name": n["name"], "type": n["type"]}
                for n in result["nodes"]
            ]
        if result.get("root"):
            result["root"] = {"name": result["root"]["name"], "type": result["root"]["type"]}
        return result
    except Exception as e:
        return {"error": str(e), "entity_name": entity_name}


def _search_peer_valuation(stock_code: str, query: str = "", top_k: int = 8) -> dict:
    """搜索可比公司估值、研报评级等信息（RAG检索）"""
    from research.rag_context import search_stock_context
    try:
        if not query:
            query = "可比公司 估值 PE PS 研报 评级 目标价"
        ctx = search_stock_context(stock_code, query, top_k=top_k)
        return {
            "data_available": bool(ctx),
            "context": ctx or "无相关数据",
            "source": "RAG检索（content_summaries + cleaned_items）",
        }
    except Exception as e:
        return {"data_available": False, "error": str(e)}


# ── 统一执行入口 ──────────────────────────────────────────────────────────────

def execute_tool(name: str, args: dict) -> str:
    """统一tool执行入口，返回JSON字符串"""
    handlers = {
        "search_driver_expectation": _search_driver_expectation,
        "get_kg_company_context": _get_kg_company_context,
        "validate_capacity_growth": _validate_capacity_growth,
        "get_peer_multiples": _get_peer_multiples,
        "search_constraint_factors": _search_constraint_factors,
        "explore_kg_graph": _explore_kg_graph,
        "search_peer_valuation": _search_peer_valuation,
    }
    handler = handlers.get(name)
    if not handler:
        return json.dumps({"error": f"Unknown tool: {name}"}, ensure_ascii=False)
    try:
        result = handler(**args)
        return json.dumps(result, ensure_ascii=False, default=str)
    except Exception as e:
        logger.error(f"Tool '{name}' 执行失败: {e}")
        return json.dumps({"error": str(e), "tool": name}, ensure_ascii=False)

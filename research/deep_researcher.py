"""深度研究主流程 — 6板块个股分析"""
import json
import logging
from pathlib import Path
from utils.model_router import call_model as _call_model, call_model_json as _call_model_json

def call_claude(system_prompt, user_message, max_tokens=4096, timeout=900, **kwargs):
    return _call_model('research', system_prompt, user_message, max_tokens=max_tokens, timeout=timeout)

def call_claude_json(system_prompt, user_message, max_tokens=4096, timeout=900):
    return _call_model_json('research', system_prompt, user_message, max_tokens=max_tokens, timeout=timeout)
from utils.db_utils import execute_insert, execute_query
from research.universal_db import (
    get_stock_profile, get_stock_technical_summary,
    get_peer_comparison, get_industry_data, get_macro_data,
)
from knowledge_graph.kg_query import get_company_context
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


# ==================== 个股深度研究（6板块分析） ====================

STEP1_BUSINESS_MODEL_PROMPT = """你是资深A股投资分析师。请基于以下个股数据，分析该公司的商业模式。

重要规则：
- 如果"事实锚点数据"中提供了分业务收入/成本数据，你必须以这些数据为准，不得编造与之矛盾的数字
- 如果没有提供事实锚点数据，可以基于公开信息合理推算，但必须在对应字段标注"推算"
- 所有占比之和应约等于100%

请输出严格的JSON格式：
{
    "value_proposition": "价值主张：公司为客户解决什么问题/提供什么价值（80字）",
    "key_partners": "关键合作伙伴：核心供应商、战略联盟、合资方（80字）",
    "key_activities": "关键活动：公司最核心的业务活动（80字）",
    "key_resources": "关键资源：核心竞争力所依赖的资源（80字）",
    "customers": "目标客户群体描述（80字）",
    "customer_relationships": "客户关系：维护客户关系的方式（80字）",
    "channels": "主要销售渠道和触达方式（80字）",
    "revenue_sources": "收入来源和盈利模式（80字）",
    "cost_structure": "主要成本构成（80字）",
    "revenue_segments_current": [
        {"name": "业务板块名", "pct": 占比百分比数字, "amount": 金额亿元或null, "trend": "增长/稳定/下降", "source": "公告/推算"}
    ],
    "revenue_segments_prev": [
        {"name": "业务板块名", "pct": 占比百分比数字, "amount": 金额亿元或null, "source": "公告/推算"}
    ],
    "revenue_period_current": "最近一期报告期（如2024H1）",
    "revenue_period_prev": "上一期报告期（如2023H1）",
    "cost_breakdown_current": [
        {"name": "成本项名", "pct": 占比百分比数字, "amount": 金额亿元或null, "source": "公告/推算"}
    ],
    "cost_breakdown_prev": [
        {"name": "成本项名", "pct": 占比百分比数字, "amount": 金额亿元或null, "source": "公告/推算"}
    ],
    "customer_industries": [
        {"name": "客户行业名", "pct": 占比百分比数字}
    ],
    "topline_drivers": [
        {
            "name": "驱动因素名",
            "direction": "正向/负向（这是结构性影响方向，不是当前变化方向）",
            "impact_pct": 对营收影响的百分比估算数字,
            "description": "如何影响营收（50字）",
            "current_trend": "上行/下行/平稳（该驱动因素当前的实际变动方向，基于最新数据）",
            "supporting": "支撑current_trend判断的具体证据（如价格数据、出货量数据、政策变化等，30字内，无数据则填null）"
        }
    ],
    "bottomline_drivers": [
        {
            "name": "驱动因素名",
            "direction": "正向/负向（结构性影响方向）",
            "impact_pct": 对成本影响的百分比估算数字,
            "description": "如何影响成本/利润（50字）",
            "current_trend": "上行/下行/平稳（该驱动因素当前的实际变动方向）",
            "supporting": "支撑current_trend判断的具体证据（30字内，无数据则填null）"
        }
    ],
    "_for_valuation": {
        "segment_drivers": [
            {
                "segment_name": "分部名称（对应revenue_segments中的name）",
                "segment_pct": 占比百分比数字,
                "drivers": [
                    {
                        "driver_name": "驱动因素名称",
                        "driver_type": "demand/supply/price",
                        "quantity_metric": "量指标名称（如出货量万台/年）",
                        "quantity_latest_value": 数值或null,
                        "quantity_source": "数据来源（如IDC报告/公司公告/行业推算）",
                        "price_metric": "价指标名称（如单台均价万元）",
                        "price_latest_value": 数值或null,
                        "price_source": "数据来源",
                        "company_share_pct": 公司市场份额百分比或null,
                        "share_source": "份额数据来源"
                    }
                ],
                "cost_drivers": [
                    {
                        "cost_item": "成本项名称",
                        "pct_of_segment_cost": 占分部成本百分比或null,
                        "external_driver": "外部价格驱动因素名称",
                        "driver_unit": "驱动因素单位",
                        "latest_value": 数值或null,
                        "value_source": "数据来源"
                    }
                ]
            }
        ]
    },
    "revenue_composition_insight": "结合两期收入结构对比，分析收入构成变化的业务含义（100字）",
    "cost_composition_insight": "结合两期成本结构对比，分析成本构成变化的业务含义（100字）"
}

要求：
- BMC 9格字段（value_proposition/key_partners/key_activities/key_resources/customers/customer_relationships/channels/revenue_sources/cost_structure）每个80字以内
- revenue_segments_current/prev: 拆解最近两期收入结构，列出2-5个主要业务板块及占比
- cost_breakdown_current/prev: 拆解最近两期成本结构，列出2-5个主要成本项及占比
- 如果"事实锚点数据"中有分业务数据，必须与之一致；没有则标注source为"推算"
- customer_industries: 列出2-5个主要客户行业及占比
- topline_drivers: 3-5个影响营收的关键驱动因素，每个驱动因素对营收的潜在影响必须超过20%，不得列入边缘性或次要因素
  - direction: 该因素对营收的结构性影响（正向=该因素增大则营收增加，负向=该因素增大则营收减少），不等于当前在涨还是在跌
  - current_trend: 该驱动因素当前实际的变动方向（上行/下行/平稳），必须基于可见数据
  - supporting: 支撑current_trend的具体证据，如有数据请引用（无数据不能瞎填，填null）
- bottomline_drivers: 3-5个影响成本/利润的关键驱动因素，每个驱动因素对成本的潜在影响必须超过20%，不得列入边缘性或次要因素
  - direction/current_trend/supporting 同上逻辑
- _for_valuation.segment_drivers: 为每个revenue_segments分部穷举收入驱动因素(量×价)
  - driver_type: demand(需求端)/supply(供给端)/price(价格端)
  - 必须标注数据来源(xxx_source)，如果数据不足标注"推算"
  - company_share_pct: 公司在该驱动因素对应市场中的份额
  - cost_drivers: 该分部的主要成本项及其外部价格驱动因素
  - 如果某个驱动因素无法量化，仍然列出并设值为null
- 所有描述要简洁，重结论轻过程

【驱动因素约束（来自financial-model-construction + company-valuation skill）】
- topline_drivers: 每个driver必须是Revenue Model Deep Dive框架中的一级驱动因素（volume/price/mix/market_share），不得列入二级或边缘因素
- bottomline_drivers: 每个driver必须是Cost Structure Analysis框架中的一级成本驱动（COGS材料/人工/折旧，或OpEx R&D/S&M/G&A），不得列入管理费用小项
- _for_valuation.segment_drivers: 必须按 Unit Economics 框架穷举（ASP×Volume，标注CAC/LTV如适用）
- impact_pct 门槛20%不变
- revenue_composition_insight: 对比revenue_segments_current和prev，解读收入结构变化的业务含义
- cost_composition_insight: 对比cost_breakdown_current和prev，解读成本结构变化的业务含义"""

STEP2_VALUE_CHAIN_PROMPT = """你是资深行业分析师。请基于以下行业和公司数据，进行产业链深度拆解。

严格规则：
- 严格基于以下提供的数据回答，禁止编造数据点、新闻标题、公司名称或统计数字
- industry_data 中每个数据点必须标注 source 字段；如数据不足，明确标注"数据不足"而非编造
- industry_news 只能引用 user_message 中提供的新闻，不得编造新闻标题/来源
- tech_barrier.tech_routes 中的 stage 只能是以下枚举值之一：实验室/测试验证/小规模商业化/量产

请输出严格的JSON格式：
{
    "upstream": [
        {"label": "原材料/零部件名称", "players": "主要供应商（50字）", "description": "特征描述（60字）"}
    ],
    "midstream": [
        {"label": "中游环节名称", "players": "主要参与者（50字）", "description": "特征描述（60字）"}
    ],
    "downstream": [
        {"label": "下游应用/客户名称", "players": "主要客户群（50字）", "description": "特征描述（60字）"}
    ],
    "company_position": "upstream 或 midstream 或 downstream（兼容字段，取第一个分部的位置）",
    "position_detail": "公司在产业链中的具体定位和竞争优势（100字）",
    "segment_positions": [
        {
            "segment_name": "分部名称（对应revenue_segments中的name）",
            "chain_position": "upstream/midstream/downstream",
            "chain_node_label": "所在环节名称（与upstream/midstream/downstream数组中某个label完全一致）",
            "sub_industry": "该分部对应的细分行业名称",
            "market_size": {"value": "数值或null", "year": "年份或null", "unit": "亿元", "source": "来源", "forecast": "未来3年CAGR%或null"},
            "company_share_pct": 市场占有率数字或null,
            "growth_drivers": [
                {"name": "驱动因素（≤3个）", "description": "描述（40字）"}
            ],
            "risk_factors": [
                {"name": "风险因素（≤3个）", "description": "描述（40字）"}
            ],
            "porter_five_forces": {
                "supplier_power": {"level": "high/medium/low", "analysis": "分析（30字）"},
                "buyer_power": {"level": "high/medium/low", "analysis": "分析（30字）"},
                "new_entrants": {"level": "high/medium/low", "analysis": "分析（30字）"},
                "substitutes": {"level": "high/medium/low", "analysis": "分析（30字）"},
                "rivalry": {"level": "high/medium/low", "analysis": "分析（30字）"}
            }
        }
    ],
    "synergies": [
        {"segment_a": "分部A", "segment_b": "分部B", "type": "技术协同/客户协同/供应链协同/品牌协同", "description": "协同效应描述（60字）", "value_impact": "positive/neutral"}
    ],
    "porter_five_forces": {
        "supplier_power": {"level": "high/medium/low", "analysis": "分析（60字）"},
        "buyer_power": {"level": "high/medium/low", "analysis": "分析（60字）"},
        "new_entrants": {"level": "high/medium/low", "analysis": "分析（60字）"},
        "substitutes": {"level": "high/medium/low", "analysis": "分析（60字）"},
        "rivalry": {"level": "high/medium/low", "analysis": "分析（60字）"}
    },
    "tech_barrier": {
        "has_tech_barrier": true或false,
        "barrier_description": "技术壁垒描述（80字）",
        "tech_routes": [
            {"route_name": "技术路线名称", "stage": "实验室/测试验证/小规模商业化/量产", "difficulty": "难点（40字）", "competitors": "主要竞争者"}
        ],
        "commercialization_cycle": "商业化周期描述（如：实验室→测试→小规模商业化→量产）"
    },
    "industry_data": [
        {"metric": "指标名称", "value": "数值", "year": "年份", "source": "数据来源（必填）", "trend": "趋势描述"}
    ],
    "industry_news": [
        {"title": "新闻标题", "date": "日期", "source": "来源", "is_driver": true或false, "driver_indicator": "驱动的指标名称（is_driver=true时填写）", "is_recent": true或false}
    ],
    "topline_indicators": [
        {"name": "指标名", "source": "产业/宏观", "impact": "对营收的影响（50字）"}
    ],
    "bottomline_indicators": [
        {"name": "指标名", "source": "产业/宏观", "impact": "对利润的影响（50字）"}
    ],
    "_for_valuation": {
        "segment_industry_context": [
            {
                "segment_name": "分部名称（对应商业模式分析中的revenue_segments）",
                "tam": {
                    "current_year_value": 数值或null,
                    "year_1_value": 数值或null,
                    "year_2_value": 数值或null,
                    "year_3_value": 数值或null,
                    "unit": "元",
                    "growth_driver": "TAM增长驱动（50字）",
                    "data_source": "数据来源"
                },
                "supply_constraints": [
                    {
                        "constraint": "约束因素描述",
                        "impact_on_company": "对公司的影响（30字）",
                        "severity": "high/medium/low",
                        "expected_duration": "预计持续时间",
                        "source": "来源"
                    }
                ],
                "pricing_power": {
                    "level": "strong/medium/weak",
                    "upstream_bargaining": "上游议价能力描述（30字）",
                    "downstream_bargaining": "下游议价能力描述（30字）",
                    "reason": "定价权判断理由（50字）"
                },
                "competitive_position": {
                    "market_structure": "monopoly/oligopoly/competitive/fragmented",
                    "company_rank": 排名数字或null,
                    "top_3_players": ["竞争对手1", "竞争对手2", "竞争对手3"],
                    "market_share_trend": "gaining/stable/losing",
                    "share_change_driver": "份额变化驱动（30字）"
                }
            }
        ],
        "policy_regulatory_factors": [
            {
                "factor": "政策/监管因素名称",
                "impact": "positive/negative/neutral",
                "affected_segments": ["受影响分部名称"],
                "magnitude": "high/medium/low",
                "source": "来源"
            }
        ]
    }
}

要求：
- upstream/midstream/downstream 均为数组，每个环节可有多个节点（如多种原材料）
- company_position 为兼容字段，取segment_positions中第一个分部的chain_position
- segment_positions: 按公司的主要revenue_segments分部逐一分析，每个分部可能处于不同产业链环节
  - chain_node_label 必须与 upstream/midstream/downstream 数组中的 label 一一对应
  - segment_news: 只能引用user_message中提供的新闻，不得编造
  - company_share_pct: 若无数据则设null，不得编造
- synergies: 列出分部之间的协同效应，若无明显协同则设为空数组
- porter_five_forces: 整体产业链的波特五力，level 必须是 high/medium/low
- tech_barrier: 如无明显技术壁垒，has_tech_barrier=false，tech_routes 可为空数组
- industry_data: 行业关键数据，source 字段必填，无来源的数据不要填写
- industry_news: 只引用提供的新闻数据，is_recent 基于新闻日期判断（近7天内=true）
- topline_indicators/bottomline_indicators: 各3-5个指标"""

STEP3_FINANCIAL_PROMPT = """你是财务分析专家。请基于以下财务数据，进行全面的财务分析。

请输出严格的JSON格式：
{
    "revenue_trend": [
        {"period": "报告期", "revenue": 营收数字, "yoy": 同比增速百分比}
    ],
    "revenue_segments": [
        {"name": "业务板块", "revenue": 营收数字, "pct": 占比百分比, "yoy": 同比增速}
    ],
    "topline_decomposition": "营收驱动分析：量价拆解，哪些因素驱动了营收变化（150字）",
    "bottomline_decomposition": "利润驱动分析：毛利率/费用率/非经常损益变化（150字）",
    "dupont": {
        "summary": "ROE变动核心驱动（80字）",
        "periods": [
            {"period": "年度", "roe": ROE百分比, "profit_margin": 净利率百分比, "asset_turnover": 资产周转率, "equity_multiplier": 权益乘数}
        ]
    },
    "key_metrics": [
        {"name": "指标名", "value": "当前值", "trend": "改善/恶化/稳定", "comment": "简评（30字）"}
    ],
    "financial_health": "财务健康度总评（80字）",
    "_for_valuation": {
        "segment_financials": [
            {
                "segment_name": "分部名称（对应商业模式分析中的revenue_segments）",
                "latest_quarterly": {
                    "period": "如2025Q3",
                    "revenue": 数值或null,
                    "estimated_gross_margin_pct": 毛利率百分比或null
                },
                "annual_history": [
                    {"year": "2024", "revenue": 数值或null, "growth_yoy_pct": 同比增速或null}
                ],
                "driver_financial_mapping": [
                    {
                        "driver_name": "驱动因素名称（对应商业模式分析中的drivers）",
                        "period": "期间",
                        "driver_value": 驱动因素量数值或null,
                        "driver_unit": "单位",
                        "implied_asp": 隐含单价数值或null,
                        "asp_unit": "单价单位",
                        "derivation": "推导过程（如：季度收入X亿/出货量Y万台=ASP Z元/台）",
                        "data_quality": "actual/estimated"
                    }
                ]
            }
        ],
        "valuation_ready_data": {
            "latest_pe_ttm": 数值或null,
            "latest_pb": 数值或null,
            "latest_ps_ttm": 数值或null,
            "latest_ev_ebitda": 数值或null,
            "fcf_latest_annual": 数值或null,
            "net_cash_or_debt": 数值或null,
            "total_shares": 数值或null,
            "latest_price": 数值或null,
            "market_cap": 数值或null
        }
    },
    "mda_summary": "管理层讨论与分析摘要，结合已知财务数据和RAG检索信息撰写（200字）",
    "income_comparison": [
        {"item": "指标名（如营业收入/净利润/毛利率）", "periods": [{"period": "年份", "value": 数值}], "change_pct": 最新期同比变化百分比或null, "comment": "简评（30字）"}
    ],
    "balance_sheet_summary": {
        "total_assets": 总资产数值或null,
        "total_liabilities": 总负债数值或null,
        "equity": 股东权益数值或null,
        "cash_and_equivalents": 货币资金数值或null,
        "comment": "资产负债表要点（80字）"
    }
}

要求：
- revenue_trend: 最近3-4年的年度营收趋势
- revenue_segments: 如果能从数据推断业务板块拆分则列出，否则按整体列
- topline_decomposition: 分析营收变化的核心驱动（量×价，新客户/老客户，新产品/老产品等）
- bottomline_decomposition: 分析利润变化的核心驱动（毛利率变化、费用率变化、减值等）
- dupont: ROE三因子分解，按年度
- key_metrics: 3-5个关键财务指标（如毛利率、净利率、ROE、资产负债率、经营现金流等）
- 数据库中有 revenue/net_profit/roe/eps，缺 total_assets/equity，请基于可用数据推算
- _for_valuation.segment_financials: 为每个收入分部提供：
  - latest_quarterly: 最新季度的分部收入和毛利率（如有分部披露）
  - annual_history: 近2-3年的分部收入和同比增速
  - driver_financial_mapping: 将商业模式分析中的驱动因素(量×价)映射到实际财务数据
    例如：出货量40万台 × 均价8万元 = 收入32亿，与实际收入32亿吻合
  - 如没有分部披露数据，按收入占比从合并报表推算，data_quality标注"estimated"
- _for_valuation.valuation_ready_data: 从行情数据和财务数据计算各类估值倍数
- mda_summary: 结合财务数据和RAG检索到的最新公告/研报，写管理层视角的业务讨论（200字）
- income_comparison: 列出3-5个核心财务指标的多期对比（revenue_trend中的各期）
- balance_sheet_summary: 提供最新期资产负债概览，无数据字段设null"""

STEP4_VALUATION_PROMPT = """你是估值分析专家。请基于以下数据进行估值分析。

请输出严格的JSON格式：
{
    "method": "选用的主要估值方法名称（如PE估值法）",
    "method_reason": "选择该方法的原因（50字）",
    "calculation_steps": [
        {"step": "步骤描述", "formula": "计算公式或逻辑", "result": "计算结果"}
    ],
    "intrinsic_value": 内在价值数字,
    "value_range": {"low": 低估值, "high": 高估值},
    "current_price": 当前股价数字,
    "upside": "上涨/下跌空间百分比字符串",
    "comparables": [{"company": "对照公司名", "pe": PE倍数, "pb": PB倍数, "ps": PS倍数}],
    "conclusion": "估值结论（100字）"
}

要求：
- calculation_steps: 列出3-5步关键计算过程，让读者能理解内在价值是怎么算出来的
- value_range: 给出估值区间（乐观/悲观情景）
- comparables: 同行业2-4家对照公司，包含PE/PB/PS
- 如数据不足以精确计算，给出合理估算并说明假设"""

STEP6_RESEARCH_DATA_PROMPT = """你是政策与宏观研究专家。请基于以下提供的宏观指标数据和新闻，分析对公司的影响。

严格规则：
- 你只能引用以下数据中的内容，禁止编造任何数据点或新闻标题
- macro_data 只能引用 user_message 中"=== 宏观指标 ==="部分提供的指标，不得编造指标值
- policy_news 只能引用 user_message 中"=== 近期新闻 ==="部分提供的新闻，不得编造新闻标题/来源
- 如果数据不足，明确标注"数据不足"而非编造

请输出严格的JSON格式：
{
    "macro_data": [
        {"indicator": "指标名称（必须来自提供的宏观指标列表）", "value": "指标值", "date": "日期", "company_relation": "与公司的关系解读（60字）"}
    ],
    "policy_news": [
        {"title": "新闻标题（必须来自提供的新闻）", "date": "日期", "source": "来源", "company_relation": "与公司的关系解读（60字）", "direction": "positive/negative/neutral"}
    ],
    "net_assessment": "综合判断：近期宏观数据和政策动向对公司整体影响（200字）"
}

要求：
- macro_data: 只列出与公司业务有实质关联的宏观指标，每条必须说明与公司的具体关系
- policy_news: 只引用提供的新闻，direction 表示对公司的影响方向
- net_assessment: 综合宏观和政策两个维度给出判断
- 如无相关数据，对应数组可为空"""

STOCK_SYNTHESIS_PROMPT = """你是资深A股投资分析师。现在你已经完成了对一只股票的6个板块深度分析，
请基于以下各板块的分析结果，给出最终的综合评估。

请输出严格的JSON格式（只需要以下字段，不要重复各板块的详细数据）：
{
    "overall_score": 0到100的整数,
    "recommendation": "强烈推荐|推荐|中性|谨慎|回避",
    "executive_summary": "一段话总结核心投资观点，包括商业模式优劣、财务趋势、估值判断、风险提示（200字内）"
}

评分标准：80-100强烈推荐 60-79推荐 40-59中性 20-39谨慎 0-19回避"""


def _calc_sector_heat(stock_code):
    """计算板块热度 — 纯数据计算，不调用Claude"""
    # 获取行业名称
    info = execute_query(
        "SELECT industry_l1, industry_l2 FROM stock_info WHERE stock_code=?",
        [stock_code],
    )
    if not info:
        return {"heat_score": 50, "industry_flow_7d": 0, "industry_rank": "N/A",
                "stock_flow_7d": 0, "summary": "无行业数据"}

    industry = info[0].get("industry_l2") or info[0].get("industry_l1") or ""

    # 行业近7日资金净流入
    ind_flows = execute_query(
        """SELECT SUM(net_inflow) as total_flow
           FROM industry_capital_flow
           WHERE industry_name LIKE ?
           ORDER BY trade_date DESC LIMIT 7""",
        [f"%{industry}%"],
    )
    industry_flow_7d = round(ind_flows[0]["total_flow"] or 0) if ind_flows and ind_flows[0]["total_flow"] else 0

    # 个股近7日资金净流入
    stock_flows = execute_query(
        """SELECT SUM(main_net_inflow) as total_flow
           FROM capital_flow
           WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT 7""",
        [stock_code],
    )
    stock_flow_7d = round(stock_flows[0]["total_flow"] or 0) if stock_flows and stock_flows[0]["total_flow"] else 0

    # 行业排名：按近7日净流入排序
    all_industries = execute_query(
        """SELECT industry_name, SUM(net_inflow) as total
           FROM industry_capital_flow
           WHERE trade_date >= (
               SELECT trade_date FROM industry_capital_flow
               ORDER BY trade_date DESC LIMIT 1 OFFSET 6
           )
           GROUP BY industry_name
           ORDER BY total DESC"""
    )
    rank = "N/A"
    total_industries = len(all_industries) if all_industries else 0
    if all_industries and industry:
        for i, row in enumerate(all_industries):
            if industry in (row.get("industry_name") or ""):
                rank = f"{i + 1}/{total_industries}"
                break

    # 热度评分：基于行业排名和资金流向
    heat_score = 50
    if total_industries > 0 and rank != "N/A":
        rank_num = int(rank.split("/")[0])
        # 排名越靠前分数越高
        heat_score = max(0, min(100, int(100 * (1 - rank_num / total_industries))))
    if industry_flow_7d > 0:
        heat_score = min(100, heat_score + 10)
    elif industry_flow_7d < 0:
        heat_score = max(0, heat_score - 10)

    summary = f"{industry}行业近7日资金净流入{industry_flow_7d/1e8:.2f}亿，排名{rank}。"
    if stock_flow_7d > 0:
        summary += f"个股主力净流入{stock_flow_7d/1e8:.2f}亿，资金面偏积极。"
    else:
        summary += f"个股主力净流出{abs(stock_flow_7d)/1e8:.2f}亿，资金面偏谨慎。"

    # 个股近15日资金流（用于前端时序图）
    stock_flow_15d_rows = execute_query(
        """SELECT trade_date, main_net_inflow as net_inflow
           FROM capital_flow
           WHERE stock_code=?
           ORDER BY trade_date DESC LIMIT 15""",
        [stock_code],
    )
    stock_flow_15d = [
        {"date": str(r.get("trade_date", "")), "net_inflow": r.get("net_inflow", 0) or 0}
        for r in (stock_flow_15d_rows or [])
    ]

    return {
        "heat_score": heat_score,
        "industry_flow_7d": industry_flow_7d,
        "industry_rank": rank,
        "stock_flow_7d": stock_flow_7d,
        "stock_flow_15d": stock_flow_15d,
        "summary": summary,
        # 后续对接字段（暂留空）
        "margin_trading": None,
        "institutional_holdings": None,
        "block_trades": None,
        "insider_changes": None,
        "etf_membership": None,
    }


def _parse_step(raw):
    """尝试从步骤结果中提取JSON"""
    if not raw or str(raw).startswith("分析失败"):
        return None
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        try:
            from utils.claude_client import _extract_json
            return _extract_json(raw)
        except Exception:
            return None


def _get_business_composition_anchor(stock_code):
    """从 stock_business_composition 表获取分业务收入/成本数据作为事实锚点"""
    rows = execute_query(
        """SELECT report_date, classify_type, item_name, revenue, revenue_pct,
                  cost, cost_pct, profit, profit_pct, gross_margin
           FROM stock_business_composition
           WHERE stock_code=%s ORDER BY report_date DESC LIMIT 30""",
        [stock_code],
    )
    if not rows:
        return None

    # 按 report_date 分组，取最近两期
    periods = {}
    for r in rows:
        rd = str(r.get("report_date", ""))
        if rd not in periods:
            periods[rd] = []
        periods[rd].append(r)

    sorted_periods = sorted(periods.keys(), reverse=True)
    if not sorted_periods:
        return None

    current_period = sorted_periods[0]
    prev_period = sorted_periods[1] if len(sorted_periods) > 1 else None

    anchor = {
        "current_period": current_period,
        "current_data": periods[current_period],
    }
    if prev_period:
        anchor["prev_period"] = prev_period
        anchor["prev_data"] = periods[prev_period]

    return anchor


def _get_financial_anchor(stock_code):
    """从 financial_reports 表获取总营收/净利润作为校验锚点"""
    rows = execute_query(
        """SELECT report_period, revenue, net_profit, revenue_yoy, profit_yoy
           FROM financial_reports WHERE stock_code=%s
           ORDER BY report_period DESC LIMIT 4""",
        [stock_code],
    )
    return rows or []


def _format_anchor_for_prompt(biz_anchor, fin_anchor):
    """将锚点数据格式化为 prompt 注入文本"""
    parts = []
    if fin_anchor:
        parts.append("=== 事实锚点：财务数据 ===")
        for f in fin_anchor[:4]:
            parts.append(
                f"  {f.get('report_period','')}: 营收{f.get('revenue','')}亿 "
                f"净利润{f.get('net_profit','')}亿 "
                f"营收YoY{f.get('revenue_yoy','')}% 利润YoY{f.get('profit_yoy','')}%"
            )

    if biz_anchor:
        parts.append(f"\n=== 事实锚点：分业务数据（{biz_anchor['current_period']}）===")
        for item in biz_anchor["current_data"]:
            parts.append(
                f"  [{item.get('classify_type','')}] {item.get('item_name','')}: "
                f"收入{item.get('revenue','')} 占比{item.get('revenue_pct','')}% "
                f"成本{item.get('cost','')} 毛利率{item.get('gross_margin','')}%"
            )
        if biz_anchor.get("prev_data"):
            parts.append(f"\n=== 事实锚点：分业务数据（{biz_anchor['prev_period']}）===")
            for item in biz_anchor["prev_data"]:
                parts.append(
                    f"  [{item.get('classify_type','')}] {item.get('item_name','')}: "
                    f"收入{item.get('revenue','')} 占比{item.get('revenue_pct','')}% "
                    f"成本{item.get('cost','')} 毛利率{item.get('gross_margin','')}%"
                )

    return "\n".join(parts) if parts else ""


def _validate_business_model(result_dict):
    """后置校验：占比之和≈100%，驱动因素过滤 impact_pct<20% 的条目，返回 (is_valid, issues)"""
    issues = []

    for key in ["revenue_segments_current", "revenue_segments_prev"]:
        segs = result_dict.get(key, [])
        if segs and isinstance(segs, list):
            total = sum(s.get("pct", 0) or 0 for s in segs)
            if total > 0 and abs(total - 100) > 15:
                issues.append(f"{key} 占比之和={total}%，偏离100%过大")

    for key in ["cost_breakdown_current", "cost_breakdown_prev"]:
        segs = result_dict.get(key, [])
        if segs and isinstance(segs, list):
            total = sum(s.get("pct", 0) or 0 for s in segs)
            if total > 0 and abs(total - 100) > 15:
                issues.append(f"{key} 占比之和={total}%，偏离100%过大")

    # 过滤 impact_pct < 20% 的驱动因素（兜底，防止模型乱发挥）
    for key in ["topline_drivers", "bottomline_drivers"]:
        drivers = result_dict.get(key, [])
        if drivers and isinstance(drivers, list):
            filtered = [d for d in drivers if (d.get("impact_pct") or 0) >= 20]
            if not filtered:
                # 如果全被过滤掉（模型没填 impact_pct），保留原始列表
                filtered = drivers
            result_dict[key] = filtered

    return (len(issues) == 0, issues)


def run_step_business_model(stock_code, stock_name, context, industry,
                             progress_callback=None, step_callback=None):
    """Step 1: 商业模式画布"""
    if progress_callback:
        progress_callback(f"[1/6] 商业模式画布 ({stock_name})...")
    try:
        # 获取事实锚点（优先级：financial_reports > 族1摘要 key_data）
        biz_anchor = _get_business_composition_anchor(stock_code)
        fin_anchor = _get_financial_anchor(stock_code)
        anchor_text = _format_anchor_for_prompt(biz_anchor, fin_anchor)

        # P3: 从族1摘要补充事实锚点（覆盖无财报数据的股票）
        try:
            from research.fact_anchors import get_summary_fact_anchors
            summary_anchors = get_summary_fact_anchors(stock_code)
            if summary_anchors and not anchor_text:
                # 仅在没有 financial_reports 锚点时补充族1锚点
                anchor_text = summary_anchors
            elif summary_anchors and anchor_text:
                anchor_text = anchor_text + "\n\n" + summary_anchors
        except Exception as _fa_e:
            logger.debug(f"族1事实锚点获取跳过: {_fa_e}")

        bm_input = f"股票: {stock_code} {stock_name}\n行业: {industry}\n\n"
        if anchor_text:
            bm_input += f"{anchor_text}\n\n"
        bm_input += f"=== 基础数据 ===\n{context[:3000]}\n"

        # RAG注入
        try:
            from research.rag_context import search_stock_context
            rag_ctx = search_stock_context(stock_code, f"{stock_name} {industry} 商业模式 收入结构 成本")
            if rag_ctx:
                bm_input += f"\n=== RAG检索到的最新相关信息 ===\n{rag_ctx}\n"
        except Exception as _rag_e:
            logger.debug(f"RAG检索跳过: {_rag_e}")

        result = call_claude_json(STEP1_BUSINESS_MODEL_PROMPT, bm_input, max_tokens=4000)

        # 后置校验（最多重试1次）
        if isinstance(result, dict):
            is_valid, issues = _validate_business_model(result)
            if not is_valid:
                logger.warning(f"商业模式校验不通过: {issues}，重试一次")
                retry_hint = f"\n\n上次生成的数据有以下问题，请修正：\n" + "\n".join(issues)
                result = call_claude_json(STEP1_BUSINESS_MODEL_PROMPT, bm_input + retry_hint, max_tokens=4000)

            # 兼容旧字段：将双期数据映射到旧字段名，确保前端不崩
            if "revenue_segments_current" in result and "revenue_segments" not in result:
                result["revenue_segments"] = result["revenue_segments_current"]
            if "cost_breakdown_current" in result and "cost_breakdown" not in result:
                result["cost_breakdown"] = result["cost_breakdown_current"]

        if step_callback:
            step_callback("business_model", result)
        return result
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logger.error(f"商业模式分析失败: {e}\n{tb}")
        # 添加详细的调试信息
        logger.error(f"DEBUG: biz_anchor type={type(biz_anchor)}, fin_anchor type={type(fin_anchor)}")
        return f"分析失败: {e}"


def run_step_value_chain(stock_code, stock_name, context, industry, peers,
                          step1_result=None, progress_callback=None, step_callback=None):
    """Step 2: 产业链地图"""
    if progress_callback:
        progress_callback(f"[2/6] 产业链地图 ({stock_name})...")
    try:
        ind_data = get_industry_data(industry) if industry else {}
        vc_input = f"股票: {stock_code} {stock_name}\n行业: {industry}\n\n"
        if ind_data.get("flows"):
            vc_input += "=== 行业资金流向 ===\n"
            for f in ind_data["flows"][:10]:
                vc_input += f"  {f.get('trade_date','')}: 净流入{f.get('net_inflow','')} 涨跌{f.get('change_pct','')}%\n"
        if ind_data.get("news"):
            vc_input += "\n=== 行业新闻（含日期，用于is_recent判断）===\n"
            for n in ind_data["news"][:15]:
                cleaned_at = n.get('cleaned_at', '')
                source = n.get('source_name', '')
                vc_input += f"  [{n.get('sentiment','')}][{cleaned_at}][{source}] {n.get('summary','')}\n"
        if peers:
            vc_input += "\n=== 同行对比 ===\n"
            for p in peers[:8]:
                vc_input += f"  {p.get('stock_code','')} {p.get('stock_name','')}: 市值{p.get('market_cap','')}亿\n"
        try:
            kg_ctx = get_company_context(stock_code)
            if kg_ctx and kg_ctx.get("text"):
                vc_input += f"\n{kg_ctx['text'][:3000]}\n"
        except Exception as e:
            logger.debug(f"KG公司上下文获取失败: {e}")
        if step1_result and not str(step1_result).startswith("分析失败"):
            s1 = step1_result if isinstance(step1_result, str) else json.dumps(step1_result, ensure_ascii=False)
            vc_input += f"\n=== 商业模式分析（含分部信息）===\n{s1[:2000]}\n"

        # RAG注入
        try:
            from research.rag_context import search_stock_context
            rag_ctx = search_stock_context(stock_code, f"{stock_name} {industry} 产业链 上下游 竞争格局")
            if rag_ctx:
                vc_input += f"\n=== RAG检索到的最新产业链信息 ===\n{rag_ctx}\n"
        except Exception as _rag_e:
            logger.debug(f"产业链RAG检索跳过: {_rag_e}")

        result = call_claude(STEP2_VALUE_CHAIN_PROMPT, vc_input, max_tokens=8192)
        if step_callback:
            step_callback("value_chain", result)
        return result
    except Exception as e:
        logger.error(f"产业链分析失败: {e}")
        return f"分析失败: {e}"


def run_step_financial(stock_code, stock_name, step1_result=None,
                        progress_callback=None, step_callback=None):
    """Step 3: 财务分析"""
    if progress_callback:
        progress_callback(f"[3/6] 财务分析 ({stock_name})...")
    try:
        fin_data = execute_query(
            """SELECT report_period, revenue, net_profit, roe, revenue_yoy, profit_yoy, eps
               FROM financial_reports WHERE stock_code=?
               ORDER BY report_period DESC LIMIT 12""",
            [stock_code],
        )
        dp_input = f"股票: {stock_code} {stock_name}\n\n=== 财务数据（最近12个季度）===\n"
        for f in (fin_data or []):
            dp_input += (f"  {f.get('report_period','')}: 营收{f.get('revenue','')} "
                         f"净利{f.get('net_profit','')} ROE{f.get('roe','')}% "
                         f"营收YoY{f.get('revenue_yoy','')}% 利润YoY{f.get('profit_yoy','')}% "
                         f"EPS{f.get('eps','')}\n")
        if not fin_data:
            dp_input += "  暂无财务数据，请基于行业平均水平估算\n"
        if step1_result and not str(step1_result).startswith("分析失败"):
            bm_text = step1_result if isinstance(step1_result, str) else json.dumps(step1_result, ensure_ascii=False)
            dp_input += f"\n=== 商业模式分析 ===\n{bm_text[:4000]}\n"

        # RAG注入
        try:
            from research.rag_context import search_stock_context
            rag_ctx = search_stock_context(stock_code, f"{stock_name} 财务 营收 利润 成本 公告")
            if rag_ctx:
                dp_input += f"\n=== RAG检索到的最新财务相关信息 ===\n{rag_ctx}\n"
        except Exception as _rag_e:
            logger.debug(f"财务RAG检索跳过: {_rag_e}")

        result = call_claude(STEP3_FINANCIAL_PROMPT, dp_input, max_tokens=4000)
        if step_callback:
            step_callback("financial", result)
        return result
    except Exception as e:
        logger.error(f"财务分析失败: {e}")
        return f"分析失败: {e}"


def run_step_valuation(stock_code, stock_name, profile, step1_result=None,
                        step2_result=None, step3_result=None,
                        context="", progress_callback=None, step_callback=None):
    """Step 4: 估值分析"""
    if progress_callback:
        progress_callback(f"[4/6] 估值分析 ({stock_name})...")
    try:
        bm_parsed = _parse_step(step1_result)
        vc_parsed = _parse_step(step2_result)
        fin_parsed = _parse_step(step3_result)

        from research.valuation_engine import run_valuation
        val_result = run_valuation(
            stock_code=stock_code,
            stock_name=stock_name,
            step1_result=bm_parsed or {},
            step2_result=vc_parsed or {},
            step3_result=fin_parsed or {},
            profile=profile,
            progress_callback=progress_callback,
        )
        result = json.dumps(val_result, ensure_ascii=False)
        if step_callback:
            step_callback("valuation", result)
        return result
    except Exception as e:
        logger.error(f"估值分析（新版）失败: {e}，降级到旧版")
        try:
            val_input = f"股票: {stock_code} {stock_name}\n\n"
            val_input += f"=== 基础数据 ===\n{context[:2500]}\n"
            s1 = step1_result if isinstance(step1_result, str) else json.dumps(step1_result or {}, ensure_ascii=False)
            s3 = step3_result if isinstance(step3_result, str) else json.dumps(step3_result or {}, ensure_ascii=False)
            if s1 and not s1.startswith("分析失败"):
                val_input += f"\n=== 商业模式分析 ===\n{s1[:1500]}\n"
            if s3 and not s3.startswith("分析失败"):
                val_input += f"\n=== 财务分析 ===\n{s3[:1500]}\n"
            result = call_claude(STEP4_VALUATION_PROMPT, val_input, max_tokens=3000)
            if step_callback:
                step_callback("valuation", result)
            return result
        except Exception as e2:
            logger.error(f"估值分析降级也失败: {e2}")
            return f"分析失败: {e}"


def run_step_sector_heat(stock_code, progress_callback=None, step_callback=None):
    """Step 5: 板块热度（纯数据计算）"""
    if progress_callback:
        progress_callback(f"[5/6] 板块热度...")
    try:
        sector_heat = _calc_sector_heat(stock_code)
        result = json.dumps(sector_heat, ensure_ascii=False)
        if step_callback:
            step_callback("sector_heat", result)
        return result
    except Exception as e:
        logger.error(f"板块热度计算失败: {e}")
        return f"分析失败: {e}"


def run_step_research_data(stock_code, stock_name, profile, industry,
                            step1_result=None, progress_callback=None, step_callback=None):
    """Step 6: 研究数据（宏观指标 + 政策新闻 + 综合评估）"""
    if progress_callback:
        progress_callback(f"[6/6] 研究数据 ({stock_name})...")
    try:
        macro_data = get_macro_data()
        # 构建 user_message，将真实数据作为锚点传入
        rd_input = f"股票: {stock_code} {stock_name}\n行业: {industry}\n\n"

        # 宏观指标（真实数据锚点）
        indicator_names = []
        if macro_data.get("indicators"):
            rd_input += "=== 宏观指标 ===\n"
            for ind in macro_data["indicators"][:20]:
                name = ind.get('indicator_name', '')
                val = ind.get('value', '')
                date = ind.get('indicator_date', '')
                rd_input += f"  {name}: {val} ({date})\n"
                if name:
                    indicator_names.append(name)

        # 近期新闻（真实数据锚点）
        if macro_data.get("news"):
            rd_input += "\n=== 近期新闻（宏观/行业）===\n"
            for n in macro_data["news"][:15]:
                summary = n.get('summary', '')
                sentiment = n.get('sentiment', '')
                cleaned_at = n.get('cleaned_at', '')
                source = n.get('source_name', '')
                rd_input += f"  [{sentiment}][{cleaned_at}][{source}] {summary}\n"

        if profile.get("news"):
            rd_input += "\n=== 个股关联新闻 ===\n"
            for n in profile["news"][:10]:
                summary = n.get('summary', '')
                sentiment = n.get('sentiment', '')
                importance = n.get('importance', '')
                cleaned_at = n.get('cleaned_at', '')
                source = n.get('source_name', '')
                rd_input += f"  [{sentiment}][{importance}⭐][{cleaned_at}][{source}] {summary}\n"

        if step1_result and not str(step1_result).startswith("分析失败"):
            s1 = step1_result if isinstance(step1_result, str) else json.dumps(step1_result, ensure_ascii=False)
            rd_input += f"\n=== 商业模式中识别的关键指标 ===\n{s1[:1500]}\n"

        # 传入指标名称列表用于前端交叉验证
        rd_input += f"\n=== 可引用的宏观指标名称列表（仅可引用此列表中的指标）===\n{json.dumps(indicator_names, ensure_ascii=False)}\n"

        result = call_claude(STEP6_RESEARCH_DATA_PROMPT, rd_input, max_tokens=3000)
        if step_callback:
            step_callback("research_data", result)
        return result
    except Exception as e:
        logger.error(f"研究数据分析失败: {e}")
        return f"分析失败: {e}"


# ── 步骤名称 → 依赖关系 ──────────────────────────────────────────────────────
_STEP_DEPS = {
    "business_model": [],
    "value_chain": ["business_model"],
    "financial": ["business_model"],
    "valuation": ["business_model", "value_chain", "financial"],
    "sector_heat": [],
    "research_data": ["business_model"],
}

ALL_STEPS = ["business_model", "value_chain", "financial", "valuation", "sector_heat", "research_data"]


def _resolve_steps(steps):
    """展开步骤列表，自动补充依赖步骤"""
    if steps is None:
        return list(ALL_STEPS)
    needed = set(steps)
    changed = True
    while changed:
        changed = False
        for s in list(needed):
            for dep in _STEP_DEPS.get(s, []):
                if dep not in needed:
                    needed.add(dep)
                    changed = True
    # 按 ALL_STEPS 顺序返回
    return [s for s in ALL_STEPS if s in needed]


def deep_research_stock(stock_code, steps=None, existing_report_id=None,
                         progress_callback=None, step_callback=None):
    """个股深度研究 — 6板块分析

    Args:
        stock_code: 股票代码
        steps: 要执行的步骤列表，None=全部。如 ['valuation'] 只重跑估值（自动补依赖）
        existing_report_id: 已有报告ID，从DB读取已有步骤结果（用于单步重跑）
        progress_callback: 进度回调 fn(msg)
        step_callback: 步骤完成回调 fn(step_name, result_text)
    """
    steps_to_run = _resolve_steps(steps)

    # === 数据充分性检查 + 自动补数据 ===
    if progress_callback:
        progress_callback("正在检查数据充分性...")

    from research.data_readiness import ensure_stock_data_ready
    readiness = ensure_stock_data_ready(
        stock_code, max_rounds=2, progress_callback=progress_callback
    )

    if not readiness["ready"]:
        logger.warning(f"{stock_code} 数据不充分: {readiness['missing']}")
        critical_missing = [d for d in readiness["missing"] if d not in ("news", "reports")]
        if critical_missing:
            logger.warning(f"{stock_code} 缺少关键数据: {critical_missing}，但仍尝试研究")

    # === 获取数据 ===
    if progress_callback:
        progress_callback("正在获取个股数据...")

    profile = get_stock_profile(stock_code)
    if not profile.get("info"):
        return {"error": f"未找到股票信息: {stock_code}"}

    tech = get_stock_technical_summary(stock_code)
    peers = get_peer_comparison(stock_code)
    context = _build_stock_context(profile, tech, peers)

    context += f"\n\n=== 数据充分性 ===\n置信度: {readiness['confidence']:.0%}\n"
    for dim, dim_info in readiness["dimensions"].items():
        status = "✅" if dim_info["ok"] else "⚠️不足"
        context += f"  {dim}: {status} ({dim_info['count']}/{dim_info['min']})\n"

    stock_name = profile["info"].get("stock_name", stock_code)
    industry = profile["info"].get("industry_l2") or profile["info"].get("industry_l1") or ""

    # === 加载已有步骤结果（单步重跑模式）===
    step_results = {}
    if existing_report_id:
        rows = execute_query(
            "SELECT report_json FROM deep_research WHERE id=?", [existing_report_id]
        )
        if rows and rows[0].get("report_json"):
            existing_report = json.loads(rows[0]["report_json"])
            # report_json 可能是 {report: {...}} 或直接是 {...}
            existing_data = existing_report.get("report", existing_report)
            for key in ALL_STEPS:
                if key in existing_data:
                    raw = existing_data[key]
                    step_results[key] = raw if isinstance(raw, str) else json.dumps(raw, ensure_ascii=False)
            logger.info(f"从报告#{existing_report_id}加载已有步骤: {list(step_results.keys())}")

    # === 执行各步骤 ===
    for step in steps_to_run:
        # 如果已有结果且不在本次要重跑的步骤中，跳过
        if step in step_results and (steps is None or step not in steps):
            logger.info(f"跳过步骤 {step}（使用已有结果）")
            continue

        s1 = step_results.get("business_model")
        s2 = step_results.get("value_chain")
        s3 = step_results.get("financial")

        if step == "business_model":
            step_results["business_model"] = run_step_business_model(
                stock_code, stock_name, context, industry,
                progress_callback=progress_callback, step_callback=step_callback,
            )
        elif step == "value_chain":
            step_results["value_chain"] = run_step_value_chain(
                stock_code, stock_name, context, industry, peers,
                step1_result=s1,
                progress_callback=progress_callback, step_callback=step_callback,
            )
        elif step == "financial":
            step_results["financial"] = run_step_financial(
                stock_code, stock_name, step1_result=s1,
                progress_callback=progress_callback, step_callback=step_callback,
            )
        elif step == "valuation":
            step_results["valuation"] = run_step_valuation(
                stock_code, stock_name, profile,
                step1_result=s1, step2_result=s2, step3_result=s3,
                context=context,
                progress_callback=progress_callback, step_callback=step_callback,
            )
        elif step == "sector_heat":
            step_results["sector_heat"] = run_step_sector_heat(
                stock_code,
                progress_callback=progress_callback, step_callback=step_callback,
            )
        elif step == "research_data":
            step_results["research_data"] = run_step_research_data(
                stock_code, stock_name, profile, industry,
                step1_result=s1,
                progress_callback=progress_callback, step_callback=step_callback,
            )

    # === 最终综合 ===
    if progress_callback:
        progress_callback("正在综合所有分析结果...")

    valid_steps = {k: v for k, v in step_results.items()
                   if v and not str(v).startswith("分析失败")}
    if not valid_steps:
        logger.error(f"{stock_code} 所有分析步骤均失败，无法综合")
        return {"error": "所有分析步骤均失败，请检查 Claude CLI 是否正常运行"}

    # 先把各步骤结果解析为JSON，构建完整report
    merged_report = {}
    for key in ALL_STEPS:
        parsed = _parse_step(step_results.get(key))
        if parsed:
            merged_report[key] = parsed

    # 综合评分
    synthesis_input = f"股票: {stock_code} {stock_name}\n\n"
    synthesis_input += f"=== 基础数据 ===\n{context[:1500]}\n\n"
    for key, label in [
        ("business_model", "商业模式"), ("value_chain", "产业链"),
        ("financial", "财务分析"), ("valuation", "估值分析"),
        ("sector_heat", "板块热度"), ("research_data", "研究数据"),
    ]:
        val = step_results.get(key)
        if val and not str(val).startswith("分析失败"):
            synthesis_input += f"=== {label} ===\n{str(val)[:1500]}\n\n"

    try:
        synthesis = call_claude_json(STOCK_SYNTHESIS_PROMPT, synthesis_input, max_tokens=1000)
    except (ValueError, json.JSONDecodeError) as e:
        logger.error(f"综合分析JSON解析失败: {e}")
        try:
            raw_text = call_claude(STOCK_SYNTHESIS_PROMPT, synthesis_input, max_tokens=1000)
            from utils.claude_client import _extract_json
            synthesis = _extract_json(raw_text)
        except Exception as e2:
            logger.error(f"综合分析重试仍失败: {e2}")
            synthesis = {"overall_score": 50, "recommendation": "中性",
                         "executive_summary": "综合评分生成失败，请参考各板块分析结果。"}
    except Exception as e:
        logger.error(f"综合分析失败: {e}")
        synthesis = {"overall_score": 50, "recommendation": "中性",
                     "executive_summary": "综合评分生成失败，请参考各板块分析结果。"}

    if not isinstance(synthesis, dict):
        synthesis = {"overall_score": 50, "recommendation": "中性",
                     "executive_summary": "综合评分生成失败。"}

    merged_report["executive_summary"] = synthesis.get("executive_summary", "")
    overall_score = synthesis.get("overall_score", 50)
    recommendation = synthesis.get("recommendation", "中性")

    if progress_callback:
        progress_callback("正在保存研究结果...")

    # 保存：单步重跑时 UPDATE，全新研究时 INSERT
    logger.info(f"DEBUG: existing_report_id={existing_report_id}, steps={steps}, type(steps)={type(steps)}, bool(steps)={bool(steps) if steps else 'N/A'}")
    if existing_report_id and steps:
        logger.info(f"Saving single step re-run: report_id={existing_report_id}, steps={steps}, merged_report keys={list(merged_report.keys())}")
        execute_insert(
            """UPDATE deep_research SET report_json=?, overall_score=?, recommendation=?,
               research_date=date('now') WHERE id=?""",
            [json.dumps(merged_report, ensure_ascii=False), overall_score,
             recommendation, existing_report_id],
        )
        logger.info(f"Saved single step re-run: report_id={existing_report_id}")
        research_id = existing_report_id
    else:
        research_id = execute_insert(
            """INSERT INTO deep_research
               (research_type, target, research_date, overall_score, report_json, recommendation)
               VALUES ('stock', ?, date('now'), ?, ?, ?)""",
            [stock_code, overall_score,
             json.dumps(merged_report, ensure_ascii=False),
             recommendation],
        )

    result = {"overall_score": overall_score, "recommendation": recommendation, "report": merged_report}

    if overall_score >= 70:
        _create_opportunity(stock_code, profile["info"], result, research_id)

    # 存入新管线
    try:
        from utils.research_store import store_as_extracted_text
        exec_summary = synthesis.get("executive_summary", "")
        report_full = f"# 个股深度研究: {stock_name}({stock_code})\n\n综合评分: {overall_score}分\n建议: {recommendation}\n\n{exec_summary}"
        store_as_extracted_text(
            title=f"个股深度研究: {stock_name}({stock_code})",
            full_text=report_full,
            summary=f"【个股研究】{stock_name} 综合评分{overall_score}分，建议{recommendation}。{exec_summary}"[:500],
            source="system_report",
            source_format="markdown",
        )
    except Exception as e:
        logger.warning(f"个股研究存为新闻失败: {e}")

    logger.info(f"个股研究完成: {stock_code}, 综合评分: {overall_score}")
    return {"research_id": research_id, "scores": {"overall": overall_score}, **result}


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

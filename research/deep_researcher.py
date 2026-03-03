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
    "topline_indicators": [
        {
            "name": "指标名",
            "source": "产业/宏观/政策/技术/竞争",
            "category": "volume/price/mix/market_share/new_market",
            "chain_position": "upstream/midstream/downstream",
            "transmission_path": "从产业链因素到公司收入的传导路径（60字）",
            "impact": "对营收的影响（50字）",
            "current_status": "当前状态描述（30字）",
            "data_available": true或false
        }
    ],
    "bottomline_indicators": [
        {
            "name": "指标名",
            "source": "产业/宏观/政策/技术/竞争",
            "category": "与cost_breakdown对齐的成本项名称",
            "chain_position": "upstream/midstream/downstream",
            "transmission_path": "从产业链因素到公司利润的传导路径（60字）",
            "impact": "对利润的影响（50字）",
            "impact_direction": "positive或negative",
            "current_status": "当前状态描述（30字）",
            "data_available": true或false
        }
    ],
    "revenue_composition_insight": "结合两期收入结构对比，分析收入构成变化的业务含义（100字）",
    "cost_composition_insight": "结合两期成本结构对比，分析成本构成变化的业务含义（100字）"
}

要求：
- BMC 9格字段（value_proposition/key_partners/key_activities/key_resources/customers/customer_relationships/channels/revenue_sources/cost_structure）每个80字以内
- revenue_segments_current/prev: 拆解最近两期收入结构，列出2-5个主要业务板块及占比
- cost_breakdown_current/prev: 拆解最近两期成本结构，列出2-5个主要成本项及占比
- 如果"事实锚点数据"中有分业务数据，必须与之一致；没有则标注source为"推算"
- customer_industries: 列出2-5个主要客户行业及占比
- topline_indicators: **穷举**所有可能影响营收的因素，至少6-10个，覆盖 volume/price/mix/market_share/new_market 各维度
  - category: volume/price/mix/market_share/new_market 五类之一
  - 每个指标必须填写 transmission_path（传导路径）和 current_status（当前状态）
  - data_available: 当前上下文中有该指标具体数据支撑则为true，否则为false
- bottomline_indicators: **穷举**所有可能影响利润的因素，至少6-10个
  - category 必须与 cost_breakdown_current 中的成本项名称对齐（如原材料/直接人工/能源/运费等），不使用会计科目
  - impact_direction: positive=降成本/增利润，negative=增成本/减利润
  - 每个指标必须填写 transmission_path 和 current_status
- topline_drivers: 3-5个影响营收的关键驱动因素，每个驱动因素对营收的潜在影响必须超过20%，不得列入边缘性或次要因素
  - direction: 该因素对营收的结构性影响（正向=该因素增大则营收增加，负向=该因素增大则营收减少），不等于当前在涨还是在跌
  - current_trend: 该驱动因素当前实际的变动方向（上行/下行/平稳），必须基于可见数据
  - supporting: 支撑current_trend的具体证据，如有数据请引用（无数据不能瞎填，填null）
  - name 必须从 topline_indicators 中选取（key drivers 是穷举因素的子集）
- bottomline_drivers: 3-5个影响成本/利润的关键驱动因素，每个驱动因素对成本的潜在影响必须超过20%，不得列入边缘性或次要因素
  - direction/current_trend/supporting 同上逻辑
  - name 必须从 bottomline_indicators 中选取（key drivers 是穷举因素的子集）
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
- competitive_comparison 中的 evidence 必须来自提供的数据，无据可查时填"数据不足"

请输出严格的JSON格式：
{
    "upstream": [
        {"label": "原材料/零部件名称", "players": "主要供应商（50字）", "description": "特征描述（60字）", "has_company_business": true或false}
    ],
    "midstream": [
        {"label": "中游环节名称", "players": "主要参与者（50字）", "description": "特征描述（60字）", "has_company_business": true或false}
    ],
    "downstream": [
        {"label": "下游应用/客户名称", "players": "主要客户群（50字）", "description": "特征描述（60字）", "has_company_business": true或false}
    ],
    "company_position": "upstream 或 midstream 或 downstream（兼容字段，取第一个分部的位置）",
    "position_detail": "公司在产业链中的具体定位和竞争优势（100字）",
    "competitive_comparison": {
        "comparison_dimensions": ["维度1", "维度2"],
        "companies": [
            {
                "name": "公司名",
                "is_target": true,
                "advantages": [
                    {"dimension": "维度", "assessment": "strong/medium/weak", "evidence": "佐证（50字，无据填'数据不足'）"}
                ]
            }
        ],
        "summary": "竞争格局总结（100字）"
    },
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
            "competitors": [
                {
                    "name": "对标公司名称",
                    "ticker": "股票代码（如有）",
                    "diff": "一句话说明该公司与目标公司在此分部业务上的核心差异（40字）"
                }
            ]
        }
    ],
    "synergies": [
        {"segment_a": "分部A", "segment_b": "分部B", "type": "技术协同/客户协同/供应链协同/品牌协同", "description": "协同效应描述（60字）", "value_impact": "positive/neutral"}
    ],
    "tech_barrier": {
        "has_tech_barrier": true或false,
        "barrier_description": "技术壁垒描述（80字）",
        "tech_routes": [
            {"route_name": "技术路线名称", "stage": "实验室/测试验证/小规模商业化/量产", "difficulty": "难点（40字）", "competitors": "主要竞争者"}
        ],
        "commercialization_cycle": "商业化周期描述（如：实验室→测试→小规模商业化→量产）"
    },
    "industry_data": [
        {
            "metric": "指标名称",
            "value": "数值（必须带单位，如'1200万吨'、'68.5%'、'$9800/吨'、'350亿元'）",
            "year": "年份",
            "source": "数据来源（必填）",
            "trend": "趋势描述",
            "line_type": "topline/bottomline/context",
            "related_driver": "关联的商业模式driver名称或null"
        }
    ],
    "industry_news": [
        {
            "title": "新闻标题（只能引用提供的新闻）",
            "date": "日期",
            "source": "来源",
            "line_type": "topline/bottomline/both/context",
            "tagged_drivers": ["关联的driver名称1", "driver名称2"],
            "sentiment": "positive/negative/neutral",
            "summary": "新闻摘要（100字）",
            "is_recent": true或false
        }
    ],
    "_for_valuation": {
        "downstream_demand_analysis": {
            "downstream_industries": [
                {
                    "industry_name": "下游行业名",
                    "mapped_segments": ["对应的公司业务分部名"],
                    "weight_pct": 权重百分比数字,
                    "growth_rate": {
                        "actual_recent": {"value_pct": 数字或null, "period": "如2024", "source": "来源"},
                        "forecast": {"value_pct": 数字或null, "period": "如2025E", "source": "来源", "confidence": "high/medium/low"}
                    },
                    "demand_driver": "需求驱动因素（40字）",
                    "demand_risk": "需求风险（40字）"
                }
            ],
            "weighted_growth_rate": {
                "value_pct": 加权增长率数字,
                "calculation": "行业A X%×权重Y% + 行业B X%×权重Y% + ...",
                "confidence": "high/medium/low",
                "note": "这是下游需求增速，非公司收入增速"
            }
        },
        "capture_rate_analysis": {
            "supply_capacity": {
                "current_utilization_pct": 数字或null,
                "expansion_projects": [
                    {
                        "project": "项目名",
                        "signal_type": "direct或indirect",
                        "new_capacity": "数值+单位或null",
                        "expected_completion": "时间",
                        "confidence": "high/medium/low",
                        "evidence": "公告原文或推断依据（50字）"
                    }
                ],
                "capacity_vs_demand": "公司新增产能 vs 可吃到的需求增量（60字）",
                "capacity_adequacy": "sufficient/tight/insufficient"
            },
            "porter_capture_analysis": {
                "rivalry": {
                    "level": "high/medium/low",
                    "market_concentration": "CR3=X%, 格局描述",
                    "competitor_expansion_summary": "主要竞对扩产计划汇总（80字）",
                    "differentiation_degree": "high/medium/low",
                    "price_war_risk": "价格战风险描述（40字）",
                    "capture_verdict": "新增需求出现时，公司能抢到的份额判断（60字）"
                },
                "new_entrants": {
                    "level": "high/medium/low",
                    "barrier_types": [
                        {"type": "资源/政策/技术/资本/规模/渠道/转换成本/学习曲线/环保/品牌",
                         "strength": "high/medium/low", "detail": "描述（40字）"}
                    ],
                    "barrier_trend": "strengthening/weakening/stable",
                    "barrier_trend_reason": "壁垒变化原因（40字）",
                    "cross_border_threat": "跨界进入者威胁描述（40字，无则null）",
                    "capture_verdict": "新玩家是否会来分蛋糕（60字）"
                },
                "substitutes": {
                    "level": "high/medium/low",
                    "substitute_products": [
                        {
                            "name": "替代品名",
                            "type": "材料替代/技术替代/模式替代/内部化替代/再生替代/政策强制替代",
                            "penetration_current": "当前渗透率或状态",
                            "penetration_trend": "accelerating/stable/slowing",
                            "price_competitiveness": "替代品价格竞争力趋势（30字）",
                            "policy_push": "有无政策推动或阻止（30字，无则null）"
                        }
                    ],
                    "capture_verdict": "下游需求会不会被替代品分流（60字）"
                },
                "supplier_power": {
                    "level": "high/medium/low",
                    "key_input_concentration": "关键原材料供给集中度（30字）",
                    "long_term_agreement_pct": "长协占比或null",
                    "backward_integration": "后向一体化能力描述（30字）",
                    "cost_implication": "上游议价能力对公司成本的影响（60字）"
                },
                "buyer_power": {
                    "level": "high/medium/low",
                    "customer_concentration": "top5客户占比或描述",
                    "switching_cost": "high/medium/low",
                    "product_importance_to_buyer": "产品占客户成本比例或描述",
                    "pricing_implication": "下游议价能力对公司定价的影响（60字）"
                }
            },
            "capture_rate_conclusion": {
                "current_market_share_pct": "当前市场份额或null",
                "share_trend": "gaining/stable/losing",
                "adjustment_factors": [
                    {"factor": "因素描述", "direction": "+或-", "magnitude": "high/medium/low", "evidence": "证据（40字）"}
                ],
                "capture_multiplier": "份额变化方向及依据（60字）",
                "confidence": "high/medium/low"
            }
        },
        "price_volume_analysis": {
            "pricing_mechanism": "交易所定价/成本加成/品牌溢价/合同定价/管制定价/招标竞价/流量变现",
            "pricing_mechanism_detail": "该行业定价机制的具体描述（60字）",
            "supply_demand_balance": {
                "industry_new_capacity": "全行业在建或新增产能规模",
                "industry_demand_growth": "全行业需求增量",
                "capacity_utilization_trend": "产能利用率趋势",
                "inventory_signal": "库存水平信号（高位/正常/低位）",
                "balance_verdict": "shortage/tight_balance/balance/mild_surplus/severe_surplus"
            },
            "price_volume_pattern": "量升价升/量升价平/量升价跌温和/量升价跌剧烈",
            "price_trend_factors": [
                {
                    "factor": "供需缺口/产品结构升级/成本传导/竞争加剧/政策调价",
                    "direction": "+或-",
                    "magnitude_pct": "影响幅度估算",
                    "evidence": "依据（40字）"
                }
            ],
            "asp_trend": {
                "company_asp_direction": "上行/平稳/下行",
                "vs_industry": "高于/持平/低于行业均价",
                "premium_stability": "溢价是否稳定（30字，无则null）"
            },
            "cost_passthrough": {
                "ability": "full/partial/none",
                "mechanism": "传导机制描述（40字）",
                "lag_months": "传导时滞月数或null",
                "historical_evidence": "历史传导案例（40字，无则null）"
            },
            "price_adjustment_conclusion": {
                "direction": "上行/平稳/下行",
                "magnitude_pct": "预计变动幅度",
                "breakdown": "供需+X% + 结构+Y% + 竞争Z% = 净W%",
                "confidence": "high/medium/low"
            }
        },
        "segment_industry_context": [
            {
                "segment_name": "分部名称（对应商业模式分析中的revenue_segments）",
                "tam": {
                    "current_year_value": 数值或null,
                    "unit": "亿元",
                    "growth_driver": "TAM增长驱动（50字）",
                    "data_source": "数据来源"
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
  - has_company_business: 若被研究公司在该环节有自营业务（如自有矿山、自有加工厂、自有销售渠道），必须为true
  - 判断依据：商业模式分析中的 revenue_segments、key_activities、key_resources 等字段
- company_position 为兼容字段，取segment_positions中第一个分部的chain_position
- competitive_comparison: 对目标公司与2-3个主要竞对进行横向对比
  - comparison_dimensions: 识别3-5个核心竞争维度（如技术壁垒、渠道覆盖、成本结构、品牌溢价、客户粘性）
  - companies[0] 必须是目标公司（is_target=true），后面列主要竞对
  - 每个维度的 assessment 必须是 strong/medium/weak
  - evidence 必须引用提供的数据，无据可查时填"数据不足"
  - summary: 100字以内的竞争格局总结
- segment_positions: 按公司的主要revenue_segments分部逐一分析，每个分部可能处于不同产业链环节
  - chain_node_label 必须与 upstream/midstream/downstream 数组中的 label 一一对应
  - competitors: 列出该分部业务的2-3个直接对标公司，每个公司用一句话说明与目标公司在此业务上的核心差异（如规模差距、成本优势、技术路线、资源禀赋、产业链位置等）。对标公司应是在同一细分领域直接竞争的公司，不是泛行业龙头
  - company_share_pct: 若无数据则设null，不得编造
- synergies: 列出分部之间的协同效应，若无明显协同则设为空数组
- tech_barrier: 如无明显技术壁垒，has_tech_barrier=false，tech_routes 可为空数组
- industry_data: 行业关键数据，source 字段必填，无来源的数据不要填写
  - value 必须带单位（如"1200万吨"、"68.5%"、"$9800/吨"），禁止裸数字
  - line_type 判断标准：沿商业模式的传导链路分类
    - topline: 最终影响公司**收入端**的（需求量变化、产品价格变化、市场规模变化、市占率变化等）。注意：上游原材料价格上涨如果能传导到产品涨价→也是topline
    - bottomline: 最终影响公司**成本/利润端**的（原材料成本不能传导至售价时、人工/能源成本、产能利用率等）
    - context: 背景性数据（不直接影响收入或利润）
  - related_driver: 关联到商业模式分析中的topline_drivers或bottomline_drivers名称；无关联则填null
- industry_news: 只引用提供的新闻数据
  - line_type 判断标准：必须站在**目标公司的角度**，沿传导链路判断最终影响的是收入端还是成本端：
    - topline: 最终影响公司**收入端**（如"铜供应短缺→铜价上涨→公司铜产品售价提升"→topline；"镍配额削减→镍供给收缩→镍价上涨→公司镍产品售价提升"→topline，虽然含"供给"二字但对生产商而言影响的是售价即收入端）
    - bottomline: 最终影响公司**成本/利润端**（如"电价上涨→冶炼成本增加且无法转嫁"→bottomline）
    - both: 同时影响收入和成本两端
    - context: 宏观背景信息，不直接沿传导链路影响公司收入或成本（如宏观经济指标、货币政策预期、行业总览性描述）
    - 关键原则：分类取决于**对目标公司的传导终点**，而非关键词字面含义。同一个事件对不同公司可能分类不同
  - tagged_drivers: 关联到商业模式分析中的driver名称列表（可为空数组）
  - sentiment: positive=利好，negative=利空，neutral=中性
  - summary: 100字以内的新闻摘要
  - is_recent: 基于新闻日期判断（近7天内=true）
- industry_data.related_driver/industry_news.tagged_drivers: 参照上方注入的"商业模式穷举因素（供引用）"中的名称，交叉关联
- _for_valuation.downstream_demand_analysis:
  - downstream_industries 必须覆盖 Step1 customer_industries 的所有行业
  - weight_pct 取自 Step1 customer_industries.pct，若无数据按 revenue_segments 推算；**所有行业 weight_pct 之和必须严格等于100%**
  - growth_rate 数据搜索策略（三级降级，禁止因为精准数据找不到就留空）：
    * L1 精准匹配：直接搜该行业增速（如"新能源电池制造行业增速2024"）
    * L2 相关/细分行业代理：若L1找不到，改用细分子行业或上游传导指标（如"动力电池出货量增速""锂电池产量同比"）
    * L3 终端需求代理：若L2仍无数据，用终端应用的产量/装机量/销量替代（如"新能源汽车销量增速""储能装机量增速"）
    * 对于对比年（当年/上年）：**无需全年数据**，某季度、某月、某时点的切片数据均可使用，但 source 字段必须标注确切含义（如"2024年1-9月累计同比+X%"、"2024Q3单季度同比+X%"）
    * 最终实在无数据时才用LLM估算，标 confidence=low，并在 source 中注明"无直接数据，行业知识估算"
  - growth_rate 优先引用 RAG 注入的实际数据，其次引用行业共识，最后才用 LLM 估算（标 low confidence）
  - weighted_growth_rate.calculation 必须写出完整加权公式
- _for_valuation.capture_rate_analysis:
  - porter_capture_analysis 每一力的分析必须以 capture_verdict 或 pricing_implication/cost_implication 结尾，回答"对公司抢占新增需求/定价/成本的影响"
  - supply_capacity.expansion_projects 区分 direct（公告）和 indirect（推断），indirect 必须写推断依据
  - barrier_types 穷举所有适用的壁垒类型（从10类中选）
  - substitute_products 穷举所有潜在替代品，每个标注渗透趋势
- _for_valuation.price_volume_analysis:
  - pricing_mechanism 必须从7种枚举中选取
  - balance_verdict 必须从5种枚举中选取
  - price_trend_factors 每个标注方向和幅度
  - price_adjustment_conclusion.breakdown 必须写出各因素加总的计算过程
  - cost_passthrough 用于 Step3 成本端分析，必须准确判断
- _for_valuation.segment_industry_context: 保留但精简，每个分部只需 TAM 和数据来源"""

# ==================== STEP3 模块化 Schema 常量 ====================

def _build_method_selection(step1_result: dict, stock_info: dict) -> dict:
    """程序化确定各分部估值方法，返回 {segment_name: {method, asset_type, notes}}

    使用 valuation_method_matrix 替代旧的 _INDUSTRY_METHOD_MAP，规则更完整（31个行业）。
    """
    from research.valuation_method_matrix import select_method_for_industry as _matrix_select
    industry_l1 = (stock_info or {}).get("industry_l1", "")
    industry_l2 = (stock_info or {}).get("industry_l2", "")
    segments = (step1_result or {}).get("revenue_segments_current") or \
               (step1_result or {}).get("revenue_segments") or []

    def _rec(l1, l2):
        rule = _matrix_select(l1, l2)
        primary = rule.get("primary", ["PE"])
        return {
            "method": primary[0],
            "notes": rule.get("notes", ""),
            "industry_l1": l1,
            "industry_l2": l2,
            "forbidden": rule.get("forbidden", []),
            "_matched_by": rule.get("_matched_by", ""),
        }

    result = {}
    if not segments:
        result["整体业务"] = _rec(industry_l1, industry_l2)
    else:
        for seg in segments:
            name = seg.get("name", "未知分部")
            result[name] = _rec(industry_l1, industry_l2)
    return result


# ── 基座 schema（所有方法必出）──
FINANCIAL_BASE_SCHEMA = '''\
    "historical_financials": {
        "periods": ["2021", "2022", "2023", "最近一期（半年报或季报）"],
        "income_statement": [
            {"item": "项目名", "values": [数值1, 数值2, 数值3, 数值4], "unit": "亿元", "latest_yoy_pct": 同比百分比或null}
        ],
        "key_ratios": [
            {"ratio": "指标名", "values": [数值1, 数值2, 数值3, 数值4], "unit": "%或倍或亿元", "trend": "改善/恶化/稳定"}
        ]
    },
    "forward_outlook": [
        {
            "metric": "指标名",
            "direction": "expected_growth/expected_decline/stable",
            "magnitude": "幅度预估（如+15%~20%）",
            "confidence": "high/medium/low",
            "reasoning_chain": [
                {"step": 1, "logic": "driver变化→传导→指标影响"}
            ],
            "evidence": ["数据证据"],
            "risk_factors": ["风险"]
        }
    ],
    "dupont": {
        "summary": "ROE变动核心驱动（80字）",
        "periods": [{"period": "年度", "roe": 百分比, "profit_margin": 百分比, "asset_turnover": 数值, "equity_multiplier": 数值}]
    },
    "balance_sheet_summary": {
        "total_assets": 数值或null, "total_liabilities": 数值或null,
        "equity": 数值或null, "cash_and_equivalents": 数值或null,
        "debt_to_equity": 百分比或null, "comment": "要点（80字）"
    },
    "assumption_tracker": [
        {"item": "假设描述", "category": "topline/bottomline", "driver_name": "driver名",
         "assumed_value": "假设值", "actual_value": "真实值或null",
         "status": "actual_replaced/assumption/partially_confirmed", "source": "来源或null"}
    ],
    "financial_health": "财务健康度总评（80字）",
    "mda_summary": "管理层讨论与分析摘要（200字）"'''

FINANCIAL_BASE_REQUIREMENTS = '''\
- historical_financials.income_statement: 至少含 营业收入/营业成本/毛利润/销管研费/营业利润/净利润，periods=3完整年度+最近一期
- historical_financials.key_ratios: 至少含 毛利率/净利率/ROE/资产负债率
- forward_outlook: 每个指标≥3步 reasoning_chain + ≥1 evidence + ≥1 risk_factor
- dupont: ROE三因子分解，按年度
- balance_sheet_summary: 无数据字段设null，不得编造
- assumption_tracker: 列出所有关键假设，有实际数据替换的标 actual_replaced'''

# ── revenue_model 模块（PE/PS/DCF/EV_EBITDA）──
FINANCIAL_REVENUE_MODEL_SCHEMA = '''\
    "revenue_model": {
        "methodology": "建模方法论（量×价拆解，80字）",
        "segments": [
            {
                "segment_name": "分部名",
                "segment_revenue_latest": 最新年营收亿元,
                "segment_pct": 占比百分比,
                "segment_gross_margin_pct": 毛利率或null,
                "model_formula": "驱动公式（如出货量×单价）",
                "industry_baseline": {
                    "l1_downstream_growth_pct": 来自程序预填的数字或null,
                    "l1_calculation": "来自程序预填的加权公式或null",
                    "l2_capture_adj_pct": 来自程序预填的数字或null,
                    "l2_reason": "来自程序预填的份额判断或null",
                    "l3_price_adj_pct": 来自程序预填的数字或null,
                    "l3_reason": "来自程序预填的价格判断或null",
                    "baseline_revenue_growth_pct": 来自程序预填的基准收入增速数字或null,
                    "source": "program_derived或no_data"
                },
                "key_drivers": [
                    {
                        "driver_name": "从topline_indicators选取",
                        "driver_type": "volume/price/mix/market_share",
                        "unit": "单位",
                        "historical": [{"period": "2022", "value": 数值, "source": "actual/estimated"}],
                        "current_value": {"value": 数值或null, "source": "来源", "is_actual": true或false},
                        "forecast": [{"period": "2025E", "value": 数值, "basis": "依据", "confidence": "high/medium/low"}],
                        "industry_constraint": "产业约束或null"
                    }
                ],
                "revenue_history": [{"period": "2022", "revenue": 亿元, "yoy_pct": 百分比}],
                "revenue_forecast": [
                    {
                        "period": "2025E",
                        "baseline_growth_pct": 与industry_baseline.baseline_revenue_growth_pct一致的数字或null,
                        "adjusted_growth_pct": LLM最终调整后增速百分比,
                        "adjustment_rationale": "产业基准X%，历史均值Y%，最新实际Z%，调整原因和幅度（80字）",
                        "revenue": 亿元,
                        "yoy_pct": 与adjusted_growth_pct一致,
                        "confidence": "high/medium/low"
                    }
                ],
                "model_vs_actual": "模型反推与实际对比（80字）"
            }
        ],
        "total_revenue_forecast": [{"period": "2025E", "revenue": 亿元, "yoy_pct": 百分比}]
    }'''

FINANCIAL_REVENUE_REQUIREMENTS = '''\
- revenue_model.segments[].key_drivers[].driver_name: 必须从上方注入的 topline_indicators 的 name 中选取
- revenue_model.segments[].model_vs_actual: 模型反推历史营收偏差>10%需解释
- revenue_model.total_revenue_forecast: 汇总各分部预测到整体
- key_drivers[] 推导规则（严格执行）：
  1. 有真实数据（已验证/actual）→ historical必须引用，forecast.basis写推导链，source标actual
  2. 有估算数据（estimated）→ confidence最高medium，basis注明"基于估算"
  3. 无数据 → confidence只能low，basis写"无可用数据，基于行业假设"，不得编造historical
- revenue_model.segments[].industry_baseline: 必须原样引用 user_message 中"产业推导收入基准"的数值
  - source=program_derived 时，baseline_revenue_growth_pct 不得修改
  - source=no_data 时，所有字段设 null
- revenue_model.segments[].revenue_forecast 推导规则（严格执行）：
  1. baseline_growth_pct: 原样引用 industry_baseline.baseline_revenue_growth_pct（来自程序预填）
  2. adjusted_growth_pct: LLM 在基准基础上做有限调整，必须完成以下 MD&A 对比：
     a. 拿 historical_avg_growth_pct（历史3年均值）与基准对比
     b. 拿 latest_actual_growth_pct（最近一期实际增速）与基准对比
     c. 解释偏差原因（产能约束/管理层指引/季节性/并购/会计政策等）
     d. 在基准上做调整，调整幅度超过10pp时 confidence 最高 medium
  3. adjustment_rationale: 必须写清楚"产业基准X%，历史均值Y%，最新实际Z%，因[原因]调整为W%"
  4. yoy_pct: 必须等于 adjusted_growth_pct
- 无 Step2 产业基准（source=no_data）时：
  - adjusted_growth_pct 基于历史趋势外推，confidence 最高 low
  - adjustment_rationale 写 "无产业传导基准，基于历史[X]%均值外推"'''

# ── profit_model 模块（PE/DCF/EV_EBITDA）──
FINANCIAL_PROFIT_MODEL_SCHEMA = '''\
    "profit_model": {
        "methodology": "利润建模方法论（80字）",
        "cogs_drivers": [
            {
                "driver_name": "从bottomline_indicators选取",
                "cost_category": "成本项（原材料/人工/制造费用/能源）",
                "pct_of_cogs": 占比百分比或null,
                "procurement_mode": "spot/long_term/self_supply/hedged",
                "current_status": "当前状态描述（30字）",
                "layer1_upstream": {
                    "price_change_pct": 上游价格变动百分比或null,
                    "cost_weight_pct": 该成本项占COGS百分比或null,
                    "passthrough_coeff": 传导系数0到1,
                    "raw_impact_pct": 对毛利率的原始影响百分比,
                    "calculation": "投入品价格X% × 占比Y% × 传导系数Z = 影响W%"
                },
                "layer2_efficiency": {
                    "scale_effect_pct": 规模效应百分比负为改善,
                    "efficiency_improvement_pct": 效率改善百分比负为改善,
                    "net_offset_pct": 合计对冲百分比负为改善,
                    "basis": "产能利用率变化/工艺改善/自动化依据（40字）"
                },
                "layer3_passthrough": {
                    "ability": "full/partial/none",
                    "lag_months": 传导时滞月数或null,
                    "passed_to_revenue_pct": 已传导至收入端的百分比,
                    "net_cost_impact_pct": 剥离传导后的净成本影响百分比,
                    "basis": "引用Step2 cost_passthrough结论（40字）"
                },
                "layer4_special": {
                    "factors": [
                        {"name": "汇率/利率/税收/环保/碳成本/减值等", "impact_pct": 百分比, "probability": "high/medium/low"}
                    ],
                    "net_special_pct": 特殊因素合计影响百分比,
                    "basis": "特殊因素列举及依据（40字）"
                },
                "total_cost_impact_pct": L1原始影响减L2对冲减L3传导加L4特殊的净值,
                "summary": "四层推导汇总：上游X% - 效率Y% - 传导Z% + 特殊W% = 净V%（30字）"
            }
        ],
        "opex_structure": {
            "rd_pct": [{"period": "2022", "pct": 百分比}, {"period": "2025E", "pct": 百分比, "basis": "依据"}],
            "sm_pct": [{"period": "2022", "pct": 百分比}, {"period": "2025E", "pct": 百分比, "basis": "依据"}],
            "ga_pct": [{"period": "2022", "pct": 百分比}, {"period": "2025E", "pct": 百分比, "basis": "依据"}],
            "operating_leverage": "经营杠杆（50字）"
        },
        "margin_forecast": [
            {"period": "2025E", "gross_margin_pct": 毛利率, "operating_margin_pct": 营业利润率, "net_margin_pct": 净利率, "basis": "逻辑（40字）"}
        ],
        "net_profit_forecast": [{"period": "2025E", "net_profit": 亿元, "yoy_pct": 百分比}],
        "eps_forecast": [{"period": "2025E", "eps": 元, "yoy_pct": 百分比, "share_count_assumption": "股本假设"}]
    }'''

FINANCIAL_PROFIT_REQUIREMENTS = '''\
- profit_model.cogs_drivers[].driver_name: 必须从上方注入的 bottomline_indicators 的 name 中选取
- profit_model.eps_forecast: 必须包含最近完整年 + 未来2年预测
- cogs_drivers[] 四层结构规则（严格执行）：
  - procurement_mode 枚举: spot/long_term/self_supply/hedged（必选其一）
  - procurement_mode → passthrough_coeff 基准: spot≈1.0, long_term≈0~0.3, self_supply≈0, hedged=1-套保比例
  - layer1_upstream.calculation: 必须写明"投入品价格X% × 占比Y% × 传导系数Z = 影响W%"
    - price_change_pct 引用 RAG上游价格数据 或 Step2 upstream_costs；无数据设null，passthrough_coeff设0
  - layer2_efficiency: 必须引用 Step2 supply_capacity（产能利用率变化）
    - scale_effect_pct: 收入增速/产能利用率提升带来的单位固定成本摊薄（负值=改善）
    - efficiency_improvement_pct: 工艺/自动化/供应链改善（负值=改善）
  - layer3_passthrough: 必须引用 Step2 price_volume_analysis.cost_passthrough
    - passed_to_revenue_pct 必须与 revenue_forecast 的 l3_price_adj 在逻辑上一致
    - ability=full → net_cost_impact_pct≈0；ability=none → net_cost_impact_pct≈layer1 raw_impact
  - layer4_special: 汇率/利率/税收/环保/碳成本/减值；无特殊因素可设 factors=[], net_special_pct=0
  - total_cost_impact_pct: 必须等于 layer1.raw_impact - layer2.net_offset - layer3.passed + layer4.net_special
  - current_status: 有实际数据必须引用，无数据设"无可用数据"
- profit_model.opex_structure: 三费率预测需引用经营杠杆分析
  - 固定成本占比高(>50%)的行业: 收入增长X%时费用率预计下降Y%，写出推导
  - 固定成本占比低(<30%)的行业: 费用率基本稳定，写出理由'''

# ── cash_flow_model 模块（仅 DCF）──
FINANCIAL_CASHFLOW_SCHEMA = '''\
    "cash_flow_model": {
        "ocf_history": [{"period": "2022", "ocf": 亿元或null, "net_income": 亿元, "ratio": OCF/NI或null}],
        "cash_conversion_assessment": "盈利质量评估（80字）",
        "working_capital": {
            "dso": {"latest": 天数或null, "trend": "改善/恶化/稳定"},
            "dio": {"latest": 天数或null, "trend": "改善/恶化/稳定"},
            "dpo": {"latest": 天数或null, "trend": "改善/恶化/稳定"},
            "ccc": {"latest": 天数或null, "comment": "评价（30字）"}
        },
        "capex": {
            "capex_pct_of_revenue": [{"period": "2022", "pct": 百分比或null}],
            "maintenance_vs_growth": "维护性X%/扩张性Y%（30字）"
        },
        "fcf_forecast": [{"period": "2025E", "fcf": 亿元, "basis": "推导依据（40字）"}],
        "wacc_assumptions": {
            "risk_free_rate": 无风险利率百分比,
            "equity_risk_premium": 股权风险溢价百分比,
            "beta": 贝塔系数或null,
            "wacc": WACC百分比,
            "terminal_growth_rate": 永续增长率百分比
        }
    }'''

FINANCIAL_CASHFLOW_REQUIREMENTS = '''\
- cash_flow_model: 仅DCF适用；ocf_history来自实际财务数据，wacc_assumptions给出合理假设依据'''

# ── book_value_analysis 模块（仅 PB）──
FINANCIAL_BOOK_VALUE_SCHEMA = '''\
    "book_value_analysis": {
        "net_assets": 净资产亿元,
        "bvps": 每股净资产元,
        "roic_history": [{"period": "2022", "roic": 百分比或null}],
        "asset_quality": "资产质量评价：商誉占比、应收坏账、存货跌价等（80字）",
        "pb_peers": [{"company": "公司名", "pb": PB倍数}]
    }'''

FINANCIAL_BOOK_VALUE_REQUIREMENTS = '''\
- book_value_analysis: 仅PB方法适用；需分析商誉减值风险及资产质量'''

# ── EBITDA add-on（追加到 profit_model 尾部，仅 EV_EBITDA）──
FINANCIAL_EBITDA_ADDON = '''\
        ,"ebitda_forecast": [{"period": "2025E", "ebitda": 亿元, "da": 折旧摊销亿元, "basis": "依据"}],
        "net_debt": 净债务亿元或null'''

# ── sensitivity_analysis（通用可选）──
FINANCIAL_SENSITIVITY_SCHEMA = '''\
    "sensitivity_analysis": [
        {
            "driver_name": "关键假设",
            "base_case": "基准值",
            "bull_case": {"value": "乐观值", "revenue_impact_pct": 百分比, "profit_impact_pct": 百分比},
            "bear_case": {"value": "悲观值", "revenue_impact_pct": 百分比, "profit_impact_pct": 百分比}
        }
    ]'''

# ── _for_valuation（保留，供 Step4 使用）──
FINANCIAL_FOR_VALUATION_SCHEMA = '''\
    "_for_valuation": {
        "segment_financials": [
            {
                "segment_name": "分部名称",
                "latest_quarterly": {"period": "如2025Q3", "revenue": 数值或null, "estimated_gross_margin_pct": 毛利率或null},
                "annual_history": [{"year": "2024", "revenue": 数值或null, "growth_yoy_pct": 同比增速或null}],
                "driver_financial_mapping": [
                    {"driver_name": "驱动因素名", "period": "期间", "driver_value": 数值或null,
                     "driver_unit": "单位", "implied_asp": 数值或null, "asp_unit": "单价单位",
                     "derivation": "推导过程", "data_quality": "actual/estimated"}
                ]
            }
        ],
        "valuation_ready_data": {
            "latest_pe_ttm": 数值或null, "latest_pb": 数值或null,
            "latest_ps_ttm": 数值或null, "latest_ev_ebitda": 数值或null,
            "fcf_latest_annual": 数值或null, "net_cash_or_debt": 数值或null,
            "total_shares": 数值或null, "latest_price": 数值或null, "market_cap": 数值或null
        }
    }'''

# ── 旧字段兼容（保留 income_comparison，供旧前端 fallback）──
FINANCIAL_INCOME_COMPARISON_SCHEMA = '''\
    "income_comparison": [
        {"item": "指标名", "periods": [{"period": "年份", "value": 数值}], "change_pct": 同比变化百分比或null, "comment": "简评（30字）"}
    ]'''

FINANCIAL_VARIANCE_SCHEMA = '''\
    "variance_analysis": {
        "revenue_variance": [
            {
                "segment_name": "分部名称",
                "industry_baseline_pct": 程序预填的基准收入增速数字或null,
                "historical_avg_growth_pct": 历史3年平均营收增速百分比或null,
                "historical_periods": "如2021-2023",
                "latest_actual_growth_pct": 最近一期实际营收增速百分比或null,
                "latest_actual_period": "如2024H1",
                "final_forecast_pct": 与revenue_forecast.adjusted_growth_pct一致,
                "variance_vs_baseline_pct": final_forecast_pct减industry_baseline_pct,
                "variance_explanation": "产业基准X%，历史均值Y%，最新实际Z%，偏差原因：[具体分析80字，不得泛泛而谈]",
                "risk_to_forecast": "上行风险：XXX可达X%；下行风险：XXX则降至X%（50字）"
            }
        ],
        "cost_variance": [
            {
                "driver_name": "与cogs_drivers中的driver_name一致",
                "industry_signal": "Step2上游价格信号及供应商议价描述（40字）",
                "historical_cogs_growth_pct": 历史成本增速百分比或null,
                "model_total_impact_pct": 与cogs_drivers.total_cost_impact_pct一致,
                "variance_explanation": "四层推导结果X%，历史成本增速Y%，主要差异来源：[具体分析60字]",
                "key_assumption_risk": "若[关键假设]变化，净成本影响从X%变为Y%（50字）"
            }
        ],
        "margin_bridge": {
            "current_gross_margin_pct": 当前毛利率百分比或null,
            "revenue_mix_effect_pct": 收入结构变化对毛利率的pp影响,
            "cost_improvement_effect_pct": 成本改善对毛利率的pp影响,
            "price_effect_pct": 价格变动对毛利率的pp影响,
            "other_effect_pct": 其他因素pp影响或null,
            "forecast_gross_margin_pct": 预测毛利率百分比,
            "bridge_narrative": "毛利率从X%→Y%：收入结构±Zpp + 成本改善±Wpp + 价格±Vpp（60字）"
        }
    }'''

FINANCIAL_VARIANCE_REQUIREMENTS = '''\
- variance_analysis.revenue_variance: 每个有industry_baseline的segment必须出现
  - industry_baseline_pct: 原样引用程序预填值（no_data时设null）
  - historical_avg_growth_pct: 基于historical_financials.income_statement计算
  - latest_actual_growth_pct: 基于财务数据最近一期计算
  - variance_explanation: 必须同时引用三个参照系（基准/历史/最新实际），不得泛泛而谈
  - risk_to_forecast: 必须给出具体的上下行触发条件和影响幅度
- variance_analysis.cost_variance: 每个cogs_driver必须出现
  - model_total_impact_pct: 必须与profit_model.cogs_drivers[].total_cost_impact_pct严格一致
  - variance_explanation: 必须解释四层推导结果与历史成本增速的差异原因
- variance_analysis.margin_bridge:
  - 各效应之和必须约等于 forecast_gross_margin_pct - current_gross_margin_pct
  - bridge_narrative 必须把数字和原因都写清楚'''

FINANCIAL_PROMPT_TEMPLATE = """你是财务分析专家。请基于以下财务数据，进行估值导向的财务建模分析。

重要规则：
- 优先使用提供的实际财务数据，其次用产业约束推算，最后才用行业平均假设
- 无数据字段设 null，不得编造数字；推算数据必须在 assumption_tracker 中标注
- driver_name 必须从注入的穷举因素清单中选取，不得另起名

请输出严格的JSON格式：
{{
{schema}
}}

要求：
{requirements}
- 数据来源优先级（严格执行）：
  1. 已验证数据（Step1/Step2结论）> 2. RAG actual > 3. RAG estimated > 4. 行业假设(标low)
- forward_outlook 每条≥3步 reasoning_chain + ≥1 evidence + ≥1 risk_factor
- 无数据字段设 null，不编造"""


def _build_financial_prompt(method_selection: dict) -> str:
    """根据估值方法需求，动态组合 STEP3 prompt schema"""
    methods_used = set()
    for seg_info in method_selection.values():
        methods_used.add(seg_info.get("method", "PE"))

    schema_parts = [FINANCIAL_BASE_SCHEMA]
    requirement_parts = [FINANCIAL_BASE_REQUIREMENTS]

    needs_revenue = bool(methods_used & {"PE", "PS", "DCF", "EV_EBITDA"})
    needs_profit = bool(methods_used & {"PE", "DCF", "EV_EBITDA"})
    needs_cashflow = "DCF" in methods_used
    needs_book_value = "PB" in methods_used
    needs_ebitda = "EV_EBITDA" in methods_used

    if needs_revenue:
        schema_parts.append(FINANCIAL_REVENUE_MODEL_SCHEMA)
        requirement_parts.append(FINANCIAL_REVENUE_REQUIREMENTS)
    if needs_profit:
        if needs_ebitda:
            # 在 profit_model 末尾追加 ebitda 字段
            schema_parts.append(FINANCIAL_PROFIT_MODEL_SCHEMA.rstrip().rstrip('}') +
                                 FINANCIAL_EBITDA_ADDON + "\n    }")
        else:
            schema_parts.append(FINANCIAL_PROFIT_MODEL_SCHEMA)
        requirement_parts.append(FINANCIAL_PROFIT_REQUIREMENTS)
    if needs_cashflow:
        schema_parts.append(FINANCIAL_CASHFLOW_SCHEMA)
        requirement_parts.append(FINANCIAL_CASHFLOW_REQUIREMENTS)
    if needs_book_value:
        schema_parts.append(FINANCIAL_BOOK_VALUE_SCHEMA)
        requirement_parts.append(FINANCIAL_BOOK_VALUE_REQUIREMENTS)

    # variance_analysis：有 revenue 或 profit 时即追加
    if needs_revenue or needs_profit:
        schema_parts.append(FINANCIAL_VARIANCE_SCHEMA)
        requirement_parts.append(FINANCIAL_VARIANCE_REQUIREMENTS)

    schema_parts.append(FINANCIAL_SENSITIVITY_SCHEMA)
    schema_parts.append(FINANCIAL_FOR_VALUATION_SCHEMA)
    schema_parts.append(FINANCIAL_INCOME_COMPARISON_SCHEMA)  # 向后兼容

    schema_json = ",\n".join(schema_parts)
    requirements_text = "\n".join(requirement_parts)

    # 计算 max_tokens：基础 4096 + 每增加一个扩展模块 +1024；variance 模块额外 +1024
    extra_modules = sum([needs_revenue, needs_profit, needs_cashflow, needs_book_value])
    variance_bonus = 1024 if (needs_revenue or needs_profit) else 0

    return FINANCIAL_PROMPT_TEMPLATE.format(
        schema=schema_json,
        requirements=requirements_text,
    ), 4096 + extra_modules * 1024 + variance_bonus

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


def _get_margin_trading_trend(stock_code: str):
    """获取最近5条融资余额趋势，用于 _calc_sector_heat 返回值。"""
    try:
        from utils.db_utils import ensure_stock_extra_data
        ensure_stock_extra_data(stock_code)
        rows = execute_query(
            "SELECT trade_date, margin_balance, short_balance, total_balance "
            "FROM margin_trading WHERE stock_code=%s ORDER BY trade_date DESC LIMIT 5",
            [stock_code],
        )
        if not rows:
            return None
        return [
            {
                "date": str(r.get("trade_date", "")),
                "margin_balance": r.get("margin_balance"),
                "short_balance": r.get("short_balance"),
                "total_balance": r.get("total_balance"),
            }
            for r in rows
        ]
    except Exception:
        return None


def _get_institutional_summary(stock_code: str):
    """获取最近2期机构持仓变动摘要。"""
    try:
        from utils.db_utils import ensure_stock_extra_data
        ensure_stock_extra_data(stock_code)
        rows = execute_query(
            "SELECT report_date, institution_type, hold_ratio, hold_change, hold_value "
            "FROM institutional_holding WHERE stock_code=%s ORDER BY report_date DESC LIMIT 10",
            [stock_code],
        )
        if not rows:
            return None
        # 按 report_date 分组，取最新两期
        periods: dict = {}
        for r in rows:
            rd = str(r.get("report_date", ""))
            periods.setdefault(rd, []).append(r)
        sorted_periods = sorted(periods.keys(), reverse=True)[:2]
        result = []
        for rd in sorted_periods:
            for r in periods[rd]:
                result.append({
                    "report_date": rd,
                    "institution_type": r.get("institution_type"),
                    "hold_ratio": r.get("hold_ratio"),
                    "hold_change": r.get("hold_change"),
                    "hold_value": r.get("hold_value"),
                })
        return result if result else None
    except Exception:
        return None


def _get_insider_summary(stock_code: str):
    """获取最近90天增减持明细。"""
    try:
        from utils.db_utils import ensure_stock_extra_data
        ensure_stock_extra_data(stock_code)
        rows = execute_query(
            "SELECT trade_date, person_name, person_role, direction, trade_amount, hold_shares_after "
            "FROM insider_trading WHERE stock_code=%s "
            "AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY) "
            "ORDER BY trade_date DESC LIMIT 20",
            [stock_code],
        )
        if not rows:
            return None
        return [
            {
                "date": str(r.get("trade_date", "")),
                "person": r.get("person_name"),
                "role": r.get("person_role"),
                "direction": r.get("direction"),
                "amount": r.get("trade_amount"),
            }
            for r in rows
        ]
    except Exception:
        return None


def _get_etf_list(stock_code: str):
    """获取该股所在ETF列表（取最新期）。"""
    try:
        from utils.db_utils import ensure_stock_extra_data
        ensure_stock_extra_data(stock_code)
        rows = execute_query(
            "SELECT etf_code, etf_name, weight, report_date "
            "FROM etf_constituent WHERE stock_code=%s ORDER BY report_date DESC LIMIT 30",
            [stock_code],
        )
        if not rows:
            return None
        # 取最新 report_date 的数据
        latest_date = str(rows[0].get("report_date", ""))
        latest = [r for r in rows if str(r.get("report_date", "")) == latest_date]
        return [
            {"etf_code": r.get("etf_code"), "etf_name": r.get("etf_name"), "weight": r.get("weight")}
            for r in latest
        ]
    except Exception:
        return None


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
        # 增量数据字段：从本地缓存表查询（ensure 后查）
        "margin_trading": _get_margin_trading_trend(stock_code),
        "institutional_holdings": _get_institutional_summary(stock_code),
        "block_trades": None,  # block_trade 表为空，跳过
        "insider_changes": _get_insider_summary(stock_code),
        "etf_membership": _get_etf_list(stock_code),
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


def _postprocess_value_chain(result, step1_result):
    """后处理产业链结果：
    1. 用 bm 层 topline_drivers/bottomline_drivers 修正 industry_data/industry_news 的 line_type
    2. 校验 industry_data value 单位
    """
    vc = _parse_step(result)
    if not isinstance(vc, dict):
        return result

    # 提取 bm 的 driver 名称列表
    bm = _parse_step(step1_result) if step1_result else {}
    if not isinstance(bm, dict):
        bm = {}
    tl_driver_names = {d.get("name", "") for d in (bm.get("topline_drivers") or []) if d.get("name")}
    bl_driver_names = {d.get("name", "") for d in (bm.get("bottomline_drivers") or []) if d.get("name")}

    # 通用语义分组表：这些关键词出现时倾向 topline/bottomline
    _TOPLINE_KEYWORDS = {"价格", "售价", "销售", "需求", "销量", "市场份额", "规模",
                         "出货", "订单", "收入", "贸易", "库存", "供需", "供给",
                         "产量", "配额", "金属价格", "铜价", "铝价", "镍价", "锡价",
                         "储备", "下游需求", "上游供给"}
    _BOTTOMLINE_KEYWORDS = {"成本", "费用", "原材料", "能源", "电价", "运费", "折旧",
                            "人工", "汇率", "利率", "税", "摊销", "环保", "利润",
                            "成本结构", "冶炼成本"}

    def _classify_by_drivers(drivers_or_driver):
        """根据 tagged_drivers/related_driver 判定 line_type
        三层策略：1) bm driver 精确匹配 2) 通用关键词分组 3) 无法判定返回 None
        """
        if isinstance(drivers_or_driver, list):
            names = drivers_or_driver
        elif isinstance(drivers_or_driver, str) and drivers_or_driver:
            names = [drivers_or_driver]
        else:
            return None

        # 第一层：精确匹配 bm driver
        hit_tl_exact = any(_fuzzy_match_driver(n, tl_driver_names) for n in names)
        hit_bl_exact = any(_fuzzy_match_driver(n, bl_driver_names) for n in names)
        if hit_tl_exact and hit_bl_exact:
            return "both"
        if hit_tl_exact:
            return "topline"
        if hit_bl_exact:
            return "bottomline"

        # 第二层：通用关键词分组
        all_text = " ".join(names)
        hit_tl_kw = any(kw in all_text for kw in _TOPLINE_KEYWORDS)
        hit_bl_kw = any(kw in all_text for kw in _BOTTOMLINE_KEYWORDS)
        if hit_tl_kw and hit_bl_kw:
            return "both"
        if hit_tl_kw:
            return "topline"
        if hit_bl_kw:
            return "bottomline"
        return None

    def _fuzzy_match_driver(name, driver_set):
        """模糊匹配 driver 名称（去掉括号内容，子串包含即可）"""
        import re
        name_clean = re.sub(r'[（(][^)）]*[)）]', '', name).strip()
        for dn in driver_set:
            dn_clean = re.sub(r'[（(][^)）]*[)）]', '', dn).strip()
            if name_clean in dn_clean or dn_clean in name_clean:
                return True
            # 关键词交叉：任何2字以上关键词匹配
            for kw in re.split(r'[、/·，,\s]+', name_clean):
                if len(kw) >= 2 and kw in dn_clean:
                    return True
        return False

    # 校验并归一化 downstream_industries weight_pct（加总必须=100%）
    fv = vc.get("_for_valuation") or {}
    dda = fv.get("downstream_demand_analysis") or {}
    dis = dda.get("downstream_industries") or []
    if dis:
        total_w = sum(float(d.get("weight_pct") or 0) for d in dis)
        if total_w > 0 and abs(total_w - 100.0) > 0.5:
            logger.warning(f"[postprocess] downstream weight_pct 加总={total_w:.1f}%，执行归一化")
            for d in dis:
                old_w = float(d.get("weight_pct") or 0)
                d["weight_pct"] = round(old_w / total_w * 100, 1)
            wgr = dda.get("weighted_growth_rate") or {}
            if wgr:
                wgr["_weight_normalized"] = f"原始权重加总{total_w:.1f}%，已按比例归一化至100%"

    # 修正 industry_data.line_type
    for d in (vc.get("industry_data") or []):
        driver_ref = d.get("related_driver", "")
        new_lt = _classify_by_drivers(driver_ref)
        if new_lt:
            d["line_type"] = new_lt
        # 如果 related_driver 模糊匹配不到，尝试用 metric 名做模糊匹配
        elif d.get("line_type") not in ("topline", "bottomline"):
            metric_lt = _classify_by_drivers(d.get("metric", ""))
            if metric_lt:
                d["line_type"] = metric_lt

    # 修正 industry_news.line_type
    # 注意：AI 判定为 context 的不覆盖（宏观背景不应被关键词硬改成 topline/bottomline）
    for n in (vc.get("industry_news") or []):
        if n.get("line_type") == "context":
            continue  # 尊重 AI 的 context 判断
        tagged = n.get("tagged_drivers") or []
        new_lt = _classify_by_drivers(tagged)
        if new_lt:
            n["line_type"] = new_lt
        # tagged_drivers 为空且 line_type 不明确时，尝试用 title 关键词
        elif not tagged and n.get("line_type") not in ("topline", "bottomline", "context"):
            title_words = n.get("title", "")[:20]
            title_lt = _classify_by_drivers(title_words)
            if title_lt and title_lt != "both":
                n["line_type"] = title_lt

    # 校验 industry_data value 单位：纯数字的根据 metric 推断补上
    import re
    _UNIT_HINTS = [
        # (metric关键词, 补充单位)
        (r"CAPEX|资本开支|投资额|市值|规模|产值|费用|收入|营收|利润", "亿美元"),
        (r"产量|需求|供应|供给|缺口|库存|出口|进口|配额|产能", "万吨"),
        (r"价格|均价|完全成本|单价|售价|成本", "美元/吨"),
        (r"CAGR|增速|增长率|占比|比例|利润率|毛利率|份额", "%"),
    ]
    for d in (vc.get("industry_data") or []):
        val = str(d.get("value", ""))
        val_stripped = re.sub(r'^[约~><=≈不足超]+', '', val.strip())
        if val_stripped and re.match(r'^[\d,.–-]+%?$', val_stripped):
            if val_stripped.endswith('%'):
                continue
            # 没有单位，根据 metric 推断
            metric = d.get("metric", "")
            guessed_unit = None
            for pattern, unit in _UNIT_HINTS:
                if re.search(pattern, metric):
                    guessed_unit = unit
                    break
            if guessed_unit:
                d["value"] = val + guessed_unit
            else:
                d["value"] = val + " (缺单位)"

    # 写回 result
    if isinstance(result, str):
        return json.dumps(vc, ensure_ascii=False)
    return vc


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
            # 注入 Step1 穷举因素供引用/交叉验证（Step2 不再生成，只引用）
            try:
                s1_parsed = _parse_step(step1_result) if isinstance(step1_result, str) else step1_result
                tl_inds = (s1_parsed or {}).get("topline_indicators") or []
                bl_inds = (s1_parsed or {}).get("bottomline_indicators") or []
                if tl_inds or bl_inds:
                    vc_input += "\n=== 商业模式穷举因素（供引用，industry_data/industry_news 请在 related_driver/tagged_drivers 中交叉关联）===\n"
                    if tl_inds:
                        vc_input += "Topline因素:\n"
                        for i in tl_inds[:12]:
                            vc_input += f"  [{i.get('category','')}] {i.get('name','')} — {i.get('current_status','')}\n"
                    if bl_inds:
                        vc_input += "Bottomline因素:\n"
                        for i in bl_inds[:12]:
                            vc_input += f"  [{i.get('category','')}] {i.get('name','')} — {i.get('current_status','')}\n"
                # 提取成本构成项名称，用于 bottomline category 对齐
                cb = (s1_parsed or {}).get("cost_breakdown_current") or (s1_parsed or {}).get("cost_breakdown") or []
                if cb and isinstance(cb, list):
                    cost_items = [c.get("name", "") for c in cb if c.get("name")]
                    if cost_items:
                        vc_input += f"\n=== 商业画布成本构成项（bottomline category 请对齐这些名称）===\n{', '.join(cost_items)}\n"
            except Exception:
                pass

        # RAG多主题检索注入
        try:
            from research.rag_context import search_stock_multi_topic, search_stock_context
            rag_ctx = search_stock_multi_topic(stock_code, stock_name, industry)
            if rag_ctx:
                vc_input += f"\n=== RAG检索到的最新产业链信息 ===\n{rag_ctx}\n"
            # 竞对RAG注入：对 top 2-3 个竞对补充竞争优势相关检索
            if peers:
                peer_rag_parts = []
                for p in peers[:3]:
                    peer_code = p.get('stock_code', '')
                    peer_name = p.get('stock_name', '')
                    if not peer_code or not peer_name:
                        continue
                    try:
                        peer_ctx = search_stock_context(
                            peer_code,
                            f"{peer_name} 竞争优势 市场份额 技术 渠道 成本",
                            top_k=3
                        )
                        if peer_ctx:
                            peer_rag_parts.append(f"【{peer_name}({peer_code})】\n{peer_ctx}")
                    except Exception as _pe:
                        logger.debug(f"竞对RAG检索跳过 {peer_code}: {_pe}")
                if peer_rag_parts:
                    vc_input += "\n=== 竞对RAG检索（用于竞争优势对比）===\n"
                    vc_input += "\n---\n".join(peer_rag_parts[:3]) + "\n"
        except Exception as _rag_e:
            logger.debug(f"产业链RAG检索跳过: {_rag_e}")

        # 产业需求传导数据注入（industry_demand_fetcher）
        s1_parsed_for_demand = _parse_step(step1_result) if isinstance(step1_result, str) else step1_result
        if not isinstance(s1_parsed_for_demand, dict):
            s1_parsed_for_demand = {}
        try:
            from research.industry_demand_fetcher import fetch_industry_demand_data
            demand_data = fetch_industry_demand_data(
                stock_code, stock_name, industry,
                customer_industries=s1_parsed_for_demand.get("customer_industries", []),
                revenue_segments=s1_parsed_for_demand.get("revenue_segments_current", []),
                cost_breakdown=s1_parsed_for_demand.get("cost_breakdown_current", []),
                step1_for_valuation=s1_parsed_for_demand.get("_for_valuation", {}),
            )
            if demand_data.get("injection_text"):
                vc_input += f"\n{demand_data['injection_text']}\n"
                logger.info(f"[Step2] industry_demand_fetcher 注入 {len(demand_data['injection_text'])}字")
        except Exception as _demand_e:
            logger.warning(f"[Step2] industry_demand_fetcher 跳过: {_demand_e}")

        result = call_claude(STEP2_VALUE_CHAIN_PROMPT, vc_input, max_tokens=8192)

        # 后处理：用 bm drivers 修正 line_type，校验 value 单位
        try:
            result = _postprocess_value_chain(result, step1_result)
        except Exception as _pp_e:
            logger.debug(f"产业链后处理跳过: {_pp_e}")

        if step_callback:
            step_callback("value_chain", result)
        return result
    except Exception as e:
        logger.error(f"产业链分析失败: {e}")
        return f"分析失败: {e}"


def _calc_l2_capture_adj(cra: dict) -> tuple[float, str]:
    """从 capture_rate_analysis 四维度计算 L2 份额调整幅度，返回 (adj_pct, breakdown_str)。

    四个维度：
      1. base_signal   — share_trend × confidence 基础信号
      2. capacity_cap  — capacity_adequacy 硬约束（insufficient 封顶 gaining）
      3. comp_discount — rivalry + substitutes 竞争强度折扣
      4. factors_bonus — adjustment_factors[] 逐条加权求和
    """
    crc = cra.get("capture_rate_conclusion") or {}
    sc = cra.get("supply_capacity") or {}
    pca = cra.get("porter_capture_analysis") or {}

    # ── 1. base_signal ───────────────────────────────────────
    share_trend = crc.get("share_trend", "stable")
    confidence = crc.get("confidence", "medium")
    _base = {"gaining": 4.0, "stable": 0.0, "losing": -4.0}.get(share_trend, 0.0)
    _conf_mult = {"high": 1.0, "medium": 0.75, "low": 0.5}.get(confidence, 0.75)
    base_signal = round(_base * _conf_mult, 2)

    # ── 2. capacity_cap ──────────────────────────────────────
    capacity_adequacy = sc.get("capacity_adequacy", "sufficient")
    cap_note = ""
    if capacity_adequacy == "insufficient":
        if base_signal > 0:
            cap_note = f"产能不足→上限封顶0（原{base_signal:+.1f}%）"
            base_signal = 0.0
        else:
            cap_note = "产能不足（份额已在流失，不额外惩罚）"
    elif capacity_adequacy == "tight":
        if base_signal > 2.0:
            cap_note = f"产能偏紧→上限封顶+2%（原{base_signal:+.1f}%）"
            base_signal = 2.0
        else:
            cap_note = "产能偏紧（未超上限）"

    # ── 3. comp_discount ─────────────────────────────────────
    rivalry_level = (pca.get("rivalry") or {}).get("level", "medium")
    sub_level = (pca.get("substitutes") or {}).get("level", "medium")
    high_count = sum(1 for lv in [rivalry_level, sub_level] if lv == "high")
    comp_discount = 1.0
    if high_count == 2:
        comp_discount = 0.60
    elif high_count == 1:
        comp_discount = 0.78
    # medium/low 均不折扣（竞争压力正常）
    comp_note = (f"竞争折扣×{comp_discount:.2f}"
                 f"（rivalry={rivalry_level}, substitutes={sub_level}）")

    # ── 4. factors_bonus ─────────────────────────────────────
    _mag_score = {"high": 1.5, "medium": 0.8, "low": 0.3}
    factors_sum = 0.0
    factor_notes = []
    for af in (crc.get("adjustment_factors") or [])[:6]:
        direction = af.get("direction", "")
        magnitude = af.get("magnitude", "low")
        factor_name = af.get("factor", "")
        score = _mag_score.get(magnitude, 0.3)
        delta = score if direction == "+" else -score if direction == "-" else 0.0
        factors_sum += delta
        factor_notes.append(f"{direction}{factor_name}({magnitude}){delta:+.1f}")
    # 钳制在 [-3, +3]
    factors_bonus = round(max(-3.0, min(3.0, factors_sum)), 2)

    # ── 合并 ─────────────────────────────────────────────────
    l2_adj = round(base_signal * comp_discount + factors_bonus, 1)
    # 最终钳制 [-8, +8]
    l2_adj = max(-8.0, min(8.0, l2_adj))

    breakdown = (
        f"基础{_base:+.0f}%×置信{_conf_mult:.2f}={base_signal:+.1f}%"
        + (f"，{cap_note}" if cap_note else "")
        + f"，{comp_note}"
        + (f"，因素修正[{' '.join(factor_notes[:3])}]={factors_bonus:+.1f}%" if factor_notes else "")
        + f" → L2={l2_adj:+.1f}%"
    )
    return l2_adj, breakdown


def _parse_price_pct(magnitude_pct_str, direction: str) -> float:
    """从 price_adjustment_conclusion.magnitude_pct 字符串解析数字。

    支持格式："+3%", "-5%", "3%", "5~10%"（取中值）, "约5%", "3pp"
    direction 用于消歧：下行且数字为正 → 取负。
    """
    import re as _re
    s = str(magnitude_pct_str).strip()
    # 范围格式 "5~10%" 或 "5-10%"：取中值
    range_match = _re.search(r"(\d+\.?\d*)\s*[~\-–]\s*(\d+\.?\d*)", s)
    if range_match:
        lo, hi = float(range_match.group(1)), float(range_match.group(2))
        val = round((lo + hi) / 2, 1)
    else:
        nums = _re.findall(r"[-+]?\d+\.?\d*", s)
        val = float(nums[0]) if nums else 0.0

    # 确保符号与 direction 一致
    if direction == "下行" and val > 0:
        val = -val
    elif direction == "上行" and val < 0:
        val = abs(val)
    # 平稳：保留原始符号（可能含微调）
    return val


def _compute_revenue_baseline(s2_parsed: dict, s1_parsed: dict) -> str:
    """从 Step2 _for_valuation 程序化计算各分部收入基准增速，返回注入文本。

    三层公式：
      L1 = downstream_demand_analysis.weighted_growth_rate.value_pct
      L2 = _calc_l2_capture_adj()（四维度：base_signal/capacity_cap/comp_discount/factors_bonus）
      L3 = price_volume_analysis.price_adjustment_conclusion（_parse_price_pct）
      baseline = (L1 + L2) × (1 + L3/100)
    """
    if not isinstance(s2_parsed, dict):
        return ""

    s2_fv = s2_parsed.get("_for_valuation") or {}
    dda = s2_fv.get("downstream_demand_analysis") or {}
    cra = s2_fv.get("capture_rate_analysis") or {}
    pva = s2_fv.get("price_volume_analysis") or {}

    wgr = dda.get("weighted_growth_rate") or {}
    pac = pva.get("price_adjustment_conclusion") or {}

    l1_growth = wgr.get("value_pct")
    l1_calc = wgr.get("calculation", "")

    # L2 — 四维度计算
    l2_adj, l2_breakdown = _calc_l2_capture_adj(cra)

    # L3 — 健壮解析
    l3_direction = pac.get("direction", "平稳")
    l3_price = _parse_price_pct(pac.get("magnitude_pct", "0"), l3_direction)
    sdb = pva.get("supply_demand_balance") or {}
    l3_reason = (f"{l3_direction}，供需={sdb.get('balance_verdict','')}"
                 f"，定价={pva.get('pricing_mechanism','')[:12]}")

    segments = (s1_parsed.get("revenue_segments_current") or
                s1_parsed.get("revenue_segments") or [])
    di_list = dda.get("downstream_industries") or []

    lines = ["\n=== 产业推导收入基准（程序计算，LLM 需对比历史财务后在 industry_baseline 字段原样引用并做 MD&A 调整）==="]

    if l1_growth is None:
        lines.append("  [无产业基准] Step2 未输出 weighted_growth_rate，所有分部 source=no_data")
        return "\n".join(lines) + "\n"

    # L2 拆解行（只输出一次，所有分部共用）
    lines.append(f"  L2份额调整: {l2_breakdown}")

    def _compute_one(seg_name: str, seg_pct: float) -> None:
        # L1：优先找该分部对应的下游行业预测增速，否则用整体加权
        seg_l1 = l1_growth
        seg_l1_src = l1_calc
        for di in di_list:
            if seg_name in (di.get("mapped_segments") or []):
                fc = (di.get("growth_rate") or {}).get("forecast") or {}
                if fc.get("value_pct") is not None:
                    seg_l1 = fc["value_pct"]
                    seg_l1_src = f"{di.get('industry_name','')}预测"
                    break

        volume_growth = round(seg_l1 + l2_adj, 1)
        baseline = round(volume_growth * (1 + l3_price / 100), 1) if l3_price != 0 else volume_growth
        pct_str = f"({seg_pct:.0f}%)" if seg_pct else ""
        lines.append(
            f"  [{seg_name}]{pct_str}"
            f" L1={seg_l1:+.1f}%（{seg_l1_src[:20]}）"
            f" + L2={l2_adj:+.1f}%"
            f" = 量增速{volume_growth:+.1f}%"
            f" × (1+价格{l3_price:+.1f}%，{l3_reason[:25]})"
            f" → 基准收入增速 {baseline:+.1f}%"
        )

    if segments:
        for seg in segments[:6]:
            _compute_one(seg.get("name", "未知分部"), seg.get("pct", 0))
    else:
        _compute_one("整体业务", 0)

    lines.append("  注：以上为产业传导基准，LLM 须在 industry_baseline 字段原样填入，再通过 MD&A 对比历史数据后给出 adjusted_growth_pct")
    return "\n".join(lines) + "\n"


def run_step_financial(stock_code, stock_name, step1_result=None, step2_result=None,
                        progress_callback=None, step_callback=None):
    """Step 3: 财务分析（估值导向模块化建模）"""
    if progress_callback:
        progress_callback(f"[3/6] 财务分析 ({stock_name})...")
    try:
        # === 1. 程序化确定估值方法 ===
        s1_parsed = _parse_step(step1_result) if step1_result else {}
        if not isinstance(s1_parsed, dict):
            s1_parsed = {}

        stock_info_rows = execute_query(
            "SELECT industry_l1, industry_l2, stock_name FROM stock_info WHERE stock_code=%s",
            [stock_code],
        )
        stock_info = stock_info_rows[0] if stock_info_rows else {}

        method_selection = _build_method_selection(s1_parsed, stock_info)
        logger.info(f"[Step3] 估值方法确定: {json.dumps(method_selection, ensure_ascii=False)}")

        # === 2. 构建动态 prompt ===
        fin_prompt, max_tokens = _build_financial_prompt(method_selection)

        # === 3. 构建 user_message ===
        fin_data = execute_query(
            """SELECT report_period, revenue, net_profit, roe, revenue_yoy, profit_yoy, eps
               FROM financial_reports WHERE stock_code=%s
               ORDER BY report_period DESC LIMIT 12""",
            [stock_code],
        )
        dp_input = f"股票: {stock_code} {stock_name}\n\n"

        # 估值方法（程序确定，非AI决定）
        dp_input += "=== 估值方法已确定（program决定，非AI决定）===\n"
        for seg_name, seg_info in method_selection.items():
            dp_input += (f"  [{seg_name}]: {seg_info['method']}"
                         f"（行业: {seg_info.get('industry_l2') or seg_info.get('industry_l1','')}"
                         f"，理由: {seg_info.get('notes','')}）\n")

        dp_input += "\n=== 财务数据（最近12个季度）===\n"
        for f in (fin_data or []):
            dp_input += (f"  {f.get('report_period','')}: 营收{f.get('revenue','')} "
                         f"净利{f.get('net_profit','')} ROE{f.get('roe','')}% "
                         f"营收YoY{f.get('revenue_yoy','')}% 利润YoY{f.get('profit_yoy','')}% "
                         f"EPS{f.get('eps','')}\n")
        if not fin_data:
            dp_input += "  暂无财务数据，请基于行业平均水平估算\n"

        # === 真实数据注入（indicator_data_fetcher V2）===
        tl_inds = s1_parsed.get("topline_indicators") or []
        bl_inds = s1_parsed.get("bottomline_indicators") or []
        s2_parsed = _parse_step(step2_result) if step2_result else {}
        if not isinstance(s2_parsed, dict):
            s2_parsed = {}

        try:
            from research.indicator_data_fetcher import fetch_indicator_data
            fetcher_result = fetch_indicator_data(
                stock_code, stock_name,
                stock_info.get("industry_l1", ""),
                tl_inds, bl_inds,
                s1_parsed, s2_parsed,
            )
            injection_text = fetcher_result.get("injection_text", "")
            if injection_text:
                dp_input += f"\n{injection_text}\n"
            logger.info(f"[Step3] fetcher注入: verified={len(fetcher_result.get('verified_data',[]))} "
                        f"tl_rag={len((fetcher_result.get('topline_rag') or {}).get('indicator_data',[]))} "
                        f"bl_rag={len((fetcher_result.get('bottomline_rag') or {}).get('indicator_data',[]))}")
        except Exception as _fetch_e:
            logger.warning(f"[Step3] indicator_data_fetcher 跳过: {_fetch_e}")

        # 穷举因素清单（来自 Step1）
        if tl_inds or bl_inds:
            dp_input += "\n=== 穷举因素清单（revenue_model/profit_model 的 driver_name 必须从此处选取）===\n"
            if tl_inds:
                dp_input += "Topline因素:\n"
                for i in tl_inds[:12]:
                    dp_input += (f"  [{i.get('category','')}] {i.get('name','')} "
                                 f"— {i.get('transmission_path','')} "
                                 f"| 现状: {i.get('current_status','')}\n")
            if bl_inds:
                dp_input += "Bottomline因素:\n"
                for i in bl_inds[:12]:
                    dp_input += (f"  [{i.get('category','')}] {i.get('name','')} "
                                 f"— {i.get('transmission_path','')} "
                                 f"| 现状: {i.get('current_status','')}\n")

        # 注入分部量价信息（来自 Step1._for_valuation）
        seg_drivers = (s1_parsed.get("_for_valuation") or {}).get("segment_drivers") or []
        if seg_drivers:
            dp_input += "\n=== 分部量价信息（来自商业模式分析）===\n"
            for sd in seg_drivers[:5]:
                dp_input += f"  [{sd.get('segment_name','')}]:\n"
                for d in (sd.get("drivers") or [])[:3]:
                    dp_input += (f"    量: {d.get('quantity_metric','')}={d.get('quantity_latest_value','null')}"
                                 f"（{d.get('quantity_source','')}）"
                                 f"  价: {d.get('price_metric','')}={d.get('price_latest_value','null')}"
                                 f"（{d.get('price_source','')}）\n")

        # 注入产业传导数据（来自 Step2._for_valuation 三大模块）
        if step2_result and not str(step2_result).startswith("分析失败") and isinstance(s2_parsed, dict):
            s2_fv = s2_parsed.get("_for_valuation") or {}

            # 下游需求传导
            dda = s2_fv.get("downstream_demand_analysis") or {}
            di_list = dda.get("downstream_industries") or []
            wgr = dda.get("weighted_growth_rate") or {}
            if di_list:
                dp_input += "\n=== 产业传导：下游需求分析（Step2, 用于收入三层推导第一层）===\n"
                for di in di_list[:6]:
                    gr = di.get("growth_rate") or {}
                    ar = gr.get("actual_recent") or {}
                    fc = gr.get("forecast") or {}
                    dp_input += (f"  [{di.get('industry_name','')}] 权重{di.get('weight_pct','')}% "
                                 f"→ 分部: {','.join(di.get('mapped_segments',[]))}\n")
                    if ar.get("value_pct") is not None:
                        dp_input += f"    实际增速: {ar['value_pct']}% ({ar.get('period','')}, {ar.get('source','')})\n"
                    if fc.get("value_pct") is not None:
                        dp_input += f"    预测增速: {fc['value_pct']}% ({fc.get('period','')}, {fc.get('confidence','')}, {fc.get('source','')})\n"
                if wgr.get("value_pct") is not None:
                    dp_input += f"  加权增长率: {wgr['value_pct']}% ({wgr.get('confidence','')}) — {wgr.get('calculation','')}\n"

            # Capture Rate
            cra = s2_fv.get("capture_rate_analysis") or {}
            crc = cra.get("capture_rate_conclusion") or {}
            sc = cra.get("supply_capacity") or {}
            if crc or sc:
                dp_input += "\n=== 产业传导：Capture Rate分析（Step2, 用于收入三层推导第二层）===\n"
                if sc.get("capacity_adequacy"):
                    dp_input += f"  产能充足性: {sc['capacity_adequacy']}\n"
                if sc.get("capacity_vs_demand"):
                    dp_input += f"  产能vs需求: {sc['capacity_vs_demand']}\n"
                if crc.get("share_trend"):
                    dp_input += f"  份额趋势: {crc['share_trend']}\n"
                if crc.get("capture_multiplier"):
                    dp_input += f"  Capture判断: {crc['capture_multiplier']} ({crc.get('confidence','')})\n"
                for af in (crc.get("adjustment_factors") or [])[:4]:
                    dp_input += f"    {af.get('direction','')}{af.get('factor','')} ({af.get('magnitude','')}) — {af.get('evidence','')}\n"

            # 量价分析
            pva = s2_fv.get("price_volume_analysis") or {}
            pac = pva.get("price_adjustment_conclusion") or {}
            cp = pva.get("cost_passthrough") or {}
            if pac or pva.get("pricing_mechanism"):
                dp_input += "\n=== 产业传导：量价分析（Step2, 用于收入第三层 + 成本第三层）===\n"
                if pva.get("pricing_mechanism"):
                    dp_input += f"  定价机制: {pva['pricing_mechanism']} — {pva.get('pricing_mechanism_detail','')}\n"
                sdb = pva.get("supply_demand_balance") or {}
                if sdb.get("balance_verdict"):
                    dp_input += f"  供需格局: {sdb['balance_verdict']}\n"
                if pva.get("price_volume_pattern"):
                    dp_input += f"  量价模式: {pva['price_volume_pattern']}\n"
                if pac.get("direction"):
                    dp_input += f"  价格调整结论: {pac.get('direction','')} {pac.get('magnitude_pct','')} ({pac.get('confidence','')})\n"
                    if pac.get("breakdown"):
                        dp_input += f"    拆分: {pac['breakdown']}\n"
                if cp.get("ability"):
                    dp_input += f"  成本传导能力: {cp['ability']}，时滞{cp.get('lag_months','未知')}个月\n"
                    if cp.get("mechanism"):
                        dp_input += f"    机制: {cp['mechanism']}\n"

            # 波特五力关键verdict（供成本端引用）
            pca = cra.get("porter_capture_analysis") or {}
            sp_power = pca.get("supplier_power") or {}
            if sp_power.get("cost_implication"):
                dp_input += f"\n=== 产业传导：上游议价（Step2, 用于成本第一层）===\n"
                dp_input += f"  供应商议价: {sp_power.get('level','')} — {sp_power['cost_implication']}\n"
                if sp_power.get("long_term_agreement_pct"):
                    dp_input += f"  长协占比: {sp_power['long_term_agreement_pct']}\n"
                if sp_power.get("backward_integration"):
                    dp_input += f"  后向一体化: {sp_power['backward_integration']}\n"

            # segment_industry_context（精简版）
            seg_ctx = s2_fv.get("segment_industry_context") or []
            if seg_ctx:
                dp_input += "\n=== 产业约束：分部TAM（来自产业链分析）===\n"
                for sc_item in seg_ctx[:5]:
                    dp_input += f"  [{sc_item.get('segment_name','')}]:\n"
                    tam = sc_item.get("tam") or {}
                    if tam.get("current_year_value"):
                        dp_input += f"    TAM当前: {tam['current_year_value']}{tam.get('unit','亿元')}（{tam.get('data_source','')}）\n"

        # === 程序预填收入基准（三层公式，注入后 LLM 必须引用）===
        try:
            dp_input += _compute_revenue_baseline(s2_parsed, s1_parsed)
        except Exception as _rb_e:
            logger.warning(f"[Step3] 收入基准计算跳过: {_rb_e}")

        if step1_result and not str(step1_result).startswith("分析失败"):
            bm_text = step1_result if isinstance(step1_result, str) else json.dumps(step1_result, ensure_ascii=False)
            dp_input += f"\n=== 商业模式分析（完整）===\n{bm_text[:3000]}\n"

        result = call_claude_json(fin_prompt, dp_input, max_tokens=max_tokens)

        # 将 method_selection 存入结果，供 Step4 直接复用
        if isinstance(result, dict):
            result["_method_selection"] = method_selection

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
                step2_result=step_results.get("value_chain"),
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

    # 不再将深度研究报告存入 content_summaries，避免污染 RAG 检索

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

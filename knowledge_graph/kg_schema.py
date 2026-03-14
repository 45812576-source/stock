"""KG Schema 常量集中管理

所有实体类型、关系类型、组合矩阵、冲突对等 schema 常量的唯一真实来源。
其他模块（kg_updater, kg_extractor_pipeline, kg_inspector）统一从此模块 import。
"""

# ==================== 实体类型 ====================

VALID_ENTITY_TYPES = {
    "market", "macro_indicator", "policy",
    "industry", "industry_chain", "theme",
    "company", "strategy",
    "commodity", "energy", "intermediate", "consumer_good",
}

# 实体类型 → 所属层级
ENTITY_LAYER_MAP = {
    "market": "市场层",
    "macro_indicator": "宏观层",
    "policy": "宏观层",
    "industry": "产业层",
    "industry_chain": "产业层",
    "theme": "产业层",
    "company": "公司层",
    "strategy": "公司层",
    "commodity": "要素层",
    "energy": "要素层",
    "intermediate": "要素层",
    "consumer_good": "要素层",
}

# 层级标签 → 包含的实体类型集合
LAYER_TO_TYPES = {
    "市场层": {"market"},
    "宏观层": {"macro_indicator", "policy"},
    "产业层": {"industry", "industry_chain", "theme"},
    "公司层": {"company", "strategy"},
    "要素层": {"commodity", "energy", "intermediate", "consumer_good"},
}


# ==================== 关系类型 ====================

VALID_RELATION_TYPES = {
    # 因果影响 (8)
    "causes_positive", "causes_negative", "cost_transmission",
    "indicator_transmission", "demand_driven", "supply_driven",
    "demand_source_of", "demand_substitute",
    # 结构归属 (10)
    "belongs_to_industry", "belongs_to_chain", "policy_affects",
    "supplier_of", "customer_of", "competitor", "substitute_threat",
    "controls", "holds_stake", "subsidiary_of",
    # 要素关联 (4)
    "major_cost_item", "major_revenue_item",
    "cost_affected_by", "revenue_affected_by",
    # 政策关联 (4)
    "benefits", "hurts", "risk_factor", "catalyst",
    # 指标关联 (4)
    "leading_indicator_of", "coincident_indicator_of",
    "lagging_indicator_of", "correlated_with",
    # 兜底 (1)
    "related",
}

RELATION_TO_CATEGORY = {
    "causes_positive": "causal", "causes_negative": "causal",
    "cost_transmission": "causal", "indicator_transmission": "causal",
    "demand_driven": "causal", "supply_driven": "causal",
    "demand_source_of": "causal", "demand_substitute": "causal",
    "belongs_to_industry": "structural", "belongs_to_chain": "structural",
    "policy_affects": "structural", "supplier_of": "structural",
    "customer_of": "structural", "competitor": "structural",
    "substitute_threat": "structural", "controls": "structural",
    "holds_stake": "structural", "subsidiary_of": "structural",
    "major_cost_item": "element", "major_revenue_item": "element",
    "cost_affected_by": "element", "revenue_affected_by": "element",
    "benefits": "policy", "hurts": "policy",
    "risk_factor": "policy", "catalyst": "policy",
    "leading_indicator_of": "indicator", "coincident_indicator_of": "indicator",
    "lagging_indicator_of": "indicator", "correlated_with": "indicator",
    "related": "structural",
}


# ==================== 合法三元组组合矩阵 ====================
# 每个元素: (source_type, relation_type, target_type)
# 基于 SKILL.md 的 28 行层间规则展开为完整的 type-level 组合

def _expand_layer_combinations():
    """展开层间规则为 (source_type, relation, target_type) 三元组集合"""
    combos = set()

    # 辅助：按层级展开
    市场 = ["market"]
    宏观指标 = ["macro_indicator"]
    政策 = ["policy"]
    产业 = ["industry", "industry_chain", "theme"]
    公司 = ["company"]
    策略 = ["strategy"]
    要素 = ["commodity", "energy", "intermediate", "consumer_good"]

    # ── Row 1: 市场 → 市场 ──
    for r in ["causes_positive", "causes_negative", "correlated_with"]:
        combos.add(("market", r, "market"))

    # ── Row 2: 市场 → 产业 ──
    for tgt in 产业:
        for r in ["causes_positive", "causes_negative", "demand_driven", "supply_driven"]:
            combos.add(("market", r, tgt))

    # ── Row 3: 市场 → 公司 ──
    for r in ["causes_positive", "causes_negative"]:
        combos.add(("market", r, "company"))

    # ── Row 4: 市场 → 要素 ──
    for tgt in 要素:
        for r in ["causes_positive", "causes_negative", "demand_driven"]:
            combos.add(("market", r, tgt))

    # ── Row 5: 市场 → 宏观指标 ──
    for r in ["causes_positive", "causes_negative"]:
        combos.add(("market", r, "macro_indicator"))

    # ── Row 6: 宏观指标 → 宏观指标 ──
    for r in ["leading_indicator_of", "coincident_indicator_of", "lagging_indicator_of",
              "correlated_with", "indicator_transmission", "causes_positive", "causes_negative"]:
        combos.add(("macro_indicator", r, "macro_indicator"))

    # ── Row 7: 宏观指标 → 市场 ──
    for r in ["causes_positive", "causes_negative"]:
        combos.add(("macro_indicator", r, "market"))

    # ── Row 8: 宏观指标 → 产业 ──
    for tgt in 产业:
        for r in ["causes_positive", "causes_negative", "demand_driven", "supply_driven"]:
            combos.add(("macro_indicator", r, tgt))

    # ── Row 9: 宏观指标 → 公司 ──
    for r in ["causes_positive", "causes_negative", "cost_affected_by",
               "benefits", "hurts", "risk_factor"]:
        combos.add(("macro_indicator", r, "company"))

    # ── Row 10: 宏观指标 → 要素 ──
    for tgt in 要素:
        for r in ["causes_positive", "causes_negative", "cost_transmission"]:
            combos.add(("macro_indicator", r, tgt))

    # ── Row 11: 政策 → 市场 ──
    for r in ["policy_affects", "causes_positive", "causes_negative",
              "benefits", "hurts", "catalyst", "risk_factor"]:
        combos.add(("policy", r, "market"))

    # ── Row 12: 政策 → 产业 ──
    for tgt in 产业:
        for r in ["benefits", "hurts", "policy_affects", "catalyst", "risk_factor",
                   "demand_driven", "causes_positive", "causes_negative"]:
            combos.add(("policy", r, tgt))

    # ── Row 13: 政策 → 公司 ──
    for r in ["benefits", "hurts", "policy_affects", "catalyst", "risk_factor",
               "cost_affected_by", "causes_positive", "causes_negative"]:
        combos.add(("policy", r, "company"))

    # ── Row 14: 政策 → 要素 ──
    for tgt in 要素:
        for r in ["policy_affects", "causes_positive", "causes_negative"]:
            combos.add(("policy", r, tgt))

    # ── Row 15: 政策 → 宏观指标 ──
    for r in ["causes_positive", "causes_negative"]:
        combos.add(("policy", r, "macro_indicator"))

    # ── Row 16: 产业 → 产业 ──
    产业内 = ["industry", "industry_chain", "theme"]
    for src in 产业内:
        for tgt in 产业内:
            for r in ["belongs_to_chain", "competitor", "substitute_threat",
                       "supplier_of", "customer_of", "demand_driven",
                       "supply_driven", "demand_substitute"]:
                combos.add((src, r, tgt))

    # ── Row 17: 产业 → 公司 ──
    for src in 产业:
        for r in ["causes_positive", "causes_negative",
                   "risk_factor", "catalyst", "benefits", "hurts",
                   "demand_source_of", "supplier_of", "customer_of",
                   "demand_driven"]:
            combos.add((src, r, "company"))

    # ── Row 18: 产业 → 要素 ──
    for src in 产业:
        for tgt in 要素:
            for r in ["demand_driven", "supply_driven", "demand_source_of"]:
                combos.add((src, r, tgt))

    # ── Row 19: 公司 → 行业 ──
    combos.add(("company", "belongs_to_industry", "industry"))

    # ── Row 20: 公司 → 产业链 ──
    combos.add(("company", "belongs_to_chain", "industry_chain"))

    # ── Row 21: 公司 → 公司 ──
    for r in ["competitor", "supplier_of", "customer_of", "substitute_threat",
              "controls", "holds_stake", "subsidiary_of",
              "catalyst", "risk_factor", "benefits", "hurts"]:
        combos.add(("company", r, "company"))

    # ── Row 22: 公司 → 要素 ──
    for tgt in 要素:
        for r in ["major_cost_item", "major_revenue_item",
                   "cost_affected_by", "revenue_affected_by",
                   "supplier_of", "customer_of"]:
            combos.add(("company", r, tgt))

    # ── Row 22b: 公司 → 政策/主题（受益/受损/催化/风险）──
    for tgt in 政策 + 产业:
        for r in ["benefits", "hurts", "catalyst", "risk_factor"]:
            combos.add(("company", r, tgt))

    # ── Row 22c: 公司 → 产业（需求驱动、供应）──
    for tgt in 产业:
        for r in ["demand_driven", "supply_driven", "major_revenue_item", "major_cost_item"]:
            combos.add(("company", r, tgt))

    # ── Row 22d: 公司 → consumer_good/intermediate（需求驱动）──
    for tgt in 要素:
        for r in ["demand_driven", "major_revenue_item", "major_cost_item",
                   "supplier_of", "customer_of"]:
            combos.add(("company", r, tgt))

    # ── Row 23: 要素 → 要素 ──
    for src in 要素:
        for tgt in 要素:
            for r in ["cost_transmission", "demand_substitute",
                       "causes_positive", "causes_negative", "supplier_of"]:
                combos.add((src, r, tgt))

    # ── Row 24: 要素 → 公司 ──
    for src in 要素:
        for r in ["cost_affected_by", "revenue_affected_by",
                   "causes_positive", "causes_negative",
                   "supplier_of", "customer_of"]:
            combos.add((src, r, "company"))

    # ── Row 25: 要素 → 产业 ──
    for src in 要素:
        for tgt in 产业:
            for r in ["supply_driven", "demand_driven",
                       "causes_positive", "causes_negative"]:
                combos.add((src, r, tgt))

    # ── Row 26: 要素 → 宏观指标 ──
    for src in 要素:
        for r in ["causes_positive", "causes_negative", "leading_indicator_of"]:
            combos.add((src, r, "macro_indicator"))

    # ── Row 27: 投资主题 → 产业 ──
    for tgt in 产业:
        for r in ["benefits", "catalyst", "causes_positive", "hurts", "risk_factor"]:
            combos.add(("theme", r, tgt))

    # ── Row 28: 投资主题 → 公司 ──
    for r in ["benefits", "catalyst", "causes_positive", "hurts", "risk_factor"]:
        combos.add(("theme", r, "company"))

    # ── Row 28b: 投资主题 → 市场/政策 ──
    for r in ["benefits", "catalyst", "risk_factor", "causes_positive", "causes_negative"]:
        combos.add(("theme", r, "market"))
        combos.add(("theme", r, "policy"))

    # ── Row 29: 公司 → 策略 ──
    # 公司采用/实施某种策略
    combos.add(("company", "related", "strategy"))

    # ── Row 30: 策略 → 要素 ──
    # 策略影响成本/收入要素
    for tgt in 要素:
        for r in ["causes_positive", "causes_negative", "cost_affected_by"]:
            combos.add(("strategy", r, tgt))

    # ── Row 31: 策略 → 公司 ──
    for r in ["causes_positive", "causes_negative", "benefits", "hurts",
               "risk_factor", "catalyst", "cost_affected_by"]:
        combos.add(("strategy", r, "company"))

    # ── Row 32: 策略 → 产业 ──
    for tgt in 产业:
        for r in ["causes_positive", "causes_negative", "benefits", "hurts"]:
            combos.add(("strategy", r, tgt))

    return combos


VALID_COMBINATIONS = _expand_layer_combinations()


# ==================== 互斥关系对 ====================
# 同一对实体间不能同时存在的关系类型对

CONFLICTING_PAIRS = [
    ("causes_positive", "causes_negative"),
    ("benefits", "hurts"),
    ("supplier_of", "customer_of"),
    ("leading_indicator_of", "lagging_indicator_of"),
    ("demand_driven", "supply_driven"),
    ("major_cost_item", "major_revenue_item"),
    ("cost_affected_by", "revenue_affected_by"),
    ("catalyst", "risk_factor"),
    ("controls", "subsidiary_of"),  # A controls B 与 A subsidiary_of B 互斥
]


# ==================== 位置依赖规则 ====================
# 需要判断产业链位置的组合行号（对应 SKILL.md 的行号）
# 强依赖: 必须标注 chain_position
# 中依赖: 上下文相关，建议标注

STRONG_POSITION_DEPENDENCY_ROWS = {8, 9, 12, 13, 21, 24, 25}
MEDIUM_POSITION_DEPENDENCY_ROWS = {16, 17, 21, 23}

# 需要 chain_position 的源层→目标层组合
POSITION_DEPENDENT_COMBOS = {
    # 强依赖
    ("宏观层", "产业层"),   # Row 8
    ("宏观层", "公司层"),   # Row 9
    ("宏观层", "要素层"),   # Row 10 (部分)
    ("要素层", "公司层"),   # Row 24
    ("要素层", "产业层"),   # Row 25
    # 政策层
    ("宏观层", "产业层"),   # Row 12 (政策=宏观层)
    ("宏观层", "公司层"),   # Row 13
}

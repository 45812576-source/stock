# Step3 财务建模：产业驱动的收入/成本预测重构

日期：2026-03-03

## 背景

上一轮已完成 Step2 产业分析的结构化传导框架（`downstream_demand_analysis`, `capture_rate_analysis`, `price_volume_analysis`），数据也已以文本形式注入 Step3 的 user_message。

但当前 Step3 的 JSON schema 仍是旧结构——让 LLM 自己填 `key_drivers[].forecast`，仅靠 requirements 文字说"遵循三层/四层框架"。LLM 容易忽略约束、自由编数。

## 设计决策

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 产业数据与财务的结合方式 | 程序预填 + LLM 验证修正 | 给 LLM 一个锚定基准，防止凭感觉编数 |
| 成本端预填 | 不预填，用 Schema 四层结构化强制 | 成本端 Step2 输出多为定性描述，不适合程序算数字 |
| LLM 角色 | "MD&A 分析师"——对比+解释+微调 | LLM 必须引用基准 → 对比历史 → 写偏差原因 → 在基准上做有限调整 |
| 偏差分析位置 | 独立 variance_analysis 模块 | 与 revenue_model/profit_model 同级，结构清晰 |

## 改动范围

全部集中在 `research/deep_researcher.py`，共 5 处改动：

### 1. `run_step_financial()` — 程序预填收入基准

在构建 `dp_input` 时，从 `s2_parsed._for_valuation` 提取三层数值，程序化计算每个 segment 的 `baseline_revenue_growth_pct`，注入 user_message。

**计算逻辑**（per segment）：
```
L1_growth = Step2.weighted_growth_rate.value_pct           # 如 +18.5%
L2_adj    = share_trend → 系数映射                          # gaining:+2~5%, stable:0, losing:-2~5%
L3_price  = price_adjustment_conclusion.magnitude_pct       # 如 +3%

baseline_volume_growth = L1_growth + L2_adj                 # 如 +20.5%
baseline_revenue_growth = baseline_volume_growth × (1 + L3_price/100)  # 如 +21.1%
```

**share_trend → 系数映射**：
| share_trend | confidence | adjustment |
|-------------|-----------|------------|
| gaining | high | +5% |
| gaining | medium/low | +2% |
| stable | * | 0% |
| losing | medium/low | -2% |
| losing | high | -5% |

**注入格式**：
```
=== 产业推导收入基准（程序计算，LLM 需对比历史财务后验证/调整）===
  [铜矿业务] L1下游加权+18.5% + L2份额gaining+2% = 量增速+20.5% × (1+价格+3%) = 基准收入增速+21.1%
  [钼钨业务] L1下游加权+8% + L2份额stable+0% = 量增速+8% × (1+价格-2%) = 基准收入增速+5.8%
```

### 2. `FINANCIAL_REVENUE_MODEL_SCHEMA` — 改 schema

每个 segment 新增 `industry_baseline`，`revenue_forecast` 增加调整字段：

```json
"industry_baseline": {
    "l1_downstream_growth_pct": 18.5,
    "l1_calculation": "新能源车25%×35% + 电网12%×30% + ...",
    "l2_capture_adj_pct": 2,
    "l2_reason": "份额gaining，产能sufficient",
    "l3_price_adj_pct": 3,
    "l3_reason": "shortage，定价交易所定价，ASP上行",
    "baseline_revenue_growth_pct": 21.1,
    "source": "program_derived"
},
"revenue_forecast": [
    {
        "period": "2025E",
        "baseline_growth_pct": 21.1,
        "adjusted_growth_pct": 17,
        "adjustment_rationale": "产业基准+21.1%，但历史3年平均营收增速仅+12%...",
        "revenue": 亿元,
        "yoy_pct": 17,
        "confidence": "medium"
    }
]
```

规则：
- `industry_baseline` 由程序预填到 user_message，LLM 必须原样引用 `baseline_revenue_growth_pct`
- `adjusted_growth_pct` 是 LLM 最终给出的数字，必须非 null
- `adjustment_rationale` 是 MD&A 核心——写清楚为什么偏离或不偏离基准
- 偏离幅度 > 10pp 时，`confidence` 最高只能 `medium`

### 3. `FINANCIAL_PROFIT_MODEL_SCHEMA` — 成本四层结构化

`cogs_drivers[]` 从扁平结构改为四层：

```json
"cogs_drivers": [
    {
        "driver_name": "铜精矿采购成本",
        "cost_category": "原材料",
        "pct_of_cogs": 45,
        "procurement_mode": "spot/long_term/self_supply/hedged",
        "layer1_upstream": {
            "price_change_pct": 5,
            "cost_weight_pct": 45,
            "passthrough_coeff": 0.3,
            "raw_impact_pct": 0.68,
            "calculation": "铜价+5% × 占比45% × 传导系数0.3 = +0.68%"
        },
        "layer2_efficiency": {
            "scale_effect_pct": -1.5,
            "efficiency_improvement_pct": -0.5,
            "net_offset_pct": -2.0,
            "basis": "产能利用率85%→92%，规模效应-1.5% + 工艺改善-0.5%"
        },
        "layer3_passthrough": {
            "ability": "partial",
            "lag_months": 3,
            "passed_to_revenue_pct": 1.5,
            "net_cost_impact_pct": -0.82,
            "basis": "partial传导，3月时滞，上游+0.68%中1.5%已反映在收入端价格"
        },
        "layer4_special": {
            "factors": [
                {"name": "汇率", "impact_pct": 0.3, "probability": "medium"},
                {"name": "碳成本", "impact_pct": 0.2, "probability": "low"}
            ],
            "net_special_pct": 0.4,
            "basis": "美元走强+0.3%（medium），碳交易+0.2%（low）"
        },
        "total_cost_impact_pct": -0.74,
        "summary": "上游+0.68% - 效率对冲2.0% - 已传导0.82% + 特殊+0.4% = 净-0.74%"
    }
]
```

规则：
- `procurement_mode` 决定 `passthrough_coeff`（spot≈1.0, long_term≈0, self_supply≈0, hedged=套保比例）
- 每层都有 `calculation`/`basis`，不能跳过
- `total_cost_impact_pct` = L1 - L2 - L3 + L4，LLM 必须算对
- `layer3_passthrough.passed_to_revenue_pct` 必须与收入端 `l3_price_adj` 逻辑一致

### 4. 新增 `FINANCIAL_VARIANCE_SCHEMA` + requirements

与 `revenue_model` / `profit_model` 同级的独立模块：

```json
"variance_analysis": {
    "revenue_variance": [
        {
            "segment_name": "铜矿业务",
            "industry_baseline_pct": 21.1,
            "historical_avg_growth_pct": 12,
            "historical_periods": "2021-2023",
            "latest_actual_growth_pct": 15,
            "latest_actual_period": "2024H1",
            "final_forecast_pct": 17,
            "variance_vs_baseline_pct": -4.1,
            "variance_explanation": "产业基准基于下游需求+18.5%和份额提升，但公司TFM扩产项目2025H2才投产...",
            "risk_to_forecast": "TFM投产延期则降至+12%，铜价超预期涨则可达+20%"
        }
    ],
    "cost_variance": [
        {
            "driver_name": "铜精矿采购成本",
            "industry_signal": "铜价上涨+5%，供应商议价power high",
            "historical_cogs_growth_pct": 8,
            "model_total_impact_pct": -0.74,
            "variance_explanation": "四层推导净影响-0.74%...",
            "key_assumption_risk": "若铜价涨幅超10%，传导系数从0.3升至0.6，净成本影响转正"
        }
    ],
    "margin_bridge": {
        "current_gross_margin_pct": 32,
        "revenue_mix_effect_pct": 1.5,
        "cost_improvement_effect_pct": 0.8,
        "price_effect_pct": 0.5,
        "forecast_gross_margin_pct": 34.8,
        "bridge_narrative": "毛利率从32%→34.8%：收入结构优化+1.5pp + 成本改善+0.8pp + 价格传导+0.5pp"
    }
}
```

规则：
- `revenue_variance` 每条必须同时列出 industry_baseline、historical_avg、latest_actual 三个参照系
- `variance_explanation` 是 MD&A 核心叙事，必须解释偏差原因
- `margin_bridge` 做毛利率桥分析，拆解每个驱动因素对利润率的 pp 贡献
- `risk_to_forecast` / `key_assumption_risk` 给出上下行风险场景

### 5. Requirements 重写

**收入端**：从"遵循三层框架"改为：
- LLM 必须引用 `industry_baseline.baseline_revenue_growth_pct` 作为起点
- 必须填写 `adjusted_growth_pct` 和 `adjustment_rationale`
- 调整依据必须引用历史财务数据（historical_avg, latest_actual）
- 偏离 > 10pp 时 confidence 最高 medium
- 无产业基准时（`source` != `program_derived`）：confidence 最高 low

**成本端**：从文本约束改为：
- 必须选择 `procurement_mode`（四选一）
- 四层必须完整填写，每层有 calculation/basis
- `total_cost_impact_pct` 必须等于 L1 - L2 - L3 + L4
- `layer3_passthrough` 必须与收入端价格调整一致

**variance_analysis**：
- 每个有 industry_baseline 的 segment 必须出现在 revenue_variance
- 每个 cogs_driver 必须出现在 cost_variance
- margin_bridge 各效应之和 = forecast - current

## 不改的部分

- `FINANCIAL_BASE_SCHEMA` — 不变
- `FINANCIAL_CASHFLOW_SCHEMA` / `FINANCIAL_BOOK_VALUE_SCHEMA` — 不变
- `industry_demand_fetcher.py` — 不变（上一轮已完成）
- Step2 prompt — 不变（上一轮已完成）
- 前端模板 — 本轮不改（后续单独调整渲染）

## 实施顺序

1. 改 `FINANCIAL_REVENUE_MODEL_SCHEMA` — 加 `industry_baseline` + 改 `revenue_forecast`
2. 改 `FINANCIAL_PROFIT_MODEL_SCHEMA` — `cogs_drivers` 四层结构化
3. 新建 `FINANCIAL_VARIANCE_SCHEMA` + `FINANCIAL_VARIANCE_REQUIREMENTS`
4. 改 `_build_financial_prompt()` — 组装新 schema
5. 改 `FINANCIAL_REVENUE_REQUIREMENTS` / `FINANCIAL_PROFIT_REQUIREMENTS` — 新规则
6. 改 `run_step_financial()` — 程序预填收入基准注入

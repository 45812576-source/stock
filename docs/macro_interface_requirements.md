# 宏观接口需求文档

> 本文档记录估值模块（`research/valuation_engine.py` Step 4d）对宏观数据模块的所有数据需求。
> 当前估值模块中，宏观调整乘数均使用占位默认值（1.0），待本文档描述的宏观模块完成后接入。

## 一、接口规范

```python
def get_macro_valuation_context(industry: str = None) -> dict:
    """获取估值所需的宏观上下文数据

    Args:
        industry: 行业名称（可选），用于返回行业相关的宏观数据

    Returns:
        {
            "liquidity_multiplier": float,      # 流动性乘数（0.8-1.2）
            "sentiment_multiplier": float,      # 情绪乘数（0.9-1.1）
            "liquidity_breakdown": {...},       # 流动性各分项指标
            "sentiment_breakdown": {...},       # 情绪各分项指标
            "industry_macro": {...},            # 行业宏观数据（如提供industry参数）
            "data_timestamp": str,             # 数据时间戳
            "confidence": float,               # 数据置信度（0-1）
        }
    """
```

## 二、流动性乘数（Liquidity Multiplier）

**作用**：反映宏观流动性对市场估值中枢的系统性抬升或压制。

**计算逻辑**：`liquidity_multiplier = base × Σ(分项权重 × 分项得分)`

| 指标 | 数据来源 | 权重 | 说明 |
|------|----------|------|------|
| M2增速 vs 名义GDP增速（超额流动性） | 人民银行 | 30% | 超额流动性越高，乘数越高 |
| DR007 vs 中性利率（2.0-2.5%） | 银行间市场（Shibor 1W代理） | 25% | Shibor 1W < 2.0% → 宽松 |
| 社融增速 YoY | 人民银行 | 20% | 社融加速扩张 → 宽松 |
| 陆股通持股变动（5日净增持市值） | 沪深港通（hsgt_holding表） | 15% | 持续净增持 → 乐观 |
| 两市日均成交额（20日均） | 交易所（market_valuation表） | 10% | 成交放量 → 活跃 |

**乘数区间**：0.80（极度紧缩）→ 1.0（中性）→ 1.20（极度宽松）

**当前状态**：占位使用 `1.0`（中性默认值）

## 三、情绪乘数（Sentiment Multiplier）

**作用**：反映市场情绪/风险偏好对个股估值的溢价或折价。

**计算逻辑**：基于多个情绪指标的综合打分。

| 指标 | 数据来源 | 权重 | 说明 |
|------|----------|------|------|
| 全A PE分位数（10年） | 东方财富（market_valuation表） | 35% | PE < 20% 分位 → 低估 |
| 市场波动率（20日） | 计算自stock_daily（上证指数） | 25% | 高波动 → 折价 |
| 融资余额变化（10日） | 交易所（margin_balance表） | 20% | 融资增加 → 乐观 |
| 海外ETF资金流向（KWEB/FXI/ASHR 5日涨跌） | 美股行情（overseas_etf表） | 15% | ETF上涨 → 外资乐观 |
| PMI景气度 | 国家统计局（macro_indicators表） | 5% | PMI > 50 → 扩张 |

**乘数区间**：0.90（极度悲观）→ 1.0（中性）→ 1.10（极度乐观）

**当前状态**：占位使用 `1.0`（中性默认值）

## 四、行业宏观数据（Industry Macro）

**作用**：为特定行业的估值假设提供宏观支撑数据。

| 数据项 | 覆盖行业 | 说明 |
|--------|----------|------|
| 大宗商品价格时序 | 化工、钢铁、有色、能源 | 月度/周度价格指数 |
| 行业PMI/景气指数 | 制造业各子行业 | NMI/BCI等 |
| 行业产能利用率 | 重资产行业 | 来自统计局 |
| 信贷数据（行业贷款余额） | 房地产、建筑 | 人民银行分行业数据 |
| 进出口数据 | 电子、机械、汽车 | 海关总署 |
| 政策事件日历 | 所有行业 | 重大政策发布节点 |

## 五、在估值模块中的使用方式

在 `research/valuation_engine.py` Step 4d（汇总）中：

```python
# 当前占位实现（valuation_engine.py 中）
"macro_adjustment": {
    "liquidity_multiplier": 1.0,
    "liquidity_basis": "宏观数据模块未完成，使用中性默认值",
    "sentiment_multiplier": 1.0,
    "sentiment_basis": "宏观数据模块未完成，使用中性默认值",
    "macro_data_available": False,
    "multiplier_note": "待宏观模块完成后，此处将接入实际流动性和情绪数据"
}

# 未来接入后（待实现）
from research.macro_valuation import get_macro_valuation_context

macro_ctx = get_macro_valuation_context(industry=industry)
adjusted_ev = base_ev * macro_ctx["liquidity_multiplier"] * macro_ctx["sentiment_multiplier"]
```

## 六、数据库存储方案（建议）

建议新增 `macro_valuation_cache` 表：

```sql
CREATE TABLE macro_valuation_cache (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    calc_date   DATE NOT NULL,
    liquidity_multiplier  DECIMAL(5,4) NOT NULL DEFAULT 1.0,
    sentiment_multiplier  DECIMAL(5,4) NOT NULL DEFAULT 1.0,
    liquidity_breakdown   JSON,
    sentiment_breakdown   JSON,
    confidence           DECIMAL(3,2) DEFAULT 0.5,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date (calc_date)
);
```

## 七、实施优先级

1. **P1（当前阻塞）**：DR007、M2增速 → 流动性乘数基础版
2. **P2（近期）**：全A PE分位数 → 情绪乘数基础版
3. **P3（中期）**：行业PMI、大宗商品价格 → 行业宏观数据
4. **P4（长期）**：融资余额、基金发行、进出口 → 完整情绪体系

---

*文档创建时间：2026-02-25*
*关联模块：`research/valuation_engine.py`（Step 4d macro_adjustment 字段）*

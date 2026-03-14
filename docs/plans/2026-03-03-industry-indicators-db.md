# Industry Indicators DB Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 建立结构化行业指标库 `industry_indicators`，在清洗管线里新增 Pipeline D 抽取行业数值，让 Step2 产业链分析优先走 SQL 查询而非 RAG 向量搜索。

**Architecture:** 云端 MySQL 新表 `industry_indicators`（与 `content_summaries` 同库）存储行业量化指标（一/二/三级行业、指标定义、分子/分母、数值、时间粒度、数据质量）。`unified_pipeline.process_single()` 在现有 A/B2/C 三条管线之后新增 Pipeline D：研报/行业分析类走精细 DeepSeek-reasoner 抽取，其他类型在 Pipeline A 的 prompt 里顺带轻量抽取。`industry_demand_fetcher` 改为 SQL-first（查云端）→ RAG-fallback 三级查询策略。

**Tech Stack:** Python 3.9+、pymysql、DeepSeek API（deepseek-reasoner for Pipeline D full，deepseek-chat for lite）、现有 `utils/db_utils.py`（`execute_cloud_query`/`execute_cloud_insert` 云端库）、现有 `config/doc_types.py`（`classify_doc_type`/`FAMILY_MAP`）

---

## Task 1: 建表 industry_indicators（云端库）

**Files:**
- Create: `db/migration_v5_industry_indicators.sql`
- Modify: `utils/db_utils.py`（新增查询函数）

**Step 1: 写 SQL migration 文件**

```sql
-- db/migration_v5_industry_indicators.sql
-- 执行位置：云端 MySQL (8.134.184.254:3301, stock_db)

CREATE TABLE IF NOT EXISTS industry_indicators (
    id                     INT PRIMARY KEY AUTO_INCREMENT,

    -- 行业分类（申万体系）
    industry_l1            VARCHAR(50)  NOT NULL,   -- 一级：有色金属
    industry_l2            VARCHAR(100) NOT NULL,   -- 二级：工业金属
    industry_l3            VARCHAR(100) DEFAULT NULL, -- 三级：铜冶炼（可空）

    -- 指标分类与定义
    metric_type            VARCHAR(50)  NOT NULL,   -- growth_rate/output/price/market_size/penetration/capacity/inventory
    metric_name            VARCHAR(200) NOT NULL,   -- 动力电池出货量同比增速
    metric_definition      TEXT         DEFAULT NULL, -- 完整定义（LLM从原文提取）
    metric_numerator       VARCHAR(200) DEFAULT NULL, -- 分子：当期出货量(GWh)
    metric_denominator     VARCHAR(200) DEFAULT NULL, -- 分母：上年同期；绝对量填统计口径

    -- 数值
    value                  DECIMAL(20,4) DEFAULT NULL,
    value_raw              VARCHAR(200) DEFAULT NULL, -- 原始文本，如"约25%"

    -- 时间（支持年/半年/季度/月/时点）
    period_type            VARCHAR(20)  DEFAULT NULL, -- year/half/quarter/month/point
    period_label           VARCHAR(50)  DEFAULT NULL, -- 2024Q3 / 2024-09 / 2024H1
    period_year            INT          DEFAULT NULL, -- 便于按年过滤
    period_end_date        DATE         DEFAULT NULL, -- 标准化终止日：2024Q3→2024-09-30

    -- 文章发布时间
    publish_date           DATE         DEFAULT NULL, -- 来源文章发布日

    -- forecast 专用（data_type=forecast 时填）
    forecast_target_label  VARCHAR(50)  DEFAULT NULL, -- 2025E / 2026Q1
    forecast_target_date   DATE         DEFAULT NULL, -- 标准化：2025E→2025-12-31

    -- 数据质量
    data_type              VARCHAR(20)  DEFAULT NULL, -- actual/forecast/estimate
    confidence             VARCHAR(10)  DEFAULT NULL, -- high/medium/low
    source_type            VARCHAR(30)  DEFAULT NULL, -- pipeline_d/pipeline_a_lite/akshare/manual
    source_doc_id          INT          DEFAULT NULL, -- extracted_texts.id
    source_snippet         TEXT         DEFAULT NULL, -- 原文30字摘录

    -- 冲突管理
    is_conflicted          TINYINT      DEFAULT 0,   -- 1=与其他来源存在>20%偏差
    conflict_note          VARCHAR(500) DEFAULT NULL, -- 冲突说明

    created_at             TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_industry  (industry_l1, industry_l2, industry_l3),
    INDEX idx_metric    (metric_type, period_year),
    INDEX idx_lookup    (industry_l2, metric_type, period_year),
    INDEX idx_source    (source_doc_id),
    INDEX idx_period    (period_end_date),
    INDEX idx_forecast  (forecast_target_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

**Step 2: 执行建表**

```bash
cd /Users/liaoxia/stock-analysis-system
python3 -c "
from utils.db_utils import execute_cloud_insert
sql = open('db/migration_v5_industry_indicators.sql').read()
for stmt in sql.split(';'):
    stmt = stmt.strip()
    if stmt:
        execute_cloud_insert(stmt)
print('建表完成')
"
```

Expected: 输出"建表完成"，无报错。

**Step 3: 在 `utils/db_utils.py` 末尾新增查询函数**

```python
# ── industry_indicators 查询（云端库）─────────────────────────────────────────

def query_industry_indicator(
    industry_name: str,
    metric_type: str = None,
    period_year: int = None,
    data_type: str = None,
    limit: int = 10,
) -> list:
    """查询行业指标，支持 L2/L3/metric_name 三列模糊匹配。走云端库。

    优先级：industry_l2 精确 > industry_l2 LIKE > industry_l3/metric_name LIKE
    """
    base_conditions = []
    params = []

    if metric_type:
        base_conditions.append("metric_type = %s")
        params.append(metric_type)
    if period_year:
        base_conditions.append("period_year >= %s")
        params.append(period_year)
    if data_type:
        base_conditions.append("data_type = %s")
        params.append(data_type)

    base_where = (" AND " + " AND ".join(base_conditions)) if base_conditions else ""
    order = "ORDER BY publish_date DESC, confidence DESC"

    # L1: 精确匹配 industry_l2
    rows = execute_cloud_query(
        f"SELECT * FROM industry_indicators WHERE industry_l2 = %s{base_where} {order} LIMIT %s",
        [industry_name] + params + [limit],
    )
    if rows:
        return rows

    # L2: LIKE 匹配 industry_l2
    rows = execute_cloud_query(
        f"SELECT * FROM industry_indicators WHERE industry_l2 LIKE %s{base_where} {order} LIMIT %s",
        [f"%{industry_name}%"] + params + [limit],
    )
    if rows:
        return rows

    # L3: 匹配 industry_l3 或 metric_name
    rows = execute_cloud_query(
        f"""SELECT * FROM industry_indicators
            WHERE (industry_l3 LIKE %s OR metric_name LIKE %s){base_where}
            {order} LIMIT %s""",
        [f"%{industry_name}%", f"%{industry_name}%"] + params + [limit],
    )
    return rows or []


def upsert_industry_indicator(row: dict) -> int:
    """写入一条指标到云端库，已存在则按优先级决定是否覆盖，数值偏差>20%标记冲突。

    唯一键：(industry_l2, metric_name, period_label, data_type)
    返回：写入/更新的 id
    """
    # 查已有记录（云端）
    existing = execute_cloud_query(
        """SELECT id, value, confidence, publish_date FROM industry_indicators
           WHERE industry_l2 = %s AND metric_name = %s
             AND period_label = %s AND data_type = %s
           LIMIT 1""",
        [
            row.get("industry_l2", ""),
            row.get("metric_name", ""),
            row.get("period_label", ""),
            row.get("data_type", "actual"),
        ],
    )

    _CONF_RANK = {"high": 3, "medium": 2, "low": 1}

    if not existing:
        cols = ", ".join(row.keys())
        placeholders = ", ".join(["%s"] * len(row))
        execute_cloud_insert(
            f"INSERT INTO industry_indicators ({cols}) VALUES ({placeholders})",
            list(row.values()),
        )
        rows = execute_cloud_query(
            "SELECT id FROM industry_indicators WHERE industry_l2=%s AND metric_name=%s AND period_label=%s ORDER BY id DESC LIMIT 1",
            [row.get("industry_l2", ""), row.get("metric_name", ""), row.get("period_label", "")],
        )
        return rows[0]["id"] if rows else -1

    ex = existing[0]
    new_conf = _CONF_RANK.get(row.get("confidence", "low"), 1)
    ex_conf = _CONF_RANK.get(ex.get("confidence", "low"), 1)

    # 冲突检测（数值偏差>20%）
    ex_val = float(ex.get("value") or 0)
    new_val = float(row.get("value") or 0)
    conflicted = False
    if ex_val != 0 and abs(ex_val - new_val) / abs(ex_val) > 0.2:
        conflicted = True
        conflict_note = f"另一来源值为{ex_val}，来源doc_id={ex.get('source_doc_id')}"
        execute_cloud_insert(
            "UPDATE industry_indicators SET is_conflicted=1, conflict_note=%s WHERE id=%s",
            [conflict_note, ex["id"]],
        )

    # 新数据更优先时覆盖
    new_date = str(row.get("publish_date") or "")
    ex_date = str(ex.get("publish_date") or "")
    should_replace = (new_conf > ex_conf) or (new_conf == ex_conf and new_date > ex_date)

    if should_replace:
        set_clause = ", ".join([f"{k}=%s" for k in row.keys()])
        execute_cloud_insert(
            f"UPDATE industry_indicators SET {set_clause}, is_conflicted=%s WHERE id=%s",
            list(row.values()) + [int(conflicted), ex["id"]],
        )

    return ex["id"]
```

**Step 4: 验证函数可导入**

```bash
cd /Users/liaoxia/stock-analysis-system
python3 -c "from utils.db_utils import query_industry_indicator, upsert_industry_indicator; print('OK')"
```

Expected: `OK`

**Step 5: Commit**

```bash
cd /Users/liaoxia/stock-analysis-system
git add db/migration_v5_industry_indicators.sql utils/db_utils.py
git commit -m "feat: add industry_indicators table + query/upsert helpers"
```

---

## Task 2: Pipeline D — 精细指标抽取模块

**Files:**
- Create: `cleaning/industry_indicator_extractor.py`

**Step 1: 创建文件**

```python
# cleaning/industry_indicator_extractor.py
"""Pipeline D — 行业指标结构化抽取

研报/行业分析类：精细模式（deepseek-reasoner）
其他类型：轻量模式（由 Pipeline A prompt 顺带，不在此文件）
"""
import json
import logging
import re
from datetime import date, datetime
from typing import Optional

from utils.db_utils import execute_cloud_query, upsert_industry_indicator

logger = logging.getLogger(__name__)

# ── Prompt ────────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """你是行业数据提取专家。从研报/行业分析文章中提取所有行业量化指标。

规则：
1. 只提取有明确数值的指标，禁止推测或捏造
2. metric_type 必须从枚举中选：growth_rate / output / price / market_size / penetration / capacity / inventory
3. 必须识别分子/分母定义（如同比=当期÷上年同期-1，环比=当期÷上期-1）
4. period_label 必须精确到粒度（写 2024Q3 而非"今年三季度"）
5. period_end_date：year→12-31，half→H1=06-30/H2=12-31，quarter→Q1=03-31/Q2=06-30/Q3=09-30/Q4=12-31，month→该月最后一天
6. forecast_target_label 只在 data_type=forecast 时填，填预测目标期（如 2025E）
7. forecast_target_date：同 period_end_date 规则推算
8. confidence：有明确出处=high；行业共识/研报预测=medium；模糊表述=low
9. source_snippet：原文中包含该数值的句子，不超过60字
10. 同一数值有歧义时，填 confidence=low 并在 metric_definition 中说明

输出严格 JSON（不要 markdown 代码块）：
{
  "indicators": [
    {
      "industry_l1": "有色金属",
      "industry_l2": "工业金属",
      "industry_l3": null,
      "metric_type": "growth_rate",
      "metric_name": "动力电池出货量同比增速",
      "metric_definition": "当月国内动力电池出货量÷上年同月出货量-1，来源：中国汽车动力电池产业创新联盟",
      "metric_numerator": "当月动力电池出货量(GWh)",
      "metric_denominator": "上年同月动力电池出货量(GWh)",
      "value": 25.3,
      "value_raw": "同比增长约25%",
      "period_type": "month",
      "period_label": "2024-09",
      "period_year": 2024,
      "period_end_date": "2024-09-30",
      "data_type": "actual",
      "confidence": "high",
      "source_snippet": "2024年9月动力电池出货量同比增长约25%，创年内新高"
    }
  ]
}"""

# ── 行业名 → 申万一级映射（快速兜底）────────────────────────────────────────

_L2_TO_L1 = {
    "动力电池": "电力设备", "锂电池": "电力设备", "储能电池": "电力设备",
    "工业金属": "有色金属", "铜": "有色金属", "铝": "有色金属", "锌": "有色金属",
    "新能源汽车": "汽车", "乘用车": "汽车",
    "光伏": "电力设备", "风电": "电力设备", "特高压": "电力设备",
    "钢铁": "钢铁", "特种钢": "钢铁",
    "半导体": "电子", "芯片": "电子",
}

def _guess_l1(l2: str) -> str:
    for key, l1 in _L2_TO_L1.items():
        if key in l2:
            return l1
    return "其他"


# ── LLM 调用 ──────────────────────────────────────────────────────────────────

def _call_deepseek_reasoner(text: str, timeout: int = 120) -> Optional[str]:
    """调用 deepseek-reasoner 做精细抽取"""
    try:
        import threading
        from openai import OpenAI
        from utils.db_utils import execute_cloud_query as _cq
        rows = _cq("SELECT value FROM system_config WHERE config_key='deepseek_api_key'")
        if not rows:
            raise RuntimeError("未找到 deepseek_api_key")
        client = OpenAI(api_key=rows[0]["value"], base_url="https://api.deepseek.com/v1")

        if len(text) > 15000:
            text = text[:15000] + "\n\n[文本已截断]"

        resp = client.chat.completions.create(
            model="deepseek-reasoner",
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": f"请从以下文章中提取行业量化指标：\n\n{text}"},
            ],
            max_tokens=4096,
            temperature=0.1,
            timeout=timeout,
        )
        return resp.choices[0].message.content
    except Exception as e:
        logger.warning(f"[Pipeline D] deepseek-reasoner 调用失败: {e}")
        return None


# ── 解析 & 写入 ───────────────────────────────────────────────────────────────

def _parse_indicators(raw: str) -> list:
    raw = raw.strip()
    # 去掉可能的 markdown 代码块
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        data = json.loads(raw.strip())
        return data.get("indicators", []) if isinstance(data, dict) else []
    except Exception:
        return []


def _normalize_indicator(ind: dict, source_doc_id: int, publish_date_str: str) -> dict:
    """归一化单条指标，填充缺省值"""
    row = {}

    # 行业
    l1 = (ind.get("industry_l1") or "").strip()
    l2 = (ind.get("industry_l2") or "").strip()
    l3 = (ind.get("industry_l3") or None)
    row["industry_l1"] = l1 or _guess_l1(l2)
    row["industry_l2"] = l2
    row["industry_l3"] = l3 or None

    # 指标
    row["metric_type"] = (ind.get("metric_type") or "").strip()
    row["metric_name"] = (ind.get("metric_name") or "").strip()
    row["metric_definition"] = ind.get("metric_definition") or None
    row["metric_numerator"] = ind.get("metric_numerator") or None
    row["metric_denominator"] = ind.get("metric_denominator") or None

    # 数值
    try:
        row["value"] = float(ind["value"]) if ind.get("value") is not None else None
    except (TypeError, ValueError):
        row["value"] = None
    row["value_raw"] = ind.get("value_raw") or None

    # 时间
    row["period_type"] = ind.get("period_type") or None
    row["period_label"] = ind.get("period_label") or None
    row["period_year"] = ind.get("period_year") or None
    row["period_end_date"] = ind.get("period_end_date") or None

    # 发布日期（从 source_doc 继承）
    try:
        row["publish_date"] = str(publish_date_str)[:10] if publish_date_str else None
    except Exception:
        row["publish_date"] = None

    # forecast
    row["forecast_target_label"] = ind.get("forecast_target_label") or None
    row["forecast_target_date"] = ind.get("forecast_target_date") or None

    # 数据质量
    row["data_type"] = ind.get("data_type") or "actual"
    row["confidence"] = ind.get("confidence") or "medium"
    row["source_type"] = "pipeline_d"
    row["source_doc_id"] = source_doc_id
    row["source_snippet"] = ind.get("source_snippet") or None

    return row


def run_pipeline_d(extracted_text_id: int, full_text: str) -> int:
    """主入口：精细模式，用于研报/行业分析类文档。

    Returns: 写入的指标条数
    """
    # 获取 publish_date
    try:
        meta = execute_cloud_query(
            """SELECT et.publish_time, sd.id as doc_id
               FROM extracted_texts et
               LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
               WHERE et.id = %s""",
            [extracted_text_id],
        )
        publish_date_str = (meta[0].get("publish_time") or "") if meta else ""
        source_doc_id = extracted_text_id  # 用 extracted_text_id 作为 source 标识
    except Exception as e:
        logger.warning(f"[Pipeline D] 获取元数据失败: {e}")
        publish_date_str = ""
        source_doc_id = extracted_text_id

    raw = _call_deepseek_reasoner(full_text)
    if not raw:
        return 0

    indicators = _parse_indicators(raw)
    if not indicators:
        logger.info(f"[Pipeline D] id={extracted_text_id} 未抽取到指标")
        return 0

    count = 0
    for ind in indicators:
        if not ind.get("industry_l2") or not ind.get("metric_name"):
            continue  # 跳过缺失关键字段的条目
        try:
            row = _normalize_indicator(ind, source_doc_id, publish_date_str)
            upsert_industry_indicator(row)
            count += 1
        except Exception as e:
            logger.warning(f"[Pipeline D] 写入失败: {e} | {ind.get('metric_name')}")

    logger.info(f"[Pipeline D] id={extracted_text_id} 写入 {count} 条指标")
    return count
```

**Step 2: 验证可导入**

```bash
cd /Users/liaoxia/stock-analysis-system
python3 -c "from cleaning.industry_indicator_extractor import run_pipeline_d; print('OK')"
```

Expected: `OK`

**Step 3: Commit**

```bash
git add cleaning/industry_indicator_extractor.py
git commit -m "feat: add Pipeline D industry indicator extractor (deepseek-reasoner)"
```

---

## Task 3: unified_pipeline 接入 Pipeline D

**Files:**
- Modify: `cleaning/unified_pipeline.py`

**Step 1: 在 `process_single()` 返回前加入 Pipeline D 触发逻辑**

在 `unified_pipeline.py` 的 `process_single()` 函数里，找到 `with ThreadPoolExecutor(max_workers=3) as pool:` 代码块之后，在 `return results` 之前插入：

```python
    # ★ Pipeline D: 行业指标结构化抽取
    indicators_count = 0
    try:
        # 判断文档类型
        meta_d = execute_cloud_query(
            """SELECT sd.doc_type, sd.file_type, et.publish_time
               FROM extracted_texts et
               LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
               WHERE et.id = %s""",
            [extracted_text_id],
        )
        doc_type_d = (meta_d[0].get("doc_type") or "") if meta_d else ""

        from config.doc_types import classify_doc_type
        family_d = classify_doc_type(doc_type_d, full_text[:200])

        if family_d in ("研报策略", "行业分析", "宏观策略"):
            # 精细模式：独立 Pipeline D
            from cleaning.industry_indicator_extractor import run_pipeline_d
            if on_status:
                on_status("D", f"行业指标抽取(精细) id={extracted_text_id}")
            indicators_count = run_pipeline_d(extracted_text_id, full_text)
        # 其他类型：轻量模式通过 Pipeline A 顺带（在 content_summarizer 里处理）
    except Exception as e:
        logger.warning(f"[D] 行业指标抽取跳过 id={extracted_text_id}: {e}")

    results["indicators"] = indicators_count
```

同时更新返回值初始化行，加入 `"indicators": 0`：

```python
    results = {"summary_id": None, "mentions": 0, "kg_rels": 0,
               "semantic_cleaned": semantic_cleaned, "chunks": chunks_count,
               "indicators": 0}
```

**Step 2: 验证 process_single 签名不变**

```bash
cd /Users/liaoxia/stock-analysis-system
python3 -c "
from cleaning.unified_pipeline import process_single
import inspect
print(inspect.signature(process_single))
"
```

Expected: `(extracted_text_id: int, need_a=True, need_b=True, need_c=True, on_status=None, rerun_a=False)`

**Step 3: Commit**

```bash
git add cleaning/unified_pipeline.py
git commit -m "feat: integrate Pipeline D into unified_pipeline process_single"
```

---

## Task 4: industry_demand_fetcher 改为 SQL-first

**Files:**
- Modify: `research/industry_demand_fetcher.py`

**Step 1: 新增 `_query_indicator_db()` 函数**

在文件顶部 `_build_downstream_queries` 之前插入：

```python
def _query_indicator_db(industry_name: str, period_year: int = 2024) -> list[dict]:
    """从 industry_indicators 表精准查询下游行业增速数据。

    返回格式与 _extract_downstream() 的 downstream_growth[] 兼容：
    [{"industry": str, "recent_growth_pct": float|None, "period": str,
      "forecast_growth_pct": float|None, "forecast_period": str,
      "data_type": str, "source_snippet": str}]
    """
    try:
        from utils.db_utils import query_industry_indicator
    except ImportError:
        return []

    # 查实际增速
    actual_rows = query_industry_indicator(
        industry_name, metric_type="growth_rate", period_year=period_year, data_type="actual"
    )
    # 查预测增速
    forecast_rows = query_industry_indicator(
        industry_name, metric_type="growth_rate", period_year=period_year, data_type="forecast"
    )

    if not actual_rows and not forecast_rows:
        # 降级：扩大年份范围再试
        actual_rows = query_industry_indicator(
            industry_name, metric_type="growth_rate", period_year=period_year - 1
        )

    if not actual_rows and not forecast_rows:
        return []

    result = {
        "industry": industry_name,
        "recent_growth_pct": None,
        "period": "",
        "forecast_growth_pct": None,
        "forecast_period": "",
        "data_type": "actual",
        "source_snippet": "",
        "_from_indicator_db": True,
    }

    if actual_rows:
        r = actual_rows[0]
        result["recent_growth_pct"] = float(r["value"]) if r.get("value") is not None else None
        result["period"] = r.get("period_label") or str(r.get("period_year") or "")
        result["source_snippet"] = r.get("source_snippet") or r.get("value_raw") or ""
        result["data_type"] = "actual"

    if forecast_rows:
        f = forecast_rows[0]
        result["forecast_growth_pct"] = float(f["value"]) if f.get("value") is not None else None
        result["forecast_period"] = f.get("forecast_target_label") or f.get("period_label") or ""

    return [result]
```

**Step 2: 修改 `fetch_industry_demand_data()` 主函数**，在 RAG 查询前先走 SQL

在 `fetch_industry_demand_data()` 里，现有批1 RAG 搜索之前插入：

```python
    # ── 优先查 industry_indicators 结构化库 ─────────────────────────
    db_downstream_growth = []
    for ci in (customer_industries or [])[:6]:
        ci_name = ci.get("name", "")
        if not ci_name:
            continue
        db_rows = _query_indicator_db(ci_name, period_year=2024)
        db_downstream_growth.extend(db_rows)
        if db_rows:
            logger.info(f"[demand_fetcher] SQL命中: {ci_name} → {db_rows[0].get('recent_growth_pct')}%")

    # 仅对 SQL 未命中的行业做 RAG 搜索
    db_covered_industries = {r["industry"] for r in db_downstream_growth}
    uncovered_cis = [ci for ci in (customer_industries or []) if ci.get("name") not in db_covered_industries]
```

然后把批1 RAG 的 `customer_industries` 替换为 `uncovered_cis`：

```python
    dq = _build_downstream_queries(stock_name, uncovered_cis)   # 原来是 customer_industries
```

最后在 LLM 提取结果里合并 SQL 数据：

```python
    # 合并 SQL 命中的数据到 downstream 结构
    if db_downstream_growth:
        existing = downstream.get("downstream_growth") or []
        # SQL 数据优先（db_covered 里的行业不被 RAG 覆盖）
        existing_industries = {d.get("industry") for d in existing}
        for db_row in db_downstream_growth:
            if db_row["industry"] not in existing_industries:
                existing.append(db_row)
        downstream["downstream_growth"] = existing
```

**Step 3: 验证导入不报错**

```bash
cd /Users/liaoxia/stock-analysis-system
python3 -c "from research.industry_demand_fetcher import fetch_industry_demand_data, _query_indicator_db; print('OK')"
```

Expected: `OK`

**Step 4: Commit**

```bash
git add research/industry_demand_fetcher.py
git commit -m "feat: industry_demand_fetcher SQL-first strategy with RAG fallback"
```

---

## Task 5: Pipeline A Lite — 非研报类顺带轻量抽取

**Files:**
- Modify: `cleaning/content_summarizer.py`

**Step 1: 读 content_summarizer.py，找到主 prompt 的 JSON 输出结构末尾**

定位 `summarize_single()` 里的 system prompt，找到 JSON 输出的最后一个字段，在其后追加：

```python
# 在 system prompt 的 JSON 输出结构里，最后加一个可选字段
"""
  "indicators": [   // 可为空数组 []，仅在文章中有明确行业量化数值时填写
    {
      "industry_l2": "动力电池",       // 申万二级行业名
      "metric_name": "出货量同比增速", // 指标名
      "metric_type": "growth_rate",   // growth_rate/output/price/market_size/penetration/capacity/inventory
      "value": 18.5,                  // 数值（% 不带%号）
      "value_raw": "同比增长18.5%",   // 原始文本
      "period_label": "2024Q3",       // 时间粒度标签
      "period_year": 2024,
      "data_type": "actual",          // actual/forecast/estimate
      "confidence": "medium",         // high/medium/low
      "source_snippet": "原文句子"    // 不超过60字
    }
  ]
"""
```

**Step 2: 在 `summarize_single()` 里，成功写入 content_summaries 后，读取 indicators 字段写入 industry_indicators**

```python
    # Pipeline A Lite: 轻量指标抽取（顺带）
    indicators_lite = result.get("indicators") or []
    if indicators_lite:
        try:
            from utils.db_utils import upsert_industry_indicator
            publish_date_str = str(row.get("publish_time") or "")[:10]
            for ind in indicators_lite:
                if not ind.get("industry_l2") or not ind.get("metric_name"):
                    continue
                lite_row = {
                    "industry_l1": ind.get("industry_l1") or "",
                    "industry_l2": ind.get("industry_l2", ""),
                    "industry_l3": None,
                    "metric_type": ind.get("metric_type") or "growth_rate",
                    "metric_name": ind.get("metric_name", ""),
                    "metric_definition": None,   # 轻量版不要求
                    "metric_numerator": None,
                    "metric_denominator": None,
                    "value": float(ind["value"]) if ind.get("value") is not None else None,
                    "value_raw": ind.get("value_raw"),
                    "period_type": None,
                    "period_label": ind.get("period_label"),
                    "period_year": ind.get("period_year"),
                    "period_end_date": None,
                    "publish_date": publish_date_str or None,
                    "forecast_target_label": None,
                    "forecast_target_date": None,
                    "data_type": ind.get("data_type", "actual"),
                    "confidence": ind.get("confidence", "medium"),
                    "source_type": "pipeline_a_lite",
                    "source_doc_id": extracted_text_id,
                    "source_snippet": ind.get("source_snippet"),
                }
                upsert_industry_indicator(lite_row)
        except Exception as e:
            logger.warning(f"[A Lite] 指标写入失败: {e}")
```

**Step 3: 验证导入**

```bash
cd /Users/liaoxia/stock-analysis-system
python3 -c "from cleaning.content_summarizer import summarize_single; print('OK')"
```

Expected: `OK`

**Step 4: Commit**

```bash
git add cleaning/content_summarizer.py
git commit -m "feat: Pipeline A lite - extract indicators from non-report docs"
```

---

## Task 6: 回填脚本 — 对已有研报跑 Pipeline D

**Files:**
- Create: `scripts/backfill_pipeline_d.py`

**Step 1: 创建回填脚本**

```python
#!/usr/bin/env python3
"""回填脚本：对已有研报/行业分析类文档跑 Pipeline D 指标抽取

用法：
    python scripts/backfill_pipeline_d.py              # 全量（慢）
    python scripts/backfill_pipeline_d.py --limit 50   # 只跑50条
    python scripts/backfill_pipeline_d.py --dry-run    # 只统计数量
"""
import argparse
import logging
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import execute_cloud_query
from config.doc_types import classify_doc_type
from cleaning.industry_indicator_extractor import run_pipeline_d

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TARGET_FAMILIES = {"研报策略", "行业分析", "宏观策略"}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # 查研报/行业分析类文档
    sql = """
        SELECT et.id, et.full_text, sd.doc_type, et.publish_time
        FROM extracted_texts et
        LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
        WHERE et.full_text IS NOT NULL AND LENGTH(et.full_text) > 200
        ORDER BY et.id DESC
    """
    if args.limit:
        sql += f" LIMIT {args.limit * 5}"  # 多取一些供筛选

    rows = execute_cloud_query(sql)
    logger.info(f"候选文档 {len(rows)} 条")

    target_rows = []
    for r in rows:
        family = classify_doc_type(r.get("doc_type") or "", (r.get("full_text") or "")[:200])
        if family in TARGET_FAMILIES:
            target_rows.append(r)
        if args.limit and len(target_rows) >= args.limit:
            break

    logger.info(f"研报/行业分析类: {len(target_rows)} 条")
    if args.dry_run:
        return

    total = 0
    for i, r in enumerate(target_rows):
        try:
            count = run_pipeline_d(r["id"], r["full_text"] or "")
            total += count
            logger.info(f"[{i+1}/{len(target_rows)}] id={r['id']} → {count} 条指标")
        except Exception as e:
            logger.error(f"id={r['id']} 失败: {e}")

    logger.info(f"回填完成，共写入 {total} 条指标")

if __name__ == "__main__":
    main()
```

**Step 2: 测试 dry-run**

```bash
cd /Users/liaoxia/stock-analysis-system
python3 scripts/backfill_pipeline_d.py --dry-run
```

Expected: 输出"候选文档 X 条"和"研报/行业分析类: Y 条"，无报错。

**Step 3: 跑小批量验证**

```bash
python3 scripts/backfill_pipeline_d.py --limit 5
```

Expected: 输出 5 条文档各自写入的指标数量，`SELECT COUNT(*) FROM industry_indicators` 大于 0。

**Step 4: Commit**

```bash
git add scripts/backfill_pipeline_d.py
git commit -m "feat: add backfill_pipeline_d.py for existing reports"
```

---

## 验收标准

```bash
# 1. 表存在（云端库）
python3 -c "from utils.db_utils import execute_cloud_query; print(execute_cloud_query('SELECT COUNT(*) as n FROM industry_indicators')[0])"

# 2. 回填后有数据
python3 scripts/backfill_pipeline_d.py --limit 10
python3 -c "from utils.db_utils import execute_cloud_query; rows=execute_cloud_query('SELECT industry_l2, metric_name, value, period_label FROM industry_indicators LIMIT 10'); [print(r) for r in rows]"

# 3. SQL 查询函数正常
python3 -c "
from utils.db_utils import query_industry_indicator
rows = query_industry_indicator('动力电池', metric_type='growth_rate', period_year=2024)
print(f'查询结果: {len(rows)} 条')
for r in rows[:3]:
    print(f'  {r[\"metric_name\"]} {r[\"period_label\"]} {r[\"value\"]}')
"

# 4. industry_demand_fetcher SQL-first 路径触发
python3 -c "
from research.industry_demand_fetcher import _query_indicator_db
rows = _query_indicator_db('动力电池', period_year=2024)
print(f'SQL命中: {rows}')
"
```

---

## 实施顺序

P0 → Task 1（建表）→ Task 2（抽取模块）→ Task 3（接入管线）→ Task 4（SQL-first 查询）→ Task 6（回填）→ Task 5（Lite，可最后做）

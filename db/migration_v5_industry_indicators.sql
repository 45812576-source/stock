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

-- 个人股票分析系统 数据库Schema
-- MySQL

-- ============ 原始数据层 ============

CREATE TABLE IF NOT EXISTS data_sources (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL UNIQUE,
    source_type VARCHAR(255) NOT NULL,
    base_url VARCHAR(512),
    daily_limit INT DEFAULT 0,
    monthly_limit INT DEFAULT 0,
    today_used INT DEFAULT 0,
    month_used INT DEFAULT 0,
    last_reset_date VARCHAR(20),
    config_json TEXT,
    enabled INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS raw_items (
    id INT PRIMARY KEY AUTO_INCREMENT,
    source_id INT NOT NULL,
    external_id VARCHAR(255),
    title VARCHAR(512),
    content LONGTEXT,
    url VARCHAR(1024),
    published_at TIMESTAMP NULL,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_status VARCHAR(50) DEFAULT 'pending',
    item_type VARCHAR(50),
    meta_json TEXT,
    FOREIGN KEY (source_id) REFERENCES data_sources(id),
    UNIQUE KEY uq_raw_items_src_ext (source_id, external_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_raw_items_status ON raw_items(processing_status);
CREATE INDEX idx_raw_items_fetched ON raw_items(fetched_at);

-- ============ 清洗数据层 ============

CREATE TABLE IF NOT EXISTS cleaned_items (
    id INT PRIMARY KEY AUTO_INCREMENT,
    raw_item_id INT NOT NULL,
    event_type VARCHAR(100),
    sentiment VARCHAR(50),
    importance INT DEFAULT 3,
    summary TEXT,
    key_points_json TEXT,
    tags_json TEXT,
    impact_analysis TEXT,
    time_horizon VARCHAR(50),
    confidence DOUBLE,
    structured_json LONGTEXT,
    cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (raw_item_id) REFERENCES raw_items(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_cleaned_items_type ON cleaned_items(event_type);
CREATE INDEX idx_cleaned_items_sentiment ON cleaned_items(sentiment);

CREATE TABLE IF NOT EXISTS item_companies (
    id INT PRIMARY KEY AUTO_INCREMENT,
    cleaned_item_id INT NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(100),
    relevance VARCHAR(50),
    impact VARCHAR(50),
    FOREIGN KEY (cleaned_item_id) REFERENCES cleaned_items(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_item_companies_stock ON item_companies(stock_code);

CREATE TABLE IF NOT EXISTS item_industries (
    id INT PRIMARY KEY AUTO_INCREMENT,
    cleaned_item_id INT NOT NULL,
    industry_name VARCHAR(255) NOT NULL,
    industry_level VARCHAR(50),
    impact VARCHAR(50),
    FOREIGN KEY (cleaned_item_id) REFERENCES cleaned_items(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS research_reports (
    id INT PRIMARY KEY AUTO_INCREMENT,
    cleaned_item_id INT NOT NULL,
    broker_name VARCHAR(255),
    analyst_name VARCHAR(255),
    report_type VARCHAR(50),
    rating VARCHAR(50),
    target_price DOUBLE,
    stock_code VARCHAR(20),
    stock_name VARCHAR(100),
    report_date VARCHAR(20),
    FOREIGN KEY (cleaned_item_id) REFERENCES cleaned_items(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_research_reports_stock ON research_reports(stock_code);
CREATE INDEX idx_research_reports_date ON research_reports(report_date);

-- ============ 行情数据层 ============

CREATE TABLE IF NOT EXISTS stock_info (
    stock_code VARCHAR(20) PRIMARY KEY,
    stock_name VARCHAR(100),
    industry_l1 VARCHAR(255),
    industry_l2 VARCHAR(255),
    market VARCHAR(10),
    list_date VARCHAR(20),
    total_shares DOUBLE,
    float_shares DOUBLE,
    market_cap DOUBLE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stock_daily (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_code VARCHAR(20) NOT NULL,
    trade_date VARCHAR(20) NOT NULL,
    `open` DOUBLE, high DOUBLE, low DOUBLE, `close` DOUBLE,
    volume DOUBLE,
    amount DOUBLE,
    turnover_rate DOUBLE,
    amplitude DOUBLE,
    change_pct DOUBLE,
    change_amount DOUBLE,
    UNIQUE KEY uq_stock_daily (stock_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_stock_daily_code_date ON stock_daily(stock_code, trade_date);

CREATE TABLE IF NOT EXISTS capital_flow (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_code VARCHAR(20) NOT NULL,
    trade_date VARCHAR(20) NOT NULL,
    main_net_inflow DOUBLE,
    super_large_net DOUBLE,
    large_net DOUBLE,
    medium_net DOUBLE,
    small_net DOUBLE,
    main_net_ratio DOUBLE,
    UNIQUE KEY uq_capital_flow (stock_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS industry_capital_flow (
    id INT PRIMARY KEY AUTO_INCREMENT,
    industry_name VARCHAR(255) NOT NULL,
    trade_date VARCHAR(20) NOT NULL,
    net_inflow DOUBLE,
    change_pct DOUBLE,
    leading_stock VARCHAR(100),
    UNIQUE KEY uq_industry_capital_flow (industry_name, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS northbound_flow (
    id INT PRIMARY KEY AUTO_INCREMENT,
    trade_date VARCHAR(20) NOT NULL UNIQUE,
    sh_net DOUBLE,
    sz_net DOUBLE,
    total_net DOUBLE,
    cumulative DOUBLE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS macro_indicators (
    id INT PRIMARY KEY AUTO_INCREMENT,
    indicator_name VARCHAR(255) NOT NULL,
    indicator_date VARCHAR(20) NOT NULL,
    value DOUBLE,
    unit VARCHAR(50),
    source VARCHAR(255),
    UNIQUE KEY uq_macro_indicators (indicator_name, indicator_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS financial_reports (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_code VARCHAR(20) NOT NULL,
    report_period VARCHAR(20) NOT NULL,
    revenue DOUBLE,
    net_profit DOUBLE,
    revenue_yoy DOUBLE,
    profit_yoy DOUBLE,
    eps DOUBLE,
    roe DOUBLE,
    beat_expectation INT,
    consensus_profit DOUBLE,
    actual_vs_consensus DOUBLE,
    report_date VARCHAR(20),
    UNIQUE KEY uq_financial_reports (stock_code, report_period)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 知识图谱层 ============

CREATE TABLE IF NOT EXISTS kg_entities (
    id INT PRIMARY KEY AUTO_INCREMENT,
    entity_type VARCHAR(100) NOT NULL,  -- market/theme/industry/industry_chain/company/macro_indicator/commodity/energy/intermediate/consumer_good/policy/revenue_element
    entity_name VARCHAR(255) NOT NULL,
    properties_json TEXT,
    description TEXT,
    investment_logic TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_kg_entities (entity_type, entity_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS kg_relationships (
    id INT PRIMARY KEY AUTO_INCREMENT,
    source_entity_id INT NOT NULL,
    target_entity_id INT NOT NULL,
    relation_type VARCHAR(100) NOT NULL,
    strength DOUBLE DEFAULT 0.5,
    direction VARCHAR(50) DEFAULT 'positive',
    evidence TEXT,
    confidence DOUBLE DEFAULT 0.5,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (source_entity_id) REFERENCES kg_entities(id),
    FOREIGN KEY (target_entity_id) REFERENCES kg_entities(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS kg_update_log (
    id INT PRIMARY KEY AUTO_INCREMENT,
    entity_id INT,
    relationship_id INT,
    action VARCHAR(50),
    old_value_json TEXT,
    new_value_json TEXT,
    source VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS kg_verification (
    id INT PRIMARY KEY AUTO_INCREMENT,
    relationship_id INT NOT NULL,
    verified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    verified_by VARCHAR(50),
    result VARCHAR(50),
    notes TEXT,
    FOREIGN KEY (relationship_id) REFERENCES kg_relationships(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 研究与机会层 ============

CREATE TABLE IF NOT EXISTS deep_research (
    id INT PRIMARY KEY AUTO_INCREMENT,
    research_type VARCHAR(50) NOT NULL,
    target VARCHAR(255) NOT NULL,
    research_date VARCHAR(20) NOT NULL,
    financial_score DOUBLE,
    valuation_score DOUBLE,
    technical_score DOUBLE,
    sentiment_score DOUBLE,
    catalyst_score DOUBLE,
    risk_score DOUBLE,
    overall_score DOUBLE,
    report_json LONGTEXT,
    recommendation TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS investment_opportunities (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_code VARCHAR(20),
    stock_name VARCHAR(100),
    opportunity_type VARCHAR(50),
    source VARCHAR(255),
    source_id INT,
    rating VARCHAR(10),
    tags_json TEXT,
    summary TEXT,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INT PRIMARY KEY AUTO_INCREMENT,
    pipeline_name VARCHAR(255) NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    status VARCHAR(50) DEFAULT 'running',
    items_processed INT DEFAULT 0,
    error_message TEXT,
    details_json TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS api_usage (
    id INT PRIMARY KEY AUTO_INCREMENT,
    api_name VARCHAR(100) NOT NULL,
    call_date VARCHAR(20) NOT NULL,
    call_count INT DEFAULT 0,
    input_tokens INT DEFAULT 0,
    output_tokens INT DEFAULT 0,
    cost_usd DOUBLE DEFAULT 0,
    UNIQUE KEY uq_api_usage (api_name, call_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 系统配置 KV 存储
CREATE TABLE IF NOT EXISTS system_config (
    config_key VARCHAR(255) PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 个股跟踪层 ============

CREATE TABLE IF NOT EXISTS watchlist (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_code VARCHAR(20) NOT NULL UNIQUE,
    stock_name VARCHAR(100),
    watch_type VARCHAR(50) DEFAULT 'interested',
    related_tags TEXT,
    notes TEXT,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS watchlist_tags (
    id INT PRIMARY KEY AUTO_INCREMENT,
    tag_name VARCHAR(255) NOT NULL UNIQUE,
    tag_type VARCHAR(50),
    watch_type VARCHAR(50) DEFAULT 'interested',
    related_stock_codes_json TEXT,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS holding_positions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(100),
    buy_date VARCHAR(20) NOT NULL,
    buy_price DOUBLE NOT NULL,
    quantity INT NOT NULL,
    status VARCHAR(50) DEFAULT 'open',
    sell_date VARCHAR(20),
    sell_price DOUBLE,
    pnl DOUBLE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS holding_research_log (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_code VARCHAR(20) NOT NULL,
    trigger_date VARCHAR(20) NOT NULL,
    trigger_type VARCHAR(50),
    trigger_item_id INT,
    research_id INT,
    change_highlights_json TEXT,
    report_pushed INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (research_id) REFERENCES deep_research(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS portfolio_reviews (
    id INT PRIMARY KEY AUTO_INCREMENT,
    review_date VARCHAR(20) NOT NULL,
    holdings_snapshot_json LONGTEXT,
    review_report LONGTEXT,
    lessons_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 热点动向层 ============

CREATE TABLE IF NOT EXISTS dashboard_tag_frequency (
    id INT PRIMARY KEY AUTO_INCREMENT,
    tag_name VARCHAR(255) NOT NULL,
    tag_type VARCHAR(50),
    dashboard_type INT NOT NULL,
    appear_date VARCHAR(20) NOT NULL,
    rank_position INT,
    context_json TEXT,
    UNIQUE KEY uq_tag_freq (tag_name, dashboard_type, appear_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_tag_freq_date ON dashboard_tag_frequency(appear_date);
CREATE INDEX idx_tag_freq_name ON dashboard_tag_frequency(tag_name);

CREATE TABLE IF NOT EXISTS tag_groups (
    id INT PRIMARY KEY AUTO_INCREMENT,
    group_name VARCHAR(255) NOT NULL,
    tags_json TEXT NOT NULL,
    group_logic TEXT,
    time_range VARCHAR(100),
    total_frequency INT DEFAULT 0,
    extra_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS tag_group_research (
    id INT PRIMARY KEY AUTO_INCREMENT,
    group_id INT NOT NULL,
    research_date VARCHAR(20) NOT NULL,
    macro_report LONGTEXT,
    macro_json LONGTEXT,
    industry_report LONGTEXT,
    industry_json LONGTEXT,
    news_summary_json LONGTEXT,
    news_parsed_json LONGTEXT,
    sector_heat_json TEXT,
    theme_heat_json LONGTEXT,
    top10_stocks_json LONGTEXT,
    logic_synthesis_json LONGTEXT,
    status VARCHAR(20) DEFAULT 'draft',
    saved_at TIMESTAMP NULL,
    portfolio_stats_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES tag_groups(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 文档存储层 ============

CREATE TABLE IF NOT EXISTS documents (
    id INT PRIMARY KEY AUTO_INCREMENT,
    doc_type VARCHAR(50) NOT NULL,
    file_type VARCHAR(50) NOT NULL,
    title VARCHAR(512) NOT NULL,
    author VARCHAR(255),
    publish_date VARCHAR(20) NOT NULL,
    source VARCHAR(255),
    oss_url VARCHAR(1024),
    text_content LONGTEXT,
    page_count INT DEFAULT 0,
    file_size INT DEFAULT 0,
    status INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_doc_type ON documents(doc_type);
CREATE INDEX idx_doc_publish_date ON documents(publish_date);
CREATE INDEX idx_doc_source ON documents(source);
CREATE INDEX idx_doc_status ON documents(status);

-- ============ 源文档导入层 ============

CREATE TABLE IF NOT EXISTS source_documents (
    id BIGINT PRIMARY KEY,
    doc_type VARCHAR(50) NOT NULL,
    file_type VARCHAR(20) NOT NULL,
    title VARCHAR(500),
    author VARCHAR(200),
    publish_date DATE,
    source VARCHAR(200),
    oss_url VARCHAR(1000),
    text_content LONGTEXT,
    extracted_text LONGTEXT,
    extract_status VARCHAR(20) DEFAULT 'pending',
    raw_item_id INT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_sd_file_type (file_type),
    KEY idx_sd_extract_status (extract_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 标签组聊天记录 ============

CREATE TABLE IF NOT EXISTS group_chat_messages (
    id INT PRIMARY KEY AUTO_INCREMENT,
    group_id INT NOT NULL,
    role VARCHAR(20) NOT NULL,
    content TEXT NOT NULL,
    metadata_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES tag_groups(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_chat_group ON group_chat_messages(group_id);

-- ============ 实时行情缓存层 ============

CREATE TABLE IF NOT EXISTS stock_realtime (
    stock_code VARCHAR(20) PRIMARY KEY,
    stock_name VARCHAR(100),
    market VARCHAR(10),
    last_price DOUBLE,
    change_pct DOUBLE,
    volume DOUBLE,
    amount DOUBLE,
    turnover_rate DOUBLE,
    pe_ratio DOUBLE,
    pb_ratio DOUBLE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 投资策略层 ============

CREATE TABLE IF NOT EXISTS investment_strategies (
    id INT PRIMARY KEY AUTO_INCREMENT,
    strategy_name VARCHAR(255) NOT NULL,
    description TEXT,
    rules_json TEXT,
    ai_rules_text TEXT,
    sort_order INT DEFAULT 0,
    is_active INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS strategy_stocks (
    id INT PRIMARY KEY AUTO_INCREMENT,
    strategy_id INT NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(100),
    source VARCHAR(50) DEFAULT 'manual',
    status VARCHAR(50) DEFAULT 'active',
    ai_reason TEXT,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,
    UNIQUE KEY uq_strategy_stock (strategy_id, stock_code),
    FOREIGN KEY (strategy_id) REFERENCES investment_strategies(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 新管线：提取层 ============

CREATE TABLE IF NOT EXISTS extracted_texts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    source_format VARCHAR(20) NOT NULL,
    publish_time DATETIME,
    extract_time DATETIME DEFAULT NOW(),
    full_text MEDIUMTEXT NOT NULL,
    source_doc_id BIGINT,
    source_ref VARCHAR(100),
    extract_quality ENUM('pending','pass','fail') DEFAULT 'pending',
    summary_status ENUM('pending','done','skipped') DEFAULT 'pending',
    kg_status ENUM('pending','done','failed') DEFAULT 'pending',
    INDEX idx_et_source (source),
    INDEX idx_et_extract_quality (extract_quality),
    INDEX idx_et_summary_status (summary_status),
    INDEX idx_et_kg_status (kg_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 新管线：内容总结层 ============

CREATE TABLE IF NOT EXISTS content_summaries (
    id INT AUTO_INCREMENT PRIMARY KEY,
    extracted_text_id INT NOT NULL,
    doc_type VARCHAR(30) DEFAULT NULL COMMENT 'announcement/financial_report/policy_doc/research_report/strategy_report/roadshow_notes/feature_news/flash_news/digest_news/data_release/market_commentary/social_post/chat_record',
    summary TEXT NOT NULL,
    fact_summary TEXT,
    opinion_summary TEXT,
    evidence_assessment TEXT,
    info_gaps TEXT,
    created_at DATETIME DEFAULT NOW(),
    INDEX idx_cs_extracted_text_id (extracted_text_id),
    INDEX idx_cs_doc_type (doc_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 为已存在的 content_summaries 表补充 doc_type 字段（幂等）
ALTER TABLE content_summaries
    ADD COLUMN IF NOT EXISTS doc_type VARCHAR(30) DEFAULT NULL
        COMMENT 'announcement/financial_report/policy_doc/research_report/strategy_report/roadshow_notes/feature_news/flash_news/digest_news/data_release/market_commentary/social_post/chat_record',
    ADD COLUMN IF NOT EXISTS family TINYINT DEFAULT NULL
        COMMENT '1=structured 2=analysis 3=informal 4=brief',
    ADD COLUMN IF NOT EXISTS type_fields JSON DEFAULT NULL
        COMMENT '族特有字段 JSON（key_facts/key_arguments/speaker/event_what 等）',
    ADD INDEX IF NOT EXISTS idx_cs_doc_type (doc_type),
    ADD INDEX IF NOT EXISTS idx_cs_family (family);

-- ============ 新管线：股票提及层 ============

CREATE TABLE IF NOT EXISTS stock_mentions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    extracted_text_id INT NOT NULL,
    stock_name VARCHAR(50) NOT NULL,
    stock_code VARCHAR(20),
    related_themes TEXT,
    related_events TEXT,
    theme_logic TEXT,
    mention_time DATETIME,
    confidence FLOAT DEFAULT 0.5,
    created_at DATETIME DEFAULT NOW(),
    INDEX idx_sm_extracted_text_id (extracted_text_id),
    INDEX idx_sm_stock_name (stock_name),
    INDEX idx_sm_mention_time (mention_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 选股策略规则库 ============

CREATE TABLE IF NOT EXISTS stock_selection_rules (
    id INT PRIMARY KEY AUTO_INCREMENT,
    category VARCHAR(50) NOT NULL,
    rule_name VARCHAR(255) NOT NULL,
    definition TEXT NOT NULL,
    layer TINYINT DEFAULT 0 COMMENT '0=未分类, 1=量化, 2=AI轻量, 3=AI深度',
    is_system INT DEFAULT 0,
    is_active INT DEFAULT 1,
    sort_order INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_rule_name (rule_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 为已存在的 stock_selection_rules 表补充 layer 字段（幂等）
ALTER TABLE stock_selection_rules
    ADD COLUMN IF NOT EXISTS layer TINYINT DEFAULT 0 COMMENT '0=未分类, 1=量化, 2=AI轻量, 3=AI深度';

-- ============ 选股规则标签计算结果 ============

CREATE TABLE IF NOT EXISTS stock_rule_tags (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_code VARCHAR(20) NOT NULL,
    rule_id INT NOT NULL,
    rule_category VARCHAR(50),
    rule_name VARCHAR(255),
    matched TINYINT DEFAULT 1,
    confidence FLOAT DEFAULT 1.0,
    evidence TEXT,
    layer TINYINT NOT NULL COMMENT '1=量化, 2=AI轻量, 3=AI深度',
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_stock_rule (stock_code, rule_id),
    INDEX idx_srt_stock (stock_code),
    INDEX idx_srt_rule (rule_id),
    INDEX idx_srt_layer (layer)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ K线阶段分析 ============

CREATE TABLE IF NOT EXISTS chart_analysis (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_code VARCHAR(20) NOT NULL,
    analysis_date DATE NOT NULL,
    stages_json MEDIUMTEXT,
    current_stage_json TEXT,
    predictions_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_stock_date (stock_code, analysis_date),
    INDEX idx_ca_stock (stock_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============ 个股报告Chatbot对话 ============

CREATE TABLE IF NOT EXISTS stock_chat_messages (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_code VARCHAR(20) NOT NULL,
    role VARCHAR(20) NOT NULL,
    content TEXT NOT NULL,
    metadata_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_stock_chat (stock_code, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

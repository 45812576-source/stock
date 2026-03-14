-- 个人股票分析系统 数据库Schema
-- SQLite

PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- ============ 原始数据层 ============

CREATE TABLE IF NOT EXISTS data_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    source_type TEXT NOT NULL,  -- 'jasper','djyanbao','fxbaogao','iwencai','akshare','audio'
    base_url TEXT,
    daily_limit INTEGER DEFAULT 0,
    monthly_limit INTEGER DEFAULT 0,
    today_used INTEGER DEFAULT 0,
    month_used INTEGER DEFAULT 0,
    last_reset_date TEXT,
    config_json TEXT,  -- 额外配置
    enabled INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    external_id TEXT,  -- 数据源原始ID，用于去重
    title TEXT,
    content TEXT,
    url TEXT,
    published_at TIMESTAMP,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_status TEXT DEFAULT 'pending',  -- pending/processing/cleaned/failed
    item_type TEXT,  -- news/report/announcement/data
    meta_json TEXT,
    FOREIGN KEY (source_id) REFERENCES data_sources(id),
    UNIQUE(source_id, external_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_items_status ON raw_items(processing_status);
CREATE INDEX IF NOT EXISTS idx_raw_items_fetched ON raw_items(fetched_at);

-- ============ 清洗数据层 ============

CREATE TABLE IF NOT EXISTS cleaned_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_item_id INTEGER NOT NULL,
    event_type TEXT,  -- macro_policy/industry_news/company_event/earnings/research_report
    sentiment TEXT,  -- positive/negative/neutral
    importance INTEGER DEFAULT 3,  -- 1-5
    summary TEXT,
    key_points_json TEXT,
    tags_json TEXT,  -- ["AI","半导体","新能源"]
    impact_analysis TEXT,
    time_horizon TEXT,  -- short/medium/long
    confidence REAL,
    structured_json TEXT,  -- 完整Skill结构化JSON（items/tags/opportunity/type_specific等）
    cleaned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (raw_item_id) REFERENCES raw_items(id)
);

CREATE INDEX IF NOT EXISTS idx_cleaned_items_type ON cleaned_items(event_type);
CREATE INDEX IF NOT EXISTS idx_cleaned_items_sentiment ON cleaned_items(sentiment);

CREATE TABLE IF NOT EXISTS item_companies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cleaned_item_id INTEGER NOT NULL,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    relevance TEXT,  -- primary/secondary/mentioned
    impact TEXT,  -- positive/negative/neutral
    FOREIGN KEY (cleaned_item_id) REFERENCES cleaned_items(id)
);

CREATE INDEX IF NOT EXISTS idx_item_companies_stock ON item_companies(stock_code);

CREATE TABLE IF NOT EXISTS item_industries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cleaned_item_id INTEGER NOT NULL,
    industry_name TEXT NOT NULL,
    industry_level TEXT,  -- level1/level2/level3
    impact TEXT,
    FOREIGN KEY (cleaned_item_id) REFERENCES cleaned_items(id)
);

CREATE TABLE IF NOT EXISTS research_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cleaned_item_id INTEGER NOT NULL,
    broker_name TEXT,
    analyst_name TEXT,
    report_type TEXT,  -- initiate/maintain/upgrade/downgrade
    rating TEXT,  -- buy/overweight/neutral/underweight/sell
    target_price REAL,
    stock_code TEXT,
    stock_name TEXT,
    report_date TEXT,
    FOREIGN KEY (cleaned_item_id) REFERENCES cleaned_items(id)
);

CREATE INDEX IF NOT EXISTS idx_research_reports_stock ON research_reports(stock_code);
CREATE INDEX IF NOT EXISTS idx_research_reports_date ON research_reports(report_date);

-- ============ 行情数据层 ============

CREATE TABLE IF NOT EXISTS stock_info (
    stock_code TEXT PRIMARY KEY,
    stock_name TEXT,
    industry_l1 TEXT,
    industry_l2 TEXT,
    market TEXT,  -- sh/sz/bj
    list_date TEXT,
    total_shares REAL,
    float_shares REAL,
    market_cap REAL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stock_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL, high REAL, low REAL, close REAL,
    volume REAL,
    amount REAL,
    turnover_rate REAL,
    amplitude REAL,
    change_pct REAL,
    change_amount REAL,
    UNIQUE(stock_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_daily_code_date ON stock_daily(stock_code, trade_date);

CREATE TABLE IF NOT EXISTS capital_flow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    main_net_inflow REAL,
    super_large_net REAL,
    large_net REAL,
    medium_net REAL,
    small_net REAL,
    main_net_ratio REAL,
    UNIQUE(stock_code, trade_date)
);

CREATE TABLE IF NOT EXISTS industry_capital_flow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    industry_name TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    net_inflow REAL,
    change_pct REAL,
    leading_stock TEXT,
    UNIQUE(industry_name, trade_date)
);

CREATE TABLE IF NOT EXISTS northbound_flow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL UNIQUE,
    sh_net REAL,  -- 沪股通净买入
    sz_net REAL,  -- 深股通净买入
    total_net REAL,
    cumulative REAL
);

CREATE TABLE IF NOT EXISTS macro_indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator_name TEXT NOT NULL,
    indicator_date TEXT NOT NULL,
    value REAL,
    unit TEXT,
    source TEXT,
    UNIQUE(indicator_name, indicator_date)
);

CREATE TABLE IF NOT EXISTS financial_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    report_period TEXT NOT NULL,  -- 2024Q1, 2024H1, 2024Q3, 2024A
    revenue REAL,
    net_profit REAL,
    revenue_yoy REAL,
    profit_yoy REAL,
    eps REAL,
    roe REAL,
    beat_expectation INTEGER,  -- 1=超预期, 0=符合, -1=不及
    consensus_profit REAL,
    actual_vs_consensus REAL,
    report_date TEXT,
    UNIQUE(stock_code, report_period)
);

-- ============ 知识图谱层 ============

CREATE TABLE IF NOT EXISTS kg_entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,  -- market/theme/industry/industry_chain/company/macro_indicator/commodity/energy/intermediate/consumer_good/policy/revenue_element
    entity_name TEXT NOT NULL,
    properties_json TEXT,
    description TEXT,
    investment_logic TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(entity_type, entity_name)
);

CREATE TABLE IF NOT EXISTS kg_relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_entity_id INTEGER NOT NULL,
    target_entity_id INTEGER NOT NULL,
    relation_type TEXT NOT NULL,  -- impacts/belongs_to/competes/supplies/benefits/related
    strength REAL DEFAULT 0.5,  -- 0-1
    direction TEXT DEFAULT 'positive',  -- positive/negative/neutral
    evidence TEXT,
    confidence REAL DEFAULT 0.5,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_entity_id) REFERENCES kg_entities(id),
    FOREIGN KEY (target_entity_id) REFERENCES kg_entities(id)
);

CREATE TABLE IF NOT EXISTS kg_update_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER,
    relationship_id INTEGER,
    action TEXT,  -- create/update/delete
    old_value_json TEXT,
    new_value_json TEXT,
    source TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kg_verification (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    relationship_id INTEGER NOT NULL,
    verified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    verified_by TEXT,  -- auto/manual
    result TEXT,  -- confirmed/weakened/invalidated
    notes TEXT,
    FOREIGN KEY (relationship_id) REFERENCES kg_relationships(id)
);

-- ============ 研究与机会层 ============

CREATE TABLE IF NOT EXISTS deep_research (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    research_type TEXT NOT NULL,  -- macro/industry/stock
    target TEXT NOT NULL,
    research_date TEXT NOT NULL,
    financial_score REAL,
    valuation_score REAL,
    technical_score REAL,
    sentiment_score REAL,
    catalyst_score REAL,
    risk_score REAL,
    overall_score REAL,
    report_json TEXT,
    recommendation TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS investment_opportunities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT,
    stock_name TEXT,
    opportunity_type TEXT,  -- value/growth/momentum/event
    source TEXT,  -- dashboard/research/manual
    source_id INTEGER,
    rating TEXT,  -- A/B/C
    tags_json TEXT,
    summary TEXT,
    status TEXT DEFAULT 'active',  -- active/positioned/expired/tracking
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name TEXT NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT DEFAULT 'running',  -- running/success/failed
    items_processed INTEGER DEFAULT 0,
    error_message TEXT,
    details_json TEXT
);

CREATE TABLE IF NOT EXISTS api_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    api_name TEXT NOT NULL,  -- claude/akshare/jasper
    call_date TEXT NOT NULL,
    call_count INTEGER DEFAULT 0,
    input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    cost_usd REAL DEFAULT 0,
    UNIQUE(api_name, call_date)
);

-- 系统配置 KV 存储
CREATE TABLE IF NOT EXISTS system_config (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
);

-- ============ 个股跟踪层 ============

CREATE TABLE IF NOT EXISTS watchlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL UNIQUE,
    stock_name TEXT,
    watch_type TEXT DEFAULT 'interested',  -- interested/holding/none
    related_tags TEXT,  -- JSON array
    notes TEXT,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS watchlist_tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag_name TEXT NOT NULL UNIQUE,
    tag_type TEXT,  -- theme/industry/macro
    watch_type TEXT DEFAULT 'interested',
    related_stock_codes_json TEXT,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS holding_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    buy_date TEXT NOT NULL,
    buy_price REAL NOT NULL,
    quantity INTEGER NOT NULL,
    status TEXT DEFAULT 'open',  -- open/closed
    sell_date TEXT,
    sell_price REAL,
    pnl REAL,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS holding_research_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code TEXT NOT NULL,
    trigger_date TEXT NOT NULL,
    trigger_type TEXT,  -- news/report/earnings/capital_flow
    trigger_item_id INTEGER,
    research_id INTEGER,
    change_highlights_json TEXT,
    report_pushed INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (research_id) REFERENCES deep_research(id)
);

CREATE TABLE IF NOT EXISTS portfolio_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_date TEXT NOT NULL,
    holdings_snapshot_json TEXT,
    review_report TEXT,
    lessons_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============ 热点动向层 ============

CREATE TABLE IF NOT EXISTS dashboard_tag_frequency (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag_name TEXT NOT NULL,
    tag_type TEXT,  -- theme/industry/macro/stock
    dashboard_type INTEGER NOT NULL,  -- 1-8
    appear_date TEXT NOT NULL,
    rank_position INTEGER,
    context_json TEXT,
    UNIQUE(tag_name, dashboard_type, appear_date)
);

CREATE INDEX IF NOT EXISTS idx_tag_freq_date ON dashboard_tag_frequency(appear_date);
CREATE INDEX IF NOT EXISTS idx_tag_freq_name ON dashboard_tag_frequency(tag_name);

CREATE TABLE IF NOT EXISTS tag_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    tags_json TEXT NOT NULL,
    group_logic TEXT,
    time_range TEXT,
    total_frequency INTEGER DEFAULT 0,
    extra_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tag_group_research (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER NOT NULL,
    research_date TEXT NOT NULL,
    macro_report TEXT,
    macro_json TEXT,
    industry_report TEXT,
    industry_json TEXT,
    news_summary_json TEXT,
    news_parsed_json TEXT,
    sector_heat_json TEXT,
    theme_heat_json TEXT,
    top10_stocks_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES tag_groups(id)
);

-- ============ 文档存储层 ============

CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_type TEXT NOT NULL,        -- 'news' / 'report'
    file_type TEXT NOT NULL,       -- 'pdf' / 'word' / 'txt'
    title TEXT NOT NULL,
    author TEXT,
    publish_date TEXT NOT NULL,
    source TEXT,
    oss_url TEXT,                  -- 文件存储路径（pdf/word）
    text_content TEXT,             -- 纯文本内容（txt 或提取后的文本）
    page_count INTEGER DEFAULT 0,
    file_size INTEGER DEFAULT 0,
    status INTEGER DEFAULT 1,     -- 0=删除 1=正常
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_doc_type ON documents(doc_type);
CREATE INDEX IF NOT EXISTS idx_doc_publish_date ON documents(publish_date);
CREATE INDEX IF NOT EXISTS idx_doc_source ON documents(source);
CREATE INDEX IF NOT EXISTS idx_doc_status ON documents(status);

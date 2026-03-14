-- ============================================================
-- migration_v2.sql — 系统设置 + 自选股大改 Schema 变更
-- 执行方式: mysql -u<user> -p<pass> stock_analysis < db/migration_v2.sql
-- ============================================================

-- ── 1. 多模型配置表 ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS model_configs (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stage VARCHAR(50) NOT NULL UNIQUE,
    provider VARCHAR(50) NOT NULL DEFAULT 'claude_cli',
    model_name VARCHAR(100) NOT NULL DEFAULT 'sonnet',
    api_key_ref VARCHAR(100),
    base_url VARCHAR(512),
    extra_json TEXT,
    enabled INT DEFAULT 1,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO model_configs (stage, provider, model_name) VALUES
  ('extraction', 'claude_cli', 'sonnet'),
  ('cleaning',   'claude_cli', 'sonnet'),
  ('kg',         'claude_cli', 'sonnet'),
  ('research',   'claude_cli', 'sonnet'),
  ('hotspot',    'claude_cli', 'sonnet'),
  ('chat',       'claude_cli', 'sonnet'),
  ('vision',     'claude_cli', 'sonnet'),
  ('audio',      'groq',       'whisper-large-v3');


-- ── 2. 自选股分组表 ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS watchlist_lists (
    id INT PRIMARY KEY AUTO_INCREMENT,
    list_type VARCHAR(30) NOT NULL,
    list_name VARCHAR(200) NOT NULL,
    strategy_id INT,
    description TEXT,
    background_info TEXT,
    show_on_overview INT DEFAULT 0,
    sort_order INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_strategy_list (list_type, strategy_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO watchlist_lists (id, list_type, list_name, show_on_overview, sort_order)
VALUES (1, 'like', '收藏', 1, 0);


-- ── 3. 分组内股票表 ──────────────────────────────────────────

CREATE TABLE IF NOT EXISTS watchlist_list_stocks (
    id INT PRIMARY KEY AUTO_INCREMENT,
    list_id INT NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(100),
    source VARCHAR(50) DEFAULT 'manual',
    ai_reason TEXT,
    status VARCHAR(30) DEFAULT 'active',
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_list_stock (list_id, stock_code),
    FOREIGN KEY (list_id) REFERENCES watchlist_lists(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ── 4. 结构化数据监控规则表 ──────────────────────────────────

CREATE TABLE IF NOT EXISTS data_monitor_rules (
    id INT PRIMARY KEY AUTO_INCREMENT,
    module_name VARCHAR(100) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    stock_pool VARCHAR(50) DEFAULT 'watchlist',
    custom_codes_json TEXT,
    lookback_days INT DEFAULT 7,
    schedule_cron VARCHAR(100),
    enabled INT DEFAULT 1,
    last_run_at TIMESTAMP NULL,
    last_status VARCHAR(30),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_module_dtype (module_name, data_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO data_monitor_rules (module_name, data_type, stock_pool, lookback_days, schedule_cron) VALUES
  ('heatmap',          'capital',   'all',       7,  '0 18 * * 1-5'),
  ('watchlist_alerts', 'daily',     'watchlist', 10, '0 16 * * 1-5'),
  ('deep_research',    'daily',     'watchlist', 30, '0 19 * * 5'),
  ('deep_research',    'financial', 'watchlist', 90, '0 20 * * 5'),
  ('screening',        'daily',     'all',        1, '0 16 * * 1-5'),
  ('screening',        'capital',   'all',       20, '0 17 * * 1-5');


-- ── 5. pipeline_runs 增加 stage 列 ──────────────────────────

ALTER TABLE pipeline_runs
  ADD COLUMN IF NOT EXISTS stage VARCHAR(50) AFTER pipeline_name;


-- ── 6. 迁移现有 watchlist 数据 ───────────────────────────────

INSERT IGNORE INTO watchlist_list_stocks (list_id, stock_code, stock_name, source, added_at)
SELECT 1,
       stock_code,
       stock_name,
       CASE watch_type WHEN 'holding' THEN 'manual' ELSE 'like' END,
       added_at
FROM watchlist;

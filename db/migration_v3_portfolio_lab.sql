-- ============================================================
-- migration_v3_portfolio_lab.sql — Portfolio实验室 Schema 变更
-- 执行方式: mysql -u<user> -p<pass> stock_analysis < db/migration_v3_portfolio_lab.sql
-- ============================================================

-- ── 1. watchlist_lists 新增字段 ────────────────────────────────
ALTER TABLE watchlist_lists ADD COLUMN investment_logic TEXT AFTER background_info;
ALTER TABLE watchlist_lists ADD COLUMN project_type VARCHAR(30) DEFAULT 'custom' AFTER investment_logic;

-- ── 2. 更新现有数据的 project_type ─────────────────────────────
UPDATE watchlist_lists SET project_type = 'portfolio' WHERE id = 1;
UPDATE watchlist_lists SET project_type = 'theme' WHERE list_type = 'theme' AND project_type = 'custom';
UPDATE watchlist_lists SET project_type = 'portfolio' WHERE list_type = 'like' AND project_type = 'custom';

-- ── 3. 项目聊天消息表 ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS project_chat_messages (
    id INT PRIMARY KEY AUTO_INCREMENT,
    project_id INT NOT NULL,
    role VARCHAR(20) NOT NULL,
    content TEXT NOT NULL,
    metadata_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (project_id) REFERENCES watchlist_lists(id) ON DELETE CASCADE,
    INDEX idx_pcm_project (project_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 4. 聊天关联策略表 ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS project_chat_strategies (
    id INT PRIMARY KEY AUTO_INCREMENT,
    message_id INT NOT NULL,
    strategy_id INT NOT NULL,
    FOREIGN KEY (message_id) REFERENCES project_chat_messages(id) ON DELETE CASCADE,
    FOREIGN KEY (strategy_id) REFERENCES investment_strategies(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

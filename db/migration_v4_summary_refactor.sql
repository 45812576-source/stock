-- ============================================================
-- migration_v4_summary_refactor.sql
-- 摘要架构简化：content_summaries 加 type_fields；
-- model_configs 加 cleaning_deep (reasoner) + 更新 cleaning 到 deepseek-chat
-- 执行: mysql -h <host> -P <port> -u <user> -p stock_analysis < db/migration_v4_summary_refactor.sql
-- ============================================================

-- ── 1. content_summaries 补充字段 ──────────────────────────────────────────
ALTER TABLE content_summaries
    ADD COLUMN IF NOT EXISTS family TINYINT DEFAULT NULL
        COMMENT '1=structured 2=analysis 3=informal 4=brief',
    ADD COLUMN IF NOT EXISTS type_fields JSON DEFAULT NULL
        COMMENT '族特有字段 JSON（key_facts/key_arguments/speaker/event_what 等）',
    ADD COLUMN IF NOT EXISTS detail_table VARCHAR(30) DEFAULT NULL
        COMMENT '(legacy) 详情表名，新数据不再写入',
    ADD COLUMN IF NOT EXISTS detail_id INT DEFAULT NULL
        COMMENT '(legacy) 详情表主键，新数据不再写入',
    ADD INDEX IF NOT EXISTS idx_cs_family (family);

-- ── 2. model_configs 补充 cleaning 系列 stage ──────────────────────────────
-- cleaning: 族1/3/4 — deepseek-chat（快，成本低）
INSERT INTO model_configs (stage, provider, model_name, api_key_ref, base_url, enabled)
VALUES ('cleaning', 'deepseek', 'deepseek-chat', 'deepseek_api_key', 'https://api.deepseek.com/v1', 1)
ON DUPLICATE KEY UPDATE
    provider = 'deepseek',
    model_name = 'deepseek-chat',
    api_key_ref = 'deepseek_api_key',
    base_url = 'https://api.deepseek.com/v1',
    enabled = 1;

-- cleaning_deep: 族2（研报/策略/路演）— deepseek-reasoner
INSERT INTO model_configs (stage, provider, model_name, api_key_ref, base_url, enabled)
VALUES ('cleaning_deep', 'deepseek', 'deepseek-reasoner', 'deepseek_api_key', 'https://api.deepseek.com/v1', 1)
ON DUPLICATE KEY UPDATE
    provider = 'deepseek',
    model_name = 'deepseek-reasoner',
    api_key_ref = 'deepseek_api_key',
    base_url = 'https://api.deepseek.com/v1',
    enabled = 1;

-- ============================================================
-- migration_v4_project_analysis.sql — Portfolio 项目级分析存储
-- 执行方式: mysql -u<user> -p<pass> stock_analysis < db/migration_v4_project_analysis.sql
-- ============================================================

-- ── watchlist_lists 新增两列 ────────────────────────────────
ALTER TABLE watchlist_lists ADD COLUMN IF NOT EXISTS source_group_id INT NULL
    COMMENT '来源热点标签组 ID（场景A：从热点一键导入）';

ALTER TABLE watchlist_lists ADD COLUMN IF NOT EXISTS analysis_json LONGTEXT NULL
    COMMENT '项目四模块分析结果 JSON（news_parsed/theme_heat/macro/industry）';

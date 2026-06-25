-- ============================================================
-- migration_v7_chain_baseline.sql — 产业链认知管理
-- ============================================================

CREATE TABLE IF NOT EXISTS chain_baseline (
    id INT PRIMARY KEY AUTO_INCREMENT,
    chain_name VARCHAR(100) NOT NULL COMMENT '产业链名称',
    version INT NOT NULL DEFAULT 1 COMMENT '版本号(递增)',
    baseline_json LONGTEXT NOT NULL COMMENT '结构化 Baseline JSON',
    source_summary TEXT COMMENT '生成来源摘要',
    created_at DATETIME DEFAULT NOW(),
    created_by VARCHAR(50) DEFAULT 'system',
    UNIQUE KEY uk_chain_version (chain_name, version),
    INDEX idx_chain_latest (chain_name, version DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='产业链认知Baseline';

CREATE TABLE IF NOT EXISTS chain_baseline_diff (
    id INT PRIMARY KEY AUTO_INCREMENT,
    chain_name VARCHAR(100) NOT NULL,
    base_version INT NOT NULL COMMENT '基于的baseline版本',
    status ENUM('generating','ready','editing','merged','rejected') DEFAULT 'generating',
    diff_json LONGTEXT COMMENT '结构化Diff JSON',
    user_edited_json LONGTEXT COMMENT '用户编辑后的Diff',
    merged_to_version INT DEFAULT NULL COMMENT '合并后的新版本号',
    input_sources_json TEXT COMMENT '输入来源记录',
    created_at DATETIME DEFAULT NOW(),
    updated_at DATETIME DEFAULT NOW() ON UPDATE NOW(),
    INDEX idx_chain_status (chain_name, status),
    INDEX idx_chain_base (chain_name, base_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='产业链认知Diff';

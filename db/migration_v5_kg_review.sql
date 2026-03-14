-- migration_v5_kg_review.sql
-- KG 人工审核系统 — 在本地库执行
-- 为 kg_entities 和 kg_relationships 增加审核状态列，新建审核历史表

-- ── 1. kg_entities 加列 ──────────────────────────────────────────────────────

ALTER TABLE kg_entities
  ADD COLUMN review_status ENUM('unreviewed','pending_approval','approved','rejected')
    NOT NULL DEFAULT 'unreviewed' AFTER entity_name,
  ADD COLUMN review_note TEXT AFTER review_status,
  ADD COLUMN reviewed_by INT AFTER review_note,
  ADD COLUMN reviewed_at DATETIME AFTER reviewed_by,
  ADD COLUMN approved_by INT AFTER reviewed_at,
  ADD COLUMN approved_at DATETIME AFTER approved_by,
  ADD INDEX idx_kg_entities_review_status (review_status);

-- ── 2. kg_relationships 加列 ──────────────────────────────────────────────────

ALTER TABLE kg_relationships
  ADD COLUMN review_status ENUM('unreviewed','pending_approval','approved','rejected')
    NOT NULL DEFAULT 'unreviewed' AFTER relation_type,
  ADD COLUMN review_note TEXT AFTER review_status,
  ADD COLUMN reviewed_by INT AFTER review_note,
  ADD COLUMN reviewed_at DATETIME AFTER reviewed_by,
  ADD COLUMN approved_by INT AFTER reviewed_at,
  ADD COLUMN approved_at DATETIME AFTER approved_by,
  ADD INDEX idx_kg_relationships_review_status (review_status);

-- ── 3. kg_review_log 审核历史表 ──────────────────────────────────────────────

-- ── 3. kg_triple_sources 加列 ──────────────────────────────────────────────────

ALTER TABLE kg_triple_sources
  ADD COLUMN review_status ENUM('unreviewed','pending_approval','approved','rejected')
    NOT NULL DEFAULT 'unreviewed' AFTER source_title,
  ADD COLUMN review_note TEXT AFTER review_status,
  ADD COLUMN reviewed_by INT AFTER review_note,
  ADD COLUMN reviewed_at DATETIME AFTER reviewed_by,
  ADD INDEX idx_kg_triple_sources_review_status (review_status);

-- ── 4. kg_review_log 审核历史表 ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS kg_review_log (
  id INT PRIMARY KEY AUTO_INCREMENT,
  target_type ENUM('entity','relationship') NOT NULL,
  target_id INT NOT NULL,
  action ENUM('approve','reject','edit','revert') NOT NULL,
  old_values JSON,
  new_values JSON,
  note TEXT,
  user_id INT NOT NULL,
  user_role VARCHAR(20),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_kg_review_log_target (target_type, target_id),
  INDEX idx_kg_review_log_user (user_id),
  INDEX idx_kg_review_log_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Source Documents 状态流转迁移脚本
-- 版本: v6
-- 日期: 2025-03-25
-- 说明: 扩展 extract_status 字段状态值，新增审核相关字段

-- 1. 修改 extract_status 字段，扩展状态值
-- 原状态: pending, done, skipped
-- 新状态: failed/pending/extracted/ready_to_pipe/processing/done/url_expired/rejected/skipped/cleaning
ALTER TABLE source_documents
    MODIFY COLUMN extract_status VARCHAR(20) DEFAULT 'pending'
        COMMENT 'failed/pending/extracted/ready_to_pipe/processing/done/url_expired/rejected/skipped/cleaning';

-- 2. 新增审核相关字段
ALTER TABLE source_documents
    ADD COLUMN reviewed_at DATETIME NULL COMMENT '审核时间',
    ADD COLUMN reviewed_by VARCHAR(50) NULL COMMENT '审核人',
    ADD COLUMN review_notes TEXT NULL COMMENT '审核备注';

-- 3. 添加索引优化查询
ALTER TABLE source_documents
    ADD INDEX idx_sd_reviewed (reviewed_at);

-- 4. 将现有的 'done' 状态迁移为 'extracted'（等待进入管线）
UPDATE source_documents
SET extract_status = 'extracted'
WHERE extract_status = 'done' AND extracted_text IS NOT NULL;

-- 5. 将已进入 extracted_texts 表的记录标记为 'ready_to_pipe'
UPDATE source_documents sd
SET extract_status = 'ready_to_pipe'
WHERE EXISTS (
    SELECT 1 FROM extracted_texts et WHERE et.source_doc_id = sd.id
) AND sd.extract_status = 'extracted';
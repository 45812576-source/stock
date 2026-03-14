-- KG 实体类型体系重构 迁移脚本
-- 合并 macro_factor + indicator → macro_indicator
-- 合并 event → policy

-- 1. 合并 macro_factor 和 indicator 为 macro_indicator
UPDATE kg_entities SET entity_type='macro_indicator' WHERE entity_type IN ('macro_factor', 'indicator');

-- 2. 合并 event 为 policy
UPDATE kg_entities SET entity_type='policy' WHERE entity_type='event';

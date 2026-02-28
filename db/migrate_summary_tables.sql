-- ============================================================
-- 摘要架构简化迁移 v2
-- 不再写 4 张 detail 表；content_summaries 新增 type_fields + family 列
-- 执行: mysql -h <host> -P <port> -u <user> -p stock_analysis < migrate_summary_tables.sql
-- ============================================================

-- content_summaries 补充字段（幂等）
ALTER TABLE content_summaries
    ADD COLUMN IF NOT EXISTS family TINYINT DEFAULT NULL
        COMMENT '1=structured 2=analysis 3=informal 4=brief',
    ADD COLUMN IF NOT EXISTS type_fields JSON DEFAULT NULL
        COMMENT '族特有字段：key_facts/type_fields/speaker/event_what 等',
    ADD INDEX IF NOT EXISTS idx_cs_family (family);

-- ── 族1-4 detail 表保留（可选，供历史数据读取），新数据不再写入 ──────────────
-- 如需彻底清理旧表可在确认无查询依赖后手动 DROP：
--   DROP TABLE IF EXISTS summary_structured, summary_analysis, summary_informal, summary_brief;

-- 确保旧 detail_table/detail_id 列不存在时补充（向前兼容）
ALTER TABLE content_summaries
    ADD COLUMN IF NOT EXISTS detail_table VARCHAR(30) DEFAULT NULL
        COMMENT '(legacy) 详情表名',
    ADD COLUMN IF NOT EXISTS detail_id INT DEFAULT NULL
        COMMENT '(legacy) 详情表主键';


-- ── 族1：结构化提取（announcement/financial_report/data_release/policy_doc/xlsx_data）──
CREATE TABLE IF NOT EXISTS summary_structured (
    id INT AUTO_INCREMENT PRIMARY KEY,
    extracted_text_id INT NOT NULL,
    doc_type VARCHAR(30) NOT NULL,
    summary TEXT NOT NULL COMMENT '一句话核心摘要（50字以内）',
    subject_entities TEXT COMMENT '涉及主体（公司/机构/政策发布方）',
    key_facts JSON COMMENT '关键事实，按doc_type结构不同',
    key_data JSON COMMENT '关键数值数据',
    effective_date VARCHAR(20) DEFAULT NULL COMMENT '生效/发布日期',
    impact_scope TEXT COMMENT '影响范围/行业',
    created_at DATETIME DEFAULT NOW(),
    INDEX idx_ss_extracted_text_id (extracted_text_id),
    INDEX idx_ss_doc_type (doc_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='族1：结构化数据提取摘要（公告/财报/数据播报/政策/统计数据）';

-- ── 族2：深度FOE分析（research_report/strategy_report/feature_news/roadshow_notes）──
CREATE TABLE IF NOT EXISTS summary_analysis (
    id INT AUTO_INCREMENT PRIMARY KEY,
    extracted_text_id INT NOT NULL,
    doc_type VARCHAR(30) NOT NULL,
    summary TEXT NOT NULL COMMENT '一句话核心摘要（50字以内）',
    fact_summary TEXT COMMENT '关键事实要点',
    opinion_summary TEXT COMMENT '主要观点和判断',
    evidence_assessment TEXT COMMENT '证据质量评估',
    info_gaps TEXT COMMENT '信息缺口',
    key_arguments JSON COMMENT '核心论点链（简化FOE结构）',
    type_fields JSON COMMENT '类型特定字段（评级/目标价/管理层表态等）',
    created_at DATETIME DEFAULT NOW(),
    INDEX idx_sa_extracted_text_id (extracted_text_id),
    INDEX idx_sa_doc_type (doc_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='族2：深度FOE分析摘要（研报/策略/专题新闻/路演纪要）';

-- ── 族3：非正式内容（social_post/chat_record）──────────────────────────────
CREATE TABLE IF NOT EXISTS summary_informal (
    id INT AUTO_INCREMENT PRIMARY KEY,
    extracted_text_id INT NOT NULL,
    doc_type VARCHAR(30) NOT NULL,
    summary TEXT NOT NULL COMMENT '一句话核心摘要（50字以内）',
    speaker TEXT COMMENT '发帖人/发言人',
    speaker_type VARCHAR(20) DEFAULT NULL COMMENT 'kol/institution/retail/unknown',
    key_claims JSON COMMENT '需验证的事实声称',
    opinions JSON COMMENT '观点列表（含情绪标注）',
    sentiment VARCHAR(10) DEFAULT NULL COMMENT 'bullish/bearish/neutral',
    created_at DATETIME DEFAULT NOW(),
    INDEX idx_si_extracted_text_id (extracted_text_id),
    INDEX idx_si_doc_type (doc_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='族3：非正式内容摘要（社媒帖子/聊天记录）';

-- ── 族4：轻量摘要（flash_news/market_commentary/digest_news拆条后/other）──
CREATE TABLE IF NOT EXISTS summary_brief (
    id INT AUTO_INCREMENT PRIMARY KEY,
    extracted_text_id INT NOT NULL,
    doc_type VARCHAR(30) NOT NULL,
    summary TEXT NOT NULL COMMENT '一句话核心摘要（50字以内）',
    event_what TEXT COMMENT '什么事',
    event_who TEXT COMMENT '涉及谁',
    impact_target TEXT COMMENT '影响谁/什么板块',
    sentiment VARCHAR(10) DEFAULT NULL COMMENT 'bullish/bearish/neutral',
    created_at DATETIME DEFAULT NOW(),
    INDEX idx_sb_extracted_text_id (extracted_text_id),
    INDEX idx_sb_doc_type (doc_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='族4：轻量摘要（快讯/市场评论/拼盘拆条/其他）';

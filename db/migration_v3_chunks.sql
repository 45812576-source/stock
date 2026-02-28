-- migration_v3_chunks.sql
-- 向量检索基础设施：text_chunks + KG关联表

-- text_chunks: 向量检索的原子单位（写本地MySQL）
CREATE TABLE IF NOT EXISTS text_chunks (
    id INT PRIMARY KEY AUTO_INCREMENT,
    extracted_text_id INT NOT NULL,
    chunk_index INT NOT NULL DEFAULT 0,
    chunk_text TEXT NOT NULL,
    char_start INT,
    char_end INT,
    doc_type VARCHAR(50),
    file_type VARCHAR(20),
    publish_time DATETIME,
    source_doc_title VARCHAR(500),
    metadata_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_et_idx (extracted_text_id, chunk_index),
    INDEX idx_et_id (extracted_text_id),
    INDEX idx_publish (publish_time),
    INDEX idx_doc_type (doc_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- kg_triple_chunks: KG关系→chunk原文佐证
CREATE TABLE IF NOT EXISTS kg_triple_chunks (
    relationship_id INT NOT NULL,
    chunk_id INT NOT NULL,
    confidence FLOAT DEFAULT 0.5,
    PRIMARY KEY (relationship_id, chunk_id),
    INDEX idx_chunk (chunk_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- chunk_entities: chunk→KG实体反向索引
CREATE TABLE IF NOT EXISTS chunk_entities (
    chunk_id INT NOT NULL,
    entity_id INT NOT NULL,
    mention_type ENUM('subject','object','mentioned') DEFAULT 'mentioned',
    PRIMARY KEY (chunk_id, entity_id),
    INDEX idx_entity (entity_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

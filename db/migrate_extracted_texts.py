"""迁移脚本：在云端创建 extracted_texts 和 content_summaries 表，在本地创建 content_summaries 表"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import _get_cloud_conn, _get_conn

CLOUD_DDL = [
    """
    CREATE TABLE IF NOT EXISTS extracted_texts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source VARCHAR(50) NOT NULL,
        source_format VARCHAR(20) NOT NULL,
        publish_time DATETIME,
        extract_time DATETIME DEFAULT NOW(),
        full_text MEDIUMTEXT NOT NULL,
        source_doc_id INT,
        source_ref VARCHAR(100),
        extract_quality ENUM('pending','pass','fail') DEFAULT 'pending',
        summary_status ENUM('pending','done','skipped') DEFAULT 'pending',
        kg_status ENUM('pending','done','failed') DEFAULT 'pending',
        INDEX idx_et_source (source),
        INDEX idx_et_extract_quality (extract_quality),
        INDEX idx_et_summary_status (summary_status),
        INDEX idx_et_kg_status (kg_status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS content_summaries (
        id INT AUTO_INCREMENT PRIMARY KEY,
        extracted_text_id INT NOT NULL,
        summary TEXT NOT NULL,
        fact_summary TEXT,
        opinion_summary TEXT,
        evidence_assessment TEXT,
        info_gaps TEXT,
        created_at DATETIME DEFAULT NOW(),
        INDEX idx_cs_extracted_text_id (extracted_text_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]

LOCAL_DDL = [
    """
    CREATE TABLE IF NOT EXISTS content_summaries (
        id INT AUTO_INCREMENT PRIMARY KEY,
        extracted_text_id INT NOT NULL,
        summary TEXT NOT NULL,
        fact_summary TEXT,
        opinion_summary TEXT,
        evidence_assessment TEXT,
        info_gaps TEXT,
        created_at DATETIME DEFAULT NOW(),
        INDEX idx_cs_extracted_text_id (extracted_text_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


def run():
    print("=== 云端建表 ===")
    cloud = _get_cloud_conn()
    try:
        with cloud.cursor() as cur:
            for ddl in CLOUD_DDL:
                cur.execute(ddl)
                print(f"  OK: {ddl.strip().splitlines()[1].strip()}")
        cloud.commit()
    finally:
        cloud.close()

    print("\n=== 本地建表 ===")
    local = _get_conn()
    try:
        with local.cursor() as cur:
            for ddl in LOCAL_DDL:
                cur.execute(ddl)
                print(f"  OK: {ddl.strip().splitlines()[1].strip()}")
        local.commit()
    finally:
        local.close()

    print("\n迁移完成。")


if __name__ == "__main__":
    run()

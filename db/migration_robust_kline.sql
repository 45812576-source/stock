-- Robust Kline 两张表（本地库）
-- 执行：在本地 MySQL (127.0.0.1:3306, DB: stock_analysis) 运行

CREATE TABLE IF NOT EXISTS robust_kline_mentions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    extracted_text_id INT NOT NULL,
    source_title VARCHAR(500),
    publish_date DATE,
    stock_name VARCHAR(50) NOT NULL,
    stock_code VARCHAR(20),
    industry VARCHAR(100),
    theme VARCHAR(200),
    highlight TEXT COMMENT 'RAG检索填充的投资亮点',
    scan_date DATE NOT NULL,
    created_at DATETIME DEFAULT NOW(),
    INDEX idx_rkm_scan_date (scan_date),
    INDEX idx_rkm_stock_code (stock_code),
    INDEX idx_rkm_stock_name (stock_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS robust_kline_candidates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(50),
    industry VARCHAR(100),
    match_type TINYINT NOT NULL COMMENT '1=连续3月阳线 2=4月内3月阳线',
    yang_months VARCHAR(200) COMMENT '阳线月份列表',
    gain_pct FLOAT COMMENT '7个月累计涨幅%',
    latest_price FLOAT,
    mention_count INT DEFAULT 1 COMMENT '近期被提及次数',
    highlight TEXT COMMENT 'RAG检索填充的投资亮点',
    scan_date DATE NOT NULL,
    created_at DATETIME DEFAULT NOW(),
    INDEX idx_rkc_scan_date (scan_date),
    INDEX idx_rkc_stock_code (stock_code),
    UNIQUE KEY uk_rkc (stock_code, scan_date, match_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Migration V8: 阶段分析自选时间 + 自动复盘
-- 2026-06-23

-- chart_analysis 新增分析范围字段
ALTER TABLE chart_analysis
  ADD COLUMN start_date_range DATE NULL AFTER analysis_date,
  ADD COLUMN end_date_range DATE NULL AFTER start_date_range;

-- 复盘记录表
CREATE TABLE IF NOT EXISTS chart_analysis_reviews (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_code VARCHAR(20) NOT NULL,
    current_analysis_id INT NOT NULL,
    previous_analysis_id INT NOT NULL,
    previous_predictions_json TEXT,
    actual_stages_json TEXT,
    review_verdict VARCHAR(20) NOT NULL DEFAULT 'unknown',  -- hit/partial/miss
    review_report TEXT,
    lessons_learned TEXT,
    reviewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_car_stock (stock_code),
    INDEX idx_car_prev (previous_analysis_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

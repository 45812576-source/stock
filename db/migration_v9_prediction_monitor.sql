-- 预测监控量化系统 — v9 migration
-- 用途：将阶段预测的触发条件量化为具体数值，定时监控检测

CREATE TABLE IF NOT EXISTS prediction_monitors (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_code VARCHAR(20) NOT NULL,
    analysis_id INT NOT NULL COMMENT '关联 chart_analysis.id',
    situation_id INT NOT NULL COMMENT '预测目标情形(1-17)',
    scenario_name VARCHAR(100),
    probability FLOAT COMMENT '初始置信度',
    trigger_logic VARCHAR(50) DEFAULT 'priority_1_all' COMMENT 'priority_1_all / any_N_of_M',
    triggers_json TEXT NOT NULL COMMENT '量化触发条件数组JSON',
    status VARCHAR(20) DEFAULT 'active' COMMENT 'active/triggered/expired/superseded',
    satisfied_count INT DEFAULT 0 COMMENT '已满足条件数',
    total_count INT NOT NULL COMMENT '总条件数',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    triggered_at TIMESTAMP NULL COMMENT '触发确认时间',
    INDEX idx_pm_stock_status (stock_code, status),
    INDEX idx_pm_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS prediction_trigger_events (
    id INT PRIMARY KEY AUTO_INCREMENT,
    monitor_id INT NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    event_type VARCHAR(50) NOT NULL COMMENT 'condition_met/all_triggered/stage_confirmed/probability_update',
    event_detail TEXT COMMENT 'JSON: 哪个条件满足/新置信度等',
    trade_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_pte_stock_date (stock_code, trade_date),
    INDEX idx_pte_monitor (monitor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

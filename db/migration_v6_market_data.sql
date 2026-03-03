-- migration_v6_market_data.sql
-- 在本地库建立6张市场增量数据缓存表 + cyq_chips_cache
-- 所有 CREATE TABLE IF NOT EXISTS，幂等可重复执行

-- 1. 大股东/高管增减持明细
CREATE TABLE IF NOT EXISTS insider_trading (
    id BIGINT NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(50),
    trade_date DATE NOT NULL,
    person_name VARCHAR(50),
    person_role VARCHAR(100),
    direction VARCHAR(10),
    trade_shares DOUBLE,
    trade_price DOUBLE,
    trade_amount DOUBLE,
    hold_shares_after DOUBLE,
    relation VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_it_stock_date (stock_code, trade_date),
    INDEX idx_it_direction (direction),
    INDEX idx_it_person (person_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. 股东人数变动（筹码集中度）
CREATE TABLE IF NOT EXISTS shareholder_count (
    id BIGINT NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(50),
    end_date DATE NOT NULL,
    holder_count BIGINT,
    holder_count_change BIGINT,
    change_pct DOUBLE,
    avg_share_per_holder DOUBLE,
    avg_amount_per_holder DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_sc_stock_date (stock_code, end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. 机构持仓（基金季报）
CREATE TABLE IF NOT EXISTS institutional_holding (
    id BIGINT NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(50),
    report_date DATE NOT NULL,
    institution_type VARCHAR(50),
    hold_shares DOUBLE,
    hold_ratio DOUBLE,
    hold_change DOUBLE,
    hold_value DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_ih_stock_date (stock_code, report_date),
    INDEX idx_ih_type (institution_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. 历史估值（PE/PB/PS/市值）
CREATE TABLE IF NOT EXISTS valuation_history (
    id BIGINT NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    pe_ttm DOUBLE,
    pb_mrq DOUBLE,
    ps_ttm DOUBLE,
    dividend_yield DOUBLE,
    market_cap DOUBLE,
    circ_market_cap DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_vh_stock_date (stock_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. ETF成分股及权重
CREATE TABLE IF NOT EXISTS etf_constituent (
    id BIGINT NOT NULL,
    etf_code VARCHAR(20) NOT NULL,
    etf_name VARCHAR(100),
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(50),
    weight DOUBLE,
    shares DOUBLE,
    amount DOUBLE,
    report_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_ec_stock (stock_code),
    INDEX idx_ec_etf (etf_code),
    INDEX idx_ec_report_date (report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 6. 融资融券余额/余量
CREATE TABLE IF NOT EXISTS margin_trading (
    id BIGINT NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(50),
    trade_date DATE NOT NULL,
    margin_balance DOUBLE,
    margin_buy_amount DOUBLE,
    margin_repay_amount DOUBLE,
    short_balance DOUBLE,
    short_sell_volume BIGINT,
    short_repay_volume BIGINT,
    short_sell_amount DOUBLE,
    total_balance DOUBLE,
    exchange VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_mt_stock_date (stock_code, trade_date),
    INDEX idx_mt_exchange (exchange)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 7. 筹码分布缓存（按需从云端 stock_db 拉取后存本地）
CREATE TABLE IF NOT EXISTS cyq_chips_cache (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    price DECIMAL(10,3) NOT NULL,
    percent DECIMAL(10,4) NOT NULL,
    UNIQUE KEY uk_stock_date_price (stock_code, trade_date, price),
    INDEX idx_cyq_stock_date (stock_code, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

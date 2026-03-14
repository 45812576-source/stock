"""迁移脚本：在云端和本地创建宏观数据相关表（5张）"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import _get_cloud_conn, _get_conn

MACRO_DDL = [
    """
    CREATE TABLE IF NOT EXISTS macro_indicators (
        id INT AUTO_INCREMENT PRIMARY KEY,
        indicator_name VARCHAR(100) NOT NULL,
        indicator_date VARCHAR(20) NOT NULL,
        value DOUBLE,
        unit VARCHAR(50),
        source VARCHAR(100),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_indicator (indicator_name, indicator_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS margin_balance (
        id INT AUTO_INCREMENT PRIMARY KEY,
        trade_date VARCHAR(20) NOT NULL,
        margin_balance DOUBLE COMMENT '融资余额(元)',
        margin_buy DOUBLE COMMENT '融资买入额(元)',
        total_balance DOUBLE COMMENT '融资融券余额(元)',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_date (trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS market_valuation (
        id INT AUTO_INCREMENT PRIMARY KEY,
        trade_date VARCHAR(20) NOT NULL,
        pe_ttm_median DOUBLE COMMENT '全A中位数PE TTM',
        pe_ttm_avg DOUBLE COMMENT '全A平均PE TTM',
        pe_quantile_10y DOUBLE COMMENT 'PE中位数近10年分位数',
        pe_quantile_all DOUBLE COMMENT 'PE中位数历史分位数',
        total_market_cap DOUBLE COMMENT '总市值(亿)',
        market_pe DOUBLE COMMENT '市盈率(乐估)',
        sh_amount DOUBLE COMMENT '沪市成交额(亿)',
        sz_amount DOUBLE COMMENT '深市成交额(亿)',
        close_index DOUBLE COMMENT '收盘指数',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_date (trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS hsgt_holding (
        id INT AUTO_INCREMENT PRIMARY KEY,
        trade_date VARCHAR(20) NOT NULL,
        stock_code VARCHAR(20) NOT NULL,
        stock_name VARCHAR(100),
        close_price DOUBLE,
        holding_shares DOUBLE COMMENT '持股股数',
        holding_market_value DOUBLE COMMENT '持股市值',
        holding_ratio_float DOUBLE COMMENT '占流通股比(%)',
        holding_ratio_total DOUBLE COMMENT '占总股本比(%)',
        change_shares DOUBLE COMMENT '增持估计-股数',
        change_market_value DOUBLE COMMENT '增持估计-市值',
        sector VARCHAR(100),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_date_stock (trade_date, stock_code),
        INDEX idx_stock (stock_code),
        INDEX idx_date (trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS overseas_etf (
        id INT AUTO_INCREMENT PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        etf_name VARCHAR(100),
        trade_date VARCHAR(20) NOT NULL,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uk_symbol_date (symbol, trade_date),
        INDEX idx_date (trade_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


def run():
    print("=== 云端建表 ===")
    cloud = _get_cloud_conn()
    try:
        with cloud.cursor() as cur:
            for ddl in MACRO_DDL:
                cur.execute(ddl)
                table_line = [l.strip() for l in ddl.strip().splitlines() if l.strip()][1]
                print(f"  OK: {table_line}")
        cloud.commit()
    finally:
        cloud.close()

    print("\n=== 本地建表 ===")
    local = _get_conn()
    try:
        with local.cursor() as cur:
            for ddl in MACRO_DDL:
                cur.execute(ddl)
                table_line = [l.strip() for l in ddl.strip().splitlines() if l.strip()][1]
                print(f"  OK: {table_line}")
        local.commit()
    finally:
        local.close()

    print("\n迁移完成。")


if __name__ == "__main__":
    run()

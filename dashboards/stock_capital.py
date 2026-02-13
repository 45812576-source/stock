"""榜单7：个股资金/市值Top10"""
from utils.db_utils import execute_query


def generate_stock_capital(date_str):
    """生成个股资金净流入Top10"""
    return execute_query(
        """SELECT cf.stock_code, si.stock_name, cf.main_net_inflow,
                  cf.super_large_net, cf.large_net, si.market_cap,
                  sd.change_pct, sd.turnover_rate
           FROM capital_flow cf
           LEFT JOIN stock_info si ON cf.stock_code=si.stock_code
           LEFT JOIN stock_daily sd ON cf.stock_code=sd.stock_code AND cf.trade_date=sd.trade_date
           WHERE cf.trade_date=?
           ORDER BY cf.main_net_inflow DESC LIMIT 10""",
        [date_str],
    )

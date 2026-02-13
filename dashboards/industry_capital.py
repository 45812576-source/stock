"""榜单3：行业资金净流入Top10"""
from utils.db_utils import execute_query


def generate_industry_capital(date_str):
    """生成行业资金净流入榜单"""
    return execute_query(
        """SELECT industry_name, net_inflow, change_pct, leading_stock
           FROM industry_capital_flow
           WHERE trade_date=?
           ORDER BY net_inflow DESC LIMIT 10""",
        [date_str],
    )

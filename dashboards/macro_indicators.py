"""榜单8：宏观资金面指标监控"""
from utils.db_utils import execute_query


def generate_macro_indicators():
    """生成宏观资金面指标监控"""
    # 最新各指标值
    latest = execute_query(
        """SELECT indicator_name, value, unit, indicator_date
           FROM macro_indicators mi
           WHERE indicator_date = (
               SELECT MAX(indicator_date) FROM macro_indicators
               WHERE indicator_name=mi.indicator_name
           )
           ORDER BY indicator_name"""
    )

    # 北向资金近期趋势
    northbound = execute_query(
        """SELECT trade_date, total_net, sh_net, sz_net
           FROM northbound_flow
           ORDER BY trade_date DESC LIMIT 30"""
    )

    return {"indicators": latest, "northbound": northbound}

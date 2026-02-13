"""榜单4：财报超预期Top10"""
from utils.db_utils import execute_query


def generate_earnings_beat():
    """生成财报超预期榜单"""
    return execute_query(
        """SELECT fr.stock_code, si.stock_name, fr.report_period,
                  fr.revenue_yoy, fr.profit_yoy, fr.actual_vs_consensus, fr.eps
           FROM financial_reports fr
           LEFT JOIN stock_info si ON fr.stock_code=si.stock_code
           WHERE fr.beat_expectation=1
           ORDER BY fr.actual_vs_consensus DESC LIMIT 10"""
    )

"""榜单5：3月券商覆盖Top10"""
from utils.db_utils import execute_query


def generate_broker_coverage_3m(date_str):
    """生成3月券商覆盖榜单"""
    return execute_query(
        """SELECT stock_code, stock_name,
                  COUNT(*) as coverage_count,
                  COUNT(DISTINCT broker_name) as broker_count,
                  GROUP_CONCAT(DISTINCT broker_name) as brokers,
                  AVG(target_price) as avg_target_price
           FROM research_reports
           WHERE report_date >= date(?, '-3 months')
           GROUP BY stock_code
           ORDER BY coverage_count DESC LIMIT 10""",
        [date_str],
    )

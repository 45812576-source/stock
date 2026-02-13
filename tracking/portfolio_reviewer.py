"""持仓复盘"""
import json
from datetime import datetime
from utils.db_utils import execute_query, execute_insert


def create_portfolio_review():
    """创建持仓复盘记录"""
    # 获取当前持仓快照
    positions = execute_query(
        "SELECT * FROM holding_positions WHERE status='open'"
    )
    snapshot = [dict(p) for p in positions]

    review_id = execute_insert(
        """INSERT INTO portfolio_reviews (review_date, holdings_snapshot_json)
           VALUES (?, ?)""",
        [datetime.now().strftime("%Y-%m-%d"),
         json.dumps(snapshot, ensure_ascii=False, default=str)],
    )
    return review_id


def get_review_history(limit=10):
    """获取复盘历史"""
    return execute_query(
        "SELECT * FROM portfolio_reviews ORDER BY review_date DESC LIMIT ?",
        [limit],
    )

"""跟踪列表管理 — 完整CRUD + 持仓管理 + PnL计算"""
import json
from datetime import datetime
from utils.db_utils import execute_query, execute_insert


# ========== Watchlist CRUD ==========

def add_to_watchlist(stock_code, stock_name=None, watch_type="interested",
                     related_tags=None, notes=None):
    """添加到跟踪列表"""
    return execute_insert(
        """INSERT OR REPLACE INTO watchlist
           (stock_code, stock_name, watch_type, related_tags, notes, updated_at)
           VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
        [stock_code, stock_name, watch_type,
         json.dumps(related_tags, ensure_ascii=False) if related_tags else None,
         notes],
    )


def remove_from_watchlist(stock_code):
    """从跟踪列表移除"""
    execute_insert("DELETE FROM watchlist WHERE stock_code=?", [stock_code])


def update_watch_type(stock_code, watch_type):
    """更新标记类型"""
    execute_insert(
        "UPDATE watchlist SET watch_type=?, updated_at=CURRENT_TIMESTAMP WHERE stock_code=?",
        [watch_type, stock_code],
    )


def get_watchlist(watch_type=None):
    """获取跟踪列表"""
    sql = "SELECT * FROM watchlist"
    params = []
    if watch_type:
        sql += " WHERE watch_type=?"
        params.append(watch_type)
    sql += " ORDER BY updated_at DESC"
    return execute_query(sql, params)


def get_stock_today_news(stock_code, date_str=None):
    """获取个股今日关联信息"""
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    return execute_query(
        """SELECT ci.id, ci.summary, ci.sentiment, ci.importance, ci.event_type,
                  ci.tags_json, ci.impact_analysis, ci.cleaned_at
           FROM item_companies ic JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
           WHERE ic.stock_code=? AND date(ci.cleaned_at)=?
           ORDER BY ci.importance DESC""",
        [stock_code, date_str],
    )


# ========== 标签跟踪 ==========

def add_tag_watch(tag_name, tag_type="theme", watch_type="interested",
                  related_stocks=None):
    """添加标签跟踪"""
    return execute_insert(
        """INSERT OR REPLACE INTO watchlist_tags
           (tag_name, tag_type, watch_type, related_stock_codes_json)
           VALUES (?, ?, ?, ?)""",
        [tag_name, tag_type, watch_type,
         json.dumps(related_stocks, ensure_ascii=False) if related_stocks else None],
    )


def get_watched_tags(watch_type=None):
    """获取跟踪的标签"""
    sql = "SELECT * FROM watchlist_tags"
    params = []
    if watch_type:
        sql += " WHERE watch_type=?"
        params.append(watch_type)
    sql += " ORDER BY added_at DESC"
    return execute_query(sql, params)


def get_tag_today_news(tag_name, date_str=None):
    """获取标签今日关联信息"""
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")
    return execute_query(
        """SELECT ci.id, ci.summary, ci.sentiment, ci.importance, ci.tags_json, ci.cleaned_at
           FROM cleaned_items ci
           WHERE ci.tags_json LIKE ? AND date(ci.cleaned_at)=?
           ORDER BY ci.importance DESC""",
        [f"%{tag_name}%", date_str],
    )


# ========== 持仓管理 ==========

def open_position(stock_code, stock_name, buy_date, buy_price, quantity, notes=None):
    """建仓"""
    pos_id = execute_insert(
        """INSERT INTO holding_positions
           (stock_code, stock_name, buy_date, buy_price, quantity, status, notes)
           VALUES (?, ?, ?, ?, ?, 'open', ?)""",
        [stock_code, stock_name, buy_date, buy_price, quantity, notes],
    )
    # 自动将股票标记为holding
    update_watch_type(stock_code, "holding")
    return pos_id


def close_position(position_id, sell_date, sell_price):
    """平仓"""
    pos = execute_query("SELECT * FROM holding_positions WHERE id=?", [position_id])
    if not pos:
        return None
    p = pos[0]
    pnl = (sell_price - p["buy_price"]) * p["quantity"]
    execute_insert(
        """UPDATE holding_positions SET status='closed', sell_date=?, sell_price=?, pnl=?
           WHERE id=?""",
        [sell_date, sell_price, pnl, position_id],
    )
    # 检查是否还有该股票的其他持仓
    remaining = execute_query(
        "SELECT id FROM holding_positions WHERE stock_code=? AND status='open'",
        [p["stock_code"]],
    )
    if not remaining:
        update_watch_type(p["stock_code"], "interested")
    return pnl


def get_open_positions():
    """获取所有未平仓持仓"""
    return execute_query(
        "SELECT * FROM holding_positions WHERE status='open' ORDER BY buy_date DESC"
    )


def get_closed_positions(limit=50):
    """获取已平仓记录"""
    return execute_query(
        "SELECT * FROM holding_positions WHERE status='closed' ORDER BY sell_date DESC LIMIT ?",
        [limit],
    )


def get_position_summary():
    """获取持仓汇总"""
    positions = get_open_positions()
    summary = {
        "total_positions": len(positions),
        "total_cost": 0,
        "stocks": [],
    }
    for p in positions:
        cost = p["buy_price"] * p["quantity"]
        summary["total_cost"] += cost

        # 获取最新价格
        latest = execute_query(
            "SELECT close FROM stock_daily WHERE stock_code=? ORDER BY trade_date DESC LIMIT 1",
            [p["stock_code"]],
        )
        current_price = latest[0]["close"] if latest else p["buy_price"]
        market_value = current_price * p["quantity"]
        pnl = market_value - cost
        pnl_pct = (pnl / cost * 100) if cost > 0 else 0

        summary["stocks"].append({
            "position_id": p["id"],
            "stock_code": p["stock_code"],
            "stock_name": p["stock_name"],
            "buy_date": p["buy_date"],
            "buy_price": p["buy_price"],
            "quantity": p["quantity"],
            "current_price": current_price,
            "cost": cost,
            "market_value": market_value,
            "pnl": round(pnl, 2),
            "pnl_pct": round(pnl_pct, 2),
            "holding_days": (datetime.now() - datetime.strptime(p["buy_date"], "%Y-%m-%d")).days,
        })

    summary["total_market_value"] = sum(s["market_value"] for s in summary["stocks"])
    summary["total_pnl"] = round(sum(s["pnl"] for s in summary["stocks"]), 2)
    return summary

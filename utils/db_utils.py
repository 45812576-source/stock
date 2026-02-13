"""数据库工具函数"""
import sqlite3
from contextlib import contextmanager
from config import DB_PATH


@contextmanager
def get_db():
    """获取数据库连接的上下文管理器"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_query(sql, params=None):
    """执行查询并返回结果列表"""
    with get_db() as conn:
        cursor = conn.execute(sql, params or [])
        return [dict(row) for row in cursor.fetchall()]


def execute_insert(sql, params=None):
    """执行插入并返回lastrowid"""
    with get_db() as conn:
        cursor = conn.execute(sql, params or [])
        return cursor.lastrowid


def execute_many(sql, params_list):
    """批量执行"""
    with get_db() as conn:
        conn.executemany(sql, params_list)


def table_row_count(table_name):
    """获取表行数"""
    rows = execute_query(f"SELECT COUNT(*) as cnt FROM {table_name}")
    return rows[0]["cnt"] if rows else 0

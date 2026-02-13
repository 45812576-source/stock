"""系统配置 KV 读写 — 优先 DB，回退环境变量"""
from utils.db_utils import execute_query, execute_insert


def get_config(key, default=""):
    """读取配置值：DB > 环境变量 > default"""
    rows = execute_query(
        "SELECT value FROM system_config WHERE key=?", [key]
    )
    if rows and rows[0]["value"]:
        return rows[0]["value"]
    return default


def set_config(key, value):
    """写入配置值"""
    execute_insert(
        """INSERT INTO system_config (key, value, updated_at)
           VALUES (?, ?, datetime('now'))
           ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
        [key, value],
    )


def get_all_config():
    """获取所有配置"""
    rows = execute_query("SELECT key, value, updated_at FROM system_config ORDER BY key")
    return {r["key"]: r["value"] for r in rows}

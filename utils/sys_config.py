"""系统配置 KV 读写 — 优先 DB，回退环境变量"""
from utils.db_utils import execute_query, execute_insert


def get_config(key, default=""):
    """读取配置值：DB > 环境变量 > default"""
    rows = execute_query(
        "SELECT value FROM system_config WHERE config_key=%s", [key]
    )
    if rows and rows[0]["value"]:
        return rows[0]["value"]
    return default


def set_config(key, value):
    """写入配置值"""
    execute_insert(
        """INSERT INTO system_config (config_key, value, updated_at)
           VALUES (%s, %s, NOW())
           ON DUPLICATE KEY UPDATE value=VALUES(value), updated_at=NOW()""",
        [key, value],
    )


def get_all_config():
    """获取所有配置"""
    rows = execute_query("SELECT config_key, value, updated_at FROM system_config ORDER BY config_key")
    return {r["config_key"]: r["value"] for r in rows}

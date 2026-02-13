"""初始化数据库"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DB_PATH, SCHEMA_PATH
import sqlite3


def init_database():
    """读取schema.sql并执行建表"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(schema_sql)

    # 兼容已有库：追加新列（忽略已存在的列）
    try:
        conn.execute("ALTER TABLE kg_entities ADD COLUMN investment_logic TEXT")
    except Exception:
        pass  # 列已存在

    # system_config 表（兼容已有库）
    conn.execute("""CREATE TABLE IF NOT EXISTS system_config (
        key TEXT PRIMARY KEY, value TEXT,
        updated_at TEXT DEFAULT (datetime('now'))
    )""")

    # 插入默认数据源
    sources = [
        ("jasper", "jasper", "https://api.jasper.ai", 0, 0),
        ("djyanbao", "djyanbao", "https://www.djyanbao.com", 0, 150),
        ("fxbaogao", "fxbaogao", "https://www.fxbaogao.com", 0, 150),
        ("akshare", "akshare", None, 0, 0),
        ("iwencai", "iwencai", "https://www.iwencai.com", 0, 0),
    ]
    for name, stype, url, dlimit, mlimit in sources:
        conn.execute(
            """INSERT OR IGNORE INTO data_sources (name, source_type, base_url, daily_limit, monthly_limit)
               VALUES (?, ?, ?, ?, ?)""",
            (name, stype, url, dlimit, mlimit),
        )
    conn.commit()
    conn.close()
    print(f"数据库初始化完成: {DB_PATH}")


if __name__ == "__main__":
    init_database()

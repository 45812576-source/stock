"""初始化 MySQL 数据库"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pymysql
from config import SCHEMA_PATH, MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB


def init_database():
    """读取 schema_mysql.sql 并逐条执行建表"""
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")

    conn = pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DB, charset="utf8mb4",
    )
    cur = conn.cursor()

    # 按分号拆分并逐条执行（跳过空语句和纯注释）
    for stmt in schema_sql.split(";"):
        # 去掉注释行，只保留实际 SQL
        lines = [l for l in stmt.splitlines() if not l.strip().startswith("--")]
        clean = "\n".join(lines).strip()
        if not clean:
            continue
        cur.execute(clean)

    # 兼容已有库：追加新列（忽略已存在的列）
    try:
        cur.execute("ALTER TABLE kg_entities ADD COLUMN investment_logic TEXT")
    except Exception:
        pass

    try:
        cur.execute("ALTER TABLE tag_groups ADD COLUMN extra_json TEXT")
    except Exception:
        pass

    # 研究层新增结构化 JSON 列
    for col in ("macro_json", "industry_json", "news_parsed_json", "theme_heat_json", "logic_synthesis_json"):
        try:
            cur.execute(f"ALTER TABLE tag_group_research ADD COLUMN {col} TEXT")
        except Exception:
            pass

    # 研究层新增状态 / 保存 / 组合统计列
    for col_def in (
        "status VARCHAR(20) DEFAULT 'draft'",
        "saved_at TIMESTAMP NULL",
        "portfolio_stats_json TEXT",
    ):
        try:
            col_name = col_def.split()[0]
            cur.execute(f"ALTER TABLE tag_group_research ADD COLUMN {col_def}")
        except Exception:
            pass

    # investment_strategies + strategy_stocks 兼容（schema 已含 CREATE IF NOT EXISTS）
    # 无需额外 ALTER，表不存在时由 schema_mysql.sql 创建

    # 插入默认数据源
    sources = [
        ("jasper", "jasper", "https://api.jasper.ai", 0, 0),
        ("djyanbao", "djyanbao", "https://www.djyanbao.com", 0, 150),
        ("fxbaogao", "fxbaogao", "https://www.fxbaogao.com", 0, 150),
        ("akshare", "akshare", None, 0, 0),
        ("iwencai", "iwencai", "https://www.iwencai.com", 0, 0),
        ("source_doc", "source_doc", None, 0, 0),
        ("zsxq", "zsxq", "https://wx.zsxq.com", 0, 0),
    ]
    for name, stype, url, dlimit, mlimit in sources:
        cur.execute(
            """INSERT IGNORE INTO data_sources
               (name, source_type, base_url, daily_limit, monthly_limit)
               VALUES (%s, %s, %s, %s, %s)""",
            (name, stype, url, dlimit, mlimit),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"MySQL 数据库初始化完成: {MYSQL_DB}@{MYSQL_HOST}:{MYSQL_PORT}")


if __name__ == "__main__":
    init_database()

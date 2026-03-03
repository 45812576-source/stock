#!/usr/bin/env python3
"""执行 migration_v6_market_data.sql — 在本地 MySQL 建立市场增量数据缓存表。

Usage:
    cd /Users/liaoxia/stock-analysis-system
    python3 scripts/migrate_v6.py
"""
import os
import sys

# 确保能 import 项目包
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import _get_conn

SQL_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "db", "migration_v6_market_data.sql",
)


def run():
    sql_text = open(SQL_FILE, encoding="utf-8").read()
    # 按分号拆分，过滤注释行和空语句
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            ok = 0
            for stmt in statements:
                # 跳过纯注释块
                lines = [l for l in stmt.splitlines() if not l.strip().startswith("--")]
                clean = "\n".join(lines).strip()
                if not clean:
                    continue
                cur.execute(clean)
                conn.commit()
                ok += 1
                # 提取表名用于提示
                words = clean.split()
                if len(words) >= 6 and words[2].upper() == "TABLE":
                    tbl = words[5].rstrip("(")
                    print(f"  ✓ {tbl}")
    finally:
        conn.close()

    print(f"\n完成：执行 {ok} 条 DDL 语句。")
    print("请用以下命令验证：")
    print('  python3 -c "from utils.db_utils import execute_query; '
          'print([r[\"Tables_in_stock_analysis\"] for r in execute_query(\"SHOW TABLES\")])"')


if __name__ == "__main__":
    run()

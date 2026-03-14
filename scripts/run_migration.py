"""执行摘要族表迁移 + 全量重跑 Pipeline A"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from config import (
    CLOUD_MYSQL_HOST, CLOUD_MYSQL_PORT, CLOUD_MYSQL_USER,
    CLOUD_MYSQL_PASSWORD, CLOUD_MYSQL_DB,
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB,
)

SQL_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        'db', 'migrate_summary_tables.sql')

def run_migration(conn, label):
    sql = open(SQL_PATH).read()
    lines = [l for l in sql.splitlines() if not l.strip().startswith('--')]
    statements = [s.strip() for s in '\n'.join(lines).split(';') if s.strip()]
    print(f"\n{label}: 共 {len(statements)} 条语句")
    ok = err = 0
    with conn.cursor() as cur:
        for stmt in statements:
            try:
                cur.execute(stmt)
                print(f"  OK: {stmt[:90].replace(chr(10), ' ')}")
                ok += 1
            except Exception as e:
                print(f"  ERR({e}): {stmt[:60].replace(chr(10), ' ')}")
                err += 1
    conn.commit()
    print(f"{label}: OK={ok} ERR={err}")

def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "migrate"

    if mode in ("migrate", "all"):
        # 云端迁移
        print("连接云端...")
        cloud = pymysql.connect(
            host=CLOUD_MYSQL_HOST, port=CLOUD_MYSQL_PORT,
            user=CLOUD_MYSQL_USER, password=CLOUD_MYSQL_PASSWORD,
            database=CLOUD_MYSQL_DB, charset='utf8mb4', connect_timeout=15,
        )
        run_migration(cloud, "云端")
        cloud.close()

        # 本地迁移
        print("\n连接本地...")
        local = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASSWORD,
            database=MYSQL_DB, charset='utf8mb4', connect_timeout=10,
        )
        run_migration(local, "本地")
        local.close()
        print("\n迁移完成！")

    if mode in ("rerun", "all"):
        batch = int(sys.argv[2]) if len(sys.argv) > 2 else 200
        print(f"\n开始重跑 Pipeline A，batch_size={batch}...")
        from cleaning.unified_pipeline import process_pending

        def on_progress(done, total, et_id, result):
            if et_id:
                print(f"  [{done}/{total}] id={et_id} summary={result.get('summary_id') if result else None}")

        r = process_pending(batch_size=batch, max_workers=3, rerun_a=True, on_progress=on_progress)
        print(f"\n重跑完成: processed={r['processed']} ok={r['ok']} fail={r['fail']} summaries={r['summaries']}")

if __name__ == "__main__":
    main()

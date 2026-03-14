"""一次性数据迁移脚本 — 建立双库架构

Step 1: 合并云端 stock_db.stock_analysis (864行) → 云端 stock_analysis.source_documents
Step 2: 同步本地 raw_items (7288行) + data_sources (9行) → 云端 stock_analysis
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import pymysql
import pymysql.cursors


def get_local():
    return pymysql.connect(
        host="127.0.0.1", port=3306,
        user="stock_user", password="stock_pass",
        database="stock_analysis", charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

def get_cloud():
    return pymysql.connect(
        host="8.134.184.254", port=3301,
        user="root", password="ZRMwE#1!z!(WLPk4LtyRg2CK#*usUI",
        database="stock_analysis", charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

def get_cloud_stock_db():
    return pymysql.connect(
        host="8.134.184.254", port=3301,
        user="root", password="ZRMwE#1!z!(WLPk4LtyRg2CK#*usUI",
        database="stock_db", charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


# ==================== Step 1 ====================
def step1_merge_stock_analysis():
    """合并云端 stock_db.stock_analysis 的 725 条独有记录到云端 stock_analysis.source_documents"""
    print("=" * 60)
    print("Step 1: 合并云端 stock_db.stock_analysis → source_documents")
    print("=" * 60)

    cloud_sd = get_cloud_stock_db()
    cloud = get_cloud()
    local = get_local()

    try:
        with cloud_sd.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM stock_analysis")
            total = cur.fetchone()["cnt"]
            print(f"  云端 stock_db.stock_analysis 共 {total} 行")

            cur.execute("SELECT * FROM stock_analysis")
            rows = cur.fetchall()

        # 先获取云端 source_documents 已有的 id
        with cloud.cursor() as cur:
            cur.execute("SELECT id FROM source_documents")
            existing_ids = {r["id"] for r in cur.fetchall()}
            print(f"  云端 source_documents 已有 {len(existing_ids)} 行")

        # 也获取本地已有的 id
        with local.cursor() as cur:
            cur.execute("SELECT id FROM source_documents")
            local_existing_ids = {r["id"] for r in cur.fetchall()}
            print(f"  本地 source_documents 已有 {len(local_existing_ids)} 行")

        inserted_cloud = 0
        inserted_local = 0
        skipped = 0

        for row in rows:
            doc_id = row["id"]

            # 映射 status → extract_status
            status_val = row.get("status", 0)
            extract_status = "done" if status_val == 1 else "pending"

            # 准备字段
            doc_type = row.get("doc_type", "news")
            file_type = row.get("file_type", "txt")
            title = row.get("title", "")
            author = row.get("author", "")
            publish_date = row.get("publish_date")
            source = row.get("source")
            oss_url = row.get("oss_url")
            text_content = row.get("text_content", "")
            created_at = row.get("created_at")
            updated_at = row.get("updated_at")

            insert_sql = """INSERT IGNORE INTO source_documents
                (id, doc_type, file_type, title, author, publish_date,
                 source, oss_url, text_content, extract_status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            params = [doc_id, doc_type, file_type, title, author, publish_date,
                      source, oss_url, text_content, extract_status, created_at, updated_at]

            # 写入云端 stock_analysis.source_documents
            if doc_id not in existing_ids:
                with cloud.cursor() as cur:
                    cur.execute(insert_sql, params)
                inserted_cloud += 1

            # 也写入本地
            if doc_id not in local_existing_ids:
                with local.cursor() as cur:
                    cur.execute(insert_sql, params)
                inserted_local += 1

            if doc_id in existing_ids and doc_id in local_existing_ids:
                skipped += 1

        cloud.commit()
        local.commit()

        print(f"  完成: 云端新增 {inserted_cloud}, 本地新增 {inserted_local}, 已存在跳过 {skipped}")

    finally:
        cloud_sd.close()
        cloud.close()
        local.close()


# ==================== Step 2 ====================
def step2_sync_to_cloud():
    """同步本地 raw_items + data_sources 到云端"""
    print("\n" + "=" * 60)
    print("Step 2: 同步本地 raw_items + data_sources → 云端")
    print("=" * 60)

    local = get_local()
    cloud = get_cloud()

    try:
        # 2a. data_sources
        print("\n  2a. 同步 data_sources...")
        with local.cursor() as cur:
            cur.execute("SELECT * FROM data_sources")
            ds_rows = cur.fetchall()
            print(f"    本地 data_sources 共 {len(ds_rows)} 行")

        ds_inserted = 0
        for row in ds_rows:
            with cloud.cursor() as cur:
                cols = list(row.keys())
                placeholders = ", ".join(["%s"] * len(cols))
                col_names = ", ".join(cols)
                cur.execute(
                    f"INSERT IGNORE INTO data_sources ({col_names}) VALUES ({placeholders})",
                    list(row.values())
                )
                if cur.rowcount > 0:
                    ds_inserted += 1
        cloud.commit()
        print(f"    云端新增 {ds_inserted} 行")

        # 2b. raw_items
        print("\n  2b. 同步 raw_items...")
        with local.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM raw_items")
            total = cur.fetchone()["cnt"]
            print(f"    本地 raw_items 共 {total} 行")

        with cloud.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM raw_items")
            cloud_total = cur.fetchone()["cnt"]
            print(f"    云端 raw_items 已有 {cloud_total} 行")

        # 分批同步
        batch_size = 500
        offset = 0
        ri_inserted = 0

        while offset < total:
            with local.cursor() as cur:
                cur.execute(f"SELECT * FROM raw_items LIMIT {batch_size} OFFSET {offset}")
                batch = cur.fetchall()

            if not batch:
                break

            for row in batch:
                with cloud.cursor() as cur:
                    cols = list(row.keys())
                    placeholders = ", ".join(["%s"] * len(cols))
                    col_names = ", ".join(cols)
                    cur.execute(
                        f"INSERT IGNORE INTO raw_items ({col_names}) VALUES ({placeholders})",
                        list(row.values())
                    )
                    if cur.rowcount > 0:
                        ri_inserted += 1

            cloud.commit()
            offset += batch_size
            print(f"    已处理 {min(offset, total)}/{total} 行...")

        print(f"    云端新增 {ri_inserted} 行")

        # 2c. 同步本地 source_documents 到云端（全量）
        print("\n  2c. 同步 source_documents → 云端...")
        with local.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM source_documents")
            sd_total = cur.fetchone()["cnt"]
            print(f"    本地 source_documents 共 {sd_total} 行")

        with cloud.cursor() as cur:
            cur.execute("SELECT COUNT(*) as cnt FROM source_documents")
            cloud_sd_total = cur.fetchone()["cnt"]
            print(f"    云端 source_documents 已有 {cloud_sd_total} 行")

        offset = 0
        sd_inserted = 0

        while offset < sd_total:
            with local.cursor() as cur:
                cur.execute(f"SELECT * FROM source_documents LIMIT {batch_size} OFFSET {offset}")
                batch = cur.fetchall()

            if not batch:
                break

            for row in batch:
                with cloud.cursor() as cur:
                    cols = list(row.keys())
                    placeholders = ", ".join(["%s"] * len(cols))
                    col_names = ", ".join(cols)
                    cur.execute(
                        f"INSERT IGNORE INTO source_documents ({col_names}) VALUES ({placeholders})",
                        list(row.values())
                    )
                    if cur.rowcount > 0:
                        sd_inserted += 1

            cloud.commit()
            offset += batch_size
            print(f"    已处理 {min(offset, sd_total)}/{sd_total} 行...")

        print(f"    云端新增 {sd_inserted} 行")

    finally:
        local.close()
        cloud.close()


# ==================== 验证 ====================
def verify():
    """验证数据行数"""
    print("\n" + "=" * 60)
    print("验证数据行数")
    print("=" * 60)

    local = get_local()
    cloud = get_cloud()

    try:
        for label, conn in [("本地", local), ("云端", cloud)]:
            print(f"\n  {label}:")
            for table in ["source_documents", "raw_items", "data_sources"]:
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT COUNT(*) as cnt FROM {table}")
                        cnt = cur.fetchone()["cnt"]
                        print(f"    {table}: {cnt} 行")
                except Exception as e:
                    print(f"    {table}: 错误 - {e}")
    finally:
        local.close()
        cloud.close()


if __name__ == "__main__":
    step1_merge_stock_analysis()
    step2_sync_to_cloud()
    verify()
    print("\n迁移完成！")

"""全量股票基础数据采集 — 行业映射 + 公司概况 + 主营构成

用法:
    python ingestion/stock_fundamentals.py --task industry   # 行业板块→个股映射
    python ingestion/stock_fundamentals.py --task profile    # 公司概况(主营业务)
    python ingestion/stock_fundamentals.py --task composition # 主营构成(收入/成本)
    python ingestion/stock_fundamentals.py --task all        # 全部
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import time
import argparse
import ssl
import pymysql
from config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

# 绕过 LibreSSL 的 SSL 问题
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_conn():
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PASSWORD, database=MYSQL_DB,
        charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor,
    )


# ==================== Task 1: 行业板块映射 ====================

def fetch_industry_mapping():
    """从东方财富获取全部行业板块及其成分股"""
    import akshare as ak
    import requests
    # patch requests to skip SSL verification
    old_get = requests.Session.get
    def patched_get(self, *a, **kw):
        kw.setdefault("verify", False)
        return old_get(self, *a, **kw)
    requests.Session.get = patched_get

    logger.info("获取行业板块列表...")
    boards = ak.stock_board_industry_name_em()
    logger.info(f"共 {len(boards)} 个行业板块")

    conn = get_conn()
    cur = conn.cursor()
    total = 0

    for idx, row in boards.iterrows():
        name = row["板块名称"]
        code = row.get("板块代码", "")

        # 跳过 Ⅲ 级板块（和 Ⅱ 级重复）
        if "Ⅲ" in name:
            continue

        try:
            cons = ak.stock_board_industry_cons_em(symbol=name)
            if cons is None or cons.empty:
                continue

            for _, s in cons.iterrows():
                stock_code = str(s.get("代码", "")).strip()
                stock_name = str(s.get("名称", "")).strip()
                if not stock_code:
                    continue

                cur.execute(
                    """REPLACE INTO industry_stock_mapping
                       (industry_name, industry_code, stock_code, stock_name)
                       VALUES (%s, %s, %s, %s)""",
                    [name, code, stock_code, stock_name],
                )
                # 同时更新 stock_info.industry_l1
                cur.execute(
                    "UPDATE stock_info SET industry_l1=%s WHERE stock_code=%s AND (industry_l1 IS NULL OR industry_l1='')",
                    [name, stock_code],
                )
                total += 1

            conn.commit()
            logger.info(f"[{idx+1}/{len(boards)}] {name}: {len(cons)}只")
            time.sleep(0.3)

        except Exception as e:
            logger.warning(f"板块 {name} 失败: {e}")
            time.sleep(1)
            continue

    conn.close()
    logger.info(f"行业映射完成: {total}条记录")
    return total


# ==================== Task 2: 公司概况 ====================

def fetch_company_profiles(batch_size=50):
    """从巨潮获取公司概况（主营业务/经营范围/简介）"""
    import akshare as ak

    conn = get_conn()
    cur = conn.cursor()

    # 找出还没有 main_business 的股票
    cur.execute("SELECT stock_code FROM stock_info WHERE main_business IS NULL OR main_business=''")
    codes = [r["stock_code"] for r in cur.fetchall()]
    logger.info(f"待采集公司概况: {len(codes)}只")

    count = 0
    for i, code in enumerate(codes):
        try:
            df = ak.stock_profile_cninfo(symbol=code)
            if df is None or df.empty:
                continue

            row = df.iloc[0]
            main_biz = str(row.get("主营业务", "") or "")
            scope = str(row.get("经营范围", "") or "")
            intro = str(row.get("机构简介", "") or "")
            industry = str(row.get("所属行业", "") or "")

            cur.execute(
                """UPDATE stock_info
                   SET main_business=%s, business_scope=%s, company_intro=%s,
                       industry_l2=COALESCE(NULLIF(industry_l2,''), %s)
                   WHERE stock_code=%s""",
                [main_biz, scope, intro, industry, code],
            )
            conn.commit()
            count += 1

            if (i + 1) % 20 == 0:
                logger.info(f"公司概况进度: {i+1}/{len(codes)} (已采集{count})")

            time.sleep(0.5)

        except Exception as e:
            logger.debug(f"公司概况 {code}: {e}")
            time.sleep(1)
            continue

    conn.close()
    logger.info(f"公司概况完成: {count}只")
    return count


# ==================== Task 3: 主营构成 ====================

def fetch_business_composition(batch_size=50):
    """从东方财富获取主营构成（收入/成本/利润分解）"""
    import akshare as ak

    conn = get_conn()
    cur = conn.cursor()

    # 找出还没有主营构成数据的股票
    cur.execute("""
        SELECT si.stock_code, si.market FROM stock_info si
        LEFT JOIN (
            SELECT DISTINCT stock_code FROM stock_business_composition
        ) bc ON si.stock_code = bc.stock_code
        WHERE bc.stock_code IS NULL
    """)
    codes = [r["stock_code"] for r in cur.fetchall()]
    logger.info(f"待采集主营构成: {len(codes)}只")

    count = 0
    for i, code in enumerate(codes):
        # 东方财富需要 SH/SZ 前缀
        if code.startswith("6"):
            symbol = f"SH{code}"
        elif code.startswith(("0", "3")):
            symbol = f"SZ{code}"
        elif code.startswith("8") or code.startswith("4"):
            symbol = f"BJ{code}"
        else:
            symbol = f"SZ{code}"

        try:
            df = ak.stock_zygc_em(symbol=symbol)
            if df is None or df.empty:
                continue

            # 只保留最近年报数据
            latest = df[df["报告日期"].str.endswith("12-31")].head(20)
            if latest.empty:
                latest = df.head(10)

            for _, row in latest.iterrows():
                report_date = str(row.get("报告日期", ""))
                classify = str(row.get("分类类型", "") or "")
                item_name = str(row.get("主营构成", ""))
                revenue = row.get("主营收入")
                rev_pct = row.get("收入比例")
                cost = row.get("主营成本")
                cost_pct = row.get("成本比例")
                profit = row.get("主营利润")
                profit_pct = row.get("利润比例")
                margin = row.get("毛利率")

                def safe_float(v):
                    try:
                        f = float(v)
                        return f if str(f) != "nan" else None
                    except (TypeError, ValueError):
                        return None

                cur.execute(
                    """REPLACE INTO stock_business_composition
                       (stock_code, report_date, classify_type, item_name,
                        revenue, revenue_pct, cost, cost_pct,
                        profit, profit_pct, gross_margin)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [code, report_date, classify, item_name,
                     safe_float(revenue), safe_float(rev_pct),
                     safe_float(cost), safe_float(cost_pct),
                     safe_float(profit), safe_float(profit_pct),
                     safe_float(margin)],
                )

            conn.commit()
            count += 1

            if (i + 1) % 20 == 0:
                logger.info(f"主营构成进度: {i+1}/{len(codes)} (已采集{count})")

            time.sleep(0.5)

        except Exception as e:
            logger.debug(f"主营构成 {code}: {e}")
            time.sleep(1)
            continue

    conn.close()
    logger.info(f"主营构成完成: {count}只")
    return count


# ==================== Main ====================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="全量股票基础数据采集")
    parser.add_argument("--task", choices=["industry", "profile", "composition", "all"],
                        default="all", help="采集任务类型")
    args = parser.parse_args()

    if args.task in ("industry", "all"):
        fetch_industry_mapping()

    if args.task in ("profile", "all"):
        fetch_company_profiles()

    if args.task in ("composition", "all"):
        fetch_business_composition()

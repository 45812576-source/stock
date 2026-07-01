"""zsxq 回溯: 处理 PDF/audio/xlsx 重类型
- 从今天开始往前到 3/19
- 每天单独 fetch（不跳过heavy），txt/mixed已存在会去重跳过
- PDF/audio/xlsx 获取一个处理一个（下载链接有时效）
- fetch 完一天就立即跑该天的情报抽取
"""
import sys, os, time, logging
from datetime import date, timedelta, datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("/tmp/zsxq_backfill_night.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

from ingestion.zsxq_source import fetch_zsxq_data
from daily_intel.scanner import scan_zsxq_today

START = date(2026, 3, 19)
END = date.today()


def main():
    logger.info(f"== Heavy types backfill: {END} → {START} ==")

    cur = END
    while cur >= START:
        day_str = str(cur)
        next_str = str(cur + timedelta(days=1))

        try:
            # fetch 该天，不跳过 heavy types
            # txt/mixed/image 已存在会被去重跳过
            # PDF/audio/xlsx 会逐条: 获取下载链接 → 下载 → 提取
            result = fetch_zsxq_data(
                start_date=day_str,
                end_date=next_str,
                max_pages=100,
                skip_heavy_types=False,
            )
            saved = result.get("saved", 0) if isinstance(result, dict) else 0

            if saved > 0:
                logger.info(f"  [{cur}] fetch: saved={saved}")
                # 有新文档，重新跑该天情报抽取
                try:
                    intel = scan_zsxq_today(scan_date=cur, skip_social_post=False)
                    logger.info(f"  [{cur}] intel: {intel}")
                except Exception as e:
                    logger.error(f"  [{cur}] intel fail: {e}")
            else:
                logger.info(f"  [{cur}] fetch: 无新增heavy文档")

        except Exception as e:
            logger.error(f"  [{cur}] fetch fail: {e}")

        cur -= timedelta(days=1)
        time.sleep(2)  # 避免触发反爬

    logger.info("== Heavy types backfill complete ==")


if __name__ == "__main__":
    main()

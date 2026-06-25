"""zsxq 回溯 Phase 3: 夜间补跑 PDF/audio/xlsx
- 对 3/19~6/17 再跑一遍 fetch，skip_heavy_types=False
- txt/mixed/image 已存在会自动去重跳过
- PDF/audio/xlsx 逐条: 获取下载链接 → 立即下载 → 立即提取
- 然后逐天对新增的 heavy 类型做 scan_zsxq_today 抽情报
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
END = date(2026, 6, 17)


def main():
    logger.info(f"== Night Phase: fetch {START}~{END} with heavy types ==")

    # Phase 3a: 重新 fetch，不跳过 heavy types
    # txt/mixed/image 已存在会去重，PDF/audio/xlsx 逐条获取+清洗
    result = fetch_zsxq_data(
        start_date=str(START),
        end_date=str(END),
        max_pages=1200,
        skip_heavy_types=False,
    )
    logger.info(f"== Night fetch done: {result} ==")

    # Phase 3b: 逐天重新跑情报抽取（把新增 heavy docs 的情报也抽出来）
    cur = START
    while cur <= END:
        try:
            intel = scan_zsxq_today(scan_date=cur, skip_social_post=False)
            if intel.get("reports", 0) > 0:
                logger.info(f"  [intel] {cur} {intel}")
        except Exception as e:
            logger.error(f"  [intel] {cur} fail: {e}")
        cur += timedelta(days=1)

    logger.info("== Night phase complete ==")


if __name__ == "__main__":
    main()

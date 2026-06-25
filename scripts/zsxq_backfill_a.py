"""zsxq 回溯采集 + 逐天清洗（方案A 优化版）
Phase 1: 一次性 fetch 3/19~6/17 全部帖子到 source_documents（白天跳 heavy）
Phase 2: 逐天对 source_documents 提取文本 + LLM 抽情报
18:00 后自动切换夜间模式（不跳过 heavy types）
"""
import sys, os, time, logging
from datetime import date, timedelta, datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("/tmp/zsxq_backfill.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

from ingestion.zsxq_source import fetch_zsxq_data
from daily_intel.scanner import scan_zsxq_today
from utils.db_utils import execute_cloud_query, execute_cloud_insert

START = date(2026, 3, 19)
END = date(2026, 6, 17)


def is_night_mode():
    return datetime.now().hour >= 18


def extract_pending_for_day(day, night):
    """对指定日期的 zsxq source_documents 提取文本"""
    from ingestion.source_extractor import _extract_single_row

    if night:
        ft_filter = "('txt','mixed','image','pdf','audio','mp3','xlsx')"
    else:
        ft_filter = "('txt','mixed','image')"

    rows = execute_cloud_query(
        f"""SELECT id, doc_type, file_type, title, text_content, oss_url, extract_status
            FROM source_documents
            WHERE source='zsxq' AND DATE(publish_date)=%s
              AND (extract_status IS NULL OR extract_status IN ('pending',''))
              AND file_type IN {ft_filter}""",
        [day.isoformat()],
    ) or []

    if not rows:
        return 0, 0
    ok = 0
    fail = 0
    for r in rows:
        try:
            result = _extract_single_row(r)
            if result.get("status") == "extracted":
                execute_cloud_insert(
                    """UPDATE source_documents
                       SET extracted_text=%s, extract_status='extracted', doc_type=%s
                       WHERE id=%s""",
                    [result["extracted"], result["doc_type"], r["id"]],
                )
                ok += 1
            elif result.get("status") == "deferred":
                execute_cloud_insert(
                    "UPDATE source_documents SET extract_status='pending_large', oss_url=%s WHERE id=%s",
                    [result["local_path"], r["id"]],
                )
            else:
                fail += 1
        except Exception as e:
            err_str = str(e)
            if any(s in err_str for s in ("401", "403", "404", "Unauthorized", "Forbidden")):
                execute_cloud_insert(
                    "UPDATE source_documents SET extract_status='skipped' WHERE id=%s",
                    [r["id"]],
                )
            else:
                execute_cloud_insert(
                    "UPDATE source_documents SET extract_status='failed' WHERE id=%s",
                    [r["id"]],
                )
                logger.warning(f"  extract sd_id={r['id']} fail: {e}")
            fail += 1
    return ok, fail


def phase1_fetch():
    """Phase 1: 一次性拉取 3/19~6/17 全部帖子"""
    night = is_night_mode()
    skip_heavy = not night
    logger.info(f"== Phase 1: fetch {START}~{END} (skip_heavy={skip_heavy}) ==")

    # zsxq API 倒序翻页，需要大量页数才能回到3月
    # 91天 × ~200条/天 / 20条/页 ≈ 910 页
    result = fetch_zsxq_data(
        start_date=str(START),
        end_date=str(END),
        max_pages=1200,
        skip_heavy_types=skip_heavy,
    )
    logger.info(f"== Phase 1 完成: {result} ==")
    return result


def phase2_clean():
    """Phase 2: 逐天提取 + 抽情报"""
    cur = START
    while cur <= END:
        night = is_night_mode()
        logger.info(f"  -- Phase 2: {cur} (night={night}) --")

        # 2a) 提取文本
        t1 = time.time()
        try:
            ok, fail = extract_pending_for_day(cur, night=night)
            logger.info(f"    [extract] {cur} ok={ok} fail={fail} ({time.time()-t1:.1f}s)")
        except Exception as e:
            logger.error(f"    [extract] {cur} fail: {e}")

        # 2b) LLM 抽情报
        t2 = time.time()
        try:
            intel = scan_zsxq_today(scan_date=cur, skip_social_post=False)
            logger.info(f"    [intel] {cur} {intel} ({time.time()-t2:.1f}s)")
        except Exception as e:
            logger.error(f"    [intel] {cur} fail: {e}")

        cur += timedelta(days=1)

    logger.info("== Phase 2 完成 ==")


def main():
    phase1_fetch()
    phase2_clean()
    logger.info("==== 全部回溯完成 ====")


if __name__ == "__main__":
    main()

"""
拉取最近5天知识星球 mp3，并逐一提取转录

运行方式: python scripts/fetch_and_extract_mp3.py

步骤：
1. 对每个 group_id 调 ZsxqSource.fetch()，拉取最近5天的帖子（已存在的会被跳过）
2. 查询云端 source_documents 中近5天 pending 的 mp3/audio
3. 逐一重试转录（取一个、处理一个，处理完再取下一个）
"""
import sys
import os
import logging
import time
from datetime import date, timedelta

# 确保项目根在 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

from config import ZSXQ_GROUP_IDS, ZSXQ_COOKIE
from utils.db_utils import execute_cloud_query, execute_cloud_insert
from ingestion.zsxq_source import ZsxqSource
from ingestion.source_extractor import _extract_mp3, _extract_audio, _semantic_clean
from config.doc_types import classify_doc_type


# ── 配置 ──────────────────────────────────────────────────────────────────

LOOKBACK_DAYS = 5  # 往前几天
TODAY = date.today()
START_DATE = str(TODAY - timedelta(days=LOOKBACK_DAYS))
END_DATE = str(TODAY)


# ── Step 1: 拉取帖子 ──────────────────────────────────────────────────────

def fetch_all_groups():
    """对每个 group_id 拉取近5天帖子（已有的会 skip）"""
    total_saved = 0
    for group_id in ZSXQ_GROUP_IDS:
        group_id = group_id.strip()
        if not group_id:
            continue
        logger.info(f"=== 拉取 group_id={group_id}，日期范围 {START_DATE} ~ {END_DATE} ===")
        src = ZsxqSource(group_id=group_id, token=ZSXQ_COOKIE)
        result = src.fetch(
            max_pages=30,
            start_date=START_DATE,
            end_date=END_DATE,
            progress_callback=lambda page, saved, msg: logger.info(f"  {msg}"),
        )
        logger.info(
            f"  group_id={group_id} 完成: saved={result['saved']}, "
            f"skipped={result['skipped']}, total_fetched={result['total_fetched']}"
        )
        total_saved += result["saved"]
        time.sleep(2)
    return total_saved


# ── Step 2: 逐一提取 pending mp3 ─────────────────────────────────────────

def extract_pending_one_by_one():
    """查 pending 音频，取一条处理一条"""
    rows = execute_cloud_query(
        """
        SELECT id, title, file_type, oss_url, text_content
        FROM source_documents
        WHERE source='zsxq'
          AND file_type IN ('mp3', 'audio')
          AND extract_status = 'pending'
          AND publish_date >= %s
        ORDER BY publish_date DESC, id
        """,
        [START_DATE],
    )

    if not rows:
        logger.info("没有 pending 的音频，跳过提取步骤")
        return

    logger.info(f"\n=== 共 {len(rows)} 条 pending 音频，逐一提取 ===")

    success, failed, skipped = 0, 0, 0
    for i, row in enumerate(rows, 1):
        doc_id = row["id"]
        title = row.get("title", "")
        file_type = row.get("file_type", "mp3")
        logger.info(f"\n[{i}/{len(rows)}] 处理 id={doc_id} | {title[:60]}")

        # 取 URL（兼容新旧格式）
        tc = (row.get("text_content") or "").strip()
        oss_url = (row.get("oss_url") or "").strip()
        url_to_use = tc if tc.startswith("http") else oss_url

        if not url_to_use or not url_to_use.startswith("http"):
            logger.warning(f"  URL 为空，标记 skipped")
            execute_cloud_insert(
                "UPDATE source_documents SET extract_status='skipped' WHERE id=%s",
                [doc_id],
            )
            skipped += 1
            continue

        try:
            # 转写
            if file_type == "mp3":
                raw_text = _extract_mp3(row)
            else:
                raw_text = _extract_audio(row)

            if not raw_text or len(raw_text.strip()) < 20:
                logger.warning(f"  转写结果太短（{len(raw_text or '')}字），标记 failed")
                execute_cloud_insert(
                    "UPDATE source_documents SET extract_status='failed' WHERE id=%s",
                    [doc_id],
                )
                failed += 1
                continue

            # 语义清洗
            cleaned = _semantic_clean(raw_text, "audio", doc_id, needs_understanding=False)
            if not cleaned:
                cleaned = raw_text

            new_doc_type = classify_doc_type(title, cleaned[:200])
            execute_cloud_insert(
                "UPDATE source_documents SET extracted_text=%s, extract_status='extracted', doc_type=%s WHERE id=%s",
                [cleaned, new_doc_type, doc_id],
            )
            logger.info(f"  ✓ 完成，{len(cleaned)} 字，doc_type={new_doc_type}")
            success += 1

        except Exception as e:
            logger.error(f"  ✗ 失败: {e}")
            execute_cloud_insert(
                "UPDATE source_documents SET extract_status='failed' WHERE id=%s",
                [doc_id],
            )
            failed += 1

    logger.info(f"\n=== 提取完成: 成功={success}, 失败={failed}, 跳过={skipped} ===")


# ── main ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info(f"开始处理，日期范围: {START_DATE} ~ {END_DATE}")
    logger.info(f"Group IDs: {ZSXQ_GROUP_IDS}")

    # Step 1: 拉取新帖子
    logger.info("\n[Step 1] 拉取知识星球近5天帖子...")
    new_saved = fetch_all_groups()
    logger.info(f"[Step 1] 完成，新增 {new_saved} 条记录")

    # Step 2: 提取 pending 音频
    logger.info("\n[Step 2] 提取 pending 的 mp3...")
    extract_pending_one_by_one()

    logger.info("\n全部完成！")

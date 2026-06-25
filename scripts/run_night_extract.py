"""晚间批量清洗脚本 — PDF 重新提取 + 音频下载&转录

使用方式:
    nohup .venv/bin/python -u scripts/run_night_extract.py > logs/night_extract.log 2>&1 &

流程:
1. 重新提取今天采集的空文本 PDF（oss_url 已过期的跳过）
2. 音频: 先 extract_batch 下载到本地 (pending → pending_large)
3. 音频: run_large_file_extract 逐条转录直到全部完成
"""
import logging
import sys
import time
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def phase1_pdf_retry():
    """重新提取今天采集但 extracted_text 为空的 PDF"""
    from utils.db_utils import execute_cloud_query, execute_cloud_insert
    from ingestion.source_extractor import _extract_single_row
    from utils.model_router import invalidate_config_cache
    invalidate_config_cache()

    rows = execute_cloud_query("""
        SELECT id, doc_type, file_type, title, text_content, oss_url, extract_status
        FROM source_documents
        WHERE source='zsxq' AND file_type='pdf'
          AND extract_status='extracted'
          AND (extracted_text IS NULL OR extracted_text='')
          AND oss_url IS NOT NULL AND oss_url != ''
          AND created_at >= '2026-06-23'
        LIMIT 50
    """)
    logger.info(f"[PDF重试] 发现 {len(rows)} 条空文本 PDF")

    success, failed = 0, 0
    for row in rows:
        try:
            # 先把状态改回 pending 让 _extract_single_row 处理
            execute_cloud_insert(
                "UPDATE source_documents SET extract_status='pending' WHERE id=%s",
                [row['id']]
            )
            row['extract_status'] = 'pending'
            result = _extract_single_row(row)
            ext = result.get('extracted', '') or ''
            if result.get('status') == 'extracted' and ext:
                execute_cloud_insert(
                    "UPDATE source_documents SET extracted_text=%s, extract_status='extracted', doc_type=%s WHERE id=%s",
                    [ext, result.get('doc_type', row['doc_type']), row['id']]
                )
                success += 1
                logger.info(f"[PDF重试] id={row['id']} 成功 ({len(ext)}字)")
            elif result.get('status') == 'deferred':
                logger.info(f"[PDF重试] id={row['id']} deferred(大文件)")
                success += 1  # 算成功，后续 run_large_file_extract 会处理
            else:
                execute_cloud_insert(
                    "UPDATE source_documents SET extract_status='failed' WHERE id=%s",
                    [row['id']]
                )
                failed += 1
        except Exception as e:
            logger.warning(f"[PDF重试] id={row['id']} 失败: {e}")
            execute_cloud_insert(
                "UPDATE source_documents SET extract_status='failed' WHERE id=%s",
                [row['id']]
            )
            failed += 1

    logger.info(f"[PDF重试] 完成: success={success}, failed={failed}")
    return {"success": success, "failed": failed}


def phase2_audio_download():
    """音频: extract_batch 下载到本地 (pending → pending_large)"""
    from ingestion.source_extractor import extract_batch
    from utils.model_router import invalidate_config_cache
    invalidate_config_cache()

    logger.info("[音频下载] 开始下载音频到本地...")
    result = extract_batch(file_type='audio', limit=200)
    logger.info(f"[音频下载] 完成: {result}")
    return result


def phase3_audio_transcribe():
    """音频: 循环调用 run_large_file_extract 直到没有 pending_large"""
    from ingestion.source_extractor import run_large_file_extract
    from utils.db_utils import execute_cloud_query
    from utils.model_router import invalidate_config_cache
    invalidate_config_cache()

    total_success, total_failed = 0, 0
    round_num = 0

    while True:
        round_num += 1
        # 检查还有多少 pending_large
        cnt = execute_cloud_query(
            "SELECT COUNT(*) AS n FROM source_documents WHERE extract_status='pending_large'"
        )
        remaining = cnt[0]['n'] if cnt else 0
        if remaining == 0:
            break

        logger.info(f"[音频转录] 第{round_num}轮, 剩余 {remaining} 条")
        result = run_large_file_extract(limit=10)
        total_success += result.get('success', 0)
        total_failed += result.get('failed', 0)

        if result.get('total', 0) == 0:
            break

        # 每轮之间休息 5 秒，避免内存压力
        time.sleep(5)

    logger.info(f"[音频转录] 全部完成: success={total_success}, failed={total_failed}")
    return {"success": total_success, "failed": total_failed}


def main():
    start = datetime.now()
    logger.info(f"=== 晚间清洗开始 {start.strftime('%Y-%m-%d %H:%M:%S')} ===")

    # Phase 1: PDF
    logger.info("--- Phase 1: PDF 重新提取 ---")
    pdf_result = phase1_pdf_retry()

    # Phase 2: 音频下载
    logger.info("--- Phase 2: 音频下载到本地 ---")
    dl_result = phase2_audio_download()

    # Phase 3: 音频转录
    logger.info("--- Phase 3: 音频批量转录 ---")
    tr_result = phase3_audio_transcribe()

    end = datetime.now()
    elapsed = (end - start).total_seconds() / 3600
    logger.info(f"=== 晚间清洗结束 {end.strftime('%Y-%m-%d %H:%M:%S')} (耗时 {elapsed:.1f}h) ===")
    logger.info(f"PDF: {pdf_result}")
    logger.info(f"音频下载: {dl_result}")
    logger.info(f"音频转录: {tr_result}")


if __name__ == "__main__":
    main()

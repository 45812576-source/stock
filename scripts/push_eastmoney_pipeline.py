"""推送 eastmoney_report 进清洗管线

用法：
  # 模式1：跑待处理的完整管线 A+C+D（offset 批次起点，batch 每批数量）
  python3 scripts/push_eastmoney_pipeline.py --mode=full --offset=0   --batch=20
  python3 scripts/push_eastmoney_pipeline.py --mode=full --offset=20  --batch=20
  python3 scripts/push_eastmoney_pipeline.py --mode=full --offset=40  --batch=20

  # 模式2：仅补 D（AC已完成的族2文档）
  python3 scripts/push_eastmoney_pipeline.py --mode=d_only --offset=0   --batch=20
  python3 scripts/push_eastmoney_pipeline.py --mode=d_only --offset=20  --batch=20
  python3 scripts/push_eastmoney_pipeline.py --mode=d_only --offset=40  --batch=20
"""
import argparse
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.db_utils import execute_cloud_query
from cleaning.unified_pipeline import process_single
from cleaning.industry_indicator_extractor import run_pipeline_d

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

FAMILY2 = ['research_report', 'strategy_report', 'roadshow_notes', 'feature_news']


def fetch_full_pending(limit: int, offset: int):
    """拉取需要跑 A/C 任意一条管线的 eastmoney 文档"""
    return execute_cloud_query('''
        SELECT DISTINCT et.id,
            (cs.id IS NULL) as need_a,
            (et.kg_status IS NULL OR et.kg_status != 'done') as need_c
        FROM extracted_texts et
        LEFT JOIN content_summaries cs ON et.id = cs.extracted_text_id
        LEFT JOIN source_documents sd ON et.source_doc_id = sd.id
        WHERE sd.source = %s
          AND et.extract_quality != %s
          AND (cs.id IS NULL OR et.kg_status IS NULL OR et.kg_status != 'done')
        ORDER BY et.id
        LIMIT %s OFFSET %s
    ''', ['eastmoney_report', 'fail', limit, offset])


def fetch_d_only_pending(limit: int, offset: int):
    """拉取 AC 已完成但未跑 D 的族2文档"""
    placeholders = ','.join(['%s'] * len(FAMILY2))
    return execute_cloud_query(f'''
        SELECT et.id, sd.doc_type, et.full_text
        FROM extracted_texts et
        JOIN source_documents sd ON et.source_doc_id = sd.id
        WHERE sd.source = %s
          AND et.summary_status = %s
          AND et.kg_status = %s
          AND sd.doc_type IN ({placeholders})
          AND (et.semantic_clean_status IS NULL OR et.semantic_clean_status != 'd_done')
        ORDER BY et.id
        LIMIT %s OFFSET %s
    ''', ['eastmoney_report', 'done', 'done'] + FAMILY2 + [limit, offset])


def run_full_batch(batch: int, offset: int, max_workers: int):
    rows = fetch_full_pending(batch, offset)
    if not rows:
        logger.info("没有待处理数据，退出")
        return

    logger.info(f"[full] 拉到 {len(rows)} 条，offset={offset}，并发={max_workers}")
    t0 = time.time()
    ok = fail = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {
            pool.submit(process_single, r['id'],
                        need_a=bool(r['need_a']),
                        need_c=bool(r['need_c']),
                        need_d=True): r
            for r in rows
        }
        for fut in as_completed(futs):
            r = futs[fut]
            try:
                res = fut.result()
                ok += 1
                logger.info(
                    f"✓ [{ok+fail}/{len(rows)}] id={r['id']} "
                    f"summary={res['summary_id']} kg={res['kg_rels']} "
                    f"chunks={res['chunks']} indicators={res['indicators']}"
                )
            except Exception as e:
                fail += 1
                logger.error(f"✗ [{ok+fail}/{len(rows)}] id={r['id']} {e}")

    elapsed = time.time() - t0
    logger.info(f"[full] 完成 ok={ok} fail={fail} 耗时={elapsed:.1f}s ({elapsed/len(rows):.1f}s/条)")


def run_d_only_batch(batch: int, offset: int, max_workers: int):
    rows = fetch_d_only_pending(batch, offset)
    if not rows:
        logger.info("没有待补 D 的数据，退出")
        return

    logger.info(f"[d_only] 拉到 {len(rows)} 条，offset={offset}，并发={max_workers}")
    t0 = time.time()
    ok = fail = 0

    def _run_d(row):
        full_text = row.get('full_text') or ''
        if not full_text.strip():
            return 0
        return run_pipeline_d(row['id'], full_text[:12000])

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(_run_d, r): r for r in rows}
        for fut in as_completed(futs):
            r = futs[fut]
            try:
                indicators = fut.result()
                ok += 1
                logger.info(f"✓ [{ok+fail}/{len(rows)}] id={r['id']} doc_type={r['doc_type']} indicators={indicators}")
            except Exception as e:
                fail += 1
                logger.error(f"✗ [{ok+fail}/{len(rows)}] id={r['id']} {e}")

    elapsed = time.time() - t0
    logger.info(f"[d_only] 完成 ok={ok} fail={fail} 耗时={elapsed:.1f}s ({elapsed/len(rows):.1f}s/条)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['full', 'd_only'], default='full')
    parser.add_argument('--offset', type=int, default=0)
    parser.add_argument('--batch', type=int, default=20)
    parser.add_argument('--workers', type=int, default=0,
                        help='并发数，默认等于 batch')
    args = parser.parse_args()

    workers = args.workers if args.workers > 0 else args.batch

    if args.mode == 'full':
        run_full_batch(args.batch, args.offset, workers)
    else:
        run_d_only_batch(args.batch, args.offset, workers)

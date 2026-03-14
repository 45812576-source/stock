"""对 eastmoney_report 已完成 semantic_clean 的研报，跑 Pipeline A（摘要）+ C（KG）+ D（行业指标）

只处理：
- source = eastmoney_report
- full_text 不为空
- A 未跑（content_summaries 无对应记录）或 C 未跑（kg_status != 'done'）

用法：
    python scripts/run_pipeline_ac_eastmoney.py
    python scripts/run_pipeline_ac_eastmoney.py --workers 3
"""
import sys
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, ".")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scripts/run_pipeline_ac_eastmoney.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def get_pending() -> list[dict]:
    from utils.db_utils import execute_cloud_query
    # 只处理已语义清洗的记录（full_text 明显短于原始 text_content，即非透传）
    rows = execute_cloud_query(
        """SELECT DISTINCT et.id,
                  (cs.id IS NULL) as need_a,
                  (et.kg_status IS NULL OR et.kg_status != 'done') as need_c
           FROM extracted_texts et
           JOIN source_documents sd ON sd.id = et.source_doc_id
           LEFT JOIN content_summaries cs ON et.id = cs.extracted_text_id
           WHERE sd.source = %s
             AND et.full_text IS NOT NULL AND et.full_text != ''
             AND LENGTH(et.full_text) < LENGTH(sd.text_content) * 0.90
             AND (cs.id IS NULL OR et.kg_status IS NULL OR et.kg_status != 'done')
           ORDER BY et.id""",
        ["eastmoney_report"]
    ) or []
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()

    from cleaning.unified_pipeline import process_single

    rows = get_pending()
    total = len(rows)
    logger.info(f"待处理: {total} 条  workers={args.workers}")

    ok = fail = 0

    def _run(row):
        return process_single(
            row["id"],
            need_a=bool(row["need_a"]),
            need_c=bool(row["need_c"]),
            need_d=True,
        )

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(_run, row): row["id"] for row in rows}
        for i, future in enumerate(as_completed(futures), 1):
            et_id = futures[future]
            try:
                future.result()
                ok += 1
            except Exception as e:
                logger.error(f"[{et_id}] 失败: {e}")
                fail += 1
            if i % 100 == 0:
                logger.info(f"进度 {i}/{total}  ok={ok} fail={fail}")

    logger.info(f"完成！ok={ok} fail={fail}")


if __name__ == "__main__":
    main()

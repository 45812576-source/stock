"""批量拉取所有产业链龙头股近2年研报并提取

用法：
    python scripts/backfill_chain_reports.py              # 全部394只（仅下载入库，不extract）
    python scripts/backfill_chain_reports.py --skip-done  # 跳过已有记录的股票
    python scripts/backfill_chain_reports.py --dry-run    # 只打印股票列表不执行
    python scripts/backfill_chain_reports.py --extract-only  # 只对已入库未提取的研报跑extract
"""
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta

sys.path.insert(0, ".")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scripts/backfill_chain_reports.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def get_chain_stocks() -> list[dict]:
    """从 chain_config 解析所有龙头股，映射到股票代码，返回去重列表"""
    from config.chain_config import CHAINS
    from utils.db_utils import execute_query

    all_names = set()
    for chain in CHAINS.values():
        for tier in chain["tiers"].values():
            all_names.update(tier.get("stocks", []))

    names = list(all_names)
    ph = ",".join(["%s"] * len(names))
    rows = execute_query(
        f"SELECT stock_code, stock_name FROM stock_info WHERE stock_name IN ({ph})",
        names,
    ) or []

    logger.info(f"chain_config 共 {len(names)} 个名称，匹配到 {len(rows)} 只股票代码")
    return [{"stock_code": r["stock_code"], "stock_name": r["stock_name"]} for r in rows]


def backfill_stock(source, stock_code: str, stock_name: str, years: int = 2) -> int:
    """拉取单只股票近N年研报入库（不做extract），返回新入库数"""
    end_date = datetime.now().strftime("%Y-%m-%d")
    begin_date = (datetime.now() - timedelta(days=365 * years)).strftime("%Y-%m-%d")

    logger.info(f"[{stock_code}] {stock_name}: {begin_date} ~ {end_date}")
    doc_ids = source._fetch_stock_range(stock_code, begin_date, end_date)
    count = len(doc_ids)
    logger.info(f"[{stock_code}] 入库 {count} 条")
    return count


def run_extract_all(concurrency: int = 3):
    """对所有已入库但未提取的 eastmoney_report 研报批量跑 extract + push，支持并发批次"""
    from utils.db_utils import execute_cloud_query
    from ingestion.source_extractor import extract_by_ids, push_to_extracted_texts_by_ids
    from concurrent.futures import ThreadPoolExecutor, as_completed

    rows = execute_cloud_query(
        "SELECT id FROM source_documents WHERE source=%s AND extract_status='pending' ORDER BY id",
        ["eastmoney_report"]
    ) or []
    ids = [r["id"] for r in rows]
    logger.info(f"待提取研报: {len(ids)} 条，并发批次: {concurrency}")
    if not ids:
        return

    batch_size = 100
    batches = [ids[i:i + batch_size] for i in range(0, len(ids), batch_size)]
    total_batches = len(batches)

    def process_batch(batch_idx, batch):
        r1 = extract_by_ids(batch)
        r2 = push_to_extracted_texts_by_ids(batch)
        logger.info(f"批次 [{batch_idx}/{total_batches}] 提取={r1}, 推管线={r2}")

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(process_batch, i + 1, batch): i
            for i, batch in enumerate(batches)
        }
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"批次失败: {e}")


def main():
    parser = argparse.ArgumentParser(description="批量拉取产业链龙头股研报")
    parser.add_argument("--skip-done", action="store_true",
                        help="跳过 eastmoney_stock_sync 中已有 backfilled_at 记录的股票")
    parser.add_argument("--years", type=int, default=2, help="回溯年数（默认2年）")
    parser.add_argument("--dry-run", action="store_true", help="只打印列表不执行采集")
    parser.add_argument("--delay", type=float, default=1.0, help="每只股票间隔秒数（默认1s）")
    parser.add_argument("--extract-only", action="store_true", help="只跑extract，不下载研报")
    args = parser.parse_args()

    if args.extract_only:
        run_extract_all()
        return

    stocks = get_chain_stocks()
    if not stocks:
        logger.error("未获取到任何股票，退出")
        return

    if args.skip_done:
        from utils.db_utils import execute_query
        done_rows = execute_query(
            "SELECT stock_code FROM eastmoney_stock_sync WHERE backfilled_at IS NOT NULL", []
        ) or []
        done_codes = {r["stock_code"] for r in done_rows}
        before = len(stocks)
        stocks = [s for s in stocks if s["stock_code"] not in done_codes]
        logger.info(f"--skip-done: 跳过已完成 {before - len(stocks)} 只，剩余 {len(stocks)} 只")

    logger.info(f"计划处理 {len(stocks)} 只股票，回溯 {args.years} 年")

    if args.dry_run:
        for s in stocks:
            print(f"{s['stock_code']}  {s['stock_name']}")
        print(f"\n共 {len(stocks)} 只（dry-run，未执行采集）")
        return

    from ingestion.eastmoney_report_source import EastmoneyReportSource
    source = EastmoneyReportSource()
    source._ensure_sync_table()

    total_new = 0
    for i, stock in enumerate(stocks, 1):
        code = stock["stock_code"]
        name = stock["stock_name"]
        try:
            cnt = backfill_stock(source, code, name, years=args.years)
            source._upsert_sync(code, name, backfilled=True, added_count=cnt)
            total_new += cnt
            logger.info(f"进度 [{i}/{len(stocks)}] {code} {name} +{cnt}  累计={total_new}")
        except Exception as e:
            logger.error(f"[{code}] {name} 失败: {e}")

        if i < len(stocks):
            time.sleep(args.delay)

    logger.info(f"完成！共处理 {len(stocks)} 只股票，新入库研报 {total_new} 条")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""并发批量清洗 — 5 worker 同时调用 Claude CLI"""
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(Path(__file__).parent.parent / "cleaning.log")),
    ],
)

from cleaning.batch_cleaner import batch_clean_parallel, get_cleaning_stats

BATCH_SIZE = 500
WORKERS = 5


def progress(current, total, title):
    if current % 10 == 0 or current <= 3 or current == total:
        stats = get_cleaning_stats()
        print(
            f"  [{current}/{total}] cleaned={stats['cleaned']} failed={stats['failed']} | {(title or '')[:40]}",
            flush=True,
        )


def main():
    stats = get_cleaning_stats()
    pending = stats["pending"]
    print(f"待清洗: {pending} 条，已清洗: {stats['cleaned']}，模型: sonnet，workers: {WORKERS}", flush=True)

    round_num = 0
    while True:
        stats = get_cleaning_stats()
        if stats["pending"] == 0:
            print("全部清洗完毕！", flush=True)
            break

        round_num += 1
        batch = min(BATCH_SIZE, stats["pending"])
        print(f"\n=== 第 {round_num} 轮（{batch} 条，{WORKERS} 并发） ===", flush=True)
        result = batch_clean_parallel(limit=batch, workers=WORKERS, progress_callback=progress)
        print(
            f"本轮结果: 成功={result['success']} 失败={result['failed']} 总计={result['total']}",
            flush=True,
        )

        if result["total"] > 0 and result["success"] / result["total"] < 0.3:
            import time
            print("成功率过低，等待 60 秒后重试...", flush=True)
            time.sleep(60)


if __name__ == "__main__":
    main()

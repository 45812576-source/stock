#!/usr/bin/env python3
"""AI 追问对话 Worker

在本地 shell 运行，轮询 DB 中 pending 的 assistant 消息，
调用 Claude CLI 生成回复写回 DB。

用法：
  python chat_worker.py              # 前台运行
  python chat_worker.py --once       # 处理一次后退出
  python chat_worker.py --interval 5 # 每 5 秒轮询一次（默认 3 秒）
"""
import argparse
import logging
import sys
import time

# 确保项目根目录在 sys.path
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from hotspot.chat_handler import process_pending_messages
from portfolio.chat_handler import process_pending_messages as process_project_pending

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ChatWorker] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="AI Chat Worker")
    parser.add_argument("--once", action="store_true", help="处理一次后退出")
    parser.add_argument("--interval", type=int, default=3, help="轮询间隔秒数")
    args = parser.parse_args()

    logger.info("Chat Worker 启动，轮询间隔 %d 秒", args.interval)

    try:
        while True:
            try:
                processed = process_pending_messages()
                processed += process_project_pending()
                if processed:
                    logger.info("本轮处理了 %d 条消息", processed)
            except Exception as e:
                logger.error("处理异常: %s", e)

            if args.once:
                break

            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Worker 已停止")


if __name__ == "__main__":
    main()

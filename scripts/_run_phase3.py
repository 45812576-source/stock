#!/usr/bin/env python3
"""Phase 3: unified_pipeline A/B2/C（独立进程，跑完自动退出释放内存）"""
import logging, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("scripts/_run_phase3.log", encoding="utf-8")])
logger = logging.getLogger(__name__)

logger.info("=== Phase 3: unified_pipeline A/B2/C ===")
from cleaning.unified_pipeline import process_pending
result = process_pending(batch_size=50, max_workers=3)
logger.info(f"Phase 3 完成: {result}")

"""数据源基类 — 限流、去重、错误处理"""
import time
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)


class BaseSource(ABC):
    """所有数据源的基类"""

    def __init__(self, source_name):
        self.source_name = source_name
        self._source_info = None

    @property
    def source_info(self):
        if self._source_info is None:
            rows = execute_query(
                "SELECT * FROM data_sources WHERE name = ?", [self.source_name]
            )
            self._source_info = rows[0] if rows else None
        return self._source_info

    @property
    def source_id(self):
        return self.source_info["id"] if self.source_info else None

    def check_limit(self):
        """检查是否超出限额"""
        info = self.source_info
        if not info:
            return True
        today = datetime.now().strftime("%Y-%m-%d")
        if info["last_reset_date"] != today:
            execute_insert(
                "UPDATE data_sources SET today_used=0, last_reset_date=? WHERE id=?",
                [today, info["id"]],
            )
            self._source_info = None
            info = self.source_info
        if info["daily_limit"] > 0 and info["today_used"] >= info["daily_limit"]:
            logger.warning(f"{self.source_name} 已达日限额")
            return False
        if info["monthly_limit"] > 0 and info["month_used"] >= info["monthly_limit"]:
            logger.warning(f"{self.source_name} 已达月限额")
            return False
        return True

    def increment_usage(self, count=1):
        """增加使用计数"""
        execute_insert(
            "UPDATE data_sources SET today_used=today_used+?, month_used=month_used+? WHERE id=?",
            [count, count, self.source_id],
        )
        self._source_info = None

    def is_duplicate(self, external_id):
        """检查是否已采集"""
        rows = execute_query(
            "SELECT id FROM raw_items WHERE source_id=? AND external_id=?",
            [self.source_id, external_id],
        )
        return len(rows) > 0

    def save_raw_item(self, external_id, title, content, url=None,
                      published_at=None, item_type="news", meta_json=None):
        """保存原始条目（自动去重）"""
        if self.is_duplicate(external_id):
            return None
        row_id = execute_insert(
            """INSERT INTO raw_items (source_id, external_id, title, content, url,
               published_at, item_type, meta_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [self.source_id, external_id, title, content, url,
             published_at, item_type, meta_json],
        )
        self.increment_usage()
        return row_id

    @abstractmethod
    def fetch(self, **kwargs):
        """子类实现具体采集逻辑"""
        pass

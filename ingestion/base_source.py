"""数据源基类 — 限流、去重、错误处理"""
import time
import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime
from utils.db_utils import execute_cloud_query, execute_cloud_insert

logger = logging.getLogger(__name__)


def _check_text_quality(text: str) -> str:
    """简单质量检查，返回 'pass' 或 'fail'"""
    if not text or len(text.strip()) < 20:
        return 'fail'
    # 乱码检测：非中文/英文/数字/标点的字符比例超过 30% 则判 fail
    total = len(text)
    garbage = len(re.findall(r'[^\u4e00-\u9fff\u3000-\u303f\uff00-\uffef'
                              r'a-zA-Z0-9\s\.,!?;:，。！？；：、""''（）【】《》\-_/\\@#%&*+=]',
                              text))
    if garbage / total > 0.3:
        return 'fail'
    return 'pass'


class BaseSource(ABC):
    """所有数据源的基类"""

    def __init__(self, source_name):
        self.source_name = source_name
        self._source_info = None

    @property
    def source_info(self):
        if self._source_info is None:
            rows = execute_cloud_query(
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
            execute_cloud_insert(
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
        execute_cloud_insert(
            "UPDATE data_sources SET today_used=today_used+?, month_used=month_used+? WHERE id=?",
            [count, count, self.source_id],
        )
        self._source_info = None

    def is_duplicate(self, external_id):
        """检查 raw_items 是否已采集（旧管线）"""
        rows = execute_cloud_query(
            "SELECT id FROM raw_items WHERE source_id=? AND external_id=?",
            [self.source_id, external_id],
        )
        return len(rows) > 0

    def is_duplicate_extracted(self, source_ref: str) -> bool:
        """检查 extracted_texts 是否已存在（新管线）"""
        rows = execute_cloud_query(
            "SELECT id FROM extracted_texts WHERE source=%s AND source_ref=%s",
            [self.source_name, source_ref],
        )
        return len(rows) > 0

    def save_source_doc(self, dedup_key: str, title: str, extracted_text: str,
                        doc_type: str = "news", file_type: str = "txt",
                        author: str = None, publish_date: str = None) -> int:
        """保存到 source_documents（新管线），以 source+dedup_key 去重

        Args:
            dedup_key: 唯一标识，有真实 URL 时传 URL，否则传 external_id
            title: 文档标题
            extracted_text: 已提取的文本内容
            doc_type: 文档类型（news/report/announcement/earnings）
            file_type: 文件类型（txt/pdf 等）
            author: 作者
            publish_date: 发布日期（str YYYY-MM-DD 或 datetime）
        Returns:
            新记录 id，重复则返回 None
        """
        existing = execute_cloud_query(
            "SELECT id FROM source_documents WHERE source=%s AND oss_url=%s LIMIT 1",
            [self.source_name, dedup_key],
        )
        if existing:
            return None
        doc_id = execute_cloud_insert(
            """INSERT INTO source_documents
               (doc_type, file_type, title, author, publish_date, source,
                oss_url, extracted_text, extract_status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'extracted')""",
            [doc_type, file_type, title, author, publish_date,
             self.source_name, dedup_key, extracted_text],
        )
        if doc_id:
            self.increment_usage()
        return doc_id

    def save_raw_item(self, external_id, title, content, url=None,
                      published_at=None, item_type="news", meta_json=None):
        """保存原始条目到 raw_items（旧管线，保留兼容）"""
        if self.is_duplicate(external_id):
            return None
        row_id = execute_cloud_insert(
            """INSERT INTO raw_items (source_id, external_id, title, content, url,
               published_at, item_type, meta_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [self.source_id, external_id, title, content, url,
             published_at, item_type, meta_json],
        )
        self.increment_usage()
        return row_id

    def save_extracted_text(self, source_ref: str, full_text: str,
                             publish_time=None, source_format: str = "text",
                             source_doc_id: int = None) -> int:
        """保存提取文本到 extracted_texts（新管线）

        Args:
            source_ref: 唯一标识（如 external_id 或 sd_{doc_id}）
            full_text: 提取的全文
            publish_time: 发布时间（datetime 或 str）
            source_format: 格式（text/markdown/pdf/audio/image）
            source_doc_id: 关联的 source_documents.id

        Returns:
            新记录 id，重复则返回 None
        """
        if self.is_duplicate_extracted(source_ref):
            return None
        quality = _check_text_quality(full_text)
        row_id = execute_cloud_insert(
            """INSERT INTO extracted_texts
               (source, source_format, publish_time, full_text,
                source_doc_id, source_ref, extract_quality)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            [self.source_name, source_format, publish_time, full_text,
             source_doc_id, source_ref, quality],
        )
        self.increment_usage()
        return row_id

    @abstractmethod
    def fetch(self, **kwargs):
        """子类实现具体采集逻辑"""
        pass


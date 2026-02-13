"""i问财数据查询"""
import logging
from ingestion.base_source import BaseSource

logger = logging.getLogger(__name__)


class IwencaiSource(BaseSource):
    """i问财查询器"""

    def __init__(self):
        super().__init__("iwencai")

    def fetch(self, query=None):
        """查询i问财
        TODO: 需验证码登录，用于按需查询
        """
        logger.info(f"i问财查询: query={query}")
        return 0

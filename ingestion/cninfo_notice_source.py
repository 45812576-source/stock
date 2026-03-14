"""巨潮资讯公司公告采集 — 通过 AKShare 获取公告元数据+链接"""
import logging
import hashlib
from datetime import datetime, timedelta

from ingestion.base_source import BaseSource

logger = logging.getLogger(__name__)

# 支持的公告分类
NOTICE_CATEGORIES = [
    "年报", "半年报", "一季报", "三季报", "业绩预告",
    "权益分派", "董事会", "监事会", "股东大会", "日常经营",
    "公司治理", "中介报告", "首发", "增发", "股权激励",
    "配股", "解禁", "公司债", "可转债", "其他融资",
    "股权变动", "补充更正", "澄清致歉", "风险提示",
    "特别处理和退市", "退市整理期",
]

# 默认采集的重点分类
DEFAULT_CATEGORIES = ["业绩预告", "风险提示", "资产重组", "股权变动", "日常经营"]


class CninfoNoticeSource(BaseSource):
    """巨潮资讯公告采集器"""

    def __init__(self):
        super().__init__("cninfo_notice")

    def fetch(self, stock_codes=None, categories=None, days=3, limit=200):
        """采集公司公告

        Args:
            stock_codes: 股票代码列表，如 ["000001", "300750"]。None 则从 watchlist 获取
            categories: 公告分类列表，如 ["业绩预告", "风险提示"]。None 用默认
            days: 回溯天数
            limit: 每个股票最大采集数
        """
        if not self.check_limit():
            logger.warning("巨潮公告已达限额")
            return 0

        import akshare as ak

        if stock_codes is None:
            stock_codes = self._get_watchlist_codes()
        if not stock_codes:
            logger.warning("无股票代码可采集公告")
            return 0

        if categories is None:
            categories = [""]  # 空字符串=全部分类

        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

        count = 0
        for code in stock_codes:
            for cat in categories:
                try:
                    df = ak.stock_zh_a_disclosure_report_cninfo(
                        symbol=code, market="沪深京",
                        category=cat,
                        start_date=start_date, end_date=end_date,
                    )
                    if df is None or df.empty:
                        continue

                    for _, row in df.head(limit).iterrows():
                        saved = self._save_notice(row, code, cat)
                        if saved:
                            count += 1
                except Exception as e:
                    logger.debug(f"公告采集 {code}/{cat}: {e}")
                    continue

        logger.info(f"巨潮公告采集完成: {count}条")
        return count

    def _save_notice(self, row, stock_code, category):
        """保存单条公告"""
        title = str(row.get("公告标题", "")).strip()
        if not title:
            return None

        pub_date = str(row.get("公告时间", ""))[:10]
        url = str(row.get("公告链接", ""))
        stock_name = str(row.get("简称", ""))

        dedup_key = url if url else hashlib.md5(
            f"cninfo:{stock_code}:{title}:{pub_date}".encode()
        ).hexdigest()

        content = f"公告标题: {title}\n股票: {stock_name}({stock_code})\n分类: {category}\n日期: {pub_date}"
        if url:
            content += f"\n链接: {url}"

        return self.save_source_doc(
            dedup_key=dedup_key,
            title=f"[{stock_name}] {title}",
            extracted_text=content,
            doc_type="announcement",
            publish_date=pub_date,
        )

    def _get_watchlist_codes(self):
        """从 watchlist 获取关注的股票代码"""
        from utils.db_utils import execute_query
        rows = execute_query(
            "SELECT stock_code FROM watchlist WHERE status IN ('watching', 'holding')"
        )
        return [r["stock_code"] for r in rows] if rows else []

"""东方财富研报采集 — 通过报告API获取研报并提取PDF全文，写入新管线 source_documents"""
import logging
import tempfile
import os

import requests

from ingestion.base_source import BaseSource
from utils.db_utils import execute_cloud_query, execute_cloud_insert

logger = logging.getLogger(__name__)

REPORT_API = "https://reportapi.eastmoney.com/report/list"
PDF_BASE = "https://pdf.dfcfw.com/pdf/H3_{}_1.pdf"

# qType: 0=个股研报, 1=行业研报, 2=策略研报
REPORT_TYPES = {
    "stock": 0,
    "industry": 1,
    "strategy": 2,
}


class EastmoneyReportSource(BaseSource):
    """东方财富研报采集器 — 获取研报元数据 + PDF全文"""

    def __init__(self):
        super().__init__("eastmoney_report")
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://data.eastmoney.com/",
        })

    def _build_and_save_report(self, item: dict) -> int | None:
        """解析单条 API 返回的研报条目，下载 PDF，写入 source_documents。

        Returns:
            新插入的 doc_id，去重跳过返回 None
        """
        title = (item.get("title") or "").strip()
        if not title:
            return None

        info_code = item.get("infoCode", "")
        org = item.get("orgSName", "")
        author = item.get("researcher", "") or item.get("author", "")
        stock_name = item.get("stockName", "")
        stock_code = item.get("stockCode", "")
        industry = item.get("industryName", "")
        pub_date = (item.get("publishDate") or "")[:10]
        rating = item.get("emRatingName", "")

        full_title = f"[{org}] {title}"
        existing = execute_cloud_query(
            "SELECT id FROM source_documents WHERE source='eastmoney_report' AND title=%s LIMIT 1",
            [full_title],
        )
        if existing:
            return None

        pdf_text = self._download_pdf_text(info_code) if info_code else ""

        meta_lines = [f"研报标题: {title}", f"机构: {org}"]
        if author:
            meta_lines.append(f"作者: {author}")
        if stock_name:
            meta_lines.append(f"个股: {stock_name}({stock_code})")
        if industry:
            meta_lines.append(f"行业: {industry}")
        if rating:
            meta_lines.append(f"评级: {rating}")
        meta_lines.append(f"发布日期: {pub_date}")

        if pdf_text:
            extracted_text = "\n".join(meta_lines) + f"\n\n=== 研报全文 ===\n{pdf_text}"
        else:
            extracted_text = "\n".join(meta_lines)

        oss_url = PDF_BASE.format(info_code) if info_code else ""
        extract_status = "extracted" if pdf_text else "pending"

        doc_id = execute_cloud_insert(
            """INSERT INTO source_documents
               (doc_type, file_type, title, author, publish_date,
                source, oss_url, extracted_text, extract_status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            ["report", "pdf", full_title, author, pub_date,
             "eastmoney_report", oss_url, extracted_text, extract_status],
        )
        if doc_id:
            self.increment_usage()
            logger.info(f"研报入库: {full_title} (PDF: {len(pdf_text)}字)")
        return doc_id

    def _download_pdf_text(self, info_code):
        """下载PDF并提取文本，失败返回空字符串"""
        try:
            import fitz
        except ImportError:
            logger.warning("PyMuPDF (fitz) 未安装，跳过PDF提取")
            return ""

        pdf_url = PDF_BASE.format(info_code)
        try:
            resp = self.session.get(pdf_url, timeout=20)
            if resp.status_code != 200 or len(resp.content) < 1000:
                return ""

            tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
            tmp.write(resp.content)
            tmp.close()

            try:
                doc = fitz.open(tmp.name)
                text = ""
                for page in doc:
                    text += page.get_text()
                doc.close()
                return text.strip()[:15000]  # 限制最大长度
            finally:
                os.unlink(tmp.name)

        except Exception as e:
            logger.debug(f"PDF提取失败 {info_code}: {e}")
            return ""

    def fetch(self, report_types=None, limit=10, days=3):
        """采集东方财富研报

        Args:
            report_types: 报告类型列表，如 ["stock", "industry"]，默认全部
            limit: 每种类型最大采集数
            days: 回溯天数
        """
        if not self.check_limit():
            logger.warning("东方财富研报已达限额")
            return 0

        from datetime import datetime, timedelta
        end_date = datetime.now().strftime("%Y-%m-%d")
        begin_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        if report_types is None:
            report_types = ["stock", "industry", "strategy"]

        count = 0
        for rtype in report_types:
            qt = REPORT_TYPES.get(rtype, 0)
            try:
                params = {
                    "industryCode": "*",
                    "pageSize": limit,
                    "industry": "*",
                    "rating": "*",
                    "ratingChange": "*",
                    "beginTime": begin_date,
                    "endTime": end_date,
                    "pageNo": 1,
                    "qType": qt,
                }
                resp = self.session.get(REPORT_API, params=params, timeout=15)
                if resp.status_code != 200:
                    logger.warning(f"东方财富研报API请求失败({rtype}): {resp.status_code}")
                    continue

                items = resp.json().get("data", [])
                for item in items[:limit]:
                    try:
                        if self._build_and_save_report(item):
                            count += 1
                    except Exception as e:
                        logger.warning(f"解析研报条目失败: {e}")
                        continue

            except Exception as e:
                logger.error(f"东方财富研报采集失败({rtype}): {e}")

        logger.info(f"东方财富研报采集: {count}条")
        return count

    def fetch_by_stock_codes(self, stock_codes: list[str], days: int = 730,
                             per_stock_limit: int = 50) -> dict:
        """按 stock_code 列表拉取近 N 天个股研报，写入 source_documents 并推入 extracted_texts。

        设计用于 daily intel 新出现的股票补拉历史研报。

        Args:
            stock_codes: A股代码列表，如 ["600519", "000858"]
            days: 回溯天数，默认 730（约两年）
            per_stock_limit: 每只股票最多入库条数（按最新时间排序取前 N 条）

        Returns:
            {"stocks": len, "fetched": N, "pushed": M, "skipped": K}
        """
        from datetime import datetime, timedelta
        from ingestion.source_extractor import push_to_extracted_texts_by_ids

        if not stock_codes:
            return {"stocks": 0, "fetched": 0, "pushed": 0, "skipped": 0}

        end_date = datetime.now().strftime("%Y-%m-%d")
        begin_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        total_fetched = 0
        total_skipped = 0
        new_doc_ids = []

        for stock_code in stock_codes:
            try:
                fetched = skipped = 0
                page = 1
                while fetched < per_stock_limit:
                    resp = self.session.get(REPORT_API, params={
                        "industryCode": "*", "pageSize": 20, "industry": "*",
                        "rating": "*", "ratingChange": "*",
                        "beginTime": begin_date, "endTime": end_date,
                        "pageNo": page, "qType": 0, "code": stock_code,
                    }, timeout=15)
                    if resp.status_code != 200:
                        break
                    data = resp.json()
                    items = data.get("data") or []
                    if not items:
                        break
                    for item in items:
                        if fetched >= per_stock_limit:
                            break
                        try:
                            doc_id = self._build_and_save_report(item)
                            if doc_id:
                                new_doc_ids.append(doc_id)
                                fetched += 1
                            else:
                                skipped += 1
                        except Exception as e:
                            logger.warning(f"fetch_by_stock_codes item失败 {stock_code}: {e}")
                    total_pages = data.get("TotalPage") or 1
                    if page >= total_pages:
                        break
                    page += 1

                total_fetched += fetched
                total_skipped += skipped
                logger.info(f"[EastmoneyReport] {stock_code}: 新入库 {fetched} 篇, 跳过(已有) {skipped} 篇")

            except Exception as e:
                logger.error(f"[EastmoneyReport] fetch_by_stock_codes {stock_code} 失败: {e}")

        # 推入 extracted_texts 管线
        push_result = {"pushed": 0, "skipped": 0, "failed": 0}
        if new_doc_ids:
            try:
                push_result = push_to_extracted_texts_by_ids(new_doc_ids)
            except Exception as e:
                logger.error(f"[EastmoneyReport] push_to_extracted_texts 失败: {e}")

        logger.info(
            f"[EastmoneyReport] fetch_by_stock_codes 完成: "
            f"股票={len(stock_codes)} 新入库={total_fetched} 已有={total_skipped} "
            f"推管线={push_result.get('pushed', 0)}"
        )
        return {
            "stocks": len(stock_codes),
            "fetched": total_fetched,
            "pushed": push_result.get("pushed", 0),
            "skipped": total_skipped,
        }

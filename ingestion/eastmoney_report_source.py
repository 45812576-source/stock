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
                        title = (item.get("title") or "").strip()
                        if not title:
                            continue

                        info_code = item.get("infoCode", "")
                        org = item.get("orgSName", "")
                        author = item.get("researcher", "") or item.get("author", "")
                        stock_name = item.get("stockName", "")
                        stock_code = item.get("stockCode", "")
                        industry = item.get("industryName", "")
                        pub_date = (item.get("publishDate") or "")[:10]
                        rating = item.get("emRatingName", "")

                        # 去重：按 title + source 查 source_documents
                        full_title = f"[{org}] {title}"
                        existing = execute_cloud_query(
                            "SELECT id FROM source_documents WHERE source='eastmoney_report' AND title=%s LIMIT 1",
                            [full_title],
                        )
                        if existing:
                            continue

                        # 提取PDF全文
                        pdf_text = ""
                        if info_code:
                            pdf_text = self._download_pdf_text(info_code)

                        # 构建提取文本（meta + PDF全文）
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

                        view_url = f"https://data.eastmoney.com/report/info/{info_code}.html"
                        extract_status = "extracted" if pdf_text else "pending"

                        doc_id = execute_cloud_insert(
                            """INSERT INTO source_documents
                               (doc_type, file_type, title, author, publish_date,
                                source, oss_url, extracted_text, extract_status)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            ["report", "pdf", full_title, author, pub_date,
                             "eastmoney_report", view_url, extracted_text, extract_status],
                        )
                        if doc_id:
                            count += 1
                            self.increment_usage()
                            logger.info(f"研报入库(source_documents): {full_title} (PDF: {len(pdf_text)}字)")

                    except Exception as e:
                        logger.warning(f"解析研报条目失败: {e}")
                        continue

            except Exception as e:
                logger.error(f"东方财富研报采集失败({rtype}): {e}")

        logger.info(f"东方财富研报采集: {count}条")
        return count

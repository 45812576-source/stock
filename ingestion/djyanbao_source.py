"""洞见研报采集 — 通过JSON API获取研报元数据 + 全文"""
import logging
import hashlib
import json
import re
import time

import requests

from ingestion.base_source import BaseSource

logger = logging.getLogger(__name__)

DJYANBAO_API = "https://www.djyanbao.com/api/report"
DJYANBAO_CONTENT_API = "https://www.djyanbao.com/api/report/{}/content"


class DjyanbaoSource(BaseSource):
    """洞见研报采集器 — 调用官方API获取研报列表+全文"""

    def __init__(self):
        super().__init__("djyanbao")
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Referer": "https://www.djyanbao.com/",
        })

    def _fetch_content(self, report_id):
        """通过content API获取研报全文"""
        try:
            resp = self.session.get(
                DJYANBAO_CONTENT_API.format(report_id), timeout=15)
            if resp.status_code != 200:
                return ""
            raw = resp.json().get("data", {}).get("content", "")
            if not raw:
                return ""
            # 清理HTML标签
            text = re.sub(r"<[^>]+>", "", raw)
            # 压缩连续空白行
            text = re.sub(r"\n{3,}", "\n\n", text).strip()
            return text[:15000]
        except Exception as e:
            logger.debug(f"获取研报全文失败 {report_id}: {e}")
            return ""

    def fetch(self, industry=None, limit=20, page=1):
        """采集洞见研报列表 + 全文

        Args:
            industry: 行业筛选（暂未对接）
            limit: 最大采集数（自动翻页，每页10条）
            page: 起始页码
        """
        if not self.check_limit():
            logger.warning("洞见研报已达限额")
            return 0

        count = 0
        current_page = page
        max_pages = (limit // 10) + 2  # 预留余量

        try:
            while count < limit and current_page < page + max_pages:
                resp = self.session.get(
                    DJYANBAO_API, params={"page": current_page}, timeout=15)
                if resp.status_code != 200:
                    logger.warning(f"洞见研报API请求失败: {resp.status_code}")
                    break

                data = resp.json().get("data", {})
                items = data.get("data", [])
                if not items:
                    break

                for item in items:
                    if count >= limit:
                        break
                    try:
                        title = (item.get("title") or "").strip()
                        if not title:
                            continue

                        org = item.get("orgName", "")
                        authors = item.get("authors", "")
                        pub_at = (item.get("publishAt") or "")[:10]
                        pages = item.get("pageTotal", 0)
                        pdf_url = item.get("fileUrl", "")
                        report_id = item.get("id", "")

                        ext_id = hashlib.md5(
                            f"djyanbao:{report_id}:{title}".encode()
                        ).hexdigest()

                        # 跳过已存在的
                        if self.is_duplicate(ext_id):
                            continue

                        # 获取全文
                        full_text = self._fetch_content(report_id)
                        time.sleep(0.3)  # 礼貌间隔

                        # 构建内容
                        meta_header = (
                            f"研报标题: {title}\n机构: {org}\n"
                            f"作者: {authors}\n发布日期: {pub_at}\n页数: {pages}"
                        )
                        if full_text and len(full_text) > 50:
                            content = meta_header + f"\n\n=== 研报全文 ===\n{full_text}"
                        else:
                            content = meta_header

                        view_url = (
                            f"https://www.djyanbao.com/report?id={report_id}"
                            if report_id else ""
                        )

                        saved = self.save_raw_item(
                            external_id=ext_id,
                            title=f"[{org}] {title}",
                            content=content,
                            url=view_url,
                            published_at=pub_at,
                            item_type="report",
                            meta_json=json.dumps({
                                "source": "djyanbao",
                                "org": org,
                                "authors": authors,
                                "pages": pages,
                                "pdf_url": pdf_url,
                                "has_full_text": bool(full_text),
                                "full_text_len": len(full_text),
                            }, ensure_ascii=False),
                        )
                        if saved:
                            count += 1
                            logger.info(
                                f"研报入库: [{org}] {title} "
                                f"(全文: {len(full_text)}字)"
                            )

                    except Exception as e:
                        logger.warning(f"解析研报条目失败: {e}")
                        continue

                current_page += 1
                time.sleep(0.5)

        except Exception as e:
            logger.error(f"洞见研报采集失败: {e}")

        logger.info(f"洞见研报采集: {count}条")
        return count

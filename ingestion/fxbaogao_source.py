"""发现报告采集 — 从首页SSR提取研报卡片"""
import logging
import hashlib
import json
import re

import requests
from bs4 import BeautifulSoup

from ingestion.base_source import BaseSource

logger = logging.getLogger(__name__)

FXBAOGAO_BASE = "https://www.fxbaogao.com"


class FxbaogaoSource(BaseSource):
    """发现报告采集器 — 解析首页SSR渲染的研报卡片"""

    def __init__(self):
        super().__init__("fxbaogao")
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "zh-CN,zh;q=0.9",
        })

    def fetch(self, industry=None, limit=20, page=1):
        """采集发现报告

        从首页SSR内容中提取研报卡片信息。
        """
        if not self.check_limit():
            logger.warning("发现报告已达限额")
            return 0

        count = 0
        try:
            resp = self.session.get(FXBAOGAO_BASE, timeout=15)
            resp.encoding = "utf-8"
            if resp.status_code != 200:
                logger.warning(f"发现报告请求失败: {resp.status_code}")
                return 0

            soup = BeautifulSoup(resp.text, "html.parser")

            # 找dataCardWrap中的研报链接（避免取到容器级别的混合文本）
            seen_ids = set()
            cards = soup.select("div[class*='dataCardWrap']")
            if not cards:
                # 兜底：直接找/pdf链接
                cards = [link.parent for link in soup.find_all("a", href=re.compile(r"/pdf\?id=\d+"))]

            for card in cards:
                if count >= limit:
                    break

                href = ""
                link = card.select_one("a[href*='/pdf?id=']") if card.name == "div" else card.find("a", href=re.compile(r"/pdf\?id="))
                if not link:
                    link = card if card.name == "a" and card.get("href", "").startswith("/pdf") else None
                if not link:
                    continue
                href = link.get("href", "")
                # 提取ID去重
                m = re.search(r"id=(\d+)", href)
                if not m:
                    continue
                report_id = m.group(1)
                if report_id in seen_ids:
                    continue
                seen_ids.add(report_id)

                # 提取标题
                title = link.get_text(strip=True)
                if not title or len(title) < 5:
                    continue

                # 提取元信息（券商|行业|日期）
                meta_el = card.select_one("div[class*='msg']") if card.name == "div" else None
                meta_text = meta_el.get_text(strip=True) if meta_el else ""

                # 解析元信息: "东方证券|信息技术2025-07-13"
                org = ""
                report_industry = ""
                pub_date = ""
                if meta_text:
                    parts = meta_text.split("|")
                    org = parts[0].strip() if parts else ""
                    if len(parts) > 1:
                        rest = parts[1].strip()
                        date_m = re.search(r"(\d{4}-\d{2}-\d{2})", rest)
                        if date_m:
                            pub_date = date_m.group(1)
                            report_industry = rest[:date_m.start()].strip()
                        else:
                            report_industry = rest
                elif "|" in title:
                    # 有时元信息混在标题里
                    parts = title.rsplit("|", 1)
                    if len(parts) == 2:
                        title = parts[0].strip()
                        rest = parts[1].strip()
                        date_m = re.search(r"(\d{4}-\d{2}-\d{2})", rest)
                        if date_m:
                            pub_date = date_m.group(1)

                full_url = f"{FXBAOGAO_BASE}{href}" if not href.startswith("http") else href
                content = f"研报标题: {title}\n机构: {org}\n行业: {report_industry}\n发布日期: {pub_date}"

                ext_id = hashlib.md5(f"fxbaogao:{report_id}:{title}".encode()).hexdigest()

                saved = self.save_raw_item(
                    external_id=ext_id,
                    title=f"[{org}] {title}" if org else title,
                    content=content,
                    url=full_url,
                    published_at=pub_date,
                    item_type="report",
                    meta_json=json.dumps({
                        "source": "fxbaogao",
                        "org": org,
                        "industry": report_industry,
                        "report_id": report_id,
                    }, ensure_ascii=False),
                )
                if saved:
                    count += 1

        except Exception as e:
            logger.error(f"发现报告采集失败: {e}")

        logger.info(f"发现报告采集: {count}条")
        return count

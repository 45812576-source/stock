"""智能新闻采集器 — 多维度覆盖宏观/行业/热点/跟踪个股"""
import logging
import hashlib
import json
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

from ingestion.base_source import BaseSource
from config import AKSHARE_DELAY

logger = logging.getLogger(__name__)


class JasperSource(BaseSource):
    """金融新闻智能采集器 — 5大维度全覆盖"""

    def __init__(self):
        super().__init__("jasper")

    def fetch(self, keywords=None, hours=24, sources=None):
        """智能采集财经新闻

        采集策略（按优先级）：
        1. 宏观快讯 — 财联社电报（全市场重大事件）
        2. 财经深度 — 财新主力新闻（深度分析）
        3. 热门个股 — 东方财富人气榜Top20的个股新闻
        4. 跟踪个股 — watchlist中感兴趣/已持仓的个股新闻
        5. 宏观政策 — CCTV新闻联播（政策风向）
        """
        if sources is None:
            sources = ["cls", "caixin", "hot_stocks", "watchlist", "cctv"]

        total = 0
        results = {}
        for src in sources:
            try:
                method = getattr(self, f"_fetch_{src}", None)
                if method:
                    count = method(keywords, hours)
                    total += count
                    results[src] = count
            except Exception as e:
                logger.error(f"采集{src}失败: {e}")
                results[src] = f"失败: {e}"

        logger.info(f"智能采集完成: 总计{total}条 | 明细: {results}")
        return total

    def _make_external_id(self, source, title, pub_time=""):
        raw = f"{source}:{title}:{pub_time}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _matches_keywords(self, text, keywords):
        if not keywords:
            return True
        text_lower = text.lower()
        return any(kw.lower() in text_lower for kw in keywords)

    def _get_session(self):
        """复用HTTP session提高连接效率"""
        if not hasattr(self, "_session"):
            self._session = requests.Session()
            self._session.trust_env = False  # 不走系统代理，直连国内新闻站
            self._session.headers.update({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "zh-CN,zh;q=0.9",
            })
        return self._session

    def _fetch_article_text(self, url):
        """从URL抓取新闻正文全文，失败返回None"""
        if not url or not url.startswith("http"):
            return None
        try:
            resp = self._get_session().get(url, timeout=8)
            resp.encoding = resp.apparent_encoding or "utf-8"
            if resp.status_code != 200:
                return None

            soup = BeautifulSoup(resp.text, "html.parser")
            # 移除干扰元素
            for tag in soup.select("script, style, nav, footer, header, .ad, .comment, aside"):
                tag.decompose()

            # 按优先级尝试常见正文容器（东方财富、证券时报、财新等）
            selectors = [
                "#ContentBody", ".newsContent", ".article-content", ".article-body",
                ".news-content", ".post-content", ".detail-content", ".content-article",
                "#article_content", ".txtinfos", ".contentwrap",
                "article .content", "article", "main .content",
            ]
            for sel in selectors:
                el = soup.select_one(sel)
                if el:
                    text = el.get_text(separator="\n", strip=True)
                    if len(text) > 100:  # 正文至少100字才算有效
                        return text[:8000]

            # 兜底：提取所有<p>标签文本
            paragraphs = [p.get_text(strip=True) for p in soup.find_all("p") if len(p.get_text(strip=True)) > 20]
            if paragraphs:
                text = "\n".join(paragraphs)
                if len(text) > 100:
                    return text[:8000]

            return None
        except Exception as e:
            logger.debug(f"全文抓取失败 {url}: {e}")
            return None

    # ========== 维度1: 财联社全球快讯 ==========
    def _fetch_cls(self, keywords, hours):
        """财联社电报 — 覆盖宏观政策、行业动态、公司公告等全市场重大事件"""
        import akshare as ak
        count = 0
        try:
            df = ak.stock_info_global_cls()
            if df is None or df.empty:
                return 0
            today = datetime.now().strftime("%Y-%m-%d")
            for _, row in df.iterrows():
                title = str(row.get("标题", "")).strip()
                content = str(row.get("内容", ""))
                pub_date = str(row.get("发布日期", ""))
                pub_time = str(row.get("发布时间", ""))

                # 用内容前50字作为标题（如果标题为空）
                if not title and content:
                    title = content[:50]
                if not title:
                    continue

                full_time = f"{pub_date} {pub_time}" if pub_date else ""
                if not self._matches_keywords(title + content, keywords):
                    continue

                ext_id = self._make_external_id("cls", title, full_time)
                saved = self.save_raw_item(
                    external_id=ext_id, title=title, content=content,
                    published_at=full_time, item_type="news",
                    meta_json=json.dumps({"source": "财联社", "category": "全球快讯"},
                                         ensure_ascii=False),
                )
                if saved:
                    count += 1
        except Exception as e:
            logger.warning(f"财联社采集失败: {e}")
        logger.info(f"财联社快讯: {count}条")
        return count

    # ========== 维度2: 财新主力新闻 ==========
    def _fetch_caixin(self, keywords, hours):
        """财新主力新闻 — 深度财经分析和行业报道"""
        import akshare as ak
        count = 0
        try:
            df = ak.stock_news_main_cx()
            if df is None or df.empty:
                return 0
            for _, row in df.iterrows():
                tag = str(row.get("tag", ""))
                summary = str(row.get("summary", "")).strip()
                url = str(row.get("url", ""))

                if not summary:
                    continue
                title = f"[{tag}] {summary[:60]}" if tag else summary[:60]

                if not self._matches_keywords(title + summary, keywords):
                    continue

                ext_id = self._make_external_id("caixin", summary[:100])

                # 尝试抓取全文
                content = summary
                if url:
                    full_text = self._fetch_article_text(url)
                    if full_text:
                        content = full_text

                saved = self.save_raw_item(
                    external_id=ext_id, title=title, content=content,
                    url=url, published_at=datetime.now().strftime("%Y-%m-%d"),
                    item_type="news",
                    meta_json=json.dumps({"source": "财新", "category": tag},
                                         ensure_ascii=False),
                )
                if saved:
                    count += 1
        except Exception as e:
            logger.warning(f"财新采集失败: {e}")
        logger.info(f"财新主力: {count}条")
        return count

    # ========== 维度3: 热门个股新闻 ==========
    def _fetch_hot_stocks(self, keywords, hours):
        """热门个股新闻 — 从人气榜Top20拉取个股新闻"""
        import akshare as ak
        count = 0
        cutoff = datetime.now() - timedelta(hours=hours)

        # 获取热门股票代码
        hot_codes = []
        try:
            df = ak.stock_hot_rank_em()
            if df is not None and not df.empty:
                for _, row in df.head(20).iterrows():
                    code = str(row.get("代码", ""))
                    # 格式: SZ002340 -> 002340
                    if code.startswith(("SZ", "SH")):
                        code = code[2:]
                    if code:
                        hot_codes.append(code)
        except Exception as e:
            logger.warning(f"获取热门股票失败: {e}")

        if not hot_codes:
            logger.info("无热门股票数据")
            return 0

        # 逐个拉取新闻
        for code in hot_codes:
            count += self._fetch_stock_news(code, cutoff, keywords, "热门股票")
            time.sleep(AKSHARE_DELAY)

        logger.info(f"热门个股新闻: {count}条 (覆盖{len(hot_codes)}只)")
        return count

    # ========== 维度4: 跟踪个股新闻 ==========
    def _fetch_watchlist(self, keywords, hours):
        """跟踪个股新闻 — watchlist中感兴趣/已持仓的股票"""
        from utils.db_utils import execute_query
        count = 0
        cutoff = datetime.now() - timedelta(hours=hours)

        # 从watchlist获取跟踪的股票
        watch_codes = []
        try:
            rows = execute_query(
                "SELECT stock_code FROM watchlist WHERE watch_type IN ('interested','holding')"
            )
            watch_codes = [r["stock_code"] for r in rows if r.get("stock_code")]
        except Exception:
            pass

        # 如果watchlist为空，用已有行情数据的股票兜底
        if not watch_codes:
            try:
                rows = execute_query(
                    "SELECT DISTINCT stock_code FROM stock_daily ORDER BY stock_code LIMIT 10"
                )
                watch_codes = [r["stock_code"] for r in rows]
            except Exception:
                pass

        if not watch_codes:
            logger.info("无跟踪个股")
            return 0

        for code in watch_codes:
            count += self._fetch_stock_news(code, cutoff, keywords, "跟踪个股")
            time.sleep(AKSHARE_DELAY)

        logger.info(f"跟踪个股新闻: {count}条 (覆盖{len(watch_codes)}只)")
        return count

    # ========== 维度5: CCTV新闻联播 ==========
    def _fetch_cctv(self, keywords, hours):
        """CCTV新闻联播 — 宏观政策风向标"""
        import akshare as ak
        count = 0
        try:
            df = ak.news_cctv(date=datetime.now().strftime("%Y%m%d"))
            if df is None or df.empty:
                return 0
            for _, row in df.iterrows():
                title = str(row.get("title", "")).strip()
                content = str(row.get("content", ""))
                if not title:
                    continue
                if not self._matches_keywords(title + content, keywords):
                    continue

                ext_id = self._make_external_id(
                    "cctv", title, datetime.now().strftime("%Y-%m-%d"))
                saved = self.save_raw_item(
                    external_id=ext_id, title=title, content=content,
                    published_at=datetime.now().strftime("%Y-%m-%d"),
                    item_type="news",
                    meta_json=json.dumps({"source": "CCTV新闻联播", "category": "宏观政策"},
                                         ensure_ascii=False),
                )
                if saved:
                    count += 1
        except Exception as e:
            logger.warning(f"CCTV采集失败: {e}")
        logger.info(f"CCTV新闻: {count}条")
        return count

    # ========== 通用: 个股新闻拉取 ==========
    def _fetch_stock_news(self, stock_code, cutoff, keywords, category):
        """拉取单只股票的新闻"""
        import akshare as ak
        count = 0
        try:
            df = ak.stock_news_em(symbol=stock_code)
            if df is None or df.empty:
                return 0
            for _, row in df.iterrows():
                title = str(row.get("新闻标题", "")).strip()
                content = str(row.get("新闻内容", ""))
                pub_time = str(row.get("发布时间", ""))
                url = str(row.get("新闻链接", ""))
                source_name = str(row.get("文章来源", "eastmoney"))

                if not title:
                    continue
                if pub_time:
                    try:
                        pub_dt = datetime.strptime(pub_time[:19], "%Y-%m-%d %H:%M:%S")
                        if pub_dt < cutoff:
                            continue
                    except ValueError:
                        pass
                if not self._matches_keywords(title + content, keywords):
                    continue

                # 尝试抓取全文
                if url:
                    full_text = self._fetch_article_text(url)
                    if full_text:
                        content = full_text

                ext_id = self._make_external_id("em", title, pub_time)
                saved = self.save_raw_item(
                    external_id=ext_id, title=title, content=content,
                    url=url, published_at=pub_time, item_type="news",
                    meta_json=json.dumps(
                        {"source": source_name, "stock": stock_code,
                         "category": category}, ensure_ascii=False),
                )
                if saved:
                    count += 1
        except Exception as e:
            logger.warning(f"采集{stock_code}新闻失败: {e}")
        return count

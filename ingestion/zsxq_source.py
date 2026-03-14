"""知识星球数据采集 — 抓取帖子（含文本/图片/PDF）存入 source_documents

流程: ZSXQ API → source_documents → source_extractor 提取 → raw_items → cleaning
"""
import time
import re
import hashlib
import logging
import requests
from datetime import datetime, date

from utils.db_utils import execute_cloud_query, execute_cloud_insert

logger = logging.getLogger(__name__)

_XML_TAG_RE = re.compile(r'<e\s[^>]*/>', re.IGNORECASE)
_URL_ENCODED_RE = re.compile(r'%[0-9A-Fa-f]{2}')

def _clean_zsxq_text(text: str) -> str:
    """清理 zsxq 文本中的 XML 标签（话题标签、外链等）"""
    if not text:
        return text
    # 把 <e type="hashtag" title="%23行业数据%23" /> 替换成 #行业数据#
    def _replace_tag(m):
        tag = m.group(0)
        # 提取 title 属性
        title_match = re.search(r'title="([^"]*)"', tag)
        if title_match:
            from urllib.parse import unquote
            return unquote(title_match.group(1))
        return ""
    text = _XML_TAG_RE.sub(_replace_tag, text)
    return text.strip()

ZSXQ_API_BASE = "https://api.zsxq.com/v2"
DEFAULT_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "origin": "https://wx.zsxq.com",
    "referer": "https://wx.zsxq.com/",
    "x-version": "2.89.0",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/144.0.0.0 Safari/537.36"
    ),
}


class ZsxqSource:
    """知识星球数据源 — 存入 source_documents"""

    def __init__(self, group_id, token):
        self.group_id = group_id
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.session.cookies.set("zsxq_access_token", token)

    # ── API 请求 ──

    def _fetch_page(self, count=20, end_time=None, retries=5):
        """获取一页帖子（含重试）"""
        url = f"{ZSXQ_API_BASE}/groups/{self.group_id}/topics"
        params = {"scope": "all", "count": min(count, 30)}
        if end_time:
            params["end_time"] = end_time
        for attempt in range(1, retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                if not data.get("succeeded"):
                    err = data.get("resp_data", {}).get("err_msg", "未知错误")
                    logger.warning(f"ZSXQ API 失败(尝试{attempt}/{retries}): {err}")
                    if attempt < retries:
                        time.sleep(3 * attempt)
                        continue
                    return []
                return data.get("resp_data", {}).get("topics", [])
            except requests.RequestException as e:
                logger.warning(f"ZSXQ 请求失败(尝试{attempt}/{retries}): {e}")
                if attempt < retries:
                    time.sleep(3 * attempt)
                    continue
                return []

    # ── 主采集入口 ──

    def fetch(self, max_pages=50, hours=None, start_date=None, end_date=None,
              progress_callback=None):
        """采集帖子并存入 source_documents

        Args:
            max_pages: 最大翻页数
            hours: 仅采集最近 N 小时（与 start_date/end_date 互斥，优先级低）
            start_date: 开始日期 str "YYYY-MM-DD" 或 date 对象（含）
            end_date: 结束日期 str "YYYY-MM-DD" 或 date 对象（含）
            progress_callback: fn(page, saved, msg)

        Returns:
            dict: {"saved": int, "skipped": int, "total_fetched": int}
        """
        start_ts = end_ts = None
        if start_date or end_date:
            if start_date:
                if isinstance(start_date, str):
                    start_date = date.fromisoformat(start_date)
                start_ts = datetime(start_date.year, start_date.month, start_date.day).timestamp()
            if end_date:
                if isinstance(end_date, str):
                    end_date = date.fromisoformat(end_date)
                end_ts = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59).timestamp()
        elif hours:
            start_ts = datetime.now().timestamp() - hours * 3600

        end_time = None
        total_fetched, saved, skipped = 0, 0, 0

        for page in range(1, max_pages + 1):
            topics = self._fetch_page(count=20, end_time=end_time)
            if not topics:
                break

            for topic in topics:
                total_fetched += 1
                create_time = topic.get("create_time", "")

                topic_ts = None
                if create_time:
                    try:
                        topic_ts = datetime.fromisoformat(
                            create_time.replace("+0800", "+08:00")
                        ).timestamp()
                    except (ValueError, AttributeError):
                        pass

                # 跳过 end_date 之后的帖子（API 按时间倒序，继续翻）
                if end_ts and topic_ts and topic_ts > end_ts:
                    end_time = create_time
                    continue

                # 早于 start_date，停止翻页
                if start_ts and topic_ts and topic_ts < start_ts:
                    logger.info("已到达时间窗口边界，停止翻页")
                    return _result(total_fetched, saved, skipped)

                ok = self._save_topic(topic)
                if ok:
                    saved += 1
                else:
                    skipped += 1

                end_time = create_time

            if progress_callback:
                progress_callback(page, saved,
                                  f"第{page}页，已保存{saved}条，跳过{skipped}条")

            logger.info(f"ZSXQ 第{page}页: 获取{len(topics)}条，"
                        f"累计保存{saved}，最早: {end_time}")
            time.sleep(1.5)

        return _result(total_fetched, saved, skipped)

    # ── 单条保存到 source_documents ──

    def _save_topic(self, topic):
        """解析帖子，按内容类型存入 source_documents"""
        topic_id = str(topic.get("topic_id") or topic.get("topic_uid", ""))
        if not topic_id:
            return False

        # 去重：查 topic 本身的非 PDF 记录（txt/image/mixed）
        ext_id = f"zsxq_{topic_id}"
        existing = execute_cloud_query(
            "SELECT id, extract_status FROM source_documents WHERE source=%s AND title LIKE %s AND file_type != 'pdf' LIMIT 1",
            ["zsxq", f"%{topic_id}%"],
        )
        if existing:
            if existing[0].get("extract_status") == "url_expired":
                # 源链接失效的条目：删除旧记录，重新采集
                logger.info(f"重新采集 url_expired 条目: topic_id={topic_id} old_id={existing[0]['id']}")
                execute_cloud_insert(
                    "DELETE FROM source_documents WHERE id=%s", [existing[0]["id"]]
                )
            else:
                # topic 本体已存在，但附件 PDF 可能需要刷新 oss_url，单独处理后返回
                text = self._extract_text(topic)
                files = self._extract_file_urls(topic)
                if files:
                    author = topic.get("talk", {}).get("owner", {}).get("name", "")
                    create_time = topic.get("create_time", "")
                    pub_date = None
                    if create_time:
                        try:
                            pub_date = datetime.fromisoformat(
                                create_time.replace("+0800", "+08:00")
                            ).strftime("%Y-%m-%d")
                        except (ValueError, AttributeError):
                            pass
                    for i, f in enumerate(files):
                        f_ext_id = f"{ext_id}_file{i}"
                        f_doc_id = abs(int(hashlib.md5(f_ext_id.encode()).hexdigest()[:15], 16))
                        f_name = f.get("name", "")
                        f_url = f.get("url", "")
                        f_type = self._guess_file_type(f_name)
                        if f_type == "pdf":
                            from config.doc_types import classify_doc_type
                            f_doc_type = classify_doc_type(f_name, text or "")
                            self._insert_and_extract_pdf(
                                doc_id=f_doc_id, doc_type=f_doc_type, file_type=f_type,
                                title=f"[ZSXQ:{topic_id}] {f_name}", author=author,
                                publish_date=pub_date, oss_url=f_url, context_text=text or "",
                            )
                return False

        # 提取内容
        text = self._extract_text(topic)
        images = self._extract_image_urls(topic)
        files = self._extract_file_urls(topic)
        author = topic.get("talk", {}).get("owner", {}).get("name", "")
        create_time = topic.get("create_time", "")

        # 生成 ID（用 hash 避免冲突）
        doc_id = abs(int(hashlib.md5(ext_id.encode()).hexdigest()[:15], 16))

        # 判断 publish_date
        pub_date = None
        if create_time:
            try:
                pub_date = datetime.fromisoformat(
                    create_time.replace("+0800", "+08:00")
                ).strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                pass

        # 判断 file_type 和组装 text_content / oss_url
        has_text = bool(text and text.strip())
        has_images = bool(images)
        has_files = bool(files)

        # PDF/文件附件 → 每个文件单独一条记录
        if has_files:
            for i, f in enumerate(files):
                f_ext_id = f"{ext_id}_file{i}"
                f_doc_id = abs(int(hashlib.md5(f_ext_id.encode()).hexdigest()[:15], 16))
                f_name = f.get("name", "")
                f_url = f.get("url", "")
                f_type = self._guess_file_type(f_name)

                from config.doc_types import classify_doc_type
                f_doc_type = classify_doc_type(f_name, text or "")

                # 音频文件：URL 带七牛 token 有效期约7天，必须立即转录
                if f_type in ("audio", "mp3"):
                    self._insert_and_extract_audio(
                        doc_id=f_doc_id,
                        doc_type=f_doc_type,
                        file_type=f_type,
                        title=f"[ZSXQ:{topic_id}] {f_name}",
                        author=author,
                        publish_date=pub_date,
                        audio_url=f_url,
                        context_text=text or "",
                    )
                else:
                    # PDF 等文件：URL 带七牛 token 约7天过期，立即提取
                    if f_type == "pdf":
                        self._insert_and_extract_pdf(
                            doc_id=f_doc_id,
                            doc_type=f_doc_type,
                            file_type=f_type,
                            title=f"[ZSXQ:{topic_id}] {f_name}",
                            author=author,
                            publish_date=pub_date,
                            oss_url=f_url,
                            context_text=text or "",
                        )
                    else:
                        self._insert_doc(
                            doc_id=f_doc_id,
                            doc_type=f_doc_type,
                            file_type=f_type,
                            title=f"[ZSXQ:{topic_id}] {f_name}",
                            author=author,
                            publish_date=pub_date,
                            oss_url=f_url,
                            text_content=text or "",
                        )

        # 图片 + 文本 → mixed / image
        if has_images:
            # 组装 [图片N] url 格式（与 source_extractor 兼容）
            img_lines = []
            for idx, url in enumerate(images, 1):
                img_lines.append(f"[图片{idx}] {url}")
            img_block = "\n".join(img_lines)

            if has_text:
                file_type = "mixed"
                content = f"{text}\n\n{img_block}"
            else:
                file_type = "image"
                content = img_block

            title = text[:50].replace("\n", " ") + "..." if has_text and len(text) > 50 else (text or "图片帖")
            from config.doc_types import classify_doc_type
            img_doc_type = classify_doc_type(title, text or "")
            self._insert_doc(
                doc_id=doc_id,
                doc_type=img_doc_type,
                file_type=file_type,
                title=f"[ZSXQ:{topic_id}] {title}",
                author=author,
                publish_date=pub_date,
                text_content=content,
            )
            return True

        # 纯文本
        if has_text:
            title = text[:50].replace("\n", " ")
            if len(text) > 50:
                title += "..."
            from config.doc_types import classify_doc_type
            txt_doc_type = classify_doc_type(title, text)
            self._insert_doc(
                doc_id=doc_id,
                doc_type=txt_doc_type,
                file_type="txt",
                title=f"[ZSXQ:{topic_id}] {title}",
                author=author,
                publish_date=pub_date,
                text_content=text,
            )
            return True

        return False

    def _insert_doc(self, doc_id, doc_type, file_type, title, author,
                    publish_date, text_content="", oss_url=None):
        """插入 source_documents"""
        try:
            execute_cloud_insert(
                """INSERT IGNORE INTO source_documents
                   (id, doc_type, file_type, title, author, publish_date,
                    source, oss_url, text_content, extract_status)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                [doc_id, doc_type, file_type, title, author, publish_date,
                 "zsxq", oss_url, text_content, "pending"],
            )
        except Exception as e:
            logger.warning(f"写入 source_documents 失败: {e}")

    def _insert_and_extract_audio(self, doc_id, doc_type, file_type, title, author,
                                   publish_date, audio_url, context_text=""):
        """插入音频文档并立即转录（URL 带七牛 token 约7天过期，必须采集时处理）"""
        # 先以 pending 状态插入，URL 存 oss_url，context_text 存 text_content（与旧记录格式一致）
        try:
            execute_cloud_insert(
                """INSERT IGNORE INTO source_documents
                   (id, doc_type, file_type, title, author, publish_date,
                    source, oss_url, text_content, extract_status)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                [doc_id, doc_type, file_type, title, author, publish_date,
                 "zsxq", audio_url, context_text, "pending"],
            )
        except Exception as e:
            logger.warning(f"写入音频 source_documents 失败: {e}")
            return

        # 立即转录
        try:
            from ingestion.source_extractor import _extract_mp3, _extract_audio, _semantic_clean
            from config.doc_types import classify_doc_type
            row = {
                "id": doc_id, "file_type": file_type,
                "text_content": context_text, "oss_url": audio_url,
                "title": title,
            }
            if file_type == "mp3":
                raw_text = _extract_mp3(row)
            else:
                raw_text = _extract_audio(row)

            if raw_text and len(raw_text.strip()) >= 20:
                cleaned = _semantic_clean(raw_text, "audio", doc_id, needs_understanding=False)
            else:
                cleaned = raw_text or ""

            new_doc_type = classify_doc_type(title, cleaned[:200]) if cleaned else doc_type
            execute_cloud_insert(
                "UPDATE source_documents SET extracted_text=%s, extract_status='extracted', doc_type=%s WHERE id=%s",
                [cleaned, new_doc_type, doc_id],
            )
            logger.info(f"音频立即转录完成 doc_id={doc_id} title={title[:40]} ({len(cleaned)}字)")
        except Exception as e:
            logger.warning(f"音频立即转录失败 doc_id={doc_id}: {e}")

    def _insert_and_extract_pdf(self, doc_id, doc_type, file_type, title, author,
                                publish_date, oss_url, context_text=""):
        """插入 PDF 文档并立即提取（URL 带七牛 token 约7天过期，必须采集时处理）

        已有提取记录时只刷新 oss_url，不重复提取。
        """
        # 检查是否已有提取记录
        existing = execute_cloud_query(
            "SELECT id, extracted_text FROM source_documents WHERE id=%s LIMIT 1",
            [doc_id],
        )
        if existing and existing[0].get("extracted_text"):
            # 已提取过，只刷新 oss_url
            execute_cloud_insert(
                "UPDATE source_documents SET oss_url=%s WHERE id=%s",
                [oss_url, doc_id],
            )
            logger.info(f"PDF 已有提取记录，刷新 oss_url doc_id={doc_id} title={title[:40]}")
            return

        # 新文档或尚未提取：插入后立即提取
        try:
            execute_cloud_insert(
                """INSERT IGNORE INTO source_documents
                   (id, doc_type, file_type, title, author, publish_date,
                    source, oss_url, text_content, extract_status)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                [doc_id, doc_type, file_type, title, author, publish_date,
                 "zsxq", oss_url, context_text, "pending"],
            )
        except Exception as e:
            logger.warning(f"写入 PDF source_documents 失败: {e}")
            return

        try:
            from ingestion.source_extractor import _extract_pdf_with_meta, _semantic_clean
            from config.doc_types import classify_doc_type
            row = {
                "id": doc_id, "file_type": file_type,
                "text_content": context_text, "oss_url": oss_url,
                "title": title,
            }
            raw_text, needs_understanding = _extract_pdf_with_meta(row)
            if raw_text and len(raw_text.strip()) >= 20:
                cleaned = _semantic_clean(raw_text, "pdf", doc_id, needs_understanding=needs_understanding)
            else:
                cleaned = raw_text or ""

            new_doc_type = classify_doc_type(title, cleaned[:200]) if cleaned else doc_type
            execute_cloud_insert(
                "UPDATE source_documents SET extracted_text=%s, extract_status='extracted', doc_type=%s WHERE id=%s",
                [cleaned, new_doc_type, doc_id],
            )
            logger.info(f"PDF 立即提取完成 doc_id={doc_id} title={title[:40]} ({len(cleaned)}字)")
        except Exception as e:
            logger.warning(f"PDF 立即提取失败 doc_id={doc_id}: {e}")

    # ── 内容提取 ──

    def _extract_text(self, topic):
        """从 topic 中提取文本"""
        parts = []
        topic_type = topic.get("type", "")

        if topic_type == "talk":
            talk = topic.get("talk", {})
            if talk.get("text"):
                parts.append(talk["text"])

        elif topic_type == "q&a":
            question = topic.get("question", {})
            if question.get("text"):
                parts.append(f"【提问】{question['text']}")
            answers = topic.get("answered_questions", topic.get("answer", {}))
            if isinstance(answers, dict) and answers.get("text"):
                parts.append(f"【回答】{answers['text']}")
            elif isinstance(answers, list):
                for a in answers:
                    if a.get("text"):
                        parts.append(f"【回答】{a['text']}")

        elif topic_type == "task":
            task = topic.get("task", {})
            if task.get("text"):
                parts.append(f"【作业】{task['text']}")

        if not parts and topic.get("text"):
            parts.append(topic["text"])

        return _clean_zsxq_text("\n\n".join(parts))

    def _extract_image_urls(self, topic):
        """提取图片 URL 列表"""
        urls = []
        talk = topic.get("talk", {})
        for img in talk.get("images", []):
            url = (img.get("large", {}).get("url")
                   or img.get("original", {}).get("url"))
            if url:
                urls.append(url)
        return urls

    def _extract_file_urls(self, topic):
        """提取文件附件信息 [{name, url, size}]

        文件列表里没有直接的 download_url，需要额外调用
        GET /v2/files/{file_id}/download_url 获取带签名的临时链接。
        """
        files = []
        talk = topic.get("talk", {})
        for f in talk.get("files", []):
            file_id = f.get("file_id")
            name = f.get("name", "")
            if not file_id:
                continue
            try:
                resp = self.session.get(
                    f"{ZSXQ_API_BASE}/files/{file_id}/download_url",
                    timeout=10,
                )
                data = resp.json()
                if data.get("succeeded"):
                    url = data.get("resp_data", {}).get("download_url", "")
                    if url:
                        files.append({"name": name, "url": url, "size": f.get("size", 0)})
                        continue
            except Exception as e:
                logger.warning(f"获取文件下载链接失败 file_id={file_id}: {e}")
            # 降级：存 file_id 占位，后续可重试
            files.append({"name": name, "url": "", "size": f.get("size", 0), "file_id": str(file_id)})
        return files

    @staticmethod
    def _guess_file_type(filename):
        """根据文件名猜测 file_type"""
        name = filename.lower()
        if name.endswith(".pdf"):
            return "pdf"
        elif name.endswith((".xlsx", ".xls")):
            return "xlsx"
        elif name.endswith((".txt", ".md", ".csv")):
            return "txt"
        elif name.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
            return "image"
        elif name.endswith((".mp3", ".wav", ".m4a")):
            return "audio"
        else:
            return "txt"  # 默认当文本处理


def _result(fetched, saved, skipped):
    return {"total_fetched": fetched, "saved": saved, "skipped": skipped}


# ── 便捷函数 ──

def fetch_zsxq_data(group_ids=None, token=None,
                    hours=None, start_date=None, end_date=None,
                    max_pages=50, progress_callback=None):
    """一键采集知识星球数据到 source_documents，支持多星球

    Args:
        group_ids: 星球群组ID，str 或 list[str]，默认从 config 读取
        token: zsxq_access_token，默认从 config 读取
        hours: 仅采集最近N小时（与 start_date/end_date 互斥）
        start_date: 开始日期 "YYYY-MM-DD"（含）
        end_date: 结束日期 "YYYY-MM-DD"（含）
        max_pages: 每个星球最大翻页数
        progress_callback: fn(page, saved, msg)

    Returns:
        dict: {"saved": int, "skipped": int, "total_fetched": int}
    """
    from config import ZSXQ_GROUP_IDS, ZSXQ_COOKIE
    if group_ids is None:
        group_ids = ZSXQ_GROUP_IDS
    if isinstance(group_ids, str):
        group_ids = [group_ids]
    tk = token or ZSXQ_COOKIE
    if not tk:
        raise ValueError("未配置 ZSXQ token，请在 config.py 或环境变量中设置 ZSXQ_COOKIE")

    total = {"saved": 0, "skipped": 0, "total_fetched": 0}
    for gid in group_ids:
        source = ZsxqSource(gid, tk)
        result = source.fetch(
            max_pages=max_pages,
            hours=hours,
            start_date=start_date,
            end_date=end_date,
            progress_callback=progress_callback,
        )
        for k in total:
            total[k] += result.get(k, 0)
    return total

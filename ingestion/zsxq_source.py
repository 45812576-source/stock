"""知识星球数据采集 — 自动抓取帖子并存入raw_items"""
import time
import json
import hashlib
import logging
import requests
from datetime import datetime
from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)

# 知识星球API基础配置
ZSXQ_API_BASE = "https://api.zsxq.com/v2"
DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Origin": "https://wx.zsxq.com",
    "Referer": "https://wx.zsxq.com/",
}


class ZsxqSource:
    """知识星球数据源"""

    def __init__(self, group_id, cookie, source_name="zsxq"):
        """
        Args:
            group_id: 星球群组ID（URL中的数字）
            cookie: 浏览器登录cookie字符串
            source_name: 数据源名称
        """
        self.group_id = group_id
        self.source_name = source_name
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.session.headers["Cookie"] = cookie

    def fetch_topics(self, count=20, end_time=None):
        """获取帖子列表

        Args:
            count: 每页数量（最大30）
            end_time: 分页游标，上一页最后一条的create_time
        """
        url = f"{ZSXQ_API_BASE}/groups/{self.group_id}/topics"
        params = {"scope": "all", "count": min(count, 30)}
        if end_time:
            params["end_time"] = end_time

        try:
            resp = self.session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if not data.get("succeeded"):
                logger.error(f"ZSXQ API失败: {data.get('resp_data', {}).get('err_msg', '未知错误')}")
                return []

            topics = data.get("resp_data", {}).get("topics", [])
            return topics

        except requests.RequestException as e:
            logger.error(f"ZSXQ请求失败: {e}")
            return []

    def fetch_all_recent(self, max_pages=5, hours=24):
        """获取最近N小时的所有帖子

        Args:
            max_pages: 最大翻页数
            hours: 时间窗口（小时）
        """
        all_topics = []
        end_time = None
        cutoff = datetime.now().timestamp() - hours * 3600

        for page in range(max_pages):
            topics = self.fetch_topics(count=20, end_time=end_time)
            if not topics:
                break

            for topic in topics:
                create_time = topic.get("create_time", "")
                # 知识星球时间格式: "2025-01-15T08:30:00.000+0800"
                try:
                    dt = datetime.fromisoformat(create_time.replace("+0800", "+08:00"))
                    if dt.timestamp() < cutoff:
                        return all_topics  # 超出时间窗口
                except (ValueError, AttributeError):
                    pass

                all_topics.append(topic)
                end_time = create_time

            time.sleep(1)  # 限流

        return all_topics

    def save_topics_to_db(self, topics):
        """将帖子保存到raw_items表

        Returns:
            dict: {"saved": int, "skipped": int}
        """
        saved, skipped = 0, 0

        for topic in topics:
            topic_id = str(topic.get("topic_id", ""))
            content = self._extract_content(topic)
            if not content:
                skipped += 1
                continue

            # 去重检查
            content_hash = hashlib.md5(content.encode()).hexdigest()
            existing = execute_query(
                "SELECT id FROM raw_items WHERE content_hash=?", [content_hash]
            )
            if existing:
                skipped += 1
                continue

            # 提取元数据
            author = topic.get("talk", {}).get("owner", {}).get("name", "")
            create_time = topic.get("create_time", "")
            topic_type = topic.get("type", "talk")
            images = self._extract_images(topic)

            title = content[:50].replace("\n", " ")
            if len(content) > 50:
                title += "..."

            execute_insert(
                """INSERT INTO raw_items
                   (source_name, source_url, title, content, content_hash,
                    published_at, meta_json, processing_status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')""",
                [self.source_name,
                 f"https://wx.zsxq.com/group/{self.group_id}",
                 title, content, content_hash, create_time,
                 json.dumps({
                     "topic_id": topic_id,
                     "author": author,
                     "type": topic_type,
                     "images": images,
                     "group_id": self.group_id,
                 }, ensure_ascii=False)],
            )
            saved += 1

        logger.info(f"ZSXQ保存: {saved}条新帖, {skipped}条跳过")
        return {"saved": saved, "skipped": skipped}

    def _extract_content(self, topic):
        """从topic中提取文本内容"""
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

        # 也检查顶层text字段
        if not parts and topic.get("text"):
            parts.append(topic["text"])

        return "\n\n".join(parts)

    def _extract_images(self, topic):
        """提取图片URL列表"""
        images = []
        talk = topic.get("talk", {})
        for img in talk.get("images", []):
            if img.get("large", {}).get("url"):
                images.append(img["large"]["url"])
            elif img.get("original", {}).get("url"):
                images.append(img["original"]["url"])
        return images


def fetch_zsxq_data(group_id, cookie, hours=24, max_pages=5):
    """便捷函数：一键采集知识星球数据

    Args:
        group_id: 星球群组ID
        cookie: 登录cookie
        hours: 采集最近N小时
        max_pages: 最大翻页数

    Returns:
        dict: {"saved": int, "skipped": int, "total_fetched": int}
    """
    source = ZsxqSource(group_id, cookie)
    topics = source.fetch_all_recent(max_pages=max_pages, hours=hours)
    result = source.save_topics_to_db(topics)
    result["total_fetched"] = len(topics)
    return result

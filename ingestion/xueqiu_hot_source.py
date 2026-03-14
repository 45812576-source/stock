"""雪球热度数据采集 — 讨论/关注/交易排行榜"""
import logging
import hashlib
import json
from datetime import datetime

from ingestion.base_source import BaseSource

logger = logging.getLogger(__name__)


class XueqiuHotSource(BaseSource):
    """雪球热度排行采集器（无需token）"""

    def __init__(self):
        super().__init__("xueqiu_hot")

    def fetch(self, rankings=None, limit=100):
        """采集雪球热度排行

        Args:
            rankings: 排行类型列表，可选 ["tweet","follow","deal"]
            limit: 每个排行最大采集数
        """
        if not self.check_limit():
            logger.warning("雪球热度已达限额")
            return 0

        import akshare as ak

        if rankings is None:
            rankings = ["tweet", "follow", "deal"]

        today = datetime.now().strftime("%Y-%m-%d")
        count = 0

        for rtype in rankings:
            try:
                if rtype == "tweet":
                    df = ak.stock_hot_tweet_xq(symbol="最热门")
                    label = "讨论排行"
                elif rtype == "follow":
                    df = ak.stock_hot_follow_xq(symbol="最热门")
                    label = "关注排行"
                elif rtype == "deal":
                    df = ak.stock_hot_deal_xq(symbol="最热门")
                    label = "交易排行"
                else:
                    continue

                if df is None or df.empty:
                    continue

                for rank, (_, row) in enumerate(df.head(limit).iterrows(), 1):
                    raw_code = str(row.get("股票代码", ""))
                    # 去掉 SH/SZ 前缀
                    code = raw_code.replace("SH", "").replace("SZ", "")
                    name = str(row.get("股票简称", ""))
                    heat = row.get("关注", 0)
                    price = row.get("最新价", 0)

                    ext_id = hashlib.md5(
                        f"xq_hot:{rtype}:{code}:{today}".encode()
                    ).hexdigest()

                    title = f"[雪球{label}] #{rank} {name}({code})"
                    content = (
                        f"排行类型: {label}\n"
                        f"排名: {rank}\n"
                        f"股票: {name}({code})\n"
                        f"热度值: {heat}\n"
                        f"最新价: {price}"
                    )

                    saved = self.save_raw_item(
                        external_id=ext_id,
                        title=title,
                        content=content,
                        published_at=today,
                        item_type="sentiment",
                        meta_json=json.dumps({
                            "source": "xueqiu",
                            "ranking_type": rtype,
                            "rank": rank,
                            "stock_code": code,
                            "stock_name": name,
                            "heat": float(heat) if heat else 0,
                            "price": float(price) if price else 0,
                            "date": today,
                        }, ensure_ascii=False),
                    )
                    if saved:
                        count += 1

                logger.info(f"雪球{label}: {len(df)}条")

            except Exception as e:
                logger.warning(f"雪球{rtype}排行采集失败: {e}")
                continue

        logger.info(f"雪球热度采集完成: {count}条")
        return count

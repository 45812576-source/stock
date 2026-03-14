"""业绩预告/快报采集 — 通过 AKShare 获取全市场业绩数据"""
import logging
import hashlib
from datetime import datetime

from ingestion.base_source import BaseSource

logger = logging.getLogger(__name__)


class EarningsSource(BaseSource):
    """业绩预告+业绩快报采集器"""

    def __init__(self):
        super().__init__("earnings")

    def fetch(self, period=None, limit=500, watchlist_only=True):
        """采集业绩预告和业绩快报

        Args:
            period: 报告期，如 "20241231"。None 则自动推断最近报告期
            limit: 最大采集数
            watchlist_only: True 只采集 watchlist 中的股票
        """
        if not self.check_limit():
            logger.warning("业绩采集已达限额")
            return 0

        if period is None:
            period = self._guess_period()

        watch_codes = set()
        if watchlist_only:
            watch_codes = set(self._get_watchlist_codes())
            if not watch_codes:
                watchlist_only = False  # fallback to all

        count = 0
        count += self._fetch_yjyg(period, limit, watch_codes, watchlist_only)
        count += self._fetch_yjkb(period, limit, watch_codes, watchlist_only)

        logger.info(f"业绩采集完成: {count}条 (期间={period})")
        return count

    def _fetch_yjyg(self, period, limit, watch_codes, watchlist_only):
        """采集业绩预告"""
        import akshare as ak
        try:
            df = ak.stock_yjyg_em(date=period)
        except Exception as e:
            logger.warning(f"业绩预告获取失败: {e}")
            return 0

        if df is None or df.empty:
            return 0

        count = 0
        for _, row in df.iterrows():
            code = str(row.get("股票代码", "")).strip()
            if watchlist_only and code not in watch_codes:
                continue

            name = str(row.get("股票简称", ""))
            forecast_type = str(row.get("预告类型", ""))
            change_reason = str(row.get("业绩变动原因", ""))
            change_pct = row.get("业绩变动幅度")
            pub_date = str(row.get("公告日期", ""))[:10]

            ext_id = hashlib.md5(
                f"yjyg:{code}:{period}:{forecast_type}".encode()
            ).hexdigest()

            title = f"[{name}] 业绩预告: {forecast_type}"
            content_lines = [
                f"股票: {name}({code})",
                f"预告类型: {forecast_type}",
                f"业绩变动: {row.get('业绩变动', '')}",
            ]
            if change_pct and str(change_pct) != "nan":
                content_lines.append(f"变动幅度: {change_pct}%")
            if change_reason:
                content_lines.append(f"变动原因: {change_reason}")

            saved = self.save_source_doc(
                dedup_key=ext_id,
                title=title,
                extracted_text="\n".join(content_lines),
                doc_type="earnings",
                publish_date=pub_date,
            )
            if saved:
                count += 1
                if count >= limit:
                    break

        logger.info(f"业绩预告: {count}条")
        return count

    def _fetch_yjkb(self, period, limit, watch_codes, watchlist_only):
        """采集业绩快报"""
        import akshare as ak
        try:
            df = ak.stock_yjkb_em(date=period)
        except Exception as e:
            logger.warning(f"业绩快报获取失败: {e}")
            return 0

        if df is None or df.empty:
            return 0

        count = 0
        for _, row in df.iterrows():
            code = str(row.get("股票代码", "")).strip()
            if watchlist_only and code not in watch_codes:
                continue

            name = str(row.get("股票简称", ""))
            revenue = row.get("营业收入-营业收入")
            profit = row.get("净利润-净利润")
            profit_yoy = row.get("净利润-同比增长")
            industry = str(row.get("所处行业", ""))
            pub_date = str(row.get("公告日期", ""))[:10]

            ext_id = hashlib.md5(
                f"yjkb:{code}:{period}".encode()
            ).hexdigest()

            title = f"[{name}] 业绩快报"
            content_lines = [
                f"股票: {name}({code})",
                f"行业: {industry}",
            ]
            if revenue and str(revenue) != "nan":
                content_lines.append(f"营业收入: {revenue/1e8:.2f}亿")
            if profit and str(profit) != "nan":
                content_lines.append(f"净利润: {profit/1e8:.2f}亿")
            if profit_yoy and str(profit_yoy) != "nan":
                content_lines.append(f"净利润同比: {profit_yoy:.2f}%")

            saved = self.save_source_doc(
                dedup_key=ext_id,
                title=title,
                extracted_text="\n".join(content_lines),
                doc_type="earnings",
                publish_date=pub_date,
            )
            if saved:
                count += 1
                if count >= limit:
                    break

        logger.info(f"业绩快报: {count}条")
        return count

    def _guess_period(self):
        """推断最近的报告期"""
        now = datetime.now()
        m = now.month
        if m <= 4:
            return f"{now.year - 1}1231"
        elif m <= 8:
            return f"{now.year}0630"
        elif m <= 10:
            return f"{now.year}0930"
        else:
            return f"{now.year}1231"

    def _get_watchlist_codes(self):
        """从 watchlist 获取关注的股票代码"""
        from utils.db_utils import execute_query
        rows = execute_query(
            "SELECT stock_code FROM watchlist WHERE status IN ('watching', 'holding')"
        )
        return [r["stock_code"] for r in rows] if rows else []

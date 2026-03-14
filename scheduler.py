"""定时任务调度 — KG自动构建 + 推理

规则:
- 每天 06:00 和 20:00 自动执行 KG 增量构建（structured模式，不调Claude）
- 每次构建完成后自动运行一次推理引擎
- 手动触发构建完成后也自动跟一次推理
"""
import logging
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone="Asia/Shanghai")

# ── 状态追踪 ──────────────────────────────────────────────────

def _ensure_state_table():
    execute_insert(
        """CREATE TABLE IF NOT EXISTS scheduler_state (
            `key` VARCHAR(255) PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )""", []
    )

def _get_state(key, default=None):
    _ensure_state_table()
    rows = execute_query("SELECT value FROM scheduler_state WHERE `key`=%s", [key])
    return rows[0]["value"] if rows else default

def _set_state(key, value):
    _ensure_state_table()
    execute_insert(
        """INSERT INTO scheduler_state (`key`, value, updated_at)
           VALUES (%s, %s, CURRENT_TIMESTAMP)
           ON DUPLICATE KEY UPDATE value=VALUES(value), updated_at=CURRENT_TIMESTAMP""",
        [key, str(value)],
    )


# ── KG 构建任务 ──────────────────────────────────────────────

def run_kg_update():
    """增量构建KG：只处理上次构建后新增的 cleaned_items"""
    since = _get_state("kg_last_auto_update")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"[Scheduler] KG自动构建开始, since={since}")

    try:
        from knowledge_graph.kg_updater import update_from_cleaned_items
        result = update_from_cleaned_items(since_date=since, use_claude=False)
        _set_state("kg_last_auto_update", now_str)
        logger.info(f"[Scheduler] KG构建完成: {result}")

        # 构建完成 → 自动跑推理
        run_inference_after_build()
        return result
    except Exception as e:
        logger.exception(f"[Scheduler] KG自动构建失败: {e}")
        return {"error": str(e)}


def run_inference_after_build():
    """构建完成后自动运行推理引擎（4条规则全跑）"""
    logger.info("[Scheduler] 自动推理开始")
    try:
        from routers.knowledge_graph import _run_inference_sync
        discovered = _run_inference_sync(rule_type="all", auto_accept=True)
        logger.info(f"[Scheduler] 自动推理完成, 发现 {len(discovered)} 条关系, 已自动采纳高置信度结果")
        return discovered
    except Exception as e:
        logger.exception(f"[Scheduler] 自动推理失败: {e}")
        return []


# ── 宏观数据采集任务 ──────────────────────────────────────────

def run_macro_daily():
    """日度宏观数据采集：Shibor/融资余额/全A PE/陆股通/海外ETF + 同步到本地"""
    logger.info("[Scheduler] 宏观日度采集开始")
    try:
        from ingestion.macro_fetcher import fetch_all_macro
        from utils.db_utils import sync_macro_to_local
        fetch_result = fetch_all_macro()
        sync_result = sync_macro_to_local()
        logger.info(f"[Scheduler] 宏观日度采集完成: fetch={fetch_result}, sync={sync_result}")
        return {"fetch": fetch_result, "sync": sync_result}
    except Exception as e:
        logger.exception(f"[Scheduler] 宏观日度采集失败: {e}")
        return {"error": str(e)}


def run_macro_monthly():
    """月度宏观数据采集：M2/社融/PMI + 同步到本地"""
    logger.info("[Scheduler] 宏观月度采集开始")
    try:
        from ingestion.macro_fetcher import fetch_all_macro_monthly
        from utils.db_utils import sync_macro_to_local
        fetch_result = fetch_all_macro_monthly()
        sync_result = sync_macro_to_local()
        logger.info(f"[Scheduler] 宏观月度采集完成: fetch={fetch_result}, sync={sync_result}")
        return {"fetch": fetch_result, "sync": sync_result}
    except Exception as e:
        logger.exception(f"[Scheduler] 宏观月度采集失败: {e}")
        return {"error": str(e)}


def run_market_data_monthly():
    """月度市场增量数据同步：insider_trading / shareholder_count / institutional_holding /
    margin_trading / valuation_history (最近30天增量) + etf_constituent (全量刷新)。

    同步策略：查本地各表已有的最大 trade_date/end_date/report_date，
    只从云端拉取比本地更新的记录（增量同步）。
    ETF 成分股每次全量刷新最新一期。

    调度：每月第 5、15、25 日 20:30 执行。
    """
    logger.info("[Scheduler] 市场增量数据月度同步开始")
    result = {
        "insider_trading": 0,
        "shareholder_count": 0,
        "institutional_holding": 0,
        "margin_trading": 0,
        "valuation_history": 0,
        "etf_constituent": 0,
    }
    try:
        from utils.db_utils import _get_conn, _get_cloud_conn
        local = _get_conn()
        cloud = _get_cloud_conn()
        try:
            with local.cursor() as lc, cloud.cursor() as cc:

                # ── 1. insider_trading — 最近30天增量 ────────────────────────
                lc.execute("SELECT COALESCE(MAX(trade_date), '1970-01-01') as max_d FROM insider_trading")
                max_d = str(lc.fetchone()['max_d'])
                cc.execute(
                    "SELECT id,stock_code,stock_name,trade_date,person_name,person_role,"
                    "direction,trade_shares,trade_price,trade_amount,hold_shares_after,relation "
                    "FROM insider_trading WHERE trade_date > %s "
                    "AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) "
                    "ORDER BY trade_date LIMIT 5000",
                    [max_d],
                )
                rows = cc.fetchall()
                for r in rows:
                    lc.execute(
                        """INSERT IGNORE INTO insider_trading
                           (id,stock_code,stock_name,trade_date,person_name,person_role,
                            direction,trade_shares,trade_price,trade_amount,hold_shares_after,relation)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [r['id'], r['stock_code'], r.get('stock_name'), r['trade_date'],
                         r.get('person_name'), r.get('person_role'), r.get('direction'),
                         r.get('trade_shares'), r.get('trade_price'), r.get('trade_amount'),
                         r.get('hold_shares_after'), r.get('relation')],
                    )
                local.commit()
                result['insider_trading'] = len(rows)

                # ── 2. shareholder_count — 最新一期 ──────────────────────────
                lc.execute("SELECT COALESCE(MAX(end_date), '1970-01-01') as max_d FROM shareholder_count")
                max_d = str(lc.fetchone()['max_d'])
                cc.execute(
                    "SELECT id,stock_code,stock_name,end_date,holder_count,holder_count_change,"
                    "change_pct,avg_share_per_holder,avg_amount_per_holder "
                    "FROM shareholder_count WHERE end_date > %s ORDER BY end_date LIMIT 5000",
                    [max_d],
                )
                rows = cc.fetchall()
                for r in rows:
                    lc.execute(
                        """INSERT IGNORE INTO shareholder_count
                           (id,stock_code,stock_name,end_date,holder_count,holder_count_change,
                            change_pct,avg_share_per_holder,avg_amount_per_holder)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [r['id'], r['stock_code'], r.get('stock_name'), r['end_date'],
                         r.get('holder_count'), r.get('holder_count_change'), r.get('change_pct'),
                         r.get('avg_share_per_holder'), r.get('avg_amount_per_holder')],
                    )
                local.commit()
                result['shareholder_count'] = len(rows)

                # ── 3. institutional_holding — 最新季报 ──────────────────────
                lc.execute("SELECT COALESCE(MAX(report_date), '1970-01-01') as max_d FROM institutional_holding")
                max_d = str(lc.fetchone()['max_d'])
                cc.execute(
                    "SELECT id,stock_code,stock_name,report_date,institution_type,"
                    "hold_shares,hold_ratio,hold_change,hold_value "
                    "FROM institutional_holding WHERE report_date > %s ORDER BY report_date LIMIT 10000",
                    [max_d],
                )
                rows = cc.fetchall()
                for r in rows:
                    lc.execute(
                        """INSERT IGNORE INTO institutional_holding
                           (id,stock_code,stock_name,report_date,institution_type,
                            hold_shares,hold_ratio,hold_change,hold_value)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [r['id'], r['stock_code'], r.get('stock_name'), r['report_date'],
                         r.get('institution_type'), r.get('hold_shares'),
                         r.get('hold_ratio'), r.get('hold_change'), r.get('hold_value')],
                    )
                local.commit()
                result['institutional_holding'] = len(rows)

                # ── 4. margin_trading — 最近30天增量 ─────────────────────────
                lc.execute("SELECT COALESCE(MAX(trade_date), '1970-01-01') as max_d FROM margin_trading")
                max_d = str(lc.fetchone()['max_d'])
                cc.execute(
                    "SELECT id,stock_code,stock_name,trade_date,margin_balance,margin_buy_amount,"
                    "margin_repay_amount,short_balance,short_sell_volume,short_repay_volume,"
                    "short_sell_amount,total_balance,exchange "
                    "FROM margin_trading WHERE trade_date > %s "
                    "AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) "
                    "ORDER BY trade_date LIMIT 5000",
                    [max_d],
                )
                rows = cc.fetchall()
                for r in rows:
                    lc.execute(
                        """INSERT IGNORE INTO margin_trading
                           (id,stock_code,stock_name,trade_date,margin_balance,margin_buy_amount,
                            margin_repay_amount,short_balance,short_sell_volume,short_repay_volume,
                            short_sell_amount,total_balance,exchange)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [r['id'], r['stock_code'], r.get('stock_name'), r['trade_date'],
                         r.get('margin_balance'), r.get('margin_buy_amount'),
                         r.get('margin_repay_amount'), r.get('short_balance'),
                         r.get('short_sell_volume'), r.get('short_repay_volume'),
                         r.get('short_sell_amount'), r.get('total_balance'), r.get('exchange')],
                    )
                local.commit()
                result['margin_trading'] = len(rows)

                # ── 5. valuation_history — 最近30天增量 ──────────────────────
                lc.execute("SELECT COALESCE(MAX(trade_date), '1970-01-01') as max_d FROM valuation_history")
                max_d = str(lc.fetchone()['max_d'])
                cc.execute(
                    "SELECT id,stock_code,trade_date,pe_ttm,pb_mrq,ps_ttm,"
                    "dividend_yield,market_cap,circ_market_cap "
                    "FROM valuation_history WHERE trade_date > %s "
                    "AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) "
                    "ORDER BY trade_date LIMIT 50000",
                    [max_d],
                )
                rows = cc.fetchall()
                for r in rows:
                    lc.execute(
                        """INSERT IGNORE INTO valuation_history
                           (id,stock_code,trade_date,pe_ttm,pb_mrq,ps_ttm,
                            dividend_yield,market_cap,circ_market_cap)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [r['id'], r['stock_code'], r['trade_date'],
                         r.get('pe_ttm'), r.get('pb_mrq'), r.get('ps_ttm'),
                         r.get('dividend_yield'), r.get('market_cap'), r.get('circ_market_cap')],
                    )
                local.commit()
                result['valuation_history'] = len(rows)

                # ── 6. etf_constituent — 最新一期全量刷新 ────────────────────
                cc.execute("SELECT MAX(report_date) as max_d FROM etf_constituent")
                r = cc.fetchone()
                latest_rd = r['max_d'] if r else None
                if latest_rd:
                    cc.execute(
                        "SELECT id,etf_code,etf_name,stock_code,stock_name,weight,shares,amount,report_date "
                        "FROM etf_constituent WHERE report_date=%s LIMIT 10000",
                        [latest_rd],
                    )
                    rows = cc.fetchall()
                    for r in rows:
                        lc.execute(
                            """INSERT IGNORE INTO etf_constituent
                               (id,etf_code,etf_name,stock_code,stock_name,weight,shares,amount,report_date)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            [r['id'], r['etf_code'], r.get('etf_name'), r['stock_code'],
                             r.get('stock_name'), r.get('weight'), r.get('shares'),
                             r.get('amount'), r.get('report_date')],
                        )
                    local.commit()
                    result['etf_constituent'] = len(rows)

        finally:
            cloud.close()
            local.close()

        logger.info(f"[Scheduler] 市场增量数据月度同步完成: {result}")
    except Exception as e:
        logger.exception(f"[Scheduler] 市场增量数据月度同步失败: {e}")
        result['error'] = str(e)

    return result


# ── 问财行业指标采集 ─────────────────────────────────────────

def run_wencai_indicators():
    """每天 21:00：从问财采集行业指标 → LLM提取 → 写入 industry_indicators"""
    logger.info("[Scheduler] 问财行业指标采集开始")
    try:
        from ingestion.wencai_indicator_fetcher import run_wencai_indicator_fetch
        result = run_wencai_indicator_fetch()
        logger.info(f"[Scheduler] 问财行业指标采集完成: {result}")
        return result
    except Exception as e:
        logger.exception(f"[Scheduler] 问财行业指标采集失败: {e}")
        return {"error": str(e)}


# ── Robust Kline 日扫描 ──────────────────────────────────────

def _run_robust_kline_daily():
    """每天16:00：扫描报告提及 → 月K线过滤 → 亮点填充"""
    logger.info("[Scheduler] Robust Kline 日扫描开始")
    try:
        from routers.robust_kline import run_robust_kline_scan
        result = run_robust_kline_scan()
        logger.info(f"[Scheduler] Robust Kline 完成: {result}")
        return result
    except Exception as e:
        logger.exception(f"[Scheduler] Robust Kline 失败: {e}")
        return {"error": str(e)}


# ── 调度器启停 ──────────────────────────────────────────────

def start_scheduler():
    """启动定时任务（FastAPI启动时调用）"""
    if scheduler.running:
        return

    # 每天 06:00
    scheduler.add_job(
        run_kg_update, CronTrigger(hour=6, minute=0),
        id="kg_auto_morning", replace_existing=True,
        name="KG早间自动构建",
    )
    # 每天 20:00
    scheduler.add_job(
        run_kg_update, CronTrigger(hour=20, minute=0),
        id="kg_auto_evening", replace_existing=True,
        name="KG晚间自动构建",
    )

    # 每天 18:30 — 宏观日度采集
    scheduler.add_job(
        run_macro_daily, CronTrigger(hour=18, minute=30),
        id="macro_daily", replace_existing=True,
        name="宏观日度采集",
    )
    # 每月 15 日 19:00 — 宏观月度采集
    scheduler.add_job(
        run_macro_monthly, CronTrigger(day=15, hour=19, minute=0),
        id="macro_monthly", replace_existing=True,
        name="宏观月度采集",
    )
    # 每月 5/15/25 日 20:30 — 市场增量数据同步
    scheduler.add_job(
        run_market_data_monthly, CronTrigger(day="5,15,25", hour=20, minute=30),
        id="market_data_monthly", replace_existing=True,
        name="市场增量数据月度同步",
    )
    # 每天 06:00 + 16:00 — Robust Kline 扫描
    scheduler.add_job(
        _run_robust_kline_daily, CronTrigger(hour=6, minute=0),
        id="robust_kline_morning", replace_existing=True,
        name="Robust Kline 早间扫描",
    )
    scheduler.add_job(
        _run_robust_kline_daily, CronTrigger(hour=16, minute=0),
        id="robust_kline_afternoon", replace_existing=True,
        name="Robust Kline 午后扫描",
    )
    # 每天 21:00 — 问财行业指标采集
    scheduler.add_job(
        run_wencai_indicators, CronTrigger(hour=21, minute=0),
        id="wencai_indicators_daily", replace_existing=True,
        name="问财行业指标采集",
    )

    # 每天 23:00 — chain_sync + theme_merger 夜间兜底
    def _run_daily_sync_nightly():
        try:
            from config.chain_sync import run_chain_sync
            result = run_chain_sync()
            logger.info(f"[Scheduler] chain_sync 夜间完成: {result}")
        except Exception as e:
            logger.warning(f"[Scheduler] chain_sync 夜间失败: {e}")
        try:
            from daily_intel.theme_merger import run_theme_merge
            result = run_theme_merge()
            logger.info(f"[Scheduler] theme_merger 夜间完成: {result}")
        except Exception as e:
            logger.warning(f"[Scheduler] theme_merger 夜间失败: {e}")

    scheduler.add_job(
        _run_daily_sync_nightly, CronTrigger(hour=23, minute=0),
        id="daily_sync_nightly", replace_existing=True,
        name="chain_sync + theme_merger 夜间兜底",
    )

    scheduler.start()
    logger.info("[Scheduler] 定时任务已启动: 06:00 + 20:00 KG自动构建, 18:30 宏观日度, 每月15日19:00 宏观月度, 每月5/15/25日20:30 市场数据同步, 06:00+16:00 Robust Kline, 21:00 问财行业指标")


def stop_scheduler():
    """停止定时任务（FastAPI关闭时调用）"""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("[Scheduler] 定时任务已停止")

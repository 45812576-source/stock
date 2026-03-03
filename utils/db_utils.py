"""数据库工具函数 — MySQL via pymysql"""
import re
import pymysql
import pymysql.cursors
from contextlib import contextmanager
from config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB


# ---------- SQL 兼容层 ----------

def _adapt_sql(sql):
    """将 SQLite 风格 SQL 自动转为 MySQL 语法"""
    # 参数占位符: ? → %s
    sql = sql.replace("?", "%s")

    # INSERT OR REPLACE → REPLACE INTO
    sql = re.sub(r"INSERT\s+OR\s+REPLACE\s+INTO", "REPLACE INTO", sql, flags=re.IGNORECASE)

    # INSERT OR IGNORE → INSERT IGNORE
    sql = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT IGNORE INTO", sql, flags=re.IGNORECASE)

    # datetime('now') → NOW()
    sql = re.sub(r"datetime\(\s*'now'\s*\)", "NOW()", sql, flags=re.IGNORECASE)

    # date('now', '-180 days') → DATE_SUB(CURDATE(), INTERVAL 180 DAY)  (硬编码天数)
    def _replace_date_hardcoded_days(m):
        sign = m.group(1)  # + or -
        num = m.group(2)
        return f"DATE_SUB(CURDATE(), INTERVAL {num} DAY)" if sign == "-" else f"DATE_ADD(CURDATE(), INTERVAL {num} DAY)"
    sql = re.sub(
        r"date\(\s*'now'\s*,\s*'([+-])(\d+)\s+days?'\s*\)",
        _replace_date_hardcoded_days, sql, flags=re.IGNORECASE,
    )

    # date('now', '-3 months') → DATE_SUB(CURDATE(), INTERVAL 3 MONTH)  (硬编码月份)
    def _replace_date_hardcoded_months(m):
        sign = m.group(1)
        num = m.group(2)
        return f"DATE_SUB(CURDATE(), INTERVAL {num} MONTH)" if sign == "-" else f"DATE_ADD(CURDATE(), INTERVAL {num} MONTH)"
    sql = re.sub(
        r"date\(\s*'now'\s*,\s*'([+-])(\d+)\s+months?'\s*\)",
        _replace_date_hardcoded_months, sql, flags=re.IGNORECASE,
    )

    # date('now', %s) → DATE_SUB(CURDATE(), INTERVAL %s DAY)  (参数化间隔)
    sql = re.sub(
        r"date\(\s*'now'\s*,\s*%s\s*\)",
        "DATE_SUB(CURDATE(), INTERVAL %s DAY)", sql, flags=re.IGNORECASE,
    )

    # date('now') → CURDATE()
    sql = re.sub(r"date\(\s*'now'\s*\)", "CURDATE()", sql, flags=re.IGNORECASE)

    return sql


def _adapt_params(params):
    """将 SQLite 风格参数转为 MySQL 参数（如 '-7 days' → 7）"""
    if params is None:
        return None
    adapted = []
    for p in params:
        if isinstance(p, str):
            m = re.match(r"^[+-]?(\d+)\s+days?$", p, re.IGNORECASE)
            if m:
                adapted.append(int(m.group(1)))
                continue
            m = re.match(r"^[+-]?(\d+)\s+months?$", p, re.IGNORECASE)
            if m:
                adapted.append(int(m.group(1)))
                continue
        adapted.append(p)
    return adapted


# ---------- 连接包装器 ----------

class _ConnWrapper:
    """包装 pymysql 连接，使 conn.execute() 自动经过兼容层"""

    def __init__(self, conn):
        self._conn = conn
        self._cursor = conn.cursor()

    def execute(self, sql, params=None):
        sql = _adapt_sql(sql)
        params = _adapt_params(params)
        self._cursor.execute(sql, params or ())
        return self._cursor

    def executemany(self, sql, params_list):
        sql = _adapt_sql(sql)
        adapted_list = [_adapt_params(p) for p in params_list]
        self._cursor.executemany(sql, adapted_list)
        return self._cursor

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._cursor.close()
        self._conn.close()


# ---------- 公共 API ----------

def _get_conn():
    """创建原始 pymysql 连接"""
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


@contextmanager
def get_db():
    """获取数据库连接的上下文管理器（返回 _ConnWrapper）"""
    raw = _get_conn()
    conn = _ConnWrapper(raw)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_query(sql, params=None):
    """执行查询并返回结果列表"""
    sql = _adapt_sql(sql)
    params = _adapt_params(params)
    raw = _get_conn()
    try:
        with raw.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()
    finally:
        raw.close()


def execute_insert(sql, params=None):
    """执行插入并返回 lastrowid"""
    sql = _adapt_sql(sql)
    params = _adapt_params(params)
    raw = _get_conn()
    try:
        with raw.cursor() as cur:
            cur.execute(sql, params or ())
            raw.commit()
            return cur.lastrowid
    finally:
        raw.close()


def execute_many(sql, params_list):
    """批量执行"""
    sql = _adapt_sql(sql)
    adapted_list = [_adapt_params(p) for p in params_list]
    raw = _get_conn()
    try:
        with raw.cursor() as cur:
            cur.executemany(sql, adapted_list)
            raw.commit()
    finally:
        raw.close()


def table_row_count(table_name):
    """获取表行数"""
    rows = execute_query(f"SELECT COUNT(*) as cnt FROM {table_name}")
    return rows[0]["cnt"] if rows else 0


# ---------- 云端连接 ----------

def _get_cloud_conn():
    """云端 MySQL 连接（带重试）"""
    import time as _time
    from config import CLOUD_MYSQL_HOST, CLOUD_MYSQL_PORT, CLOUD_MYSQL_USER, CLOUD_MYSQL_PASSWORD, CLOUD_MYSQL_DB
    last_err = None
    for attempt in range(3):
        try:
            return pymysql.connect(
                host=CLOUD_MYSQL_HOST,
                port=CLOUD_MYSQL_PORT,
                user=CLOUD_MYSQL_USER,
                password=CLOUD_MYSQL_PASSWORD,
                database=CLOUD_MYSQL_DB,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=15,
                read_timeout=60,
                write_timeout=60,
            )
        except Exception as e:
            last_err = e
            if attempt < 2:
                _time.sleep(2 ** attempt)
    raise last_err


def get_config(key: str) -> str:
    """从云端 system_config 表读取配置值"""
    rows = execute_cloud_query("SELECT value FROM system_config WHERE config_key=%s", [key])
    return rows[0]["value"] if rows else ""


def execute_cloud_query(sql, params=None):
    """在云端执行查询并返回结果列表（连接断开自动重试一次）"""
    import time as _time
    sql = _adapt_sql(sql)
    params = _adapt_params(params)
    for attempt in range(2):
        raw = _get_cloud_conn()
        try:
            with raw.cursor() as cur:
                cur.execute(sql, params or ())
                return cur.fetchall()
        except pymysql.err.OperationalError:
            raw.close()
            if attempt == 0:
                _time.sleep(1)
                continue
            raise
        finally:
            try:
                raw.close()
            except Exception:
                pass


def execute_cloud_insert(sql, params=None):
    """在云端执行插入并返回 lastrowid（连接断开自动重试一次）"""
    import time as _time
    sql = _adapt_sql(sql)
    params = _adapt_params(params)
    for attempt in range(2):
        raw = _get_cloud_conn()
        try:
            with raw.cursor() as cur:
                cur.execute(sql, params or ())
                raw.commit()
                return cur.lastrowid
        except pymysql.err.OperationalError:
            raw.close()
            if attempt == 0:
                _time.sleep(1)
                continue
            raise
        finally:
            try:
                raw.close()
            except Exception:
                pass


def cloud_stockdb_query(sql, params=None):
    """在云端 stock_db 执行查询并返回结果列表"""
    raw = _get_cloud_stockdb_conn()
    try:
        with raw.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()
    finally:
        raw.close()


# ---------- 本地同步 ----------

def sync_cleaned_to_local(cleaned_id):
    """将云端清洗结果同步到本地库（cleaned_items + item_companies + item_industries + research_reports）"""
    import logging
    logger = logging.getLogger(__name__)

    cloud = _get_cloud_conn()
    local = _get_conn()
    try:
        with cloud.cursor() as cc:
            # 1. cleaned_items
            cc.execute("SELECT * FROM cleaned_items WHERE id=%s", [cleaned_id])
            row = cc.fetchone()
            if not row:
                return

            with local.cursor() as lc:
                lc.execute(
                    """REPLACE INTO cleaned_items
                       (id, raw_item_id, event_type, sentiment, importance,
                        summary, key_points_json, tags_json, impact_analysis,
                        time_horizon, confidence, structured_json, cleaned_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [row[k] for k in ['id', 'raw_item_id', 'event_type', 'sentiment',
                     'importance', 'summary', 'key_points_json', 'tags_json',
                     'impact_analysis', 'time_horizon', 'confidence',
                     'structured_json', 'cleaned_at']]
                )

                # 2. item_companies
                cc.execute("SELECT * FROM item_companies WHERE cleaned_item_id=%s", [cleaned_id])
                for r in cc.fetchall():
                    lc.execute(
                        """REPLACE INTO item_companies
                           (id, cleaned_item_id, stock_code, stock_name, relevance, impact)
                           VALUES (%s,%s,%s,%s,%s,%s)""",
                        [r['id'], r['cleaned_item_id'], r['stock_code'],
                         r['stock_name'], r['relevance'], r['impact']]
                    )

                # 3. item_industries
                cc.execute("SELECT * FROM item_industries WHERE cleaned_item_id=%s", [cleaned_id])
                for r in cc.fetchall():
                    lc.execute(
                        """REPLACE INTO item_industries
                           (id, cleaned_item_id, industry_name, industry_level, impact)
                           VALUES (%s,%s,%s,%s,%s)""",
                        [r['id'], r['cleaned_item_id'], r['industry_name'],
                         r['industry_level'], r['impact']]
                    )

                # 4. research_reports
                cc.execute("SELECT * FROM research_reports WHERE cleaned_item_id=%s", [cleaned_id])
                for r in cc.fetchall():
                    lc.execute(
                        """REPLACE INTO research_reports
                           (id, cleaned_item_id, broker_name, analyst_name,
                            report_type, rating, target_price, stock_code,
                            stock_name, report_date)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [r['id'], r['cleaned_item_id'], r['broker_name'],
                         r['analyst_name'], r['report_type'], r['rating'],
                         r['target_price'], r['stock_code'], r['stock_name'],
                         r['report_date']]
                    )

            local.commit()
    except Exception as e:
        logger.warning(f"同步到本地失败 cleaned_id={cleaned_id}: {e}")
    finally:
        cloud.close()
        local.close()


def sync_summary_to_local(summary_id):
    """将云端 content_summaries 记录同步到本地库"""
    import logging
    logger = logging.getLogger(__name__)

    cloud = _get_cloud_conn()
    local = _get_conn()
    try:
        with cloud.cursor() as cc:
            cc.execute("SELECT * FROM content_summaries WHERE id=%s", [summary_id])
            row = cc.fetchone()
            if not row:
                return
            with local.cursor() as lc:
                lc.execute(
                    """REPLACE INTO content_summaries
                       (id, extracted_text_id, doc_type, summary, fact_summary, opinion_summary,
                        evidence_assessment, info_gaps, family, type_fields, detail_table, detail_id, created_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [row['id'], row['extracted_text_id'], row.get('doc_type'),
                     row['summary'], row.get('fact_summary'), row.get('opinion_summary'),
                     row.get('evidence_assessment'), row.get('info_gaps'),
                     row.get('family'), row.get('type_fields'),
                     row.get('detail_table'), row.get('detail_id'),
                     row['created_at']]
                )
        local.commit()
    except Exception as e:
        logger.warning(f"同步 content_summaries 到本地失败 summary_id={summary_id}: {e}")
    finally:
        cloud.close()
        local.close()


def sync_mentions_to_local(mention_ids: list):
    """将云端 stock_mentions 记录同步到本地库"""
    import logging
    logger = logging.getLogger(__name__)
    if not mention_ids:
        return

    cloud = _get_cloud_conn()
    local = _get_conn()
    try:
        placeholders = ",".join(["%s"] * len(mention_ids))
        with cloud.cursor() as cc:
            cc.execute(
                f"SELECT * FROM stock_mentions WHERE id IN ({placeholders})",
                mention_ids,
            )
            rows = cc.fetchall()
            if not rows:
                return
            with local.cursor() as lc:
                for row in rows:
                    lc.execute(
                        """REPLACE INTO stock_mentions
                           (id, extracted_text_id, stock_name, stock_code,
                            related_themes, related_events, theme_logic, mention_time)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [row['id'], row['extracted_text_id'], row.get('stock_name'),
                         row.get('stock_code'), row.get('related_themes'),
                         row.get('related_events'), row.get('theme_logic'),
                         row.get('mention_time')],
                    )
        local.commit()
    except Exception as e:
        logger.warning(f"同步 stock_mentions 到本地失败: {e}")
    finally:
        cloud.close()
        local.close()


def sync_new_pipeline_records(batch_size: int = 500) -> dict:
    """增量同步三张新管线表：extracted_texts / content_summaries / stock_mentions
    按 id 增量，只同步本地尚未存在的记录。
    返回 {'extracted_texts': n, 'content_summaries': n, 'stock_mentions': n}
    """
    import logging
    logger = logging.getLogger(__name__)
    result = {'extracted_texts': 0, 'content_summaries': 0, 'stock_mentions': 0}

    cloud = _get_cloud_conn()
    local = _get_conn()
    try:
        with cloud.cursor() as cc, local.cursor() as lc:
            # ── 1. extracted_texts ──────────────────────────────────
            lc.execute("SELECT COALESCE(MAX(id), 0) as max_id FROM extracted_texts")
            local_max = lc.fetchone()['max_id']
            cc.execute(
                "SELECT id, full_text, source, source_format, publish_time "
                "FROM extracted_texts WHERE id > %s ORDER BY id LIMIT %s",
                [local_max, batch_size]
            )
            rows = cc.fetchall()
            for r in rows:
                lc.execute(
                    """REPLACE INTO extracted_texts
                       (id, full_text, source, source_format, publish_time)
                       VALUES (%s,%s,%s,%s,%s)""",
                    [r['id'], r['full_text'], r['source'],
                     r['source_format'], r['publish_time']]
                )
            result['extracted_texts'] = len(rows)

            # ── 2. content_summaries ────────────────────────────────
            lc.execute("SELECT COALESCE(MAX(id), 0) as max_id FROM content_summaries")
            local_max = lc.fetchone()['max_id']
            cc.execute(
                "SELECT id, extracted_text_id, doc_type, summary, fact_summary, "
                "opinion_summary, evidence_assessment, info_gaps, family, type_fields, created_at "
                "FROM content_summaries WHERE id > %s ORDER BY id LIMIT %s",
                [local_max, batch_size]
            )
            rows = cc.fetchall()
            for r in rows:
                lc.execute(
                    """REPLACE INTO content_summaries
                       (id, extracted_text_id, doc_type, summary, fact_summary,
                        opinion_summary, evidence_assessment, info_gaps, family, type_fields, created_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [r['id'], r['extracted_text_id'], r.get('doc_type'),
                     r['summary'], r['fact_summary'], r['opinion_summary'],
                     r['evidence_assessment'], r['info_gaps'],
                     r.get('family'), r.get('type_fields'), r['created_at']]
                )
            result['content_summaries'] = len(rows)

            # ── 3. stock_mentions ───────────────────────────────────
            lc.execute("SELECT COALESCE(MAX(id), 0) as max_id FROM stock_mentions")
            local_max = lc.fetchone()['max_id']
            cc.execute(
                "SELECT id, extracted_text_id, stock_name, stock_code, "
                "related_themes, related_events, theme_logic, mention_time "
                "FROM stock_mentions WHERE id > %s ORDER BY id LIMIT %s",
                [local_max, batch_size]
            )
            rows = cc.fetchall()
            for r in rows:
                lc.execute(
                    """REPLACE INTO stock_mentions
                       (id, extracted_text_id, stock_name, stock_code,
                        related_themes, related_events, theme_logic, mention_time)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [r['id'], r['extracted_text_id'], r['stock_name'], r['stock_code'],
                     r['related_themes'], r['related_events'], r['theme_logic'], r['mention_time']]
                )
            result['stock_mentions'] = len(rows)

        local.commit()
        logger.info(f"增量同步完成: {result}")
    except Exception as e:
        logger.error(f"增量同步失败: {e}")
        result['error'] = str(e)
    finally:
        cloud.close()
        local.close()

    return result


# ---------- 云端行情数据同步 ----------

def _get_cloud_stockdb_conn():
    """云端 stock_db 数据库连接"""
    from config import CLOUD_MYSQL_HOST, CLOUD_MYSQL_PORT, CLOUD_MYSQL_USER, CLOUD_MYSQL_PASSWORD
    return pymysql.connect(
        host=CLOUD_MYSQL_HOST,
        port=CLOUD_MYSQL_PORT,
        user=CLOUD_MYSQL_USER,
        password=CLOUD_MYSQL_PASSWORD,
        database='stock_db',  # 行情数据在 stock_db
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def sync_stock_data_from_cloud(stock_code: str, days: int = 180) -> dict:
    """从云端 stock_db 同步股票行情数据到本地

    Args:
        stock_code: 股票代码 (如 '603009')
        days: 同步最近多少天的数据

    Returns:
        {'kline': 同步条数, 'capital': 同步条数, 'error': 错误信息}
    """
    import logging
    logger = logging.getLogger(__name__)
    result = {'kline': 0, 'capital': 0, 'error': None}

    cloud = _get_cloud_stockdb_conn()
    local = _get_conn()
    try:
        with cloud.cursor() as cc:
            # 1. 同步 K 线数据 (stock_data -> stock_daily)
            cc.execute(
                """SELECT trade_date, symbol as stock_code, open_price as open, high_price as high,
                          low_price as low, close_price as close, volume, amount,
                          turnover_rate, change_percent as change_pct
                   FROM stock_data
                   WHERE symbol = %s AND trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
                   ORDER BY trade_date""",
                [stock_code, days]
            )
            kline_rows = cc.fetchall()

            if kline_rows:
                with local.cursor() as lc:
                    for r in kline_rows:
                        lc.execute(
                            """REPLACE INTO stock_daily
                               (stock_code, trade_date, open, high, low, close, volume, amount,
                                turnover_rate, change_pct)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            [r['stock_code'], r['trade_date'], r['open'], r['high'],
                             r['low'], r['close'], r['volume'], r['amount'],
                             r['turnover_rate'], r['change_pct']]
                        )
                    result['kline'] = len(kline_rows)

            # 2. 同步资金流数据 (fund_flow_history -> capital_flow)
            cc.execute(
                """SELECT trade_date, symbol as stock_code,
                          buy_lg_amount, sell_lg_amount,
                          buy_elg_amount, sell_elg_amount
                   FROM fund_flow_history
                   WHERE symbol = %s AND trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
                   ORDER BY trade_date""",
                [stock_code, days]
            )
            flow_rows = cc.fetchall()

            if flow_rows:
                with local.cursor() as lc:
                    for r in flow_rows:
                        # 计算主力净流入 = 大单买入 - 大单卖出 + 超大单买入 - 超大单卖出
                        main_net = (r.get('buy_lg_amount') or 0) - (r.get('sell_lg_amount') or 0) + \
                                   (r.get('buy_elg_amount') or 0) - (r.get('sell_elg_amount') or 0)
                        super_large = (r.get('buy_elg_amount') or 0) - (r.get('sell_elg_amount') or 0)
                        large = (r.get('buy_lg_amount') or 0) - (r.get('sell_lg_amount') or 0)

                        lc.execute(
                            """REPLACE INTO capital_flow
                               (stock_code, trade_date, main_net_inflow, super_large_net, large_net)
                               VALUES (%s, %s, %s, %s, %s)""",
                            [r['stock_code'], r['trade_date'], main_net, super_large, large]
                        )
                    result['capital'] = len(flow_rows)

            local.commit()
            logger.info(f"从云端同步 {stock_code}: K线={result['kline']} 资金流={result['capital']}")

    except Exception as e:
        result['error'] = str(e)
        logger.error(f"从云端同步 {stock_code} 失败: {e}")
    finally:
        cloud.close()
        local.close()

    return result


def ensure_stock_data(stock_code: str, days: int = 180) -> dict:
    """确保本地有股票数据，没有则从云端同步

    Args:
        stock_code: 股票代码
        days: 检查/同步最近多少天的数据

    Returns:
        {'has_data': bool, 'synced': bool, 'kline': int, 'capital': int}
    """
    result = {'has_data': False, 'synced': False, 'kline': 0, 'capital': 0}

    # 检查本地是否有数据
    rows = execute_query(
        """SELECT COUNT(*) as cnt FROM stock_daily
           WHERE stock_code = %s AND trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)""",
        [stock_code, days]
    )
    local_count = rows[0]['cnt'] if rows else 0

    if local_count > 0:
        result['has_data'] = True
        result['kline'] = local_count
        return result

    # 本地无数据，从云端同步
    sync_result = sync_stock_data_from_cloud(stock_code, days)
    result['synced'] = True
    result['kline'] = sync_result.get('kline', 0)
    result['capital'] = sync_result.get('capital', 0)
    result['has_data'] = result['kline'] > 0
    result['error'] = sync_result.get('error')

    return result


# ---------- 宏观数据同步 ----------

def sync_macro_to_local(batch_size: int = 500) -> dict:
    """增量同步 5 张宏观表：macro_indicators / margin_balance / market_valuation /
    hsgt_holding / overseas_etf。按 id 增量，只同步本地尚未存在的记录。
    返回各表同步条数字典。
    """
    import logging
    logger = logging.getLogger(__name__)
    result = {
        'macro_indicators': 0,
        'margin_balance': 0,
        'market_valuation': 0,
        'hsgt_holding': 0,
        'overseas_etf': 0,
    }

    cloud = _get_cloud_conn()
    local = _get_conn()
    try:
        with cloud.cursor() as cc, local.cursor() as lc:

            # ── 1. macro_indicators ──────────────────────────────────────────
            lc.execute("SELECT COALESCE(MAX(id), 0) as max_id FROM macro_indicators")
            local_max = lc.fetchone()['max_id']
            cc.execute(
                "SELECT id, indicator_name, indicator_date, value, unit, source "
                "FROM macro_indicators WHERE id > %s ORDER BY id LIMIT %s",
                [local_max, batch_size]
            )
            rows = cc.fetchall()
            for r in rows:
                lc.execute(
                    """REPLACE INTO macro_indicators
                       (id, indicator_name, indicator_date, value, unit, source)
                       VALUES (%s,%s,%s,%s,%s,%s)""",
                    [r['id'], r['indicator_name'], r['indicator_date'],
                     r['value'], r['unit'], r['source']]
                )
            result['macro_indicators'] = len(rows)

            # ── 2. margin_balance ────────────────────────────────────────────
            lc.execute("SELECT COALESCE(MAX(id), 0) as max_id FROM margin_balance")
            local_max = lc.fetchone()['max_id']
            cc.execute(
                "SELECT id, trade_date, margin_balance, margin_buy, total_balance "
                "FROM margin_balance WHERE id > %s ORDER BY id LIMIT %s",
                [local_max, batch_size]
            )
            rows = cc.fetchall()
            for r in rows:
                lc.execute(
                    """REPLACE INTO margin_balance
                       (id, trade_date, margin_balance, margin_buy, total_balance)
                       VALUES (%s,%s,%s,%s,%s)""",
                    [r['id'], r['trade_date'], r['margin_balance'],
                     r['margin_buy'], r['total_balance']]
                )
            result['margin_balance'] = len(rows)

            # ── 3. market_valuation ──────────────────────────────────────────
            lc.execute("SELECT COALESCE(MAX(id), 0) as max_id FROM market_valuation")
            local_max = lc.fetchone()['max_id']
            cc.execute(
                "SELECT id, trade_date, pe_ttm_median, pe_ttm_avg, pe_quantile_10y, "
                "pe_quantile_all, total_market_cap, market_pe, sh_amount, sz_amount, close_index "
                "FROM market_valuation WHERE id > %s ORDER BY id LIMIT %s",
                [local_max, batch_size]
            )
            rows = cc.fetchall()
            for r in rows:
                lc.execute(
                    """REPLACE INTO market_valuation
                       (id, trade_date, pe_ttm_median, pe_ttm_avg, pe_quantile_10y,
                        pe_quantile_all, total_market_cap, market_pe, sh_amount, sz_amount, close_index)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [r['id'], r['trade_date'], r['pe_ttm_median'], r['pe_ttm_avg'],
                     r['pe_quantile_10y'], r['pe_quantile_all'], r['total_market_cap'],
                     r['market_pe'], r['sh_amount'], r['sz_amount'], r['close_index']]
                )
            result['market_valuation'] = len(rows)

            # ── 4. hsgt_holding ──────────────────────────────────────────────
            lc.execute("SELECT COALESCE(MAX(id), 0) as max_id FROM hsgt_holding")
            local_max = lc.fetchone()['max_id']
            cc.execute(
                "SELECT id, trade_date, stock_code, stock_name, close_price, "
                "holding_shares, holding_market_value, holding_ratio_float, "
                "holding_ratio_total, change_shares, change_market_value, sector "
                "FROM hsgt_holding WHERE id > %s ORDER BY id LIMIT %s",
                [local_max, batch_size]
            )
            rows = cc.fetchall()
            for r in rows:
                lc.execute(
                    """REPLACE INTO hsgt_holding
                       (id, trade_date, stock_code, stock_name, close_price,
                        holding_shares, holding_market_value, holding_ratio_float,
                        holding_ratio_total, change_shares, change_market_value, sector)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [r['id'], r['trade_date'], r['stock_code'], r['stock_name'],
                     r['close_price'], r['holding_shares'], r['holding_market_value'],
                     r['holding_ratio_float'], r['holding_ratio_total'],
                     r['change_shares'], r['change_market_value'], r['sector']]
                )
            result['hsgt_holding'] = len(rows)

            # ── 5. overseas_etf ──────────────────────────────────────────────
            lc.execute("SELECT COALESCE(MAX(id), 0) as max_id FROM overseas_etf")
            local_max = lc.fetchone()['max_id']
            cc.execute(
                "SELECT id, symbol, etf_name, trade_date, open, high, low, close, volume "
                "FROM overseas_etf WHERE id > %s ORDER BY id LIMIT %s",
                [local_max, batch_size]
            )
            rows = cc.fetchall()
            for r in rows:
                lc.execute(
                    """REPLACE INTO overseas_etf
                       (id, symbol, etf_name, trade_date, open, high, low, close, volume)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    [r['id'], r['symbol'], r['etf_name'], r['trade_date'],
                     r['open'], r['high'], r['low'], r['close'], r['volume']]
                )
            result['overseas_etf'] = len(rows)

        local.commit()
        logger.info(f"宏观数据增量同步完成: {result}")
    except Exception as e:
        logger.error(f"宏观数据同步失败: {e}")
        result['error'] = str(e)
    finally:
        cloud.close()
        local.close()

    return result


# ── industry_indicators 查询（云端库）─────────────────────────────────────────

def query_industry_indicator(
    industry_name: str,
    metric_type: str = None,
    period_year: int = None,
    data_type: str = None,
    limit: int = 10,
) -> list:
    """查询行业指标，支持 L2/L3/metric_name 三列模糊匹配。走云端库。

    优先级：industry_l2 精确 > industry_l2 LIKE > industry_l3/metric_name LIKE
    """
    base_conditions = []
    params = []

    if metric_type:
        base_conditions.append("metric_type = %s")
        params.append(metric_type)
    if period_year is not None:
        base_conditions.append("period_year >= %s")
        params.append(period_year)
    if data_type:
        base_conditions.append("data_type = %s")
        params.append(data_type)

    base_where = (" AND " + " AND ".join(base_conditions)) if base_conditions else ""
    order = "ORDER BY publish_date DESC, confidence DESC"

    # L1: 精确匹配 industry_l2
    rows = execute_cloud_query(
        f"SELECT * FROM industry_indicators WHERE industry_l2 = %s{base_where} {order} LIMIT %s",
        [industry_name] + params + [limit],
    )
    if rows:
        return rows

    # L2: LIKE 匹配 industry_l2
    rows = execute_cloud_query(
        f"SELECT * FROM industry_indicators WHERE industry_l2 LIKE %s{base_where} {order} LIMIT %s",
        [f"%{industry_name}%"] + params + [limit],
    )
    if rows:
        return rows

    # L3: 匹配 industry_l3 或 metric_name
    rows = execute_cloud_query(
        f"""SELECT * FROM industry_indicators
            WHERE (industry_l3 LIKE %s OR metric_name LIKE %s){base_where}
            {order} LIMIT %s""",
        [f"%{industry_name}%", f"%{industry_name}%"] + params + [limit],
    )
    return rows or []


def upsert_industry_indicator(row: dict) -> int:
    """写入一条指标到云端库，已存在则按优先级决定是否覆盖，数值偏差>20%标记冲突。

    唯一键：(industry_l2, metric_name, period_label, data_type)
    返回：写入/更新的 id
    """
    _ALLOWED_COLS = {
        "industry_l1", "industry_l2", "industry_l3",
        "metric_type", "metric_name", "metric_definition", "metric_numerator", "metric_denominator",
        "value", "value_raw",
        "period_type", "period_label", "period_year", "period_end_date",
        "publish_date",
        "forecast_target_label", "forecast_target_date",
        "data_type", "confidence", "source_type", "source_doc_id", "source_snippet",
        "is_conflicted", "conflict_note",
    }
    row = {k: v for k, v in row.items() if k in _ALLOWED_COLS}

    # 查已有记录（云端）
    existing = execute_cloud_query(
        """SELECT id, value, confidence, publish_date FROM industry_indicators
           WHERE industry_l2 = %s AND metric_name = %s
             AND period_label = %s AND data_type = %s
           LIMIT 1""",
        [
            row.get("industry_l2", ""),
            row.get("metric_name", ""),
            row.get("period_label", ""),
            row.get("data_type", "actual"),
        ],
    )

    _CONF_RANK = {"high": 3, "medium": 2, "low": 1}

    if not existing:
        cols = ", ".join(row.keys())
        placeholders = ", ".join(["%s"] * len(row))
        execute_cloud_insert(
            f"INSERT INTO industry_indicators ({cols}) VALUES ({placeholders})",
            list(row.values()),
        )
        rows = execute_cloud_query(
            "SELECT id FROM industry_indicators WHERE industry_l2=%s AND metric_name=%s AND period_label=%s AND data_type=%s ORDER BY id DESC LIMIT 1",
            [row.get("industry_l2", ""), row.get("metric_name", ""), row.get("period_label", ""), row.get("data_type", "actual")],
        )
        return rows[0]["id"] if rows else -1

    ex = existing[0]
    new_conf = _CONF_RANK.get(row.get("confidence", "low"), 1)
    ex_conf = _CONF_RANK.get(ex.get("confidence", "low"), 1)

    # 冲突检测（数值偏差>20%）
    ex_val = float(ex.get("value") or 0)
    new_val = float(row.get("value") or 0)
    conflicted = False
    if ex_val != 0 and abs(ex_val - new_val) / abs(ex_val) > 0.2:
        conflicted = True
        conflict_note = f"另一来源值为{ex_val}，来源doc_id={ex.get('source_doc_id')}"
        execute_cloud_insert(
            "UPDATE industry_indicators SET is_conflicted=1, conflict_note=%s WHERE id=%s",
            [conflict_note, ex["id"]],
        )

    # 新数据更优先时覆盖
    new_date = str(row.get("publish_date") or "")
    ex_date = str(ex.get("publish_date") or "")
    should_replace = (new_conf > ex_conf) or (new_conf == ex_conf and new_date > ex_date)

    if should_replace:
        set_clause = ", ".join([f"{k}=%s" for k in row.keys()])
        execute_cloud_insert(
            f"UPDATE industry_indicators SET {set_clause}, is_conflicted=%s WHERE id=%s",
            list(row.values()) + [int(conflicted), ex["id"]],
        )

    return ex["id"]

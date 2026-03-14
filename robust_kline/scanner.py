"""robust_kline/scanner.py — 报告扫描 + 股票提取

每日 06:00 + 16:00 / 手动触发：
  1. 从云端 extracted_texts JOIN source_documents，按关键字 + 帖主（夏天/白白）筛选报告
  2. 跳过同 scan_date 内已处理过的 extracted_text_id（下午跑时不重复处理早上已扫的）
  3. DeepSeek 提取报告中的股票列表（行业/主题-名称-代码-题材）
  4. 结果写入本地 robust_kline_mentions 表
  5. 通过 stock_info 匹配 stock_code（若 DeepSeek 未给出代码）
"""
import json
import logging
import re
import threading
from datetime import date, datetime

from utils.db_utils import execute_cloud_query, execute_query, execute_insert, execute_many

logger = logging.getLogger(__name__)

# ── 关键字 ──────────────────────────────────────────────────────────
_KEYWORDS = [
    "调研日报", "评级日报", "脱水研报", "早知道",
    "强势股脱水", "风口研报", "公告全知道",
]

# 知识星球帖主白名单（author 字段匹配）
_AUTHORS = ["夏天", "白白"]

# ── DeepSeek 客户端（lazy singleton，复用 source_extractor 的实现）──

_deepseek_client = None
_deepseek_lock = threading.Lock()


def _get_deepseek():
    global _deepseek_client
    if _deepseek_client is None:
        with _deepseek_lock:
            if _deepseek_client is None:
                from openai import OpenAI
                rows = execute_cloud_query(
                    "SELECT value FROM system_config WHERE config_key='deepseek_api_key'"
                )
                if not rows:
                    raise RuntimeError("system_config 中未找到 deepseek_api_key")
                _deepseek_client = OpenAI(
                    api_key=rows[0]["value"],
                    base_url="https://api.deepseek.com/v1",
                )
    return _deepseek_client


def _call_deepseek(system_prompt: str, text: str, max_tokens: int = 2048) -> str:
    if len(text) > 10000:
        text = text[:10000] + "\n\n[文本已截断]"
    client = _get_deepseek()
    resp = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text},
        ],
        max_tokens=max_tokens,
        temperature=0.2,
        timeout=90,
    )
    return resp.choices[0].message.content


# ── 股票提取 Prompt ──────────────────────────────────────────────────

_EXTRACT_PROMPT = """你是专业的A股研究助手。以下是一篇研报或日报的内容摘要，请提取所有被明确提及或分析的A股股票。

输出严格的 JSON 数组，每个元素包含：
- stock_name: 股票简称（如 "宁德时代"）
- stock_code: A股代码（如 "300750"，若未提及则为 null）
- industry: 所属行业或板块（如 "新能源"、"半导体"）
- theme: 报告中提到的核心投资题材（1-3个关键词，用逗号分隔，如 "固态电池,出口"）

示例输出：
[
  {"stock_name": "宁德时代", "stock_code": "300750", "industry": "新能源", "theme": "固态电池,储能"},
  {"stock_name": "比亚迪", "stock_code": "002594", "industry": "新能源汽车", "theme": "出口,智驾"}
]

只输出 JSON 数组，不要有任何其他文字。如果没有找到股票，输出空数组 []。"""


# ── 查询已处理的 et_id（用于跳过重复）────────────────────────────

def _get_scanned_et_ids(scan_date: date) -> set:
    """查本地表，取当日已扫描过的 extracted_text_id 集合"""
    rows = execute_query(
        "SELECT DISTINCT extracted_text_id FROM robust_kline_mentions WHERE scan_date=%s",
        [str(scan_date)],
    )
    return {r["extracted_text_id"] for r in (rows or [])}


# ── 从 source_documents 查标题/帖主匹配 ──────────────────────────

def _fetch_reports_for_date(scan_date: date) -> list[dict]:
    """从云端查满足关键字 OR 帖主的当日报告（extracted_texts + source_documents）

    匹配逻辑：标题含关键字 OR 帖主是夏天/白白
    """
    # 标题关键字条件
    like_parts = " OR ".join([f"sd.title LIKE %s" for _ in _KEYWORDS])
    params = [f"%{kw}%" for kw in _KEYWORDS]

    # 帖主条件
    author_parts = " OR ".join(["sd.author = %s" for _ in _AUTHORS])
    params += list(_AUTHORS)

    # 日期
    params += [str(scan_date), str(scan_date)]

    sql = f"""
        SELECT et.id AS extracted_text_id,
               sd.title AS source_title,
               DATE(COALESCE(et.publish_time, sd.publish_date)) AS publish_date,
               et.full_text
        FROM extracted_texts et
        JOIN source_documents sd ON et.source_doc_id = sd.id
        WHERE (({like_parts}) OR ({author_parts}))
          AND DATE(COALESCE(et.publish_time, sd.publish_date)) BETWEEN %s AND %s
          AND et.full_text IS NOT NULL
          AND LENGTH(et.full_text) > 100
        ORDER BY et.id
        LIMIT 100
    """
    try:
        rows = execute_cloud_query(sql, params)
        return rows or []
    except Exception as e:
        logger.error(f"[Scanner] 查询云端报告失败: {e}")
        return []


# ── stock_name → stock_code 匹配 ────────────────────────────────────

def _build_name_code_map() -> dict[str, str]:
    """从本地 stock_info 建立 name→code 映射"""
    rows = execute_query("SELECT stock_code, stock_name FROM stock_info")
    m: dict[str, str] = {}
    for r in (rows or []):
        if r.get("stock_name"):
            m[r["stock_name"]] = r["stock_code"]
    return m


# ── 主扫描函数 ───────────────────────────────────────────────────────

def _ensure_tables():
    """确保本地表存在（用 pymysql 直连避免 %s 注释被误解析）"""
    import pymysql
    from config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
                           password=MYSQL_PASSWORD, database=MYSQL_DB, charset="utf8mb4")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS robust_kline_mentions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    extracted_text_id INT NOT NULL,
                    source_title VARCHAR(500),
                    publish_date DATE,
                    stock_name VARCHAR(50) NOT NULL,
                    stock_code VARCHAR(20),
                    industry VARCHAR(100),
                    theme VARCHAR(200),
                    highlight TEXT,
                    scan_date DATE NOT NULL,
                    created_at DATETIME DEFAULT NOW(),
                    INDEX idx_rkm_scan_date (scan_date),
                    INDEX idx_rkm_stock_code (stock_code),
                    INDEX idx_rkm_stock_name (stock_name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS robust_kline_candidates (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    stock_code VARCHAR(20) NOT NULL,
                    stock_name VARCHAR(50),
                    industry VARCHAR(100),
                    match_type TINYINT NOT NULL,
                    yang_months VARCHAR(200),
                    gain_pct FLOAT,
                    latest_price FLOAT,
                    mention_count INT DEFAULT 1,
                    highlight TEXT,
                    scan_date DATE NOT NULL,
                    created_at DATETIME DEFAULT NOW(),
                    INDEX idx_rkc_scan_date (scan_date),
                    INDEX idx_rkc_stock_code (stock_code),
                    UNIQUE KEY uk_rkc (stock_code, scan_date, match_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
        conn.commit()
    finally:
        conn.close()


def scan_today(scan_date: date = None) -> dict:
    """扫描今日报告，提取股票写入 robust_kline_mentions

    Returns:
        {"reports": n, "stocks_extracted": n, "inserted": n, "errors": [...]}
    """
    if scan_date is None:
        scan_date = date.today()

    _ensure_tables()
    name_code_map = _build_name_code_map()

    # 查已扫描过的 et_id，下午跑时跳过早上已处理的
    scanned_ids = _get_scanned_et_ids(scan_date)

    all_reports = _fetch_reports_for_date(scan_date)
    reports = [r for r in all_reports if r["extracted_text_id"] not in scanned_ids]
    skipped = len(all_reports) - len(reports)
    logger.info(f"[Scanner] 找到 {len(all_reports)} 篇报告, 跳过已扫描 {skipped} 篇, 待处理 {len(reports)} 篇 (scan_date={scan_date})")

    total_inserted = 0
    total_stocks = 0
    errors = []

    for report in reports:
        et_id = report["extracted_text_id"]
        source_title = report.get("source_title") or ""
        publish_date = report.get("publish_date")
        full_text = report.get("full_text") or ""

        try:
            raw = _call_deepseek(_EXTRACT_PROMPT, full_text)
            # 提取 JSON 数组
            m = re.search(r"\[.*\]", raw, re.DOTALL)
            if not m:
                continue
            stocks = json.loads(m.group(0))
        except Exception as e:
            logger.warning(f"[Scanner] et_id={et_id} DeepSeek 提取失败: {e}")
            errors.append({"et_id": et_id, "error": str(e)})
            continue

        rows_to_insert = []
        for s in stocks:
            if not isinstance(s, dict):
                continue
            sname = (s.get("stock_name") or "").strip()
            if not sname:
                continue
            scode = (s.get("stock_code") or "").strip() or name_code_map.get(sname)
            industry = (s.get("industry") or "").strip()[:100]
            theme = (s.get("theme") or "").strip()[:200]
            rows_to_insert.append((
                et_id, source_title[:500], publish_date,
                sname[:50], scode[:20] if scode else None,
                industry, theme, None, str(scan_date),
            ))

        if rows_to_insert:
            try:
                execute_many(
                    """INSERT INTO robust_kline_mentions
                       (extracted_text_id, source_title, publish_date,
                        stock_name, stock_code, industry, theme, highlight, scan_date)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    rows_to_insert,
                )
                total_inserted += len(rows_to_insert)
                total_stocks += len(rows_to_insert)
                logger.info(f"[Scanner] et_id={et_id} 插入 {len(rows_to_insert)} 条")
            except Exception as e:
                logger.warning(f"[Scanner] et_id={et_id} 写入失败: {e}")
                errors.append({"et_id": et_id, "error": str(e)})

    return {
        "reports": len(reports),
        "stocks_extracted": total_stocks,
        "inserted": total_inserted,
        "errors": errors,
    }

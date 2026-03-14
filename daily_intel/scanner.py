"""daily_intel/scanner.py — 扫描 zsxq + 处理手动录入 → daily_intel_stocks

全流程：
  1. scan_zsxq_today: 云端 extracted_texts → 提取 → 写 daily_intel_stocks
  2. process_manual_items: 云端 daily_intel_items(pending) → 提取 → 写 daily_intel_stocks
  3. run_daily_intel_pipeline: 上述两步 + 触发爸爸备选筛选
"""
import logging
from datetime import date

from utils.db_utils import execute_query, execute_cloud_query, execute_cloud_insert, execute_cloud_many, execute_insert

logger = logging.getLogger(__name__)


# ── 建表 ─────────────────────────────────────────────────────────

def _ensure_tables():
    """确保云端两张新表存在（已迁移到云端，建表用云端连接）"""
    execute_cloud_insert("""
        CREATE TABLE IF NOT EXISTS daily_intel_items (
            id INT AUTO_INCREMENT PRIMARY KEY,
            input_text TEXT NOT NULL,
            input_date DATE NOT NULL,
            process_status ENUM('pending','done','error') DEFAULT 'pending',
            created_at DATETIME DEFAULT NOW()
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    execute_cloud_insert("""
        CREATE TABLE IF NOT EXISTS daily_intel_stocks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            scan_date DATE NOT NULL,
            source_type ENUM('zsxq','manual') NOT NULL,
            source_id BIGINT,
            source_title VARCHAR(500),
            stock_name VARCHAR(50) NOT NULL,
            stock_code VARCHAR(20),
            industry VARCHAR(100),
            business_desc VARCHAR(200),
            event_type VARCHAR(50),
            event_summary TEXT,
            created_at DATETIME DEFAULT NOW(),
            INDEX idx_dis_scan_date (scan_date),
            INDEX idx_dis_stock_code (stock_code),
            INDEX idx_dis_source (source_type, source_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


# ── 查文档 doc_type ────────────────────────────────────────────────

def _get_doc_type(source_doc_id: int) -> str:
    """从 source_documents 获取 doc_type（参数是 source_documents.id）"""
    rows = execute_cloud_query(
        "SELECT doc_type FROM source_documents WHERE id = %s",
        [source_doc_id],
    )
    return rows[0]["doc_type"] if rows else ""


def _get_extracted_text_id(source_doc_id: int) -> int | None:
    """从 source_doc_id 查对应的 extracted_texts.id"""
    rows = execute_cloud_query(
        "SELECT id FROM extracted_texts WHERE source_doc_id = %s LIMIT 1",
        [source_doc_id],
    )
    return rows[0]["id"] if rows else None


# ── stock_name → stock_code 映射 ─────────────────────────────────

def _build_name_code_map() -> dict[str, str]:
    rows = execute_query("SELECT stock_code, stock_name FROM stock_info")
    return {r["stock_name"]: r["stock_code"] for r in (rows or []) if r.get("stock_name")}


# ── 写入 daily_intel_stocks ───────────────────────────────────────

def _insert_stocks(stocks: list[dict], scan_date: date, source_type: str,
                   source_id: int, source_title: str, name_code_map: dict) -> int:
    """将提取结果批量写入 daily_intel_stocks，返回插入行数"""
    rows = []
    for s in stocks:
        sname = s.get("stock_name") or ""
        scode = s.get("stock_code") or name_code_map.get(sname) or None
        rows.append((
            str(scan_date), source_type, source_id, source_title[:500] if source_title else None,
            sname[:50], scode[:20] if scode else None,
            (s.get("industry") or "")[:100],
            (s.get("business_desc") or "")[:200],
            (s.get("event_type") or "")[:50],
            s.get("event_summary") or None,
        ))
    if not rows:
        return 0
    try:
        execute_cloud_many(
            """INSERT INTO daily_intel_stocks
               (scan_date, source_type, source_id, source_title,
                stock_name, stock_code, industry, business_desc, event_type, event_summary)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            rows,
        )
        return len(rows)
    except Exception as e:
        logger.warning(f"[Scanner] 写入 daily_intel_stocks 失败: {e}")
        return 0


# ── 查已扫描的 zsxq source_id ────────────────────────────────────

def _get_scanned_zsxq_ids(scan_date: date) -> set:
    rows = execute_cloud_query(
        """SELECT DISTINCT source_id FROM daily_intel_stocks
           WHERE scan_date=%s AND source_type='zsxq'""",
        [str(scan_date)],
    )
    return {r["source_id"] for r in (rows or [])}


# ── 云端查 zsxq 当日报告 ──────────────────────────────────────────

# PDF 必须包含这些关键词之一才纳入 daily intel
_PDF_TITLE_KEYWORDS = ["早知道", "龙虎榜", "脱水研报", "调研日报", "评级日报", "强势股脱水", "公告全知道", "机构龙虎榜"]


def _fetch_zsxq_reports(scan_date: date) -> list[dict]:
    """直接读 source_documents：txt/mixed 用 text_content，audio/pdf 用 extracted_text
    PDF 仅纳入 title 含指定关键词的帖子（早知道/龙虎榜/脱水研报等）。
    """
    sql = """
        SELECT id AS extracted_text_id,
               title AS source_title,
               file_type,
               COALESCE(
                   NULLIF(TRIM(extracted_text), ''),
                   NULLIF(TRIM(text_content), '')
               ) AS full_text
        FROM source_documents
        WHERE source = 'zsxq'
          AND publish_date = %s
          AND (
              (file_type IN ('txt', 'mixed') AND text_content IS NOT NULL AND LENGTH(TRIM(text_content)) > 100)
              OR
              (file_type IN ('audio', 'mp3', 'pdf') AND extracted_text IS NOT NULL AND LENGTH(TRIM(extracted_text)) > 100)
          )
        ORDER BY id
        LIMIT 500
    """
    try:
        rows = execute_cloud_query(sql, [str(scan_date)]) or []
        result = []
        for r in rows:
            if not r.get("full_text") or len(r["full_text"].strip()) <= 100:
                continue
            # PDF 需要 title 包含指定关键词才纳入
            if r.get("file_type") == "pdf":
                title = r.get("source_title") or ""
                if not any(kw in title for kw in _PDF_TITLE_KEYWORDS):
                    continue
            result.append(r)
        return result
    except Exception as e:
        logger.error(f"[Scanner] 查询云端 zsxq 报告失败: {e}")
        return []


# ── 主函数 ───────────────────────────────────────────────────────

def scan_zsxq_today(scan_date: date = None, skip_social_post: bool = False) -> dict:
    """扫描当日 zsxq extracted_texts → 提取 → 写 daily_intel_stocks

    Args:
        skip_social_post: 跳过 social_post 拆条流程（避免加载 BERT），改用普通提取

    Returns:
        {"reports": n, "extracted": n, "inserted": n, "errors": [...]}
    """
    if scan_date is None:
        scan_date = date.today()

    _ensure_tables()
    name_code_map = _build_name_code_map()
    scanned_ids = _get_scanned_zsxq_ids(scan_date)

    all_reports = _fetch_zsxq_reports(scan_date)
    reports = [r for r in all_reports if r["extracted_text_id"] not in scanned_ids]
    logger.info(
        f"[Scanner] zsxq 找到 {len(all_reports)} 篇, "
        f"已扫描 {len(scanned_ids)} 篇, 待处理 {len(reports)} 篇"
    )

    from daily_intel.extractor import extract_stocks_from_text

    total_inserted = 0
    total_extracted = 0
    errors = []

    for report in reports:
        sd_id = report["extracted_text_id"]  # 实际是 source_documents.id
        source_title = report.get("source_title") or ""
        full_text = report.get("full_text") or ""

        try:
            # social_post 走拆条流程（幂等，拆条+摘要+stocks一次完成）
            doc_type = _get_doc_type(sd_id)
            if doc_type == "social_post" and not skip_social_post:
                real_et_id = _get_extracted_text_id(sd_id)
                if real_et_id:
                    from cleaning.content_summarizer import ensure_social_post_split
                    result = ensure_social_post_split(real_et_id)
                    stocks = result.get("stocks") or []
                    # ensure_social_post_split 已写 daily_intel_stocks，无需再写
                    total_extracted += len(stocks)
                    if not result.get("from_cache"):
                        total_inserted += len(stocks)
                    logger.info(
                        f"[Scanner] zsxq sd_id={sd_id} et_id={real_et_id} social_post "
                        f"拆条={len(result.get('items', []))} stocks={len(stocks)} "
                        f"cache={result.get('from_cache')}"
                    )
                else:
                    logger.warning(f"[Scanner] zsxq sd_id={sd_id} 无对应 extracted_texts，跳过拆条")
            else:
                stocks = extract_stocks_from_text(full_text, source_title)
                total_extracted += len(stocks)
                if stocks:
                    n = _insert_stocks(stocks, scan_date, "zsxq", sd_id, source_title, name_code_map)
                    total_inserted += n
                    logger.info(f"[Scanner] zsxq sd_id={sd_id} 提取 {len(stocks)} 条, 写入 {n} 条")
        except Exception as e:
            logger.warning(f"[Scanner] zsxq sd_id={sd_id} 处理失败: {e}")
            errors.append({"sd_id": sd_id, "error": str(e)})

    return {
        "reports": len(reports),
        "extracted": total_extracted,
        "inserted": total_inserted,
        "errors": errors,
    }


def process_manual_items() -> dict:
    """处理 pending 的手动录入条目 → 提取 → 写 daily_intel_stocks

    Returns:
        {"processed": n, "inserted": n, "errors": [...]}
    """
    _ensure_tables()
    name_code_map = _build_name_code_map()

    pending = execute_cloud_query(
        "SELECT id, input_text, input_date FROM daily_intel_items WHERE process_status='pending'"
    )
    if not pending:
        return {"processed": 0, "inserted": 0, "errors": []}

    from daily_intel.extractor import extract_stocks_from_text

    processed = 0
    total_inserted = 0
    errors = []

    for item in pending:
        item_id = item["id"]
        input_text = item.get("input_text") or ""
        input_date = item.get("input_date") or date.today()

        try:
            stocks = extract_stocks_from_text(input_text)
            if stocks:
                n = _insert_stocks(stocks, input_date, "manual", item_id, None, name_code_map)
                total_inserted += n
            execute_cloud_insert(
                "UPDATE daily_intel_items SET process_status='done' WHERE id=%s",
                [item_id],
            )
            processed += 1
            logger.info(f"[Scanner] manual item_id={item_id} 提取 {len(stocks)} 条, 写入 {total_inserted} 条")
        except Exception as e:
            logger.warning(f"[Scanner] manual item_id={item_id} 处理失败: {e}")
            execute_cloud_insert(
                "UPDATE daily_intel_items SET process_status='error' WHERE id=%s",
                [item_id],
            )
            errors.append({"item_id": item_id, "error": str(e)})

    return {"processed": processed, "inserted": total_inserted, "errors": errors}


def run_daily_intel_pipeline(scan_date: date = None) -> dict:
    """全流程：扫描 zsxq + 处理手动录入 + 触发爸爸备选筛选

    Returns:
        {"zsxq": {...}, "manual": {...}, "filter": {...}}
    """
    if scan_date is None:
        scan_date = date.today()

    result = {}

    logger.info(f"[Pipeline] 开始每日情报全流程 scan_date={scan_date}")

    try:
        result["zsxq"] = scan_zsxq_today(scan_date)
        logger.info(f"[Pipeline] zsxq 扫描完成: {result['zsxq']}")
    except Exception as e:
        logger.exception(f"[Pipeline] zsxq 扫描失败: {e}")
        result["zsxq"] = {"error": str(e)}

    try:
        result["manual"] = process_manual_items()
        logger.info(f"[Pipeline] 手动录入处理完成: {result['manual']}")
    except Exception as e:
        logger.exception(f"[Pipeline] 手动录入处理失败: {e}")
        result["manual"] = {"error": str(e)}

    try:
        from robust_kline.filter import filter_candidates
        result["filter"] = filter_candidates(scan_date)
        logger.info(f"[Pipeline] 爸爸备选筛选完成: {result['filter']}")
    except Exception as e:
        logger.exception(f"[Pipeline] 爸爸备选筛选失败: {e}")
        result["filter"] = {"error": str(e)}

    return result

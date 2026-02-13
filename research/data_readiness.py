"""数据充分性检查 + 自动补数据 — 深度研究前置模块"""
import logging
from datetime import datetime, timedelta
from utils.db_utils import execute_query

logger = logging.getLogger(__name__)

# 各维度最低阈值
THRESHOLDS = {
    "info":       {"min": 1,  "label": "基础信息"},
    "daily":      {"min": 60, "label": "日线行情"},
    "capital":    {"min": 20, "label": "资金流向"},
    "financials": {"min": 2,  "label": "财务数据"},
    "news":       {"min": 3,  "label": "相关新闻"},
    "reports":    {"min": 0,  "label": "券商研报"},
}


def check_stock_readiness(stock_code):
    """检查个股研究数据充分性，返回各维度状态"""
    dims = {}

    # 基础信息：必须存在且有行业/市值
    info_rows = execute_query(
        "SELECT * FROM stock_info WHERE stock_code=?", [stock_code]
    )
    has_info = (
        len(info_rows) > 0
        and info_rows[0].get("industry_l1")
        and info_rows[0].get("market_cap")
    )
    dims["info"] = {
        "ok": bool(has_info),
        "count": 1 if has_info else 0,
        "min": THRESHOLDS["info"]["min"],
        "detail": "基础信息完整" if has_info else "缺少行业或市值",
    }

    # 日线行情 ≥60个交易日
    daily_rows = execute_query(
        "SELECT COUNT(*) as cnt FROM stock_daily WHERE stock_code=?",
        [stock_code],
    )
    daily_cnt = daily_rows[0]["cnt"] if daily_rows else 0
    dims["daily"] = {
        "ok": daily_cnt >= THRESHOLDS["daily"]["min"],
        "count": daily_cnt,
        "min": THRESHOLDS["daily"]["min"],
        "detail": f"{daily_cnt}个交易日",
    }

    # 资金流向 ≥20个交易日
    cap_rows = execute_query(
        "SELECT COUNT(*) as cnt FROM capital_flow WHERE stock_code=?",
        [stock_code],
    )
    cap_cnt = cap_rows[0]["cnt"] if cap_rows else 0
    dims["capital"] = {
        "ok": cap_cnt >= THRESHOLDS["capital"]["min"],
        "count": cap_cnt,
        "min": THRESHOLDS["capital"]["min"],
        "detail": f"{cap_cnt}个交易日",
    }

    # 财务数据 ≥2个报告期
    fin_rows = execute_query(
        "SELECT COUNT(*) as cnt FROM financial_reports WHERE stock_code=?",
        [stock_code],
    )
    fin_cnt = fin_rows[0]["cnt"] if fin_rows else 0
    dims["financials"] = {
        "ok": fin_cnt >= THRESHOLDS["financials"]["min"],
        "count": fin_cnt,
        "min": THRESHOLDS["financials"]["min"],
        "detail": f"{fin_cnt}个报告期",
    }

    # 相关新闻 ≥3条 (via item_companies -> cleaned_items)
    news_rows = execute_query(
        """SELECT COUNT(*) as cnt FROM cleaned_items ci
           JOIN item_companies ic ON ic.cleaned_item_id = ci.id
           WHERE ic.stock_code=?""",
        [stock_code],
    )
    news_cnt = news_rows[0]["cnt"] if news_rows else 0
    dims["news"] = {
        "ok": news_cnt >= THRESHOLDS["news"]["min"],
        "count": news_cnt,
        "min": THRESHOLDS["news"]["min"],
        "detail": f"{news_cnt}条新闻",
    }

    # 券商研报 ≥0（不阻塞）
    rpt_rows = execute_query(
        "SELECT COUNT(*) as cnt FROM research_reports WHERE stock_code=?",
        [stock_code],
    )
    rpt_cnt = rpt_rows[0]["cnt"] if rpt_rows else 0
    dims["reports"] = {
        "ok": True,  # min=0, 永远达标但影响置信度
        "count": rpt_cnt,
        "min": THRESHOLDS["reports"]["min"],
        "detail": f"{rpt_cnt}篇研报",
    }

    # 汇总
    missing = [k for k, v in dims.items() if not v["ok"]]
    # 置信度：达标维度占比，研报额外加分
    ok_count = sum(1 for v in dims.values() if v["ok"])
    confidence = ok_count / len(dims)
    if rpt_cnt >= 3:
        confidence = min(1.0, confidence + 0.05)

    return {
        "ready": len(missing) == 0,
        "confidence": round(confidence, 2),
        "dimensions": dims,
        "missing": missing,
    }


def fetch_missing_data(stock_code, missing_dims, progress_callback=None):
    """按需补充缺失维度的数据，返回各维度补充结果"""
    results = {}

    for dim in missing_dims:
        label = THRESHOLDS.get(dim, {}).get("label", dim)
        if progress_callback:
            progress_callback(f"正在补充{label}...")

        try:
            if dim == "info":
                results[dim] = _fetch_info(stock_code)
            elif dim == "daily":
                results[dim] = _fetch_daily(stock_code)
            elif dim == "capital":
                results[dim] = _fetch_capital(stock_code)
            elif dim == "financials":
                results[dim] = _fetch_financials(stock_code)
            elif dim == "news":
                results[dim] = _fetch_news(stock_code)
            else:
                results[dim] = 0
        except Exception as e:
            logger.error(f"补充{label}失败: {e}")
            results[dim] = 0

    return results


def _fetch_info(stock_code):
    """补充基础信息：先确保stock_info存在，再拉详情"""
    from ingestion.akshare_source import fetch_stock_info, fetch_stock_detail

    # 检查stock_info是否存在
    rows = execute_query(
        "SELECT stock_code FROM stock_info WHERE stock_code=?", [stock_code]
    )
    if not rows:
        fetch_stock_info()

    ok = fetch_stock_detail(stock_code)
    return 1 if ok else 0


def _fetch_daily(stock_code):
    """补充日线行情"""
    from ingestion.akshare_source import fetch_stock_daily

    start = (datetime.now() - timedelta(days=180)).strftime("%Y%m%d")
    return fetch_stock_daily(stock_code, start_date=start)


def _fetch_capital(stock_code):
    """补充资金流向"""
    from ingestion.akshare_source import fetch_capital_flow

    return fetch_capital_flow(stock_code)


def _fetch_financials(stock_code):
    """补充财务数据"""
    from ingestion.akshare_source import fetch_financial_data

    return fetch_financial_data(stock_code)


def _fetch_news(stock_code):
    """补充个股新闻：采集 + 清洗"""
    from ingestion.jasper_source import JasperSource
    from cleaning.claude_processor import clean_single_item

    # 1. 采集个股新闻到 raw_items
    src = JasperSource()
    cutoff = datetime.now() - timedelta(hours=72)
    raw_count = src._fetch_stock_news(stock_code, cutoff, None, "深度研究补充")
    logger.info(f"{stock_code} 补充采集新闻: {raw_count}条")

    # 2. 清洗新采集的 pending raw_items
    pending = execute_query(
        """SELECT id FROM raw_items
           WHERE processing_status='pending'
           ORDER BY fetched_at DESC LIMIT 10"""
    )
    cleaned = 0
    for item in pending:
        try:
            cid = clean_single_item(item["id"])
            if cid:
                cleaned += 1
        except Exception as e:
            logger.warning(f"清洗raw_item {item['id']}失败: {e}")

    logger.info(f"{stock_code} 清洗完成: {cleaned}/{len(pending)}条")
    return cleaned


def ensure_stock_data_ready(stock_code, max_rounds=2, progress_callback=None):
    """确保数据充分的主循环：check → fetch → re-check"""
    for round_num in range(1, max_rounds + 1):
        if progress_callback:
            progress_callback(f"数据充分性检查 (第{round_num}轮)...")

        readiness = check_stock_readiness(stock_code)

        if readiness["ready"]:
            logger.info(f"{stock_code} 数据充分 (第{round_num}轮), 置信度={readiness['confidence']}")
            return readiness

        logger.info(
            f"{stock_code} 第{round_num}轮检查: 缺失={readiness['missing']}, "
            f"置信度={readiness['confidence']}"
        )

        # 最后一轮不再补数据，直接返回
        if round_num == max_rounds:
            logger.warning(f"{stock_code} 经过{max_rounds}轮仍不充分: {readiness['missing']}")
            return readiness

        # 补数据
        if progress_callback:
            progress_callback(f"正在自动补充缺失数据: {readiness['missing']}...")

        fetch_results = fetch_missing_data(
            stock_code, readiness["missing"], progress_callback
        )
        logger.info(f"{stock_code} 补数据结果: {fetch_results}")

    return readiness

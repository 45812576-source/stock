"""robust_kline/highlight.py — 投资亮点填充

对 robust_kline_mentions 和 robust_kline_candidates 中 highlight 为空的股票，
调用 hybrid_search 检索相关信息，再用 DeepSeek 总结为 2-3 句投资亮点。
"""
import logging
import threading
from datetime import date

from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)

# ── DeepSeek 客户端（lazy singleton）────────────────────────────────

_deepseek_client = None
_deepseek_lock = threading.Lock()


def _get_deepseek():
    global _deepseek_client
    if _deepseek_client is None:
        with _deepseek_lock:
            if _deepseek_client is None:
                from openai import OpenAI
                from utils.db_utils import execute_cloud_query
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


def _call_deepseek(context_text: str, stock_name: str) -> str:
    system_prompt = (
        "你是专业的A股投资研究助手。请根据以下研究材料，"
        f"为【{stock_name}】写出 2-3 句简洁的投资亮点摘要。"
        "重点突出核心逻辑和催化剂，用中文，不超过150字。"
    )
    if len(context_text) > 3000:
        context_text = context_text[:3000] + "...[已截断]"
    client = _get_deepseek()
    resp = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": context_text},
        ],
        max_tokens=300,
        temperature=0.3,
        timeout=60,
    )
    return resp.choices[0].message.content.strip()


# ── 核心逻辑 ─────────────────────────────────────────────────────────

def _search_and_summarize(stock_name: str):
    """hybrid_search → DeepSeek 总结"""
    try:
        from retrieval.hybrid import hybrid_search
        result = hybrid_search(
            query=f"{stock_name} 投资亮点 核心逻辑",
            context={"entity_names": [stock_name]},
            strategy="auto",
            max_context_chars=3000,
            top_k=8,
        )
        ctx = (result.merged_context or "").strip()
        if not ctx or len(ctx) < 50:
            return None
        return _call_deepseek(ctx, stock_name)
    except Exception as e:
        logger.warning(f"[Highlight] {stock_name} hybrid_search 失败: {e}")
        return None


def fill_highlights(scan_date: date = None) -> dict:
    """填充 mentions + candidates 中 highlight 为空的记录

    Returns:
        {"mentions_filled": n, "candidates_filled": n, "errors": n}
    """
    if scan_date is None:
        scan_date = date.today()

    mentions_filled = 0
    candidates_filled = 0
    errors = 0

    # ── 1. robust_kline_mentions ────────────────────────────────────
    ment_rows = execute_query(
        """SELECT id, stock_name FROM robust_kline_mentions
           WHERE scan_date = %s AND (highlight IS NULL OR highlight = '')
           GROUP BY id, stock_name""",
        [str(scan_date)],
    )
    processed_names: dict[str, str | None] = {}

    for row in (ment_rows or []):
        stock_name = row["stock_name"]
        if stock_name in processed_names:
            hl = processed_names[stock_name]
        else:
            hl = _search_and_summarize(stock_name)
            processed_names[stock_name] = hl

        if hl:
            try:
                execute_insert(
                    "UPDATE robust_kline_mentions SET highlight=%s WHERE id=%s",
                    [hl, row["id"]],
                )
                mentions_filled += 1
            except Exception as e:
                logger.warning(f"[Highlight] mention id={row['id']} 写入失败: {e}")
                errors += 1

    # ── 2. robust_kline_candidates ──────────────────────────────────
    cand_rows = execute_query(
        """SELECT id, stock_name FROM robust_kline_candidates
           WHERE scan_date = %s AND (highlight IS NULL OR highlight = '')""",
        [str(scan_date)],
    )
    for row in (cand_rows or []):
        stock_name = row["stock_name"] or ""
        if not stock_name:
            continue
        if stock_name in processed_names:
            hl = processed_names[stock_name]
        else:
            hl = _search_and_summarize(stock_name)
            processed_names[stock_name] = hl

        if hl:
            try:
                execute_insert(
                    "UPDATE robust_kline_candidates SET highlight=%s WHERE id=%s",
                    [hl, row["id"]],
                )
                candidates_filled += 1
            except Exception as e:
                logger.warning(f"[Highlight] candidate id={row['id']} 写入失败: {e}")
                errors += 1

    logger.info(
        f"[Highlight] mentions_filled={mentions_filled}, "
        f"candidates_filled={candidates_filled}, errors={errors}"
    )
    return {
        "mentions_filled": mentions_filled,
        "candidates_filled": candidates_filled,
        "errors": errors,
    }

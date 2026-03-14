"""theme_merger.py — 每日 Daily Intel 主题归纳与合并

流程：
1. 读当天 daily_intel_stocks 的所有 event_summary
2. 参考近30天已有主题名（用于跨日一致性）
3. AI 归纳出 ≤15 个主题，每个主题含名称、投资逻辑、涉及股票、提及次数
4. 写入 daily_intel_themes 表（云端）
"""

import json
import logging
from datetime import date as date_cls, timedelta

logger = logging.getLogger(__name__)

# ── 建表 ─────────────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS daily_intel_themes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    scan_date DATE NOT NULL,
    theme_name VARCHAR(100) NOT NULL,
    theme_logic TEXT,
    stocks JSON COMMENT '涉及股票名列表',
    mention_count INT DEFAULT 0 COMMENT '该主题当日提及条目数',
    created_at DATETIME DEFAULT NOW(),
    INDEX idx_dit_scan_date (scan_date),
    INDEX idx_dit_theme (theme_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _ensure_table():
    from utils.db_utils import execute_cloud_query
    try:
        execute_cloud_query(_CREATE_TABLE_SQL)
    except Exception as e:
        logger.warning(f"[ThemeMerger] 建表失败（可能已存在）: {e}")


# ── 读取数据 ──────────────────────────────────────────────────────

def _fetch_day_summaries(scan_date: str) -> list[dict]:
    """读当天股票情报，每只股票取最具代表性的一条 summary，最多200只"""
    from utils.db_utils import execute_cloud_query
    rows = execute_cloud_query(
        """SELECT stock_name, stock_code, industry,
                  SUBSTRING(MAX(event_summary), 1, 100) AS summary,
                  COUNT(*) AS cnt
           FROM daily_intel_stocks
           WHERE scan_date = %s
             AND event_summary IS NOT NULL AND event_summary != ''
             AND stock_name IS NOT NULL AND stock_name != ''
           GROUP BY stock_name, stock_code, industry
           ORDER BY cnt DESC
           LIMIT 200""",
        [scan_date],
    ) or []
    return [dict(r) for r in rows]


def _fetch_recent_theme_names(days: int = 30) -> list[str]:
    """读近N天已有主题名，供 AI 参考保持一致性"""
    from utils.db_utils import execute_cloud_query
    rows = execute_cloud_query(
        """SELECT theme_name, MAX(scan_date) AS last_seen
           FROM daily_intel_themes
           WHERE scan_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
           GROUP BY theme_name
           ORDER BY last_seen DESC
           LIMIT 60""",
        [days],
    ) or []
    return [r["theme_name"] for r in rows]


# ── AI 归纳 ──────────────────────────────────────────────────────

def _ai_merge_themes(summaries: list[dict], existing_names: list[str]) -> list[dict]:
    """调用 AI 将当天所有 event_summary 归纳为 ≤15 个主题。

    Returns:
        [{"theme_name": str, "theme_logic": str, "stocks": [str], "mention_count": int}, ...]
    """
    from utils.model_router import call_model_json

    # 构建输入文本（限制总长度避免超 token）
    lines = []
    total_chars = 0
    for r in summaries:
        line = f"【{r['stock_name']}｜{r.get('industry','')}】{(r.get('summary') or '')[:120]}"
        total_chars += len(line)
        if total_chars > 60000:
            break
        lines.append(line)

    stock_block = "\n".join(lines)

    existing_hint = ""
    if existing_names:
        existing_hint = f"\n\n参考近期已有主题名（尽量复用，保持跨日一致性）：\n" + "、".join(existing_names[:20])

    system = "你是A股投资研究专家，擅长从新闻事件中归纳投资主题。只输出合法JSON，不输出任何解释。"
    user = f"""以下是今日股票情报摘要（格式：【股票名｜行业】事件摘要）：

{stock_block}{existing_hint}

请将上述股票情报归纳为不超过15个投资主题，要求：
1. 主题名简洁（4-10字中文），如"AI算力扩张"、"半导体国产替代"、"储能需求爆发"
2. 同一主题下的股票逻辑相近
3. 每个主题至少包含1只股票
4. mention_count = 该主题涵盖的情报条目总数（即对应股票的 cnt 之和）

输出 JSON 数组：
[
  {{
    "theme_name": "主题名",
    "theme_logic": "一句话投资逻辑（30字内）",
    "stocks": ["股票A", "股票B", ...],
    "mention_count": 12
  }},
  ...
]"""

    try:
        result = call_model_json("kg", system, user, max_tokens=4000, timeout=180)
        if isinstance(result, list):
            return result
        for v in result.values():
            if isinstance(v, list):
                return v
    except Exception as e:
        logger.warning(f"[ThemeMerger] AI归纳失败: {e}")
    return []


# ── 写入 ─────────────────────────────────────────────────────────

def _save_themes(scan_date: str, themes: list[dict]):
    """将主题写入 daily_intel_themes，先删当天旧数据再插入"""
    from utils.db_utils import execute_cloud_query, execute_cloud_insert

    # 删当天旧数据（幂等）
    execute_cloud_query(
        "DELETE FROM daily_intel_themes WHERE scan_date = %s",
        [scan_date],
    )

    for t in themes:
        stocks_json = json.dumps(t.get("stocks") or [], ensure_ascii=False)
        execute_cloud_insert(
            """INSERT INTO daily_intel_themes
               (scan_date, theme_name, theme_logic, stocks, mention_count)
               VALUES (%s, %s, %s, %s, %s)""",
            [
                scan_date,
                (t.get("theme_name") or "")[:100],
                t.get("theme_logic") or "",
                stocks_json,
                int(t.get("mention_count") or 0),
            ],
        )
    logger.info(f"[ThemeMerger] 写入 {len(themes)} 个主题 scan_date={scan_date}")


# ── 主入口 ────────────────────────────────────────────────────────

def run_theme_merge(scan_date: str = None) -> dict:
    """归纳当天 Daily Intel 主题并写入 daily_intel_themes。

    Returns: {"themes": int, "stocks_covered": int}
    """
    if not scan_date:
        scan_date = str(date_cls.today())

    logger.info(f"[ThemeMerger] 开始归纳 scan_date={scan_date}")

    _ensure_table()

    summaries = _fetch_day_summaries(scan_date)
    if not summaries:
        logger.info("[ThemeMerger] 当天无数据，跳过")
        return {"themes": 0, "stocks_covered": 0}

    existing_names = _fetch_recent_theme_names()
    themes = _ai_merge_themes(summaries, existing_names)

    if not themes:
        logger.warning("[ThemeMerger] AI未返回任何主题")
        return {"themes": 0, "stocks_covered": 0}

    _save_themes(scan_date, themes)

    stocks_covered = len({s for t in themes for s in (t.get("stocks") or [])})
    result = {"themes": len(themes), "stocks_covered": stocks_covered}
    logger.info(f"[ThemeMerger] 完成: {result}")
    return result

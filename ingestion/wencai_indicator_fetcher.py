"""问财行业指标采集模块

遍历 industry_indicator_dict.yaml → 查询问财 → LLM 结构化提取 → 写入 industry_indicators

反爬策略：随机间隔 8~18s、每10条额外暂停、查询顺序随机化、指数退避、每日限额 60 次
"""
import json
import logging
import random
import time
import threading
from typing import Optional
import yaml
from datetime import datetime, date
from pathlib import Path

from config import (
    PROJECT_ROOT,
    WENCAI_MIN_DELAY, WENCAI_MAX_DELAY, WENCAI_MAX_DAILY_QUERIES,
)
from utils.db_utils import (
    execute_cloud_query, execute_cloud_insert,
    upsert_industry_indicator,
)

logger = logging.getLogger(__name__)

# ── 反爬配置 ──────────────────────────────────────────────────────────────────

WENCAI_MAX_CONSECUTIVE_FAIL = 3
WENCAI_COOLDOWN_SECONDS = 600
WENCAI_BATCH_PAUSE_EVERY = 10
WENCAI_BATCH_PAUSE_SECONDS = (30, 60)

# ── 状态管理（云端 scheduler_state 表，本地可能不可用）───────────────────────

def _ensure_state_table():
    execute_cloud_insert(
        """CREATE TABLE IF NOT EXISTS scheduler_state (
            `key` VARCHAR(255) PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )""", []
    )

def _get_state(key, default=None):
    _ensure_state_table()
    rows = execute_cloud_query("SELECT value FROM scheduler_state WHERE `key`=%s", [key])
    return rows[0]["value"] if rows else default

def _set_state(key, value):
    _ensure_state_table()
    execute_cloud_insert(
        """INSERT INTO scheduler_state (`key`, value, updated_at)
           VALUES (%s, %s, CURRENT_TIMESTAMP)
           ON DUPLICATE KEY UPDATE value=VALUES(value), updated_at=CURRENT_TIMESTAMP""",
        [key, str(value)],
    )


# ── YAML 字典加载 ─────────────────────────────────────────────────────────────

DICT_PATH = PROJECT_ROOT / "config" / "industry_indicator_dict.yaml"

def load_indicator_dict() -> list[dict]:
    """加载 YAML 字典，返回展平后的查询计划列表。
    每条: {l1, l2, l3, dimension, metric_name, metric_type, query}
    """
    with open(DICT_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    industries = data.get("industries", {})
    plan = []

    for l1, l2_dict in industries.items():
        for l2, l3_dict in l2_dict.items():
            for l3_key, dimensions in l3_dict.items():
                l3 = None if l3_key == "_default" else l3_key
                for dimension in ("demand", "cost", "competition", "capacity"):
                    indicators = dimensions.get(dimension) or []
                    for ind in indicators:
                        name = ind.get("name", "")
                        metric_type = ind.get("metric_type", "")
                        for q in ind.get("queries", []):
                            plan.append({
                                "l1": l1,
                                "l2": l2,
                                "l3": l3,
                                "dimension": dimension,
                                "metric_name": name,
                                "metric_type": metric_type,
                                "query": q,
                            })
    return plan


def build_query_plan(dict_data: list[dict], year: int = None) -> list[dict]:
    """展开 {year} 占位符，返回最终查询计划。

    策略：优先按季度粒度查询，确保拿到季度数据而非仅全年。
    - 当前年：按已过去的季度逐季查（如现在 2026Q1，则查 2026Q1）
    - 上一年：查全年 + 4 个季度（Q1~Q4）
    """
    if year is None:
        year = datetime.now().year
    prev_year = year - 1
    current_month = datetime.now().month
    # 当前年已过去的季度
    current_q = (current_month - 1) // 3  # 0=Q1未完, 1=Q1完, 2=Q1Q2完, 3=Q1Q2Q3完

    expanded = []
    for item in dict_data:
        q_template = item["query"]

        # 上一年：全年 + 4 个季度
        for suffix in ["", "一季度", "二季度", "三季度", "四季度"]:
            new_item = dict(item)
            base = q_template.replace("{year}", str(prev_year))
            if suffix:
                # 在年份后插入季度，如 "2025年一季度中国动力电池装机量"
                base = base.replace(f"{prev_year}年", f"{prev_year}年{suffix}")
            new_item["query"] = base
            new_item["query_year"] = prev_year
            new_item["query_quarter"] = suffix or "全年"
            expanded.append(new_item)

        # 当前年：全年 + 已过去的季度
        quarter_names = ["一季度", "二季度", "三季度", "四季度"]
        suffixes_current = [""]  # 全年
        for qi in range(current_q):
            suffixes_current.append(quarter_names[qi])
        # 如果在Q1中（1-3月），也查Q1（可能有部分数据）
        if current_q == 0 and current_month >= 1:
            suffixes_current.append("一季度")

        for suffix in suffixes_current:
            new_item = dict(item)
            base = q_template.replace("{year}", str(year))
            if suffix:
                base = base.replace(f"{year}年", f"{year}年{suffix}")
            new_item["query"] = base
            new_item["query_year"] = year
            new_item["query_quarter"] = suffix or "全年"
            expanded.append(new_item)

    return expanded


# ── PyWenCai 查询包装 ─────────────────────────────────────────────────────────

def query_wencai_with_retry(question: str, max_retries: int = 3) -> list[dict]:
    """查询问财，返回资讯摘要列表。使用 query_type='zhishi' 获取资讯而非选股。"""
    import pandas as pd

    for attempt in range(max_retries):
        try:
            import pywencai
            # query_type='zhishi' 强制返回资讯/知识类结果，避免被误解为选股
            result = pywencai.get(question=question, query_type='zhishi', loop=False)

            # 情况1: 返回资讯摘要（理想情况）
            if isinstance(result, dict) and 'title_content' in result:
                df = result['title_content']
                if isinstance(df, pd.DataFrame) and len(df) > 0:
                    articles = []
                    for _, row in df.iterrows():
                        articles.append({
                            "title": str(row.get("title_content", "")),
                            "summary": str(row.get("summary_content", "")),
                            "date": str(row.get("publish_date", "")),
                            "url": str(row.get("url", "")),
                        })
                    return articles

            # 情况2: 返回 dict 但无 title_content
            if isinstance(result, dict):
                for key in result:
                    val = result[key]
                    if isinstance(val, str) and len(val) > 20:
                        return [{"title": "", "summary": val, "date": "", "url": ""}]

            return []
        except Exception as e:
            logger.warning(f"[wencai] 查询失败 attempt={attempt}: {e}")
            time.sleep(2 ** attempt)

    return []


# ── DeepSeek LLM 调用 ─────────────────────────────────────────────────────────

_ds_client = None
_ds_lock = threading.Lock()

def _get_deepseek():
    global _ds_client
    if _ds_client is None:
        with _ds_lock:
            if _ds_client is None:
                from openai import OpenAI
                rows = execute_cloud_query(
                    "SELECT value FROM system_config WHERE config_key='deepseek_api_key'"
                )
                if not rows:
                    raise RuntimeError("system_config 中未找到 deepseek_api_key")
                _ds_client = OpenAI(
                    api_key=rows[0]["value"], base_url="https://api.deepseek.com/v1"
                )
    return _ds_client


def llm_extract_indicators(text: str, item: dict) -> list[dict]:
    """调用 DeepSeek 从资讯摘要中提取结构化指标"""
    l3_str = item.get("l3") or ""
    prompt = f"""从以下行业资讯中提取关于「{item['metric_name']}」的量化指标。

行业: {item['l1']} > {item['l2']}{(' > ' + l3_str) if l3_str else ''}
指标类型: {item['metric_type']}

资讯内容:
{text}

严格按JSON输出（不要 markdown 代码块）:
{{
  "indicators": [
    {{
      "industry_l1": "",
      "industry_l2": "",
      "industry_l3": null,
      "metric_type": "",
      "metric_name": "",
      "value": 数值,
      "value_raw": "原始文本",
      "period_type": "year/quarter/month",
      "period_label": "2024",
      "period_year": 2024,
      "period_end_date": "2024-12-31",
      "data_type": "actual/forecast",
      "confidence": "high/medium/low",
      "source_snippet": "原文≤60字"
    }}
  ]
}}

规则：
1. 只提取有明确数值的，无数据则返回空 indicators 数组
2. value 为纯数值（百分比去掉%号，如25.3）
3. confidence：有明确出处和数据来源=high；行业共识=medium；模糊表述=low
4. period_end_date 规则：year→12-31，quarter Q1→03-31 Q2→06-30 Q3→09-30 Q4→12-31，month→该月最后一天"""

    try:
        client = _get_deepseek()
        if len(text) > 8000:
            text = text[:8000] + "\n[文本已截断]"

        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "你是行业数据提取专家。严格按 JSON 格式输出，不要使用 markdown 代码块。"},
                {"role": "user", "content": prompt},
            ],
            max_tokens=2048,
            temperature=0.1,
            timeout=60,
        )
        raw = resp.choices[0].message.content or ""
        return _parse_indicators_json(raw)
    except Exception as e:
        logger.warning(f"[wencai] LLM 调用失败: {e}")
        return []


def _parse_indicators_json(raw: str) -> list[dict]:
    """解析 LLM 返回的 JSON"""
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        data = json.loads(raw.strip())
        return data.get("indicators", []) if isinstance(data, dict) else []
    except Exception:
        # 尝试找 JSON 对象
        brace_start = raw.find("{")
        brace_end = raw.rfind("}")
        if brace_start >= 0 and brace_end > brace_start:
            try:
                data = json.loads(raw[brace_start:brace_end + 1])
                return data.get("indicators", []) if isinstance(data, dict) else []
            except Exception:
                pass
        return []


# ── 入库 ──────────────────────────────────────────────────────────────────────

def save_indicators(indicators: list[dict], item: dict) -> int:
    """写入 industry_indicators，source_type='wencai'"""
    count = 0
    for ind in indicators:
        if not ind.get("metric_name") or ind.get("value") is None:
            continue

        row = {
            "industry_l1": ind.get("industry_l1") or item["l1"],
            "industry_l2": ind.get("industry_l2") or item["l2"],
            "industry_l3": ind.get("industry_l3") or item.get("l3"),
            "metric_type": ind.get("metric_type") or item["metric_type"],
            "metric_name": ind.get("metric_name") or item["metric_name"],
            "value": float(ind["value"]) if ind.get("value") is not None else None,
            "value_raw": ind.get("value_raw"),
            "period_type": ind.get("period_type"),
            "period_label": ind.get("period_label"),
            "period_year": ind.get("period_year"),
            "period_end_date": ind.get("period_end_date"),
            "data_type": ind.get("data_type", "actual"),
            "confidence": ind.get("confidence", "medium"),
            "source_type": "wencai",
            "source_doc_id": None,
            "source_snippet": ind.get("source_snippet"),
        }

        try:
            upsert_industry_indicator(row)
            count += 1
        except Exception as e:
            logger.warning(f"[wencai] 写入失败: {e} | {ind.get('metric_name')}")

    return count


# ── 进度管理 ──────────────────────────────────────────────────────────────────

def _get_progress() -> dict:
    """获取各指标最后采集时间"""
    raw = _get_state("wencai_indicator_progress", "{}")
    try:
        return json.loads(raw)
    except Exception:
        return {}

def _set_progress(progress: dict):
    _set_state("wencai_indicator_progress", json.dumps(progress, ensure_ascii=False))

def _get_daily_count() -> int:
    """获取今日已查询次数"""
    raw = _get_state("wencai_indicator_daily_count", "0|1970-01-01")
    parts = raw.split("|")
    if len(parts) == 2 and parts[1] == str(date.today()):
        return int(parts[0])
    return 0

def _set_daily_count(count: int):
    _set_state("wencai_indicator_daily_count", f"{count}|{date.today()}")


# ── 单指标测试入口 ────────────────────────────────────────────────────────────

def fetch_single_indicator(l1: str, l2: str, l3: Optional[str],
                           metric_name: str, metric_type: str,
                           query: str = None, dry_run: bool = False) -> dict:
    """单个指标测试入口"""
    if query is None:
        year = datetime.now().year
        query = f"{year}年{metric_name}"

    logger.info(f"[wencai] 查询: {query}")
    articles = query_wencai_with_retry(query)
    if not articles:
        logger.info("[wencai] 未获取到资讯")
        return {"articles": 0, "indicators": 0}

    # 拼接摘要
    text_parts = []
    for a in articles:
        text_parts.append(f"标题: {a['title']}\n摘要: {a['summary']}\n日期: {a['date']}\n")
    text = "\n---\n".join(text_parts)

    item = {
        "l1": l1, "l2": l2, "l3": l3,
        "metric_name": metric_name, "metric_type": metric_type,
    }

    indicators = llm_extract_indicators(text, item)
    logger.info(f"[wencai] LLM 提取到 {len(indicators)} 条指标")

    if dry_run:
        for ind in indicators:
            logger.info(f"  [dry-run] {ind.get('metric_name')}: {ind.get('value')} ({ind.get('period_label')})")
        return {"articles": len(articles), "indicators": len(indicators), "data": indicators}

    saved = save_indicators(indicators, item)
    return {"articles": len(articles), "indicators": len(indicators), "saved": saved}


# ── 主入口 ────────────────────────────────────────────────────────────────────

def run_wencai_indicator_fetch(dry_run: bool = False, limit: int = 0):
    """主入口：遍历字典 → 查询问财 → LLM 提取 → 入库

    Args:
        dry_run: True 则只查询不写入
        limit: 限制查询条数，0 表示不限制（受每日限额控制）
    """
    logger.info("[wencai] 行业指标采集开始")

    # 1. 加载字典 & 展开查询计划
    dict_data = load_indicator_dict()
    query_plan = build_query_plan(dict_data)
    logger.info(f"[wencai] 查询计划: {len(query_plan)} 条")

    # 2. 按进度排序（距上次采集最久的优先）
    progress = _get_progress()
    today_str = str(date.today())

    def sort_key(item):
        key = f"{item['l2']}|{item['metric_name']}"
        last = progress.get(key, "1970-01-01")
        return last

    query_plan.sort(key=sort_key)

    # 3. 去重：同一 (l2, metric_name, year, quarter) 只查一条
    seen = set()
    deduped = []
    for item in query_plan:
        key = f"{item['l2']}|{item['metric_name']}|{item.get('query_year', '')}|{item.get('query_quarter', '')}"
        if key not in seen:
            seen.add(key)
            deduped.append(item)

    logger.info(f"[wencai] 去重后查询计划: {len(deduped)} 条")

    # 4. 执行
    daily_count = _get_daily_count()
    max_queries = WENCAI_MAX_DAILY_QUERIES
    if limit > 0:
        max_queries = min(limit, max_queries)

    consecutive_fail = 0
    total_indicators = 0
    total_queries = 0

    for i, item in enumerate(deduped):
        if daily_count >= max_queries:
            logger.info(f"[wencai] 达到每日上限 {max_queries}，停止")
            break

        query = item["query"]
        logger.info(f"[wencai] [{i+1}/{len(deduped)}] 查询: {query}")

        # 查询问财
        articles = query_wencai_with_retry(query)
        daily_count += 1
        total_queries += 1
        _set_daily_count(daily_count)

        if not articles:
            logger.info(f"[wencai] 未获取到资讯，跳过")
            time.sleep(WENCAI_MIN_DELAY)
            continue

        consecutive_fail = 0

        # 拼接摘要文本
        text_parts = []
        for a in articles:
            text_parts.append(f"标题: {a['title']}\n摘要: {a['summary']}\n日期: {a['date']}\n")
        text = "\n---\n".join(text_parts)

        # LLM 提取
        indicators = llm_extract_indicators(text, item)

        if indicators:
            if dry_run:
                for ind in indicators:
                    logger.info(f"  [dry-run] {ind.get('metric_name')}: {ind.get('value')} ({ind.get('period_label')})")
                total_indicators += len(indicators)
            else:
                saved = save_indicators(indicators, item)
                total_indicators += saved
                logger.info(f"[wencai] 写入 {saved} 条指标")

        # 更新进度
        key = f"{item['l2']}|{item['metric_name']}"
        progress[key] = today_str
        _set_progress(progress)

        # 固定间隔
        time.sleep(WENCAI_MIN_DELAY)

    # 记录本次执行
    _set_state("wencai_indicator_last_run", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    result = {
        "total_queries": total_queries,
        "total_indicators": total_indicators,
        "dry_run": dry_run,
    }
    logger.info(f"[wencai] 采集完成: {result}")
    return result

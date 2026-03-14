"""Portfolio实验室 — 项目级四模块分析生成

两种路径：
  场景A: source_group_id 有值 → 直接从 tag_group_research 复制，零 AI 调用
  场景B: 无来源标签组 → 从篮子股票反推标签，复用 hotspot 分析链路

AI 约束：
  - 完全复用 hotspot/tag_group_research.py 的四个 Prompt + Skill 注入
  - 所有 AI 输出均有严格 JSON schema 约束
  - 注入 stock-analysis-critical-thinking 批判性思维清单
  - _call_json_with_retry 强制 JSON 解析，失败自动重试
"""
import json
import logging
from datetime import datetime
from typing import Optional

from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)


# ── 入口函数 ──────────────────────────────────────────────────

def generate_project_analysis(project_id: int, progress_callback=None) -> dict:
    """生成项目四模块分析，写入 watchlist_lists.analysis_json，返回结果 dict。
    同时用 AI 建议更精准的项目名称和核心投资逻辑，写回 watchlist_lists。
    """

    def _progress(msg, pct=None):
        if progress_callback:
            progress_callback(msg, pct)

    _progress("读取项目信息", 2)
    project = execute_query("SELECT * FROM watchlist_lists WHERE id=%s", [project_id])
    if not project:
        return {"ok": False, "error": "项目不存在"}
    project = dict(project[0])

    source_group_id = project.get("source_group_id")
    investment_logic = (project.get("investment_logic") or project.get("background_info") or "").strip()
    current_name = (project.get("list_name") or "").strip()

    if source_group_id:
        result = _copy_from_hotspot(source_group_id, _progress)
        result["source"] = "hotspot_import"
        result["source_group_id"] = source_group_id
    else:
        result = _generate_from_basket(project_id, investment_logic, _progress)
        result["source"] = "project_generate"

    result["generated_at"] = datetime.now().isoformat()

    # ── AI 建议项目名称 + 核心投资逻辑（严格 JSON schema）──
    _progress("AI 建议项目名称和逻辑", 92)
    try:
        suggested = _suggest_project_meta(
            current_name=current_name,
            current_logic=investment_logic,
            macro=result.get("macro") or {},
            industry=result.get("industry") or {},
            news=result.get("news_parsed") or [],
            tags=result.get("tags_used") or [],
        )
        result["suggested_name"] = suggested.get("name", "")
        result["suggested_logic"] = suggested.get("logic", "")

        # 自动写回：名称只在原名为空或是通用占位词时覆盖；逻辑始终覆盖
        update_fields, update_params = [], []
        placeholder_names = {"新项目", "新策略", "新建项目", "", "theme", "portfolio"}
        if current_name in placeholder_names and suggested.get("name"):
            update_fields.append("list_name=%s")
            update_params.append(suggested["name"])
        if suggested.get("logic"):
            update_fields.append("investment_logic=%s")
            update_params.append(suggested["logic"])
        if update_fields:
            update_params.append(project_id)
            execute_insert(
                f"UPDATE watchlist_lists SET {', '.join(update_fields)} WHERE id=%s",
                update_params,
            )
    except Exception as e:
        logger.warning(f"AI 建议项目 meta 失败: {e}")
        result["suggested_name"] = ""
        result["suggested_logic"] = ""

    # 写回分析 JSON
    _progress("保存分析结果", 95)
    execute_insert(
        "UPDATE watchlist_lists SET analysis_json=%s WHERE id=%s",
        [json.dumps(result, ensure_ascii=False, default=str), project_id],
    )

    _progress("完成", 100)
    return {"ok": True, "analysis": result}


# ── AI 建议项目 Meta ──────────────────────────────────────────

PROJECT_META_PROMPT = """你是投资组合命名专家。根据以下已生成的分析内容，为这个投资组合建议一个精准的项目名称和核心投资逻辑。

要求：
- name: 5-12个字，简洁点出主题，不要用"策略"/"组合"等废话后缀，举例："AI算力产业链"、"新能源出海"、"消费复苏龙头"
- logic: 50-120字，说明这个组合的核心投资主线——为什么这些股票在一起？共同的催化剂是什么？受益路径是什么？不要空泛，要具体到行业/政策/技术驱动

请严格输出以下 JSON（不要输出其他内容）：
{"name": "项目名称", "logic": "核心投资逻辑..."}"""


# ── 场景A：直接从 tag_group_research 复制 ─────────────────────

def _copy_from_hotspot(group_id: int, _progress) -> dict:
    _progress("读取热点研究结果", 10)
    rows = execute_query(
        """SELECT news_parsed_json, theme_heat_json, macro_json, industry_json
           FROM tag_group_research
           WHERE group_id=%s
           ORDER BY research_date DESC LIMIT 1""",
        [group_id],
    )
    if not rows:
        logger.warning(f"热点组 {group_id} 无研究记录，转为场景B生成")
        # fallback: group_id 无研究，以空 tags 走 B 路径
        return _generate_from_basket_with_tags([], "", _progress)

    row = rows[0]
    _progress("复制分析数据", 80)

    def _parse(col):
        v = row.get(col)
        if not v:
            return None
        try:
            return json.loads(v)
        except Exception:
            return None

    return {
        "news_parsed": _parse("news_parsed_json") or [],
        "theme_heat": _parse("theme_heat_json"),
        "macro": _parse("macro_json"),
        "industry": _parse("industry_json"),
    }


# ── 场景B：从篮子反推标签 → 走分析链路 ─────────────────────────

def _generate_from_basket(project_id: int, investment_logic: str, _progress) -> dict:
    _progress("从篮子股票反推标签", 5)

    # 获取篮子股票代码
    rows = execute_query(
        "SELECT stock_code FROM watchlist_list_stocks WHERE list_id=%s AND status='active'",
        [project_id],
    )
    stock_codes = [r["stock_code"] for r in (rows or [])]

    # fallback: portfolio 项目 id=1 用持仓
    if not stock_codes:
        rows = execute_query(
            "SELECT stock_code FROM holding_positions WHERE status='open'", []
        )
        stock_codes = [r["stock_code"] for r in (rows or [])]

    # 从篮子股票的新闻关联中提取高频标签
    tags = _infer_tags_from_stocks(stock_codes)

    return _generate_from_basket_with_tags(tags, investment_logic, _progress,
                                           stock_codes=stock_codes)


def _infer_tags_from_stocks(stock_codes: list) -> list:
    """从篮子股票关联的新闻里提取高频标签（Top 8）"""
    if not stock_codes:
        return []

    codes_ph = ",".join(["%s"] * len(stock_codes))
    rows = execute_query(
        f"""SELECT ci.tags_json
            FROM cleaned_items ci
            JOIN item_companies ic ON ci.id = ic.cleaned_item_id
            WHERE ic.stock_code IN ({codes_ph})
              AND ci.cleaned_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
              AND ci.tags_json IS NOT NULL AND ci.tags_json != '[]'
            ORDER BY ci.cleaned_at DESC
            LIMIT 100""",
        stock_codes,
    )

    tag_freq: dict = {}
    for r in (rows or []):
        try:
            tags = json.loads(r["tags_json"]) if r.get("tags_json") else []
        except Exception:
            continue
        for t in tags:
            if t and len(t) >= 2:
                tag_freq[t] = tag_freq.get(t, 0) + 1

    # 按频次排序，取 Top 8
    sorted_tags = sorted(tag_freq.items(), key=lambda x: x[1], reverse=True)
    return [t for t, _ in sorted_tags[:8]]


def _generate_from_basket_with_tags(tags: list, investment_logic: str,
                                    _progress, stock_codes: list = None) -> dict:
    """有了标签后，复用 hotspot 分析链路生成四模块"""

    # 导入 hotspot 层的数据收集函数和 Prompt
    from hotspot.tag_group_research import (
        _collect_news,
        _parse_news_with_claude,
        _collect_theme_heat,
        _collect_macro_data,
        _call_json_with_retry,
        _inject_critical_thinking,
        _extract_skill_key_sections,
        MACRO_ANALYSIS_PROMPT,
        INDUSTRY_ANALYSIS_PROMPT,
    )
    from utils.skill_registry import get_skill_content

    # 投资逻辑：用项目自己的，如果没有则用标签拼凑
    logic = investment_logic or ("围绕 " + "、".join(tags) + " 的投资组合") if tags else "综合投资组合"

    # 1. 收集 + 解析关联新闻
    _progress("[1/4] 收集关联新闻", 15)
    if tags:
        news_data = _collect_news(tags)
    elif stock_codes:
        # 无标签时直接从篮子股票关联新闻取
        news_data = _collect_news_by_stocks(stock_codes)
    else:
        news_data = []

    _progress("[2/4] AI 解析新闻", 30)
    news_parsed = _parse_news_with_claude(news_data[:20], logic) if news_data else []

    # 2. 主题热度（纯 DB，无 AI）
    _progress("[2/4] 收集主题热度", 40)
    theme_heat = _collect_theme_heat(tags) if tags else None

    # 3. 宏观分析
    _progress("[3/4] AI 宏观分析", 50)
    macro_data = _safe_collect_macro()
    context = _build_context(tags, logic, news_data, macro_data)
    macro_json = {}
    try:
        macro_skill = get_skill_content("macro-stock-analysis")
        macro_excerpt = _extract_skill_key_sections(macro_skill, max_chars=3000) if macro_skill else ""
        macro_prompt = MACRO_ANALYSIS_PROMPT.format(macro_skill_excerpt=macro_excerpt)
        macro_prompt = _inject_critical_thinking(macro_prompt)
        macro_json = _call_json_with_retry(macro_prompt, context, max_tokens=3000)
    except Exception as e:
        logger.warning(f"项目宏观分析失败: {e}")

    # 4. 行业分析
    _progress("[4/4] AI 行业分析", 70)
    industry_json = {}
    try:
        industry_skill = get_skill_content("event-industry-impact")
        industry_excerpt = _extract_skill_key_sections(industry_skill, max_chars=3000) if industry_skill else ""
        industry_prompt = INDUSTRY_ANALYSIS_PROMPT.format(industry_skill_excerpt=industry_excerpt)
        industry_prompt = _inject_critical_thinking(industry_prompt)
        industry_json = _call_json_with_retry(industry_prompt, context, max_tokens=2000)
    except Exception as e:
        logger.warning(f"项目行业分析失败: {e}")

    return {
        "tags_used": tags,
        "news_parsed": news_parsed,
        "theme_heat": theme_heat,
        "macro": macro_json,
        "industry": industry_json,
    }


def _collect_news_by_stocks(stock_codes: list) -> list:
    """无标签时，直接按股票代码关联 item_companies 取新闻"""
    if not stock_codes:
        return []
    codes_ph = ",".join(["%s"] * len(stock_codes))
    rows = execute_query(
        f"""SELECT ci.id, ci.summary, ci.sentiment, ci.importance, ci.tags_json, ci.cleaned_at
            FROM cleaned_items ci
            JOIN item_companies ic ON ci.id = ic.cleaned_item_id
            WHERE ic.stock_code IN ({codes_ph})
              AND ci.cleaned_at >= DATE_SUB(NOW(), INTERVAL 14 DAY)
            ORDER BY ci.importance DESC, ci.cleaned_at DESC
            LIMIT 30""",
        stock_codes,
    )
    seen, result = set(), []
    for r in (rows or []):
        if r["id"] not in seen:
            seen.add(r["id"])
            d = dict(r)
            d["match_tags"] = 1
            result.append(d)
    return result


def _safe_collect_macro() -> dict:
    try:
        from hotspot.tag_group_research import _collect_macro_data
        return _collect_macro_data()
    except Exception as e:
        logger.warning(f"宏观数据收集失败: {e}")
        return {"macro_indicators": [], "northbound_flow": []}


def _build_context(tags: list, logic: str, news_data: list, macro_data: dict) -> str:
    """组装 AI 分析上下文（简化版，聚焦于项目投资逻辑）"""
    tags_str = "、".join(tags) if tags else "综合"
    ctx = f"投资组合标签: {tags_str}\n投资逻辑: {logic}\n\n"

    ctx += "=== 关联新闻摘要 ===\n"
    for n in news_data[:15]:
        icon = {"positive": "利好", "negative": "利空"}.get(n.get("sentiment"), "中性")
        ctx += f"- [{icon}][重要性{n.get('importance', 0)}] {n.get('summary', '')}\n"

    ctx += "\n=== 数据库宏观指标（近30日） ===\n"
    for row in macro_data.get("macro_indicators", [])[:30]:
        ctx += f"- {row.get('indicator_name', '')} ({row.get('indicator_date', '')}): {row.get('value', '')} {row.get('unit', '')}\n"

    ctx += "\n=== 北向资金（近7日） ===\n"
    for row in macro_data.get("northbound_flow", [])[:7]:
        ctx += f"- {row.get('trade_date', '')}: 净流入 {row.get('total_net', '')}亿\n"

    return ctx


def _suggest_project_meta(current_name, current_logic, macro, industry, news, tags) -> dict:
    """用 AI 建议更精准的项目名称和核心投资逻辑，严格 JSON 输出"""
    from hotspot.tag_group_research import _call_json_with_retry

    # 组装简短上下文（避免过长）
    ctx_parts = []
    if tags:
        ctx_parts.append(f"投资组合标签: {', '.join(tags)}")
    if current_name:
        ctx_parts.append(f"当前项目名: {current_name}")
    if current_logic:
        ctx_parts.append(f"当前投资逻辑: {current_logic}")
    if macro.get("summary"):
        ctx_parts.append(f"宏观摘要: {macro['summary'][:200]}")
    if industry.get("summary"):
        ctx_parts.append(f"行业摘要: {industry['summary'][:200]}")
    # 取前5条高importance新闻的 highlight
    top_news = sorted(news, key=lambda x: x.get("importance", 0), reverse=True)[:5]
    for n in top_news:
        hl = n.get("highlight") or n.get("fact") or ""
        if hl:
            ctx_parts.append(f"关键新闻: {hl[:100]}")

    user_msg = "\n".join(ctx_parts)
    result = _call_json_with_retry(PROJECT_META_PROMPT, user_msg, max_tokens=300)
    if isinstance(result, dict) and result.get("name") and result.get("logic"):
        return result
    return {}


# ── 读取已保存的分析（供 context API 调用）────────────────────

def get_saved_analysis(project_id: int) -> Optional[dict]:
    """读取 watchlist_lists.analysis_json，解析后返回，无数据返回 None"""
    rows = execute_query(
        "SELECT analysis_json FROM watchlist_lists WHERE id=%s", [project_id]
    )
    if not rows or not rows[0].get("analysis_json"):
        return None
    try:
        return json.loads(rows[0]["analysis_json"])
    except Exception:
        return None

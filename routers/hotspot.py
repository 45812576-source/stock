"""热点研究 — FastAPI 路由"""
import json
import logging
import threading
import time
import uuid
from datetime import datetime
from fastapi import APIRouter, BackgroundTasks, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from utils.db_utils import execute_query, execute_insert
from utils.auth_deps import get_current_user, get_optional_user, TokenData
from utils.quota_service import check_quota, consume_quota
from hotspot.chat_handler import get_chat_history, submit_chat_message, get_pending_reply, ensure_chat_greeting
from hotspot.tag_recommender import (
    get_top_tags, get_tag_dashboard_distribution,
    recommend_tag_groups, merge_and_filter_groups,
    save_tag_group, get_saved_groups, delete_tag_group, clear_all_tag_groups,
    get_group_related_news,
)
from hotspot.tag_group_analyzer import analyze_tag_group
from hotspot.tag_group_research import research_tag_group, get_group_research_history
from knowledge_graph.kg_manager import add_entity, update_entity, get_entity_by_id, add_relationship

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/hotspot", tags=["hotspot"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

# ── 研究任务进度追踪 ─────────────────────────────────────────
_hotspot_tasks: dict = {}
_hotspot_lock = threading.Lock()


def _create_hotspot_task(group_id, group_name=""):
    task_id = uuid.uuid4().hex[:8]
    with _hotspot_lock:
        _hotspot_tasks[task_id] = {
            "id": task_id, "group_id": group_id,
            "group_name": group_name, "type": "hotspot",
            "status": "running", "progress": 0,
            "message": "准备中...", "created_at": time.time(),
            "finished_at": None, "research_id": None,
        }
    return task_id


def _update_hotspot_task(task_id, **kw):
    with _hotspot_lock:
        if task_id in _hotspot_tasks:
            _hotspot_tasks[task_id].update(kw)


def _make_hotspot_progress_cb(task_id):
    def cb(msg, pct=None):
        updates = {"message": msg}
        if pct is not None:
            updates["progress"] = pct
        _update_hotspot_task(task_id, **updates)
    return cb


# ── 数据查询 ──────────────────────────────────────────────────

def get_top_hotspots(days: int = 7, limit: int = 10) -> list:
    try:
        tags = get_top_tags(days, limit)
        for i, t in enumerate(tags):
            t["rank"] = i + 1
            t["momentum"] = round(t.get("total_freq", 0) * 10 / max(days, 1), 1)
        return tags
    except Exception:
        return []


def _merge_similar_tags(tags):
    """合并相似标签：短标签是长标签的子串时，合并到短标签下。
    例如 '周期' 和 '周期复苏' → '周期'，'化工' 和 '基础化工' → '化工'。
    输入: get_top_tags 返回的 list[dict]，按 total_freq 降序。
    返回: 合并后的 list[dict]，freq 累加。
    """
    # 过滤单字标签（太短没有区分度）
    tags = [t for t in tags if len(t["tag_name"]) >= 2]

    merged = []          # [(canonical_name, merged_tag_dict)]
    alias_map = {}       # alias_name -> canonical_name

    for tag in tags:
        name = tag["tag_name"]
        if name in alias_map:
            # 已被合并到某个主标签
            canon = alias_map[name]
            for m_name, m_tag in merged:
                if m_name == canon:
                    m_tag["total_freq"] = m_tag.get("total_freq", 0) + tag.get("total_freq", 0)
                    break
            continue

        # 检查是否是已有主标签的超集/子集
        found = False
        for m_name, m_tag in merged:
            # name 包含 m_name（如 "周期复苏" 包含 "周期"）→ 合并到 m_name
            if len(name) > len(m_name) and m_name in name:
                alias_map[name] = m_name
                m_tag["total_freq"] = m_tag.get("total_freq", 0) + tag.get("total_freq", 0)
                found = True
                break
            # m_name 包含 name（如已存"基础化工"，来了"化工"）→ name 更短，替换为主标签
            if len(m_name) > len(name) and name in m_name:
                alias_map[m_name] = name
                tag["total_freq"] = tag.get("total_freq", 0) + m_tag.get("total_freq", 0)
                # 替换 merged 中的条目
                for i, (mn, mt) in enumerate(merged):
                    if mn == m_name:
                        merged[i] = (name, tag)
                        break
                found = True
                break

        if not found:
            merged.append((name, tag))

    # 返回时保留 alias_map 供查询用
    result_tags = [m_tag for _, m_tag in merged]
    return result_tags, alias_map


def get_frequency_chart_data(days: int = 30, start_date: str = None, end_date: str = None) -> dict:
    """综合热度热力图数据：Y轴=标签(Top20按合计排序)，X轴=日期+合计"""
    try:
        raw_tags = get_top_tags(days, 50)  # 多取一些，合并后再取 Top20
        merged_tags, alias_map = _merge_similar_tags(raw_tags)
        # 构建反向映射：canonical_name -> [canonical_name, alias1, alias2, ...]
        canon_names = {}
        for tag in merged_tags:
            canon_names[tag["tag_name"]] = [tag["tag_name"]]
        for alias, canon in alias_map.items():
            if canon in canon_names:
                canon_names[canon].append(alias)
        top_tags = merged_tags
        all_capital = []

        # 日期条件 — 用 raw_items.published_at（新闻发布时间）而非 cleaned_at（入库时间）
        if start_date and end_date:
            news_date_cond = "COALESCE(ri.published_at, ci.cleaned_at) >= ? AND COALESCE(ri.published_at, ci.cleaned_at) <= ?"
            news_date_params = [start_date, end_date + " 23:59:59"]
            cap_date_cond = "trade_date >= ? AND trade_date <= ?"
            cap_date_params = [start_date, end_date]
        else:
            news_date_cond = "COALESCE(ri.published_at, ci.cleaned_at) >= DATE_SUB(CURDATE(), INTERVAL ? DAY)"
            news_date_params = [days]
            cap_date_cond = "trade_date >= DATE_SUB(CURDATE(), INTERVAL ? DAY)"
            cap_date_params = [days]

        # 生成完整日期列表
        from datetime import datetime as dt, timedelta
        if start_date and end_date:
            s = dt.strptime(start_date, "%Y-%m-%d")
            e = dt.strptime(end_date, "%Y-%m-%d")
        else:
            e = dt.now()
            s = e - timedelta(days=days)
        all_dates = []
        cur = s
        while cur <= e:
            all_dates.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)

        series = {}
        for tag in top_tags:
            name = tag["tag_name"]
            # 该主标签及其所有别名
            all_names = canon_names.get(name, [name])

            # 构建 LIKE 条件覆盖所有别名
            news_like_parts = " OR ".join(["ci.tags_json LIKE ?" for _ in all_names])
            news_like_params = [f"%{n}%" for n in all_names]

            cap_like_parts = " OR ".join(["industry_name LIKE ?" for _ in all_names])
            cap_like_params = [f"%{n}%" for n in all_names]

            news_rows = execute_query(
                f"""SELECT DATE(COALESCE(ri.published_at, ci.cleaned_at)) as day,
                          COUNT(*) as mention_count,
                          SUM(ci.importance) as weighted_mentions
                   FROM cleaned_items ci
                   LEFT JOIN raw_items ri ON ci.raw_item_id = ri.id
                   WHERE ({news_like_parts}) AND {news_date_cond}
                   GROUP BY DATE(COALESCE(ri.published_at, ci.cleaned_at)) ORDER BY day""",
                news_like_params + news_date_params,
            )

            capital_rows = execute_query(
                f"""SELECT trade_date as day, SUM(net_inflow) as net_flow
                   FROM industry_capital_flow
                   WHERE ({cap_like_parts}) AND {cap_date_cond}
                   GROUP BY trade_date""",
                cap_like_params + cap_date_params,
            )
            capital_map = {str(r["day"]): float(r["net_flow"] or 0) for r in capital_rows}
            all_capital.extend(abs(v) for v in capital_map.values() if v)

            day_data = {}
            for r in news_rows:
                d = str(r["day"])
                day_data[d] = {"news_heat": float(r["weighted_mentions"] or 0), "capital": float(capital_map.get(d, 0))}
            for d, v in capital_map.items():
                if d not in day_data:
                    day_data[d] = {"news_heat": 0, "capital": v}

            series[name] = {"days": day_data}

        # 补充来源1：content_summaries 关键词频次
        try:
            from utils.content_query import query_content_summaries, extract_keywords_from_summary
            cs_rows = query_content_summaries(
                doc_types=["policy_doc", "data_release", "strategy_report",
                           "market_commentary", "research_report",
                           "announcement", "feature_news", "flash_news"],
                date_str=None, limit=200, fallback_days=days,
            )
            for cs in cs_rows:
                kws = extract_keywords_from_summary(
                    (cs.get("summary") or "") + " " + (cs.get("fact_summary") or ""), max_kw=3
                )
                day_str = str(cs.get("publish_time") or cs.get("created_at") or "")[:10]
                for kw in kws:
                    if kw not in series:
                        series[kw] = {"days": {}}
                    if day_str not in series[kw]["days"]:
                        series[kw]["days"][day_str] = {"news_heat": 0, "capital": 0}
                    series[kw]["days"][day_str]["news_heat"] += 1
        except Exception:
            pass

        # 补充来源2：stock_mentions related_themes 频次
        try:
            from utils.content_query import query_stock_mentions
            sm_rows = query_stock_mentions(limit=500, days=days)
            import json as _json
            for sm in sm_rows:
                themes_raw = sm.get("related_themes") or ""
                if themes_raw.startswith("["):
                    try:
                        themes = _json.loads(themes_raw)
                    except Exception:
                        themes = [themes_raw]
                else:
                    themes = [t.strip() for t in themes_raw.split(",") if t.strip()]
                day_str = str(sm.get("mention_time", ""))[:10]
                for theme in themes:
                    if not theme:
                        continue
                    if theme not in series:
                        series[theme] = {"days": {}}
                    if day_str not in series[theme]["days"]:
                        series[theme]["days"][day_str] = {"news_heat": 0, "capital": 0}
                    series[theme]["days"][day_str]["news_heat"] += 2  # 主题权重稍高
        except Exception:
            pass

        # 标准化资金流
        max_capital = max(all_capital) if all_capital else 1
        max_news = max(
            (d["news_heat"] for s in series.values() for d in s["days"].values()),
            default=1,
        ) or 1

        # 计算每个标签每天的热度 + 合计，按合计排序取 Top20
        tag_heat = {}
        for name, data in series.items():
            daily = {}
            total = 0.0
            for day in all_dates:
                d = data["days"].get(day, {"news_heat": 0, "capital": 0})
                cap_norm = abs(d["capital"]) / max_capital * max_news * 0.3 if max_capital else 0
                heat = round(d["news_heat"] + cap_norm, 1)
                daily[day] = heat
                total += heat
            tag_heat[name] = {"daily": daily, "total": round(total, 1)}

        # 按合计降序取 Top 20
        sorted_tags = sorted(tag_heat.keys(), key=lambda t: tag_heat[t]["total"], reverse=True)[:20]

        return {
            "dates": all_dates,
            "tags": sorted_tags,
            "heat_map": {t: tag_heat[t] for t in sorted_tags},
        }
    except Exception:
        import traceback
        traceback.print_exc()
        return {}


def get_tag_clusters(days: int = 7) -> list:
    """获取已保存的标签组 + 颜色分配"""
    colors = ["#135bec", "#10b981", "#f97316", "#a855f7", "#ef4444", "#06b6d4"]
    try:
        groups = get_saved_groups()
        for i, g in enumerate(groups):
            g["tags"] = json.loads(g.get("tags_json") or "[]")
            g["color"] = colors[i % len(colors)]
            # 解析额外筛选信息
            extra = json.loads(g.get("extra_json") or "{}") if g.get("extra_json") else {}
            g["macro_positive"] = extra.get("macro_positive", False)
            g["industry_positive"] = extra.get("industry_positive", False)
            g["leader_stock"] = extra.get("leader_stock", "")
            g["leader_net_inflow"] = extra.get("leader_net_inflow", 0)
            g["group_total_inflow"] = extra.get("group_total_inflow", 0)
            g["group_total_cap"] = extra.get("group_total_cap", 0)
            g["group_stock_count"] = extra.get("group_stock_count", 0)
            g["daily_inflow"] = extra.get("daily_inflow", [])
            g["kg_stock_groups"] = extra.get("kg_stock_groups", [])
            # 最近研究摘要
            history = get_group_research_history(g["id"], 1)
            g["has_research"] = bool(history)
            if history:
                g["last_research_date"] = str(history[0].get("created_at", ""))[:10]
        return groups[:12]
    except Exception:
        return []


def _md_to_html(text: str) -> str:
    """简易 markdown -> HTML: **bold** 和换行"""
    if not text:
        return text
    import re
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    text = text.replace('\n\n', '</p><p class="mt-3">')
    text = text.replace('\n', '<br>')
    return text


def get_research_data(group_id: int) -> dict:
    """获取标签组的完整研究数据"""
    try:
        groups = get_saved_groups()
        group = next((g for g in groups if g["id"] == group_id), None)
        if not group:
            return None

        tags = json.loads(group.get("tags_json") or "[]")
        group["tags"] = tags

        # 加载最近研究结果
        history = get_group_research_history(group_id, 1)
        research = {}
        if history:
            h = history[0]
            macro = h.get("macro_report") or ""
            industry = h.get("industry_report") or ""
            if "生成失败" in macro:
                macro = ""
            if "生成失败" in industry:
                industry = ""

            # 解析结构化 JSON 字段（新增）
            macro_json = json.loads(h["macro_json"]) if h.get("macro_json") else {}
            industry_json = json.loads(h["industry_json"]) if h.get("industry_json") else {}
            news_parsed = json.loads(h["news_parsed_json"]) if h.get("news_parsed_json") else []
            theme_heat = json.loads(h["theme_heat_json"]) if h.get("theme_heat_json") else {}
            logic_synthesis = json.loads(h["logic_synthesis_json"]) if h.get("logic_synthesis_json") else {}
            industry_heat = json.loads(h["industry_heat_json"]) if h.get("industry_heat_json") else {}

            research = {
                "id": h["id"],
                "macro_report": _md_to_html(macro),
                "industry_report": _md_to_html(industry),
                "macro_json": macro_json,
                "industry_json": industry_json,
                "news": json.loads(h["news_summary_json"]) if h.get("news_summary_json") else [],
                "news_parsed": news_parsed,
                "sector_heat": json.loads(h["sector_heat_json"]) if h.get("sector_heat_json") else [],
                "theme_heat": theme_heat,
                "industry_heat": industry_heat,
                "top10_stocks": json.loads(h["top10_stocks_json"]) if h.get("top10_stocks_json") else [],
                "logic_synthesis": logic_synthesis,
                "research_date": str(h.get("created_at", ""))[:10],
                "status": h.get("status") or "draft",
                "portfolio_stats": json.loads(h["portfolio_stats_json"]) if h.get("portfolio_stats_json") else None,
            }

        # 补充机构态度摘要：对 top10_stocks 相关股票批量查机构持仓+股东人数
        if research.get("top10_stocks"):
            research["institution_attitude"] = _get_institution_attitude(research["top10_stocks"])

        return {"group": group, "research": research}
    except Exception:
        return None


def _get_institution_attitude(top10_stocks: list) -> dict:
    """批量查 tag_group 相关股票的机构持仓和股东人数变动，返回摘要字典。"""
    try:
        # 收集所有股票代码（最多取12只）
        stock_codes = []
        for grp in top10_stocks:
            for s in grp.get("stocks", []):
                code = s.get("stock_code")
                if code and code not in stock_codes:
                    stock_codes.append(code)
                if len(stock_codes) >= 12:
                    break
            if len(stock_codes) >= 12:
                break

        if not stock_codes:
            return {}

        # 按需同步数据
        from utils.db_utils import ensure_stock_extra_data
        for code in stock_codes[:6]:  # 只同步前6只，避免太慢
            ensure_stock_extra_data(code)

        # 查机构持仓（取最新2期均值，按持仓比例排序）
        placeholders = ",".join(["%s"] * len(stock_codes))
        inst_rows = execute_query(
            f"""SELECT stock_code, report_date, SUM(hold_ratio) as total_hold_ratio,
                       SUM(hold_change) as total_hold_change, COUNT(DISTINCT institution_type) as inst_types
                FROM institutional_holding
                WHERE stock_code IN ({placeholders})
                GROUP BY stock_code, report_date
                ORDER BY report_date DESC, total_hold_ratio DESC""",
            stock_codes,
        ) or []

        # 取各股最新期数据
        inst_map = {}
        for r in inst_rows:
            code = r["stock_code"]
            if code not in inst_map:
                inst_map[code] = r

        # 查股东户数（取最新2期变化）
        holder_rows = execute_query(
            f"""SELECT stock_code, end_date, holder_count, change_pct
                FROM shareholder_count
                WHERE stock_code IN ({placeholders})
                ORDER BY end_date DESC""",
            stock_codes,
        ) or []

        holder_map = {}
        for r in holder_rows:
            code = r["stock_code"]
            if code not in holder_map:
                holder_map[code] = r

        # 汇总摘要
        inst_top = sorted(inst_map.values(), key=lambda r: r.get("total_hold_ratio") or 0, reverse=True)[:5]
        inst_summary = []
        for r in inst_top:
            change = r.get("total_hold_change")
            direction = "增持" if change and change > 0 else ("减持" if change and change < 0 else "持平")
            inst_summary.append({
                "stock_code": r["stock_code"],
                "hold_ratio": r.get("total_hold_ratio"),
                "hold_change": change,
                "direction": direction,
                "report_date": str(r.get("report_date", "")),
            })

        holder_top = sorted(
            holder_map.values(),
            key=lambda r: abs(r.get("change_pct") or 0),
            reverse=True,
        )[:5]
        holder_summary = []
        for r in holder_top:
            pct = r.get("change_pct")
            holder_summary.append({
                "stock_code": r["stock_code"],
                "holder_count": r.get("holder_count"),
                "change_pct": pct,
                "trend": "集中" if pct and pct < 0 else ("分散" if pct and pct > 0 else "持平"),
                "end_date": str(r.get("end_date", "")),
            })

        return {
            "inst_holdings": inst_summary,
            "holder_counts": holder_summary,
        }
    except Exception:
        return {}


@router.get("/api/pool", response_class=JSONResponse)
def api_pool_list():
    """获取待研究池列表"""
    rows = execute_query(
        "SELECT id, item_type, ref_id, title, snapshot, added_at FROM discovery_pool ORDER BY added_at DESC LIMIT 100"
    )
    result = []
    for r in rows:
        snap = r.get("snapshot")
        if isinstance(snap, str):
            try:
                snap = json.loads(snap)
            except Exception:
                snap = {}
        result.append({
            "id": r["id"],
            "item_type": r["item_type"],
            "ref_id": r["ref_id"],
            "title": r["title"],
            "snapshot": snap or {},
            "added_at": str(r["added_at"])[:10] if r.get("added_at") else "",
        })
    return result


@router.post("/api/pool/add", response_class=JSONResponse)
async def api_pool_add(request: Request):
    """添加条目到待研究池"""
    try:
        body = await request.json()
        item_type = body.get("item_type", "news")
        ref_id = body.get("ref_id")
        title = body.get("title", "")[:500]
        snapshot = body.get("snapshot", {})
        # 去重：同 ref_id + item_type 不重复添加
        if ref_id:
            exists = execute_query(
                "SELECT id FROM discovery_pool WHERE item_type=%s AND ref_id=%s LIMIT 1",
                (item_type, ref_id)
            )
            if exists:
                return {"ok": False, "msg": "已在池中"}
        execute_insert(
            "INSERT INTO discovery_pool (item_type, ref_id, title, snapshot) VALUES (%s, %s, %s, %s)",
            (item_type, ref_id, title, json.dumps(snapshot, ensure_ascii=False))
        )
        return {"ok": True}
    except Exception as e:
        logger.warning(f"pool add 失败: {e}")
        return {"ok": False, "msg": str(e)}


@router.delete("/api/pool/{pool_id}", response_class=JSONResponse)
def api_pool_delete(pool_id: int):
    """从待研究池移除条目"""
    try:
        execute_insert("DELETE FROM discovery_pool WHERE id=%s", (pool_id,))
        return {"ok": True}
    except Exception as e:
        logger.warning(f"pool delete 失败: {e}")
        return {"ok": False, "msg": str(e)}


@router.get("/api/daily-intel-theme-trend", response_class=JSONResponse)
def api_daily_intel_theme_trend(days: int = 7):
    """综合热度趋势：从 daily_intel_themes 读 AI 归纳主题，按天展示 mention_count，Top20"""
    try:
        from utils.db_utils import execute_cloud_query
        from datetime import datetime, timedelta

        rows = execute_cloud_query(
            """SELECT scan_date, theme_name, mention_count
               FROM daily_intel_themes
               WHERE scan_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
               ORDER BY scan_date""",
            [days],
        ) or []

        theme_totals: dict[str, int] = {}
        theme_daily: dict[str, dict[str, int]] = {}
        for r in rows:
            name = r["theme_name"]
            day  = str(r["scan_date"])[:10]
            cnt  = int(r["mention_count"] or 0)
            theme_totals[name] = theme_totals.get(name, 0) + cnt
            if name not in theme_daily:
                theme_daily[name] = {}
            theme_daily[name][day] = cnt

        top_themes = sorted(theme_totals.items(), key=lambda x: x[1], reverse=True)[:20]

        today = datetime.now().date()
        dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]

        heat_map = {
            name: {"total": total, "daily": theme_daily.get(name, {})}
            for name, total in top_themes
        }
        return {"dates": dates, "tags": [t[0] for t in top_themes], "heat_map": heat_map}
    except Exception as e:
        logger.warning(f"daily-intel-theme-trend 失败: {e}")
        return {"dates": [], "tags": [], "heat_map": {}}


@router.get("/api/daily-intel-trend", response_class=JSONResponse)
def api_daily_intel_trend(days: int = 7):
    """综合热度趋势（旧）：daily_intel_stocks 按股票出现次数，按天统计，Top20"""
    try:
        from config.chain_config import CHAINS
        from utils.db_utils import execute_cloud_query
        from datetime import datetime, timedelta

        rows = execute_cloud_query(
            """SELECT stock_name, stock_code, DATE(scan_date) AS day, COUNT(*) AS cnt
               FROM daily_intel_stocks
               WHERE scan_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
                 AND stock_name IS NOT NULL AND stock_name != ''
                 AND stock_code IS NOT NULL AND stock_code != ''
               GROUP BY stock_name, stock_code, DATE(scan_date)
               ORDER BY day""",
            [days],
        ) or []

        # 汇总每只股票总次数，取 Top20
        stock_totals = {}
        stock_daily = {}
        for r in rows:
            name = r["stock_name"]
            day = str(r["day"])[:10]
            cnt = int(r["cnt"] or 0)
            stock_totals[name] = stock_totals.get(name, 0) + cnt
            if name not in stock_daily:
                stock_daily[name] = {}
            stock_daily[name][day] = cnt

        top_stocks = sorted(stock_totals.items(), key=lambda x: x[1], reverse=True)[:20]

        # 生成完整日期列表
        today = datetime.now().date()
        dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]

        heat_map = {}
        for name, total in top_stocks:
            heat_map[name] = {"total": total, "daily": stock_daily.get(name, {})}

        return {"dates": dates, "tags": [s[0] for s in top_stocks], "heat_map": heat_map}
    except Exception as e:
        logger.warning(f"daily-intel-trend 失败: {e}")
        return {"dates": [], "tags": [], "heat_map": {}}


@router.get("/api/daily-intel-baskets", response_class=JSONResponse)
def api_daily_intel_baskets(days: int = 7):
    """走马灯：daily_intel_stocks 按产业链归组，返回篮子列表"""
    try:
        from config.chain_config import CHAINS, CHAIN_ORDER
        from utils.db_utils import execute_cloud_query

        rows = execute_cloud_query(
            """SELECT stock_name, stock_code, COUNT(DISTINCT source_id) AS cnt
               FROM daily_intel_stocks
               WHERE scan_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
                 AND stock_name IS NOT NULL AND stock_name != ''
               GROUP BY stock_name, stock_code
               ORDER BY cnt DESC""",
            [days],
        ) or []

        # stock_name -> {code, cnt}
        stock_map = {r["stock_name"]: {"code": r["stock_code"] or "", "cnt": int(r["cnt"] or 0)} for r in rows}

        baskets = []
        for chain_name in CHAIN_ORDER:
            chain = CHAINS.get(chain_name, {})
            for tier_key, tier in chain.get("tiers", {}).items():
                label = tier.get("label", tier_key)  # 直接用细分产业链描述
                tier_stocks = []
                total_cnt = 0
                for sname in tier.get("stocks", []):
                    if sname in stock_map:
                        info = stock_map[sname]
                        tier_stocks.append({"stock_name": sname, "stock_code": info["code"], "cnt": info["cnt"]})
                        total_cnt += info["cnt"]
                if tier_stocks:
                    tier_stocks.sort(key=lambda x: x["cnt"], reverse=True)
                    baskets.append({
                        "theme": label,
                        "group_logic": tier.get("label", ""),
                        "stocks": tier_stocks,
                        "mention_count": total_cnt,
                        "chunk_count": len(tier_stocks),
                    })

        # 按总提及次数排序
        baskets.sort(key=lambda x: x["mention_count"], reverse=True)
        return baskets
    except Exception as e:
        logger.warning(f"daily-intel-baskets 失败: {e}")
        return []


@router.get("/api/daily-intel-industry", response_class=JSONResponse)
def api_daily_intel_industry(days: int = 7):
    """细分行业热度：daily_intel_stocks.industry 按天统计，格式[一级-二级]"""
    try:
        from config.chain_config import CHAINS, CHAIN_ORDER
        from utils.db_utils import execute_cloud_query
        from datetime import datetime, timedelta

        rows = execute_cloud_query(
            """SELECT stock_name, stock_code, DATE(scan_date) AS day, COUNT(DISTINCT source_id) AS cnt
               FROM daily_intel_stocks
               WHERE scan_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
                 AND stock_name IS NOT NULL AND stock_name != ''
               GROUP BY stock_name, stock_code, DATE(scan_date)""",
            [days],
        ) or []

        # 构建 stock_name -> chain-tier 映射
        stock_to_industry = {}
        for chain_name in CHAIN_ORDER:
            chain = CHAINS.get(chain_name, {})
            for tier_key, tier in chain.get("tiers", {}).items():
                label = tier.get("label", tier_key)  # 直接用细分产业链描述
                for sname in tier.get("stocks", []):
                    if sname not in stock_to_industry:
                        stock_to_industry[sname] = label

        # 按 (industry_label, day) 统计
        today = datetime.now().date()
        dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]

        industry_day = {}
        for r in rows:
            name = r["stock_name"]
            label = stock_to_industry.get(name)
            if not label:
                continue
            day = str(r["day"])[:10]
            cnt = int(r["cnt"] or 0)
            if label not in industry_day:
                industry_day[label] = {"daily": {}, "total": 0}
            industry_day[label]["daily"][day] = industry_day[label]["daily"].get(day, 0) + cnt
            industry_day[label]["total"] += cnt

        industries = sorted(
            [{"name": k, "total": v["total"], "daily": v["daily"], "ai_direction": "neutral"}
             for k, v in industry_day.items()],
            key=lambda x: x["total"], reverse=True
        )[:30]

        return {"dates": dates, "industries": industries}
    except Exception as e:
        logger.warning(f"daily-intel-industry 失败: {e}")
        return {"dates": [], "industries": []}


@router.get("/api/mention-baskets", response_class=JSONResponse)
def api_mention_baskets(days: int = 30):
    """按 related_themes 聚合 stock_mentions 为篮子，最新30个"""
    try:
        from utils.content_query import aggregate_mentions_by_theme
        return aggregate_mentions_by_theme(days=days, limit=30)
    except Exception as e:
        logger.warning(f"mention-baskets 失败: {e}")
        return []


@router.get("/api/stock-baskets", response_class=JSONResponse)
def api_stock_baskets(days: int = 3):
    """向量语义聚类股票篮子（≤12主题）"""
    try:
        from hotspot.basket_builder import build_stock_baskets
        return build_stock_baskets(days=days)
    except Exception as e:
        logger.warning(f"stock-baskets 失败: {e}")
        return []


@router.get("/api/industry-chunk-heatmap", response_class=JSONResponse)
def api_industry_chunk_heatmap(days: int = 7):
    """细分行业 by 天 chunk 出现次数热力图"""
    try:
        from hotspot.industry_heatmap import get_industry_chunk_heatmap
        return get_industry_chunk_heatmap(days=days)
    except Exception as e:
        logger.warning(f"industry-chunk-heatmap 失败: {e}")
        return {"dates": [], "industries": []}


# ── 页面路由 ──────────────────────────────────────────────────

def _calc_days(start: str = None, end: str = None, days: int = 7) -> tuple:
    """计算日期范围，返回 (days, start_date, end_date, custom_range)"""
    if start and end:
        from datetime import datetime as dt
        try:
            s = dt.strptime(start, "%Y-%m-%d")
            e = dt.strptime(end, "%Y-%m-%d")
            delta = (e - s).days
            return max(delta, 1), start, end, True
        except ValueError:
            pass
    end_date = datetime.now().strftime("%Y-%m-%d")
    from datetime import timedelta
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    return days, start_date, end_date, False


@router.get("", response_class=HTMLResponse)
def hotspot_overview(request: Request, days: int = 7, start: str = None, end: str = None, user: TokenData = Depends(get_optional_user)):
    # 检查热点权限 - 未登录用户也可访问，显示对应状态
    if user:
        user_id = user.user_id
        can_access, msg = check_quota(user_id, 'hotspot')
        from utils.quota_service import get_user_quota
        quota = get_user_quota(user_id)
        user_role = user.role
    else:
        can_access = True
        msg = ""
        quota = {}
        user_role = "guest"

    d, start_date, end_date, custom_range = _calc_days(start, end, days)
    ctx = {
        "request": request,
        "active_page": "hotspot",
        "access_denied": not can_access,
        "deny_message": msg,
        "user_role": user_role,
        "quota": quota,
        "days": d,
        "start_date": start_date,
        "end_date": end_date,
        "custom_range": custom_range,
        "chart_data": get_frequency_chart_data(d, start_date=start_date, end_date=end_date),
        "clusters": get_tag_clusters(d),
    }
    return templates.TemplateResponse("hotspot.html", ctx)


@router.get("/research/{group_id}", response_class=HTMLResponse)
def hotspot_research(request: Request, group_id: int, tab: str = "logic"):
    data = get_research_data(group_id)
    if not data:
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/hotspot")

    ctx = {
        "request": request,
        "active_page": "hotspot",
        "group": data["group"],
        "research": data["research"],
        "active_tab": tab,
    }
    return templates.TemplateResponse("hotspot_research.html", ctx)


@router.post("/recommend", response_class=HTMLResponse)
def trigger_recommend(request: Request, days: int = 7, start: str = None, end: str = None, user: TokenData = Depends(get_optional_user)):
    """触发 AI 标签组推荐"""
    if not user:
        d, start_date, end_date, custom_range = _calc_days(start, end, days)
        ctx = {
            "request": request,
            "active_page": "hotspot",
            "days": d, "start_date": start_date, "end_date": end_date,
            "custom_range": custom_range,
            "user_role": "guest",
            "access_denied": False, "deny_message": "", "quota": {},
            "chart_data": get_frequency_chart_data(d, start_date=start_date, end_date=end_date),
            "clusters": get_tag_clusters(d),
            "recommend_error": "请先登录后使用 AI 推荐功能",
        }
        return templates.TemplateResponse("hotspot.html", ctx)

    user_id = user.user_id

    # 检查标签组配额
    can_create, msg = check_quota(user_id, 'tag_group')
    if not can_create:
        ctx = {
            "request": request,
            "active_page": "hotspot",
            "recommend_error": msg,
        }
        return templates.TemplateResponse("hotspot.html", ctx)

    # 消耗配额
    consume_quota(user_id, 'tag_group', 1)

    d, start_date, end_date, custom_range = _calc_days(start, end, days)
    error = None
    try:
        groups = recommend_tag_groups(d, top_n=20)
        # 二次合并 + 三重筛选 → Top 12
        filtered = merge_and_filter_groups(groups, days=d)
        # 清空旧标签组，再保存新结果
        clear_all_tag_groups()
        for g in filtered:
            extra = {
                "macro_positive": g.get("macro_positive", False),
                "industry_positive": g.get("industry_positive", False),
                "leader_stock": g.get("leader_stock", ""),
                "leader_code": g.get("leader_code", ""),
                "leader_net_inflow": g.get("leader_net_inflow", 0),
                "group_total_inflow": g.get("group_total_inflow", 0),
                "group_total_cap": g.get("group_total_cap", 0),
                "group_stock_count": g.get("group_stock_count", 0),
                "daily_inflow": g.get("daily_inflow", []),
                "kg_stock_groups": g.get("kg_stock_groups", []),
            }
            save_tag_group(
                g.get("group_name", " + ".join(g["tags"][:3])),
                g["tags"],
                group_logic=g.get("group_logic"),
                time_range=d,
                extra=extra,
            )
    except Exception as e:
        error = str(e)

    ctx = {
        "request": request,
        "active_page": "hotspot",
        "days": d,
        "start_date": start_date,
        "end_date": end_date,
        "custom_range": custom_range,
        "chart_data": get_frequency_chart_data(d, start_date=start_date, end_date=end_date),
        "clusters": get_tag_clusters(d),
        "recommend_error": error,
    }
    return templates.TemplateResponse("hotspot.html", ctx)


@router.post("/research/{group_id}/run")
def run_deep_research(group_id: int, background_tasks: BackgroundTasks, user: TokenData = Depends(get_optional_user)):
    """触发深度研究（后台执行，返回 task_id）"""
    if not user:
        return JSONResponse({"ok": False, "error": "请先登录后使用深度研究功能"}, status_code=200)

    user_id = user.user_id

    # 检查研究配额
    can_run, msg = check_quota(user_id, 'research')
    if not can_run:
        return JSONResponse({"ok": False, "error": msg}, status_code=403)

    # 消耗配额
    consume_quota(user_id, 'research', 1)

    groups = get_saved_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    group_name = group["group_name"] if group else f"标签组#{group_id}"
    task_id = _create_hotspot_task(group_id, group_name)

    def _run():
        try:
            result = research_tag_group(
                group_id,
                progress_callback=_make_hotspot_progress_cb(task_id),
            )
            rid = result.get("research_id") if result else None
            _update_hotspot_task(task_id, status="done", progress=100,
                                message="研究完成", finished_at=time.time(),
                                research_id=rid)
        except Exception as e:
            logger.error(f"标签组研究失败: {e}")
            _update_hotspot_task(task_id, status="error", progress=100,
                                message=f"失败: {e}", finished_at=time.time())

    background_tasks.add_task(_run)
    return JSONResponse({"ok": True, "task_id": task_id, "message": "研究已启动"})


@router.get("/api/research-tasks")
def get_hotspot_research_tasks():
    """获取所有热点研究任务状态"""
    now = time.time()
    with _hotspot_lock:
        stale = [k for k, v in _hotspot_tasks.items()
                 if v["finished_at"] and now - v["finished_at"] > 3600]
        for k in stale:
            del _hotspot_tasks[k]
        return list(_hotspot_tasks.values())


@router.post("/api/save-to-kg", response_class=JSONResponse)
async def save_to_kg(request: Request):
    """将标签组保存到知识图谱（theme 类型实体）"""
    body = await request.json()
    group_id = body.get("group_id")
    if not group_id:
        return JSONResponse({"error": "缺少 group_id"}, status_code=400)

    groups = get_saved_groups()
    group = next((g for g in groups if g["id"] == int(group_id)), None)
    if not group:
        return JSONResponse({"error": "标签组不存在"}, status_code=404)

    group_name = group["group_name"]
    group_logic = group.get("group_logic") or ""
    tags = json.loads(group.get("tags_json") or "[]")

    properties = {"tags": tags, "source": "hotspot_tag_group", "source_id": group_id}

    # 检查是否已存在同名 theme 实体
    existing = execute_query(
        "SELECT id FROM kg_entities WHERE entity_type='theme' AND entity_name=?",
        [group_name],
    )
    if existing:
        eid = existing[0]["id"]
        update_entity(eid, properties=properties, investment_logic=group_logic)
    else:
        eid = add_entity(
            entity_type="theme",
            entity_name=group_name,
            investment_logic=group_logic,
            properties=properties,
        )

    return {"ok": True, "entity_id": eid, "message": f"已保存到知识图谱: {group_name}"}


@router.post("/group/{group_id}/delete")
def delete_group(group_id: int):
    delete_tag_group(group_id)
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/hotspot", status_code=303)


@router.get("/research/{group_id}/followup", response_class=HTMLResponse)
def hotspot_followup(request: Request, group_id: int):
    """追踪研究配置页"""
    data = get_research_data(group_id)
    if not data:
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/hotspot")

    ctx = {
        "request": request,
        "active_page": "hotspot",
        "group": data["group"],
        "research": data["research"],
    }
    return templates.TemplateResponse("hotspot_followup.html", ctx)


@router.post("/research/{group_id}/run-followup")
async def run_followup_research(request: Request, group_id: int,
                                background_tasks: BackgroundTasks):
    """触发追踪研究（带配置）— 后台执行"""
    form = await request.form()
    groups = get_saved_groups()
    group = next((g for g in groups if g["id"] == group_id), None)
    group_name = group["group_name"] if group else f"标签组#{group_id}"
    task_id = _create_hotspot_task(group_id, group_name)

    def _run():
        try:
            result = research_tag_group(
                group_id,
                progress_callback=_make_hotspot_progress_cb(task_id),
            )
            rid = result.get("research_id") if result else None
            _update_hotspot_task(task_id, status="done", progress=100,
                                message="研究完成", finished_at=time.time(),
                                research_id=rid)
        except Exception as e:
            logger.error(f"标签组追踪研究失败: {e}")
            _update_hotspot_task(task_id, status="error", progress=100,
                                message=f"失败: {e}", finished_at=time.time())

    background_tasks.add_task(_run)
    return JSONResponse({"ok": True, "task_id": task_id, "message": "追踪研究已启动"})


@router.post("/research/{group_id}/update-stocks")
async def update_research_stocks(request: Request, group_id: int):
    """更新研究结果中的推荐个股（用户手动编辑）"""
    from utils.db_utils import execute_insert
    body = await request.json()
    stocks_data = body.get("stocks", [])

    # 找到最新的研究记录
    rows = execute_query(
        "SELECT id FROM tag_group_research WHERE group_id=? ORDER BY id DESC LIMIT 1",
        [group_id],
    )
    if not rows:
        return JSONResponse({"error": "该标签组尚无研究记录"}, status_code=404)

    research_id = rows[0]["id"]
    execute_insert(
        "UPDATE tag_group_research SET top10_stocks_json=? WHERE id=?",
        [json.dumps(stocks_data, ensure_ascii=False), research_id],
    )
    return {"ok": True, "message": "推荐个股已更新"}


@router.get("/api/search-stocks")
def search_stocks(q: str = ""):
    """搜索股票（代码或名称模糊匹配）"""
    if not q or len(q) < 1:
        return []
    rows = execute_query(
        """SELECT stock_code, stock_name, market FROM stock_info
           WHERE stock_code LIKE ? OR stock_name LIKE ?
           LIMIT 15""",
        [f"%{q}%", f"%{q}%"],
    )
    return [{"stock_code": r["stock_code"], "stock_name": r["stock_name"],
             "market": r.get("market") or ""} for r in rows]


# ── Tab 片段路由（AJAX 局部刷新）────────────────────────────

@router.get("/research/{group_id}/tab/{tab_name}", response_class=HTMLResponse)
def get_tab_content(request: Request, group_id: int, tab_name: str):
    """返回指定 tab 的 HTML 片段"""
    data = get_research_data(group_id)
    if not data:
        return HTMLResponse("<p class='text-slate-500 text-sm'>数据加载失败</p>")

    ctx = {
        "request": request,
        "group": data["group"],
        "research": data["research"],
        "active_tab": tab_name,
    }
    return templates.TemplateResponse("partials/hotspot_tab_content.html", ctx)


# ── Chat API ─────────────────────────────────────────────────

@router.get("/research/{group_id}/chat/history")
def chat_history(group_id: int):
    """返回该标签组的聊天历史"""
    messages = get_chat_history(group_id)
    return {"ok": True, "messages": messages}


@router.post("/research/{group_id}/chat/send")
async def chat_send(request: Request, group_id: int):
    """接收用户消息，存入 DB 等待 worker 处理"""
    body = await request.json()
    message = body.get("message", "").strip()
    if not message:
        return JSONResponse({"error": "消息不能为空"}, status_code=400)

    result = submit_chat_message(group_id, message)
    return result


@router.get("/research/{group_id}/chat/poll")
def chat_poll(group_id: int):
    """前端轮询：检查 pending 回复是否已完成"""
    return get_pending_reply(group_id)


@router.post("/research/{group_id}/chat/greet")
def chat_greet(group_id: int):
    """首次打开聊天面板时发出主动问候（无历史时生效）"""
    return ensure_chat_greeting(group_id)


# ── Save / Dismiss / Set-Pending ─────────────────────────────

@router.post("/research/{group_id}/save")
async def save_research(group_id: int):
    """保存研究报告 + 计算组合统计 + 触发 KG 更新"""
    # 找到最新研究
    rows = execute_query(
        "SELECT id, top10_stocks_json FROM tag_group_research "
        "WHERE group_id=? ORDER BY id DESC LIMIT 1",
        [group_id],
    )
    if not rows:
        return JSONResponse({"error": "该标签组尚无研究记录"}, status_code=404)

    research_id = rows[0]["id"]
    top10_stocks = json.loads(rows[0].get("top10_stocks_json") or "[]")

    # 1. 计算组合统计
    portfolio_stats = _calc_portfolio_stats(top10_stocks)

    # 2. 更新状态
    execute_insert(
        "UPDATE tag_group_research SET status='saved', saved_at=NOW(), "
        "portfolio_stats_json=? WHERE id=?",
        [json.dumps(portfolio_stats, ensure_ascii=False, default=str), research_id],
    )

    # 3. 触发 KG 更新
    try:
        _save_to_kg_with_stocks(group_id, top10_stocks)
    except Exception as e:
        logger.warning(f"KG 更新失败: {e}")

    return {
        "ok": True,
        "message": "研究已保存",
        "portfolio_stats": portfolio_stats,
    }


@router.post("/research/{group_id}/dismiss")
async def dismiss_research(group_id: int):
    """标记为解散"""
    rows = execute_query(
        "SELECT id FROM tag_group_research WHERE group_id=? ORDER BY id DESC LIMIT 1",
        [group_id],
    )
    if rows:
        execute_insert(
            "UPDATE tag_group_research SET status='dismissed' WHERE id=?",
            [rows[0]["id"]],
        )
    return {"ok": True, "message": "已解散"}


@router.post("/research/{group_id}/set-pending")
async def set_pending_research(group_id: int):
    """标记为待定"""
    rows = execute_query(
        "SELECT id FROM tag_group_research WHERE group_id=? ORDER BY id DESC LIMIT 1",
        [group_id],
    )
    if rows:
        execute_insert(
            "UPDATE tag_group_research SET status='pending' WHERE id=?",
            [rows[0]["id"]],
        )
    return {"ok": True, "message": "已标记为待定"}


# ── Helper: 组合统计 ──────────────────────────────────────────

def _calc_portfolio_stats(top10_stocks: list) -> dict:
    """计算推荐个股的组合统计"""
    all_codes = []
    for grp in top10_stocks:
        for s in grp.get("stocks", []):
            all_codes.append(s.get("stock_code"))
    all_codes = [c for c in all_codes if c]

    if not all_codes:
        return {"total_inflow": 0, "total_cap": 0, "daily_inflow": [], "stock_count": 0}

    # 总市值：优先用 stock_info.market_cap（亿元单位），否则用最新收盘价 × 总股本
    placeholders = ",".join(["?"] * len(all_codes))
    cap_rows = execute_query(
        f"SELECT SUM(market_cap) as total_cap FROM stock_info WHERE stock_code IN ({placeholders}) AND market_cap > 0",
        all_codes,
    )
    total_cap = float(cap_rows[0]["total_cap"] or 0) * 1e8 if cap_rows else 0  # 转换为元

    # 如果 market_cap 为空，尝试用 close × total_shares 计算
    if total_cap == 0:
        cap_rows2 = execute_query(
            f"""SELECT SUM(sd.close * si.total_shares) as total_cap
                FROM stock_daily sd
                JOIN stock_info si ON sd.stock_code = si.stock_code
                WHERE sd.stock_code IN ({placeholders})
                AND si.total_shares > 0
                AND sd.trade_date >= DATE_SUB(CURDATE(), INTERVAL 10 DAY)""",
            all_codes,
        )
        total_cap = float(cap_rows2[0]["total_cap"] or 0) if cap_rows2 else 0

    flow_rows = execute_query(
        f"""SELECT SUM(main_net_inflow) as total_inflow
            FROM capital_flow
            WHERE stock_code IN ({placeholders})
            AND trade_date >= (SELECT DISTINCT trade_date FROM capital_flow ORDER BY trade_date DESC LIMIT 1 OFFSET 6)""",
        all_codes,
    )
    total_inflow = float(flow_rows[0]["total_inflow"] or 0) if flow_rows else 0

    # 每日资金流入（最近7个交易日）
    daily_rows = execute_query(
        f"""SELECT trade_date, SUM(main_net_inflow) as inflow
            FROM capital_flow
            WHERE stock_code IN ({placeholders})
            AND trade_date >= (SELECT DISTINCT trade_date FROM capital_flow ORDER BY trade_date DESC LIMIT 1 OFFSET 6)
            GROUP BY trade_date ORDER BY trade_date""",
        all_codes,
    )
    daily_inflow = [
        {"date": str(r["trade_date"]), "inflow": float(r["inflow"] or 0)}
        for r in daily_rows
    ]

    return {
        "total_inflow": total_inflow,
        "total_cap": total_cap,
        "daily_inflow": daily_inflow,
        "stock_count": len(all_codes),
    }


def _save_to_kg_with_stocks(group_id: int, top10_stocks: list):
    """保存到 KG 并建立 stock 关系"""
    groups = get_saved_groups()
    group = next((g for g in groups if g["id"] == int(group_id)), None)
    if not group:
        return

    group_name = group["group_name"]
    group_logic = group.get("group_logic") or ""
    tags = json.loads(group.get("tags_json") or "[]")

    properties = {"tags": tags, "source": "hotspot_tag_group", "source_id": group_id}

    # 创建或更新 theme 实体
    existing = execute_query(
        "SELECT id FROM kg_entities WHERE entity_type='theme' AND entity_name=?",
        [group_name],
    )
    if existing:
        theme_id = existing[0]["id"]
        update_entity(theme_id, properties=properties, investment_logic=group_logic)
    else:
        theme_id = add_entity(
            entity_type="theme",
            entity_name=group_name,
            investment_logic=group_logic,
            properties=properties,
        )

    if not theme_id:
        return

    # 建立 theme → company 关系
    for grp in top10_stocks:
        for s in grp.get("stocks", []):
            code = s.get("stock_code")
            name = s.get("stock_name", code)
            if not code:
                continue
            # 确保 company 实体存在
            comp_rows = execute_query(
                "SELECT id FROM kg_entities WHERE entity_type='company' AND entity_name=?",
                [code],
            )
            if comp_rows:
                comp_id = comp_rows[0]["id"]
            else:
                comp_id = add_entity(
                    entity_type="company",
                    entity_name=code,
                    description=name,
                    properties={"stock_code": code, "stock_name": name},
                )
            if comp_id:
                add_relationship(
                    source_id=theme_id,
                    target_id=comp_id,
                    relation_type="recommends",
                    strength=0.7,
                    direction="positive",
                    evidence=f"标签组研究推荐: {group_name}",
                    confidence=0.6,
                )

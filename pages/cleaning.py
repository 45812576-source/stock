"""信息采集与清洗 — 采集、清洗、浏览、设置"""
import streamlit as st
import sys
import json
import threading
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.db_utils import execute_query, execute_insert
from utils.fetch_config import load_fetch_settings, save_fetch_settings, SOURCE_GROUPS, SOURCE_CATALOG


@st.cache_data(ttl=30)
def _get_today_source_counts():
    """获取今日各采集源的采集数量"""
    today = datetime.now().strftime("%Y-%m-%d")
    counts = {}
    # jasper子源通过meta_json区分
    jasper_patterns = {
        "cls": "全球快讯", "caixin": "财新",
        "hot_stocks": "热门股票", "watchlist": "跟踪个股", "cctv": "CCTV",
    }
    for key, pattern in jasper_patterns.items():
        try:
            rows = execute_query(
                "SELECT COUNT(*) as cnt FROM raw_items r JOIN data_sources d ON r.source_id=d.id "
                "WHERE d.name='jasper' AND date(r.fetched_at)=? AND r.meta_json LIKE ?",
                [today, f"%{pattern}%"])
            counts[key] = rows[0]["cnt"] if rows else 0
        except Exception:
            counts[key] = 0
    # 独立源
    for key, db_name in [("djyanbao", "djyanbao"), ("fxbaogao", "fxbaogao"), ("em_report", "eastmoney_report")]:
        try:
            rows = execute_query(
                "SELECT COUNT(*) as cnt FROM raw_items r JOIN data_sources d ON r.source_id=d.id "
                "WHERE d.name=? AND date(r.fetched_at)=?", [db_name, today])
            counts[key] = rows[0]["cnt"] if rows else 0
        except Exception:
            counts[key] = 0
    # 知识星球
    try:
        rows = execute_query(
            "SELECT COUNT(*) as cnt FROM raw_items WHERE date(fetched_at)=? AND meta_json LIKE '%知识星球%'",
            [today])
        counts["zsxq"] = rows[0]["cnt"] if rows else 0
    except Exception:
        counts["zsxq"] = 0
    return counts


st.title("📰 信息采集与清洗")

# ========== 操作消息持久化 ==========
if "op_messages" not in st.session_state:
    st.session_state.op_messages = []


def add_message(msg_type, text):
    st.session_state.op_messages.append({
        "type": msg_type, "text": text,
        "time": datetime.now().strftime("%H:%M:%S")
    })
    st.session_state.op_messages = st.session_state.op_messages[-10:]


# ========== 后台任务系统 ==========
def _start_bg_task(task_key, steps):
    """启动后台采集任务，UI立即恢复可用"""
    state = {
        "running": True, "done": False,
        "results": [], "current": "", "progress": 0, "total": len(steps),
        "started_at": datetime.now().strftime("%H:%M:%S"),
    }
    st.session_state[task_key] = state

    def worker():
        for i, (name, func) in enumerate(steps):
            state["current"] = name
            state["progress"] = i
            try:
                count = func()
                state["results"].append(f"✅ {name}: {count}条")
            except Exception as e:
                state["results"].append(f"❌ {name}: {e}")
        state["running"] = False
        state["done"] = True
        state["progress"] = state["total"]

    t = threading.Thread(target=worker, daemon=True)
    t.start()


def _is_any_bg_running(*task_keys):
    """检查是否有后台任务在运行（含超时清理）"""
    for k in task_keys:
        s = st.session_state.get(k)
        if s and s.get("running"):
            # 超过5分钟视为僵尸任务，自动清理
            try:
                started = datetime.strptime(s["started_at"], "%H:%M:%S").replace(
                    year=datetime.now().year, month=datetime.now().month, day=datetime.now().day)
                if (datetime.now() - started).total_seconds() > 300:
                    s["running"] = False
                    s["done"] = True
                    s["results"].append("⚠️ 任务超时，已自动清理")
                    continue
            except Exception:
                pass
            return True
    return False


def _render_bg_status_inline(task_key, label="任务"):
    """内联渲染后台任务状态（不改变widget树结构）"""
    if task_key not in st.session_state:
        return False
    state = st.session_state[task_key]
    if state.get("running"):
        prog = state["progress"] / state["total"] if state["total"] > 0 else 0
        st.progress(prog, text=f"⏳ {label}: {state['current']} ({state['progress']}/{state['total']})")
        for r in state["results"]:
            st.caption(r)
        return True
    elif state.get("done"):
        for r in state["results"]:
            st.caption(r)
        del st.session_state[task_key]
        _get_cleaning_stats.clear()
        return True
    return False


def _build_fetch_steps(src_cfg, hours):
    """根据配置构建采集步骤列表 — 动态遍历所有 enabled 源"""
    import os
    from utils.sys_config import get_config

    # fetcher_type → lambda 工厂
    def _make_fetcher(key, cfg, fetcher_type, hours):
        if fetcher_type == "jasper":
            return lambda k=key, h=hours: __import__("ingestion.jasper_source", fromlist=["JasperSource"]).JasperSource().fetch(hours=h, sources=[k])
        elif fetcher_type == "djyanbao":
            lim = cfg.get("limit", 100)
            return lambda l=lim: __import__("ingestion.djyanbao_source", fromlist=["DjyanbaoSource"]).DjyanbaoSource().fetch(limit=l)
        elif fetcher_type == "fxbaogao":
            lim = cfg.get("limit", 100)
            return lambda l=lim: __import__("ingestion.fxbaogao_source", fromlist=["FxbaogaoSource"]).FxbaogaoSource().fetch(limit=l)
        elif fetcher_type == "em_report":
            lim = cfg.get("limit", 10)
            return lambda l=lim: __import__("ingestion.eastmoney_report_source", fromlist=["EastmoneyReportSource"]).EastmoneyReportSource().fetch(limit=l)
        elif fetcher_type == "zsxq":
            zsxq_cookie = st.session_state.get("zsxq_cookie_val", "") or get_config("zsxq_cookie") or os.environ.get("ZSXQ_COOKIE", "")
            if not zsxq_cookie:
                return None
            from config import ZSXQ_GROUP_ID
            mp = cfg.get("max_pages", 5)
            return lambda c=zsxq_cookie, h=hours, p=mp: __import__("ingestion.zsxq_source", fromlist=["fetch_zsxq_data"]).fetch_zsxq_data(ZSXQ_GROUP_ID, c, hours=h, max_pages=p).get("saved", 0)
        return None

    group_order = list(SOURCE_GROUPS.keys())
    items = []
    for key, cfg in src_cfg.items():
        if not cfg.get("enabled"):
            continue
        fetcher_type = cfg.get("fetcher_type", "")
        fn = _make_fetcher(key, cfg, fetcher_type, hours)
        if fn is not None:
            items.append((key, cfg["label"], cfg.get("group", "news"), fn))
    items.sort(key=lambda x: group_order.index(x[2]) if x[2] in group_order else 99)
    return [(label, fn) for _, label, _, fn in items]



if st.session_state.op_messages:
    with st.expander(f"📋 操作记录 ({len(st.session_state.op_messages)}条)", expanded=False):
        for msg in reversed(st.session_state.op_messages):
            getattr(st, msg["type"], st.info)(f"[{msg['time']}] {msg['text']}")

# ========== 顶部统计（常驻） ==========
@st.cache_data(ttl=30)
def _get_cleaning_stats():
    from cleaning.batch_cleaner import get_cleaning_stats
    return get_cleaning_stats()


try:
    stats = _get_cleaning_stats()
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("待清洗", stats.get("pending", 0))
    m2.metric("处理中", stats.get("processing", 0))
    m3.metric("已清洗", stats.get("cleaned", 0))
    m4.metric("失败", stats.get("failed", 0))
    m5.metric("今日清洗", stats.get("today_cleaned", 0))
except Exception:
    st.caption("统计数据加载失败")

st.markdown("---")

# ========== 四个标签页 ==========
tab_fetch, tab_clean, tab_browse, tab_settings = st.tabs([
    "📥 采集", "🧹 清洗", "📋 数据浏览", "⚙️ 设置"
])

# ==================== Tab 1: 采集 ====================
_FETCH_TASK_KEYS = ["bg_fetch_all", "bg_fetch_news", "bg_fetch_fxbg", "bg_fetch_emrpt", "bg_fetch_djyb", "bg_fetch_zsxq"]

with tab_fetch:
    # 后台任务状态（顶部显示）
    _fetch_has_status = False
    for _fk in _FETCH_TASK_KEYS:
        _fetch_has_status = _render_bg_status_inline(_fk, "采集") or _fetch_has_status
    if _fetch_has_status:
        if st.button("🔄 刷新状态", key="ft_refresh"):
            st.rerun()
        st.markdown("---")

    # 一键全量采集
    st.markdown("**一键采集**")
    if st.button("🚀 一键采集全部信息源", type="primary", use_container_width=True, key="ft_btn_all"):
        if _is_any_bg_running(*_FETCH_TASK_KEYS):
            st.warning("有采集任务正在运行，请等待完成")
        else:
            settings = load_fetch_settings()
            steps = _build_fetch_steps(settings["sources"], settings["news_hours"])
            if not steps:
                st.warning("没有启用任何采集源，请在「设置」标签页中配置")
            else:
                _start_bg_task("bg_fetch_all", steps)
                st.rerun()

    st.markdown("---")

    # 分源采集
    st.markdown("**分源采集**")
    fc1, fc2 = st.columns(2)

    with fc1:
        if st.button("📥 财经新闻", use_container_width=True, key="ft_btn_news"):
            if _is_any_bg_running(*_FETCH_TASK_KEYS):
                st.warning("有采集任务正在运行")
            else:
                _start_bg_task("bg_fetch_news", [("财经新闻",
                    lambda: __import__("ingestion.jasper_source", fromlist=["JasperSource"])
                        .JasperSource().fetch(hours=24))])
                st.rerun()

        if st.button("📄 发现报告", use_container_width=True, key="ft_btn_fxbg"):
            if _is_any_bg_running(*_FETCH_TASK_KEYS):
                st.warning("有采集任务正在运行")
            else:
                _start_bg_task("bg_fetch_fxbg", [("发现报告",
                    lambda: __import__("ingestion.fxbaogao_source", fromlist=["FxbaogaoSource"])
                        .FxbaogaoSource().fetch(limit=10))])
                st.rerun()

    with fc2:
        if st.button("📊 东财研报(PDF)", use_container_width=True, key="ft_btn_emrpt"):
            if _is_any_bg_running(*_FETCH_TASK_KEYS):
                st.warning("有采集任务正在运行")
            else:
                _start_bg_task("bg_fetch_emrpt", [("东财研报",
                    lambda: __import__("ingestion.eastmoney_report_source", fromlist=["EastmoneyReportSource"])
                        .EastmoneyReportSource().fetch(limit=5))])
                st.rerun()

        if st.button("📄 洞见研报", use_container_width=True, key="ft_btn_djyb"):
            if _is_any_bg_running(*_FETCH_TASK_KEYS):
                st.warning("有采集任务正在运行")
            else:
                _start_bg_task("bg_fetch_djyb", [("洞见研报",
                    lambda: __import__("ingestion.djyanbao_source", fromlist=["DjyanbaoSource"])
                        .DjyanbaoSource().fetch(limit=10))])
                st.rerun()

        if st.button("🌟 知识星球", use_container_width=True, key="ft_btn_zsxq"):
            if _is_any_bg_running(*_FETCH_TASK_KEYS):
                st.warning("有采集任务正在运行")
            else:
                import os
                cookie = st.session_state.get("zsxq_cookie_val", "") or os.environ.get("ZSXQ_COOKIE", "")
                if not cookie:
                    add_message("warning", "请先在「设置」标签页配置知识星球Cookie")
                else:
                    from config import ZSXQ_GROUP_ID
                    _start_bg_task("bg_fetch_zsxq", [("知识星球",
                        lambda c=cookie: __import__("ingestion.zsxq_source", fromlist=["fetch_zsxq_data"])
                            .fetch_zsxq_data(ZSXQ_GROUP_ID, c, hours=24).get("saved", 0))])
                    st.rerun()

# ==================== Tab 2: 清洗 ====================
_CLEAN_TASK_KEYS = ["bg_clean", "bg_retry", "bg_dashboard"]

with tab_clean:
    st.subheader("清洗与榜单生成")

    # 后台任务状态（顶部显示）
    _clean_has_status = False
    for _ck in _CLEAN_TASK_KEYS:
        _clean_has_status = _render_bg_status_inline(_ck, "清洗") or _clean_has_status
    if _clean_has_status:
        if st.button("🔄 刷新状态", key="cl_refresh"):
            st.rerun()
        st.markdown("---")

    cl1, cl2 = st.columns([1, 1])

    with cl1:
        clean_limit = st.number_input("清洗数量", min_value=1, max_value=200, value=20,
                                      key="cl_clean_limit")
        deep = st.checkbox("深度分析（高重要性条目）", key="cl_deep_analysis")

        if st.button("🧹 批量清洗", type="primary", use_container_width=True, key="cl_btn_clean"):
            if _is_any_bg_running(*_CLEAN_TASK_KEYS):
                st.warning("有清洗任务正在运行，请等待完成")
            else:
                pending_count = execute_query(
                    "SELECT COUNT(*) as cnt FROM raw_items WHERE processing_status='pending'"
                )
                if not pending_count or pending_count[0]["cnt"] == 0:
                    add_message("warning", "没有待清洗的数据，请先采集")
                else:
                    lim = clean_limit
                    da = deep
                    def _do_clean():
                        from cleaning.batch_cleaner import batch_clean
                        r = batch_clean(limit=lim, deep_analysis=da)
                        return f"成功{r['success']}, 失败{r['failed']}, 总计{r['total']}"
                    _start_bg_task("bg_clean", [("批量清洗", _do_clean)])
                    st.rerun()

    with cl2:
        if st.button("🔄 重试失败条目", use_container_width=True, key="cl_btn_retry"):
            if _is_any_bg_running(*_CLEAN_TASK_KEYS):
                st.warning("有清洗任务正在运行")
            else:
                def _do_retry():
                    from cleaning.batch_cleaner import retry_failed
                    r = retry_failed(limit=20)
                    return f"成功{r['success']}, 失败{r['failed']}"
                _start_bg_task("bg_retry", [("重试失败", _do_retry)])
                st.rerun()

        st.markdown("---")

        if st.button("🏆 生成Dashboard榜单", use_container_width=True, key="cl_btn_dashboard"):
            if _is_any_bg_running(*_CLEAN_TASK_KEYS):
                st.warning("有清洗任务正在运行")
            else:
                def _do_dashboard():
                    from dashboards.pipeline import generate_all_dashboards
                    return generate_all_dashboards()
                _start_bg_task("bg_dashboard", [("榜单生成", _do_dashboard)])
                st.rerun()

        st.caption("清洗完成后生成榜单，供「每日概览」和「热点研究」使用")

# ==================== 清洗结果渲染函数 ====================

def _render_skill_result(sj, c, item):
    """渲染Skill格式的结构化清洗结果"""
    basic = sj.get("basic", {})
    tags = sj.get("tags", {})
    opp = sj.get("opportunity", {})
    summary = sj.get("summary", {})
    items = sj.get("items", [])
    ts = sj.get("type_specific", {})

    # --- 1. 基础信息栏 ---
    rc1, rc2, rc3, rc4 = st.columns(4)
    info_type = basic.get("info_type", sj.get("event_type", ""))
    sentiment = sj.get("sentiment", c.get("sentiment", ""))
    importance = sj.get("importance", c.get("importance", 0)) or 0
    confidence = sj.get("confidence", c.get("confidence", 0)) or 0

    rc1.text(f"类型: {info_type}")
    sentiment_map = {"positive": "🟢 利好", "negative": "🔴 利空", "neutral": "⚪ 中性"}
    rc2.text(sentiment_map.get(sentiment, sentiment))
    rc3.text(f"重要性: {'⭐' * min(importance, 5)}")
    rc4.text(f"置信度: {confidence:.0%}" if isinstance(confidence, float) else f"置信度: {confidence}")

    # --- 2. 摘要分析 ---
    core_facts = summary.get("core_facts", []) if isinstance(summary, dict) else []
    if core_facts:
        st.info("**核心事实:** " + "；".join(core_facts))

    opinions = summary.get("opinions", []) if isinstance(summary, dict) else []
    if opinions:
        st.markdown("**关键观点:**")
        for op in opinions[:5]:
            if isinstance(op, dict):
                src = op.get("source", "")
                logic = op.get("logic", "")
                st.markdown(f"- {op.get('opinion', '')}  \n  `{src}` | 逻辑: {logic}")

    # --- 3. MECE标签体系 ---
    tag_parts = []
    if tags.get("market"):
        tag_parts.append(f"`{tags['market']}`")
    if tags.get("board") and tags["board"] != "null":
        tag_parts.append(f"`{tags['board']}`")
    if tags.get("sw_industry_l1"):
        chain = tags["sw_industry_l1"]
        if tags.get("sw_industry_l2"):
            chain += f" > {tags['sw_industry_l2']}"
        if tags.get("sw_industry_l3"):
            chain += f" > {tags['sw_industry_l3']}"
        tag_parts.append(f"`{chain}`")
    if tags.get("invest_theme"):
        tag_parts.append(f"`{tags['invest_theme']}`")
    for st_tag in (tags.get("sub_theme") or []):
        tag_parts.append(f"`{st_tag}`")
    if tags.get("event_type"):
        nature = tags.get("event_nature", "")
        tag_parts.append(f"`{tags['event_type']}/{nature}`")
    if tags.get("impact_level"):
        tag_parts.append(f"`影响:{tags['impact_level']}`")
    if tags.get("timeliness"):
        tag_parts.append(f"`{tags['timeliness']}`")
    if tags.get("persistence"):
        tag_parts.append(f"`{tags['persistence']}`")

    if tag_parts:
        st.markdown("**标签:** " + " ".join(tag_parts))

    # --- 4. 投资机会 ---
    overall = opp.get("overall", {})
    if overall:
        level = overall.get("level", "○")
        action = overall.get("action", "")
        attention = overall.get("attention", "")
        opp_type = overall.get("opp_type", "")

        opp_line = f"**投资机会:** {level}"
        if opp_type:
            opp_line += f" | {opp_type}"
        if attention:
            opp_line += f" | 关注度:{attention}"
        if action:
            opp_line += f" | {action}"
        st.markdown(opp_line)

        if overall.get("logic"):
            st.caption(f"逻辑: {overall['logic']}")
        if overall.get("catalyst"):
            st.caption(f"催化剂: {overall['catalyst']}")
        if overall.get("risk"):
            st.caption(f"⚠️ 风险: {overall['risk']}")

    # 四类机会明细
    hit_cats = []
    for cat_name in ["超预期财报", "机构密集覆盖", "重大利好", "政策风向"]:
        cat = opp.get(cat_name, {})
        if cat.get("hit"):
            detail = cat.get("detail", {})
            detail_str = " | ".join(f"{k}:{v}" for k, v in detail.items() if v) if isinstance(detail, dict) else ""
            hit_cats.append(f"✅ {cat_name}: {detail_str}")
    if hit_cats:
        with st.expander("📊 命中的投资机会类别"):
            for hc in hit_cats:
                st.markdown(hc)

    # --- 5. 全文逐条整理 ---
    if items:
        with st.expander(f"📝 全文逐条整理 ({len(items)}条)"):
            for it in items:
                if not isinstance(it, dict):
                    continue
                idx = it.get("id", "")
                st.markdown(f"**【条目{idx}】**")
                st.markdown(f"- **事实:** {it.get('fact', '')}")
                if it.get("opinion"):
                    st.markdown(f"- **观点:** {it.get('opinion', '')}")
                    if it.get("opinion_source"):
                        st.markdown(f"- **发布方:** {it['opinion_source']}")
                    if it.get("assumption"):
                        st.markdown(f"- **假设前提:** {it['assumption']}")
                    if it.get("evidence"):
                        st.markdown(f"- **数据支撑:** {it['evidence']}")
                    if it.get("logic_chain"):
                        st.markdown(f"- **逻辑链条:** {it['logic_chain']}")
                st.markdown("---")

    # --- 6. 类型特定字段 ---
    if ts and isinstance(ts, dict) and any(v for v in ts.values() if v):
        with st.expander("🔍 类型特定信息"):
            level = ts.get("level", "")
            if level:
                st.markdown(f"**层级:** {level}")

            # 研报类
            if ts.get("institution"):
                st.markdown(f"**机构:** {ts['institution']}  **分析师:** {ts.get('analyst', '')}")
            if ts.get("rating"):
                tp = ts.get("target_price")
                cp = ts.get("current_price")
                upside = ts.get("upside", "")
                rating_line = f"**评级:** {ts['rating']}"
                if tp:
                    rating_line += f"  **目标价:** {tp}"
                if cp:
                    rating_line += f"  (现价: {cp})"
                if upside:
                    rating_line += f"  空间: {upside}"
                st.markdown(rating_line)
            if ts.get("industry_rating"):
                st.markdown(f"**行业评级:** {ts['industry_rating']}  **趋势:** {ts.get('trend', '')}")
            if ts.get("core_logic"):
                st.markdown("**核心逻辑:**")
                for i, logic in enumerate(ts["core_logic"], 1):
                    st.markdown(f"  {i}. {logic}")
            if ts.get("core_view"):
                st.markdown(f"**核心观点:** {ts['core_view']}")

            # 盈利预测
            if ts.get("earnings_forecast"):
                st.markdown("**盈利预测:**")
                ef_data = []
                for ef in ts["earnings_forecast"]:
                    if isinstance(ef, dict):
                        ef_data.append(ef)
                if ef_data:
                    import pandas as pd
                    st.dataframe(pd.DataFrame(ef_data), use_container_width=True, hide_index=True)

            if ts.get("valuation"):
                st.markdown(f"**估值:** {ts['valuation']}")
            if ts.get("risks"):
                st.markdown("**风险:** " + "；".join(ts["risks"]))

            # 新闻类
            if ts.get("domain"):
                st.markdown(f"**领域:** {ts['domain']}  **范围:** {ts.get('scope', '')}")
            if ts.get("transmission_path"):
                st.markdown(f"**传导路径:** {ts['transmission_path']}")
            if ts.get("industry_chain"):
                st.markdown(f"**产业链:** {ts['industry_chain']}  **位置:** {ts.get('chain_position', '')}")
            if ts.get("chain_analysis") and isinstance(ts["chain_analysis"], dict):
                ca = ts["chain_analysis"]
                for pos in ["upstream", "midstream", "downstream"]:
                    if ca.get(pos):
                        pos_cn = {"upstream": "上游", "midstream": "中游", "downstream": "下游"}[pos]
                        st.markdown(f"  - **{pos_cn}:** {ca[pos]}")

            # 推荐个股
            if ts.get("recommended_stocks"):
                st.markdown("**推荐个股:**")
                for rs in ts["recommended_stocks"]:
                    if isinstance(rs, dict):
                        st.markdown(
                            f"  - {rs.get('code', '')} {rs.get('name', '')} | "
                            f"{rs.get('reason', '')} | 目标价:{rs.get('target_price', '—')} | {rs.get('rating', '')}"
                        )
            if ts.get("key_stocks"):
                st.markdown("**重点影响个股:**")
                for ks in ts["key_stocks"]:
                    if isinstance(ks, dict):
                        st.markdown(
                            f"  - {ks.get('code', '')} {ks.get('name', '')} | "
                            f"位置:{ks.get('position', '')} | 影响:{ks.get('impact', '')} | {ks.get('logic', '')}"
                        )

            # 受益行业
            if ts.get("affected_industries"):
                st.markdown("**受影响行业:**")
                for ai in ts["affected_industries"]:
                    if isinstance(ai, dict):
                        st.markdown(f"  - {ai.get('name', '')}: {ai.get('impact', '')}")
            if ts.get("benefited_industries"):
                st.markdown("**受益行业:**")
                for bi in ts["benefited_industries"]:
                    if isinstance(bi, dict):
                        st.markdown(f"  - {bi.get('name', '')}: {bi.get('reason', '')}")

            # 公告类
            if ts.get("announcement_type"):
                st.markdown(f"**公告类型:** {ts['announcement_type']}")
            if ts.get("key_data") and isinstance(ts["key_data"], dict):
                for k, v in ts["key_data"].items():
                    st.markdown(f"  - {k}: {v}")

    # --- 7. 关联公司和行业 ---
    comps = sj.get("companies", [])
    if comps:
        st.markdown("**关联公司:** " + " ".join(
            f"{'🟢' if co.get('impact') == 'positive' else '🔴' if co.get('impact') == 'negative' else '⚪'} "
            f"{co.get('stock_code', '')} {co.get('stock_name', '')}({co.get('relevance', '')})"
            for co in comps if isinstance(co, dict)
        ))

    inds = sj.get("industries", [])
    if inds:
        st.markdown("**关联行业:** " + ", ".join(
            f"{ind.get('industry_name', '')}({ind.get('impact', '')})"
            for ind in inds if isinstance(ind, dict)
        ))

    # 研报信息
    rr = sj.get("research_report")
    if rr and isinstance(rr, dict) and rr.get("broker_name"):
        st.markdown(
            f"**研报:** {rr['broker_name']} {rr.get('analyst_name', '')} | "
            f"评级: {rr.get('rating', '')} | 目标价: {rr.get('target_price') or '—'}"
        )


def _render_legacy_result(c, item):
    """渲染旧格式清洗结果（兼容）"""
    rc1, rc2, rc3, rc4 = st.columns(4)
    rc1.text(f"类型: {c['event_type']}")
    sentiment_map = {"positive": "🟢 利好", "negative": "🔴 利空", "neutral": "⚪ 中性"}
    rc2.text(sentiment_map.get(c["sentiment"], c["sentiment"]))
    rc3.text(f"重要性: {'⭐' * (c['importance'] or 0)}")
    rc4.text(f"置信度: {c['confidence']}")

    st.info(f"**摘要:** {c['summary']}")

    tags = json.loads(c.get("tags_json") or "[]")
    if tags:
        st.markdown("**标签:** " + " ".join(f"`{t}`" for t in tags))

    key_points = json.loads(c.get("key_points_json") or "[]")
    if key_points:
        st.markdown("**要点:**")
        for kp in key_points:
            st.markdown(f"- {kp}")

    if c.get("impact_analysis"):
        st.markdown(f"**影响分析:** {c['impact_analysis']}")

    companies = execute_query(
        "SELECT * FROM item_companies WHERE cleaned_item_id=?", [c["id"]]
    )
    if companies:
        st.markdown("**关联公司:**")
        for comp in companies:
            icon = {"positive": "🟢", "negative": "🔴"}.get(comp["impact"], "⚪")
            st.markdown(
                f"  {icon} {comp['stock_code']} "
                f"{comp['stock_name'] or ''} ({comp['relevance']})"
            )

    industries = execute_query(
        "SELECT * FROM item_industries WHERE cleaned_item_id=?", [c["id"]]
    )
    if industries:
        st.markdown("**关联行业:** " +
                    ", ".join(f"{i['industry_name']}({i['impact']})" for i in industries))

    reports = execute_query(
        "SELECT * FROM research_reports WHERE cleaned_item_id=?", [c["id"]]
    )
    if reports:
        for rr in reports:
            st.markdown(
                f"**研报:** {rr['broker_name']} {rr['analyst_name'] or ''} | "
                f"评级: {rr['rating']} | 目标价: {rr['target_price'] or '—'}"
            )


# ==================== Tab 3: 数据浏览 ====================
with tab_browse:
    st.subheader("数据浏览")
    # 筛选栏
    f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
    with f1:
        status_filter = st.selectbox("状态", ["全部", "pending", "cleaned", "failed", "processing"],
                                     key="br_status_filter")
    with f2:
        try:
            source_names = [s["name"] for s in execute_query("SELECT name FROM data_sources")]
        except Exception:
            source_names = []
        source_filter = st.selectbox("数据源", ["全部"] + source_names, key="br_source_filter")
    with f3:
        date_filter = st.date_input("日期", datetime.now(), key="br_date_filter")
    with f4:
        search = st.text_input("搜索标题/内容", key="br_search")

    try:
        sql = """SELECT r.id, r.title, r.content, r.processing_status, r.fetched_at,
                        r.item_type, r.url, d.name as source
                 FROM raw_items r JOIN data_sources d ON r.source_id=d.id"""
        params = []
        conditions = []

        if status_filter != "全部":
            conditions.append("r.processing_status=?")
            params.append(status_filter)
        if source_filter != "全部":
            conditions.append("d.name=?")
            params.append(source_filter)
        if date_filter:
            conditions.append("date(r.fetched_at)=?")
            params.append(date_filter.strftime("%Y-%m-%d"))
        if search:
            conditions.append("(r.title LIKE ? OR r.content LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY r.fetched_at DESC LIMIT 100"

        items = execute_query(sql, params)

        if items:
            st.caption(f"显示 {len(items)} 条（最多100条）")

            for item in items:
                status_icon = {
                    "pending": "⏳", "processing": "⚙️",
                    "cleaned": "✅", "failed": "❌",
                }.get(item["processing_status"], "❓")

                header = f"{status_icon} [{item['source']}] {item['title'] or '无标题'}"

                with st.expander(header):
                    ic1, ic2, ic3 = st.columns(3)
                    ic1.text(f"采集: {item['fetched_at']}")
                    ic2.text(f"类型: {item['item_type'] or '—'}")
                    ic3.text(f"状态: {item['processing_status']}")

                    if item.get("url"):
                        st.markdown(f"[原文链接]({item['url']})")

                    if item.get("content"):
                        st.text_area("原始内容", item["content"][:500],
                                     height=100, disabled=True, key=f"raw_{item['id']}")

                    # 清洗结果
                    cleaned = execute_query(
                        "SELECT * FROM cleaned_items WHERE raw_item_id=?", [item["id"]]
                    )
                    if cleaned:
                        c = cleaned[0]
                        st.markdown("**清洗结果:**")

                        # 尝试读取structured_json（新格式）
                        sj = None
                        try:
                            sj_raw = c.get("structured_json") or c.get("structured_json", None)
                        except Exception:
                            sj_raw = None
                        if sj_raw:
                            try:
                                sj = json.loads(sj_raw)
                            except (json.JSONDecodeError, TypeError):
                                pass

                        if sj:
                            _render_skill_result(sj, c, item)
                        else:
                            # 旧格式兼容显示
                            _render_legacy_result(c, item)
                    elif item["processing_status"] == "pending":
                        if st.button("清洗此条", key=f"clean_{item['id']}"):
                            try:
                                from cleaning.claude_processor import clean_single_item
                                with st.spinner("清洗中..."):
                                    result = clean_single_item(item["id"])
                                if result:
                                    add_message("success", f"清洗完成: {item['title'][:30]}")
                                    _get_cleaning_stats.clear()
                                    st.rerun()
                                else:
                                    add_message("error", "清洗失败")
                                    st.rerun()
                            except Exception as e:
                                add_message("error", f"错误: {e}")
                                st.rerun()
        else:
            st.caption("暂无数据，请先采集")
    except Exception as e:
        st.error(f"查询失败: {e}")

# ==================== Tab 4: 设置 ====================
with tab_settings:
    st.subheader("系统设置")

    # ---- 采集配置 ----
    st.markdown("**📥 一键采集配置**")
    st.caption("调整后点击「保存配置」，下次一键采集会按此配置执行")

    settings = load_fetch_settings()
    src_cfg = settings["sources"]

    # 全局参数
    new_hours = st.number_input(
        "新闻采集时间窗口（小时）", min_value=1, max_value=168, value=settings["news_hours"],
        key="st_news_hours", help="财联社/财新/热门股票/自选股/知识星球的时间过滤窗口")

    st.markdown("---")

    # 逐源配置
    group_labels = {"news": "📰 新闻类", "report": "📄 研报类", "community": "💬 社群类"}
    current_group = None
    today_counts = _get_today_source_counts()

    for key in ["cls", "caixin", "hot_stocks", "watchlist", "cctv", "em_report", "djyanbao", "fxbaogao", "zsxq"]:
        cfg = src_cfg[key]
        grp = cfg["group"]
        if grp != current_group:
            st.markdown(f"**{group_labels[grp]}**")
            current_group = grp

        col_toggle, col_label, col_param = st.columns([1, 3, 2])
        with col_toggle:
            cfg["enabled"] = st.checkbox("启用", value=cfg["enabled"], key=f"st_en_{key}", label_visibility="collapsed")
        with col_label:
            status_icon = "🟢" if cfg["enabled"] else "⚫"
            cnt = today_counts.get(key, 0)
            cnt_text = f"  ·  今日已采 **{cnt}** 条" if cnt > 0 else "  ·  今日未采集"
            st.markdown(f"{status_icon} **{cfg['label']}**{cnt_text}  \n{cfg['desc']}")
        with col_param:
            if "limit" in cfg:
                cfg["limit"] = st.number_input(
                    "采集上限", min_value=10, max_value=500, value=cfg.get("limit", 100),
                    key=f"st_lim_{key}", help="列表页最大采集条数")
            elif "max_pages" in cfg:
                cfg["max_pages"] = st.number_input(
                    "最大页数", min_value=1, max_value=20, value=cfg.get("max_pages", 5),
                    key=f"st_mp_{key}", help="每页约30条")
            else:
                st.caption("—")

    st.markdown("---")

    if st.button("💾 保存采集配置", type="primary", key="st_btn_save_fetch"):
        settings["news_hours"] = new_hours
        settings["sources"] = src_cfg
        save_fetch_settings(settings)
        add_message("success", "采集配置已保存")
        st.rerun()

    st.markdown("---")

    # ---- 知识星球Cookie ----
    st.markdown("**🌟 知识星球Cookie**")
    st.text_area("Cookie", value="", height=80, key="zsxq_cookie_val",
                 help="从浏览器开发者工具中复制Cookie，也可设置环境变量ZSXQ_COOKIE")
    st.caption("步骤：浏览器打开 wx.zsxq.com → F12开发者工具 → Network → "
               "点击api.zsxq.com请求 → Headers中复制Cookie值")

    st.markdown("---")

    # ---- 数据源配额 ----
    st.markdown("**📊 数据源配额**")
    try:
        sources = execute_query("SELECT * FROM data_sources")
        for src in sources:
            sc1, sc2, sc3, sc4 = st.columns([3, 2, 2, 1])
            with sc1:
                enabled = "🟢" if src["enabled"] else "🔴"
                st.text(f"{enabled} {src['name']} ({src['source_type']})")
            with sc2:
                if src["daily_limit"] and src["daily_limit"] > 0:
                    pct = min(src["today_used"] / src["daily_limit"], 1.0)
                    st.progress(pct, text=f"日: {src['today_used']}/{src['daily_limit']}")
                else:
                    st.text("日: 无限制")
            with sc3:
                if src["monthly_limit"] and src["monthly_limit"] > 0:
                    pct = min(src["month_used"] / src["monthly_limit"], 1.0)
                    st.progress(pct, text=f"月: {src['month_used']}/{src['monthly_limit']}")
                else:
                    st.text("月: 无限制")
            with sc4:
                st.text(f"今日: {src['today_used']}")
    except Exception:
        st.caption("数据库未初始化")

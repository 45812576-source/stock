"""个股研究 — watchlist/持仓/搜索 → 个股详情(K线+深度研究)"""
import streamlit as st
import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.db_utils import execute_query, execute_insert
from tracking.watchlist_manager import (
    add_to_watchlist, remove_from_watchlist, update_watch_type,
    get_watchlist, get_stock_today_news,
    add_tag_watch, get_watched_tags, get_tag_today_news,
    open_position, close_position, get_open_positions,
    get_closed_positions, get_position_summary,
)
from tracking.holding_analyzer import (
    check_holding_updates, get_today_push_notifications,
    get_stock_research_history,
)

st.title("🔬 个股研究")

today = datetime.now().strftime("%Y-%m-%d")

# session_state 路由
if "sr_view" not in st.session_state:
    st.session_state["sr_view"] = "list"
if "sr_stock_code" not in st.session_state:
    st.session_state["sr_stock_code"] = ""
if "sr_stock_name" not in st.session_state:
    st.session_state["sr_stock_name"] = ""


def go_to_detail(code, name=""):
    st.session_state["sr_view"] = "detail"
    st.session_state["sr_stock_code"] = code
    st.session_state["sr_stock_name"] = name


def go_to_list():
    st.session_state["sr_view"] = "list"


# ==================== 详情视图 ====================
if st.session_state["sr_view"] == "detail":
    stock_code = st.session_state["sr_stock_code"]
    stock_name = st.session_state["sr_stock_name"]

    display_name = f"{stock_code} {stock_name}" if stock_name else stock_code
    st.button("← 返回列表", on_click=go_to_list)
    st.subheader(f"📈 {display_name}")

    # 上半部分：K线图
    from components.stock_chart_view import render_stock_chart
    render_stock_chart(stock_code, key_prefix="sr_chart")

    st.markdown("---")

    # 下半部分：深度研究
    from components.stock_research_view import (
        render_stock_research_trigger,
        render_stock_research_result,
        render_stock_research_history,
    )
    render_stock_research_trigger(stock_code, key_prefix="sr")
    render_stock_research_history(stock_code, key_prefix="sr")

    st.stop()

# ==================== 列表视图 ====================

# 搜索框：直接输入股票代码进入研究
search_col1, search_col2 = st.columns([3, 1])
with search_col1:
    search_code = st.text_input("🔍 输入股票代码直接研究", placeholder="例如: 600519")
with search_col2:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("进入研究", use_container_width=True) and search_code:
        go_to_detail(search_code.strip())
        st.rerun()

# ==================== 推送通知面板 ====================
try:
    notifications = get_today_push_notifications(today)
    if notifications:
        st.warning(f"📢 今日有 {len(notifications)} 条持仓研究推送")
        with st.expander("查看推送通知", expanded=False):
            for n in notifications:
                name = n.get("stock_name") or n["stock_code"]
                highlights = json.loads(n["change_highlights_json"]) if n.get("change_highlights_json") else {}
                st.markdown(f"### {n['stock_code']} {name}")
                st.caption(f"触发类型: {n.get('trigger_type','')} | 时间: {n.get('created_at','')}")
                if highlights.get("report"):
                    st.markdown(highlights["report"])
                if highlights.get("triggers"):
                    st.markdown("**触发信息:**")
                    for t in highlights["triggers"]:
                        icon = {"positive": "🟢", "negative": "🔴"}.get(t.get("sentiment"), "⚪")
                        st.markdown(f"- {icon} [{t.get('importance','')}⭐] {t.get('summary','')}")
                st.markdown("---")
except Exception:
    pass

# ==================== 操作栏 ====================
col_op1, col_op2, col_op3 = st.columns(3)
with col_op1:
    if st.button("🔄 检查持仓更新", use_container_width=True):
        with st.spinner("正在检查持仓个股更新..."):
            try:
                triggered = check_holding_updates(today)
                if triggered:
                    st.success(f"触发 {len(triggered)} 条变化分析")
                    for t in triggered:
                        st.info(f"{t['stock_code']} {t['stock_name']}: {t['trigger_count']}条新信息")
                else:
                    st.info("持仓个股暂无新的重要更新")
            except Exception as e:
                st.error(f"检查失败: {e}")

# ==================== 四栏视图切换 ====================
view = st.radio("视图", ["📌 感兴趣", "💰 已持仓", "📋 全部", "🏷️ 标签跟踪"], horizontal=True)

# ==================== 添加跟踪 ====================
with st.expander("➕ 添加跟踪"):
    add_tab1, add_tab2 = st.tabs(["添加个股", "添加标签"])
    with add_tab1:
        ac1, ac2, ac3 = st.columns(3)
        with ac1:
            new_code = st.text_input("股票代码", key="add_stock_code")
        with ac2:
            new_name = st.text_input("股票名称", key="add_stock_name")
        with ac3:
            new_type = st.selectbox("标记类型", ["interested", "holding"], key="add_watch_type",
                                    format_func=lambda x: "感兴趣" if x == "interested" else "已持仓")
        new_tags = st.text_input("关联标签（逗号分隔）", key="add_tags")
        new_notes = st.text_input("备注", key="add_notes")
        if st.button("添加个股", key="btn_add_stock"):
            if new_code:
                tags = [t.strip() for t in new_tags.split(",") if t.strip()] if new_tags else None
                add_to_watchlist(new_code, new_name or None, new_type, tags, new_notes or None)
                st.success(f"已添加: {new_code} {new_name}")
                st.rerun()
    with add_tab2:
        tc1, tc2, tc3 = st.columns(3)
        with tc1:
            tag_name = st.text_input("标签名称", key="add_tag_name")
        with tc2:
            tag_type = st.selectbox("标签类型", ["theme", "industry", "macro_indicator"], key="add_tag_type",
                                    format_func=lambda x: {"theme": "主题", "industry": "行业", "macro_indicator": "宏观指标"}.get(x, x))
        with tc3:
            tag_watch = st.selectbox("关注类型", ["interested", "holding"], key="add_tag_watch",
                                     format_func=lambda x: "感兴趣" if x == "interested" else "重点关注")
        tag_stocks = st.text_input("关联股票代码（逗号分隔）", key="add_tag_stocks")
        if st.button("添加标签", key="btn_add_tag"):
            if tag_name:
                stocks = [s.strip() for s in tag_stocks.split(",") if s.strip()] if tag_stocks else None
                add_tag_watch(tag_name, tag_type, tag_watch, stocks)
                st.success(f"已添加标签: {tag_name}")
                st.rerun()

st.markdown("---")

# ==================== 已持仓视图：持仓概览 ====================
if view in ["💰 已持仓", "📋 全部"]:
    try:
        summary = get_position_summary()
        if summary["total_positions"] > 0:
            st.subheader("💰 持仓概览")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("持仓数", summary["total_positions"])
            m2.metric("总成本", f"¥{summary['total_cost']:,.0f}")
            m3.metric("总市值", f"¥{summary.get('total_market_value', 0):,.0f}")
            pnl = summary["total_pnl"]
            pnl_color = "normal" if pnl >= 0 else "inverse"
            m4.metric("总盈亏", f"¥{pnl:,.2f}", delta=f"{pnl:+,.2f}", delta_color=pnl_color)

            df_pos = pd.DataFrame(summary["stocks"])
            if not df_pos.empty:
                df_display = df_pos[["stock_code", "stock_name", "buy_date", "buy_price",
                                     "quantity", "current_price", "pnl", "pnl_pct", "holding_days"]].copy()
                df_display.columns = ["代码", "名称", "买入日期", "买入价", "数量",
                                      "现价", "盈亏", "盈亏%", "持仓天数"]
                st.dataframe(df_display, use_container_width=True, hide_index=True)

            # 建仓/平仓操作
            with st.expander("📝 持仓操作"):
                op_tab1, op_tab2 = st.tabs(["建仓", "平仓"])
                with op_tab1:
                    pc1, pc2, pc3, pc4 = st.columns(4)
                    with pc1:
                        pos_code = st.text_input("股票代码", key="pos_code")
                    with pc2:
                        pos_name = st.text_input("股票名称", key="pos_name")
                    with pc3:
                        pos_price = st.number_input("买入价", min_value=0.01, value=10.0, key="pos_price")
                    with pc4:
                        pos_qty = st.number_input("数量(股)", min_value=100, value=1000, step=100, key="pos_qty")
                    pos_date = st.date_input("买入日期", value=datetime.now(), key="pos_date")
                    pos_notes = st.text_input("备注", key="pos_notes")
                    if st.button("确认建仓", key="btn_open_pos"):
                        if pos_code:
                            open_position(pos_code, pos_name or pos_code,
                                          pos_date.strftime("%Y-%m-%d"), pos_price, pos_qty,
                                          pos_notes or None)
                            st.success(f"建仓成功: {pos_code} {pos_qty}股 @ {pos_price}")
                            st.rerun()
                with op_tab2:
                    open_pos = get_open_positions()
                    if open_pos:
                        pos_options = {f"{p['id']}: {p['stock_code']} {p.get('stock_name','')} "
                                       f"({p['quantity']}股@{p['buy_price']})": p["id"] for p in open_pos}
                        sel_pos = st.selectbox("选择持仓", list(pos_options.keys()), key="sel_close_pos")
                        sc1, sc2 = st.columns(2)
                        with sc1:
                            sell_price = st.number_input("卖出价", min_value=0.01, value=10.0, key="sell_price")
                        with sc2:
                            sell_date = st.date_input("卖出日期", value=datetime.now(), key="sell_date")
                        if st.button("确认平仓", key="btn_close_pos"):
                            pid = pos_options[sel_pos]
                            pnl_val = close_position(pid, sell_date.strftime("%Y-%m-%d"), sell_price)
                            if pnl_val is not None:
                                st.success(f"平仓成功，盈亏: ¥{pnl_val:,.2f}")
                                st.rerun()
                    else:
                        st.caption("暂无持仓可平仓")

            # 已平仓记录
            with st.expander("📊 已平仓记录"):
                closed = get_closed_positions(20)
                if closed:
                    df_closed = pd.DataFrame(closed)
                    cols = ["stock_code", "stock_name", "buy_date", "buy_price",
                            "sell_date", "sell_price", "quantity", "pnl"]
                    df_c = df_closed[[c for c in cols if c in df_closed.columns]].copy()
                    df_c.columns = ["代码", "名称", "买入日期", "买入价",
                                    "卖出日期", "卖出价", "数量", "盈亏"][:len(df_c.columns)]
                    st.dataframe(df_c, use_container_width=True, hide_index=True)
                else:
                    st.caption("暂无已平仓记录")
    except Exception as e:
        st.caption(f"持仓数据加载失败: {e}")

# ==================== 个股跟踪列表 ====================
if view != "🏷️ 标签跟踪":
    st.subheader("📋 跟踪列表")
    watch_filter = None
    if view == "📌 感兴趣":
        watch_filter = "interested"
    elif view == "💰 已持仓":
        watch_filter = "holding"

    try:
        stocks = get_watchlist(watch_filter)
        if stocks:
            for s in stocks:
                type_emoji = "📌" if s["watch_type"] == "interested" else "💰"
                label = f"{type_emoji} {s['stock_code']} {s['stock_name'] or ''}"
                tags_str = ""
                if s.get("related_tags"):
                    try:
                        tags = json.loads(s["related_tags"])
                        tags_str = " | ".join(f"#{t}" for t in tags) if tags else ""
                    except (json.JSONDecodeError, TypeError):
                        pass
                if tags_str:
                    label += f"  ({tags_str})"

                with st.expander(label):
                    # 基本信息行
                    ic1, ic2, ic3, ic4 = st.columns(4)
                    ic1.caption(f"标记: {'感兴趣' if s['watch_type'] == 'interested' else '已持仓'}")
                    ic2.caption(f"添加: {s.get('added_at', '')[:10]}")
                    ic3.caption(f"更新: {s.get('updated_at', '')[:10] if s.get('updated_at') else ''}")
                    if s.get("notes"):
                        ic4.caption(f"备注: {s['notes']}")

                    # 操作按钮
                    bc1, bc2, bc3, bc4 = st.columns(4)
                    with bc1:
                        if s["watch_type"] == "interested":
                            if st.button("升级为持仓", key=f"upgrade_{s['stock_code']}"):
                                update_watch_type(s["stock_code"], "holding")
                                st.rerun()
                        else:
                            if st.button("降为感兴趣", key=f"downgrade_{s['stock_code']}"):
                                update_watch_type(s["stock_code"], "interested")
                                st.rerun()
                    with bc2:
                        if st.button("🗑️ 移除", key=f"remove_{s['stock_code']}"):
                            remove_from_watchlist(s["stock_code"])
                            st.success(f"已移除 {s['stock_code']}")
                            st.rerun()
                    with bc3:
                        if st.button("🔬 研究", key=f"research_{s['stock_code']}"):
                            go_to_detail(s["stock_code"], s.get("stock_name") or "")
                            st.rerun()

                    # 今日关联信息
                    news = get_stock_today_news(s["stock_code"], today)
                    if news:
                        st.markdown(f"**📰 今日关联信息 ({len(news)}条)**")
                        for r in news:
                            icon = {"positive": "🟢", "negative": "🔴"}.get(r.get("sentiment"), "⚪")
                            imp = r.get("importance", 0)
                            must_read = " 🔥必读" if imp >= 4 else ""
                            st.markdown(f"{icon} [{imp}⭐]{must_read} {r.get('summary', '')}")
                    else:
                        st.caption("今日暂无关联信息")

                    # 持仓个股：显示研究历史
                    if s["watch_type"] == "holding":
                        history = get_stock_research_history(s["stock_code"], 5)
                        if history:
                            st.markdown("**🔬 近期变化分析**")
                            for h in history:
                                hl = json.loads(h["change_highlights_json"]) if h.get("change_highlights_json") else {}
                                st.caption(f"📅 {h.get('trigger_date', '')} | 触发: {h.get('trigger_type', '')}")
                                if hl.get("report"):
                                    st.markdown(hl["report"][:300] + ("..." if len(hl.get("report", "")) > 300 else ""))
        else:
            st.caption("暂无跟踪个股，请在上方添加")
    except Exception as e:
        st.caption(f"数据加载失败: {e}")

# ==================== 标签跟踪视图 ====================
if view == "🏷️ 标签跟踪":
    st.subheader("🏷️ 标签跟踪")
    try:
        tags = get_watched_tags()
        if tags:
            for tag in tags:
                type_map = {"theme": "🎯主题", "industry": "🏭行业", "macro": "🌐宏观"}
                tag_label = f"{type_map.get(tag.get('tag_type'), '🏷️')} {tag['tag_name']}"
                watch_label = "重点关注" if tag.get("watch_type") == "holding" else "感兴趣"
                with st.expander(f"{tag_label} [{watch_label}]"):
                    st.caption(f"添加时间: {tag.get('added_at', '')[:10]}")
                    if tag.get("related_stock_codes_json"):
                        try:
                            related = json.loads(tag["related_stock_codes_json"])
                            if related:
                                st.markdown(f"**关联股票:** {', '.join(related)}")
                        except (json.JSONDecodeError, TypeError):
                            pass
                    tag_news = get_tag_today_news(tag["tag_name"], today)
                    if tag_news:
                        st.markdown(f"**📰 今日关联信息 ({len(tag_news)}条)**")
                        for tn in tag_news[:10]:
                            icon = {"positive": "🟢", "negative": "🔴"}.get(tn.get("sentiment"), "⚪")
                            st.markdown(f"{icon} [{tn.get('importance', 0)}⭐] {tn.get('summary', '')}")
                    else:
                        st.caption("今日暂无关联信息")
        else:
            st.caption("暂无跟踪标签，请在上方添加")
    except Exception as e:
        st.caption(f"标签数据加载失败: {e}")

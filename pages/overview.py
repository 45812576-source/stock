"""每日概览 — 今日指标 + 8类Highlight榜单"""
import streamlit as st
import sys
import json
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils.db_utils import execute_query, execute_insert

st.title("📊 每日概览")

today = datetime.now().strftime("%Y-%m-%d")

# ==================== 数据采集状态 ====================
st.subheader("数据采集状态")
col1, col2, col3, col4 = st.columns(4)

try:
    raw_today = execute_query(
        "SELECT COUNT(*) as cnt FROM raw_items WHERE date(fetched_at)=?", [today]
    )[0]["cnt"]
    cleaned_today = execute_query(
        "SELECT COUNT(*) as cnt FROM cleaned_items WHERE date(cleaned_at)=?", [today]
    )[0]["cnt"]
    failed_today = execute_query(
        "SELECT COUNT(*) as cnt FROM raw_items WHERE processing_status='failed' AND date(fetched_at)=?",
        [today],
    )[0]["cnt"]
    opportunities = execute_query(
        "SELECT COUNT(*) as cnt FROM investment_opportunities WHERE status='active'"
    )[0]["cnt"]
except Exception:
    raw_today = cleaned_today = failed_today = opportunities = 0

with col1:
    st.metric("已采集", raw_today)
with col2:
    st.metric("已清洗", cleaned_today)
with col3:
    st.metric("失败", failed_today)
with col4:
    st.metric("活跃机会", opportunities)

# 跟踪个股提醒
st.subheader("跟踪个股提醒")
try:
    interested = execute_query(
        "SELECT COUNT(*) as cnt FROM watchlist WHERE watch_type='interested'"
    )[0]["cnt"]
    holding = execute_query(
        "SELECT COUNT(*) as cnt FROM watchlist WHERE watch_type='holding'"
    )[0]["cnt"]
except Exception:
    interested = holding = 0

col1, col2 = st.columns(2)
with col1:
    st.info(f"感兴趣个股: {interested} 只")
with col2:
    st.warning(f"已持仓个股: {holding} 只")

st.markdown("---")

# ==================== Highlight榜单 ====================
# 日期选择 + 生成按钮
top_col1, top_col2, top_col3 = st.columns([2, 1, 1])
with top_col1:
    selected_date = st.date_input("选择日期", datetime.now())
    date_str = selected_date.strftime("%Y-%m-%d")
with top_col2:
    if st.button("🔄 生成今日榜单", use_container_width=True):
        try:
            from dashboards.pipeline import generate_all_dashboards
            with st.spinner("正在生成8类榜单..."):
                results = generate_all_dashboards(date_str)
            st.success(f"生成完成: {json.dumps(results, ensure_ascii=False, default=str)[:200]}")
            st.rerun()
        except Exception as e:
            st.error(f"生成失败: {e}")
with top_col3:
    try:
        tag_count = execute_query(
            "SELECT COUNT(DISTINCT tag_name) as cnt FROM dashboard_tag_frequency WHERE appear_date=?",
            [date_str],
        )[0]["cnt"]
        st.metric("今日标签数", tag_count)
    except Exception:
        st.metric("今日标签数", 0)

st.markdown("---")

tabs = st.tabs([
    "1.宏观利好利空", "2.行业重大利好", "3.行业资金流入",
    "4.财报超预期", "5.券商覆盖3月", "6.券商覆盖1月",
    "7.个股资金Top10", "8.宏观指标监控"
])

# ===== Tab 1: 宏观利好利空 =====
with tabs[0]:
    try:
        positives = execute_query(
            """SELECT ci.summary, ci.importance, ci.tags_json, ci.impact_analysis
               FROM cleaned_items ci
               WHERE ci.event_type='macro_policy' AND ci.sentiment='positive'
               AND date(ci.cleaned_at)=? ORDER BY ci.importance DESC LIMIT 10""",
            [date_str],
        )
        negatives = execute_query(
            """SELECT ci.summary, ci.importance, ci.tags_json, ci.impact_analysis
               FROM cleaned_items ci
               WHERE ci.event_type='macro_policy' AND ci.sentiment='negative'
               AND date(ci.cleaned_at)=? ORDER BY ci.importance DESC LIMIT 10""",
            [date_str],
        )
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### 🟢 利好")
            for p in positives:
                tags = json.loads(p.get("tags_json") or "[]")
                tag_str = " ".join(f"`{t}`" for t in tags[:3])
                st.success(f"**[{'⭐' * p['importance']}]** {p['summary']}\n\n{tag_str}")
            if not positives:
                st.caption("暂无宏观利好")
        with col2:
            st.markdown("### 🔴 利空")
            for n in negatives:
                tags = json.loads(n.get("tags_json") or "[]")
                tag_str = " ".join(f"`{t}`" for t in tags[:3])
                st.error(f"**[{'⭐' * n['importance']}]** {n['summary']}\n\n{tag_str}")
            if not negatives:
                st.caption("暂无宏观利空")

        if positives or negatives:
            fig = go.Figure(data=[
                go.Bar(name="利好", x=["宏观政策"], y=[len(positives)], marker_color="red"),
                go.Bar(name="利空", x=["宏观政策"], y=[len(negatives)], marker_color="green"),
            ])
            fig.update_layout(height=200, barmode="group", template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)
    except Exception:
        st.caption("暂无数据")

# ===== Tab 2: 行业重大利好Top10 =====
with tabs[1]:
    try:
        rows = execute_query(
            """SELECT ii.industry_name, ci.summary, ci.importance, ci.tags_json
               FROM item_industries ii JOIN cleaned_items ci ON ii.cleaned_item_id=ci.id
               WHERE ci.sentiment='positive' AND date(ci.cleaned_at)=?
               ORDER BY ci.importance DESC LIMIT 10""",
            [date_str],
        )
        if rows:
            for i, r in enumerate(rows, 1):
                tags = json.loads(r.get("tags_json") or "[]")
                tag_str = " ".join(f"`{t}`" for t in tags[:3])
                st.markdown(f"**{i}. {r['industry_name']}** {'⭐' * r['importance']}")
                st.markdown(f"  {r['summary']} {tag_str}")

            df = pd.DataFrame(rows)
            fig = px.bar(df, x="industry_name", y="importance",
                         title="行业利好重要性分布", color="importance",
                         color_continuous_scale="Reds")
            fig.update_layout(height=300, template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("暂无数据")
    except Exception:
        st.caption("暂无数据")

# ===== Tab 3: 行业资金净流入Top10 =====
with tabs[2]:
    try:
        rows = execute_query(
            """SELECT industry_name, net_inflow, change_pct, leading_stock
               FROM industry_capital_flow WHERE trade_date=?
               ORDER BY net_inflow DESC LIMIT 10""",
            [date_str],
        )
        if rows:
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True)

            fig = px.bar(df, x="industry_name", y="net_inflow",
                         title="行业资金净流入Top10",
                         color="net_inflow", color_continuous_scale="RdYlGn")
            fig.update_layout(height=400, template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("暂无数据")
    except Exception:
        st.caption("暂无数据")

# ===== Tab 4: 财报超预期Top10 =====
with tabs[3]:
    try:
        rows = execute_query(
            """SELECT fr.stock_code, si.stock_name, fr.report_period,
                      fr.revenue_yoy, fr.profit_yoy, fr.actual_vs_consensus, fr.eps
               FROM financial_reports fr
               LEFT JOIN stock_info si ON fr.stock_code=si.stock_code
               WHERE fr.beat_expectation=1
               ORDER BY fr.actual_vs_consensus DESC LIMIT 10"""
        )
        if rows:
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True)

            if "actual_vs_consensus" in df.columns and df["actual_vs_consensus"].notna().any():
                fig = px.bar(df, x="stock_name", y="actual_vs_consensus",
                             title="财报超预期幅度", color="actual_vs_consensus",
                             color_continuous_scale="Greens")
                fig.update_layout(height=350, template="plotly_dark")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("暂无数据")
    except Exception:
        st.caption("暂无数据")

# ===== Tab 5: 3月券商覆盖Top10 =====
with tabs[4]:
    try:
        rows = execute_query(
            """SELECT stock_code, stock_name,
                      COUNT(*) as coverage_count,
                      COUNT(DISTINCT broker_name) as broker_count,
                      GROUP_CONCAT(DISTINCT broker_name) as brokers,
                      ROUND(AVG(target_price), 2) as avg_target
               FROM research_reports
               WHERE report_date >= date(?, '-3 months')
               GROUP BY stock_code ORDER BY coverage_count DESC LIMIT 10""",
            [date_str],
        )
        if rows:
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True)

            fig = px.bar(df, x="stock_name", y="coverage_count",
                         title="3月券商覆盖次数", color="broker_count",
                         hover_data=["brokers"])
            fig.update_layout(height=350, template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("暂无数据")
    except Exception:
        st.caption("暂无数据")

# ===== Tab 6: 1月券商覆盖Top10 =====
with tabs[5]:
    try:
        rows = execute_query(
            """SELECT stock_code, stock_name,
                      COUNT(*) as coverage_count,
                      COUNT(DISTINCT broker_name) as broker_count,
                      GROUP_CONCAT(DISTINCT broker_name) as brokers,
                      ROUND(AVG(target_price), 2) as avg_target
               FROM research_reports
               WHERE report_date >= date(?, '-1 months')
               GROUP BY stock_code ORDER BY coverage_count DESC LIMIT 10""",
            [date_str],
        )
        if rows:
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True)

            fig = px.bar(df, x="stock_name", y="coverage_count",
                         title="1月券商覆盖次数", color="broker_count")
            fig.update_layout(height=350, template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("暂无数据")
    except Exception:
        st.caption("暂无数据")

# ===== Tab 7: 个股资金Top10 =====
with tabs[6]:
    try:
        rows = execute_query(
            """SELECT cf.stock_code, si.stock_name, cf.main_net_inflow,
                      cf.super_large_net, cf.large_net, si.market_cap,
                      sd.change_pct, sd.turnover_rate
               FROM capital_flow cf
               LEFT JOIN stock_info si ON cf.stock_code=si.stock_code
               LEFT JOIN stock_daily sd ON cf.stock_code=sd.stock_code AND cf.trade_date=sd.trade_date
               WHERE cf.trade_date=?
               ORDER BY cf.main_net_inflow DESC LIMIT 10""",
            [date_str],
        )
        if rows:
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True)

            fig = px.bar(df, x="stock_name", y="main_net_inflow",
                         title="个股主力资金净流入Top10",
                         color="main_net_inflow", color_continuous_scale="RdYlGn")
            fig.update_layout(height=400, template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("暂无数据")
    except Exception:
        st.caption("暂无数据")

# ===== Tab 8: 宏观资金面指标 =====
with tabs[7]:
    try:
        indicators = execute_query(
            """SELECT indicator_name, value, unit, indicator_date
               FROM macro_indicators mi
               WHERE indicator_date = (
                   SELECT MAX(indicator_date) FROM macro_indicators
                   WHERE indicator_name=mi.indicator_name
               )
               ORDER BY indicator_name"""
        )
        if indicators:
            st.markdown("### 最新宏观指标")
            df = pd.DataFrame(indicators)
            st.dataframe(df, use_container_width=True)

        northbound = execute_query(
            """SELECT trade_date, total_net, sh_net, sz_net
               FROM northbound_flow ORDER BY trade_date DESC LIMIT 30"""
        )
        if northbound:
            st.markdown("### 北向资金近30日趋势")
            nb_df = pd.DataFrame(northbound)
            nb_df = nb_df.sort_values("trade_date")
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=nb_df["trade_date"], y=nb_df["total_net"],
                                     name="北向净流入", fill="tozeroy"))
            if nb_df["sh_net"].notna().any():
                fig.add_trace(go.Scatter(x=nb_df["trade_date"], y=nb_df["sh_net"],
                                         name="沪股通", line=dict(dash="dash")))
            if nb_df["sz_net"].notna().any():
                fig.add_trace(go.Scatter(x=nb_df["trade_date"], y=nb_df["sz_net"],
                                         name="深股通", line=dict(dash="dash")))
            fig.update_layout(height=400, template="plotly_dark", title="北向资金净流入")
            st.plotly_chart(fig, use_container_width=True)

        if not indicators and not northbound:
            st.caption("暂无数据")
    except Exception:
        st.caption("暂无数据")

# ===== 底部：标签频次热力图 =====
st.markdown("---")
st.subheader("标签频次分布")
try:
    tag_data = execute_query(
        """SELECT tag_name, dashboard_type, COUNT(*) as freq
           FROM dashboard_tag_frequency
           WHERE appear_date=?
           GROUP BY tag_name, dashboard_type
           ORDER BY freq DESC""",
        [date_str],
    )
    if tag_data:
        df = pd.DataFrame(tag_data)
        pivot = df.pivot_table(index="tag_name", columns="dashboard_type",
                               values="freq", fill_value=0)
        dashboard_names = {
            1: "宏观", 2: "行业新闻", 3: "行业资金", 4: "财报",
            5: "券商3月", 6: "券商1月", 7: "个股资金", 8: "宏观指标",
        }
        pivot.columns = [dashboard_names.get(c, str(c)) for c in pivot.columns]

        fig = px.imshow(pivot, text_auto=True, aspect="auto",
                        title="标签在各榜单出现频次",
                        color_continuous_scale="YlOrRd")
        fig.update_layout(height=max(300, len(pivot) * 25), template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("暂无标签数据，请先生成榜单")
except Exception:
    st.caption("暂无标签数据")

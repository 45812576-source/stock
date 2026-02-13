"""热点研究 — L1:标签频次+标签组推荐 → L2:单标签/标签组深度研究"""
import streamlit as st
import sys
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.db_utils import execute_query
from hotspot.tag_recommender import (
    get_top_tags, get_tag_dashboard_distribution,
    recommend_tag_groups, save_tag_group, get_saved_groups, delete_tag_group,
    get_group_related_news,
)
from hotspot.tag_group_analyzer import analyze_tag_group
from hotspot.tag_group_research import research_tag_group, get_group_research_history

st.title("🔥 热点研究")

# session_state 路由
if "hs_level" not in st.session_state:
    st.session_state["hs_level"] = "overview"
if "hs_research_type" not in st.session_state:
    st.session_state["hs_research_type"] = ""
if "hs_research_target" not in st.session_state:
    st.session_state["hs_research_target"] = None


def go_to_tag_research(tag_name):
    st.session_state["hs_level"] = "research"
    st.session_state["hs_research_type"] = "single_tag"
    st.session_state["hs_research_target"] = tag_name


def go_to_group_research(group_id):
    st.session_state["hs_level"] = "research"
    st.session_state["hs_research_type"] = "tag_group"
    st.session_state["hs_research_target"] = group_id


def go_to_overview():
    st.session_state["hs_level"] = "overview"


def _render_research(research, group_id):
    """渲染6维深度研究结果"""
    t1, t2, t3, t4, t5, t6 = st.tabs([
        "📝 成组逻辑", "📰 关联新闻", "📈 板块热度",
        "🌐 宏观分析", "🏭 行业分析", "🏆 推荐个股"
    ])

    with t1:
        if research.get("group_logic"):
            st.markdown(research["group_logic"])
        elif research.get("macro_report"):
            st.info("成组逻辑请通过「快速分析」生成")

    with t2:
        news = research.get("news", [])
        if news:
            st.caption(f"共 {len(news)} 条关联新闻（按关联度×重要性排序）")
            for n in news:
                icon = {"positive": "🟢", "negative": "🔴"}.get(n.get("sentiment"), "⚪")
                match = n.get("match_tags", n.get("match_count", 1))
                st.markdown(
                    f"{icon} [{n.get('importance', 0)}⭐ | 关联{match}标签] "
                    f"{n.get('summary', '')}")
                st.caption(f"  类型: {n.get('event_type', '')} | "
                           f"时间: {str(n.get('cleaned_at', ''))[:10]}")
        else:
            st.caption("暂无关联新闻")

    with t3:
        heat = research.get("sector_heat", [])
        if heat:
            for sh in heat:
                total = sh.get("total_inflow", 0)
                color = "🟢" if total > 0 else "🔴"
                val = total / 1e8 if abs(total) > 1e6 else total / 1e4
                unit = "亿" if abs(total) > 1e6 else "万"
                st.markdown(f"{color} **{sh['tag']}**: 累计净流入 {val:.2f}{unit}")

            chart_data = []
            for sh in heat:
                for f in sh.get("daily_flows", sh.get("flows", [])):
                    date_key = f.get("date", f.get("trade_date", ""))
                    inflow = f.get("inflow", f.get("net_inflow", 0)) or 0
                    chart_data.append({
                        "日期": date_key,
                        "净流入": inflow / 1e4,
                        "板块": sh["tag"],
                    })
            if chart_data:
                df_heat = pd.DataFrame(chart_data)
                fig = px.line(df_heat, x="日期", y="净流入", color="板块",
                              title="板块资金净流入趋势（万元）")
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("暂无板块热度数据")

    with t4:
        if research.get("macro_report"):
            st.markdown(research["macro_report"])
        else:
            st.caption("请先执行深度研究生成宏观报告")

    with t5:
        if research.get("industry_report"):
            st.markdown(research["industry_report"])
        else:
            st.caption("请先执行深度研究生成行业报告")

    with t6:
        stocks = research.get("top10_stocks", [])
        if stocks:
            df_stocks = pd.DataFrame(stocks)
            if "stock_name" in df_stocks.columns and "total_inflow" in df_stocks.columns:
                fig = px.bar(df_stocks, x="stock_name", y="total_inflow",
                             title="推荐个股Top10（按资金净流入）",
                             labels={"stock_name": "股票", "total_inflow": "净流入"},
                             color="total_inflow",
                             color_continuous_scale=["#F44336", "#4CAF50"])
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)

            col_map = {"stock_code": "代码", "stock_name": "名称",
                       "total_inflow": "净流入", "inflow_ratio": "占比%",
                       "match_tags": "关联标签数"}
            df_show = df_stocks[[c for c in col_map if c in df_stocks.columns]].copy()
            df_show.columns = [col_map[c] for c in df_show.columns]
            st.dataframe(df_show, use_container_width=True, hide_index=True)
        else:
            st.caption("暂无推荐个股数据")


# ==================== Level 2: 研究视图 ====================
if st.session_state["hs_level"] == "research":
    st.button("← 返回热点概览", on_click=go_to_overview)

    if st.session_state["hs_research_type"] == "single_tag":
        # 单标签研究
        tag_name = st.session_state["hs_research_target"]
        st.subheader(f"🏷️ 标签研究: {tag_name}")

        # 标签在各榜单的分布
        try:
            dist = get_tag_dashboard_distribution(tag_name)
            if dist:
                st.markdown("**标签在各榜单的分布:**")
                dashboard_names = {
                    1: "宏观", 2: "行业新闻", 3: "行业资金", 4: "财报",
                    5: "券商3月", 6: "券商1月", 7: "个股资金", 8: "宏观指标",
                }
                for d in dist:
                    name = dashboard_names.get(d.get("dashboard_type"), str(d.get("dashboard_type")))
                    st.markdown(f"- **{name}**: {d.get('freq', 0)}次")
        except Exception:
            pass

        # 相关资讯
        st.markdown("---")
        st.markdown("**📰 相关资讯:**")
        try:
            news = execute_query(
                """SELECT ci.summary, ci.sentiment, ci.importance, ci.cleaned_at, ci.event_type
                   FROM cleaned_items ci
                   WHERE ci.tags_json LIKE ?
                   ORDER BY ci.importance DESC LIMIT 20""",
                [f"%{tag_name}%"],
            )
            if news:
                for n in news:
                    icon = {"positive": "🟢", "negative": "🔴"}.get(n.get("sentiment"), "⚪")
                    st.markdown(f"{icon} [{n.get('importance', 0)}⭐] {n.get('summary', '')}")
                    st.caption(f"  类型: {n.get('event_type', '')} | 时间: {str(n.get('cleaned_at', ''))[:10]}")
            else:
                st.caption("暂无相关资讯")
        except Exception:
            st.caption("暂无相关资讯")

        # 触发行业级深度研究
        st.markdown("---")
        if st.button("🔬 触发行业级深度研究", key="hs_industry_research"):
            with st.spinner("正在生成行业深度研究报告..."):
                try:
                    from research.deep_researcher import deep_research_industry
                    result = deep_research_industry(tag_name)
                    if result and not result.get("error"):
                        st.success("研究完成")
                        report = result.get("report", {})
                        sections = [
                            ("overview", "📋 行业概况"), ("competition", "🏆 竞争格局"),
                            ("value_chain", "🔗 产业链分析"), ("drivers", "🚀 驱动因素"),
                            ("opportunities", "💡 投资机会"), ("risks", "⚠️ 风险提示"),
                        ]
                        for key, title in sections:
                            content = report.get(key)
                            if content:
                                st.markdown(f"**{title}**")
                                st.markdown(content)
                    else:
                        st.error(f"研究失败: {result.get('error', '未知错误')}")
                except Exception as e:
                    st.error(f"研究异常: {e}")

    elif st.session_state["hs_research_type"] == "tag_group":
        # 标签组研究
        group_id = st.session_state["hs_research_target"]
        st.subheader(f"📦 标签组研究 (ID: {group_id})")

        # 加载标签组信息
        try:
            saved_groups = get_saved_groups()
            group = next((g for g in saved_groups if g["id"] == group_id), None)
            if group:
                tags = json.loads(group["tags_json"]) if group.get("tags_json") else []
                tags_display = " | ".join(f"#{t}" for t in tags)
                st.markdown(f"**标签组:** {group['group_name']}")
                st.markdown(f"**标签:** {tags_display}")
                if group.get("group_logic"):
                    st.markdown(f"**成组逻辑:** {group['group_logic']}")

                # 触发深度研究
                if st.button("🔬 执行深度研究", key=f"hs_deep_{group_id}"):
                    with st.spinner("正在生成深度研究报告（需要调用Claude API）..."):
                        try:
                            result = research_tag_group(group_id)
                            if result:
                                st.session_state[f"hs_research_{group_id}"] = result
                                st.success("研究完成")
                        except Exception as e:
                            st.error(f"研究失败: {e}")

                # 展示研究结果
                research = st.session_state.get(f"hs_research_{group_id}")
                if not research:
                    history = get_group_research_history(group_id, 1)
                    if history:
                        h = history[0]
                        research = {
                            "macro_report": h.get("macro_report"),
                            "industry_report": h.get("industry_report"),
                            "news": json.loads(h["news_summary_json"]) if h.get("news_summary_json") else [],
                            "sector_heat": json.loads(h["sector_heat_json"]) if h.get("sector_heat_json") else [],
                            "top10_stocks": json.loads(h["top10_stocks_json"]) if h.get("top10_stocks_json") else [],
                        }

                if research:
                    _render_research(research, group_id)
            else:
                st.warning("未找到该标签组")
        except Exception as e:
            st.caption(f"加载失败: {e}")

    st.stop()

# ==================== Level 1: 概览视图 ====================

# 时间段选择
days = st.selectbox("时间范围", [7, 14, 30], format_func=lambda x: f"近{x}天")

# 标签频次概览
st.subheader("📊 标签频次概览")
try:
    top_tags = get_top_tags(days, 30)
    if top_tags:
        df_tags = pd.DataFrame(top_tags)

        fig = px.bar(df_tags.head(20), x="tag_name", y="total_freq",
                     color="tag_type", title=f"近{days}天高频标签Top20",
                     labels={"tag_name": "标签", "total_freq": "出现次数",
                             "tag_type": "类型"},
                     color_discrete_map={"theme": "#FF9800", "industry": "#2196F3",
                                         "stock": "#4CAF50", "macro": "#E91E63"})
        fig.update_layout(xaxis_tickangle=-45, height=400)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("查看标签明细"):
            df_show = df_tags[["tag_name", "tag_type", "total_freq",
                               "dashboards", "first_appear", "last_appear"]].copy()
            df_show.columns = ["标签", "类型", "频次", "涉及榜单", "首次出现", "最后出现"]
            st.dataframe(df_show, use_container_width=True, hide_index=True)
    else:
        st.caption("暂无标签频次数据，请先运行榜单生成流水线")
except Exception as e:
    st.caption(f"数据加载失败: {e}")

st.markdown("---")

# 标签组推荐
st.subheader("🔗 标签组推荐")
if st.button("🔄 生成推荐标签组", key="btn_recommend"):
    with st.spinner("正在分析标签共现关系..."):
        try:
            groups = recommend_tag_groups(days, top_n=5)
            if groups:
                st.session_state["recommended_groups"] = groups
                st.success(f"生成 {len(groups)} 个推荐标签组")
            else:
                st.info("数据不足，无法生成推荐")
        except Exception as e:
            st.error(f"推荐失败: {e}")

# 展示推荐结果
rec_groups = st.session_state.get("recommended_groups", [])
if rec_groups:
    for i, g in enumerate(rec_groups):
        tags_display = " | ".join(f"#{t}" for t in g["tags"])
        dashboards_str = ", ".join(g.get("dashboards", []))
        with st.expander(f"📦 {g['group_name']} (总频次: {g['frequency']})"):
            st.markdown(f"**标签:** {tags_display}")
            if g.get("group_logic"):
                st.markdown(f"**投资逻辑:** {g['group_logic']}")
            st.markdown(f"**涉及榜单:** {dashboards_str}")
            st.caption(f"标签数: {g.get('tag_count', len(g['tags']))}")

            # 显示关联清洗新闻摘要
            related_news = get_group_related_news(g["tags"], days, limit=5)
            if related_news:
                st.markdown(f"**相关信息 ({len(related_news)}条):**")
                for n in related_news:
                    icon = {"positive": "🟢", "negative": "🔴"}.get(
                        n.get("sentiment"), "⚪")
                    st.markdown(f"- {icon} [{n.get('importance', 0)}⭐] "
                                f"{n.get('summary', '')}")

            gc1, gc2, gc3, gc4 = st.columns(4)
            with gc1:
                if st.button("💾 保存标签组", key=f"save_group_{i}"):
                    gid = save_tag_group(g["group_name"], g["tags"],
                                         group_logic=g.get("group_logic"),
                                         time_range=days)
                    if gid:
                        st.success(f"已保存 (ID: {gid})")
            with gc2:
                if st.button("🔍 快速分析", key=f"quick_analyze_{i}"):
                    with st.spinner("分析中..."):
                        try:
                            analysis = analyze_tag_group(g["tags"], days)
                            if analysis.get("group_logic"):
                                st.markdown("**成组逻辑:**")
                                st.markdown(analysis["group_logic"])
                            if analysis.get("news_ranked"):
                                st.markdown(f"**关联新闻 ({len(analysis['news_ranked'])}条):**")
                                for n in analysis["news_ranked"][:5]:
                                    icon = {"positive": "🟢", "negative": "🔴"}.get(
                                        n.get("sentiment"), "⚪")
                                    st.markdown(f"- {icon} [{n.get('importance', 0)}⭐] "
                                                f"{n.get('summary', '')}")
                        except Exception as e:
                            st.error(f"分析失败: {e}")
            with gc3:
                # 标签下拉选择器 + 研究此标签
                tag_options = g["tags"]
                if tag_options:
                    sel_tag = st.selectbox("选择标签", tag_options, key=f"sel_tag_{i}")
                    if st.button("🏷️ 研究此标签", key=f"tag_research_{i}"):
                        go_to_tag_research(sel_tag)
                        st.rerun()
            with gc4:
                # 需要先保存才能做标签组研究
                st.caption("保存后可在下方进行标签组研究")

st.markdown("---")

# 已保存标签组 + 深度研究
st.subheader("📦 已保存标签组")
try:
    saved_groups = get_saved_groups()
    if saved_groups:
        for g in saved_groups:
            tags = json.loads(g["tags_json"]) if g.get("tags_json") else []
            tags_display = " | ".join(f"#{t}" for t in tags)

            with st.expander(f"📦 {g['group_name']} (频次: {g.get('total_frequency', 0)})"):
                st.markdown(f"**标签:** {tags_display}")
                st.caption(f"时间范围: 近{g.get('time_range', 7)}天 | "
                           f"创建: {str(g.get('created_at', ''))[:10]}")
                if g.get("group_logic"):
                    st.markdown(f"**成组逻辑:** {g['group_logic']}")

                rc1, rc2, rc3 = st.columns(3)
                with rc1:
                    if st.button("🔬 深度研究", key=f"deep_{g['id']}"):
                        go_to_group_research(g["id"])
                        st.rerun()
                with rc3:
                    if st.button("🗑️ 删除", key=f"del_group_{g['id']}"):
                        delete_tag_group(g["id"])
                        st.rerun()

                # 展示最近研究结果摘要
                history = get_group_research_history(g["id"], 1)
                if history:
                    h = history[0]
                    st.caption(f"最近研究: {str(h.get('created_at', ''))[:10]}")
                    if h.get("macro_report"):
                        st.markdown(h["macro_report"][:200] + "...")
    else:
        st.caption("暂无已保存标签组，请先生成推荐并保存")
except Exception as e:
    st.caption(f"加载失败: {e}")

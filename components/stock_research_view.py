"""个股研究组件 — 6维雷达图 + 报告展示 + 触发研究 + 历史记录"""
import streamlit as st
import plotly.graph_objects as go
from research.deep_researcher import deep_research_stock
from research.report_generator import (
    get_research_report, list_research_records,
)


def render_stock_research_result(result):
    """渲染个股研究结果：6维雷达图 + 报告展示"""
    scores = result.get("scores", {})
    report = result.get("report", {})
    rec = result.get("recommendation", "")

    rec_colors = {"强烈推荐": "🟢", "推荐": "🟢", "中性": "🟡", "谨慎": "🟠", "回避": "🔴"}
    st.markdown(f"### {rec_colors.get(rec, '⚪')} 建议: {rec} | 综合评分: {scores.get('overall', '—')}/100")

    if report.get("executive_summary"):
        st.info(report["executive_summary"])

    col_radar, col_scores = st.columns([2, 1])
    with col_radar:
        dims = ["财务", "估值", "技术", "情绪", "催化", "风险"]
        keys = ["financial", "valuation", "technical", "sentiment", "catalyst", "risk"]
        values = [scores.get(k, 0) or 0 for k in keys]

        fig = go.Figure(data=go.Scatterpolar(
            r=values + [values[0]],
            theta=dims + [dims[0]],
            fill="toself",
            fillcolor="rgba(76, 175, 80, 0.2)",
            line=dict(color="#4CAF50", width=2),
        ))
        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            showlegend=False, height=350, margin=dict(l=60, r=60, t=30, b=30),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_scores:
        for dim, key in zip(dims, keys):
            val = scores.get(key, 0) or 0
            color = "🟢" if val >= 70 else "🟡" if val >= 50 else "🔴"
            st.markdown(f"{color} **{dim}**: {val}/100")
        st.markdown(f"---\n**综合**: {scores.get('overall', 0)}/100")

    report_sections = [
        ("financial_analysis", "💰 财务分析"),
        ("valuation_analysis", "📊 估值分析"),
        ("technical_analysis", "📈 技术分析"),
        ("sentiment_analysis", "🎭 市场情绪"),
        ("catalyst_analysis", "⚡ 催化剂"),
        ("risk_analysis", "⚠️ 风险分析"),
        ("conclusion", "📝 投资结论"),
    ]
    for key, title in report_sections:
        content = report.get(key)
        if content:
            st.markdown(f"**{title}**")
            st.markdown(content)


def render_stock_research_trigger(stock_code, key_prefix="sr"):
    """触发个股深度研究按钮+进度条"""
    if st.button("🔬 开始深度研究", use_container_width=True, key=f"{key_prefix}_trigger_{stock_code}"):
        progress = st.progress(0)
        status = st.empty()
        total_steps = 7
        step = [0]

        def on_progress(msg):
            step[0] += 1
            progress.progress(min(step[0] / total_steps, 0.95))
            status.text(msg)

        try:
            result = deep_research_stock(stock_code, progress_callback=on_progress)
            progress.progress(1.0)
            if result.get("error"):
                st.error(f"研究失败: {result['error']}")
            else:
                st.success(f"研究完成! (ID: {result.get('research_id', '')})")
                st.session_state[f"{key_prefix}_latest_{stock_code}"] = result
        except Exception as e:
            st.error(f"研究异常: {e}")

    # 展示最新结果
    latest = st.session_state.get(f"{key_prefix}_latest_{stock_code}")
    if latest and not latest.get("error"):
        st.markdown("---")
        st.subheader("📊 研究结果")
        render_stock_research_result(latest)


def render_stock_research_history(stock_code, key_prefix="sr"):
    """展示个股历史研究记录"""
    st.subheader("📚 历史研究记录")
    try:
        records = list_research_records("stock", 20)
        # 过滤为当前股票
        stock_records = [r for r in records if r.get("target") == stock_code]
        if stock_records:
            for r in stock_records:
                score = r.get("overall_score", "—")
                rec = r.get("recommendation", "—")
                with st.expander(
                    f"📈 {r['target']} — 评分:{score} 建议:{rec} ({r.get('research_date', '')})",
                    key=f"{key_prefix}_hist_{r['id']}",
                ):
                    report = get_research_report(r["id"])
                    if report and report.get("type") == "stock":
                        render_stock_research_result(report)
        else:
            st.caption("暂无该股票的研究记录")
    except Exception as e:
        st.caption(f"加载失败: {e}")

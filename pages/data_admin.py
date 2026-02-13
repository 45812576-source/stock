"""行情数据 — 结构化市场数据拉取（AKShare）"""
import streamlit as st
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

st.title("📈 行情数据")

# ==================== 基础数据拉取 ====================
st.subheader("基础数据拉取")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("🔄 初始化数据库"):
        from db.init_db import init_database
        init_database()
        st.success("完成")
        st.rerun()

with col2:
    if st.button("📥 拉取股票基础信息"):
        try:
            from ingestion.akshare_source import fetch_stock_info
            count = fetch_stock_info()
            st.success(f"拉取完成: {count}条")
        except Exception as e:
            st.error(f"失败: {e}")

with col3:
    stock_to_fetch = st.text_input("股票代码", key="fetch_code")
    if st.button("📈 拉取个股行情"):
        if stock_to_fetch:
            try:
                from ingestion.akshare_source import (
                    fetch_stock_daily, fetch_capital_flow,
                    fetch_stock_detail, fetch_financial_data,
                )
                with st.spinner("拉取中..."):
                    d = fetch_stock_daily(stock_to_fetch)
                    c = fetch_capital_flow(stock_to_fetch)
                    fetch_stock_detail(stock_to_fetch)
                    f = fetch_financial_data(stock_to_fetch)
                st.success(f"日线: {d}条, 资金流: {c}条, 财务: {f}条")
            except Exception as e:
                st.error(f"失败: {e}")

# ==================== 市场数据拉取 ====================
st.markdown("---")
st.subheader("市场数据拉取")

col4, col5 = st.columns(2)
with col4:
    if st.button("🏭 拉取行业资金流向"):
        try:
            from ingestion.akshare_source import fetch_industry_capital_flow
            count = fetch_industry_capital_flow()
            st.success(f"完成: {count}条")
        except Exception as e:
            st.error(f"失败: {e}")

with col5:
    if st.button("🧭 拉取北向资金"):
        try:
            from ingestion.akshare_source import fetch_northbound_flow
            count = fetch_northbound_flow()
            st.success(f"完成: {count}条")
        except Exception as e:
            st.error(f"失败: {e}")

# ==================== 批量拉取 ====================
st.markdown("---")
st.subheader("📦 批量拉取")
st.caption("一键拉取所有结构化行情数据（股票信息 → 行业资金流 → 北向资金）")

if st.button("🚀 执行批量拉取", type="primary"):
    progress = st.progress(0)
    status = st.empty()
    results = []

    steps = [
        ("拉取股票基础信息", lambda: __import__("ingestion.akshare_source", fromlist=["fetch_stock_info"]).fetch_stock_info()),
        ("拉取行业资金流向", lambda: __import__("ingestion.akshare_source", fromlist=["fetch_industry_capital_flow"]).fetch_industry_capital_flow()),
        ("拉取北向资金", lambda: __import__("ingestion.akshare_source", fromlist=["fetch_northbound_flow"]).fetch_northbound_flow()),
    ]

    for i, (name, func) in enumerate(steps):
        status.text(f"正在{name}...")
        try:
            count = func()
            results.append(f"✅ {name}: {count}")
        except Exception as e:
            results.append(f"❌ {name}: {e}")
        progress.progress((i + 1) / len(steps))

    status.empty()
    progress.empty()
    for r in results:
        st.write(r)

"""系统管理"""
import streamlit as st
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.db_utils import execute_query, table_row_count
from config import DB_PATH

st.title("⚙️ 系统管理")

# 数据库状态
st.subheader("数据库统计")
tables = [
    "data_sources", "raw_items", "cleaned_items", "item_companies",
    "item_industries", "research_reports", "stock_info", "stock_daily",
    "capital_flow", "industry_capital_flow", "northbound_flow",
    "macro_indicators", "financial_reports", "kg_entities",
    "kg_relationships", "deep_research", "investment_opportunities",
    "watchlist", "holding_positions", "dashboard_tag_frequency",
    "tag_groups", "pipeline_runs", "api_usage",
]
try:
    import pandas as pd
    stats = []
    for t in tables:
        try:
            cnt = table_row_count(t)
            stats.append({"表名": t, "行数": cnt})
        except Exception:
            stats.append({"表名": t, "行数": "N/A"})
    st.dataframe(pd.DataFrame(stats), use_container_width=True)
    db_size = DB_PATH.stat().st_size / 1024 / 1024 if DB_PATH.exists() else 0
    st.caption(f"数据库大小: {db_size:.2f} MB")
except Exception as e:
    st.error(f"数据库未初始化: {e}")
    if st.button("初始化数据库"):
        from db.init_db import init_database
        init_database()
        st.success("数据库初始化完成")
        st.rerun()

# API用量
st.markdown("---")
st.subheader("API用量统计")
try:
    usage = execute_query(
        """SELECT api_name, call_date, call_count, input_tokens, output_tokens, cost_usd
           FROM api_usage ORDER BY call_date DESC LIMIT 30"""
    )
    if usage:
        import pandas as pd
        st.dataframe(pd.DataFrame(usage), use_container_width=True)
    else:
        st.caption("暂无API调用记录")
except Exception:
    st.caption("暂无数据")

# 流水线日志
st.markdown("---")
st.subheader("流水线执行日志")
try:
    logs = execute_query(
        """SELECT pipeline_name, started_at, finished_at, status, items_processed, error_message
           FROM pipeline_runs ORDER BY started_at DESC LIMIT 20"""
    )
    if logs:
        import pandas as pd
        st.dataframe(pd.DataFrame(logs), use_container_width=True)
    else:
        st.caption("暂无执行记录")
except Exception:
    st.caption("暂无数据")

# 数据源配置
st.markdown("---")
st.subheader("数据源配置")
try:
    sources = execute_query("SELECT * FROM data_sources")
    if sources:
        import pandas as pd
        st.dataframe(pd.DataFrame(sources), use_container_width=True)
except Exception:
    st.caption("暂无数据")

"""个人股票分析系统 — Streamlit主入口"""
import streamlit as st
import sys
from pathlib import Path

# 确保项目根目录在path中
sys.path.insert(0, str(Path(__file__).parent))

from config import PAGE_TITLE, PAGE_ICON, LAYOUT

st.set_page_config(page_title=PAGE_TITLE, page_icon=PAGE_ICON, layout=LAYOUT)

# 使用 st.navigation 定义分组侧边栏
analysis_pages = [
    st.Page("pages/overview.py", title="每日概览", icon="📊"),
    st.Page("pages/stock_research.py", title="个股研究", icon="🔬"),
    st.Page("pages/hotspot_research.py", title="热点研究", icon="🔥"),
]

admin_pages = [
    st.Page("pages/cleaning.py", title="信息采集与清洗", icon="📰"),
    st.Page("pages/kg_admin.py", title="知识图谱管理", icon="🕸️"),
    st.Page("pages/data_admin.py", title="行情数据", icon="📈"),
    st.Page("pages/system.py", title="系统管理", icon="⚙️"),
]

pg = st.navigation({
    "分析面板": analysis_pages,
    "管理后台": admin_pages,
})

pg.run()

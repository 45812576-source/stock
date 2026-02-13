"""知识图谱 — 可视化 + 实体管理 + 关系浏览 + 路径查找"""
import streamlit as st
import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from knowledge_graph.kg_manager import (
    add_entity, get_entity_by_id, update_entity, delete_entity,
    find_entity, get_all_entities, get_entity_count,
    add_relationship, update_relationship, delete_relationship,
    get_entity_relations, get_subgraph, get_kg_stats, get_update_log,
)
from knowledge_graph.kg_updater import update_from_cleaned_items
from knowledge_graph.kg_query import search_entities, find_path, get_related_stocks

st.title("🕸️ 知识图谱管理")

# ==================== 统计概览 ====================
try:
    stats = get_kg_stats()
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("实体总数", stats["total_entities"])
    m2.metric("关系总数", stats["total_relationships"])
    type_str = " | ".join(f"{k}:{v}" for k, v in stats.get("entity_by_type", {}).items())
    m3.metric("实体分布", type_str[:30] if type_str else "无")
    rel_str = " | ".join(f"{k}:{v}" for k, v in stats.get("rel_by_type", {}).items())
    m4.metric("关系分布", rel_str[:30] if rel_str else "无")
except Exception:
    st.caption("知识图谱数据加载中...")

st.markdown("---")

# ==================== 功能Tab ====================
tab_search, tab_viz, tab_path, tab_manage, tab_update = st.tabs(
    ["🔍 搜索浏览", "🕸️ 图谱可视化", "🔗 路径查找", "✏️ 管理", "🔄 更新"]
)

# ==================== Tab 1: 搜索浏览 ====================
with tab_search:
    sc1, sc2 = st.columns([3, 1])
    with sc1:
        search_kw = st.text_input("搜索实体", placeholder="输入公司名、行业名或主题...",
                                  key="search_kw")
    with sc2:
        search_type = st.selectbox("类型过滤", ["全部", "company", "industry", "theme",
                                                "macro_factor", "indicator"], key="search_type")

    if search_kw:
        try:
            etype = None if search_type == "全部" else search_type
            results = search_entities(search_kw, entity_type=etype, limit=30)
            if results:
                st.caption(f"找到 {len(results)} 个实体")
                for ent in results:
                    type_icons = {"company": "🏢", "industry": "🏭", "theme": "🎯",
                                  "macro_factor": "🌐", "indicator": "📊"}
                    icon = type_icons.get(ent["entity_type"], "📌")
                    with st.expander(f"{icon} [{ent['entity_type']}] {ent['entity_name']}"):
                        if ent.get("description"):
                            st.markdown(f"**描述:** {ent['description']}")
                        if ent.get("properties_json"):
                            try:
                                props = json.loads(ent["properties_json"])
                                st.json(props)
                            except (json.JSONDecodeError, TypeError):
                                pass
                        st.caption(f"创建: {ent.get('created_at', '')[:10]} | "
                                   f"更新: {ent.get('updated_at', '')[:10]}")

                        # 显示关系
                        rels = get_entity_relations(ent["id"])
                        if rels["outgoing"]:
                            st.markdown("**→ 出向关系:**")
                            for r in rels["outgoing"]:
                                dir_icon = {"positive": "🟢", "negative": "🔴"}.get(
                                    r.get("direction"), "⚪")
                                st.markdown(
                                    f"  {dir_icon} —[{r['relation_type']}]→ "
                                    f"{r.get('target_name', '')} "
                                    f"(强度:{r.get('strength', 0):.1f})")
                        if rels["incoming"]:
                            st.markdown("**← 入向关系:**")
                            for r in rels["incoming"]:
                                dir_icon = {"positive": "🟢", "negative": "🔴"}.get(
                                    r.get("direction"), "⚪")
                                st.markdown(
                                    f"  {dir_icon} {r.get('source_name', '')} "
                                    f"—[{r['relation_type']}]→ "
                                    f"(强度:{r.get('strength', 0):.1f})")

                        # 关联股票
                        stocks = get_related_stocks(ent["entity_name"])
                        if stocks:
                            st.markdown("**📈 关联股票:**")
                            for s in stocks:
                                st.markdown(f"  - {s['entity_name']} "
                                            f"(强度:{s.get('strength', 0):.1f}, "
                                            f"{s.get('direction', '')})")
            else:
                st.caption("未找到匹配实体")
        except Exception as e:
            st.error(f"搜索失败: {e}")

    # 实体列表浏览
    st.markdown("---")
    st.subheader("实体列表")
    browse_type = st.selectbox("按类型浏览", ["全部", "company", "industry", "theme",
                                              "macro_factor", "indicator"], key="browse_type")
    try:
        btype = None if browse_type == "全部" else browse_type
        count = get_entity_count(btype)
        st.caption(f"共 {count} 个实体")
        entities = get_all_entities(entity_type=btype, limit=50)
        if entities:
            df = pd.DataFrame(entities)
            cols = ["id", "entity_type", "entity_name", "description", "updated_at"]
            df_show = df[[c for c in cols if c in df.columns]].copy()
            df_show.columns = ["ID", "类型", "名称", "描述", "更新时间"][:len(df_show.columns)]
            st.dataframe(df_show, use_container_width=True, hide_index=True)
    except Exception as e:
        st.caption(f"加载失败: {e}")

# ==================== Tab 2: 图谱可视化 ====================
with tab_viz:
    st.subheader("子图可视化")
    viz_search = st.text_input("输入中心实体名称", key="viz_search",
                               placeholder="输入实体名搜索并可视化其关系网络")
    viz_depth = st.slider("展开深度", 1, 3, 2, key="viz_depth")

    if viz_search:
        try:
            found = find_entity(viz_search)
            if found:
                sel_ent = found[0]
                st.info(f"中心实体: [{sel_ent['entity_type']}] {sel_ent['entity_name']}")

                subgraph = get_subgraph(sel_ent["id"], depth=viz_depth)
                nodes = subgraph["nodes"]
                edges = subgraph["edges"]

                if nodes:
                    try:
                        from pyvis.network import Network
                        import streamlit.components.v1 as components

                        net = Network(height="500px", width="100%", bgcolor="#0e1117",
                                      font_color="white", directed=True)
                        net.barnes_hut(gravity=-3000, central_gravity=0.3,
                                       spring_length=150)

                        type_colors = {
                            "company": "#4CAF50", "industry": "#2196F3",
                            "theme": "#FF9800", "macro_factor": "#E91E63",
                            "indicator": "#9C27B0",
                        }
                        type_shapes = {
                            "company": "dot", "industry": "diamond",
                            "theme": "triangle", "macro_factor": "star",
                            "indicator": "square",
                        }

                        added_nodes = set()
                        for n in nodes:
                            nid = str(n["id"])
                            if nid not in added_nodes:
                                color = type_colors.get(n["entity_type"], "#607D8B")
                                shape = type_shapes.get(n["entity_type"], "dot")
                                size = 30 if n["id"] == sel_ent["id"] else 20
                                net.add_node(nid, label=n["entity_name"],
                                             color=color, shape=shape, size=size,
                                             title=f"[{n['entity_type']}] {n.get('description', '')}")
                                added_nodes.add(nid)

                        added_edges = set()
                        for e in edges:
                            src = str(e["source_entity_id"])
                            tgt = str(e["target_entity_id"])
                            edge_key = f"{src}-{tgt}-{e['relation_type']}"
                            if edge_key not in added_edges and src in added_nodes and tgt in added_nodes:
                                edge_color = {"positive": "#4CAF50", "negative": "#F44336"}.get(
                                    e.get("direction"), "#9E9E9E")
                                width = max(1, float(e.get("strength", 0.5)) * 4)
                                net.add_edge(src, tgt, label=e["relation_type"],
                                             color=edge_color, width=width,
                                             title=e.get("evidence", ""))
                                added_edges.add(edge_key)

                        # 保存并展示
                        html_path = "/tmp/kg_graph.html"
                        net.save_graph(html_path)
                        with open(html_path, "r", encoding="utf-8") as f:
                            html_content = f.read()
                        components.html(html_content, height=520, scrolling=True)

                        st.caption(f"节点: {len(added_nodes)} | 边: {len(added_edges)}")

                    except ImportError:
                        st.warning("pyvis未安装，使用文本模式展示")
                        st.markdown(f"**节点 ({len(nodes)}):**")
                        for n in nodes:
                            st.markdown(f"- [{n['entity_type']}] {n['entity_name']}")
                        st.markdown(f"**边 ({len(edges)}):**")
                        for e in edges:
                            st.markdown(f"- {e.get('source_entity_id')} "
                                        f"—[{e['relation_type']}]→ {e.get('target_entity_id')}")
                else:
                    st.caption("该实体暂无关系网络")
            else:
                st.caption("未找到匹配实体")
        except Exception as e:
            st.error(f"可视化失败: {e}")

# ==================== Tab 3: 路径查找 ====================
with tab_path:
    st.subheader("实体间路径查找")
    pc1, pc2 = st.columns(2)
    with pc1:
        path_src = st.text_input("起始实体", key="path_src", placeholder="如：宁德时代")
    with pc2:
        path_tgt = st.text_input("目标实体", key="path_tgt", placeholder="如：新能源")

    if st.button("查找路径", key="btn_find_path") and path_src and path_tgt:
        try:
            src_ents = find_entity(path_src)
            tgt_ents = find_entity(path_tgt)
            if not src_ents:
                st.warning(f"未找到实体: {path_src}")
            elif not tgt_ents:
                st.warning(f"未找到实体: {path_tgt}")
            else:
                path = find_path(src_ents[0]["id"], tgt_ents[0]["id"], max_depth=5)
                if path:
                    st.success(f"找到路径（{len(path)}步）")
                    path_names = []
                    for pid in path:
                        ent = get_entity_by_id(pid)
                        if ent:
                            path_names.append(f"[{ent['entity_type']}] {ent['entity_name']}")
                    st.markdown(" → ".join(path_names))
                else:
                    st.info("未找到连接路径（最大深度5）")
        except Exception as e:
            st.error(f"路径查找失败: {e}")

    # 关联股票查找
    st.markdown("---")
    st.subheader("关联股票查找")
    stock_search = st.text_input("输入主题/行业/宏观因素", key="stock_search",
                                 placeholder="如：AI、半导体、碳中和")
    if stock_search:
        try:
            stocks = get_related_stocks(stock_search)
            if stocks:
                st.markdown(f"**与「{stock_search}」关联的股票:**")
                df_stocks = pd.DataFrame(stocks)
                df_stocks.columns = ["公司", "关联强度", "影响方向"]
                df_stocks = df_stocks.sort_values("关联强度", ascending=False)
                st.dataframe(df_stocks, use_container_width=True, hide_index=True)
            else:
                st.caption("未找到关联股票")
        except Exception as e:
            st.caption(f"查找失败: {e}")

# ==================== Tab 4: 管理 ====================
with tab_manage:
    manage_tab1, manage_tab2, manage_tab3 = st.tabs(["添加实体", "添加关系", "删除"])

    with manage_tab1:
        with st.form("add_entity_form"):
            ae1, ae2 = st.columns(2)
            with ae1:
                etype = st.selectbox("实体类型",
                    ["company", "industry", "theme", "macro_factor", "indicator"],
                    format_func=lambda x: {"company": "🏢 公司", "industry": "🏭 行业",
                        "theme": "🎯 主题", "macro_factor": "🌐 宏观因素",
                        "indicator": "📊 指标"}.get(x, x))
            with ae2:
                ename = st.text_input("实体名称")
            edesc = st.text_area("描述", height=80)
            eprops = st.text_input("属性JSON（可选）", placeholder='{"stock_code": "300750"}')
            if st.form_submit_button("添加实体"):
                if ename:
                    props = None
                    if eprops:
                        try:
                            props = json.loads(eprops)
                        except json.JSONDecodeError:
                            st.error("属性JSON格式错误")
                            props = None
                    eid = add_entity(etype, ename, edesc or None, props)
                    if eid:
                        st.success(f"已添加实体: {ename} (ID: {eid})")

    with manage_tab2:
        with st.form("add_rel_form"):
            rc1, rc2 = st.columns(2)
            with rc1:
                rel_src = st.text_input("源实体名称", key="rel_src")
            with rc2:
                rel_tgt = st.text_input("目标实体名称", key="rel_tgt")
            rc3, rc4, rc5 = st.columns(3)
            with rc3:
                rel_type = st.selectbox("关系类型",
                    ["impacts", "belongs_to", "competes", "supplies", "benefits", "related"])
            with rc4:
                rel_str = st.slider("强度", 0.1, 1.0, 0.5, 0.1, key="rel_str")
            with rc5:
                rel_dir = st.selectbox("方向", ["positive", "negative", "neutral"],
                    format_func=lambda x: {"positive": "🟢 正向", "negative": "🔴 负向",
                        "neutral": "⚪ 中性"}.get(x, x))
            rel_evidence = st.text_input("依据", key="rel_evidence")
            if st.form_submit_button("添加关系"):
                if rel_src and rel_tgt:
                    src_ents = find_entity(rel_src)
                    tgt_ents = find_entity(rel_tgt)
                    if not src_ents:
                        st.error(f"未找到源实体: {rel_src}")
                    elif not tgt_ents:
                        st.error(f"未找到目标实体: {rel_tgt}")
                    else:
                        rid = add_relationship(src_ents[0]["id"], tgt_ents[0]["id"],
                                               rel_type, rel_str, rel_dir,
                                               rel_evidence or None)
                        if rid:
                            st.success(f"已添加关系: {rel_src} —[{rel_type}]→ {rel_tgt}")

    with manage_tab3:
        st.markdown("**删除实体**")
        del_name = st.text_input("输入要删除的实体名称", key="del_name")
        if del_name:
            del_ents = find_entity(del_name)
            if del_ents:
                for de in del_ents:
                    dc1, dc2 = st.columns([3, 1])
                    dc1.text(f"[{de['entity_type']}] {de['entity_name']} (ID:{de['id']})")
                    if dc2.button("删除", key=f"del_{de['id']}"):
                        delete_entity(de["id"])
                        st.success(f"已删除: {de['entity_name']}")
                        st.rerun()

# ==================== Tab 5: 更新 ====================
with tab_update:
    st.subheader("从清洗数据更新图谱")
    uc1, uc2 = st.columns(2)
    with uc1:
        update_since = st.date_input("起始日期",
            value=datetime.now() - timedelta(days=7), key="update_since")
    with uc2:
        use_claude = st.checkbox("使用Claude智能提取（消耗API额度）", value=False)

    if st.button("🔄 开始更新", key="btn_update_kg"):
        progress = st.progress(0)
        status = st.empty()

        def on_progress(current, total, msg):
            if total > 0:
                progress.progress(current / total)
            status.text(msg)

        try:
            result = update_from_cleaned_items(
                since_date=update_since.strftime("%Y-%m-%d"),
                use_claude=use_claude,
                progress_callback=on_progress,
            )
            progress.progress(1.0)
            st.success(f"更新完成: 处理{result['processed']}条, "
                       f"新增实体{result['entities']}, 新增关系{result['relationships']}")
        except Exception as e:
            st.error(f"更新失败: {e}")

    # 变更日志
    st.markdown("---")
    st.subheader("变更日志")
    try:
        logs = get_update_log(30)
        if logs:
            df_log = pd.DataFrame(logs)
            cols = ["id", "action", "entity_id", "relationship_id", "source", "updated_at"]
            df_show = df_log[[c for c in cols if c in df_log.columns]].copy()
            df_show.columns = ["ID", "操作", "实体ID", "关系ID", "来源", "时间"][:len(df_show.columns)]
            st.dataframe(df_show, use_container_width=True, hide_index=True)
        else:
            st.caption("暂无变更记录")
    except Exception:
        st.caption("变更日志加载失败")

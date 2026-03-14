# DB Data Consumption Audit

## 架构现状

### 云端 (8.134.184.254:3301, stock_analysis DB + stock_db)
**stock_analysis DB:**
- source_documents, extracted_texts, raw_items, data_sources
- content_summaries, stock_mentions (Pipeline A/B2 输出)
- kg_entities, kg_relationships (Pipeline C 输出，也有本地副本)
- cleaned_items, item_companies, item_industries, research_reports (旧管线)
- macro_indicators, margin_balance, market_valuation, hsgt_holding, overseas_etf

**stock_db (行情库):**
- stock_data (K线), fund_flow_history (资金流)

### 本地 (127.0.0.1:3306)
- cleaned_items/item_companies/item_industries (同步自云端)
- source_documents, raw_items (同步自云端，全量)
- content_summaries, extracted_texts, stock_mentions (由 sync_new_pipeline_records 增量同步)
- stock_info, stock_daily, capital_flow (akshare直写)
- kg_entities/kg_relationships (KG本地读写)
- tag_groups/tag_group_research/dashboard_tag_frequency (热点分析)
- watchlist/holding_positions/deep_research
- text_chunks/kg_triple_chunks (向量切片，仅本地)
- stock_rule_tags, stock_selection_rules (规则标签)

## 消费点全景

### ✅ 正常（符合架构设计）
| 模块 | 读 | 写 | 说明 |
|------|----|----|------|
| routers/* (大部分) | 本地 execute_query | - | 正常 |
| utils/content_query.py | 本地 content_summaries+extracted_texts | - | 依赖本地同步 |
| hotspot/* | 本地 | 本地 | 正常 |
| tagging/l1_quant_engine.py | 本地 | 本地 | 正常 |
| retrieval/chunker.py | 本地 | 本地 | 正常 |
| knowledge_graph/kg_extractor_pipeline.py | 云端 extracted_texts | 本地 kg_* | 正确，是Pipeline C |
| ingestion/* | - | 云端 source_documents | 正确 |
| cleaning/* | 云端 extracted_texts | 云端 content_summaries | 正确 |
| routers/datacollect.py | 云端 | 云端 | 数据管理页，正确 |
| routers/summary_review.py | 云端 content_summaries | 云端 | 管理员审核，正确 |
| routers/settings.py | 云端 extracted_texts | 云端 | 管道管理，正确 |
| retrieval/summary_chunker.py | 云端 content_summaries | 本地 text_chunks | 批量回填，正确 |

### ❌ 问题1：capital/market routers 直连云端行情库（最大问题）
- routers/capital.py — 自建 _cloud_query() 直连云端 stock_db，查 stock_data + fund_flow_history
- routers/market.py — 导入 cloud_stockdb_query as _cq，查同样的表
- **问题**：每次资金面请求都打云端，有延迟，也有 _cap_cache 内存缓存但无持久化
- **背景**：本地已有 stock_daily + capital_flow，是 akshare 实时写入的
- **差距**：stock_data/fund_flow_history 是行情服务写入云端的，比 akshare 数据更全（全市场）
- **解法选项**：
  A. 扩展 sync_macro_to_local 定期同步 stock_data/fund_flow_history 到本地（但体积大）
  B. 保持直连云端，加更好的缓存（当前方式）
  C. 增量同步最近30天数据（scheduler 每日）

### ❌ 问题2：tagging/l2_ai_engine 读 stock_mentions 直连云端
- l2_ai_engine.py L11-13: _q() = execute_cloud_query
- 读 stock_mentions 从云端（已同步到本地）
- l3_deep_engine.py L17-18: _cq() = execute_cloud_query
- 读 stock_mentions 从云端
- **解法**：l2/l3 读 stock_mentions 改为本地，已有同步数据

### ❌ 问题3：research 模块直连云端
- research/indicator_data_fetcher.py — 读云端 content_summaries + cleaned_items
- research/fact_anchors.py — 读云端 content_summaries + cleaned_items
- **问题**：深度研究时每次调用都打云端
- **解法**：content_summaries 已同步本地，改用本地即可

### ⚠️ 问题4：content_summaries 同步字段不完整
- sync_new_pipeline_records() 和 sync_summary_to_local() 同步时缺少 type_fields / family 列
- 本地查询若用到这两列会得到 NULL
- content_query.py 不用这两列，暂时没问题
- 但 pipeline 管理、summary review 都直接读云端，不受影响

### ⚠️ 问题5：overview.py /api/news-feed 直连云端
- L642-685: 直接用 execute_cloud_query 读 content_summaries
- 其他 overview 端点用 query_content_summaries()（本地）
- 不一致，容易混淆
- **解法**：改用本地（依赖 sync_new_pipeline_records 定期运行）

## 同步机制现状
- sync_cleaned_to_local() — 旧管线，cleaning完成后即时同步
- sync_summary_to_local() — Pipeline A 完成后即时同步（字段不完整）
- sync_mentions_to_local() — Pipeline B2 完成后即时同步
- sync_new_pipeline_records() — 增量全量同步三张表（scheduler调用）
- sync_macro_to_local() — 5张宏观表增量同步（scheduler调用）
- sync_stock_data_from_cloud() — 按个股触发同步K线（ensure_stock_data调用）

## 建议优先级
1. **高**：统一 l2/l3_ai_engine 从本地读 stock_mentions（避免每次标注打云端）
2. **高**：research/indicator_data_fetcher + fact_anchors 改用本地 content_summaries
3. **中**：overview.py /api/news-feed 改用本地（需确认 sync 定期运行）
4. **中**：capital/market 行情数据 — 确认云端 stock_data/fund_flow_history 已有数据后考虑定期增量同步
5. **低**：sync_summary_to_local 补充 type_fields/family 列同步

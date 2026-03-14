# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 架构概览

个人股票分析系统，FastAPI + MySQL 双库架构，集成 Claude/DeepSeek API 做金融信息结构化清洗。

## 双库分工

```
云端 MySQL (8.134.184.254:3301, DB: stock_analysis)
├── source_documents    ← zsxq/报告采集的原始文档（doc_type: news/report/audio）
├── extracted_texts     ← 从 source_documents 提取的纯文本（三条管线的输入）
├── raw_items           ← 旧采集管道写入（source_id → data_sources）
├── data_sources        ← 数据源配额管理
├── content_summaries   ← Pipeline A: Claude FOE 总结结果
├── stock_mentions      ← Pipeline B2: DeepSeek 股票提及提取
├── kg_entities/kg_relationships ← Pipeline C: KG 三元组（DeepSeek）
├── cleaned_items       ← 旧 Claude 清洗结果
├── item_companies/item_industries ← 关联表
└── research_reports    ← 研报元数据

本地 MySQL (127.0.0.1:3306, DB: stock_analysis)
├── cleaned_items/item_companies/item_industries ← 同步自云端
├── source_documents/raw_items ← 同步自云端（全量）
├── stock_info/capital_flow/stock_daily ← akshare 直接写本地
├── kg_entities/kg_relationships ← 知识图谱（本地读写）
├── dashboard_tag_frequency/tag_groups/tag_group_research ← 热点分析
└── watchlist/holding_positions ← 持仓/自选
```

**规则：**
- 采集管道（ingestion/）全部写**云端** → `execute_cloud_query` / `execute_cloud_insert`
- 清洗管道（cleaning/）读写**云端**，完成后通过 `sync_cleaned_to_local()` 同步到本地
- 前端展示（dashboards/pages/routers/）读**本地** → `execute_query`
- 行情数据（akshare）直接写**本地** → `execute_insert`

## 数据流（新管线）

```
外部数据源 → ingestion/ → 云端 source_documents（zsxq/报告）
                                    ↓ source_extractor.py
                            云端 extracted_texts（纯文本）
                                    ↓ cleaning/unified_pipeline.py（并发三条）
                    ┌───────────────┼───────────────┐
              Pipeline A          Pipeline B2      Pipeline C
          content_summaries    stock_mentions    kg triples
           (Claude FOE)         (DeepSeek)       (DeepSeek)
```

旧管线（raw_items → cleaned_items）仍存在但使用较少，新内容优先走 source_documents → extracted_texts。

## 关键文件

| 文件 | 职责 |
|------|------|
| `config/__init__.py` | 本地/云端 MySQL 连接配置，API keys |
| `utils/db_utils.py` | `execute_query/insert`(本地), `execute_cloud_query/insert`(云端), `sync_cleaned_to_local()` |
| `utils/model_router.py` | 多厂商模型调度（claude_cli/openai/deepseek），从 `model_configs` 表按 stage 路由 |
| `utils/skill_registry.py` | 加载 `~/.claude/skills/` 下的 skill 文件作为分析 prompt 模板 |
| `ingestion/base_source.py` | 数据源基类（限流、去重）→ 写云端 |
| `ingestion/zsxq_source.py` | 知识星球采集 → 写云端 source_documents |
| `ingestion/source_extractor.py` | 文档提取（PDF/图片/音频）→ 写云端 extracted_texts |
| `ingestion/eastmoney_report_source.py` | 东方财富研报（PDF全文）→ 写云端 |
| `ingestion/fxbaogao_source.py` | 发现报告采集 → 写云端 source_documents |
| `ingestion/djyanbao_source.py` | 洞见研报采集 → 写云端 source_documents |
| `cleaning/unified_pipeline.py` | 统一清洗管线，并发执行 A/B2/C 三条 |
| `cleaning/content_summarizer.py` | Pipeline A: Claude FOE 总结 |
| `cleaning/stock_mentions_extractor.py` | Pipeline B2: DeepSeek 股票提及 |
| `cleaning/claude_processor.py` | 旧 Claude 结构化清洗（cleaned_items） |
| `knowledge_graph/kg_extractor_pipeline.py` | KG 三元组提取管线 |
| `hotspot/tag_group_analyzer.py` | 热点标签组分析 |
| `research/deep_researcher.py` | 深度研究报告生成 |
| `tracking/auto_updater.py` | 持仓/自选自动更新 |
| `agent/tools.py` | Agent 工具集（供 routers/agent_chat.py 调用） |
| `scheduler.py` | APScheduler 定时任务（KG 06:00/20:00，时区 Asia/Shanghai） |
| `chat_worker.py` | 后台轮询 DB 处理待回复 AI 追问消息 |

## 信息源（ingestion/）

| source_name | 类型 | 写入表 |
|-------------|------|--------|
| `zsxq` | 知识星球帖子 | source_documents (doc_type=news) |
| `fxbaogao` | 发现报告研报 | source_documents (doc_type=report) |
| `djyanbao` | 洞见研报 | source_documents (doc_type=report) |
| `eastmoney_report` | 东方财富研报 PDF | raw_items |
| `jasper` | Jasper AI 资讯 | raw_items |
| `iwencai` | 问财新闻 | raw_items |
| `cninfo_notice` | 巨潮公告 | raw_items |
| `earnings` | 财报数据 | raw_items |
| `source_doc` | 手动上传文档 | raw_items |
| `akshare` | 行情/资金流向 | 本地直写 |

**注意：** `research_reports` 和 `documents` 表目前为空（0行），研报内容实际存在 `source_documents`（doc_type=report）中。

## 常用命令

```bash
cd /Users/liaoxia/stock-analysis-system

# FastAPI Web 应用（主入口）
uvicorn app_web:app --reload --host 0.0.0.0 --port 8501

# Streamlit 应用（旧入口）
streamlit run app.py

# AI 追问对话 Worker
python chat_worker.py              # 前台运行
python chat_worker.py --once       # 处理一次后退出

# 数据库
python scripts/migrate_dual_db.py  # 一次性迁移
python seed_test_data.py           # 生成测试数据（写本地）
```

## 注意事项

- 已彻底移除 SQLite 依赖，全部使用 pymysql
- `execute_query/insert` = 本地 MySQL；`execute_cloud_query/insert` = 云端 MySQL
- SQL 兼容层 `_adapt_sql()` 自动将 SQLite 语法转为 MySQL（历史遗留，新代码直接用 MySQL 语法）
- `model_router.py` 支持 `claude_cli`（默认，通过本地 claude CLI 子进程）、`openai`、`deepseek` 三种厂商
- Skills 目录 `~/.claude/skills/` 中的文件被 `skill_registry.py` 加载为分析 prompt 模板

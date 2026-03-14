# 每日概览页面重构 — 工作档案

## 需求概要
将概览页从当前纵向堆叠布局改为左右结构：左4/5（三个tab）+ 右1/5（固定AI分析助理）

### Tab 1: 持有组合信息聚合
- 第一栏：公司（公告+财报）、公司研报
- 第二栏：产业链、产业、主题新闻、产业研报
- 第三栏：宏观（影响公司和行业的宏观新闻），可拖拽到AI助理

### Tab 2: 大盘资金面
- 迁移现有市场总览页面内容
- 行业资金热力图完全采用原概览页的行业资金热度+解读

### Tab 3: 新闻聚合器
- 四个容器：宏观级/行业级/个股级/风险聚焦
- 最近3天（按source时间），各20条，按重要级别排序
- UI交互保持一致：标题+摘要，展开FOE summary
- 可拖拽给AI解读
- 可like到机会发现页面
- 可关联到portfolio项目

## 现有代码结构

### 路由
- `routers/overview.py` — 概览页路由+数据查询
- `routers/market.py` — 市场总览路由+资金面API
- `routers/project_chat.py` — Portfolio聊天API

### 模板
- `templates/overview.html` — 当前概览页（~550行）
- `templates/market.html` — 当前市场页（~550行）
- `templates/base.html` — 基础布局（侧边栏+主内容区）
- `templates/partials/_foe_card.html` — FOE卡片组件

### 数据模型
- `cleaned_items` — 结构化新闻（event_type, sentiment, importance 1-5, structured_json）
- `content_summaries` — FOE摘要（doc_type, fact_summary, opinion_summary, evidence_assessment）
- `item_companies` — 新闻→股票关联
- `item_industries` — 新闻→行业关联
- `capital_flow` — 资金流向数据
- `stock_mentions` — 股票提及+主题

### 现有概览页数据函数
- `get_watchlist_alerts()` — 自选股行情+新闻
- `get_industry_heat()` — 行业资金热力图
- `get_capital_insight()` — 资金热度解读
- `get_macro_news()` — 宏观新闻
- `get_research_picks()` — 研报精选
- `get_events()` — 事件跟踪
- `get_risk_warnings()` — 风险预警

### 现有市场页API（已在market.py中）
- `/market/api/indices` — 指数数据
- `/market/api/index-chart` — K线图
- `/market/api/capital/market-flow` — 成交额+主力资金
- `/market/api/capital/margin` — 融资余额
- `/market/api/capital/southbound` — 南向资金
- `/market/api/capital/etf-shares` — ETF成交量
- `/market/api/capital/industry-flow` — 行业资金热力图
- `/market/api/capital/summary` — 资金面汇总

### 聊天系统
- `portfolio/chat_handler.py` — 聊天消息处理
- `chat_worker.py` — 后台AI回复worker

## 已确认
- AI分析助理：复用现有 portfolio chat（project_chat.py API）
- "持有"：指"收藏"这一默认 portfolio（watchlist_lists id=1）
- importance 排序：已统一标准，直接用 importance 字段
- "机会发现页面"：已存在，即热点(hotspot)页面
- Tab1 持有组合：基本延用目前 overview 的 layout
- 拖拽：HTML5 Drag & Drop，新闻卡片 draggable → 右侧 chat drop zone → FOE 正文注入聊天上下文，用户可针对新闻提问

## 实施进度
- [x] 代码调研完成
- [ ] 设计方案确认
- [ ] 实施

# 运营后台 UI/UX 重构需求

> 用途：交给 Stitch（或其他 UI 设计工具/设计师）做专业设计。
> 设计方向：**浅色为主 + Ant Design Pro 企业风**。
> 现有技术栈：Vue 3 + Vite + Element Plus + Vue Router + Pinia + axios（暗色主题）。本次为设计层重构，后端接口与功能边界不变。

---

## 一、现状诊断（痛点）

当前后台为暗色主题（`#0F1117` 背景），220px 固定侧边栏 + 顶栏标题，几乎每个页面都用 `el-tabs` 把二级功能平铺在页内，页面由「tab → 卡片 → 表格」无限堆叠而成。主要问题：

| 维度 | 现状 | 问题 |
|------|------|------|
| 导航 | 侧边栏 4 项 + 顶栏标题 | 无面包屑、无全局搜索；二级功能全塞进页内 tab，层级扁平 |
| 信息架构 | 数据管理 5 tab、知识图谱 6 tab | tab 过载，用户看不到全貌，找功能靠试 |
| 页面结构 | 卡片+表格无限堆叠 | 无视觉层次，一屏信息量过大又缺重点 |
| 数据展示 | 统计卡纯数字+文字；表格 `size=small border` | 卡片无图标/趋势/对比；表格密不透风、可读性差 |
| 操作 | 送审/入库/重试/审核等 text 按钮平铺一排 | 主次不分，危险操作（删除）与普通操作同级 |
| 任务反馈 | 3 秒轮询 + 顶栏 popover + 页内进度卡 | 反馈弱且分散，长任务无全局可见状态中心 |
| 状态态 | 仅 `v-loading` 遮罩 | 无骨架屏、无空状态、无错误态引导 |
| 设计系统 | 仅 7 个 CSS 变量、单一主色 | 无系统化色阶/字阶/间距/圆角/阴影 token |
| 审核台 | 90% 宽弹窗 iframe+textarea | 高频核心场景挤在 dialog 里，效率低 |
| 登录 | 360px 极简卡片 | 无品牌感 |

---

## 二、设计目标与原则

1. **企业级中后台观感** —— 对标 Ant Design Pro，规整、组件完整、专业可信。
2. **浅色主题为主** —— 明亮清爽，适合长时间数据运营作业（可保留暗色作为后续可选变体，非本期重点）。
3. **信息分层** —— 用「概览 → 列表 → 详情」三级递进替代无脑 tab 平铺；一屏只强调一个主任务。
4. **数据密集但可呼吸** —— 表格保持高信息密度，通过间距、分组、斑马纹、粘性表头、列设置提升可读性。
5. **反馈即时且集中** —— 全局任务中心统一承载所有异步任务（清洗管线、批量下载、回填等）。
6. **一致性** —— 全站复用同一套 token 与组件，保证 Stitch 逐页产出视觉统一。

---

## 三、设计系统规范（Design Tokens）

以 Ant Design 5（Pro）默认体系为基准。

### 色彩
- **主色 Primary**：`#1677FF`（AntD 蓝），提供 1–10 完整色阶（浅底、hover、active、focus）。
- **背景层次**：
  - 页面底色 `#F0F2F5`（AntD Pro 经典灰）
  - 卡片/容器 `#FFFFFF`
  - 次级填充/表头 `#FAFAFA`
- **语义色**：success `#52C41A` / warning `#FAAD14` / error `#FF4D4F` / info `#1677FF`，各含「浅底 + 深字」用于 Tag/状态点。
- **文本**：主 `rgba(0,0,0,.88)` / 次 `rgba(0,0,0,.65)` / 辅助 `rgba(0,0,0,.45)` / 禁用 `rgba(0,0,0,.25)`。
- **边框/分割线**：`#F0F0F0`（分割）/ `#D9D9D9`（输入边框）。

### 字体
- 字体栈：`-apple-system, "Segoe UI", "PingFang SC", "Microsoft YaHei", Inter, sans-serif`。
- 字阶：12 / 14（正文基准）/ 16 / 20 / 24 / 30。
- 数字统计使用 **tabular-nums** 等宽对齐。

### 间距 / 圆角 / 阴影
- 间距：8px 基准栅格（4 / 8 / 12 / 16 / 24 / 32）。
- 圆角：基础 `6px`（AntD v5 默认），卡片 8px，Tag/小控件 4px。
- 阴影：卡片 `boxShadow: 0 1px 2px rgba(0,0,0,.03), 0 1px 6px -1px rgba(0,0,0,.02)`；悬浮层三级投影。

### 组件规范（对标 antd / ProComponents）
- 按钮：主 `primary` / 次 `default` / 文本 `text` / 危险 `danger`，三尺寸（large/middle/small）。
- 表格：**ProTable** 模式 —— 顶部筛选表单区 + 工具栏（刷新/列设置/密度/全屏）+ 分页 + 排序 + 粘性表头 + 行悬停。
- 表单：**ProForm** 分组，label 顶部或左侧对齐，必填/校验/联动完整。
- 卡片：**ProCard**，含标题、右上操作区、可折叠、栅格拆分。
- 统计：**Statistic** + 图标 + 环比（上升绿/下降红）。
- 状态：Tag（语义色）、Badge（角标/状态点）、Segmented（分段选择）。
- 空/加载/错误：**Empty**（空状态）/ **Skeleton**（骨架屏）/ **Result**（错误页含重试）。
- 抽屉 **Drawer** 优先用于「编辑/详情」，替代大量 Dialog。

---

## 四、全局框架（AppShell）

采用 **Ant Design Pro Layout**：侧边导航 + 顶栏 + 面包屑 + 内容区。

### 侧边导航
- 可折叠（展开 240px / 收起 80px 仅图标）。
- 支持**二级分组**，把当前塞在页内 tab 的功能上升为可见子菜单：
  - **数据管理** › 数据概况 / 源文档 / 清洗管线 / 结构化数据 / 选股策略
  - **知识图谱** › Schema / 可视化 / 实体管理 / 推理引擎 / 巡检 / 审核工作台
  - **系统设置** › API·模型 / Skill 编辑器
  - **用户管理** › 用户 / 积分包（仅超级管理员可见）

### 顶栏
- 左：面包屑（模块 › 子页）。
- 中：全局搜索（可选，唤起命令面板搜文档/用户/功能）。
- 右：**任务中心**图标（带角标）+ 用户菜单（头像/用户名/角色 Tag/登出）。

### 任务中心（关键）
- 点击顶栏图标展开 **Drawer**，统一列出所有运行中 / 最近完成任务。
- 每项含：任务名、进度条、当前处理项、状态、**暂停 / 恢复 / 取消**操作。
- 暂停采用软暂停语义，UI 明确标注「软暂停：停止推进新任务，在途批次会跑完」。

### 通用态（每个列表/卡片都要定义）
- 加载：Skeleton 骨架屏（非全屏遮罩）。
- 空：Empty 插画 + 说明 + 主操作按钮。
- 错误：Result 组件，展示原因 + 重试按钮。

---

## 五、分模块页面需求

### 1. 数据管理

**数据概况**
- 顶部 4 张 KPI 卡（文档总数 / 已提取 / 待提取 / 知识图谱关系数），带图标、环比、迷你趋势条。
- 「信息源明细」ProTable。
- 「文档类型分布」用**图表**（柱状/环形）替代当前一堆 Tag。

**源文档**
- 状态筛选做成顶部 **Segmented 分段器**（可点选、高亮当前，显示各状态计数）。
- 筛选栏（搜索 / 类型 / 信息源）成组左对齐；批量操作（送审 / 入库 / 重试）右对齐，选中后浮出操作条并显示选中计数。
- ProTable：粘性表头、行悬停、列设置、密度切换。

**审核工作台**（从弹窗升级为独立页）
- 左原文预览（iframe/PDF），右提取文本编辑器。
- 顶部动作条：重新提取 / 通过 / 拒绝 / 上一条 · 下一条。
- 支持键盘快捷键连续审核。

**清洗管线**
- A / C / D / A+C+D 与回填工具重组为**卡片式任务启动器**（每个管线一张 ProCard：名称 + 说明 + 参数 + 启动按钮）。
- 运行进度并入**全局任务中心**，页面不阻塞。
- 清洗日志用带筛选的表格/时间线替代 `<pre>` 文本块。

**结构化数据**
- 数据新鲜度做成状态卡（新鲜/滞后用色点提示）。
- 批量下载表单分组（股票池 / 时间范围 / 类型）。
- 监控规则 ProTable + **Drawer** 式增改。

**选股策略**
- 规则库列表 + Drawer 编辑。
- 标签引擎运行状态可视化（进度 + 结果统计）。

### 2. 知识图谱
- 顶部实体 / 关系统计做成 KPI 条。
- 可视化页给足画布空间（大图 + 侧栏筛选/图例）。
- 审核工作台采用「待审列表 + 详情」双栏。

### 3. 系统设置
- 收敛为 **API·模型配置** 与 **Skill 编辑器** 两块。
- 配置项用分组 ProForm 卡片。
- Skill 编辑器：左列表 + 右编辑器（代码区等宽字体 + 保存 / 测试）。

### 4. 用户管理
- 顶部概览 KPI（总用户 / 总积分 / 今日活跃 / 角色分布环形图）。
- 用户表增加头像与状态色点；操作收进「⋯」下拉；危险操作二次确认。
- 积分包用**卡片网格** + 上下架开关。

### 5. 登录
- 左右分栏：左品牌视觉区（产品名 + 插画/渐变），右登录表单卡。
- 含加载态与错误提示。

---

## 六、交互与反馈规范

- 所有异步操作：按钮 loading + 完成 Toast（message）+ 失败可重试。
- 危险操作：二次确认弹窗（Popconfirm / Modal.confirm），红色强调。
- 长任务：一律进任务中心，页面不阻塞。
- 列表：分页 / 每页条数 / 排序 / 列宽与列显隐记忆。
- 快捷键：审核台连续操作、全局搜索唤起。

---

## 七、响应式与可访问性

- 主战场为桌面（≥1280px），需在 1440 / 1920 下布局稳定。
- ≤1280 时侧边栏自动收起为图标态。
- 对比度符合 WCAG AA；所有交互元素有 hover / focus 态。

---

## 八、交付给 Stitch 的建议流程

1. 先让 Stitch 产出 **Design System 页**（色彩 / 字体 / 组件），锁定 token，再逐页生成，保证一致。
2. 每页 prompt 结构：`角色（数据运营后台）+ 主题（浅色 Ant Design Pro）+ 页面目标 + 布局分区 + 关键组件 + 状态态`。
3. 逐页顺序建议：AppShell 框架 → 数据概况 → 源文档列表 → 审核工作台 → 清洗管线 → 用户管理 → 登录。

### 可直接粘贴的 Stitch 起手 Prompt（英文）

> Design a professional light-themed enterprise admin dashboard in the Ant Design Pro style. Layout: collapsible left sidebar with grouped two-level navigation, top bar with breadcrumbs + global search + a task-center icon (with badge) + user menu. Page background #F0F2F5, cards #FFFFFF, primary color #1677FF, text rgba(0,0,0,.88). Base border-radius 6px. Use KPI statistic cards with icons and trend deltas, dense but readable ProTable-style data tables with sticky headers, column settings, row hover and pagination, ProForm grouped forms, Drawers for edit/detail, and consistent Empty / Skeleton / Result states. Clean, regular, trustworthy enterprise look.

### 逐页 Prompt 示例（源文档列表页）

> Ant Design Pro style, light theme. A "Source Documents" management page: top segmented status filter (with counts per status), a filter bar (search, doc type, source) on the left and batch actions (Submit for review, Ingest, Retry) on the right that float when rows are selected. Below: a dense ProTable with checkbox selection, sticky header, column settings, density toggle, row hover, status tags in semantic colors, a fixed right "Review" action column, and pagination. Include empty and loading (skeleton) states.

---

## 九、边界说明

- 本需求仅覆盖**设计层**；后端接口、功能边界、数据流不变。
- 迁移功能（结构化数据 / 选股策略）的后端路径仍为 `/settings/api/*`，设计时无需关心。
- 暗色主题作为后续可选变体，非本期交付重点。

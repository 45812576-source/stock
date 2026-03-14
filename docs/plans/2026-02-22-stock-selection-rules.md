# 选股策略规则系统 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 重写设置页的选股策略 Tab，改为"标签筛选式"规则库，预置 ~50 条系统选股规则（纯 AI prompt 模板），支持手动新增/删除，同步更新 stock-recommendation Skill。

**Architecture:** 新建 `stock_selection_rules` 表存储所有规则（系统预置 + 用户自定义），后端提供 CRUD API + 初始化种子接口，前端用分类标签栏 + 卡片列表展示，Skill 文件引用规则库作为 prompt 模板。

**Tech Stack:** Python FastAPI / MySQL / Jinja2 HTML / Tailwind CSS

---

## 规则分类体系（10 大类）

| 分类 key | 分类名 | 规则数 | 说明 |
|----------|--------|--------|------|
| `fundamental_profit` | 基本面·盈利 | 6 | PE/ROE/毛利率/净利率等盈利指标选股 |
| `fundamental_growth` | 基本面·成长 | 6 | 收入增速/利润增速/PEG等成长指标 |
| `fundamental_structure` | 基本面·收入结构 | 5 | 收入集中度/新业务占比/第二曲线等 |
| `valuation` | 估值比较 | 6 | 同行PE最低/PB最低/PS最低/EV-EBITDA等 |
| `capital_flow` | 资金面 | 8 | 国家队/ETF重仓/北向/游资/主力净流入等 |
| `technical` | 技术面 | 6 | 突破新高/均线多头/放量/MACD金叉等 |
| `industry_chain` | 产业链·龙头 | 5 | 龙头属性/细分第一/业务模式最接近等 |
| `event_driven` | 事件驱动 | 5 | 政策受益/业绩超预期/并购重组/解禁等 |
| `risk_control` | 风险控制 | 5 | 高股息/低波动/低负债/现金流充裕等 |
| `smart_money` | 聪明钱 | 4 | 社保重仓/QFII/险资/公募新进等 |

**总计约 56 条系统预置规则**

---

### Task 1: 新建 DB 表 + 种子数据文件

**Files:**
- Create: `config/stock_selection_presets.py`
- Modify: `db/schema_mysql.sql` (追加建表语句)

**Step 1: 在 schema_mysql.sql 末尾追加建表语句**

```sql
CREATE TABLE IF NOT EXISTS stock_selection_rules (
    id INT PRIMARY KEY AUTO_INCREMENT,
    category VARCHAR(50) NOT NULL,
    rule_name VARCHAR(255) NOT NULL,
    definition TEXT NOT NULL,
    is_system INT DEFAULT 0,
    is_active INT DEFAULT 1,
    sort_order INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_rule_name (rule_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

**Step 2: 创建 config/stock_selection_presets.py**

包含 `RULE_CATEGORIES` 字典（分类元数据）和 `PRESET_RULES` 列表（所有预置规则）。每条规则结构：`{"category": str, "rule_name": str, "definition": str}`。

**Step 3: 在本地 MySQL 执行建表**

Run: `cd /Users/liaoxia/stock-analysis-system && python -c "from utils.db_utils import execute_insert; execute_insert('''CREATE TABLE IF NOT EXISTS stock_selection_rules (id INT PRIMARY KEY AUTO_INCREMENT, category VARCHAR(50) NOT NULL, rule_name VARCHAR(255) NOT NULL, definition TEXT NOT NULL, is_system INT DEFAULT 0, is_active INT DEFAULT 1, sort_order INT DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, UNIQUE KEY uq_rule_name (rule_name)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4''', [])"`

---

### Task 2: 后端 API — 规则 CRUD + 种子初始化

**Files:**
- Modify: `routers/settings.py` — 替换投资策略 API 区块

**新增 API 端点：**

| Method | Path | 功能 |
|--------|------|------|
| GET | `/settings/api/selection-rules` | 返回所有活跃规则（含分类元数据） |
| POST | `/settings/api/selection-rules/seed` | 初始化/重置系统预置规则 |
| POST | `/settings/api/selection-rules` | 新增自定义规则 |
| PUT | `/settings/api/selection-rules/{id}` | 更新规则（仅自定义） |
| DELETE | `/settings/api/selection-rules/{id}` | 删除规则（仅自定义） |

**Step 1: 替换 settings.py 中投资策略相关代码**

删除旧的 `save_strategy` / `delete_strategy` / `api_list_strategies` 三个端点，替换为新的选股规则 API。

`settings_strategy_page` 改为查询 `stock_selection_rules` 表 + 分类元数据。

**Step 2: 实现种子初始化逻辑**

`seed` 接口：遍历 `PRESET_RULES`，INSERT IGNORE 到表中（`is_system=1`），返回新增数量。

---

### Task 3: 前端重写 — 标签筛选式 UI

**Files:**
- Modify: `templates/settings.html` — 替换 `{% elif tab == 'strategy' %}` 区块（HTML + JS）

**UI 结构：**

```
┌─────────────────────────────────────────────────────┐
│ 📊 选股策略规则库                    [初始化系统规则] │
│ 系统预置 56 条 · 自定义 3 条                         │
├─────────────────────────────────────────────────────┤
│ [全部] [基本面·盈利] [基本面·成长] [估值比较]        │
│ [资金面] [技术面] [产业链·龙头] [事件驱动] ...       │
├─────────────────────────────────────────────────────┤
│ ┌──────────────────────┐ ┌──────────────────────┐   │
│ │ 🏷 同行PE最低         │ │ 🏷 ROE最高           │   │
│ │ 在同一细分行业内...   │ │ 筛选ROE连续3年...    │   │
│ │ [系统] [基本面·盈利]  │ │ [系统] [基本面·盈利] │   │
│ └──────────────────────┘ └──────────────────────┘   │
│ ...                                                  │
├─────────────────────────────────────────────────────┤
│ ➕ 新增自定义规则                                    │
│ ┌─ 分类: [下拉选择] ─── 名称: [________] ──────┐    │
│ │ 定义: [_________________________________]     │    │
│ │                              [保存]           │    │
│ └───────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

**Step 1: 替换 HTML 模板中 strategy tab 的内容**

**Step 2: 替换 JS 中 strategy 相关函数**

---

### Task 4: 更新 stock-recommendation Skill

**Files:**
- Modify: `~/.claude/skills/stock-recommendation/SKILL.md`

**改动要点：**
- 在 Skill 开头新增"规则库引用"章节，说明系统有 N 条预置选股规则可供 AI 参考
- 将原有的四维评分体系保留为核心方法论
- 新增"规则驱动选股"模式：AI 根据用户选择的规则定义去分析推荐
- 规则定义作为 system prompt 的一部分注入

---

### Task 5: 连接 — strategy 页面路由更新 + 旧表兼容

**Files:**
- Modify: `routers/settings.py` — `settings_strategy_page` 函数
- Modify: `routers/portfolio.py` — 如果引用了旧策略，保持兼容

**Step 1: 更新 settings_strategy_page**

查询 `stock_selection_rules` 表，按分类分组传给模板。

**Step 2: portfolio.py 中旧的 investment_strategies 引用保持不动**

旧表 `investment_strategies` 和 `strategy_stocks` 不删除，portfolio 模块继续使用。新的选股规则是独立的规则库，供 AI 选股时参考。

---

## 执行顺序

Task 1 → Task 2 → Task 3 → Task 4 → Task 5（线性依赖）

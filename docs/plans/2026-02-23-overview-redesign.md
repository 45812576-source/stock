# 每日概览页面重构 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将概览页重构为左4/5（三Tab）+ 右1/5（固定AI分析助理）的左右布局，整合持有组合、大盘资金面、新闻聚合三大模块，支持新闻拖拽到AI助理解读。

**Architecture:**
- 前端：overview.html 重写为左右分栏 + 三Tab结构，右侧复用 portfolio chat 组件
- 后端：overview.py 新增 Tab2/Tab3 所需的 API 端点，Tab2 复用 market.py 已有 API
- 交互：HTML5 Drag & Drop，新闻卡片 draggable → chat drop zone → FOE 正文注入聊天上下文

**Tech Stack:** FastAPI + Jinja2 + Tailwind CSS + HTMX + Plotly.js + HTML5 Drag & Drop API

---

## 整体结构

```
overview.html 新布局:
┌──────────────────────────────────────────┬──────────┐
│  左侧 4/5                                │ 右侧 1/5 │
│  ┌─────┬──────────┬──────────┐           │          │
│  │Tab1 │  Tab2    │  Tab3    │           │ AI分析   │
│  │持有  │  资金面   │  新闻    │           │ 助理     │
│  └─────┴──────────┴──────────┘           │          │
│                                          │ (固定)   │
│  [Tab Content Area]                      │          │
│                                          │ chat     │
│                                          │ messages │
│                                          │          │
│                                          │ [input]  │
└──────────────────────────────────────────┴──────────┘
```

### Tab1: 持有组合信息聚合
延用现有 overview 的自选股 carousel layout，数据源改为默认收藏组(id=1)的股票。
三栏信息：公司新闻+研报 | 产业链+行业新闻+产业研报 | 宏观新闻

### Tab2: 大盘资金面
迁移 market.html 的资金面仪表盘（指数卡片、K线图、资金面图表）。
行业资金热力图 + 资金热度解读完全采用原概览页的实现。

### Tab3: 新闻聚合器
四容器：宏观级 / 行业级 / 个股级 / 风险聚焦
最近3天，各20条，按 importance DESC 排序。
FOE 卡片交互不变，增加拖拽、like到热点、关联portfolio。

---

## Task 1: overview.html 骨架重构 — 左右分栏 + 三Tab + 右侧Chat

**目标：** 把 overview.html 从纵向堆叠改为左右分栏结构，左侧三个 Tab 切换，右侧固定 AI 分析助理面板。此步只搭骨架，Tab 内容区先放占位符。

**Files:**
- Modify: `templates/overview.html` — 重写 `{% block content %}` 和 `{% block extra_styles %}`
- Reference: `templates/portfolio.html:70-129` — 参考其左右分栏 + chat 面板结构

**Step 1: 重写 overview.html 的 block content**

将现有 `{% block content %}` 替换为：

```html
{% block content %}
<div class="flex flex-col h-full overflow-hidden" id="overview-root">

  <!-- Sticky Header -->
  <header class="sticky top-0 z-30 flex items-center justify-between px-6 py-3 bg-background-dark/90 backdrop-blur-md border-b border-border-dark flex-shrink-0">
    <div class="flex items-center gap-4">
      <h2 class="text-xl font-bold">每日概览</h2>
      <span class="px-2 py-0.5 bg-green-500/10 text-green-500 text-[10px] font-bold uppercase rounded flex items-center gap-1">
        <span class="w-1.5 h-1.5 bg-green-500 rounded-full animate-pulse"></span> Live
      </span>
    </div>
    <div class="flex items-center gap-3">
      <input type="date" id="date-picker" value="{{ date }}"
             class="bg-card-dark border border-border-dark rounded-lg text-sm px-4 py-2 text-slate-200 focus:ring-primary focus:border-primary"
             onchange="window.location.href='/overview?date='+this.value">
      <button hx-post="/overview/refresh?date={{ date }}" hx-target="#overview-root" hx-swap="innerHTML"
              hx-indicator="#refresh-spin"
              class="bg-primary text-white px-4 py-2 rounded-lg text-sm font-medium hover:bg-primary/90 transition-colors flex items-center gap-2">
        <svg id="refresh-spin" class="w-4 h-4 htmx-indicator animate-spin" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12a9 9 0 11-6.219-8.56"/></svg>
        <span class="material-icons-round text-sm">refresh</span> 刷新
      </button>
    </div>
  </header>

  <!-- 主体：左右分栏 -->
  <div class="flex flex-1 overflow-hidden">

    <!-- ══ 左侧 4/5：三Tab ══ -->
    <div class="flex-1 flex flex-col overflow-hidden min-w-0">
      <!-- Tab Bar -->
      <div class="flex border-b border-border-dark bg-card-dark/50 flex-shrink-0 px-6">
        <button class="ov-tab active" data-tab="portfolio" onclick="switchOvTab('portfolio')">
          <span class="material-icons-round text-sm">account_balance_wallet</span> 持有组合
        </button>
        <button class="ov-tab" data-tab="capital" onclick="switchOvTab('capital')">
          <span class="material-icons-round text-sm">analytics</span> 大盘资金面
        </button>
        <button class="ov-tab" data-tab="news" onclick="switchOvTab('news')">
          <span class="material-icons-round text-sm">newspaper</span> 新闻聚合
        </button>
      </div>

      <!-- Tab Content -->
      <div class="flex-1 overflow-y-auto" id="ov-tab-content">
        <div id="tab-portfolio" class="ov-tab-pane active p-6">
          <p class="text-slate-500 text-sm text-center py-20">持有组合加载中...</p>
        </div>
        <div id="tab-capital" class="ov-tab-pane hidden p-6">
          <p class="text-slate-500 text-sm text-center py-20">大盘资金面加载中...</p>
        </div>
        <div id="tab-news" class="ov-tab-pane hidden p-6">
          <p class="text-slate-500 text-sm text-center py-20">新闻聚合加载中...</p>
        </div>
      </div>
    </div>

    <!-- ══ 右侧 1/5：AI 分析助理 ══ -->
    <div class="flex-shrink-0 border-l border-border-dark bg-slate-900/50 flex flex-col relative"
         style="width: 320px" id="ai-chat-panel">
      <div class="px-4 py-3 border-b border-border-dark flex items-center gap-2">
        <span class="material-icons-round text-sm text-primary">smart_toy</span>
        <span class="text-xs font-bold text-slate-400">AI 分析助理</span>
      </div>
      <div class="flex-1 overflow-y-auto px-3 py-3" id="ovChatMessages">
        <div class="text-center text-xs text-slate-600 py-4">拖拽新闻到此处，或直接提问</div>
      </div>
      <!-- Drop zone overlay（拖拽时显示） -->
      <div id="chatDropOverlay" class="hidden absolute inset-0 bg-primary/10 border-2 border-dashed border-primary rounded-lg z-10 flex items-center justify-center pointer-events-none">
        <span class="text-primary text-sm font-bold">释放以添加到对话</span>
      </div>
      <!-- Chat Input -->
      <div class="px-3 pb-3 pt-1 border-t border-border-dark">
        <div id="chatContext" class="hidden mb-2 p-2 bg-slate-800/60 rounded-lg text-[10px] text-slate-400 max-h-20 overflow-y-auto relative">
          <button onclick="clearChatContext()" class="absolute top-1 right-1 text-slate-600 hover:text-white">
            <span class="material-icons-round text-xs">close</span>
          </button>
          <span id="chatContextText"></span>
        </div>
        <div class="flex gap-2">
          <input type="text" id="ovChatInput" placeholder="输入问题..."
                 class="flex-1 bg-slate-800/50 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white placeholder-slate-600 focus:border-primary focus:outline-none"
                 onkeydown="if(event.key==='Enter') sendOvChat()">
          <button onclick="sendOvChat()" id="ovChatSendBtn"
                  class="bg-primary/20 text-primary px-3 py-2 rounded-lg text-sm hover:bg-primary/30 transition">
            <span class="material-icons-round text-base">send</span>
          </button>
        </div>
      </div>
    </div>

  </div>
</div>
{% endblock %}
```

**Step 2: 更新 extra_styles，添加 Tab 样式**

在 `{% block extra_styles %}` 中保留现有 carousel/heatmap/insight 样式，新增：

```css
/* ── Overview Tab ── */
.ov-tab { padding: 10px 16px; font-size: 12px; font-weight: 700; color: #64748b; cursor: pointer;
          border-bottom: 2px solid transparent; transition: all 0.2s; display: flex; align-items: center; gap: 6px; }
.ov-tab:hover { color: #e2e8f0; }
.ov-tab.active { color: #135bec; border-bottom-color: #135bec; }
.ov-tab-pane.hidden { display: none; }
```

**Step 3: 添加 Tab 切换 + Chat 占位 JS**

在 `{% block scripts %}` 中：

```javascript
// ── Tab 切换 ──
window.switchOvTab = function(tab) {
  document.querySelectorAll('.ov-tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tab));
  document.querySelectorAll('.ov-tab-pane').forEach(p => {
    p.classList.toggle('active', p.id === 'tab-' + tab);
    p.classList.toggle('hidden', p.id !== 'tab-' + tab);
  });
  if (tab === 'capital' && !window._capitalLoaded) { window._capitalLoaded = true; loadCapitalTab(); }
  if (tab === 'news' && !window._newsLoaded) { window._newsLoaded = true; loadNewsTab(); }
};
function loadCapitalTab() { /* Task 4 填充 */ }
function loadNewsTab() { /* Task 6 填充 */ }

// ── Chat 占位 ──
function sendOvChat() { /* Task 7 填充 */ }
function clearChatContext() {
  document.getElementById('chatContext').classList.add('hidden');
  document.getElementById('chatContextText').textContent = '';
  window._chatContextData = null;
}
```

**Step 4: 验证**

`uvicorn app_web:app --reload --port 8501` → `/overview`
- 左右分栏，三 Tab 可切换
- 右侧 chat 面板固定
- 现有 header 日期选择和刷新按钮正常

**Step 5: Commit**

```bash
git add templates/overview.html
git commit -m "refactor: overview skeleton - left/right split, 3 tabs, AI chat panel"
```

---

## Task 2: Tab1 持有组合 — 后端数据改造

**目标：** 修改 overview.py 的数据查询，Tab1 数据源改为默认收藏组(id=1)的股票，并为三栏信息（公司/产业/宏观）提供结构化数据。

**Files:**
- Modify: `routers/overview.py` — 修改 `get_watchlist_alerts()` 和 `overview_page()` 路由

**Step 1: 修改 get_watchlist_alerts() 数据源**

当前逻辑是查 `show_on_overview=1` 的多个 list。改为固定读 id=1（默认收藏组）：

```python
def get_portfolio_holdings(date_str: str) -> list:
    """从默认收藏组(id=1)取出股票，查询行情和三栏新闻"""
    try:
        stocks = execute_query(
            """SELECT DISTINCT wls.stock_code, wls.stock_name
                FROM watchlist_list_stocks wls
                WHERE wls.list_id = 1 AND wls.status='active'
                LIMIT 20""",
        ) or []
        stocks = [dict(r) for r in stocks]

        for s in stocks:
            # 行情 + 10日价格历史（复用现有逻辑）
            daily = execute_query(
                "SELECT change_pct, close, volume, amount, trade_date FROM stock_daily WHERE stock_code=%s ORDER BY trade_date DESC LIMIT 10",
                [s["stock_code"]],
            )
            if daily:
                s["market"] = {k: v for k, v in daily[0].items() if k != 'trade_date'}
                s["price_history"] = [float(d["close"]) for d in reversed(daily) if d.get("close") is not None]
            else:
                s["market"] = {}
                s["price_history"] = []

            # ── 第一栏：公司新闻+公告+财报+公司研报 ──
            s["company_news"] = _get_company_news(s["stock_code"])

            # ── 第二栏：产业链+行业新闻+产业研报 ──
            s["industry_news"] = _get_industry_news(s["stock_code"])

            # ── 第三栏：宏观新闻 ──
            s["macro_news"] = _get_macro_news_for_stock(s["stock_code"])

        return stocks
    except Exception:
        return []
```

**Step 2: 添加三栏数据查询辅助函数**

```python
def _get_company_news(stock_code: str) -> list:
    """公司公告+财报+公司研报"""
    rows = execute_query("""
        SELECT cs.id, cs.summary, cs.fact_summary, cs.opinion_summary,
               cs.evidence_assessment, cs.info_gaps, cs.doc_type, et.publish_time
        FROM content_summaries cs
        JOIN extracted_texts et ON cs.extracted_text_id = et.id
        JOIN stock_mentions sm ON sm.extracted_text_id = et.id
        WHERE sm.stock_code = %s
          AND cs.doc_type IN ('announcement','financial_report','research_report')
          AND et.publish_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        ORDER BY et.publish_time DESC LIMIT 5
    """, [stock_code])
    return [dict(r) for r in (rows or [])]


def _get_industry_news(stock_code: str) -> list:
    """产业链+行业+主题新闻+产业研报"""
    # 先查股票所属行业
    ind = execute_query(
        "SELECT industry_l1, industry_l2 FROM stock_info WHERE stock_code=%s",
        [stock_code],
    )
    if not ind:
        return []
    ind_names = [v for v in [ind[0].get("industry_l1"), ind[0].get("industry_l2")] if v]
    if not ind_names:
        return []
    placeholders = ",".join(["%s"] * len(ind_names))
    rows = execute_query(f"""
        SELECT cs.id, cs.summary, cs.fact_summary, cs.opinion_summary,
               cs.evidence_assessment, cs.info_gaps, cs.doc_type, et.publish_time
        FROM content_summaries cs
        JOIN extracted_texts et ON cs.extracted_text_id = et.id
        WHERE cs.doc_type IN ('feature_news','flash_news','research_report','strategy_report')
          AND et.publish_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
          AND (cs.summary LIKE CONCAT('%%', %s, '%%')
               OR EXISTS (SELECT 1 FROM item_industries ii
                          JOIN cleaned_items ci ON ii.cleaned_item_id = ci.id
                          WHERE ii.industry_name IN ({placeholders})
                            AND ci.summary = cs.summary))
        ORDER BY et.publish_time DESC LIMIT 5
    """, [ind_names[0]] + ind_names)
    return [dict(r) for r in (rows or [])]


def _get_macro_news_for_stock(stock_code: str) -> list:
    """影响该公司和所在行业的宏观新闻"""
    rows = execute_query("""
        SELECT cs.id, cs.summary, cs.fact_summary, cs.opinion_summary,
               cs.evidence_assessment, cs.info_gaps, cs.doc_type, et.publish_time
        FROM content_summaries cs
        JOIN extracted_texts et ON cs.extracted_text_id = et.id
        WHERE cs.doc_type IN ('policy_doc','data_release','market_commentary','strategy_report')
          AND et.publish_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        ORDER BY et.publish_time DESC LIMIT 5
    """)
    return [dict(r) for r in (rows or [])]
```

**Step 3: 更新路由 context**

在 `overview_page()` 中将 `watchlist_alerts` 改为 `portfolio_holdings`：

```python
ctx = {
    ...
    "portfolio_holdings": get_portfolio_holdings(date_str),
    ...
}
```

同步更新 `refresh_dashboards()` 中的 context。

**Step 4: 验证**

重启服务，访问 `/overview`，检查后端日志无报错，`portfolio_holdings` 数据正常返回。

**Step 5: Commit**

```bash
git add routers/overview.py
git commit -m "feat: overview Tab1 backend - portfolio holdings with 3-column news data"
```

---

## Task 3: Tab1 持有组合 — 前端渲染

**目标：** 在 Tab1 中渲染持有组合的三栏信息，延用现有 carousel 布局（左侧股票列表 + 右侧详情面板），详情面板改为三栏。

**Files:**
- Modify: `templates/overview.html` — 填充 `#tab-portfolio` 内容

**Step 1: 替换 tab-portfolio 占位内容**

将 `#tab-portfolio` 的占位符替换为 Jinja2 模板，复用现有 carousel 结构：

```html
<div id="tab-portfolio" class="ov-tab-pane active p-6">
  {% if portfolio_holdings %}
  <div class="wl-container">
    <!-- 左侧：股票列表（自动滚动） -->
    <div class="wl-list">
      <div class="wl-list-inner" id="wl-scroll">
        {% for s in portfolio_holdings %}
        {% set chg = s.market.get('change_pct') %}
        {% set chg_class = 'text-red-500' if chg and chg > 0 else 'text-green-500' if chg and chg < 0 else 'text-slate-400' %}
        <div class="wl-list-item" data-idx="{{ loop.index0 }}" onclick="wlSelect({{ loop.index0 }})">
          <div class="wl-list-item-name">{{ s.stock_name or s.stock_code }}</div>
          <div class="wl-list-item-price {{ chg_class }}">
            {% if s.market.get('close') %}¥{{ '%.2f'|format(s.market.close) }}{% endif %}
            {% if chg is not none %}<span class="text-[9px]">{{ '%+.2f'|format(chg) }}%</span>{% endif %}
          </div>
          {% if s.price_history %}
          <svg class="wl-list-item-chart" viewBox="0 0 80 24" preserveAspectRatio="none" data-prices="{{ s.price_history | tojson }}">
            <polyline class="sparkline-line" fill="none" stroke="{{ '#ef4444' if chg and chg >= 0 else '#22c55e' }}" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round" points=""/>
          </svg>
          {% endif %}
        </div>
        {% endfor %}
        <!-- 复制一份用于无缝循环 -->
        {% for s in portfolio_holdings %}
        {% set chg = s.market.get('change_pct') %}
        {% set chg_class = 'text-red-500' if chg and chg > 0 else 'text-green-500' if chg and chg < 0 else 'text-slate-400' %}
        <div class="wl-list-item" data-idx="{{ loop.index0 }}" onclick="wlSelect({{ loop.index0 }})">
          <div class="wl-list-item-name">{{ s.stock_name or s.stock_code }}</div>
          <div class="wl-list-item-price {{ chg_class }}">
            {% if s.market.get('close') %}¥{{ '%.2f'|format(s.market.close) }}{% endif %}
            {% if chg is not none %}<span class="text-[9px]">{{ '%+.2f'|format(chg) }}%</span>{% endif %}
          </div>
          {% if s.price_history %}
          <svg class="wl-list-item-chart" viewBox="0 0 80 24" preserveAspectRatio="none" data-prices="{{ s.price_history | tojson }}">
            <polyline class="sparkline-line" fill="none" stroke="{{ '#ef4444' if chg and chg >= 0 else '#22c55e' }}" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round" points=""/>
          </svg>
          {% endif %}
        </div>
        {% endfor %}
      </div>
    </div>
    <!-- 右侧：三栏详情面板 -->
    <div class="wl-detail" id="wl-detail">
      <div class="carousel-expanded-card" id="wl-detail-content">
        <div class="wl-detail-empty text-sm py-12">点击左侧股票查看详情</div>
      </div>
    </div>
  </div>
  <script id="portfolio-data" type="application/json">{{ portfolio_holdings | tojson }}</script>
  {% else %}
  <div class="text-center text-slate-500 text-sm py-12">默认收藏组暂无股票，请先在个股研究中添加</div>
  {% endif %}
</div>
```

**Step 2: 更新 wlSelect() JS 渲染三栏详情**

修改 JS 中的 `wlSelect` 函数，详情面板改为三栏（公司/产业/宏观），每栏用 FOE 卡片展示：

```javascript
(function() {
  var dataEl = document.getElementById('portfolio-data');
  if (!dataEl) return;
  var stocks = JSON.parse(dataEl.textContent);
  if (!stocks.length) return;
  var detailContent = document.getElementById('wl-detail-content');

  function escHtml(s) { if (!s) return ''; var d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

  function renderFoeList(items, label) {
    if (!items || !items.length) return '<p class="text-[9px] text-slate-600">暂无' + label + '</p>';
    var html = '';
    items.forEach(function(item, i) {
      var summary = item.summary || item.fact_summary || '';
      var dragData = JSON.stringify({summary: summary, fact: item.fact_summary||'', opinion: item.opinion_summary||'', evidence: item.evidence_assessment||'', id: item.id});
      html += '<div class="foe-card-mini mb-2 p-2 bg-slate-800/40 rounded-lg cursor-grab hover:bg-slate-800/60 transition" draggable="true" data-foe=\'' + escHtml(dragData) + '\'>';
      html += '<p class="text-[10px] text-slate-300 leading-relaxed line-clamp-2">' + escHtml(summary.slice(0, 120)) + '</p>';
      if (item.doc_type) html += '<span class="text-[8px] text-slate-600 mt-1 inline-block">' + escHtml(item.doc_type) + '</span>';
      html += '</div>';
    });
    return html;
  }

  window.wlSelect = function(idx) {
    document.querySelectorAll('.wl-list-item').forEach(function(el) {
      el.classList.toggle('active', parseInt(el.dataset.idx) === idx);
    });
    var s = stocks[idx], m = s.market || {}, chg = m.change_pct;
    var chgClass = chg > 0 ? 'text-red-500' : chg < 0 ? 'text-green-500' : 'text-slate-400';
    var chgStr = chg != null ? (chg > 0 ? '+' : '') + chg.toFixed(2) + '%' : '';

    var html = '<div class="flex items-center gap-3 mb-3">';
    html += '<a href="/stock/' + s.stock_code + '" class="text-sm font-bold hover:text-primary">' + escHtml(s.stock_name || s.stock_code) + '</a>';
    html += '<span class="text-xs text-slate-500">' + s.stock_code + '</span>';
    if (chgStr) html += '<span class="text-sm font-bold ' + chgClass + '">' + chgStr + '</span>';
    html += '</div>';

    // 三栏
    html += '<div class="grid grid-cols-3 gap-3 max-h-[calc(100%-40px)] overflow-y-auto">';
    html += '<div><div class="text-[10px] text-amber-400 font-bold mb-2">▸ 公司新闻/研报</div>' + renderFoeList(s.company_news, '公司新闻') + '</div>';
    html += '<div><div class="text-[10px] text-blue-400 font-bold mb-2">▸ 产业/行业</div>' + renderFoeList(s.industry_news, '行业新闻') + '</div>';
    html += '<div><div class="text-[10px] text-cyan-400 font-bold mb-2">▸ 宏观</div>' + renderFoeList(s.macro_news, '宏观新闻') + '</div>';
    html += '</div>';

    detailContent.innerHTML = html;
    // 绑定拖拽事件
    bindDragEvents();
  };

  if (stocks.length) wlSelect(0);
})();
```

**Step 3: 验证**

访问 `/overview`，Tab1 应显示：
- 左侧股票列表自动滚动
- 点击股票，右侧显示三栏信息
- 每个新闻卡片有 `draggable="true"` 属性

**Step 4: Commit**

```bash
git add templates/overview.html
git commit -m "feat: overview Tab1 frontend - 3-column portfolio holdings with FOE cards"
```

---

## Task 4: Tab2 大盘资金面 — 前端迁移

**目标：** 将 market.html 的资金面仪表盘迁移到 Tab2，行业资金热力图 + 解读完全采用原概览页的实现（服务端渲染）。图表部分通过 JS 懒加载调用 market.py 已有 API。

**Files:**
- Modify: `templates/overview.html` — 填充 `#tab-capital` 内容 + 添加 JS
- Reference: `templates/market.html:93-158` — 资金面仪表盘 HTML
- Reference: `templates/overview.html` 原行业资金热度 section

**Step 1: 填充 tab-capital HTML**

将 `#tab-capital` 占位替换为：

```html
<div id="tab-capital" class="ov-tab-pane hidden p-6">

  <!-- 指数卡片 -->
  <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 mb-4" id="capIndexCards">
    {% for idx in index_list %}
    <div class="card p-3 hover:border-primary transition-colors cursor-pointer" onclick="selectCapIndex('{{ idx.code }}')">
      <div class="flex items-center justify-between mb-1">
        <span class="text-[10px] font-bold text-slate-400">{{ idx.name }}</span>
        <span class="w-1.5 h-1.5 rounded-full" style="background:{{ idx.color }}"></span>
      </div>
      <p class="text-sm font-extrabold text-white cap-idx-price" id="cap-idx-{{ idx.code }}">--</p>
      <span class="text-[10px] font-bold text-slate-500 cap-idx-pct" id="cap-pct-{{ idx.code }}">--</span>
    </div>
    {% endfor %}
  </div>

  <!-- 行业资金热力图 + 解读（服务端渲染，完全迁移自原概览页） -->
  <div class="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-4">
    <div class="lg:col-span-2 card flex flex-col">
      <div class="p-3 border-b border-border-dark flex justify-between items-center">
        <div class="flex items-center gap-2">
          <span class="material-icons-round text-amber-500 text-sm">grid_view</span>
          <h4 class="font-bold text-xs">行业资金热度</h4>
        </div>
        <div class="flex gap-1" id="cap-heat-tabs">
          <button class="heat-tab active" data-mode="net" onclick="switchHeatTab('net')">净流入/出</button>
          <button class="heat-tab" data-mode="gross" onclick="switchHeatTab('gross')">毛流入/出</button>
        </div>
      </div>
      <div class="p-3 overflow-x-auto flex-1">
        <!-- 复用原概览页的 industry_heat Jinja2 渲染 -->
        {% include "partials/_industry_heatmap.html" %}
      </div>
    </div>
    <div class="card flex flex-col" style="max-height: 500px;">
      <div class="p-3 border-b border-border-dark flex items-center gap-2">
        <span class="material-icons-round text-cyan-500 text-sm">insights</span>
        <h4 class="font-bold text-xs">资金热度解读</h4>
      </div>
      <div class="flex-1 p-3 insight-scroll overflow-y-auto">
        {% if capital_insight %}
        <div class="space-y-2">
          {% for ins in capital_insight %}
          <div class="p-2 rounded-lg {{ 'bg-red-500/5 border border-red-500/10' if ins.sentiment == 'negative' else 'bg-green-500/5 border border-green-500/10' if ins.sentiment == 'positive' else 'bg-slate-500/5 border border-slate-500/10' }}">
            <div class="flex items-center gap-2 mb-1">
              <span class="text-[8px] font-bold px-1 py-0.5 rounded {{ 'bg-amber-500/20 text-amber-400' if ins.category == '宏观资金面' else 'bg-blue-500/20 text-blue-400' }}">{{ ins.category }}</span>
            </div>
            {% if ins.fact %}<p class="text-[10px] text-slate-300 leading-relaxed">{{ ins.fact[:120] }}</p>{% endif %}
            {% if ins.opinion %}<p class="text-[10px] text-slate-500 italic">{{ ins.opinion[:100] }}</p>{% endif %}
          </div>
          {% endfor %}
        </div>
        {% else %}
        <p class="text-xs text-slate-600 text-center py-4">暂无解读</p>
        {% endif %}
      </div>
    </div>
  </div>

  <!-- 资金面图表（JS 懒加载） -->
  <div class="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">
    <div class="card p-3"><h4 class="text-[10px] font-bold text-slate-400 mb-2">成交额 + 主力资金</h4><div id="capFlowChart" style="height:180px;"></div></div>
    <div class="card p-3"><h4 class="text-[10px] font-bold text-slate-400 mb-2">融资余额</h4><div id="capMarginChart2" style="height:180px;"></div></div>
    <div class="card p-3"><h4 class="text-[10px] font-bold text-slate-400 mb-2">南向资金</h4><div id="capSouthChart2" style="height:180px;"></div></div>
    <div class="card p-3"><h4 class="text-[10px] font-bold text-slate-400 mb-2">ETF 成交量</h4><div id="capEtfChart2" style="height:180px;"></div></div>
  </div>
</div>
```

**Step 2: 抽取行业热力图为 partial**

将原 overview.html 中 `<!-- 净流入/出 视图 -->` 到 `<!-- 毛流入/出 视图 -->` 结束的热力图 table HTML 抽取到 `templates/partials/_industry_heatmap.html`，这样 Tab2 可以 `{% include %}` 复用。

**Step 3: 添加 loadCapitalTab() JS**

```javascript
async function loadCapitalTab() {
  // 加载指数
  try {
    const data = await fetch('/market/api/indices?days=20').then(r => r.json());
    for (const idx of data) {
      const priceEl = document.getElementById('cap-idx-' + idx.code);
      const pctEl = document.getElementById('cap-pct-' + idx.code);
      if (priceEl) priceEl.textContent = idx.latest != null ? idx.latest.toLocaleString() : '--';
      if (pctEl && idx.change_pct != null) {
        pctEl.textContent = (idx.change_pct >= 0 ? '+' : '') + idx.change_pct + '%';
        pctEl.className = 'text-[10px] font-bold cap-idx-pct ' + (idx.change_pct >= 0 ? 'text-red-400' : 'text-emerald-400');
      }
    }
  } catch(e) {}

  // 加载资金面图表（复用 market.py API）
  const qs = '?start=' + new Date(Date.now() - 30*86400000).toISOString().slice(0,10) + '&end=' + new Date().toISOString().slice(0,10);
  Promise.all([
    loadCapChart('/market/api/capital/market-flow' + qs, 'capFlowChart', 'flow'),
    loadCapChart('/market/api/capital/margin' + qs, 'capMarginChart2', 'margin'),
    loadCapChart('/market/api/capital/southbound' + qs, 'capSouthChart2', 'south'),
    loadCapChart('/market/api/capital/etf-shares' + qs, 'capEtfChart2', 'etf'),
  ]);
}

async function loadCapChart(url, elId, type) {
  // 复用 market.html 中的图表渲染逻辑，适配新 element ID
  // 具体 Plotly 代码从 market.html 的 loadCapMarketFlow/loadCapMargin/loadCapSouthbound/loadCapEtf 复制
  try {
    const data = await fetch(url).then(r => r.json());
    if (!data || (Array.isArray(data) && !data.length) || (!Array.isArray(data) && !Object.keys(data).length)) return;
    const plotOpts = { margin:{t:5,b:25,l:45,r:10}, paper_bgcolor:'rgba(0,0,0,0)', plot_bgcolor:'rgba(0,0,0,0)',
                       font:{color:'#94a3b8',size:9}, xaxis:{gridcolor:'rgba(100,116,139,0.1)'},
                       yaxis:{gridcolor:'rgba(100,116,139,0.1)'}, showlegend:false };
    // type-specific rendering...
    if (type === 'flow') {
      const dates = data.map(d=>d.trade_date), amounts = data.map(d=>d.total_amount/1e8), nets = data.map(d=>d.main_net/1e8);
      Plotly.newPlot(elId, [
        {x:dates,y:amounts,type:'bar',name:'成交额(亿)',marker:{color:'rgba(59,130,246,0.5)'}},
        {x:dates,y:nets,type:'scatter',mode:'lines',name:'主力净流入(亿)',line:{color:'#ef4444',width:1.5},yaxis:'y2'},
      ], {...plotOpts, yaxis2:{overlaying:'y',side:'right',showgrid:false}, bargap:0.3}, {displayModeBar:false,responsive:true});
    } else if (type === 'margin') {
      const dates = data.map(d=>d.trade_date), bal = data.map(d=>(d.margin_balance||0)/1e8);
      Plotly.newPlot(elId, [{x:dates,y:bal,type:'scatter',mode:'lines+markers',line:{color:'#f97316',width:1.5},marker:{size:3}}],
        plotOpts, {displayModeBar:false,responsive:true});
    } else if (type === 'south') {
      const dates = data.map(d=>d.trade_date), nets = data.map(d=>d.net_buy/1e8);
      Plotly.newPlot(elId, [{x:dates,y:nets,type:'bar',marker:{color:nets.map(v=>v>=0?'#ef4444':'#10b981')}}],
        {...plotOpts, bargap:0.3}, {displayModeBar:false,responsive:true});
    } else if (type === 'etf') {
      const traces = Object.entries(data).map(([code, etf]) => ({
        x:etf.data.map(d=>d.trade_date), y:etf.data.map(d=>d.amount/1e8),
        type:'scatter', mode:'lines', name:etf.name, line:{width:1.5},
      }));
      Plotly.newPlot(elId, traces, {...plotOpts, showlegend:true, legend:{font:{size:8},orientation:'h',y:-0.2}},
        {displayModeBar:false,responsive:true});
    }
  } catch(e) { console.error('Cap chart load failed:', type, e); }
}
```

**Step 4: 更新 overview.py 路由 context**

确保 `overview_page()` 传入 `index_list`（从 market.py 导入 INDEX_LIST）：

```python
from routers.market import INDEX_LIST

# 在 ctx 中添加：
"index_list": INDEX_LIST,
```

**Step 5: 验证**

切换到 Tab2，应看到：
- 指数卡片加载实时数据
- 行业资金热力图（服务端渲染）正常显示
- 资金热度解读面板正常
- 四个资金面图表懒加载

**Step 6: Commit**

```bash
git add templates/overview.html templates/partials/_industry_heatmap.html routers/overview.py
git commit -m "feat: overview Tab2 - capital dashboard with heatmap migration"
```

---

## Task 5: Tab3 新闻聚合器 — 后端 API

**目标：** 新增 `/overview/api/news-feed` API，返回四个容器（宏观级/行业级/个股级/风险聚焦）的新闻数据，最近3天，各20条，按 importance DESC 排序。

**Files:**
- Modify: `routers/overview.py` — 新增 API 端点

**Step 1: 添加新闻聚合 API**

```python
@router.get("/api/news-feed", response_class=JSONResponse)
def api_news_feed(days: int = 3):
    """新闻聚合器：四容器，最近 days 天，各20条，按 importance DESC"""
    from fastapi.responses import JSONResponse
    try:
        base_select = """
            SELECT cs.id, cs.doc_type, cs.summary, cs.fact_summary,
                   cs.opinion_summary, cs.evidence_assessment, cs.info_gaps,
                   et.publish_time,
                   ci.importance, ci.sentiment, ci.event_type
            FROM content_summaries cs
            JOIN extracted_texts et ON cs.extracted_text_id = et.id
            LEFT JOIN cleaned_items ci ON ci.summary = cs.summary
            WHERE et.publish_time >= DATE_SUB(NOW(), INTERVAL %s DAY)
        """

        # 宏观级
        macro = execute_query(f"""
            {base_select}
              AND cs.doc_type IN ('policy_doc','data_release','market_commentary','strategy_report')
            ORDER BY COALESCE(ci.importance, 3) DESC, et.publish_time DESC LIMIT 20
        """, [days]) or []

        # 行业级
        industry = execute_query(f"""
            {base_select}
              AND cs.doc_type IN ('feature_news','flash_news','research_report')
              AND EXISTS (SELECT 1 FROM item_industries ii
                          JOIN cleaned_items ci2 ON ii.cleaned_item_id = ci2.id
                          WHERE ci2.summary = cs.summary)
            ORDER BY COALESCE(ci.importance, 3) DESC, et.publish_time DESC LIMIT 20
        """, [days]) or []

        # 个股级
        stock = execute_query(f"""
            {base_select}
              AND cs.doc_type IN ('announcement','financial_report','feature_news','flash_news')
              AND EXISTS (SELECT 1 FROM stock_mentions sm
                          WHERE sm.extracted_text_id = cs.extracted_text_id)
            ORDER BY COALESCE(ci.importance, 3) DESC, et.publish_time DESC LIMIT 20
        """, [days]) or []

        # 风险聚焦：负面情绪 + 风险关键词
        risk = execute_query(f"""
            {base_select}
              AND (ci.sentiment = 'negative'
                   OR cs.summary LIKE '%%风险%%' OR cs.summary LIKE '%%下跌%%'
                   OR cs.summary LIKE '%%利空%%' OR cs.summary LIKE '%%减持%%'
                   OR cs.summary LIKE '%%违约%%' OR cs.summary LIKE '%%退市%%')
            ORDER BY COALESCE(ci.importance, 3) DESC, et.publish_time DESC LIMIT 20
        """, [days]) or []

        def _serialize(rows):
            result = []
            for r in rows:
                d = dict(r)
                # publish_time 转字符串
                if d.get("publish_time") and hasattr(d["publish_time"], "strftime"):
                    d["publish_time"] = d["publish_time"].strftime("%Y-%m-%d %H:%M")
                result.append(d)
            return result

        return {
            "macro": _serialize(macro),
            "industry": _serialize(industry),
            "stock": _serialize(stock),
            "risk": _serialize(risk),
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"macro": [], "industry": [], "stock": [], "risk": []}
```

**Step 2: 验证**

`curl http://localhost:8501/overview/api/news-feed | python -m json.tool | head -50`

应返回四个数组，每个最多20条，按 importance 降序。

**Step 3: Commit**

```bash
git add routers/overview.py
git commit -m "feat: overview news-feed API - 4 containers with importance ranking"
```

---

## Task 6: Tab3 新闻聚合器 — 前端渲染

**目标：** Tab3 四容器 UI，FOE 卡片交互保持一致，每条新闻可拖拽、可 like 到热点、可关联 portfolio。

**Files:**
- Modify: `templates/overview.html` — 填充 `#tab-news` + 添加 JS

**Step 1: 填充 tab-news HTML**

```html
<div id="tab-news" class="ov-tab-pane hidden p-6">
  <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
    <!-- 宏观级 -->
    <div class="card">
      <div class="px-3 py-2 border-b border-border-dark flex items-center gap-2">
        <span class="material-icons-round text-blue-400 text-sm">public</span>
        <h4 class="text-xs font-bold">宏观级</h4>
        <span class="text-[9px] text-slate-600 ml-auto" id="news-macro-count">0</span>
      </div>
      <div class="p-3 max-h-[60vh] overflow-y-auto" id="news-macro-list"></div>
    </div>
    <!-- 行业级 -->
    <div class="card">
      <div class="px-3 py-2 border-b border-border-dark flex items-center gap-2">
        <span class="material-icons-round text-amber-400 text-sm">factory</span>
        <h4 class="text-xs font-bold">行业级</h4>
        <span class="text-[9px] text-slate-600 ml-auto" id="news-industry-count">0</span>
      </div>
      <div class="p-3 max-h-[60vh] overflow-y-auto" id="news-industry-list"></div>
    </div>
    <!-- 个股级 -->
    <div class="card">
      <div class="px-3 py-2 border-b border-border-dark flex items-center gap-2">
        <span class="material-icons-round text-green-400 text-sm">trending_up</span>
        <h4 class="text-xs font-bold">个股级</h4>
        <span class="text-[9px] text-slate-600 ml-auto" id="news-stock-count">0</span>
      </div>
      <div class="p-3 max-h-[60vh] overflow-y-auto" id="news-stock-list"></div>
    </div>
    <!-- 风险聚焦 -->
    <div class="card">
      <div class="px-3 py-2 border-b border-border-dark flex items-center gap-2">
        <span class="material-icons-round text-red-400 text-sm">warning</span>
        <h4 class="text-xs font-bold">风险聚焦</h4>
        <span class="text-[9px] text-slate-600 ml-auto" id="news-risk-count">0</span>
      </div>
      <div class="p-3 max-h-[60vh] overflow-y-auto" id="news-risk-list"></div>
    </div>
  </div>
</div>
```

**Step 2: 添加 loadNewsTab() + 渲染函数**

```javascript
async function loadNewsTab() {
  try {
    const data = await fetch('/overview/api/news-feed?days=3').then(r => r.json());
    renderNewsList('news-macro-list', 'news-macro-count', data.macro || []);
    renderNewsList('news-industry-list', 'news-industry-count', data.industry || []);
    renderNewsList('news-stock-list', 'news-stock-count', data.stock || []);
    renderNewsList('news-risk-list', 'news-risk-count', data.risk || []);
    bindDragEvents(); // 绑定拖拽
  } catch(e) { console.error('News feed load failed:', e); }
}

function renderNewsList(listId, countId, items) {
  document.getElementById(countId).textContent = items.length + '条';
  const el = document.getElementById(listId);
  if (!items.length) { el.innerHTML = '<p class="text-[9px] text-slate-600 text-center py-4">暂无</p>'; return; }

  el.innerHTML = items.map(function(item, i) {
    var summary = item.summary || item.fact_summary || '';
    var dragData = JSON.stringify({
      id: item.id, summary: summary,
      fact: item.fact_summary || '', opinion: item.opinion_summary || '',
      evidence: item.evidence_assessment || '', info_gaps: item.info_gaps || ''
    });
    var importanceBadge = item.importance ? '<span class="text-[8px] px-1 py-0.5 rounded bg-primary/20 text-primary">' + item.importance + '</span>' : '';
    var timeBadge = item.publish_time ? '<span class="text-[8px] text-slate-600">' + item.publish_time.slice(0, 10) + '</span>' : '';
    var docBadge = item.doc_type ? '<span class="text-[8px] px-1 py-0.5 rounded bg-slate-700/60 text-slate-400">' + item.doc_type + '</span>' : '';

    return '<div class="foe-card card mb-2 overflow-hidden" draggable="true" data-foe=\'' + _escAttr(dragData) + '\'>' +
      '<div class="foe-summary flex items-start gap-2 px-3 py-2 cursor-pointer hover:bg-slate-800/40 transition-colors" onclick="toggleFoeCard(this)">' +
        '<p class="flex-1 text-[11px] text-slate-200 leading-snug line-clamp-2">' + _escHtml(summary) + '</p>' +
        '<div class="flex items-center gap-1 flex-shrink-0">' + importanceBadge + docBadge + timeBadge +
          '<span class="material-icons-round foe-chevron text-slate-500 text-sm transition-transform">expand_more</span>' +
        '</div>' +
      '</div>' +
      '<div class="foe-detail hidden border-t border-slate-800 px-3 py-2 space-y-2">' +
        (item.fact_summary ? '<div><span class="text-[9px] font-bold text-amber-400">事实</span><p class="text-[10px] text-slate-300 mt-0.5">' + _escHtml(item.fact_summary) + '</p></div>' : '') +
        (item.opinion_summary ? '<div><span class="text-[9px] font-bold text-blue-400">观点</span><p class="text-[10px] text-slate-300 mt-0.5">' + _escHtml(item.opinion_summary) + '</p></div>' : '') +
        (item.evidence_assessment ? '<div><span class="text-[9px] font-bold text-emerald-400">证据</span><p class="text-[10px] text-slate-400 mt-0.5">' + _escHtml(item.evidence_assessment) + '</p></div>' : '') +
        (item.info_gaps ? '<div><span class="text-[9px] font-bold text-violet-400">信息缺口</span><p class="text-[10px] text-slate-500 mt-0.5 italic">' + _escHtml(item.info_gaps) + '</p></div>' : '') +
        '<div class="flex gap-2 pt-1 border-t border-slate-800/50">' +
          '<button onclick="likeToHotspot(' + item.id + ')" class="text-[9px] text-slate-500 hover:text-amber-400 flex items-center gap-0.5"><span class="material-icons-round text-xs">favorite_border</span>收藏到热点</button>' +
          '<button onclick="linkToPortfolio(' + item.id + ')" class="text-[9px] text-slate-500 hover:text-primary flex items-center gap-0.5"><span class="material-icons-round text-xs">link</span>关联项目</button>' +
        '</div>' +
      '</div>' +
    '</div>';
  }).join('');
}

function _escHtml(s) { if (!s) return ''; var d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function _escAttr(s) { return s.replace(/'/g, '&#39;').replace(/"/g, '&quot;'); }

// like 到热点 + 关联 portfolio 的占位函数
function likeToHotspot(newsId) { showToast('已收藏到热点（待实现完整逻辑）'); }
function linkToPortfolio(newsId) { showToast('关联项目（待实现完整逻辑）'); }
```

**Step 3: 验证**

切换到 Tab3，应看到四个容器各自加载新闻，FOE 卡片可展开/折叠，有拖拽属性。

**Step 4: Commit**

```bash
git add templates/overview.html
git commit -m "feat: overview Tab3 - news aggregator with 4 containers"
```

---

## Task 7: 拖拽交互 + AI Chat 集成

**目标：** 实现 HTML5 Drag & Drop，新闻卡片拖到右侧 chat 面板释放后，FOE 正文注入聊天上下文，用户可针对新闻提问。Chat 复用 portfolio chat API（project_id=1，即默认收藏组）。

**Files:**
- Modify: `templates/overview.html` — 添加拖拽 + chat JS

**Step 1: 拖拽事件绑定**

```javascript
// ── Drag & Drop ──
function bindDragEvents() {
  document.querySelectorAll('[draggable="true"][data-foe]').forEach(function(el) {
    el.addEventListener('dragstart', function(e) {
      e.dataTransfer.setData('text/plain', el.getAttribute('data-foe'));
      e.dataTransfer.effectAllowed = 'copy';
      document.getElementById('chatDropOverlay').classList.remove('hidden');
    });
    el.addEventListener('dragend', function() {
      document.getElementById('chatDropOverlay').classList.add('hidden');
    });
  });
}

// Chat panel drop zone
(function() {
  var panel = document.getElementById('ai-chat-panel');
  panel.addEventListener('dragover', function(e) { e.preventDefault(); e.dataTransfer.dropEffect = 'copy'; });
  panel.addEventListener('dragenter', function(e) { e.preventDefault(); document.getElementById('chatDropOverlay').classList.remove('hidden'); });
  panel.addEventListener('dragleave', function(e) {
    if (!panel.contains(e.relatedTarget)) document.getElementById('chatDropOverlay').classList.add('hidden');
  });
  panel.addEventListener('drop', function(e) {
    e.preventDefault();
    document.getElementById('chatDropOverlay').classList.add('hidden');
    try {
      var data = JSON.parse(e.dataTransfer.getData('text/plain'));
      // 组装 FOE 上下文
      var ctx = '';
      if (data.fact) ctx += '【事实】' + data.fact + '\n';
      if (data.opinion) ctx += '【观点】' + data.opinion + '\n';
      if (data.evidence) ctx += '【证据】' + data.evidence + '\n';
      if (data.info_gaps) ctx += '【信息缺口】' + data.info_gaps + '\n';
      if (!ctx && data.summary) ctx = data.summary;

      // 显示上下文预览
      window._chatContextData = ctx;
      document.getElementById('chatContextText').textContent = ctx.slice(0, 200) + (ctx.length > 200 ? '...' : '');
      document.getElementById('chatContext').classList.remove('hidden');
      document.getElementById('ovChatInput').focus();
      document.getElementById('ovChatInput').placeholder = '针对这条新闻提问...';
    } catch(err) { console.error('Drop parse error:', err); }
  });
})();
```

**Step 2: Chat 发送 + 轮询（复用 portfolio chat API）**

```javascript
var _ovChatProjectId = 1; // 默认收藏组
var _ovChatPollTimer = null;

function appendOvChatMsg(role, text, id, isPending) {
  var el = document.getElementById('ovChatMessages');
  var msgId = id ? ' id="msg-' + id + '"' : '';
  var cls = role === 'user' ? 'chat-msg chat-msg-user' : 'chat-msg chat-msg-ai';
  var bubbleCls = role === 'user' ? 'chat-bubble bg-primary/15 border border-primary/25 rounded-xl rounded-br-sm'
                                  : 'chat-bubble bg-slate-800/60 border border-slate-700 rounded-xl rounded-bl-sm';
  var html = '<div class="' + cls + '"' + msgId + '><div class="' + bubbleCls + '">';
  if (isPending) html += '<span class="material-icons-round text-xs animate-spin text-primary mr-1">refresh</span>';
  html += '<span class="text-xs text-slate-300 whitespace-pre-wrap">' + _escHtml(text) + '</span>';
  html += '</div></div>';
  el.insertAdjacentHTML('beforeend', html);
  el.scrollTop = el.scrollHeight;
}

async function sendOvChat() {
  var input = document.getElementById('ovChatInput');
  var msg = input.value.trim();
  if (!msg) return;

  // 如果有拖拽上下文，拼接到消息前
  var fullMsg = msg;
  if (window._chatContextData) {
    fullMsg = '以下是一条新闻的结构化信息：\n' + window._chatContextData + '\n\n我的问题：' + msg;
    clearChatContext();
  }

  input.value = '';
  input.placeholder = '输入问题...';
  appendOvChatMsg('user', msg);

  var pendingId = 'pending-' + Date.now();
  appendOvChatMsg('assistant', '思考中...', pendingId, true);

  try {
    var resp = await fetch('/portfolio/api/projects/' + _ovChatProjectId + '/chat', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({message: fullMsg}),
    });
    var data = await resp.json();
    if (!data.ok) {
      var el = document.getElementById('msg-' + pendingId);
      if (el) el.querySelector('span:last-child').textContent = '发送失败: ' + (data.error || '');
      return;
    }
  } catch(e) {
    var el = document.getElementById('msg-' + pendingId);
    if (el) el.querySelector('span:last-child').textContent = '网络错误';
    return;
  }

  // 轮询回复
  if (_ovChatPollTimer) clearInterval(_ovChatPollTimer);
  _ovChatPollTimer = setInterval(async function() {
    try {
      var resp = await fetch('/portfolio/api/projects/' + _ovChatProjectId + '/chat/poll');
      var data = await resp.json();
      if (data.reply) {
        clearInterval(_ovChatPollTimer);
        _ovChatPollTimer = null;
        var el = document.getElementById('msg-' + pendingId);
        if (el) el.remove();
        appendOvChatMsg('assistant', data.reply);
      } else if (data.status === 'error') {
        clearInterval(_ovChatPollTimer);
        _ovChatPollTimer = null;
        var el = document.getElementById('msg-' + pendingId);
        if (el) el.querySelector('span:last-child').textContent = '回复失败';
      }
    } catch(e) {}
  }, 2000);
}
```

**Step 3: 添加 chat 相关 CSS**

在 `{% block extra_styles %}` 中添加（复用 portfolio.html 的 chat 样式）：

```css
.chat-msg { margin-bottom: 8px; max-width: 95%; }
.chat-msg-user { margin-left: auto; }
.chat-bubble { padding: 8px 12px; font-size: 0.75rem; line-height: 1.5; }
```

**Step 4: 验证**

1. 从 Tab1/Tab3 拖拽新闻卡片到右侧 chat 面板
2. 释放后，chat input 上方显示 FOE 上下文预览
3. 输入问题，发送，等待 AI 回复
4. 可以清除上下文后直接提问

**Step 5: Commit**

```bash
git add templates/overview.html
git commit -m "feat: overview drag-and-drop news to AI chat with context injection"
```

---

## Task 8: 清理 + Sparkline + 收尾

**目标：** 清理旧概览页残留代码，确保 sparkline 渲染、heatmap tab 切换、FOE 卡片展开等现有交互在新布局下正常工作。

**Files:**
- Modify: `templates/overview.html` — 清理旧 block content 残留，整合所有 JS
- Modify: `routers/overview.py` — 清理不再需要的旧数据函数（如 `get_watchlist_alerts` 可保留但标记 deprecated）

**Step 1: 确保 sparkline 渲染器在新布局下工作**

sparkline 渲染代码（`svg[data-prices]` 遍历）需要在 Tab1 内容渲染后执行：

```javascript
// 在 wlSelect 末尾或 DOMContentLoaded 中调用
function renderSparklines() {
  document.querySelectorAll('svg[data-prices]').forEach(function(svg) {
    try {
      var prices = JSON.parse(svg.getAttribute('data-prices'));
      if (!prices || prices.length < 2) return;
      var w = 80, h = 24, pad = 2;
      var minP = Math.min.apply(null, prices), maxP = Math.max.apply(null, prices);
      var range = maxP - minP || 1;
      var pts = prices.map(function(p, i) {
        var x = pad + (i / (prices.length - 1)) * (w - pad * 2);
        var y = pad + (1 - (p - minP) / range) * (h - pad * 2);
        return x.toFixed(1) + ',' + y.toFixed(1);
      });
      svg.querySelector('polyline').setAttribute('points', pts.join(' '));
    } catch(e) {}
  });
}
renderSparklines();
```

**Step 2: 确保 heatmap tab 切换正常**

`switchHeatTab` 函数已在现有代码中定义，确认 `#heatmap-net` 和 `#heatmap-gross` 在新 Tab2 中的 ID 不冲突。

**Step 3: 确保 FOE 卡片 toggleFoeCard 全局可用**

```javascript
function toggleFoeCard(summaryEl) {
  var card = summaryEl.closest('.foe-card');
  var detail = card.querySelector('.foe-detail');
  var chevron = card.querySelector('.foe-chevron');
  detail.classList.toggle('hidden');
  chevron.style.transform = detail.classList.contains('hidden') ? '' : 'rotate(180deg)';
}
```

**Step 4: 更新 overview.py 的 refresh 路由**

确保 `refresh_dashboards()` 也传入新的 context 字段（`portfolio_holdings`, `index_list`）。

**Step 5: 全面验证**

1. Tab1: 股票列表滚动、sparkline、点击切换详情、三栏新闻、拖拽
2. Tab2: 指数卡片、热力图、解读面板、四个资金面图表
3. Tab3: 四容器新闻加载、FOE 展开/折叠、拖拽、like/关联按钮
4. 右侧 Chat: 拖拽注入上下文、发送消息、接收回复
5. Header: 日期选择、刷新按钮

**Step 6: Commit**

```bash
git add templates/overview.html routers/overview.py
git commit -m "chore: overview redesign cleanup - sparklines, heatmap, FOE toggle integration"
```

"""三层选股规则标签计算引擎

L1: 纯量化计算 — 从 stock_daily/financial_reports/capital_flow 等表直接计算
L2: AI 轻量标注 — 基于 stock_mentions/content_summaries 已有数据
L3: AI 深度分析 — 仅对已有≥2个标签或在 watchlist 中的股票
"""

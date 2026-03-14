"""daily_intel — 每日情报包

数据流：
  知识星球 zsxq / 手动录入
    → DeepSeek 结构化提取
    → daily_intel_stocks（每公司一行）
    → robust_kline/filter.py（月K阳线筛选）→ robust_kline_candidates
"""

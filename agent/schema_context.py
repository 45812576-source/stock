"""Schema 上下文生成器

从 schema_mysql.sql 解析表结构，生成带业务语义的精简描述，
作为 system prompt 的一部分注入给 DeepSeek，让模型理解数据库结构。
"""
from functools import lru_cache

# 业务语义映射：表名 → 精简描述
_TABLE_DOCS = {
    "raw_items": (
        "原始采集信息（新闻/公告/研报/社群帖子等）",
        "source_id(数据源), title(标题), content(正文), published_at(发布时间), "
        "processing_status(处理状态:pending/done/failed), item_type(类型)"
    ),
    "cleaned_items": (
        "AI清洗后的结构化信息，每条对应一条raw_item",
        "event_type(事件类型), sentiment(情绪:positive/negative/neutral), "
        "importance(重要性1-5), summary(摘要), key_points_json(要点JSON数组), "
        "tags_json(标签JSON数组), impact_analysis(影响分析), "
        "time_horizon(时间维度:短期/中期/长期), confidence(置信度0-1), cleaned_at(清洗时间)"
    ),
    "item_companies": (
        "清洗信息关联的公司（一条信息可关联多家公司）",
        "cleaned_item_id, stock_code(股票代码), stock_name(股票名称), "
        "relevance(相关度:high/medium/low), impact(影响:positive/negative/neutral)"
    ),
    "item_industries": (
        "清洗信息关联的行业",
        "cleaned_item_id, industry_name(行业名称), industry_level(层级), impact(影响)"
    ),
    "research_reports": (
        "研报信息（从cleaned_items中提取的研报元数据）",
        "cleaned_item_id, broker_name(券商), analyst_name(分析师), "
        "report_type(类型), rating(评级:买入/增持/中性/减持/卖出), "
        "target_price(目标价), stock_code, stock_name, report_date"
    ),
    "stock_info": (
        "股票基本信息（akshare同步）",
        "stock_code(主键), stock_name, industry_l1(一级行业), industry_l2(二级行业), "
        "market(市场:SH/SZ), list_date(上市日期), total_shares(总股本), "
        "float_shares(流通股), market_cap(市值)"
    ),
    "stock_daily": (
        "股票日K线行情数据",
        "stock_code, trade_date, open/high/low/close(开高低收), "
        "volume(成交量), amount(成交额), turnover_rate(换手率), "
        "change_pct(涨跌幅%), change_amount(涨跌额)"
    ),
    "capital_flow": (
        "个股资金流向（主力/超大单/大单净流入）",
        "stock_code, trade_date, main_net_inflow(主力净流入), "
        "super_large_net(超大单净流入), large_net(大单净流入), "
        "medium_net(中单净流入), small_net(小单净流入), main_net_ratio(主力净流入占比%)"
    ),
    "industry_capital_flow": (
        "行业资金流向",
        "industry_name, trade_date, net_inflow(净流入), change_pct(涨跌幅), leading_stock(领涨股)"
    ),
    "northbound_flow": (
        "北向资金（沪深港通）每日净流入",
        "trade_date, sh_net(沪股通净流入亿元), sz_net(深股通净流入亿元), "
        "total_net(合计净流入), cumulative(累计净流入)"
    ),
    "macro_indicators": (
        "宏观经济指标（CPI/PPI/PMI/社融等）",
        "indicator_name(指标名称), indicator_date, value(数值), unit(单位), source(来源)"
    ),
    "financial_reports": (
        "上市公司财报数据（季报/年报）",
        "stock_code, report_period(报告期如2024Q3), revenue(营收), net_profit(净利润), "
        "revenue_yoy(营收同比%), profit_yoy(净利润同比%), eps(每股收益), roe(净资产收益率), "
        "beat_expectation(是否超预期:1/0), report_date(披露日期)"
    ),
    "kg_entities": (
        "知识图谱实体（市场/主题/行业/产业链/公司/宏观指标/大宗商品/政策等）",
        "entity_type(类型:market/theme/industry/industry_chain/company/macro_indicator/"
        "commodity/energy/policy/revenue_element/intermediate/consumer_good), "
        "entity_name(实体名称), description(描述), investment_logic(投资逻辑)"
    ),
    "kg_relationships": (
        "知识图谱关系（因果/结构/供应链/政策影响等）",
        "source_entity_id, target_entity_id, relation_type(关系类型), "
        "strength(强度0-1), direction(方向:positive/negative/neutral), "
        "evidence(证据), confidence(置信度)"
    ),
    "deep_research": (
        "个股/行业深度研究报告",
        "research_type(类型:stock/industry), target(研究对象), research_date, "
        "financial_score/valuation_score/technical_score/sentiment_score/catalyst_score/risk_score/overall_score(各维度评分), "
        "report_json(完整报告JSON), recommendation(建议)"
    ),
    "investment_opportunities": (
        "投资机会记录",
        "stock_code, stock_name, opportunity_type(机会类型), source(来源), "
        "rating(评级), tags_json(标签), summary(摘要), status(状态:active/closed)"
    ),
    "watchlist": (
        "自选股列表",
        "stock_code, stock_name, watch_type(关注类型:interested/tracking/holding), "
        "related_tags(关联标签), notes(备注), added_at"
    ),
    "holding_positions": (
        "持仓记录",
        "stock_code, stock_name, buy_date(买入日期), buy_price(买入价), "
        "quantity(数量), status(状态:open/closed), sell_date, sell_price, pnl(盈亏), notes"
    ),
    "portfolio_reviews": (
        "组合复盘记录",
        "review_date, holdings_snapshot_json(持仓快照), review_report(复盘报告), lessons_json(经验教训)"
    ),
    "tag_groups": (
        "热点标签组（将相关标签聚合为一个投资主题）",
        "group_name(组名), tags_json(标签列表JSON), group_logic(投资逻辑), "
        "time_range(时间范围), total_frequency(总出现频次)"
    ),
    "tag_group_research": (
        "标签组深度研究报告（宏观+行业+个股推荐+综合论证）",
        "group_id, research_date, macro_report(宏观分析), industry_report(行业分析), "
        "top10_stocks_json(推荐个股JSON), logic_synthesis_json(综合论证JSON), status(状态)"
    ),
    "dashboard_tag_frequency": (
        "热点标签出现频次（用于热力图）",
        "tag_name, tag_type, dashboard_type, appear_date, rank_position(排名)"
    ),
    "extracted_texts": (
        "从文档提取的原始文本（新管线）",
        "source(来源), source_format(格式), publish_time, full_text(全文), "
        "summary_status(总结状态:pending/done/skipped), kg_status(KG提取状态)"
    ),
    "content_summaries": (
        "文档内容总结（事实/观点分离）",
        "extracted_text_id, doc_type(文档类型:announcement/financial_report/research_report等), "
        "summary(综合摘要), fact_summary(事实摘要), opinion_summary(观点摘要), "
        "evidence_assessment(证据评估), info_gaps(信息缺口)"
    ),
    "stock_mentions": (
        "文档中提及的股票（含关联主题和事件）",
        "extracted_text_id, stock_name, stock_code, related_themes(关联主题), "
        "related_events(关联事件), theme_logic(主题逻辑), mention_time"
    ),
    "investment_strategies": (
        "投资策略定义",
        "strategy_name, description, rules_json(量化规则JSON), ai_rules_text(AI规则文本), is_active"
    ),
    "strategy_stocks": (
        "策略选出的股票池",
        "strategy_id, stock_code, stock_name, source(来源:manual/ai), "
        "status(状态:active/removed), ai_reason(AI选股理由)"
    ),
    "stock_selection_rules": (
        "选股规则库",
        "category(类别), rule_name(规则名), definition(规则定义), "
        "layer(层级:1=量化/2=AI轻量/3=AI深度), is_active"
    ),
    "stock_rule_tags": (
        "股票规则标签计算结果（某股票是否满足某规则）",
        "stock_code, rule_id, rule_name, matched(是否匹配:1/0), "
        "confidence(置信度), evidence(证据), layer, computed_at"
    ),
    "data_sources": (
        "数据源配置（采集管道的数据来源）",
        "name(数据源名称), source_type(类型), daily_limit/monthly_limit(配额), "
        "today_used/month_used(已用量), enabled(是否启用)"
    ),
    "group_chat_messages": (
        "标签组AI对话历史",
        "group_id, role(user/assistant), content(消息内容), created_at"
    ),
    "stock_realtime": (
        "股票实时行情缓存",
        "stock_code(主键), stock_name, last_price(最新价), change_pct(涨跌幅), "
        "volume, amount, pe_ratio(市盈率), pb_ratio(市净率), updated_at"
    ),
}


@lru_cache(maxsize=1)
def get_schema_context() -> str:
    """生成精简的数据库 schema 描述，用于注入 system prompt"""
    lines = [
        "## 数据库结构（MySQL，数据库名: stock_analysis）\n",
        "以下是可查询的主要表：\n",
    ]
    for table, (desc, fields) in _TABLE_DOCS.items():
        lines.append(f"**{table}** — {desc}")
        lines.append(f"  字段: {fields}\n")

    lines.append(
        "\n## 查询规范\n"
        "- 所有查询只读，禁止 INSERT/UPDATE/DELETE\n"
        "- 时间字段格式: trade_date/report_date 为 'YYYY-MM-DD' 字符串，"
        "cleaned_at/created_at 为 TIMESTAMP\n"
        "- JSON 字段（*_json）存储 JSON 字符串，需用 JSON_EXTRACT 或在应用层解析\n"
        "- stock_code 格式: 6位数字字符串，如 '600519'（茅台）、'000001'（平安银行）\n"
        "- importance 字段: 1=极低, 2=低, 3=中, 4=高, 5=极高\n"
        "- sentiment 字段: positive=利好, negative=利空, neutral=中性\n"
    )
    return "\n".join(lines)

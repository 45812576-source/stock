"""文档类型定义与关键词分类器

DOC_TYPES: 精细分类 schema
classify_doc_type(title, content_preview) -> str: 纯关键词匹配，无命中返回 "other"
"""

# ── Schema ──────────────────────────────────────────────────────────────────
# 每条: (英文标识, 中文标签, 描述)
DOC_TYPES = [
    ("announcement",      "公司公告",   "定期报告、重大事项、股权变动等"),
    ("financial_report",  "财报正文",   "年报/半年报/季报"),
    ("policy_doc",        "政策文件",   "政策文件/监管通知"),
    ("research_report",   "研报",       "深度分析，含评级/目标价"),
    ("strategy_report",   "策略报告",   "宏观/行业/主题，无具体标的评级"),
    ("roadshow_notes",    "路演纪要",   "路演/调研纪要"),
    ("feature_news",      "专题新闻",   "有深度，有背景分析"),
    ("flash_news",        "快讯",       "单条，事件驱动，短文本"),
    ("digest_news",       "拼盘快讯",   "多条汇总，无深度"),
    ("data_release",      "数据播报",   "PMI、CPI、就业等经济数据"),
    ("market_commentary", "市场评论",   "盘中/盘后市场评论"),
    ("social_post",       "社媒帖子",   "雪球、微博等"),
    ("chat_record",       "聊天记录",   "聊天记录/群消息"),
    ("xlsx_data",         "统计数据",   "Excel/CSV表格数据"),
    ("other",             "其他",       "未能匹配的文档"),
]

# 仅英文标识列表（供下拉选项等使用）
DOC_TYPE_KEYS = [d[0] for d in DOC_TYPES]

# 中文标签映射
DOC_TYPE_LABELS = {d[0]: d[1] for d in DOC_TYPES}

# ── 关键词规则 ───────────────────────────────────────────────────────────────
# 格式: {doc_type: {"title": [关键词...], "content": [关键词...]}}
# title 命中得 2 分，content 命中得 1 分，取最高分类型
_RULES = {
    "announcement": {
        "title": [
            "公告", "披露", "股权激励", "定向增发", "配股", "回购", "分红",
            "重大资产", "关联交易", "股东大会", "董事会", "监事会",
            "业绩预告", "业绩快报", "半年报", "年报", "季报",
            "信息披露", "临时公告", "重大事项",
        ],
        "content": [
            "本公司", "本公告", "特此公告", "董事会决议", "股东大会决议",
            "证监会", "交易所", "上市公司",
        ],
    },
    "financial_report": {
        "title": [
            "年度报告", "半年度报告", "季度报告", "年报全文", "财务报告",
            "合并财务报表", "审计报告",
        ],
        "content": [
            "资产负债表", "利润表", "现金流量表", "每股收益", "净利润",
            "营业收入", "毛利率", "资产总额", "负债合计",
        ],
    },
    "policy_doc": {
        "title": [
            "通知", "意见", "办法", "规定", "条例", "政策", "监管",
            "指导意见", "实施方案", "发改委", "证监会", "银保监",
            "国务院", "财政部", "央行", "人民银行",
        ],
        "content": [
            "政策", "监管", "法规", "合规", "监管要求", "政府", "部门",
        ],
    },
    "research_report": {
        "title": [
            "研报", "深度报告", "深度研究", "投资评级", "目标价",
            "买入", "增持", "中性", "减持", "卖出", "强烈推荐",
            "首次覆盖", "维持评级", "调高评级", "调低评级",
        ],
        "content": [
            "目标价", "评级", "EPS", "PE", "PB", "ROE", "EBITDA",
            "盈利预测", "估值", "分析师", "研究员",
        ],
    },
    "strategy_report": {
        "title": [
            "策略", "宏观", "行业展望", "市场展望", "投资策略",
            "配置建议", "主题投资", "行业比较", "大类资产",
        ],
        "content": [
            "宏观经济", "行业配置", "资产配置", "风险偏好", "市场风格",
        ],
    },
    "roadshow_notes": {
        "title": [
            "路演", "调研", "纪要", "交流纪要", "投资者交流",
            "机构调研", "电话会议", "业绩说明会",
        ],
        "content": [
            "问答", "Q&A", "投资者问", "公司答", "管理层", "调研纪要",
        ],
    },
    "feature_news": {
        "title": [
            "深度", "专题", "解读", "分析", "背景", "影响", "展望",
            "全面解析", "深度解读",
        ],
        "content": [
            "背景分析", "深度解读", "专题报道",
        ],
    },
    "flash_news": {
        "title": [
            "快讯", "速报", "突发", "紧急", "最新消息", "刚刚",
        ],
        "content": [
            "据悉", "消息人士", "最新消息",
        ],
    },
    "digest_news": {
        "title": [
            "早报", "晚报", "日报", "周报", "月报", "要闻", "汇总",
            "精选", "摘要", "综述", "盘点",
        ],
        "content": [
            "要闻汇总", "新闻摘要", "今日要闻",
        ],
    },
    "data_release": {
        "title": [
            "PMI", "CPI", "PPI", "GDP", "就业", "非农", "通胀",
            "数据", "统计", "经济数据", "指数发布",
        ],
        "content": [
            "同比", "环比", "数据显示", "统计局", "国家统计",
        ],
    },
    "market_commentary": {
        "title": [
            "盘前", "盘中", "盘后", "收盘", "开盘", "午评", "早评",
            "市场评论", "行情", "复盘",
        ],
        "content": [
            "大盘", "指数", "涨跌", "成交量", "北向资金", "两市",
        ],
    },
    "social_post": {
        "title": [
            "雪球", "微博", "微信", "知乎", "帖子", "ZSXQ", "星球",
        ],
        "content": [
            "转发", "点赞", "评论", "@",
        ],
    },
    "chat_record": {
        "title": [
            "群消息", "聊天记录", "微信群", "钉钉", "飞书",
        ],
        "content": [
            "群主", "群成员", "消息记录",
        ],
    },
    "xlsx_data": {
        "title": [
            "统计数据", "数据表", "数据汇总", "统计表", "明细表", "报表",
        ],
        "content": [
            "Sheet", "CSV", "sheet_name",
        ],
    },
}

# ── doc_type → 摘要族映射 ────────────────────────────────────────────────────
# 1=structured(结构化提取)  2=analysis(深度FOE)  3=informal(非正式)  4=brief(轻量)
FAMILY_MAP: dict[str, int] = {
    "announcement":      1,
    "financial_report":  1,
    "data_release":      1,
    "policy_doc":        1,
    "xlsx_data":         1,
    "research_report":   2,
    "strategy_report":   2,
    "feature_news":      2,
    "roadshow_notes":    2,
    "social_post":       3,
    "chat_record":       3,
    "flash_news":        4,
    "market_commentary": 4,
    "digest_news":       4,  # 拆条后每条按实际类型再分发，整体先归 4
    "other":             4,
}

FAMILY_TABLES = {
    1: "summary_structured",
    2: "summary_analysis",
    3: "summary_informal",
    4: "summary_brief",
}


# ── 分类器 ───────────────────────────────────────────────────────────────────

def classify_doc_type(title: str, content_preview: str = "") -> str:
    """关键词匹配分类

    title 命中得 2 分，content_preview 前 200 字命中得 1 分。
    取最高分类型，无命中返回 "other"。

    Args:
        title: 文档标题
        content_preview: 内容预览（取前 200 字即可）

    Returns:
        doc_type 英文标识字符串
    """
    title_lower = (title or "").lower()
    content_lower = (content_preview or "")[:200].lower()

    scores: dict[str, int] = {}

    for doc_type, rules in _RULES.items():
        score = 0
        for kw in rules.get("title", []):
            if kw.lower() in title_lower:
                score += 2
        for kw in rules.get("content", []):
            if kw.lower() in content_lower:
                score += 1
        if score > 0:
            scores[doc_type] = score

    if not scores:
        return "other"

    return max(scores, key=lambda k: scores[k])

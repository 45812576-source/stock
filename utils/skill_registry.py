"""分析调用点注册表 — 记录系统中所有 Claude API 调用点"""
from pathlib import Path
from typing import Optional

SKILLS_DIR = Path.home() / ".claude" / "skills"

# 按模块分组的分析调用点注册表
ANALYSIS_REGISTRY = {
    "cleaning": {
        "label": "信息清洗",
        "icon": "cleaning_services",
        "color": "amber",
        "entries": [
            {
                "function": "clean_single_item()",
                "file": "cleaning/claude_processor.py",
                "label": "信息结构化清洗",
                "desc": "将原始信息清洗为结构化数据",
                "skill_name": "information-cleaning-structuring",
                "prompt_var": "CLEANING_SYSTEM_PROMPT",
                "icon": "auto_fix_high",
            },
            {
                "function": "clean_with_event_analysis()",
                "file": "cleaning/claude_processor.py",
                "label": "事件深度分析",
                "desc": "对清洗后的信息进行事件级别深度分析",
                "skill_name": "stock-event-analysis",
                "prompt_var": "EVENT_ANALYSIS_PROMPT",
                "icon": "event_note",
            },
        ],
    },
    "research": {
        "label": "深度研究",
        "icon": "science",
        "color": "blue",
        "entries": [
            {
                "function": "deep_research_stock() Step 1",
                "file": "research/deep_researcher.py",
                "label": "财务建模",
                "desc": "构建公司财务模型",
                "skill_name": "financial-model-construction",
                "prompt_var": None,
                "icon": "account_balance",
            },
            {
                "function": "deep_research_stock() Step 2",
                "file": "research/deep_researcher.py",
                "label": "估值分析",
                "desc": "公司估值模型分析",
                "skill_name": "company-valuation",
                "prompt_var": None,
                "icon": "price_check",
            },
            {
                "function": "deep_research_stock() Step 3",
                "file": "research/deep_researcher.py",
                "label": "技术分析",
                "desc": "K线图技术分析",
                "skill_name": "stock-chart-analysis",
                "prompt_var": None,
                "icon": "candlestick_chart",
            },
            {
                "function": "deep_research_stock() Step 4",
                "file": "research/deep_researcher.py",
                "label": "买卖点预测",
                "desc": "动态买卖点预测",
                "skill_name": "dynamic-stock-chart-predict",
                "prompt_var": None,
                "icon": "trending_up",
            },
            {
                "function": "deep_research_stock() Step 5",
                "file": "research/deep_researcher.py",
                "label": "事件情绪分析",
                "desc": "事件与市场情绪分析",
                "skill_name": "stock-event-analysis",
                "prompt_var": None,
                "icon": "sentiment_satisfied",
            },
            {
                "function": "deep_research_stock() 综合",
                "file": "research/deep_researcher.py",
                "label": "综合评分",
                "desc": "汇总各维度生成综合评分",
                "skill_name": None,
                "prompt_var": "STOCK_SYNTHESIS_PROMPT",
                "icon": "grade",
            },
            {
                "function": "deep_research_industry() Step 1",
                "file": "research/deep_researcher.py",
                "label": "行业结构分析",
                "desc": "行业结构与竞争格局分析",
                "skill_name": "event-industry-impact",
                "prompt_var": None,
                "icon": "domain",
            },
            {
                "function": "deep_research_industry() Step 2",
                "file": "research/deep_researcher.py",
                "label": "宏观影响分析",
                "desc": "宏观经济对行业的影响",
                "skill_name": "macro-stock-analysis",
                "prompt_var": None,
                "icon": "public",
            },
            {
                "function": "deep_research_industry() 综合",
                "file": "research/deep_researcher.py",
                "label": "行业综合评分",
                "desc": "行业综合评分与建议",
                "skill_name": None,
                "prompt_var": "INDUSTRY_SYNTHESIS_PROMPT",
                "icon": "assessment",
            },
            {
                "function": "deep_research_macro() Step 1",
                "file": "research/deep_researcher.py",
                "label": "宏观指标分析",
                "desc": "宏观经济指标分析",
                "skill_name": "macro-stock-analysis",
                "prompt_var": None,
                "icon": "show_chart",
            },
            {
                "function": "deep_research_macro() Step 2",
                "file": "research/deep_researcher.py",
                "label": "宏观洞察",
                "desc": "宏观趋势洞察生成",
                "skill_name": "macro-insight-generation",
                "prompt_var": None,
                "icon": "lightbulb",
            },
            {
                "function": "deep_research_macro() 综合",
                "file": "research/deep_researcher.py",
                "label": "宏观综合评分",
                "desc": "宏观综合评分与展望",
                "skill_name": None,
                "prompt_var": "MACRO_SYNTHESIS_PROMPT",
                "icon": "summarize",
            },
        ],
    },
    "hotspot": {
        "label": "热点追踪",
        "icon": "local_fire_department",
        "color": "orange",
        "entries": [
            {
                "function": "analyze_tag_group()",
                "file": "hotspot/tag_group_analyzer.py",
                "label": "标签组逻辑分析",
                "desc": "分析标签组内在逻辑关系",
                "skill_name": None,
                "prompt_var": "GROUP_LOGIC_PROMPT",
                "icon": "hub",
            },
            {
                "function": "research_tag_group() 宏观",
                "file": "hotspot/tag_group_research.py",
                "label": "标签组宏观分析",
                "desc": "标签组宏观经济影响分析",
                "skill_name": None,
                "prompt_var": "MACRO_ANALYSIS_PROMPT",
                "icon": "language",
            },
            {
                "function": "research_tag_group() 行业",
                "file": "hotspot/tag_group_research.py",
                "label": "标签组行业分析",
                "desc": "标签组行业影响分析",
                "skill_name": None,
                "prompt_var": "INDUSTRY_ANALYSIS_PROMPT",
                "icon": "factory",
            },
            {
                "function": "recommend_tag_groups()",
                "file": "hotspot/tag_recommender.py",
                "label": "标签组验证",
                "desc": "验证推荐标签组的合理性",
                "skill_name": None,
                "prompt_var": "TAG_VALIDATION_PROMPT",
                "icon": "verified",
            },
        ],
    },
    "knowledge_graph": {
        "label": "知识图谱",
        "icon": "share",
        "color": "purple",
        "entries": [
            {
                "function": "update_from_cleaned_items()",
                "file": "knowledge_graph/kg_updater.py",
                "label": "KG实体关系提取",
                "desc": "从清洗数据中提取实体与关系",
                "skill_name": None,
                "prompt_var": "KG_EXTRACTION_PROMPT",
                "icon": "account_tree",
            },
        ],
    },
    "tracking": {
        "label": "持仓跟踪",
        "icon": "track_changes",
        "color": "emerald",
        "entries": [
            {
                "function": "check_holding_updates()",
                "file": "tracking/holding_analyzer.py",
                "label": "持仓变化分析",
                "desc": "分析持仓变化并生成提醒",
                "skill_name": None,
                "prompt_var": "CHANGE_HIGHLIGHT_PROMPT",
                "icon": "notifications_active",
            },
        ],
    },
}


def get_analysis_registry():
    """返回按模块分组的注册表，附带每个 Skill 的状态"""
    result = {}
    for module, group in ANALYSIS_REGISTRY.items():
        items_with_status = []
        for item in group["entries"]:
            entry = dict(item)
            if entry["skill_name"]:
                status = get_skill_status(entry["skill_name"])
                entry["skill_status"] = status
            else:
                entry["skill_status"] = None
            items_with_status.append(entry)
        result[module] = {
            "label": group["label"],
            "icon": group["icon"],
            "color": group["color"],
            "entries": items_with_status,
        }
    return result


def get_skill_status(skill_name: str) -> dict:
    """检查 Skill 文件是否存在、字符数"""
    skill_path = SKILLS_DIR / skill_name / "SKILL.md"
    if skill_path.exists():
        content = skill_path.read_text(encoding="utf-8")
        return {"exists": True, "chars": len(content), "path": str(skill_path)}
    return {"exists": False, "chars": 0, "path": str(skill_path)}


def get_skill_content(skill_name: str) -> Optional[str]:
    """读取 Skill 文件内容"""
    skill_path = SKILLS_DIR / skill_name / "SKILL.md"
    if skill_path.exists():
        return skill_path.read_text(encoding="utf-8")
    return None

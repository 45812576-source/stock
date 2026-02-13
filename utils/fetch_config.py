"""非结构化信息源 — 集中配置模块

唯一数据源: fetch_settings.json
首次启动时从 SOURCE_CATALOG 初始化默认配置。
"""
from __future__ import annotations

import json
from pathlib import Path

_FETCH_SETTINGS_PATH = Path(__file__).parent.parent / "fetch_settings.json"

# ==================== 预置源目录 ====================
SOURCE_CATALOG = {
    "cls":        {"label": "财联社快讯",   "group": "news",      "icon": "newspaper",   "desc": "全市场宏观快讯电报",           "fetcher_type": "jasper"},
    "caixin":     {"label": "财新深度",     "group": "news",      "icon": "newspaper",   "desc": "深度财经分析报道",             "fetcher_type": "jasper"},
    "hot_stocks": {"label": "热门股票新闻", "group": "news",      "icon": "newspaper",   "desc": "东方财富人气榜Top20个股新闻",  "fetcher_type": "jasper"},
    "watchlist":  {"label": "自选股新闻",   "group": "news",      "icon": "newspaper",   "desc": "watchlist中关注/持仓个股新闻", "fetcher_type": "jasper"},
    "cctv":       {"label": "CCTV新闻联播", "group": "news",      "icon": "newspaper",   "desc": "当日新闻联播，宏观政策风向",   "fetcher_type": "jasper"},
    "djyanbao":   {"label": "洞见研报",     "group": "report",    "icon": "description", "desc": "券商/行业研报元数据",           "fetcher_type": "djyanbao",  "limit": 100},
    "fxbaogao":   {"label": "发现报告",     "group": "report",    "icon": "description", "desc": "行业报告元数据",               "fetcher_type": "fxbaogao",  "limit": 100},
    "em_report":  {"label": "东财研报(PDF)", "group": "report",    "icon": "description", "desc": "东方财富研报，含PDF全文提取",   "fetcher_type": "em_report", "limit": 10},
    "zsxq":       {"label": "知识星球",     "group": "community", "icon": "forum",       "desc": "知识星球社群帖子",             "fetcher_type": "zsxq",      "max_pages": 5},
}

# 默认启用状态
_DEFAULT_ENABLED = {
    "cls": True, "caixin": True, "hot_stocks": True, "watchlist": True, "cctv": True,
    "djyanbao": False, "fxbaogao": False, "em_report": True, "zsxq": True,
}

SOURCE_GROUPS = {
    "news":      {"label": "新闻资讯", "icon": "newspaper",   "color": "blue"},
    "report":    {"label": "研报数据", "icon": "description", "color": "orange"},
    "community": {"label": "社群舆情", "icon": "forum",       "color": "purple"},
}


def _default_settings() -> dict:
    """从 CATALOG 构建默认 settings"""
    sources = {}
    for key, cat in SOURCE_CATALOG.items():
        entry = dict(cat)
        entry["enabled"] = _DEFAULT_ENABLED.get(key, False)
        sources[key] = entry
    return {"news_hours": 24, "sources": sources}


def load_fetch_settings() -> dict:
    """加载配置。文件不存在则从默认值初始化并写入。"""
    if _FETCH_SETTINGS_PATH.exists():
        try:
            with open(_FETCH_SETTINGS_PATH, "r", encoding="utf-8") as f:
                saved = json.load(f)
            # 确保每个源都有 catalog 中的元数据字段
            for key, src in list(saved.get("sources", {}).items()):
                if key in SOURCE_CATALOG:
                    for field in ("label", "group", "icon", "desc", "fetcher_type"):
                        src.setdefault(field, SOURCE_CATALOG[key].get(field, ""))
            saved.setdefault("news_hours", 24)
            return saved
        except Exception:
            pass
    # 首次：写入默认值
    settings = _default_settings()
    save_fetch_settings(settings)
    return json.loads(json.dumps(settings))


def save_fetch_settings(settings: dict) -> None:
    """写入 fetch_settings.json"""
    with open(_FETCH_SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)


def add_source(key: str) -> tuple[bool, str]:
    """从 CATALOG 添加一个源到 settings。返回 (ok, msg)。"""
    if key not in SOURCE_CATALOG:
        return False, f"未知源: {key}"
    settings = load_fetch_settings()
    if key in settings["sources"]:
        return False, f"源 {key} 已存在"
    entry = dict(SOURCE_CATALOG[key])
    entry["enabled"] = True
    settings["sources"][key] = entry
    save_fetch_settings(settings)
    return True, "ok"


def add_custom_source(
    key: str, label: str, group: str,
    desc: str = "", icon: str = "article",
    fetcher_type: str = "jasper",
    limit: int | None = None,
    max_pages: int | None = None,
) -> tuple[bool, str]:
    """添加自定义源到 settings。返回 (ok, msg)。"""
    if not key or not label:
        return False, "key 和 label 不能为空"
    if group not in SOURCE_GROUPS:
        return False, f"无效分组: {group}，可选: {', '.join(SOURCE_GROUPS)}"
    settings = load_fetch_settings()
    if key in settings["sources"]:
        return False, f"源 {key} 已存在"
    entry = {
        "label": label, "group": group, "icon": icon,
        "desc": desc, "fetcher_type": fetcher_type, "enabled": True,
    }
    if limit is not None:
        entry["limit"] = int(limit)
    if max_pages is not None:
        entry["max_pages"] = int(max_pages)
    settings["sources"][key] = entry
    save_fetch_settings(settings)
    return True, "ok"


def delete_source(key: str) -> tuple[bool, str]:
    """从 settings 中移除一个源。返回 (ok, msg)。"""
    settings = load_fetch_settings()
    if key not in settings["sources"]:
        return False, f"源 {key} 不存在"
    del settings["sources"][key]
    save_fetch_settings(settings)
    return True, "ok"


def get_available_sources() -> dict:
    """返回 CATALOG 中尚未添加到 settings 的源。"""
    settings = load_fetch_settings()
    existing = set(settings["sources"].keys())
    return {k: v for k, v in SOURCE_CATALOG.items() if k not in existing}

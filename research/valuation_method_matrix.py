"""行业-估值方法匹配矩阵（申万一级行业，31个）

用于：
1. 4a 方法选择 prompt 注入（format_method_matrix_for_prompt）
2. _validate_and_fix_4c_output 程序化合规校验
"""

# ── 核心矩阵 ─────────────────────────────────────────────────────────────────
INDUSTRY_METHOD_MATRIX = {
    "银行": {
        "primary": ["PB", "DDM"],
        "alternative": ["PE"],
        "forbidden": ["PS", "EV_EBITDA"],
        "notes": "利差驱动重资产，PB为核心估值锚；股息稳定大行可辅助DDM；ROE稳定时PE有效",
        "asset_type": "financial",
    },
    "非银金融": {
        "primary": ["PB", "PEV"],
        "alternative": ["PE"],
        "forbidden": ["PS"],
        "notes": "券商强周期用PB；保险用PEV（内含价值倍数）；信托/期货可用PE",
        "asset_type": "financial",
    },
    "房地产": {
        "primary": ["NAV", "PB"],
        "alternative": ["PE"],
        "forbidden": ["PS", "EV_EBITDA"],
        "notes": "核心是土地/在建项目的资产价值，NAV折现最准；开发商亏损期间禁PE",
        "asset_type": "asset_heavy",
    },
    "公用事业": {
        "primary": ["PE", "DDM", "EV_EBITDA"],
        "alternative": ["PB"],
        "forbidden": ["PS"],
        "notes": "稳定现金流优先PE/DDM；有资产折旧的管网/水务用EV-EBITDA",
        "asset_type": "regulated",
    },
    "煤炭": {
        "primary": ["DCF", "PB", "EV_EBITDA"],
        "alternative": ["PE"],
        "forbidden": ["PS"],
        "notes": "资源量×预期煤价→DCF；周期顶部用PB；禁PS因煤炭利润率差异极大",
        "asset_type": "resource",
    },
    "石油石化": {
        "primary": ["DCF", "EV_EBITDA"],
        "alternative": ["PB"],
        "forbidden": ["PS"],
        "notes": "储量×油价→DCF；上下游一体化用EV-EBITDA；禁PS",
        "asset_type": "resource",
    },
    "有色金属": {
        "primary": ["DCF", "EV_EBITDA", "PB"],
        "alternative": ["PE"],
        "forbidden": ["PS"],
        "notes": "资源储量×金属价格DCF；冶炼/加工环节EV-EBITDA；禁PS",
        "asset_type": "resource",
    },
    "钢铁": {
        "primary": ["PB", "EV_EBITDA"],
        "alternative": ["PE"],
        "forbidden": ["PS"],
        "notes": "强周期重资产，PB估值底部支撑；EV-EBITDA消除折旧影响；禁PS",
        "asset_type": "cyclical_asset_heavy",
    },
    "化工": {
        "primary": ["PE", "EV_EBITDA"],
        "alternative": ["PS"],
        "forbidden": [],
        "notes": "专用化学品PE；大宗化工EV-EBITDA；亏损/高成长型化工可用PS",
        "asset_type": "cyclical",
    },
    "建筑材料": {
        "primary": ["PE", "EV_EBITDA"],
        "alternative": ["PB"],
        "forbidden": [],
        "notes": "水泥/玻璃等周期品EV-EBITDA；装饰材料品牌溢价用PE",
        "asset_type": "cyclical",
    },
    "建筑装饰": {
        "primary": ["PE", "PB"],
        "alternative": ["EV_EBITDA"],
        "forbidden": ["PS"],
        "notes": "工程承包PE；有息负债高时PB更准；禁PS因收入确认方式差异大",
        "asset_type": "service",
    },
    "机械设备": {
        "primary": ["PE", "PS"],
        "alternative": ["EV_EBITDA"],
        "forbidden": [],
        "notes": "成熟设备制造商PE；高增长细分/亏损期用PS；重资产设备EV-EBITDA",
        "asset_type": "manufacturing",
    },
    "电力设备": {
        "primary": ["PE", "PS"],
        "alternative": ["DCF"],
        "forbidden": [],
        "notes": "新能源高增长标的PS；成熟盈利用PE；长期稳定的储能/电网DCF",
        "asset_type": "manufacturing",
    },
    "国防军工": {
        "primary": ["PS", "PE"],
        "alternative": ["PB"],
        "forbidden": [],
        "notes": "军工特殊性，订单收入可见性强但利润率波动，PS优先；核心总装PE",
        "asset_type": "defense",
    },
    "汽车": {
        "primary": ["PE", "PS"],
        "alternative": ["EV_EBITDA"],
        "forbidden": [],
        "notes": "成熟整车PE；电动车/新势力高增长用PS；零部件EV-EBITDA",
        "asset_type": "cyclical",
    },
    "家用电器": {
        "primary": ["PE", "DCF"],
        "alternative": ["PS"],
        "forbidden": [],
        "notes": "白电品牌龙头PE/DCF；小家电成长期PS",
        "asset_type": "consumer",
    },
    "轻工制造": {
        "primary": ["PE", "PS"],
        "alternative": ["PB"],
        "forbidden": [],
        "notes": "文具/家居PE；造纸/包装等重资产PB",
        "asset_type": "manufacturing",
    },
    "纺织服装": {
        "primary": ["PE", "PS"],
        "alternative": ["PB"],
        "forbidden": [],
        "notes": "品牌服装PE；ODM/制造商PS",
        "asset_type": "consumer",
    },
    "商贸零售": {
        "primary": ["PE", "PS", "EV_EBITDA"],
        "alternative": ["PB"],
        "forbidden": [],
        "notes": "电商平台PS/PE；实体零售EV-EBITDA（含租金摊销）",
        "asset_type": "consumer_service",
    },
    "社会服务": {
        "primary": ["PE", "PS"],
        "alternative": ["EV_EBITDA"],
        "forbidden": [],
        "notes": "高增长服务业PS；成熟连锁PE；重资产（酒店/景区）EV-EBITDA",
        "asset_type": "service",
    },
    "食品饮料": {
        "primary": ["PE", "DCF"],
        "alternative": ["PS"],
        "forbidden": [],
        "notes": "白酒/调味品品牌溢价高PE；乳制品/速食DCF；亏损新品牌PS",
        "asset_type": "consumer",
    },
    "农林牧渔": {
        "primary": ["PS", "PB"],
        "alternative": ["PE"],
        "forbidden": [],
        "notes": "养殖周期底部亏损用PS/PB；景气期PE；种子资产价值PB",
        "asset_type": "cyclical",
    },
    "医药生物": {
        "primary": ["PE", "PS", "rNPV"],
        "alternative": ["EV_EBITDA"],
        "forbidden": [],
        "notes": "成熟制药PE；创新药研发期rNPV；仿制药/CXO PS；器械EV-EBITDA",
        "asset_type": "healthcare",
    },
    "计算机": {
        "primary": ["PS", "PE"],
        "alternative": ["EV_EBITDA"],
        "forbidden": [],
        "notes": "SaaS/高增长软件PS；成熟IT服务PE；亏损阶段PS为主",
        "asset_type": "tech",
    },
    "电子": {
        "primary": ["PE", "PS"],
        "alternative": ["EV_EBITDA"],
        "forbidden": [],
        "notes": "芯片设计成长期PS；封测/组件代工PE；重资产晶圆厂EV-EBITDA",
        "asset_type": "tech",
    },
    "通信": {
        "primary": ["PE", "EV_EBITDA"],
        "alternative": ["PS"],
        "forbidden": [],
        "notes": "运营商EV-EBITDA（资本密集）；设备商PE；新兴通信应用PS",
        "asset_type": "tech",
    },
    "传媒": {
        "primary": ["PE", "PS"],
        "alternative": ["EV_EBITDA"],
        "forbidden": [],
        "notes": "内容平台PS；广告/出版PE；流媒体亏损期PS",
        "asset_type": "media",
    },
    "交通运输": {
        "primary": ["EV_EBITDA", "PE", "PB"],
        "alternative": ["DCF"],
        "forbidden": ["PS"],
        "notes": "航空/航运EV-EBITDA消除折旧；铁路/港口PE；重资产基建PB；禁PS",
        "asset_type": "cyclical_asset_heavy",
    },
    "环保": {
        "primary": ["PE", "DCF"],
        "alternative": ["EV_EBITDA"],
        "forbidden": [],
        "notes": "特许经营长期现金流DCF；运营期PE；建设期EV-EBITDA",
        "asset_type": "regulated",
    },
    "综合": {
        "primary": ["NAV", "PB"],
        "alternative": ["PE"],
        "forbidden": [],
        "notes": "多元化控股集团NAV（分部加总）；无法拆分时PB",
        "asset_type": "conglomerate",
    },
    # 默认（未匹配时）
    "_default": {
        "primary": ["PE"],
        "alternative": ["PS", "EV_EBITDA"],
        "forbidden": [],
        "notes": "无行业匹配，使用通用PE估值",
        "asset_type": "general",
    },
}

# ── 二级行业补丁（覆盖一级规则）────────────────────────────────────────────
INDUSTRY_L2_OVERRIDES = {
    "保险": {"primary": ["PEV", "PB"], "alternative": ["PE"], "forbidden": ["PS"]},
    "券商": {"primary": ["PB", "PE"], "alternative": ["ROE_PB"], "forbidden": ["PS"]},
    "创新药": {"primary": ["rNPV", "PS"], "alternative": ["DCF"], "forbidden": ["PE"]},
    "CXO": {"primary": ["PE", "PS"], "alternative": ["DCF"], "forbidden": []},
    "新能源整车": {"primary": ["PS", "PE"], "alternative": ["DCF"], "forbidden": []},
    "光伏": {"primary": ["PE", "PS"], "alternative": ["EV_EBITDA"], "forbidden": []},
    "储能": {"primary": ["PE", "PS"], "alternative": ["DCF"], "forbidden": []},
    "白酒": {"primary": ["PE", "DCF"], "alternative": ["PS"], "forbidden": []},
    "养殖": {"primary": ["PS", "PB"], "alternative": ["PE"], "forbidden": []},
}


def select_method_for_industry(industry_l1: str, industry_l2: str = None) -> dict:
    """根据申万行业返回推荐的估值方法规则

    优先级：二级行业补丁 > 一级行业矩阵 > 默认规则

    Returns:
        dict with keys: primary, alternative, forbidden, notes, asset_type
    """
    # 尝试二级行业补丁
    if industry_l2 and industry_l2 in INDUSTRY_L2_OVERRIDES:
        patch = INDUSTRY_L2_OVERRIDES[industry_l2]
        # 从一级行业获取基础规则，再覆盖
        base = INDUSTRY_METHOD_MATRIX.get(industry_l1, INDUSTRY_METHOD_MATRIX["_default"]).copy()
        base.update(patch)
        base["_matched_by"] = f"L2:{industry_l2}"
        return base

    # 精确匹配一级行业
    if industry_l1 and industry_l1 in INDUSTRY_METHOD_MATRIX:
        result = INDUSTRY_METHOD_MATRIX[industry_l1].copy()
        result["_matched_by"] = f"L1:{industry_l1}"
        return result

    # 模糊匹配（包含关键词）
    for key, rule in INDUSTRY_METHOD_MATRIX.items():
        if key.startswith("_"):
            continue
        if key in (industry_l1 or "") or (industry_l1 or "") in key:
            result = rule.copy()
            result["_matched_by"] = f"fuzzy:{key}"
            return result

    # 默认规则
    result = INDUSTRY_METHOD_MATRIX["_default"].copy()
    result["_matched_by"] = "default"
    return result


def format_method_matrix_for_prompt(industry_l1: str, industry_l2: str = None) -> str:
    """格式化行业-方法矩阵为 prompt 注入文本

    只输出目标行业 + 相邻行业（asset_type相同），控制 token 数量。
    """
    rule = select_method_for_industry(industry_l1, industry_l2)
    matched_by = rule.get("_matched_by", "default")

    lines = [
        f"## 行业估值方法匹配（{industry_l1}{'/' + industry_l2 if industry_l2 else ''}）",
        f"匹配依据: {matched_by}",
        f"推荐方法(首选): {', '.join(rule.get('primary', ['PE']))}",
        f"备选方法: {', '.join(rule.get('alternative', []) or ['—'])}",
        f"禁用方法: {', '.join(rule.get('forbidden', []) or ['无'])}",
        f"行业特点: {rule.get('notes', '')}",
        "",
        "## 跨行业禁用规则（硬性约束，不可违反）",
        "- 资源/大宗商品(煤炭/石油/有色金属): 禁止 PS",
        "- 金融(银行/非银金融): 禁止 PS；银行禁止 EV_EBITDA",
        "- 亏损公司任意行业: 禁止 PE（改用 PS 或 PB）",
        "- 重资产周期(钢铁/交通运输): 禁止 PS",
        "- 房地产: 禁止 PS 和 EV_EBITDA",
    ]
    return "\n".join(lines)


def check_method_compliance(method: str, industry_l1: str, industry_l2: str = None) -> tuple[bool, str]:
    """检查估值方法是否符合行业规则

    Returns:
        (is_compliant: bool, reason: str)
    """
    rule = select_method_for_industry(industry_l1, industry_l2)
    forbidden = rule.get("forbidden", [])

    # 标准化方法名
    method_normalized = method.upper().replace("/", "_").replace("-", "_").replace(" ", "_")
    # 别名映射
    aliases = {
        "EV/EBITDA": "EV_EBITDA",
        "EVEBITDA": "EV_EBITDA",
        "EV-EBITDA": "EV_EBITDA",
    }
    method_normalized = aliases.get(method_normalized, method_normalized)

    for f in forbidden:
        f_normalized = f.upper().replace("/", "_").replace("-", "_")
        if method_normalized == f_normalized:
            return False, f"方法 {method} 在行业 {industry_l1} 中被禁用（规则: {rule.get('notes', '')}）"

    return True, "合规"

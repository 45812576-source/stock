"""KG 实体名规范化清洗器

规则化清洗（不依赖AI，纯正则+字典），在写入 KG 前统一过一遍。
返回 None 表示该实体应丢弃。

嵌入点：
  - kg_extractor_pipeline._reduce_and_write() 写入前
  - kg_inspector.cross_complete() 写入前
  - kg_inspector.name_cleanup() 存量清洗
"""
import re
import logging

logger = logging.getLogger(__name__)

# ── 通用：动词/事件后缀 ────────────────────────────────────────────────────────
_EVENT_SUFFIXES = re.compile(
    r'(争夺|之争|跑通|落地|爆发|加速|升级|深化|推进|启动|突破|崛起|来袭|来临|开启|开始|持续|延续)$'
)

# ── 通用：模糊后缀 ─────────────────────────────────────────────────────────────
_FUZZY_SUFFIXES = re.compile(
    r'(及其他行业|及其他|等行业|等领域|等|相关行业|相关领域|相关产业)$'
)

# ── commodity/energy：价格/行情后缀 ───────────────────────────────────────────
_PRICE_SUFFIXES = re.compile(
    r'(价格|走势|行情|期货|现货价|报价|价|涨价|降价|价格走势|价格上涨|价格下跌)$'
)

# ── macro_indicator：地理前缀 + 频率后缀 ──────────────────────────────────────
_MACRO_GEO_PREFIX = re.compile(
    r'^(中国|美国|欧洲|欧元区|日本|全球|主要经济体|新兴市场|发达国家|发展中国家)'
)
_MACRO_FREQ_SUFFIX = re.compile(
    r'(同比|环比|增速|增长率|变动|增幅|降幅|数据|指数)$'
)

# ── 地理定语前缀（commodity/energy/industry 适用） ────────────────────────────
_GEO_PREFIX = re.compile(
    r'^(国内|国际|中国|美国|欧洲|日本|韩国|印度|哈萨克斯坦|澳大利亚|巴西|俄罗斯|南非|印尼|东南亚|中东|非洲)'
)

# ── theme：地理前缀 ────────────────────────────────────────────────────────────
_THEME_GEO_PREFIX = re.compile(
    r'^(中国|国内|国际|全球|美国|欧洲)'
)

# ── industry_chain：简称补全 ──────────────────────────────────────────────────
# 以"链"结尾但不是"产业链"/"供应链"/"价值链"的，补全为"产业链"
_CHAIN_ABBR = re.compile(r'^(.+)链$')
_CHAIN_VALID_ENDINGS = ('产业链', '供应链', '价值链')

# ── industry：误删保护 ────────────────────────────────────────────────────────
# 这些词是合法的行业分类，不应被删除（即使含"服务"等泛化词）
_INDUSTRY_WHITELIST = {
    '商务服务', '信息服务', '软件服务', '金融服务', '医疗服务', '教育服务',
    '物流服务', '餐饮服务', '酒店服务', '旅游服务', '家政服务', '养老服务',
    '互联网服务', '云服务', '数据服务', '安全服务', '检测服务', '咨询服务',
    '工程服务', '环保服务', '农业服务', '文化服务', '体育服务', '健康服务',
}


def normalize_entity_name(entity_type: str, name: str):
    """清洗实体名，返回 None 表示应丢弃该实体

    Args:
        entity_type: 实体类型（industry/theme/commodity/energy/macro_indicator/industry_chain等）
        name: 原始实体名

    Returns:
        清洗后的实体名，或 None（应丢弃）
    """
    if not name:
        return None
    name = name.strip()

    # ── 1. 通用：去掉动词/事件后缀 ──────────────────────────────────────────
    name = _EVENT_SUFFIXES.sub('', name).strip()

    # ── 2. 通用：去掉模糊后缀 ────────────────────────────────────────────────
    name = _FUZZY_SUFFIXES.sub('', name).strip()

    # ── 3. 按类型特殊处理 ────────────────────────────────────────────────────

    if entity_type in ('commodity', 'energy'):
        # 去掉价格/行情后缀
        name = _PRICE_SUFFIXES.sub('', name).strip()
        # 去掉地理定语前缀
        name = _GEO_PREFIX.sub('', name).strip()

    elif entity_type == 'macro_indicator':
        # 去掉地理前缀
        name = _MACRO_GEO_PREFIX.sub('', name).strip()
        # 去掉频率后缀
        name = _MACRO_FREQ_SUFFIX.sub('', name).strip()

    elif entity_type == 'industry_chain':
        # 简称补全：火箭链 → 火箭产业链
        m = _CHAIN_ABBR.match(name)
        if m and not any(name.endswith(e) for e in _CHAIN_VALID_ENDINGS):
            name = m.group(1) + '产业链'

    elif entity_type == 'theme':
        # 去掉地理前缀
        name = _THEME_GEO_PREFIX.sub('', name).strip()

    elif entity_type == 'industry':
        # 保护白名单（按服务类型细分的合法行业）
        if name in _INDUSTRY_WHITELIST:
            return name
        # 去掉地理定语前缀
        name = _GEO_PREFIX.sub('', name).strip()

    # ── 4. 长度检查 ──────────────────────────────────────────────────────────
    if len(name) < 2:
        return None

    return name


def normalize_entity_name_safe(entity_type: str, name: str) -> str:
    """安全版本：清洗失败时返回原名（不丢弃）"""
    try:
        result = normalize_entity_name(entity_type, name)
        return result if result is not None else name
    except Exception as e:
        logger.warning(f"normalize_entity_name error: {e}")
        return name

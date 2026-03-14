"""标签聚合器 — 将碎片化标签聚合为分层展示的投资标签

分层结构：
  L1 行业层：来自 stock_info.industry_l1/l2（1-2个）
  L2 投资主题层：AI 判断投资相关度高的主题标签（2-3个）
  L3 事件/观点层：其他标签，按需展开（最多5个）
"""
import json
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# ── 泛化词黑名单：这些词太泛，只能进 L1，不进 L2 ─────────────────────────────

GENERIC_WORDS = {
    # 一级行业（太泛）
    '电子', '计算机', '通信', '传媒', '机械', '电力设备', '汽车', '家电',
    '食品饮料', '医药生物', '农林牧渔', '银行', '非银金融', '房地产',
    '建筑装饰', '建筑材料', '钢铁', '有色', '煤炭', '石油石化', '基础化工',
    '公用事业', '交通运输', '商业贸易', '休闲服务', '纺织服饰', '轻工制造',
    '国防军工', '综合',
    # 泛化词
    '科技', '制造业', '新兴产业', '传统产业', '蓝筹', '成长股', '价值股',
    '大盘', '小盘', '中盘', '主板', '创业板', '科创板', '北交所',
    'A股', '港股', '美股', '上市公司', '龙头企业', '行业龙头',
}

# ── 同义词映射：常见同义词归一到标准写法 ─────────────────────────────────────

SYNONYM_MAP = {
    # AI 相关
    '人工智能': 'AI',
    '人工智能芯片': 'AI芯片',
    '算力芯片': 'AI芯片',
    'GPU': 'AI芯片',
    'AI算力': 'AI芯片',
    '大模型': 'AI大模型',
    'GPT': 'AI大模型',
    'ChatGPT': 'AI大模型',
    'AIGC': 'AI应用',
    '生成式AI': 'AI应用',
    # 新能源车
    '新能源汽车': '新能源车',
    '电动车': '新能源车',
    '电动汽车': '新能源车',
    'EV': '新能源车',
    '锂电池': '锂电',
    '动力电池': '锂电',
    '储能电池': '储能',
    # 光伏
    '太阳能': '光伏',
    '光伏组件': '光伏',
    '光伏电站': '光伏',
    # 半导体
    '芯片': '半导体',
    '集成电路': '半导体',
    'IC': '半导体',
    '晶圆': '半导体',
    # 医药
    '创新药': '创新药',
    '生物药': '生物医药',
    '抗体药': '生物医药',
    'CXO': '医药外包',
    'CRO': '医药外包',
    'CDMO': '医药外包',
    # 消费
    '白酒': '白酒',
    '茅台': '白酒',
    '免税': '免税',
    '跨境电商': '跨境电商',
    # 其他
    '机器人': '机器人',
    '人形机器人': '机器人',
    '工业机器人': '机器人',
    '服务机器人': '机器人',
    '军工': '国防军工',
    '军工电子': '国防军工',
    '元宇宙': '元宇宙',
    'VR': 'VR/AR',
    'AR': 'VR/AR',
    '虚拟现实': 'VR/AR',
}


def _normalize_tag(tag: str) -> str:
    """标准化标签：去空格、同义词归一"""
    tag = (tag or '').strip()
    return SYNONYM_MAP.get(tag, tag)


def _hash_tags(tags: list) -> str:
    """计算标签列表的 hash，用于判断是否需要重新聚合"""
    content = json.dumps(sorted(tags), ensure_ascii=False)
    return hashlib.md5(content.encode()).hexdigest()


def _get_industry_tags(stock_code: str) -> list:
    """从 stock_info 获取 L1 行业标签"""
    try:
        from utils.db_utils import execute_query
        rows = execute_query(
            "SELECT industry_l1, industry_l2 FROM stock_info WHERE stock_code=%s",
            [stock_code]
        )
        if not rows:
            return []
        r = rows[0]
        tags = []
        if r.get('industry_l2'):
            tags.append(r['industry_l2'])
        if r.get('industry_l1') and r['industry_l1'] not in tags:
            tags.append(r['industry_l1'])
        return tags[:2]
    except Exception:
        return []


def _get_cached_tags(stock_code: str, raw_tags_hash: str) -> Optional[dict]:
    """从缓存获取聚合结果"""
    try:
        from utils.db_utils import execute_query
        rows = execute_query(
            """SELECT l1_tags, l2_tags, l3_tags, raw_tags_hash, updated_at
               FROM stock_tags_cache WHERE stock_code=%s""",
            [stock_code]
        )
        if not rows:
            return None
        r = rows[0]
        # hash 匹配且缓存未过期（24小时）
        if r['raw_tags_hash'] == raw_tags_hash:
            updated = r.get('updated_at')
            if updated and (datetime.now() - updated) < timedelta(hours=24):
                return {
                    'L1': json.loads(r['l1_tags'] or '[]'),
                    'L2': json.loads(r['l2_tags'] or '[]'),
                    'L3': json.loads(r['l3_tags'] or '[]'),
                }
        return None
    except Exception:
        return None


def _save_cached_tags(stock_code: str, raw_tags_hash: str, result: dict):
    """保存聚合结果到缓存"""
    try:
        from utils.db_utils import execute_insert
        execute_insert(
            """INSERT INTO stock_tags_cache (stock_code, l1_tags, l2_tags, l3_tags, raw_tags_hash)
               VALUES (%s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
                 l1_tags=VALUES(l1_tags), l2_tags=VALUES(l2_tags),
                 l3_tags=VALUES(l3_tags), raw_tags_hash=VALUES(raw_tags_hash), updated_at=NOW()""",
            [stock_code,
             json.dumps(result.get('L1', []), ensure_ascii=False),
             json.dumps(result.get('L2', []), ensure_ascii=False),
             json.dumps(result.get('L3', []), ensure_ascii=False),
             raw_tags_hash]
        )
    except Exception as e:
        logger.warning(f"保存标签缓存失败: {e}")


def _ai_aggregate_tags(raw_tags: list, stock_code: str) -> dict:
    """调用 AI 聚合标签，返回分层结果"""
    if not raw_tags:
        return {'L1': [], 'L2': [], 'L3': []}

    # 去重 + 标准化
    unique_tags = list(set(_normalize_tag(t) for t in raw_tags if t))
    if not unique_tags:
        return {'L1': [], 'L2': [], 'L3': []}

    system_prompt = """你是股票投资专家。请将以下标签按投资价值分层：

L2 投资主题层：对投资决策有直接帮助的主题标签（如具体赛道、热点概念）
L3 事件/观点层：其他辅助性标签（如公司事件、泛化描述）

规则：
1. 太泛的词（如"电子"、"科技"）只能放 L3
2. 同类标签只保留最有代表性的一个
3. L2 最多 3 个，L3 最多 5 个
4. 对 L2 标签打分（投资相关度 1-100）

返回 JSON：
{"L2":[{"name":"标签","score":85}],"L3":["标签1","标签2"]}"""

    user_msg = f"股票代码：{stock_code}\n待分类标签：{json.dumps(unique_tags[:20], ensure_ascii=False)}"

    try:
        from utils.model_router import call_model_json
        result = call_model_json('hotspot', system_prompt, user_msg, max_tokens=500, timeout=30)

        l2 = []
        if isinstance(result, dict):
            for item in result.get('L2', []):
                if isinstance(item, dict) and item.get('name'):
                    l2.append({'name': item['name'], 'score': item.get('score', 50)})
                elif isinstance(item, str):
                    l2.append({'name': item, 'score': 50})
            l3 = result.get('L3', [])
            if isinstance(l3, dict):
                l3 = l3.get('tags', [])
        else:
            l3 = []

        return {'L1': [], 'L2': l2[:3], 'L3': l3[:5]}
    except Exception as e:
        logger.warning(f"AI 聚合标签失败: {e}")
        # 降级：直接返回原始标签作为 L3
        return {'L1': [], 'L2': [], 'L3': unique_tags[:5]}


def aggregate_tags(stock_code: str, raw_tags: list = None) -> dict:
    """聚合标签，返回分层结果

    Args:
        stock_code: 股票代码
        raw_tags: 原始标签列表（可选，不传则自动查询）

    Returns:
        {
            'L1': ['半导体', '电子'],  # 行业层
            'L2': [                    # 投资主题层
                {'name': 'AI芯片', 'score': 85, 'heat': 12},
                {'name': '机器人', 'score': 75, 'heat': 8}
            ],
            'L3': ['业绩增长', '产能扩张']  # 事件层
        }
    """
    # 1. 获取 L1 行业标签
    l1_tags = _get_industry_tags(stock_code)

    # 2. 如果没有传入 raw_tags，从数据库查询
    if raw_tags is None:
        try:
            from utils.db_utils import execute_query
            rows = execute_query(
                """SELECT DISTINCT ci.tags_json
                   FROM item_companies ic JOIN cleaned_items ci ON ic.cleaned_item_id=ci.id
                   WHERE ic.stock_code=%s AND ci.cleaned_at >= DATE_SUB(NOW(), INTERVAL 60 DAY)
                   ORDER BY ci.cleaned_at DESC LIMIT 30""",
                [stock_code]
            )
            raw_tags = []
            for r in (rows or []):
                try:
                    tags = json.loads(r.get('tags_json') or '[]')
                    raw_tags.extend(tags)
                except:
                    pass
        except Exception:
            raw_tags = []

    # 3. 计算 hash，检查缓存
    raw_tags_hash = _hash_tags(raw_tags)
    cached = _get_cached_tags(stock_code, raw_tags_hash)
    if cached:
        cached['L1'] = l1_tags
        return cached

    # 4. 调用 AI 聚合
    result = _ai_aggregate_tags(raw_tags, stock_code)
    result['L1'] = l1_tags

    # 5. 添加热度信息（出现次数）
    tag_counts = {}
    for t in raw_tags:
        t = _normalize_tag(t)
        tag_counts[t] = tag_counts.get(t, 0) + 1

    for item in result.get('L2', []):
        name = item.get('name', '')
        item['heat'] = tag_counts.get(name, 0)

    # 6. 保存缓存
    _save_cached_tags(stock_code, raw_tags_hash, result)

    return result


def get_display_tags(stock_code: str) -> dict:
    """获取个股页面展示用的分层标签

    Returns:
        {
            'core': [  # 核心标签（L1 + L2），默认展示
                {'name': '半导体', 'type': 'industry'},
                {'name': 'AI芯片', 'type': 'theme', 'score': 85, 'heat': 12},
            ],
            'more': [  # 更多标签（L3），点击展开
                {'name': '业绩增长', 'type': 'event'},
            ]
        }
    """
    layered = aggregate_tags(stock_code)

    core = []
    for t in layered.get('L1', []):
        core.append({'name': t, 'type': 'industry'})

    for item in layered.get('L2', []):
        core.append({
            'name': item.get('name', ''),
            'type': 'theme',
            'score': item.get('score', 50),
            'heat': item.get('heat', 0)
        })

    more = []
    for t in layered.get('L3', []):
        more.append({'name': t, 'type': 'event'})

    return {'core': core, 'more': more}

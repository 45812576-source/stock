"""chain_sync.py — 自动将 daily_intel_stocks 中的新股票同步到 chain_config.py

流程：
1. 查询 daily_intel_stocks 中当天出现的股票（去重）
2. 过滤掉已在 chain_config 里登记的
3. 对剩余股票：先按关键词规则快速匹配，匹配不上的全部交给 AI
4. AI 判断两种情况：
   a. 归属现有产业链/层 → 直接追加到对应 tier["stocks"]
   b. 不属于任何现有链 → AI 给出新链名/层名/描述 → 在 CHAINS 里新建整个链块
5. 所有写入的股票都标记为 STOCK_TAGS["news"]
"""

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

_CHAIN_CONFIG_PATH = Path(__file__).parent / "chain_config.py"

# ── 关键词快速匹配规则（命中则跳过 AI，降低 token 消耗）─────────────
_KEYWORD_RULES = [
    ("新能源车", "上上游", ["锂业", "钴业", "锂矿", "钴矿"]),
    ("新能源车", "上游",   ["正极材料", "负极材料", "隔膜", "电解液", "铜箔", "前驱体"]),
    ("新能源车", "中游",   ["动力电池", "锂电池", "电机电控"]),
    ("新能源车", "下游",   ["新能源整车", "汽车零部件"]),
    ("新能源车", "设备",   ["锂电设备"]),
    ("半导体",   "上上游", ["光刻胶", "靶材", "湿电子化学品", "特种气体"]),
    ("半导体",   "上游",   ["半导体设备", "刻蚀设备", "CVD设备"]),
    ("半导体",   "中游设计", ["IC设计", "芯片设计", "集成电路设计"]),
    ("半导体",   "中游制造", ["晶圆代工", "封测"]),
    ("半导体",   "下游",   ["PCB", "被动元件", "MLCC"]),
    ("AI算力基础设施", "上游",  ["AI芯片", "GPU芯片", "NPU"]),
    ("AI算力基础设施", "中游",  ["服务器", "液冷散热", "光模块", "数据中心"]),
    ("AI算力基础设施", "下游",  ["AI应用", "大模型应用"]),
    ("光伏",     "上游",   ["硅料", "多晶硅", "光伏银浆", "光伏玻璃"]),
    ("光伏",     "中游",   ["电池片", "异质结", "TOPCon", "光伏硅片"]),
    ("光伏",     "下游",   ["光伏组件", "光伏逆变器"]),
    ("光伏",     "设备",   ["光伏设备"]),
    ("风电",     "上游",   ["风电铸件", "风电主轴", "风电叶片", "碳纤维"]),
    ("风电",     "中游",   ["风电整机", "风机制造"]),
    ("风电",     "下游",   ["塔筒", "海缆", "海上风电"]),
    ("储能",     "上游",   ["储能电池", "磷酸铁锂"]),
    ("储能",     "中游",   ["储能PCS", "储能变流器"]),
    ("储能",     "下游",   ["储能系统集成", "储能运营"]),
    ("电力设备", "上游",   ["变压器", "高压开关"]),
    ("电力设备", "中游",   ["继电保护", "电网自动化"]),
    ("电力设备", "配套",   ["电力电缆", "电力机器人"]),
    ("电力设备", "终端",   ["低压电器", "智能电表", "充电桩"]),
    ("军工",     "上游",   ["航空钛合金", "军工碳纤维", "高温合金"]),
    ("军工",     "中游",   ["军用连接器", "军用雷达", "红外探测"]),
    ("军工",     "下游",   ["航空发动机", "军用卫星", "军用舰船"]),
    ("消费电子", "上游",   ["消费电子PCB", "FPC软板", "覆铜板"]),
    ("消费电子", "中游",   ["声学器件", "光学模组", "精密结构件"]),
    ("消费电子", "下游",   ["手机ODM", "显示面板", "整机组装"]),
    ("智能驾驶", "感知层", ["车载摄像头", "激光雷达", "毫米波雷达"]),
    ("智能驾驶", "计算层", ["域控制器", "智能座舱"]),
    ("智能驾驶", "执行层", ["线控底盘", "电子转向"]),
    ("能源电力运营", "水电运营",    ["水力发电"]),
    ("能源电力运营", "火电运营",    ["火力发电"]),
    ("能源电力运营", "新能源运营",  ["新能源发电运营"]),
    ("能源电力运营", "天然气/综合", ["城市燃气", "天然气分销"]),
    ("化工新材料", "上游",  ["氟化工", "有机硅", "MDI聚氨酯"]),
    ("化工新材料", "中游",  ["特种气体", "精细化工"]),
    ("化工新材料", "下游",  ["改性塑料", "功能性材料"]),
    ("钢铁基建材料", "上游-矿产", ["铁矿石", "焦煤"]),
    ("钢铁基建材料", "中游-特钢", ["特种钢材", "高温合金钢"]),
    ("钢铁基建材料", "中游-普钢", ["普通钢铁", "不锈钢"]),
    ("钢铁基建材料", "中游-建材", ["水泥", "玻璃纤维", "防水涂料"]),
    ("碳中和环保", "资源回收", ["电池回收", "金属再生"]),
    ("碳中和环保", "危废处理", ["危险废物处理", "工业固废"]),
    ("碳中和环保", "节能减排", ["节能环保", "碳捕集"]),
    ("医药CRO",  "临床前CRO", ["临床前研究", "药物发现CRO"]),
    ("医药CRO",  "临床CRO",   ["临床研究外包", "CRO服务"]),
    ("医药CRO",  "CDMO",      ["CDMO", "合同定制研发生产"]),
    ("稀土永磁", "上游",  ["稀土矿", "稀土分离"]),
    ("稀土永磁", "中游",  ["钕铁硼", "永磁材料"]),
    ("稀土永磁", "下游",  ["永磁电机应用"]),
]

# Material Icons 候选（供新链分配图标）
_ICON_POOL = [
    "category", "layers", "grain", "precision_manufacturing", "biotech",
    "agriculture", "local_shipping", "flight", "anchor", "computer",
    "build", "water", "forest", "medication", "attach_money",
]
_COLOR_POOL = [
    "#0ea5e9", "#d946ef", "#f59e0b", "#10b981", "#6366f1",
    "#ef4444", "#14b8a6", "#f97316", "#8b5cf6", "#ec4899",
]


# ── 辅助：读取当前 chain_config ───────────────────────────────────

def _get_known_stocks() -> set:
    """返回 chain_config 中已登记的所有股票名集合"""
    from config.chain_config import CHAINS
    known = set()
    for chain in CHAINS.values():
        for tier in chain["tiers"].values():
            known.update(tier["stocks"])
    return known


def _get_chain_count() -> int:
    from config.chain_config import CHAINS
    return len(CHAINS)


# ── 关键词快速匹配 ────────────────────────────────────────────────

def _keyword_match(stock_name: str, industry: str) -> tuple[str, str] | None:
    text = (stock_name or "") + " " + (industry or "")
    for chain_name, tier_key, keywords in _KEYWORD_RULES:
        if any(kw in text for kw in keywords):
            return chain_name, tier_key
    return None


# ── AI 分类（支持新建链）─────────────────────────────────────────

def _ai_classify(stocks: list[dict]) -> list[dict]:
    """批量判断股票归属。

    对每只股票返回：
      - 归属现有链：{"stock_name", "chain": "已有链名", "tier": "已有层key", "new_chain": false}
      - 需新建链：  {"stock_name", "chain": "新链名", "tier": "新层key",
                    "tier_label": "层描述", "new_chain": true,
                    "chain_icon": "material_icon", "chain_color": "#hex"}
    """
    if not stocks:
        return []

    from config.chain_config import CHAINS, CHAIN_ORDER
    from utils.model_router import call_model_json

    chain_desc = "\n".join(
        f"- {chain}: " + "、".join(
            f"{tk}({t['label'][:20]})"
            for tk, t in CHAINS[chain]["tiers"].items()
        )
        for chain in CHAIN_ORDER
    )

    stock_lines = "\n".join(
        f"{i+1}. 股票名={s['stock_name']} 代码={s.get('stock_code','')} "
        f"行业={s.get('industry','')} 事件={s.get('event_summary','')[:80]}"
        for i, s in enumerate(stocks)
    )

    icon_hint = "、".join(_ICON_POOL[:8])
    color_hint = "、".join(_COLOR_POOL[:6])

    system = "你是A股产业链分析专家。只输出合法JSON，不输出任何解释。"
    user = f"""已有产业链（链名: 层key(层描述)）：
{chain_desc}

请对以下每只股票判断：
1. 如能归入已有产业链的某个层 → new_chain=false，填写 chain（已有链名）和 tier（已有层key，必须与上面完全一致）
2. 如不属于任何已有产业链 → new_chain=true，自行命名新链名（简短中文，如"消费品牌"、"航运物流"），
   命名新层key（如"上游"/"中游"/"下游"或更具体的名称），给出 tier_label（10字内中文描述），
   从以下选一个 chain_icon：{icon_hint}，
   从以下选一个 chain_color：{color_hint}

{stock_lines}

输出 JSON 数组（顺序与输入一致）：
[
  {{"stock_name":"xxx","chain":"已有链名","tier":"已有层key","new_chain":false}},
  {{"stock_name":"yyy","chain":"新链名","tier":"新层key","tier_label":"层描述","new_chain":true,"chain_icon":"icon","chain_color":"#hex"}},
  ...
]"""

    try:
        result = call_model_json("kg", system, user, max_tokens=3000, timeout=180)
        if isinstance(result, list):
            return result
        for v in result.values():
            if isinstance(v, list):
                return v
    except Exception as e:
        logger.warning(f"[ChainSync] AI分类失败: {e}")
    return []


# ── 写入 chain_config.py ─────────────────────────────────────────

def _write_to_config(
    existing_additions: list[tuple[str, str, str]],
    new_chain_additions: list[dict],
):
    """统一写入入口。

    existing_additions: [(chain_name, tier_key, stock_name), ...]  已有链
    new_chain_additions: [{"chain","tier","tier_label","chain_icon","chain_color","stock_name"}, ...]  新链
    """
    content = _CHAIN_CONFIG_PATH.read_text(encoding="utf-8")

    # 1. 处理已有链的追加
    for chain_name, tier_key, stock_name in existing_additions:
        content = _insert_stock_to_tier(content, chain_name, tier_key, stock_name)

    # 2. 处理新建链
    #    先按链名归组，一条链可能来自多只股票
    new_chains: dict[str, dict] = {}  # chain_name -> {icon, color, tiers: {tier_key: {label, stocks:[]}}}
    for item in new_chain_additions:
        cname = item["chain"]
        tkey  = item["tier"]
        if cname not in new_chains:
            new_chains[cname] = {
                "icon":  item.get("chain_icon", "category"),
                "color": item.get("chain_color", "#64748b"),
                "tiers": {},
            }
        if tkey not in new_chains[cname]["tiers"]:
            new_chains[cname]["tiers"][tkey] = {
                "label":  item.get("tier_label", tkey),
                "stocks": [],
            }
        new_chains[cname]["tiers"][tkey]["stocks"].append(item["stock_name"])

    for cname, chain_def in new_chains.items():
        # 检查该链名是否已存在（可能本批多只股票归同一新链，第一次创建后就存在了）
        if f'"{cname}"' in content:
            # 链已存在（本轮前面刚创建），只追加股票和层
            for tkey, tier_def in chain_def["tiers"].items():
                # 尝试追加到已有层，若层不存在则在该链内新建层
                if f'"{tkey}"' not in content[content.find(f'"{cname}"'):]:
                    content = _insert_new_tier(content, cname, tkey, tier_def["label"], tier_def["stocks"])
                else:
                    for sname in tier_def["stocks"]:
                        content = _insert_stock_to_tier(content, cname, tkey, sname)
        else:
            content = _insert_new_chain(content, cname, chain_def)
            logger.info(f"[ChainSync] 新建产业链: {cname} ({list(chain_def['tiers'].keys())})")

    _CHAIN_CONFIG_PATH.write_text(content, encoding="utf-8")

    # 3. 写 STOCK_TAGS
    all_additions = (
        [(c, t, s) for c, t, s in existing_additions] +
        [(item["chain"], item["tier"], item["stock_name"]) for item in new_chain_additions]
    )
    _append_stock_tags(all_additions)


def _insert_stock_to_tier(content: str, chain_name: str, tier_key: str, stock_name: str) -> str:
    """在已有 chain/tier 的 stocks 列表末尾追加股票名，返回新 content"""
    chain_start = content.find(f'"{chain_name}"')
    if chain_start == -1:
        logger.warning(f"[ChainSync] 未找到链: {chain_name}")
        return content

    pattern = re.compile(
        r'("' + re.escape(tier_key) + r'"\s*:\s*\{[^}]*?"stocks"\s*:\s*\[)(.*?)(\])',
        re.DOTALL,
    )
    sub = content[chain_start:]
    m = pattern.search(sub)
    if not m:
        logger.warning(f"[ChainSync] 未找到 {chain_name}/{tier_key} 的 stocks 列表")
        return content

    existing = m.group(2)
    if f'"{stock_name}"' in existing:
        return content  # 已存在

    last_quote = existing.rfind('"')
    if last_quote == -1:
        replacement = m.group(1) + f'"{stock_name}"' + m.group(3)
    else:
        indent_m = re.search(r'\n(\s+)"[^"]+"\s*,?\s*$', existing)
        indent = indent_m.group(1) if indent_m else "                    "
        new_stocks = existing.rstrip() + f',  # auto-added\n{indent}"{stock_name}"'
        replacement = m.group(1) + new_stocks + m.group(3)

    abs_start = chain_start + m.start()
    abs_end   = chain_start + m.end()
    logger.info(f"[ChainSync] 追加: {chain_name}/{tier_key} ← {stock_name}")
    return content[:abs_start] + replacement + content[abs_end:]


def _insert_new_tier(content: str, chain_name: str, tier_key: str, tier_label: str, stocks: list[str]) -> str:
    """在已有 chain 内新建一个 tier 块，插入到 tiers 字典末尾"""
    chain_start = content.find(f'"{chain_name}"')
    if chain_start == -1:
        return content

    # 找到该链的 "tiers": { ... } 块的结束 }
    tiers_pos = content.find('"tiers"', chain_start)
    if tiers_pos == -1:
        return content
    brace_start = content.find('{', tiers_pos)
    if brace_start == -1:
        return content

    depth = 1
    i = brace_start + 1
    while i < len(content) and depth > 0:
        if content[i] == '{':
            depth += 1
        elif content[i] == '}':
            depth -= 1
        i += 1
    tiers_end = i - 1  # 指向 tiers 字典的 }

    stocks_repr = ',\n                    '.join(f'"{s}"' for s in stocks)
    new_tier = (
        f'\n            "{tier_key}": {{\n'
        f'                "label": "{tier_label}",\n'
        f'                "stocks": [{stocks_repr}],  # auto-added\n'
        f'            }},'
    )
    logger.info(f"[ChainSync] 新建层: {chain_name}/{tier_key}")
    return content[:tiers_end] + new_tier + '\n        ' + content[tiers_end:]


def _insert_new_chain(content: str, chain_name: str, chain_def: dict) -> str:
    """在 CHAINS 字典末尾新建完整链块，并追加到 CHAIN_ORDER"""
    # 构建 tiers 字符串
    tiers_str = ""
    for tier_key, tier in chain_def["tiers"].items():
        stocks_repr = ',\n                    '.join(f'"{s}"' for s in tier["stocks"])
        tiers_str += (
            f'            "{tier_key}": {{\n'
            f'                "label": "{tier["label"]}",\n'
            f'                "stocks": [{stocks_repr}],  # auto-added\n'
            f'            }},\n'
        )

    new_block = (
        f'\n    "{chain_name}": {{\n'
        f'        "icon": "{chain_def["icon"]}",\n'
        f'        "color": "{chain_def["color"]}",\n'
        f'        "tiers": {{\n'
        f'{tiers_str}'
        f'        }},\n'
        f'    }},\n'
    )

    # 找 CHAINS = { ... } 的结束 }
    chains_pos = content.find("CHAINS = {")
    if chains_pos == -1:
        logger.warning("[ChainSync] 未找到 CHAINS = {")
        return content

    brace_start = content.find('{', chains_pos)
    depth = 1
    i = brace_start + 1
    while i < len(content) and depth > 0:
        if content[i] == '{':
            depth += 1
        elif content[i] == '}':
            depth -= 1
        i += 1
    chains_end = i - 1  # 指向 CHAINS 的 }

    content = content[:chains_end] + new_block + content[chains_end:]

    # 追加到 CHAIN_ORDER 列表
    order_m = re.search(r'(CHAIN_ORDER\s*=\s*\[)(.*?)(\])', content, re.DOTALL)
    if order_m:
        new_order = order_m.group(2).rstrip() + f'\n    "{chain_name}",  # auto-added\n'
        content = content[:order_m.start()] + order_m.group(1) + new_order + order_m.group(3) + content[order_m.end():]

    return content


def _append_stock_tags(additions: list[tuple[str, str, str]]):
    """在 STOCK_TAGS 字典中追加 news 标记"""
    if not additions:
        return

    content = _CHAIN_CONFIG_PATH.read_text(encoding="utf-8")
    tags_start = content.find("STOCK_TAGS = {")
    if tags_start == -1:
        logger.warning("[ChainSync] 未找到 STOCK_TAGS")
        return

    brace_pos = tags_start + len("STOCK_TAGS = {")
    depth = 1
    i = brace_pos
    while i < len(content) and depth > 0:
        if content[i] == '{':
            depth += 1
        elif content[i] == '}':
            depth -= 1
        i += 1
    end_pos = i - 1

    for _, _, stock_name in additions:
        if f'"{stock_name}"' in content[tags_start:end_pos]:
            continue
        insert = f'\n    "{stock_name}": "news",  # auto-added'
        content = content[:end_pos] + insert + "\n" + content[end_pos:]
        end_pos += len(insert) + 1
        logger.info(f"[ChainSync] STOCK_TAGS ← news: {stock_name}")

    _CHAIN_CONFIG_PATH.write_text(content, encoding="utf-8")


# ── 主入口 ────────────────────────────────────────────────────────

def run_chain_sync(scan_date: str = None) -> dict:
    """同步当天 daily_intel_stocks 新股票到 chain_config.py

    Returns: {added_existing: int, added_new_chain: int, skipped: int}
    """
    from utils.db_utils import execute_cloud_query
    from datetime import date as date_cls

    if not scan_date:
        scan_date = str(date_cls.today())

    logger.info(f"[ChainSync] 开始同步 scan_date={scan_date}")

    rows = execute_cloud_query(
        """SELECT DISTINCT stock_name, stock_code, industry,
                  MAX(event_type) AS event_type,
                  MAX(event_summary) AS event_summary
           FROM daily_intel_stocks
           WHERE scan_date = %s AND stock_name IS NOT NULL AND stock_name != ''
           GROUP BY stock_name, stock_code, industry""",
        [scan_date],
    ) or []

    if not rows:
        logger.info("[ChainSync] 当天无数据，跳过")
        return {"added_existing": 0, "added_new_chain": 0, "skipped": 0}

    known = _get_known_stocks()
    unknown = [r for r in rows if r["stock_name"] not in known]
    skipped = len(rows) - len(unknown)

    if not unknown:
        logger.info(f"[ChainSync] {len(rows)} 只股票均已登记，跳过")
        return {"added_existing": 0, "added_new_chain": 0, "skipped": skipped}

    logger.info(f"[ChainSync] 发现 {len(unknown)} 只未登记股票")

    # 1. 关键词快速匹配
    existing_additions: list[tuple[str, str, str]] = []
    ai_pending: list[dict] = []

    for r in unknown:
        match = _keyword_match(r["stock_name"], r.get("industry", ""))
        if match:
            existing_additions.append((match[0], match[1], r["stock_name"]))
        else:
            ai_pending.append(r)

    # 2. AI 处理剩余
    new_chain_additions: list[dict] = []

    if ai_pending:
        ai_results = _ai_classify(ai_pending)
        for i, res in enumerate(ai_results):
            stock_name = res.get("stock_name") or ai_pending[i]["stock_name"]
            if not res.get("chain") or not res.get("tier"):
                logger.info(f"[ChainSync] AI未能分类: {stock_name}")
                continue
            if res.get("new_chain"):
                new_chain_additions.append({
                    "chain":       res["chain"],
                    "tier":        res["tier"],
                    "tier_label":  res.get("tier_label", res["tier"]),
                    "chain_icon":  res.get("chain_icon", "category"),
                    "chain_color": res.get("chain_color", "#64748b"),
                    "stock_name":  stock_name,
                })
            else:
                existing_additions.append((res["chain"], res["tier"], stock_name))

    # 3. 写入文件
    _write_to_config(existing_additions, new_chain_additions)

    # 4. reload 模块
    try:
        import importlib
        import config.chain_config as _cc
        importlib.reload(_cc)
    except Exception as e:
        logger.warning(f"[ChainSync] reload 失败（下次启动生效）: {e}")

    result = {
        "added_existing":  len(existing_additions),
        "added_new_chain": len(new_chain_additions),
        "skipped":         skipped,
    }
    logger.info(f"[ChainSync] 完成: {result}")
    return result

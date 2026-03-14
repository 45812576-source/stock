"""chain_sync.py — 自动将 daily_intel_stocks 中的新股票同步到 chain_config.py

流程：
1. 查询 daily_intel_stocks 中当天出现的股票（去重）
2. 过滤掉已在 chain_config 里登记的
3. 对剩余股票：先按关键词规则匹配产业链/层，匹配不上的批量交给 AI 判断
4. 将结果写入 config/chain_config.py 对应 tier["stocks"] 列表
"""

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# chain_config.py 文件路径
_CHAIN_CONFIG_PATH = Path(__file__).parent / "chain_config.py"

# ── 关键词匹配规则 ────────────────────────────────────────────────
# 格式：(chain_name, tier_key, [关键词列表])
# industry 字段或 stock_name 包含关键词时匹配
_KEYWORD_RULES = [
    ("新能源车", "上上游", ["锂", "钴", "镍", "锂业", "钴业"]),
    ("新能源车", "上游",   ["正极", "负极", "隔膜", "电解液", "铜箔", "前驱体"]),
    ("新能源车", "中游",   ["动力电池", "锂电池", "电机", "电控"]),
    ("新能源车", "下游",   ["整车", "汽车零部件", "新能源汽车"]),
    ("新能源车", "设备",   ["锂电设备", "卷绕", "涂布"]),
    ("半导体",   "上上游", ["硅片", "靶材", "光刻胶", "湿电子", "特气", "电子气体"]),
    ("半导体",   "上游",   ["半导体设备", "刻蚀", "CVD", "单晶炉", "测试设备"]),
    ("半导体",   "中游设计", ["芯片设计", "IC设计", "集成电路设计"]),
    ("半导体",   "中游制造", ["晶圆", "封测", "封装"]),
    ("半导体",   "下游",   ["PCB", "电路板", "被动元件", "MLCC", "电容"]),
    ("AI算力基础设施", "上游",  ["AI芯片", "GPU", "NPU"]),
    ("AI算力基础设施", "中游",  ["服务器", "数据中心", "液冷", "散热", "光通信", "算力"]),
    ("AI算力基础设施", "下游",  ["AI应用", "云服务", "大模型"]),
    ("光伏",     "上游",   ["硅料", "多晶硅", "光伏辅材", "银浆", "光伏玻璃"]),
    ("光伏",     "中游",   ["硅片", "电池片", "异质结", "TOPCon"]),
    ("光伏",     "下游",   ["光伏组件", "逆变器", "组件"]),
    ("光伏",     "设备",   ["光伏设备", "丝网印刷", "镀膜"]),
    ("风电",     "上游",   ["铸件", "主轴", "叶片", "碳纤维", "玻纤", "风电轴承"]),
    ("风电",     "中游",   ["风电整机", "风机"]),
    ("风电",     "下游",   ["塔筒", "海缆", "升压站"]),
    ("储能",     "上游",   ["储能电池", "磷酸铁锂"]),
    ("储能",     "中游",   ["PCS", "储能逆变", "变流器"]),
    ("储能",     "下游",   ["储能系统", "储能集成", "储能运营"]),
    ("电力设备", "上游",   ["变压器", "高压开关", "发电设备"]),
    ("电力设备", "中游",   ["继电保护", "电网自动化", "电力信息化"]),
    ("电力设备", "配套",   ["电力电缆", "电力机器人", "电力工程"]),
    ("电力设备", "终端",   ["低压电器", "智能电表", "充电桩"]),
    ("军工",     "上游",   ["钛合金", "碳纤维", "特钢", "高温合金"]),
    ("军工",     "中游",   ["连接器", "雷达", "红外", "军用电子"]),
    ("军工",     "下游",   ["航空发动机", "舰船", "卫星", "战斗机"]),
    ("消费电子", "上游",   ["FPC", "覆铜板", "消费电子PCB"]),
    ("消费电子", "中游",   ["声学", "光学模组", "结构件", "精密制造"]),
    ("消费电子", "下游",   ["手机ODM", "显示模组", "面板"]),
    ("智能驾驶", "感知层", ["车载摄像头", "激光雷达", "毫米波雷达"]),
    ("智能驾驶", "计算层", ["域控制器", "智能座舱", "自动驾驶算法"]),
    ("智能驾驶", "执行层", ["线控底盘", "电子转向", "智能驾驶零部件"]),
    ("能源电力运营", "水电运营",   ["水力发电", "水电"]),
    ("能源电力运营", "火电运营",   ["火力发电", "火电"]),
    ("能源电力运营", "新能源运营", ["新能源发电", "风光运营"]),
    ("能源电力运营", "天然气/综合", ["天然气", "城市燃气", "LNG"]),
    ("化工新材料", "上游",  ["氟化工", "有机硅", "MDI", "基础化工"]),
    ("化工新材料", "中游",  ["特种气体", "精细化工"]),
    ("化工新材料", "下游",  ["改性塑料", "功能材料", "染料"]),
    ("钢铁基建材料", "上游-矿产", ["铁矿石", "焦煤"]),
    ("钢铁基建材料", "中游-特钢", ["特种钢", "特钢", "高温合金"]),
    ("钢铁基建材料", "中游-普钢", ["普通钢铁", "不锈钢"]),
    ("钢铁基建材料", "中游-建材", ["水泥", "玻纤", "涂料", "建材"]),
    ("碳中和环保", "资源回收", ["废旧电池回收", "金属再生", "再生资源"]),
    ("碳中和环保", "危废处理", ["危废", "工业固废"]),
    ("碳中和环保", "节能减排", ["节能", "碳捕集", "环保工程"]),
    ("医药CRO",  "临床前CRO", ["临床前", "CRO"]),
    ("医药CRO",  "临床CRO",   ["临床研究", "CRO", "医药研发外包"]),
    ("医药CRO",  "CDMO",      ["CDMO", "合同生产", "原料药"]),
    ("稀土永磁", "上游",  ["稀土", "稀土矿", "稀土分离"]),
    ("稀土永磁", "中游",  ["钕铁硼", "永磁材料", "磁材"]),
    ("稀土永磁", "下游",  ["永磁电机", "稀土应用"]),
]


def _get_known_stocks() -> set:
    """返回 chain_config 中已登记的所有股票名集合"""
    from config.chain_config import CHAINS
    known = set()
    for chain in CHAINS.values():
        for tier in chain["tiers"].values():
            known.update(tier["stocks"])
    return known


def _keyword_match(stock_name: str, industry: str) -> tuple[str, str] | None:
    """关键词匹配，返回 (chain_name, tier_key) 或 None"""
    text = (stock_name or "") + " " + (industry or "")
    for chain_name, tier_key, keywords in _KEYWORD_RULES:
        if any(kw in text for kw in keywords):
            return chain_name, tier_key
    return None


def _ai_classify(stocks: list[dict]) -> list[dict]:
    """用 AI 批量判断股票归属，返回 [{stock_name, stock_code, chain, tier, confidence}]

    stocks: [{stock_name, stock_code, industry, event_type, event_summary}]
    """
    if not stocks:
        return []

    from config.chain_config import CHAINS, CHAIN_ORDER
    from utils.model_router import call_model_json

    chain_desc = "\n".join(
        f"- {chain}: " + ", ".join(
            f"{tk}({t['label'][:15]})"
            for tk, t in CHAINS[chain]["tiers"].items()
        )
        for chain in CHAIN_ORDER
    )

    stock_lines = "\n".join(
        f"{i+1}. 股票名={s['stock_name']} 代码={s.get('stock_code','')} 行业={s.get('industry','')} 事件={s.get('event_summary','')[:60]}"
        for i, s in enumerate(stocks)
    )

    system = "你是A股产业链分析专家，根据股票信息判断其所属产业链和层级。只输出JSON。"
    user = f"""产业链配置：
{chain_desc}

请判断以下股票各属于哪条产业链和层级，如果明显不属于以上任何产业链则chain填null。

{stock_lines}

输出格式（JSON数组，顺序与输入一致）：
[
  {{"stock_name": "xxx", "chain": "新能源车" 或 null, "tier": "上游" 或 null}},
  ...
]"""

    try:
        result = call_model_json("kg", system, user, max_tokens=2000, timeout=120)
        if isinstance(result, list):
            return result
        # 有时返回 {"result": [...]}
        for v in result.values():
            if isinstance(v, list):
                return v
    except Exception as e:
        logger.warning(f"[ChainSync] AI分类失败: {e}")
    return []


def _append_to_chain_config(additions: list[tuple[str, str, str]]):
    """将 (chain_name, tier_key, stock_name) 写入 chain_config.py

    在对应 tier 的 stocks 列表末尾追加，保持文件其余内容不变。
    """
    if not additions:
        return

    content = _CHAIN_CONFIG_PATH.read_text(encoding="utf-8")

    for chain_name, tier_key, stock_name in additions:
        # 找到 chain_name 和 tier_key 对应的 stocks 列表，在最后一个元素后追加
        # 匹配模式：在 "tier_key": { ... "stocks": [...] } 块里
        # 用正则找到该 tier 的 stocks 列表结束位置（最后一个 "]" 前）
        # 策略：找到 tier_key 出现位置，再往后找最近的 stocks 列表

        pattern = re.compile(
            r'("' + re.escape(tier_key) + r'"\s*:\s*\{[^}]*?"stocks"\s*:\s*\[)(.*?)(\])',
            re.DOTALL,
        )
        # 先找 chain_name 位置，限定搜索范围
        chain_start = content.find(f'"{chain_name}"')
        if chain_start == -1:
            logger.warning(f"[ChainSync] chain_config 中未找到链: {chain_name}")
            continue

        # 在 chain_name 之后的文本里匹配
        sub = content[chain_start:]
        m = pattern.search(sub)
        if not m:
            logger.warning(f"[ChainSync] 未找到 {chain_name}/{tier_key} 的 stocks 列表")
            continue

        # 检查是否已存在
        existing_stocks_text = m.group(2)
        if f'"{stock_name}"' in existing_stocks_text:
            logger.debug(f"[ChainSync] {stock_name} 已存在于 {chain_name}/{tier_key}")
            continue

        # 在 ] 前插入新股票名
        original = m.group(0)
        # 取最后一个真实股票名后的缩进格式
        last_quote = existing_stocks_text.rfind('"')
        if last_quote == -1:
            # stocks 列表为空
            replacement = m.group(1) + f'"{stock_name}"' + m.group(3)
        else:
            # 在最后一个 " 之后加逗号和新条目（沿用相同缩进）
            indent_match = re.search(r'\n(\s+)"[^"]+"\s*,?\s*$', existing_stocks_text)
            indent = indent_match.group(1) if indent_match else "                    "
            new_stocks = existing_stocks_text.rstrip() + f',  # auto-added\n{indent}"{stock_name}"'
            replacement = m.group(1) + new_stocks + m.group(3)

        abs_start = chain_start + m.start()
        abs_end = chain_start + m.end()
        content = content[:abs_start] + replacement + content[abs_end:]
        logger.info(f"[ChainSync] 写入: {chain_name}/{tier_key} ← {stock_name}")

    _CHAIN_CONFIG_PATH.write_text(content, encoding="utf-8")
    _append_stock_tags(additions)


def _append_stock_tags(additions: list[tuple[str, str, str]]):
    """在 STOCK_TAGS 字典中追加 news 标记的股票"""
    if not additions:
        return

    content = _CHAIN_CONFIG_PATH.read_text(encoding="utf-8")

    # 找到 STOCK_TAGS = { ... } 块的结束 }
    tags_start = content.find("STOCK_TAGS = {")
    if tags_start == -1:
        logger.warning("[ChainSync] chain_config.py 中未找到 STOCK_TAGS，跳过标记写入")
        return

    # 找到对应的结束 }（从 tags_start 往后找第一个独立的 }）
    brace_pos = content.find("STOCK_TAGS = {") + len("STOCK_TAGS = {")
    depth = 1
    i = brace_pos
    while i < len(content) and depth > 0:
        if content[i] == "{":
            depth += 1
        elif content[i] == "}":
            depth -= 1
        i += 1
    end_pos = i - 1  # 指向 }

    for chain_name, tier_key, stock_name in additions:
        tag_line = f'    "{stock_name}": "news",  # auto-added'
        # 检查是否已存在
        if f'"{stock_name}"' in content[tags_start:end_pos]:
            continue
        # 在 } 前插入
        insert = "\n" + tag_line
        content = content[:end_pos] + insert + "\n" + content[end_pos:]
        end_pos += len(insert) + 1  # 维护插入偏移
        logger.info(f"[ChainSync] STOCK_TAGS ← news: {stock_name}")

    _CHAIN_CONFIG_PATH.write_text(content, encoding="utf-8")


def run_chain_sync(scan_date: str = None) -> dict:
    """主入口：同步当天 daily_intel_stocks 新股票到 chain_config.py

    Returns: {added: int, skipped: int, unmatched: int}
    """
    from utils.db_utils import execute_cloud_query
    from datetime import date as date_cls

    if not scan_date:
        scan_date = str(date_cls.today())

    logger.info(f"[ChainSync] 开始同步 scan_date={scan_date}")

    # 1. 查当天出现的所有股票
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
        logger.info("[ChainSync] 当天无 daily_intel_stocks 数据，跳过")
        return {"added": 0, "skipped": 0, "unmatched": 0}

    # 2. 过滤已知股票
    known = _get_known_stocks()
    unknown = [r for r in rows if r["stock_name"] not in known]

    if not unknown:
        logger.info(f"[ChainSync] 当天 {len(rows)} 只股票均已登记，无需同步")
        return {"added": 0, "skipped": len(rows), "unmatched": 0}

    logger.info(f"[ChainSync] 发现 {len(unknown)} 只未登记股票，开始匹配")

    # 3. 关键词匹配
    additions = []
    ai_pending = []

    for r in unknown:
        match = _keyword_match(r["stock_name"], r.get("industry", ""))
        if match:
            additions.append((match[0], match[1], r["stock_name"]))
        else:
            ai_pending.append(r)

    # 4. AI 兜底
    unmatched_count = 0
    if ai_pending:
        ai_results = _ai_classify(ai_pending)
        for i, res in enumerate(ai_results):
            if not res.get("chain") or not res.get("tier"):
                unmatched_count += 1
                logger.info(f"[ChainSync] AI无法归类: {ai_pending[i]['stock_name']}")
                continue
            chain = res["chain"]
            tier = res["tier"]
            stock_name = res.get("stock_name") or ai_pending[i]["stock_name"]
            additions.append((chain, tier, stock_name))

    # 5. 写入 chain_config.py
    _append_to_chain_config(additions)

    # 6. 重新加载模块（让本次进程生效）
    try:
        import importlib
        import config.chain_config as _cc
        importlib.reload(_cc)
    except Exception as e:
        logger.warning(f"[ChainSync] reload chain_config 失败（下次启动生效）: {e}")

    result = {"added": len(additions), "skipped": len(known & {r["stock_name"] for r in rows}), "unmatched": unmatched_count}
    logger.info(f"[ChainSync] 完成: {result}")
    return result

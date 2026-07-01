"""产业链认知上下文提供器 — chain/chain_context_provider.py

统一接口，为个股分析报告、投资主题研究、KG关系抽取等下游模块提供
现成的产业链认知（Baseline）作为起点，避免每次从零推理。

对外接口：
  get_chain_for_stock(stock_code) → str|None
  get_chain_context(chain_name=None, stock_code=None) → dict
  get_chain_context_for_prompt(chain_name=None, stock_code=None, max_chars=6000) → str
  get_chain_seed_for_kg(chain_name) → dict
"""
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── 内部缓存 ─────────────────────────────────────────────────────
_stock_chain_map: dict[str, str] = {}  # stock_code → chain_name
_stock_chain_map_built = False


# ══════════════════════════════════════════════════════════════════
# 1. 股票→产业链映射
# ══════════════════════════════════════════════════════════════════

def _build_stock_chain_map():
    """构建 stock_code → chain_name 映射（首次调用时触发）"""
    global _stock_chain_map, _stock_chain_map_built
    if _stock_chain_map_built:
        return

    from config.chain_config import CHAINS
    from utils.db_utils import execute_query

    # 收集每条链的所有股票名称
    chain_stocks: dict[str, list[str]] = {}
    for chain_name, chain in CHAINS.items():
        names = []
        for tier in chain.get("tiers", {}).values():
            names.extend(tier.get("stocks", []))
        chain_stocks[chain_name] = names

    # 批量查 stock_name → stock_code
    all_names = []
    for names in chain_stocks.values():
        all_names.extend(names)
    all_names = list(set(all_names))

    if not all_names:
        _stock_chain_map_built = True
        return

    # 分批查询（避免 SQL 过长）
    batch_size = 200
    name_code_map: dict[str, str] = {}
    for i in range(0, len(all_names), batch_size):
        batch = all_names[i:i + batch_size]
        ph = ",".join(["?"] * len(batch))
        rows = execute_query(
            f"SELECT stock_code, stock_name FROM stock_info WHERE stock_name IN ({ph})",
            batch,
        ) or []
        for r in rows:
            name_code_map[r["stock_name"]] = r["stock_code"]

    # 构建反向映射：stock_code → chain_name（若一个股票属于多链，取第一个）
    for chain_name, names in chain_stocks.items():
        for name in names:
            code = name_code_map.get(name)
            if code and code not in _stock_chain_map:
                _stock_chain_map[code] = chain_name

    _stock_chain_map_built = True
    logger.info(f"[chain_context] stock→chain 映射已构建: {len(_stock_chain_map)} 条")


def get_chain_for_stock(stock_code: str) -> Optional[str]:
    """根据股票代码查询所属产业链名称。

    Returns:
        产业链名称，如 "半导体"、"新能源车"；不属于任何链返回 None
    """
    _build_stock_chain_map()
    return _stock_chain_map.get(stock_code)


def get_chains_for_stocks(stock_codes: list[str]) -> dict[str, str]:
    """批量查询：{stock_code: chain_name}（仅含有映射的股票）"""
    _build_stock_chain_map()
    return {code: _stock_chain_map[code] for code in stock_codes if code in _stock_chain_map}


# ══════════════════════════════════════════════════════════════════
# 2. 获取产业链认知上下文（结构化）
# ══════════════════════════════════════════════════════════════════

def get_chain_context(
    chain_name: str = None,
    stock_code: str = None,
) -> dict:
    """获取产业链认知上下文（结构化 dict）。

    优先使用 chain_name；若只给 stock_code，自动查映射。

    Returns:
        {
            "chain_name": str,
            "found": bool,
            "baseline": {...},     # 完整 Baseline JSON（structure/drivers/macro_relations）
            "version": int,
            "tier_for_stock": str|None,   # 该股票所在的环节 tier_key
            "tier_label": str|None,
        }
    """
    if not chain_name and stock_code:
        chain_name = get_chain_for_stock(stock_code)

    if not chain_name:
        return {"chain_name": None, "found": False, "baseline": None}

    # 从 DB 取最新 Baseline
    try:
        from chain.chain_baseline import get_latest_baseline
        bl = get_latest_baseline(chain_name)
    except Exception as e:
        logger.warning(f"[chain_context] 获取 baseline 失败: {e}")
        bl = None

    if not bl:
        # 尝试返回静态配置信息（无 Baseline 时降级）
        from config.chain_config import CHAINS
        chain_cfg = CHAINS.get(chain_name)
        if not chain_cfg:
            return {"chain_name": chain_name, "found": False, "baseline": None}
        return {
            "chain_name": chain_name,
            "found": False,
            "baseline": None,
            "config_tiers": {k: v["label"] for k, v in chain_cfg.get("tiers", {}).items()},
            "message": "Baseline 尚未生成，仅返回静态配置",
        }

    baseline_json = bl.get("baseline_json")
    if isinstance(baseline_json, str):
        baseline_json = json.loads(baseline_json)

    # 确定该股票在哪个 tier
    tier_key = None
    tier_label = None
    if stock_code:
        tier_key, tier_label = _find_stock_tier(stock_code, chain_name)

    return {
        "chain_name": chain_name,
        "found": True,
        "baseline": baseline_json,
        "version": bl.get("version"),
        "tier_for_stock": tier_key,
        "tier_label": tier_label,
    }


def _find_stock_tier(stock_code: str, chain_name: str) -> tuple:
    """查找股票在该产业链的环节"""
    from config.chain_config import CHAINS
    from utils.db_utils import execute_query

    chain = CHAINS.get(chain_name, {})
    # 先查股票名
    rows = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=?", [stock_code])
    if not rows:
        return (None, None)
    stock_name = rows[0]["stock_name"]

    for tier_key, tier in chain.get("tiers", {}).items():
        if stock_name in tier.get("stocks", []):
            return (tier_key, tier.get("label", ""))
    return (None, None)


# ══════════════════════════════════════════════════════════════════
# 3. LLM Prompt 格式化（报告生成用）
# ══════════════════════════════════════════════════════════════════

def get_chain_context_for_prompt(
    chain_name: str = None,
    stock_code: str = None,
    max_chars: int = 6000,
) -> str:
    """将产业链认知格式化为 LLM 可直接使用的文本上下文。

    适用于个股分析、投资主题报告的 system/user prompt 注入。

    Returns:
        格式化文本（截断到 max_chars），或空字符串（无数据时）
    """
    ctx = get_chain_context(chain_name=chain_name, stock_code=stock_code)
    if not ctx.get("found") or not ctx.get("baseline"):
        return ""

    baseline = ctx["baseline"]
    chain = ctx["chain_name"]
    parts = [f"=== 产业链认知: {chain} ===\n"]

    # 产业链结构
    structure = baseline.get("structure", [])
    if structure:
        parts.append("【产业链结构】")
        for s in structure:
            tier_label = s.get("tier_label", s.get("tier_key", ""))
            segments = ", ".join(s.get("key_segments", [])[:6])
            companies = ", ".join(s.get("key_companies", [])[:5])
            desc = s.get("description", "")[:150]
            parts.append(f"  [{tier_label}] 细分: {segments} | 代表: {companies}")
            if desc:
                parts.append(f"    {desc}")
        parts.append("")

    # 供需-成本-收入 Driver 矩阵
    drivers = baseline.get("drivers", [])
    if drivers:
        parts.append("【供需Driver矩阵】")
        for d in drivers:
            tier = d.get("tier_key", "")
            parts.append(f"  [{tier}]")

            # 供给因素
            sf = d.get("supply_factors", [])
            if sf:
                factors = "; ".join(
                    f.get("name", str(f)) if isinstance(f, dict) else str(f)
                    for f in sf[:4]
                )
                parts.append(f"    供给: {factors}")

            # 需求因素
            df = d.get("demand_factors", [])
            if df:
                factors = "; ".join(
                    f.get("name", str(f)) if isinstance(f, dict) else str(f)
                    for f in df[:4]
                )
                parts.append(f"    需求: {factors}")

            # 成本要素
            ce = d.get("cost_elements", [])
            if ce:
                for el in ce[:4]:
                    name = el.get("name", "") if isinstance(el, dict) else str(el)
                    idata = el.get("industry_data", "") if isinstance(el, dict) else ""
                    data_str = ""
                    if isinstance(idata, dict) and idata.get("items"):
                        data_str = "; ".join(
                            f"{it.get('metric_name','')}: {it.get('value','')}"
                            for it in idata["items"][:3]
                        )
                    elif isinstance(idata, str):
                        data_str = idata[:80]
                    parts.append(f"    成本[{name}]: {data_str}" if data_str else f"    成本: {name}")

            # 收入要素
            re_ = d.get("revenue_elements", [])
            if re_:
                for el in re_[:4]:
                    name = el.get("name", "") if isinstance(el, dict) else str(el)
                    idata = el.get("industry_data", "") if isinstance(el, dict) else ""
                    data_str = ""
                    if isinstance(idata, dict) and idata.get("items"):
                        data_str = "; ".join(
                            f"{it.get('metric_name','')}: {it.get('value','')}"
                            for it in idata["items"][:3]
                        )
                    elif isinstance(idata, str):
                        data_str = idata[:80]
                    parts.append(f"    收入[{name}]: {data_str}" if data_str else f"    收入: {name}")

        parts.append("")

    # 宏观关系
    macro = baseline.get("macro_relations", [])
    if macro:
        parts.append("【宏观传导关系】")
        for m in macro[:6]:
            indicator = m.get("macro_indicator", "")
            path = m.get("transmission_path", "")[:120]
            direction = m.get("impact_direction", "")
            parts.append(f"  {indicator} → {path} (影响: {direction})")
        parts.append("")

    # 标注该股票所在位置
    if stock_code and ctx.get("tier_for_stock"):
        parts.append(f"[当前个股 {stock_code} 位于: {ctx['tier_label']}({ctx['tier_for_stock']})]")

    text = "\n".join(parts)
    return text[:max_chars]


# ══════════════════════════════════════════════════════════════════
# 4. KG 种子数据（KG关系抽取起点）
# ══════════════════════════════════════════════════════════════════

def get_chain_seed_for_kg(chain_name: str) -> dict:
    """从产业链 Baseline 提取 KG 种子实体和关系。

    KG 抽取管道可用此数据作为已知实体/关系的起点，
    仅做增量补充，无需从零推理。

    Returns:
        {
            "chain_name": str,
            "entities": [{"name": str, "type": str, "tier": str}],
            "relations": [{"source": str, "relation": str, "target": str}],
            "drivers_context": str,  # 供需Driver的文本摘要（供LLM参考）
        }
    """
    ctx = get_chain_context(chain_name=chain_name)
    if not ctx.get("found") or not ctx.get("baseline"):
        # 降级：从静态配置提取基础实体
        return _seed_from_config(chain_name)

    baseline = ctx["baseline"]
    entities = []
    relations = []
    seen_entities = set()

    def _add_entity(name, etype, tier=""):
        if name and name not in seen_entities:
            seen_entities.add(name)
            entities.append({"name": name, "type": etype, "tier": tier})

    # 产业链本身
    _add_entity(chain_name, "industry_chain")

    # 从 structure 提取
    for s in baseline.get("structure", []):
        tier_key = s.get("tier_key", "")
        tier_label = s.get("tier_label", "")
        _add_entity(tier_label, "industry_segment", tier_key)

        # 产业链 → 环节
        relations.append({
            "source": chain_name,
            "relation": "has_segment",
            "target": tier_label,
        })

        for seg in s.get("key_segments", []):
            _add_entity(seg, "sub_segment", tier_key)
            relations.append({
                "source": tier_label,
                "relation": "contains_subsegment",
                "target": seg,
            })

        for company in s.get("key_companies", []):
            _add_entity(company, "company", tier_key)
            relations.append({
                "source": company,
                "relation": "belongs_to_segment",
                "target": tier_label,
            })

    # 从 drivers 提取
    driver_lines = []
    for d in baseline.get("drivers", []):
        tier_key = d.get("tier_key", "")

        for sf in d.get("supply_factors", []):
            name = sf.get("name", str(sf)) if isinstance(sf, dict) else str(sf)
            _add_entity(name, "supply_factor", tier_key)
            relations.append({"source": name, "relation": "supply_driver_of", "target": tier_key})

        for df in d.get("demand_factors", []):
            name = df.get("name", str(df)) if isinstance(df, dict) else str(df)
            _add_entity(name, "demand_factor", tier_key)
            relations.append({"source": name, "relation": "demand_driver_of", "target": tier_key})

        for el in d.get("cost_elements", []):
            name = el.get("name", "") if isinstance(el, dict) else str(el)
            _add_entity(name, "cost_element", tier_key)
            relations.append({"source": name, "relation": "cost_of", "target": tier_key})

        for el in d.get("revenue_elements", []):
            name = el.get("name", "") if isinstance(el, dict) else str(el)
            _add_entity(name, "revenue_element", tier_key)
            relations.append({"source": name, "relation": "revenue_of", "target": tier_key})

        # 竞争格局摘要
        competition = d.get("competition", "")
        if competition:
            driver_lines.append(f"[{tier_key}] 竞争: {competition[:100]}")

    # 从 macro_relations 提取
    for m in baseline.get("macro_relations", []):
        indicator = m.get("macro_indicator", "")
        _add_entity(indicator, "macro_indicator")
        direction = m.get("impact_direction", "neutral")
        relations.append({
            "source": indicator,
            "relation": f"macro_impact_{direction}",
            "target": chain_name,
        })

    return {
        "chain_name": chain_name,
        "entities": entities,
        "relations": relations,
        "drivers_context": "\n".join(driver_lines) if driver_lines else "",
    }


def _seed_from_config(chain_name: str) -> dict:
    """从静态配置提取基础 KG 种子（Baseline 不存在时的降级方案）"""
    from config.chain_config import CHAINS

    chain = CHAINS.get(chain_name, {})
    entities = [{"name": chain_name, "type": "industry_chain", "tier": ""}]
    relations = []

    for tier_key, tier in chain.get("tiers", {}).items():
        label = tier.get("label", tier_key)
        entities.append({"name": label, "type": "industry_segment", "tier": tier_key})
        relations.append({"source": chain_name, "relation": "has_segment", "target": label})

        for company in tier.get("stocks", [])[:10]:
            entities.append({"name": company, "type": "company", "tier": tier_key})
            relations.append({"source": company, "relation": "belongs_to_segment", "target": label})

    return {
        "chain_name": chain_name,
        "entities": entities,
        "relations": relations,
        "drivers_context": "",
    }

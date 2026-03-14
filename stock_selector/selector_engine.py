"""selector_engine — 选股机器人主调度流程

完整链路：
  用户输入
  → Call 1 关键词提取
  → KG 多跳查询 → 候选池
  → Call 2 筛选条件拆解
  → 并行执行数据模块（kline_calc / capital_tracker）
  → 综合排序
  → Call 4 推荐理由（每只）
  → [可选] Call 5 整体总结
  → 返回结果
"""
import logging
from typing import Optional

from stock_selector.kg_resolver import kg_resolve
from stock_selector.llm_calls import call1_extract_keywords, call2_parse_filters, call4_reason, call5_summary
from stock_selector.kline_calc import run_kline_calc, capital_tracker_inflow

logger = logging.getLogger(__name__)


# ==================== 模块执行分发 ====================

def _run_module(codes: list[str], skill: str, action: str, params: dict) -> dict[str, dict]:
    """执行单个筛选模块，返回 {code: result}"""
    if skill == "kline_calc":
        return run_kline_calc(codes, action, params)
    elif skill == "capital_tracker":
        if action in ("consecutive_inflow", "net_inflow_sum"):
            return capital_tracker_inflow(codes, action, params)
        else:
            # 其他 capital_tracker actions 暂未实现，返回 no_data
            logger.info(f"capital_tracker.{action} not yet implemented, skipping")
            return {code: {"pass": False, "reason": "not_implemented"} for code in codes}
    else:
        logger.info(f"Module {skill}.{action} not yet implemented, skipping")
        return {code: {"pass": False, "reason": "not_implemented"} for code in codes}


def _apply_combine(module_results: list[dict[str, dict]], combine: str) -> set[str]:
    """按 AND/OR 逻辑合并各模块结果，返回通过的 code 集合"""
    if not module_results:
        return set()

    # 过滤掉全部 not_implemented 的模块
    valid_results = [
        r for r in module_results
        if any(v.get("reason") != "not_implemented" for v in r.values())
    ]
    if not valid_results:
        return set(module_results[0].keys()) if module_results else set()

    if combine == "OR":
        passed = set()
        for r in valid_results:
            passed |= {code for code, v in r.items() if v.get("pass")}
        return passed
    else:  # AND
        sets = [{code for code, v in r.items() if v.get("pass")} for r in valid_results]
        result = sets[0]
        for s in sets[1:]:
            result &= s
        return result


def _build_conditions(code: str, modules: list[dict], module_results: list[dict[str, dict]]) -> list[str]:
    """构建该股票命中的筛选条件描述"""
    conditions = []
    for i, mod in enumerate(modules):
        if i >= len(module_results):
            break
        res = module_results[i].get(code, {})
        if res.get("pass"):
            conditions.append(f"{mod['skill']}.{mod['action']}")
    return conditions


# ==================== 主流程 ====================

def run_selector(user_message: str, need_summary: bool = False) -> dict:
    """选股机器人主入口

    Args:
        user_message: 用户输入（可以是长文本+问题）
        need_summary: 是否触发 Call 5 整体总结

    Returns:
        {
            "candidates_count": int,       # KG 候选池大小
            "filtered_count": int,         # 筛选后数量
            "stocks": [                    # top N 结果
                {
                    "code": str,
                    "name": str,
                    "score": float,
                    "kg_paths": list[str],
                    "conditions": list[str],
                    "reason": str,
                }
            ],
            "summary": str,                # Call 5 整体总结（可选）
            "debug": dict,                 # 调试信息
        }
    """
    debug = {}

    # ── Step 1: Call 1 关键词提取 ──
    logger.info("selector: Call 1 - keyword extraction")
    kw_result = call1_extract_keywords(user_message)
    keywords = kw_result.get("keywords", [])
    entity_types = kw_result.get("entity_types", [])
    relation_hint = kw_result.get("relation_hint", "benefits")
    debug["call1"] = kw_result
    logger.info(f"selector: keywords={keywords}, relation_hint={relation_hint}")

    # ── Step 2: KG 多跳查询 ──
    candidates = []
    if keywords:
        logger.info("selector: KG multi-hop query")
        candidates = kg_resolve(keywords, entity_types, relation_hint)
    debug["candidates_count"] = len(candidates)
    logger.info(f"selector: {len(candidates)} candidates from KG")

    # ── Step 3: Call 2 筛选条件拆解 ──
    logger.info("selector: Call 2 - filter parsing")
    filter_result = call2_parse_filters(user_message)
    modules = filter_result.get("modules", [])
    combine = filter_result.get("combine", "AND")
    sort_by = filter_result.get("sort_by", "")
    limit = filter_result.get("limit", 10)
    debug["call2"] = filter_result
    logger.info(f"selector: {len(modules)} filter modules, combine={combine}")

    # ── Step 4: 执行数据模块 ──
    codes = [c["code"] for c in candidates if c.get("code")]
    module_results: list[dict[str, dict]] = []

    if modules and codes:
        logger.info(f"selector: running {len(modules)} modules on {len(codes)} codes")
        for mod in modules:
            res = _run_module(codes, mod["skill"], mod["action"], mod.get("params", {}))
            module_results.append(res)

        # 合并筛选
        passed_codes = _apply_combine(module_results, combine)
        filtered = [c for c in candidates if c["code"] in passed_codes]
        logger.info(f"selector: {len(filtered)} passed after filtering")
    else:
        # 无筛选条件，直接用 KG 候选池
        filtered = candidates

    debug["filtered_count"] = len(filtered)

    # ── Step 5: 综合排序 ──
    # KG 分 + 模块指标分（如 sort_by 指定了某个指标）
    def _score(c):
        base = c.get("score", 0)
        # 如果 sort_by 指定了某个模块指标，提取该值加权
        if sort_by and module_results:
            skill_action = sort_by.split(".")
            if len(skill_action) == 2:
                for i, mod in enumerate(modules):
                    if mod["skill"] == skill_action[0] and mod["action"] == skill_action[1]:
                        if i < len(module_results):
                            metric = module_results[i].get(c["code"], {})
                            # 取第一个数值型字段
                            for v in metric.values():
                                if isinstance(v, (int, float)) and v != 0:
                                    base += v * 0.01
                                    break
        return base

    top = sorted(filtered, key=_score, reverse=True)[:limit]

    # ── Step 6: Call 4 推荐理由 ──
    logger.info(f"selector: Call 4 - generating reasons for {len(top)} stocks")
    stocks = []
    for c in top:
        conditions = _build_conditions(c["code"], modules, module_results)
        reason = call4_reason(c["code"], c["name"], c.get("paths", []), conditions)
        stocks.append({
            "code": c["code"],
            "name": c["name"],
            "score": round(c.get("score", 0), 3),
            "kg_paths": c.get("paths", []),
            "conditions": conditions,
            "reason": reason,
        })

    # ── Step 7: Call 5 整体总结（可选）──
    summary = ""
    if need_summary and stocks:
        logger.info("selector: Call 5 - overall summary")
        summary = call5_summary(user_message[:300], stocks)

    return {
        "candidates_count": len(candidates),
        "filtered_count": len(filtered),
        "stocks": stocks,
        "summary": summary,
        "debug": debug,
    }

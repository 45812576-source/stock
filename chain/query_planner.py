"""产业链认知 — LLM 驱动的查询规划器

核心职责：
  给定一个 Baseline 的 element（如"原材料成本", driver="钢铁、铜等大宗价格影响"），
  让 LLM 理解该要素在产业链中的实际含义，生成一组具体的、可检索的指标查询问题列表。

流程：
  1. 将 Baseline 的 element 上下文（name, driver, share_pct, trend, tier_label）打包为 prompt
  2. LLM 输出结构化 JSON：每个问题包含 query(搜索词), target(检索目标), rationale(为什么查这个)
  3. 输出作为 industry_indicators 和 问财 的搜索输入

设计原则：
  - 宁缺毋滥：LLM 生成的 query 必须足够具体，避免抽象词
  - 批量处理：一次 LLM 调用规划所有 elements（节省调用次数）
  - 可缓存：同一 chain 的 query plan 可以缓存复用
"""
import json
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
# 主入口
# ══════════════════════════════════════════════════════════════════

def plan_queries_for_baseline(
    baseline_json: dict,
    chain_name: str,
) -> Dict[str, List[dict]]:
    """为整个 Baseline 的所有 elements 生成查询计划。

    返回:
      {
        "原材料成本": [
          {"query": "螺纹钢价格", "target": "indicator_db", "rationale": "风电塔筒主要材料"},
          {"query": "铜材价格走势 2025", "target": "wencai", "rationale": "电缆用铜材"},
          ...
        ],
        "运输成本": [...],
        ...
      }
    """
    drivers = baseline_json.get("drivers", [])
    structure = baseline_json.get("structure", [])

    # 收集所有 element 上下文
    elements_context = []
    for driver in drivers:
        tier_key = driver.get("tier_key", "")
        tier_label = _find_tier_label(structure, tier_key)
        el_type = "cost"
        for el in driver.get("cost_elements", []):
            if isinstance(el, dict) and el.get("name"):
                elements_context.append({
                    "name": el["name"],
                    "type": "cost",
                    "tier_label": tier_label,
                    "driver": el.get("driver", ""),
                    "share_pct": el.get("share_pct", ""),
                    "trend": el.get("trend", ""),
                })
        for el in driver.get("revenue_elements", []):
            if isinstance(el, dict) and el.get("name"):
                elements_context.append({
                    "name": el["name"],
                    "type": "revenue",
                    "tier_label": tier_label,
                    "driver": el.get("driver", ""),
                    "share_pct": el.get("share_pct", ""),
                    "trend": el.get("trend", ""),
                })

    if not elements_context:
        return {}

    # 调用 LLM 生成查询计划
    return _call_llm_plan(chain_name, elements_context)


# ══════════════════════════════════════════════════════════════════
# LLM 调用
# ══════════════════════════════════════════════════════════════════

_SYSTEM_PROMPT = """你是一位产业链分析专家。你的任务是：根据给定的产业链要素信息，规划一组**具体的、可检索的**行业指标查询问题。

## 核心规则

1. **具体化**：不要使用抽象词。例如"原材料成本"不是一个好的搜索词，"螺纹钢价格"才是。
2. **组成拆解**：如果一个要素由多种材料/因素构成，分别为每种生成独立的查询。
3. **权重意识**：优先为占比大的组成生成查询。如钢材占80%，优先查钢；铜占10%，次优先。
4. **变体覆盖**：同一个商品生成2-3个搜索变体。如钢材→"螺纹钢价格"、"热轧板卷价格"、"钢材价格走势"。
5. **时效性**：搜索词应面向最新数据（加上年份或"最新"等限定）。
6. **精确匹配优先**：生成的 query 应尽量匹配指标数据库中可能存在的 metric_name 格式（如"XXX价格"、"XXX产量"、"XXX同比增速"）。

## 输出格式

对每个 element 输出 2~6 个查询项：
```json
{
  "element_name": [
    {
      "query": "具体搜索词（如：螺纹钢价格）",
      "target": "indicator_db 或 wencai",
      "rationale": "为什么查这个（如：占原材料成本80%的主要材料）"
    }
  ]
}
```

target 说明：
- "indicator_db"：适合查已有结构化指标（精确的指标名，如"螺纹钢价格"、"国内风电新增装机"）
- "wencai"：适合查实时市场数据或需要问句形式的（如"2025年风电叶片碳纤维用量"）

## 关键约束
- 每个 query 必须足够具体，能直接作为数据库 metric_name 的搜索关键词
- 绝对不要生成"原材料成本"、"运输成本"这样的抽象搜索词
- 如果 driver 字段提到了具体材料/因素，必须围绕这些展开
- 如果 driver 为空，根据产业链常识推断主要组成"""


def _call_llm_plan(
    chain_name: str,
    elements_context: List[dict],
) -> Dict[str, List[dict]]:
    """调用 LLM 生成查询计划。"""
    try:
        from utils.model_router import call_model_json
    except ImportError:
        logger.error("[query_planner] model_router 不可用")
        return {}

    # 构建 user message
    user_msg = f"""产业链：{chain_name}

以下是该产业链 Baseline 中的要素列表，请为每个要素生成具体的搜索查询计划：

"""
    for i, el in enumerate(elements_context, 1):
        user_msg += f"""{i}. 要素名: {el['name']}
   类型: {el['type']}（{'成本要素' if el['type'] == 'cost' else '收入要素'}）
   所属环节: {el['tier_label']}
   驱动因素: {el['driver'] or '未指定'}
   占比: {el['share_pct'] or '未指定'}
   趋势: {el['trend'] or '未指定'}

"""

    user_msg += """请输出 JSON 格式的查询计划。key 为 element_name，value 为查询数组。"""

    try:
        result = call_model_json(
            stage="cleaning",  # qwen3-coder-plus, 降级备选: research(deepseek)
            system_prompt=_SYSTEM_PROMPT,
            user_message=user_msg,
            max_tokens=4096,
            timeout=120,
        )
        if isinstance(result, dict):
            # 验证格式
            validated = {}
            for name, queries in result.items():
                if isinstance(queries, list):
                    valid_queries = []
                    for q in queries:
                        if isinstance(q, dict) and q.get("query"):
                            valid_queries.append({
                                "query": q["query"],
                                "target": q.get("target", "indicator_db"),
                                "rationale": q.get("rationale", ""),
                            })
                    if valid_queries:
                        validated[name] = valid_queries
            logger.info(f"[query_planner] chain={chain_name}, "
                       f"elements={len(validated)}, "
                       f"total_queries={sum(len(v) for v in validated.values())}")
            return validated
        else:
            logger.warning(f"[query_planner] LLM 返回非 dict: {type(result)}")
            return {}
    except Exception as e:
        logger.error(f"[query_planner] LLM 调用失败: {e}")
        return {}


# ══════════════════════════════════════════════════════════════════
# 单元素快速规划（用于增量补充）
# ══════════════════════════════════════════════════════════════════

def plan_queries_for_element(
    element_name: str,
    chain_name: str,
    tier_label: str = "",
    driver: str = "",
    share_pct: str = "",
    trend: str = "",
    el_type: str = "cost",
) -> List[dict]:
    """为单个 element 生成查询计划（轻量调用）。"""
    result = _call_llm_plan(chain_name, [{
        "name": element_name,
        "type": el_type,
        "tier_label": tier_label,
        "driver": driver,
        "share_pct": share_pct,
        "trend": trend,
    }])
    return result.get(element_name, [])


# ══════════════════════════════════════════════════════════════════
# 辅助
# ══════════════════════════════════════════════════════════════════

def _find_tier_label(structure: list, tier_key: str) -> str:
    for s in structure:
        if s.get("tier_key") == tier_key:
            return s.get("tier_label", "")
    return ""

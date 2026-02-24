"""KG 深度语义校验 — 用 AI 判断实体名是否符合其类型定义

独立入口，不并入常规巡检。
- validate_entities_by_type(entity_type, batch_size=25, dry_run=False)
- run_semantic_validation(dry_run=False, progress_callback=None)
"""
from __future__ import annotations
import json
import logging
from utils.db_utils import execute_query, execute_insert
from utils.model_router import call_model_json

logger = logging.getLogger(__name__)

# ── 每种实体类型的精确定义 + 排除项（用于 prompt）──────────────────────────────

ENTITY_TYPE_DEFINITIONS = {
    "company": {
        "label": "公司",
        "definition": "在中国A股、港股或美股上市的具体公司，或有明确股票代码的企业实体。",
        "examples": ["比亚迪", "宁德时代", "腾讯控股", "贵州茅台"],
        "exclusions": "行业名称（如'新能源汽车'）、产品名称、概念主题、政策名称、宏观指标、地名、人名",
    },
    "industry": {
        "label": "行业",
        "definition": "具体的产业分类或行业细分，通常是国民经济行业分类中的某一类别，或证券市场常用的行业划分。",
        "examples": ["新能源汽车", "半导体", "医疗器械", "白酒", "光伏"],
        "exclusions": "具体公司名、投资主题（如'AI+'）、政策名称、宏观指标、产品名称",
    },
    "industry_chain": {
        "label": "产业链",
        "definition": "描述某一产业从上游到下游的完整供应链结构，通常以'XX产业链'或'XX供应链'命名，或明确指代某产业的上中下游环节。",
        "examples": ["锂电池产业链", "光伏产业链", "半导体产业链", "汽车供应链"],
        "exclusions": "单一行业名（不含链条概念）、具体公司、投资主题",
    },
    "theme": {
        "label": "投资主题",
        "definition": "资本市场中的投资概念或热点主题，通常是跨行业的投资逻辑或市场叙事，不对应单一行业。",
        "examples": ["AI+", "数字经济", "国产替代", "碳中和", "一带一路"],
        "exclusions": "具体行业名称、具体公司、政策文件名称、宏观经济指标",
    },
    "policy": {
        "label": "政策",
        "definition": "政府、监管机构或央行发布的具体政策、法规、规划或指导意见，通常有明确的发布主体和政策名称。",
        "examples": ["双碳政策", "新能源汽车补贴政策", "房地产调控政策", "降准"],
        "exclusions": "宏观经济指标（如GDP、CPI）、投资主题、行业名称、公司名称",
    },
    "macro_indicator": {
        "label": "宏观指标",
        "definition": "衡量宏观经济运行状况的统计指标，通常由统计局、央行等机构定期发布，有明确的数值和频率。",
        "examples": ["GDP增速", "CPI", "PPI", "PMI", "M2", "社会融资规模"],
        "exclusions": "政策名称、行业名称、公司名称、投资主题",
    },
    "commodity": {
        "label": "大宗商品",
        "definition": "在期货或现货市场交易的标准化大宗原材料或农产品，通常有交易所报价。",
        "examples": ["铜", "铁矿石", "原油", "黄金", "大豆", "螺纹钢"],
        "exclusions": "加工后的工业品、消费品、半成品、能源品（能源单独分类）",
    },
    "energy": {
        "label": "能源",
        "definition": "能源类大宗商品或能源形式，包括化石能源和新能源。",
        "examples": ["原油", "天然气", "煤炭", "电力", "氢能", "LNG"],
        "exclusions": "能源相关行业（如'光伏行业'）、能源公司、能源政策",
    },
    "intermediate": {
        "label": "半成品/中间品",
        "definition": "工业生产中的中间投入品，已经过初步加工但尚未成为最终消费品，主要用于进一步生产。",
        "examples": ["碳酸锂", "多晶硅", "乙烯", "钢坯", "芯片晶圆"],
        "exclusions": "原材料大宗商品、最终消费品、能源品",
    },
    "consumer_good": {
        "label": "消费品",
        "definition": "面向终端消费者的最终产品，包括耐用消费品和快速消费品。",
        "examples": ["智能手机", "新能源汽车整车", "白酒", "家电", "医药"],
        "exclusions": "工业中间品、原材料、能源品、服务类",
    },
    "market": {
        "label": "市场",
        "definition": "特定的金融市场或资产市场，通常指某个国家或地区的证券市场、债券市场、外汇市场等。",
        "examples": ["A股市场", "港股市场", "美股市场", "债券市场", "外汇市场"],
        "exclusions": "行业、公司、宏观指标、政策",
    },
    "strategy": {
        "label": "战略/商业模式",
        "definition": "企业层面的战略举措或商业模式，通常描述某种经营策略或竞争方式。",
        "examples": ["垂直整合", "平台化战略", "出海战略", "轻资产模式"],
        "exclusions": "行业名称、公司名称、投资主题、政策",
    },
}

# ── Prompt 模板 ────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """你是知识图谱数据质量审核专家。你的任务是判断给定的实体名称列表中，哪些不符合指定实体类型的定义。

判断标准：
1. 严格按照类型定义判断，不要宽泛解释
2. 如果实体名明显属于其他类型，则判定为不符合（放入 invalid）
3. 如果实体名是泛化概念、无意义词汇、或明显错误，则判定为不符合（放入 invalid）
4. 对于边界情况，倾向于保留（不删除）
5. 如果实体名包含多余的定语、后缀、事件描述，但核心概念是合法的，则放入 rename 而非 invalid。例如：
   - "火箭链" → rename 为 "火箭产业链"
   - "AI超级入口争夺" → rename 为 "AI超级入口"
   - "化妆品制造及其他行业" → rename 为 "化妆品制造"
6. industry 类型中，按服务类型细分的名称（如"商务服务""信息服务"）是合法的行业分类，不要误判

返回 JSON 格式：
{
  "invalid": ["应删除的实体名1", ...],
  "rename": {"原名": "建议改名", ...},
  "reason": {"实体名": "简短原因", ...}
}

如果全部符合，返回 {"invalid": [], "rename": {}, "reason": {}}"""


def _build_user_prompt(entity_type: str, names: list[str]) -> str:
    defn = ENTITY_TYPE_DEFINITIONS.get(entity_type, {})
    label = defn.get("label", entity_type)
    definition = defn.get("definition", "")
    examples = "、".join(defn.get("examples", []))
    exclusions = defn.get("exclusions", "")

    return f"""实体类型：{label}（{entity_type}）

定义：{definition}
典型示例：{examples}
排除项（这些不属于此类型）：{exclusions}

请判断以下实体名称中，哪些不符合"{label}"的定义：

{json.dumps(names, ensure_ascii=False, indent=2)}

返回 JSON，格式如上。"""


def validate_entities_by_type(
    entity_type: str,
    batch_size: int = 25,
    dry_run: bool = False,
    progress_callback=None,
) -> dict:
    """校验单个实体类型的所有实体名是否语义合法

    Args:
        entity_type: 实体类型
        batch_size: 每批发给 AI 的实体数量
        dry_run: True 则只统计不删除
        progress_callback: callback(current_batch, total_batches, msg)

    Returns:
        {
            "entity_type": str,
            "total": int,
            "invalid_count": int,
            "deleted_count": int,
            "invalid_names": [{"name": str, "id": int, "reason": str}],
            "renamed_count": int,
            "renamed_names": [{"name": str, "id": int, "new_name": str, "reason": str}],
        }
    """
    rows = execute_query(
        "SELECT id, entity_name FROM kg_entities WHERE entity_type=%s ORDER BY id",
        [entity_type],
    )
    if not rows:
        return {
            "entity_type": entity_type,
            "total": 0,
            "invalid_count": 0,
            "deleted_count": 0,
            "invalid_names": [],
            "renamed_count": 0,
            "renamed_names": [],
        }

    all_entities = [(r["id"], r["entity_name"]) for r in rows]
    total = len(all_entities)
    invalid_names = []
    rename_names = []

    # 分批处理
    batches = [all_entities[i:i+batch_size] for i in range(0, total, batch_size)]
    total_batches = len(batches)

    for batch_idx, batch in enumerate(batches):
        if progress_callback:
            progress_callback(
                batch_idx + 1, total_batches,
                f"{entity_type}: 批次 {batch_idx+1}/{total_batches}"
            )

        names = [name for _, name in batch]
        name_to_id = {name: eid for eid, name in batch}

        try:
            result = call_model_json(
                "kg",
                SYSTEM_PROMPT,
                _build_user_prompt(entity_type, names),
                max_tokens=1024,
                timeout=60,
            )
        except Exception as e:
            logger.warning(f"语义校验 AI 调用失败 type={entity_type} batch={batch_idx}: {e}")
            continue

        if not result or not isinstance(result, dict):
            continue

        invalid_list = result.get("invalid") or []
        reasons = result.get("reason") or {}
        rename_map = result.get("rename") or {}

        for name in invalid_list:
            if name in name_to_id:
                invalid_names.append({
                    "name": name,
                    "id": name_to_id[name],
                    "reason": reasons.get(name, ""),
                })

        for old_name, new_name in rename_map.items():
            if old_name in name_to_id and new_name and len(new_name) >= 2:
                rename_names.append({
                    "name": old_name,
                    "id": name_to_id[old_name],
                    "new_name": new_name,
                    "reason": reasons.get(old_name, ""),
                })

    # 删除不符合的实体（连带删除关系）
    deleted_count = 0
    if invalid_names and not dry_run:
        ids = [item["id"] for item in invalid_names]
        batch_size_del = 200
        for i in range(0, len(ids), batch_size_del):
            chunk = ids[i:i+batch_size_del]
            ph = ",".join(["%s"] * len(chunk))
            execute_insert(
                f"DELETE FROM kg_relationships WHERE source_entity_id IN ({ph}) OR target_entity_id IN ({ph})",
                chunk + chunk,
            )
            execute_insert(f"DELETE FROM kg_entities WHERE id IN ({ph})", chunk)
        deleted_count = len(ids)
        logger.info(f"语义校验删除: type={entity_type}, count={deleted_count}")

    # 重命名边界情况实体
    renamed_count = 0
    if rename_names and not dry_run:
        for item in rename_names:
            execute_insert(
                "UPDATE kg_entities SET entity_name=%s, updated_at=NOW() WHERE id=%s",
                [item["new_name"], item["id"]],
            )
        renamed_count = len(rename_names)
        logger.info(f"语义校验重命名: type={entity_type}, count={renamed_count}")

    return {
        "entity_type": entity_type,
        "total": total,
        "invalid_count": len(invalid_names),
        "deleted_count": deleted_count,
        "invalid_names": invalid_names,
        "renamed_count": renamed_count,
        "renamed_names": rename_names,
    }


# 校验顺序：高风险类型优先（数量多、容易混入噪声的）
VALIDATION_ORDER = [
    "theme", "intermediate", "consumer_good", "commodity",
    "energy", "strategy", "market", "macro_indicator",
    "policy", "industry_chain", "industry", "company",
]


def run_semantic_validation(
    dry_run: bool = False,
    entity_types: list[str] = None,
    batch_size: int = 25,
    progress_callback=None,
) -> dict:
    """全量语义校验入口

    Args:
        dry_run: True 则只统计不删除
        entity_types: 指定校验的类型列表，None 则全量
        batch_size: 每批实体数
        progress_callback: callback(type_idx, total_types, entity_type, msg)

    Returns:
        {
            "total_checked": int,
            "total_invalid": int,
            "total_deleted": int,
            "by_type": {entity_type: result_dict, ...},
        }
    """
    types_to_check = entity_types or VALIDATION_ORDER
    total_types = len(types_to_check)
    by_type = {}
    total_checked = 0
    total_invalid = 0
    total_deleted = 0
    total_renamed = 0

    for type_idx, entity_type in enumerate(types_to_check):
        if progress_callback:
            progress_callback(type_idx + 1, total_types, entity_type, f"开始校验 {entity_type}")

        def _type_progress(cur, tot, msg):
            if progress_callback:
                progress_callback(type_idx + 1, total_types, entity_type, msg)

        result = validate_entities_by_type(
            entity_type,
            batch_size=batch_size,
            dry_run=dry_run,
            progress_callback=_type_progress,
        )
        by_type[entity_type] = result
        total_checked += result["total"]
        total_invalid += result["invalid_count"]
        total_deleted += result["deleted_count"]
        total_renamed += result["renamed_count"]

        logger.info(
            f"语义校验 {entity_type}: total={result['total']}, "
            f"invalid={result['invalid_count']}, deleted={result['deleted_count']}, "
            f"renamed={result['renamed_count']}"
        )

    return {
        "total_checked": total_checked,
        "total_invalid": total_invalid,
        "total_deleted": total_deleted,
        "total_renamed": total_renamed,
        "by_type": by_type,
    }

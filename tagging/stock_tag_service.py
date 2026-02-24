"""统一股票标签服务 — 三源聚合

三类标签：
  1. 选股标签：stock_rule_tags（L1量化 + L2 AI轻量 + L3 AI深度）
  2. 行业标签：KG belongs_to_industry 关系（全显示）
  3. 投资主题标签：KG theme 实体关系（全显示）

对外接口：
  get_stock_tags(stock_code) -> StockTagResult
  get_stock_tags_batch(stock_codes) -> dict[code, StockTagResult]
"""
import json
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class SelectionTag:
    """选股标签（来自 stock_rule_tags）"""
    name: str
    category: str          # 技术形态 / 资金面 / 盈利质量 / 估值 / 风险收益
    layer: int             # 1=量化 2=AI轻量 3=AI深度
    confidence: float
    evidence: str = ""


@dataclass
class StockTagResult:
    """个股标签聚合结果"""
    stock_code: str
    # 行业标签（来自KG，全显示）
    industry_tags: list[str] = field(default_factory=list)
    # 投资主题标签（来自KG，全显示）
    theme_tags: list[str] = field(default_factory=list)
    # 选股标签（来自 stock_rule_tags，只显示 matched=1 的）
    selection_tags: list[SelectionTag] = field(default_factory=list)

    def to_display_dict(self) -> dict:
        """转为前端展示格式"""
        return {
            "industry": [{"name": t, "type": "industry"} for t in self.industry_tags],
            "themes": [{"name": t, "type": "theme"} for t in self.theme_tags],
            "selection": [
                {
                    "name": t.name,
                    "type": "selection",
                    "category": t.category,
                    "layer": t.layer,
                    "confidence": t.confidence,
                    "evidence": t.evidence,
                }
                for t in self.selection_tags
            ],
        }

    def to_flat_list(self) -> list[dict]:
        """扁平化标签列表（兼容旧模板）"""
        result = []
        for t in self.industry_tags:
            result.append({"name": t, "type": "industry"})
        for t in self.theme_tags:
            result.append({"name": t, "type": "theme"})
        for t in self.selection_tags:
            result.append({
                "name": t.name,
                "type": "selection",
                "category": t.category,
                "layer": t.layer,
                "confidence": t.confidence,
            })
        return result


def _lq(sql, params=None):
    from utils.db_utils import execute_query
    return execute_query(sql, params or []) or []


# ── 1. 行业标签（KG） ──────────────────────────────────────────────────────────

def get_industry_tags_from_kg(stock_code: str) -> list[str]:
    """从KG获取行业标签：company → belongs_to_industry → industry"""
    # 先找公司实体（按 external_id=stock_code 或 entity_name=stock_name）
    info = _lq("SELECT stock_name FROM stock_info WHERE stock_code=%s", [stock_code])
    stock_name = info[0]["stock_name"] if info else None

    conditions = ["ke_src.external_id=%s"]
    params = [stock_code]
    if stock_name:
        conditions.append("ke_src.entity_name=%s")
        params.append(stock_name)

    where = " OR ".join(conditions)
    rows = _lq(
        f"""SELECT ke_tgt.entity_name
            FROM kg_entities ke_src
            JOIN kg_relationships kr ON kr.source_entity_id = ke_src.id
            JOIN kg_entities ke_tgt ON kr.target_entity_id = ke_tgt.id
            WHERE ({where})
              AND ke_src.entity_type = 'company'
              AND kr.relation_type = 'belongs_to_industry'
              AND ke_tgt.entity_type = 'industry'
            ORDER BY kr.strength DESC""",
        params,
    )
    return [r["entity_name"] for r in rows]


# ── 2. 投资主题标签（KG） ──────────────────────────────────────────────────────

def get_theme_tags_from_kg(stock_code: str) -> list[str]:
    """从KG获取投资主题标签：company → related → theme"""
    info = _lq("SELECT stock_name FROM stock_info WHERE stock_code=%s", [stock_code])
    stock_name = info[0]["stock_name"] if info else None

    conditions = ["ke_src.external_id=%s"]
    params = [stock_code]
    if stock_name:
        conditions.append("ke_src.entity_name=%s")
        params.append(stock_name)

    where = " OR ".join(conditions)
    rows = _lq(
        f"""SELECT ke_tgt.entity_name, MAX(kr.strength) as max_strength
            FROM kg_entities ke_src
            JOIN kg_relationships kr ON kr.source_entity_id = ke_src.id
            JOIN kg_entities ke_tgt ON kr.target_entity_id = ke_tgt.id
            WHERE ({where})
              AND ke_src.entity_type = 'company'
              AND ke_tgt.entity_type = 'theme'
            GROUP BY ke_tgt.entity_name
            ORDER BY max_strength DESC""",
        params,
    )
    return [r["entity_name"] for r in rows]


# ── 3. 选股标签（stock_rule_tags） ────────────────────────────────────────────

def get_selection_tags(stock_code: str) -> list[SelectionTag]:
    """从 stock_rule_tags 获取已匹配的选股标签"""
    rows = _lq(
        """SELECT rule_name, rule_category, layer, confidence, evidence
           FROM stock_rule_tags
           WHERE stock_code=%s AND matched=1
           ORDER BY layer ASC, confidence DESC""",
        [stock_code],
    )
    return [
        SelectionTag(
            name=r["rule_name"],
            category=r.get("rule_category") or "",
            layer=r.get("layer") or 1,
            confidence=float(r.get("confidence") or 0.5),
            evidence=(r.get("evidence") or "")[:200],
        )
        for r in rows
    ]


# ── 主入口 ────────────────────────────────────────────────────────────────────

def get_stock_tags(stock_code: str) -> StockTagResult:
    """获取个股全量标签（三源聚合）"""
    result = StockTagResult(stock_code=stock_code)
    try:
        result.industry_tags = get_industry_tags_from_kg(stock_code)
    except Exception as e:
        logger.warning(f"获取行业标签失败 {stock_code}: {e}")

    try:
        result.theme_tags = get_theme_tags_from_kg(stock_code)
    except Exception as e:
        logger.warning(f"获取主题标签失败 {stock_code}: {e}")

    try:
        result.selection_tags = get_selection_tags(stock_code)
    except Exception as e:
        logger.warning(f"获取选股标签失败 {stock_code}: {e}")

    return result


def get_stock_tags_batch(stock_codes: list[str]) -> dict:
    """批量获取标签（返回 {stock_code: StockTagResult}）"""
    return {code: get_stock_tags(code) for code in stock_codes}

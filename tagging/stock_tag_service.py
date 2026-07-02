"""统一股票标签服务 — 三源聚合

三类标签：
  1. 选股标签：stock_rule_tags（L1量化 + L2 AI轻量 + L3 AI深度）
  2. 行业标签：KG belongs_to_industry 关系（按 strength 分层）
  3. 投资主题标签：KG theme 实体关系（按 strength 分层）

分层逻辑：
  - 核心层 (strength >= 0.9): 首页直接显示
  - 相关层 (strength 0.8): 点击展开第一层
  - 关联层 (strength <= 0.7): 再点击展开第二层

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
class IndustryTag:
    """行业/主题标签（带层级信息）"""
    name: str
    strength: float
    tier: int              # 1=核心 2=相关 3=关联


@dataclass
class StockTagResult:
    """个股标签聚合结果"""
    stock_code: str
    # 行业标签（分层: tier1=核心, tier2=相关, tier3=关联）
    industry_tags_layered: list[IndustryTag] = field(default_factory=list)
    # 投资主题标签（分层）
    theme_tags_layered: list[IndustryTag] = field(default_factory=list)
    # 选股标签（来自 stock_rule_tags，只显示 matched=1 的）
    selection_tags: list[SelectionTag] = field(default_factory=list)

    # 兼容旧接口
    @property
    def industry_tags(self) -> list[str]:
        return [t.name for t in self.industry_tags_layered]

    @property
    def theme_tags(self) -> list[str]:
        return [t.name for t in self.theme_tags_layered]

    def to_display_dict(self) -> dict:
        """转为前端展示格式（带层级信息）"""
        return {
            "industry": [
                {"name": t.name, "type": "industry", "strength": t.strength, "tier": t.tier}
                for t in self.industry_tags_layered
            ],
            "themes": [
                {"name": t.name, "type": "theme", "strength": t.strength, "tier": t.tier}
                for t in self.theme_tags_layered
            ],
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
        for t in self.industry_tags_layered:
            result.append({"name": t.name, "type": "industry", "tier": t.tier})
        for t in self.theme_tags_layered:
            result.append({"name": t.name, "type": "theme", "tier": t.tier})
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


# ── 1. 行业标签（KG, 分层） ────────────────────────────────────────────────────

def _strength_to_tier(strength: float) -> int:
    """按 strength 分层: >=0.9→核心, 0.8→相关, <=0.7→关联"""
    if strength >= 0.9:
        return 1
    elif strength >= 0.8:
        return 2
    return 3


def get_industry_tags_from_kg(stock_code: str) -> list[IndustryTag]:
    """从KG获取行业标签（带 strength 分层）"""
    info = _lq("SELECT stock_name FROM stock_info WHERE stock_code=%s", [stock_code])
    stock_name = info[0]["stock_name"] if info else None

    conditions = ["ke_src.external_id=%s"]
    params = [stock_code]
    if stock_name:
        conditions.append("ke_src.entity_name=%s")
        params.append(stock_name)

    where = " OR ".join(conditions)
    rows = _lq(
        f"""SELECT ke_tgt.entity_name, MAX(kr.strength) as strength
            FROM kg_entities ke_src
            JOIN kg_relationships kr ON kr.source_entity_id = ke_src.id
            JOIN kg_entities ke_tgt ON kr.target_entity_id = ke_tgt.id
            WHERE ({where})
              AND ke_src.entity_type = 'company'
              AND kr.relation_type = 'belongs_to_industry'
              AND ke_tgt.entity_type = 'industry'
            GROUP BY ke_tgt.entity_name
            ORDER BY strength DESC""",
        params,
    )
    return [
        IndustryTag(
            name=r["entity_name"],
            strength=float(r.get("strength") or 0.7),
            tier=_strength_to_tier(float(r.get("strength") or 0.7)),
        )
        for r in rows
    ]


# ── 2. 投资主题标签（KG, 分层） ────────────────────────────────────────────────

def get_theme_tags_from_kg(stock_code: str) -> list[IndustryTag]:
    """从KG获取投资主题标签（带 strength 分层）"""
    info = _lq("SELECT stock_name FROM stock_info WHERE stock_code=%s", [stock_code])
    stock_name = info[0]["stock_name"] if info else None

    conditions = ["ke_src.external_id=%s"]
    params = [stock_code]
    if stock_name:
        conditions.append("ke_src.entity_name=%s")
        params.append(stock_name)

    where = " OR ".join(conditions)
    rows = _lq(
        f"""SELECT ke_tgt.entity_name, MAX(kr.strength) as strength
            FROM kg_entities ke_src
            JOIN kg_relationships kr ON kr.source_entity_id = ke_src.id
            JOIN kg_entities ke_tgt ON kr.target_entity_id = ke_tgt.id
            WHERE ({where})
              AND ke_src.entity_type = 'company'
              AND ke_tgt.entity_type = 'theme'
            GROUP BY ke_tgt.entity_name
            ORDER BY strength DESC""",
        params,
    )
    return [
        IndustryTag(
            name=r["entity_name"],
            strength=float(r.get("strength") or 0.7),
            tier=_strength_to_tier(float(r.get("strength") or 0.7)),
        )
        for r in rows
    ]


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
        result.industry_tags_layered = get_industry_tags_from_kg(stock_code)
    except Exception as e:
        logger.warning(f"获取行业标签失败 {stock_code}: {e}")

    try:
        result.theme_tags_layered = get_theme_tags_from_kg(stock_code)
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

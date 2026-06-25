"""产业链认知管理 — Baseline 生成 / Diff 增量 / 合并版本

核心流程:
1. generate_baseline(chain_name): 首次生成或重建结构化行业认知
2. generate_diff(chain_name): 基于增量数据生成 Diff 报告
3. merge_diff(chain_name, diff_id): 将用户确认的 Diff 合并到 Baseline
"""
import json
import logging
from datetime import datetime
from typing import Optional

from utils.db_utils import (
    execute_query, execute_insert,
    execute_cloud_query, execute_cloud_insert,
)
from utils.model_router import call_model_json as _call_model_json

logger = logging.getLogger(__name__)


def _call_llm_json(system: str, user: str, max_tokens: int = 6000):
    return _call_model_json("cleaning", system, user, max_tokens=max_tokens, timeout=900)


# ══════════════════════════════════════════════════════════════════
# 辅助：数据收集
# ══════════════════════════════════════════════════════════════════

def _get_chain_stock_codes(chain_name: str) -> list[str]:
    """获取产业链下所有股票代码"""
    from config.chain_config import CHAINS
    chain = CHAINS.get(chain_name)
    if not chain:
        return []
    all_names = []
    for tier in chain.get("tiers", {}).values():
        all_names.extend(tier.get("stocks", []))
    if not all_names:
        return []
    ph = ",".join(["%s"] * len(all_names))
    rows = execute_query(
        f"SELECT stock_code FROM stock_info WHERE stock_name IN ({ph})",
        all_names,
    ) or []
    return [r["stock_code"] for r in rows if r.get("stock_code")]


def _get_chain_tiers_context(chain_name: str) -> str:
    """将产业链 tiers 结构序列化为 LLM 可读文本"""
    from config.chain_config import CHAINS
    chain = CHAINS.get(chain_name, {})
    lines = [f"产业链: {chain_name}"]
    for tier_key, tier in chain.get("tiers", {}).items():
        stocks = ", ".join(tier.get("stocks", [])[:8])
        lines.append(f"  [{tier_key}] {tier['label']} — 代表: {stocks}")
    return "\n".join(lines)


def _collect_knowledge_chunks(chain_name: str, stock_codes: list, since_date: str = None) -> str:
    """通过 hybrid_search 收集产业链相关知识 chunks"""
    try:
        from retrieval.hybrid import hybrid_search
    except ImportError:
        logger.warning("hybrid_search 不可用，跳过知识检索")
        return ""

    from config.chain_config import CHAINS
    chain = CHAINS.get(chain_name, {})

    queries = [chain_name]
    for tier in list(chain.get("tiers", {}).values())[:5]:
        queries.append(f"{chain_name} {tier['label']}")

    all_texts = []
    seen_ids = set()
    for q in queries:
        try:
            hr = hybrid_search(
                q,
                context={"stock_codes": stock_codes[:20], "theme_tags": [chain_name]},
                top_k=10,
            )
            for c in (hr.chunks or []):
                cid = getattr(c, "chunk_id", None) or id(c)
                if cid in seen_ids:
                    continue
                # 增量过滤
                if since_date and hasattr(c, "publish_time") and c.publish_time:
                    if str(c.publish_time)[:10] < since_date:
                        continue
                seen_ids.add(cid)
                text = getattr(c, "chunk_text", "") or getattr(c, "text", "")
                if text:
                    all_texts.append(text[:500])
        except Exception as e:
            logger.warning(f"hybrid_search({q}) 失败: {e}")

    return "\n---\n".join(all_texts[:30])


def _collect_research_reports(chain_name: str, since_date: str = None) -> str:
    """收集与该产业链关联的 tag_group_research 报告"""
    where_extra = ""
    params = [f"%{chain_name}%"]
    if since_date:
        where_extra = " AND r.created_at >= %s"
        params.append(since_date)

    rows = execute_query(
        f"""SELECT r.macro_json, r.industry_json, r.logic_synthesis_json
            FROM tag_group_research r
            INNER JOIN tag_groups g ON r.group_id = g.id
            WHERE g.tags_json LIKE ?{where_extra.replace('%s', '?')}
            ORDER BY r.created_at DESC LIMIT 3""",
        params,
    ) or []

    parts = []
    for r in rows:
        for field in ("macro_json", "industry_json", "logic_synthesis_json"):
            val = r.get(field)
            if val:
                try:
                    obj = json.loads(val) if isinstance(val, str) else val
                    parts.append(json.dumps(obj, ensure_ascii=False)[:1500])
                except Exception:
                    parts.append(str(val)[:1000])
    return "\n---\n".join(parts[:6])


def _collect_daily_intel(chain_name: str, since_date: str = None) -> str:
    """收集产业链情报（通过 chain_intel_map 关联）"""
    params = [chain_name]
    date_filter = ""
    if since_date:
        date_filter = " AND d.scan_date >= %s"
        params.append(since_date)

    rows = execute_cloud_query(
        f"""SELECT d.stock_name, d.industry, d.event_type, d.event_summary, d.scan_date
            FROM daily_intel_stocks d
            INNER JOIN chain_intel_map m ON d.id = m.intel_id
            WHERE m.chain_name = %s{date_filter}
            ORDER BY d.scan_date DESC
            LIMIT 50""",
        params,
    ) or []

    lines = []
    for r in rows:
        lines.append(f"[{r['scan_date']}] {r['stock_name']}({r.get('industry','')}) "
                     f"{r.get('event_type','')}: {(r.get('event_summary') or '')[:200]}")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════
# Baseline 生成
# ══════════════════════════════════════════════════════════════════

_BASELINE_SYSTEM = """你是产业链研究专家，精通 Porter 五力模型、产业生命周期、价值链分析和宏观经济传导机制。
根据用户提供的产业链配置、关联研究报告和知识库信息，生成一份**深度结构化的行业认知 Baseline**。

## 分析框架要求

### 1. 产业链结构 (structure)
对每个环节进行深度分析：
- 细分赛道及代表公司
- 该环节的技术壁垒和进入门槛
- 行业集中度和竞争格局(CR5/CR10估算)
- 所处生命周期阶段(导入期/成长期/成熟期/衰退期)

### 2. 供需-成本-收入 Driver 矩阵 (drivers)
对每个环节穷尽分析：
- 供给因素：产能、良率、资本开支周期、技术迭代
- 需求因素：下游应用拉动、替代需求、政策驱动
- 成本要素(重点)：每项成本需带具体行业数据说明(占比/趋势/驱动因素)
- 收入要素(重点)：每项收入需带量价拆分、增速驱动因素
- 竞争格局：Porter五力分析总结

### 3. 宏观关系图 (macro_relations)
运用宏观-股票传导机制框架，穷尽分析：
- 货币政策指标(利率、M2、社融)的传导链
- 汇率变动的影响路径
- 产业政策、监管变化的影响
- 全球供应链/贸易环境的影响
- 大宗商品价格联动
每个指标务必说明：传导路径 → 影响哪个环节 → 具体如何影响收入/成本

## 输出严格 JSON，结构如下：
{
  "structure": [
    {
      "tier_key": "环节key",
      "tier_label": "环节名称",
      "key_segments": ["细分1", "细分2"],
      "description": "该环节详细概述(2-3句)",
      "key_companies": ["公司1", "公司2"],
      "entry_barriers": "进入壁垒说明",
      "concentration": "行业集中度(如 CR5≈60%)",
      "lifecycle_stage": "成长期/成熟期等",
      "value_share": "该环节在产业链中的价值占比估算"
    }
  ],
  "drivers": [
    {
      "tier_key": "环节key",
      "supply_factors": ["供给因子1", "供给因子2"],
      "demand_factors": ["需求因子1", "需求因子2"],
      "cost_elements": [
        {
          "name": "成本项名称",
          "share_pct": "占总成本比例(如35%)",
          "trend": "上升/下降/稳定",
          "driver": "驱动因素说明",
          "industry_data": "相关行业数据/价格/趋势描述"
        }
      ],
      "revenue_elements": [
        {
          "name": "收入项名称",
          "volume_driver": "量的驱动因素",
          "price_driver": "价的驱动因素",
          "growth_rate": "近期增速估算",
          "industry_data": "相关市场规模/增长数据"
        }
      ],
      "competition": "Porter五力总结：供应商议价力x/买方议价力x/新进入者威胁x/替代品威胁x/行业竞争x(高/中/低)"
    }
  ],
  "macro_relations": [
    {
      "macro_indicator": "宏观指标名",
      "indicator_category": "货币政策/财政政策/汇率/大宗商品/产业政策/国际贸易",
      "transmission_path": "A→B→C的完整传导链(具体到影响哪个环节的收入/成本)",
      "affected_tiers": ["受影响环节key"],
      "impact_direction": "positive/negative",
      "impact_mechanism": "具体影响成本端还是收入端，如何量化",
      "lag_periods": "传导时滞(如1-3个月)",
      "confidence": "high/medium/low",
      "historical_evidence": "历史案例简述(如有)"
    }
  ]
}

要求：
- structure 完整覆盖产业链各环节，附详细描述、壁垒、集中度
- drivers 穷尽每个环节的供需-成本-收入-竞争五维分析；成本和收入必须是对象数组(含数据)
- macro_relations 至少列出8个以上宏观指标及完整传导链
- 基于通识+提供的实际数据，务实客观，尽量带具体数据/比例
- 只输出 JSON，不要其他文字"""


def generate_baseline(chain_name: str, progress_callback=None) -> dict:
    """首次生成或重建产业链认知 Baseline"""
    def _progress(msg, pct=None):
        if progress_callback:
            progress_callback(msg, pct)

    _progress("收集产业链配置", 5)
    tiers_ctx = _get_chain_tiers_context(chain_name)
    stock_codes = _get_chain_stock_codes(chain_name)

    _progress("检索知识库 chunks", 20)
    knowledge = _collect_knowledge_chunks(chain_name, stock_codes)

    _progress("收集研究报告", 35)
    research = _collect_research_reports(chain_name)

    _progress("收集每日情报", 45)
    intel = _collect_daily_intel(chain_name)

    # 组装用户消息
    user_msg = f"""## 产业链配置
{tiers_ctx}

## 关联研究报告
{research[:4000] if research else '（暂无）'}

## 知识库相关内容
{knowledge[:4000] if knowledge else '（暂无）'}

## 近期情报
{intel[:3000] if intel else '（暂无）'}

请根据以上信息和你的行业通识，为"{chain_name}"产业链生成结构化行业认知 Baseline（JSON）。"""

    _progress("AI 生成 Baseline", 55)
    baseline_json = _call_llm_json(_BASELINE_SYSTEM, user_msg, max_tokens=12000)

    if not baseline_json or not isinstance(baseline_json, dict):
        raise ValueError(f"LLM 返回异常: {type(baseline_json)}")

    # 检查已有版本
    existing = execute_cloud_query(
        "SELECT MAX(version) as mv FROM chain_baseline WHERE chain_name=%s",
        [chain_name],
    )
    next_ver = 1
    if existing and existing[0].get("mv"):
        next_ver = existing[0]["mv"] + 1

    _progress("保存到数据库", 90)
    source_summary = f"首次生成: tiers={len(baseline_json.get('structure', []))}, " \
                     f"drivers={len(baseline_json.get('drivers', []))}, " \
                     f"macro={len(baseline_json.get('macro_relations', []))}"

    execute_cloud_insert(
        """INSERT INTO chain_baseline (chain_name, version, baseline_json, source_summary, created_at)
           VALUES (%s, %s, %s, %s, NOW())""",
        [chain_name, next_ver, json.dumps(baseline_json, ensure_ascii=False), source_summary],
    )

    _progress("完成", 100)
    return {"version": next_ver, "baseline": baseline_json}


# ══════════════════════════════════════════════════════════════════
# Diff 增量生成
# ══════════════════════════════════════════════════════════════════

_DIFF_SYSTEM = """你是产业链研究专家。对比现有行业认知 Baseline 与最新增量信息，
生成一份结构化的**变更报告（Diff）**。

输出严格 JSON，结构如下：
{
  "changes": [
    {
      "category": "structure|drivers|macro_relations",
      "change_type": "add|modify|remove",
      "tier_key": "相关环节（如适用）",
      "detail": "变更内容描述",
      "evidence": "来源佐证（引用具体事实/数据）",
      "importance": "high|medium|low"
    }
  ],
  "summary": "本次变更的整体摘要（2-3句话）"
}

要求：
- 只输出真正有意义的变更（新环节/公司、要素重大变化、新宏观关联等）
- 每项变更必须附带 evidence（引用来源中的具体事实）
- 如果没有实质性变化，changes 可以为空数组
- 只输出 JSON，不要其他文字"""


def generate_diff(chain_name: str, progress_callback=None) -> dict:
    """基于增量数据生成 Diff 报告"""
    def _progress(msg, pct=None):
        if progress_callback:
            progress_callback(msg, pct)

    # 1. 读取当前最新 baseline
    _progress("读取现有 Baseline", 5)
    rows = execute_cloud_query(
        """SELECT id, version, baseline_json, created_at
           FROM chain_baseline WHERE chain_name=%s
           ORDER BY version DESC LIMIT 1""",
        [chain_name],
    )
    if not rows:
        raise ValueError(f"产业链 '{chain_name}' 尚无 Baseline，请先生成")

    baseline_row = rows[0]
    base_version = baseline_row["version"]
    baseline_json = json.loads(baseline_row["baseline_json"]) if isinstance(baseline_row["baseline_json"], str) else baseline_row["baseline_json"]
    since_date = str(baseline_row["created_at"])[:10]

    # 2. 收集增量数据
    _progress("收集增量研究报告", 20)
    research = _collect_research_reports(chain_name, since_date=since_date)

    _progress("收集增量情报", 35)
    intel = _collect_daily_intel(chain_name, since_date=since_date)

    _progress("检索增量知识", 50)
    stock_codes = _get_chain_stock_codes(chain_name)
    knowledge = _collect_knowledge_chunks(chain_name, stock_codes, since_date=since_date)

    if not research and not intel and not knowledge:
        # 无增量数据
        _progress("无增量数据", 100)
        return {"no_changes": True, "message": f"自 {since_date} 以来无新增数据"}

    # 3. 调用 LLM 生成 Diff
    user_msg = f"""## 现有 Baseline（v{base_version}）
```json
{json.dumps(baseline_json, ensure_ascii=False)[:6000]}
```

## 增量研究报告（自 {since_date} 以来）
{research[:3000] if research else '（无新报告）'}

## 增量每日情报
{intel[:3000] if intel else '（无新情报）'}

## 增量知识库内容
{knowledge[:3000] if knowledge else '（无新知识）'}

请对比 Baseline 与上述增量信息，输出结构化 Diff JSON。"""

    _progress("AI 生成 Diff", 65)
    diff_json = _call_llm_json(_DIFF_SYSTEM, user_msg, max_tokens=4000)

    if not diff_json or not isinstance(diff_json, dict):
        raise ValueError(f"LLM Diff 返回异常: {type(diff_json)}")

    # 4. 存入数据库
    _progress("保存 Diff", 90)
    input_sources = {
        "since_date": since_date,
        "research_available": bool(research),
        "intel_count": len(intel.split("\n")) if intel else 0,
        "knowledge_available": bool(knowledge),
    }

    execute_cloud_insert(
        """INSERT INTO chain_baseline_diff
           (chain_name, base_version, status, diff_json, input_sources_json, created_at)
           VALUES (%s, %s, 'ready', %s, %s, NOW())""",
        [chain_name, base_version,
         json.dumps(diff_json, ensure_ascii=False),
         json.dumps(input_sources, ensure_ascii=False)],
    )

    # 获取刚插入的 ID
    id_row = execute_cloud_query(
        "SELECT MAX(id) as did FROM chain_baseline_diff WHERE chain_name=%s AND base_version=%s",
        [chain_name, base_version],
    )
    diff_id = id_row[0]["did"] if id_row else None

    _progress("完成", 100)
    return {"diff_id": diff_id, "base_version": base_version, "diff": diff_json}


# ══════════════════════════════════════════════════════════════════
# Diff 合并
# ══════════════════════════════════════════════════════════════════

_MERGE_SYSTEM = """你是产业链研究专家。根据用户确认的变更（Diff），将其应用到现有 Baseline 上，
生成**完整的新版 Baseline**。

要求：
- 将 Diff 中 add 类型的变更追加到相应位置
- 将 modify 类型的变更替换原有内容
- 将 remove 类型的变更从 Baseline 中删除
- 保持输出结构与原 Baseline 完全一致
- 只输出完整的新 Baseline JSON（与原结构相同）
- 不要输出其他文字"""


def merge_diff(chain_name: str, diff_id: int) -> dict:
    """将用户确认的 Diff 合并到 Baseline，生成新版本"""
    # 1. 读取 Diff
    diff_row = execute_cloud_query(
        "SELECT * FROM chain_baseline_diff WHERE id=%s AND chain_name=%s",
        [diff_id, chain_name],
    )
    if not diff_row:
        raise ValueError(f"Diff #{diff_id} 不存在")
    diff_row = diff_row[0]

    if diff_row["status"] not in ("ready", "editing"):
        raise ValueError(f"Diff 状态为 {diff_row['status']}，无法合并")

    # 优先使用用户编辑版本
    diff_content = diff_row.get("user_edited_json") or diff_row.get("diff_json")
    if isinstance(diff_content, str):
        diff_content = json.loads(diff_content)

    base_version = diff_row["base_version"]

    # 2. 读取对应的 Baseline
    baseline_row = execute_cloud_query(
        "SELECT baseline_json FROM chain_baseline WHERE chain_name=%s AND version=%s",
        [chain_name, base_version],
    )
    if not baseline_row:
        raise ValueError(f"Baseline v{base_version} 不存在")

    baseline_json = baseline_row[0]["baseline_json"]
    if isinstance(baseline_json, str):
        baseline_json = json.loads(baseline_json)

    # 3. LLM 合并
    user_msg = f"""## 现有 Baseline（v{base_version}）
```json
{json.dumps(baseline_json, ensure_ascii=False)}
```

## 已确认的变更（Diff）
```json
{json.dumps(diff_content, ensure_ascii=False)}
```

请将 Diff 中的变更应用到 Baseline，输出完整的新 Baseline JSON。"""

    new_baseline = _call_llm_json(_MERGE_SYSTEM, user_msg, max_tokens=6000)
    if not new_baseline or not isinstance(new_baseline, dict):
        raise ValueError(f"LLM 合并返回异常: {type(new_baseline)}")

    # 4. 写入新版本
    new_version = base_version + 1
    source_summary = f"合并 Diff #{diff_id}: {len(diff_content.get('changes', []))} 项变更"

    execute_cloud_insert(
        """INSERT INTO chain_baseline (chain_name, version, baseline_json, source_summary, created_at)
           VALUES (%s, %s, %s, %s, NOW())
           ON DUPLICATE KEY UPDATE baseline_json=VALUES(baseline_json), source_summary=VALUES(source_summary)""",
        [chain_name, new_version, json.dumps(new_baseline, ensure_ascii=False), source_summary],
    )

    # 5. 更新 Diff 状态
    execute_cloud_insert(
        "UPDATE chain_baseline_diff SET status='merged', merged_to_version=%s, updated_at=NOW() WHERE id=%s",
        [new_version, diff_id],
    )

    return {"new_version": new_version, "baseline": new_baseline}


# ══════════════════════════════════════════════════════════════════
# 查询接口
# ══════════════════════════════════════════════════════════════════

def get_latest_baseline(chain_name: str) -> Optional[dict]:
    """获取最新 Baseline"""
    rows = execute_cloud_query(
        """SELECT id, chain_name, version, baseline_json, source_summary, created_at
           FROM chain_baseline WHERE chain_name=%s
           ORDER BY version DESC LIMIT 1""",
        [chain_name],
    )
    if not rows:
        return None
    row = dict(rows[0])
    if isinstance(row["baseline_json"], str):
        row["baseline_json"] = json.loads(row["baseline_json"])
    return row


def get_baseline_history(chain_name: str) -> list:
    """获取版本历史列表（不含完整 JSON）"""
    rows = execute_cloud_query(
        """SELECT id, version, source_summary, created_at
           FROM chain_baseline WHERE chain_name=%s
           ORDER BY version DESC""",
        [chain_name],
    ) or []
    return [dict(r) for r in rows]


def get_latest_diff(chain_name: str) -> Optional[dict]:
    """获取最新待审核的 Diff"""
    rows = execute_cloud_query(
        """SELECT * FROM chain_baseline_diff
           WHERE chain_name=%s AND status IN ('ready','editing')
           ORDER BY created_at DESC LIMIT 1""",
        [chain_name],
    )
    if not rows:
        return None
    row = dict(rows[0])
    for f in ("diff_json", "user_edited_json", "input_sources_json"):
        if row.get(f) and isinstance(row[f], str):
            try:
                row[f] = json.loads(row[f])
            except Exception:
                pass
    return row


def save_diff_edit(diff_id: int, edited_json: dict):
    """保存用户编辑后的 Diff"""
    execute_cloud_insert(
        "UPDATE chain_baseline_diff SET user_edited_json=%s, status='editing', updated_at=NOW() WHERE id=%s",
        [json.dumps(edited_json, ensure_ascii=False), diff_id],
    )


def reject_diff(diff_id: int):
    """拒绝 Diff"""
    execute_cloud_insert(
        "UPDATE chain_baseline_diff SET status='rejected', updated_at=NOW() WHERE id=%s",
        [diff_id],
    )

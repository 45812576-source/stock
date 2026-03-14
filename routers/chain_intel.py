"""产业链情报分类模块 — routers/chain_intel.py

功能：
- 从 daily_intel_stocks 拉取情报，用 AI 归类到产业链+环节
- 支持动态新建产业链/环节（写 chain_config_dynamic 表）
- 提供前端查询接口
"""
import json
import logging
from pathlib import Path

from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from utils.db_utils import execute_query, execute_insert, execute_cloud_query, execute_cloud_insert
from utils.model_router import call_model_json
from config.chain_config import CHAINS

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/chain-intel", tags=["chain-intel"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


# ── 构建产业链上下文（给AI参考） ─────────────────────────────────

def _build_chain_context() -> str:
    """所有静态链+动态链的 chain>tier(label) 列表"""
    lines = []
    # 静态配置
    for chain_name, chain in CHAINS.items():
        for tier_key, tier in chain["tiers"].items():
            lines.append(f"{chain_name} > {tier_key}（{tier['label']}）")
    # 动态配置
    dyn = execute_query(
        "SELECT chain_name, tier_key, tier_label FROM chain_config_dynamic ORDER BY chain_name, tier_key"
    ) or []
    for r in dyn:
        entry = f"{r['chain_name']} > {r['tier_key']}（{r['tier_label']}）"
        if entry not in lines:
            lines.append(entry)
    return "\n".join(lines)


# ── AI 分类单条 intel ─────────────────────────────────────────────

def _classify_intel(intel: dict) -> dict | None:
    """调用 AI 判断 intel 属于哪个产业链/环节，返回分类结果"""
    chain_context = _build_chain_context()

    system = """你是A股产业链分类专家。根据股票信息，判断它最合适属于哪条产业链的哪个环节。

规则：
1. 优先从已有产业链列表中选择最匹配的
2. 若确实不属于任何已有链，才新建（is_new=true）
3. tier_key 格式参考：上上游/上游/中游/下游/下下游/配套，或 "上游-细分名称"
4. 必须返回合法JSON，不要输出其他内容

返回格式：
{"chain_name":"xxx","tier_key":"xxx","tier_label":"xxx","is_new":false}"""

    user = f"""已有产业链列表：
{chain_context}

待分类股票：
股票名称：{intel.get('stock_name', '')}
所属行业：{intel.get('industry', '')}
业务描述：{intel.get('business_desc', '')}
事件类型：{intel.get('event_type', '')}
事件摘要：{intel.get('event_summary', '') or ''}

请输出JSON分类结果。"""

    try:
        result = call_model_json("hotspot", system, user)
        if not result or "chain_name" not in result:
            logger.warning(f"AI分类返回异常: {result}")
            return None
        return result
    except Exception as e:
        logger.error(f"AI分类失败 intel_id={intel.get('id')}: {e}")
        return None


def _ensure_dynamic_chain(chain_name: str, tier_key: str, tier_label: str):
    """如果该链/环节不在静态配置，写入 chain_config_dynamic"""
    if chain_name in CHAINS and tier_key in CHAINS[chain_name]["tiers"]:
        return
    execute_insert(
        """INSERT IGNORE INTO chain_config_dynamic (chain_name, tier_key, tier_label)
           VALUES (%s, %s, %s)""",
        [chain_name, tier_key, tier_label],
    )


def _save_classification(intel_id: int, chain_name: str, tier_key: str, tier_label: str):
    """写入 chain_intel_map（云端）"""
    execute_cloud_insert(
        """INSERT INTO chain_intel_map (intel_id, chain_name, tier_key, tier_label)
           VALUES (%s, %s, %s, %s)
           ON DUPLICATE KEY UPDATE
             chain_name=VALUES(chain_name),
             tier_key=VALUES(tier_key),
             tier_label=VALUES(tier_label),
             classified_at=CURRENT_TIMESTAMP""",
        [intel_id, chain_name, tier_key, tier_label],
    )


# ── 批量分类任务（后台执行） ──────────────────────────────────────

_classify_running = False


def _run_classify_batch(limit: int = 50):
    """后台批量分类：取未分类的 intel，逐条 AI 分类后写库"""
    global _classify_running
    if _classify_running:
        return
    _classify_running = True
    try:
        rows = execute_cloud_query(
            """SELECT d.* FROM daily_intel_stocks d
               LEFT JOIN chain_intel_map m ON d.id = m.intel_id
               WHERE m.intel_id IS NULL
               ORDER BY d.scan_date DESC, d.id DESC
               LIMIT %s""",
            [limit],
        ) or []

        logger.info(f"开始分类 {len(rows)} 条未分类情报")
        ok = 0
        for intel in rows:
            intel = dict(intel)
            result = _classify_intel(intel)
            if not result:
                continue
            chain_name = result["chain_name"]
            tier_key = result["tier_key"]
            tier_label = result["tier_label"]
            is_new = result.get("is_new", False)

            if is_new:
                _ensure_dynamic_chain(chain_name, tier_key, tier_label)

            _save_classification(intel["id"], chain_name, tier_key, tier_label)
            ok += 1

        logger.info(f"分类完成：{ok}/{len(rows)} 条成功")
    except Exception as e:
        logger.error(f"批量分类异常: {e}")
    finally:
        _classify_running = False


# ── 页面入口 ──────────────────────────────────────────────────────

@router.get("", response_class=HTMLResponse)
def chain_intel_page(request: Request):
    return templates.TemplateResponse("chain_intel.html", {
        "request": request,
        "active_page": "chain-intel",
    })


# ── API: 触发批量分类 ─────────────────────────────────────────────

@router.post("/api/classify")
def api_classify(body: dict, background_tasks: BackgroundTasks):
    """触发后台批量分类"""
    global _classify_running
    if _classify_running:
        return {"ok": False, "message": "分类任务正在运行中"}
    limit = min(int(body.get("limit", 50)), 200)
    background_tasks.add_task(_run_classify_batch, limit)
    return {"ok": True, "message": f"已启动后台分类（最多 {limit} 条）"}


# ── API: 分类单条（同步） ─────────────────────────────────────────

@router.post("/api/classify-one")
def api_classify_one(body: dict):
    """同步分类单条 intel，返回结果"""
    intel_id = body.get("intel_id")
    if not intel_id:
        return JSONResponse({"ok": False, "error": "缺少 intel_id"})

    row = execute_cloud_query(
        "SELECT * FROM daily_intel_stocks WHERE id=%s", [intel_id]
    ) or []
    if not row:
        return JSONResponse({"ok": False, "error": "intel 不存在"})

    intel = dict(row[0])
    result = _classify_intel(intel)
    if not result:
        return JSONResponse({"ok": False, "error": "AI分类失败"})

    if result.get("is_new"):
        _ensure_dynamic_chain(result["chain_name"], result["tier_key"], result["tier_label"])
    _save_classification(intel_id, result["chain_name"], result["tier_key"], result["tier_label"])

    return {"ok": True, "intel_id": intel_id, "result": result}


# ── API: 覆盖分类（手动修正） ─────────────────────────────────────

@router.post("/api/reclassify")
def api_reclassify(body: dict):
    """手动覆盖某条 intel 的分类"""
    intel_id = body.get("intel_id")
    chain_name = body.get("chain_name", "").strip()
    tier_key = body.get("tier_key", "").strip()
    tier_label = body.get("tier_label", "").strip()
    if not all([intel_id, chain_name, tier_key, tier_label]):
        return JSONResponse({"ok": False, "error": "参数不完整"})

    _ensure_dynamic_chain(chain_name, tier_key, tier_label)
    _save_classification(intel_id, chain_name, tier_key, tier_label)
    return {"ok": True}


# ── API: 未分类情报列表 ───────────────────────────────────────────

@router.get("/api/unclassified")
def api_unclassified(limit: int = 30):
    rows = execute_cloud_query(
        """SELECT d.id, d.scan_date, d.stock_name, d.stock_code,
                  d.industry, d.business_desc, d.event_type, d.event_summary
           FROM daily_intel_stocks d
           LEFT JOIN chain_intel_map m ON d.id = m.intel_id
           WHERE m.intel_id IS NULL
           ORDER BY d.scan_date DESC, d.id DESC
           LIMIT %s""",
        [limit],
    ) or []
    return {
        "ok": True,
        "total": len(rows),
        "items": [dict(r) for r in rows],
    }


# ── API: 已分类情报列表（按链/日期过滤） ─────────────────────────

@router.get("/api/classified")
def api_classified(chain_name: str = "", date: str = "", limit: int = 50):
    wheres = ["1=1"]
    params = []
    if chain_name:
        wheres.append("m.chain_name=%s")
        params.append(chain_name)
    if date:
        wheres.append("d.scan_date=%s")
        params.append(date)
    where_str = " AND ".join(wheres)

    rows = execute_cloud_query(
        f"""SELECT d.id, d.scan_date, d.stock_name, d.stock_code,
                   d.industry, d.business_desc, d.event_type, d.event_summary,
                   m.chain_name, m.tier_key, m.tier_label
            FROM daily_intel_stocks d
            INNER JOIN chain_intel_map m ON d.id = m.intel_id
            WHERE {where_str}
            ORDER BY d.scan_date DESC, d.id DESC
            LIMIT %s""",
        params + [limit],
    ) or []
    return {
        "ok": True,
        "total": len(rows),
        "items": [dict(r) for r in rows],
    }


# ── API: 分类状态统计 ─────────────────────────────────────────────

@router.get("/api/stats")
def api_stats():
    total = (execute_cloud_query("SELECT COUNT(*) cnt FROM daily_intel_stocks") or [{}])[0].get("cnt", 0)
    classified = (execute_cloud_query("SELECT COUNT(*) cnt FROM chain_intel_map") or [{}])[0].get("cnt", 0)
    # 按产业链分布
    by_chain = execute_cloud_query(
        """SELECT chain_name, COUNT(*) cnt FROM chain_intel_map
           GROUP BY chain_name ORDER BY cnt DESC"""
    ) or []
    # 最近分类日期
    latest = execute_cloud_query(
        """SELECT MAX(d.scan_date) mx FROM daily_intel_stocks d
           INNER JOIN chain_intel_map m ON d.id=m.intel_id"""
    ) or []
    return {
        "ok": True,
        "total": int(total),
        "classified": int(classified),
        "unclassified": int(total) - int(classified),
        "running": _classify_running,
        "by_chain": [{"chain": r["chain_name"], "cnt": r["cnt"]} for r in by_chain],
        "latest_date": str(latest[0]["mx"]) if latest and latest[0]["mx"] else "",
    }


# ── API: 某产业链+某环节的情报（供明细页调用） ───────────────────

@router.get("/api/by-chain")
def api_by_chain(chain_name: str, tier_key: str = "", limit: int = 20):
    """返回某产业链（某环节）的情报列表，供明细页展示"""
    if tier_key:
        rows = execute_cloud_query(
            """SELECT d.id, d.scan_date, d.stock_name, d.stock_code,
                      d.industry, d.event_type, d.event_summary,
                      m.tier_key, m.tier_label
               FROM daily_intel_stocks d
               INNER JOIN chain_intel_map m ON d.id=m.intel_id
               WHERE m.chain_name=%s AND m.tier_key=%s
               ORDER BY d.scan_date DESC, d.id DESC
               LIMIT %s""",
            [chain_name, tier_key, limit],
        ) or []
    else:
        rows = execute_cloud_query(
            """SELECT d.id, d.scan_date, d.stock_name, d.stock_code,
                      d.industry, d.event_type, d.event_summary,
                      m.tier_key, m.tier_label
               FROM daily_intel_stocks d
               INNER JOIN chain_intel_map m ON d.id=m.intel_id
               WHERE m.chain_name=%s
               ORDER BY d.scan_date DESC, d.id DESC
               LIMIT %s""",
            [chain_name, limit],
        ) or []
    return {
        "ok": True,
        "chain_name": chain_name,
        "items": [dict(r) for r in rows],
    }

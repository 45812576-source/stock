"""自选页面路由 — 策略管理 + 股票筛选 + 手动管理"""
import json
import logging
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from utils.db_utils import execute_query, execute_insert
from utils.auth_deps import get_current_user, get_optional_user, TokenData
from utils.quota_service import check_portfolio_limit, consume_quota

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/portfolio", tags=["portfolio"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


def _trigger_report_pull(stock_code: str):
    """后台线程补拉东方财富研报（若该股票在库中无研报）"""
    import threading
    def _bg(code: str):
        try:
            existing = execute_query(
                "SELECT id FROM source_documents WHERE source='eastmoney_report' AND text_content LIKE %s LIMIT 1",
                [f"%({code})%"],
            )
            if not existing:
                from ingestion.eastmoney_report_source import EastmoneyReportSource
                EastmoneyReportSource().fetch_by_stock_codes([code], days=730, per_stock_limit=50)
                logger.info(f"[Portfolio] 研报补拉完成: {code}")
        except Exception as e:
            logger.warning(f"[Portfolio] 研报补拉失败 {code}: {e}")
    threading.Thread(target=_bg, args=(stock_code,), daemon=True).start()


# ==================== 页面路由 ====================

@router.get("", response_class=HTMLResponse)
def portfolio_page(request: Request):
    """自选页面"""
    return templates.TemplateResponse("portfolio.html", {
        "request": request,
        "active_page": "portfolio",
    })


# ==================== API ====================

@router.get("/api/strategies", response_class=JSONResponse)
def api_strategies():
    """返回 95 个预置选股策略标签（来自 PRESET_RULES）"""
    from config.stock_selection_presets import PRESET_RULES, RULE_CATEGORIES
    result = []
    for i, r in enumerate(PRESET_RULES):
        cat = r.get("category", "")
        cat_meta = RULE_CATEGORIES.get(cat, {})
        result.append({
            "id": i,
            "strategy_name": r["rule_name"],
            "category": cat,
            "category_label": cat_meta.get("label", ""),
            "definition": r.get("definition", ""),
        })
    return {"strategies": result}


@router.get("/api/strategy/{strategy_id}/stocks", response_class=JSONResponse)
def api_strategy_stocks(strategy_id: int):
    """返回策略内股票 + 实时行情摘要"""
    rows = execute_query(
        """SELECT ss.*, si.industry_l1, si.market_cap,
                  sd.close as latest_price, sd.change_pct, sd.trade_date, sd.turnover_rate
           FROM strategy_stocks ss
           LEFT JOIN stock_info si ON ss.stock_code = si.stock_code
           LEFT JOIN (
               SELECT sd1.stock_code, sd1.close, sd1.change_pct, sd1.trade_date, sd1.turnover_rate
               FROM stock_daily sd1
               INNER JOIN (
                   SELECT stock_code, MAX(trade_date) as max_date
                   FROM stock_daily GROUP BY stock_code
               ) sd2 ON sd1.stock_code = sd2.stock_code AND sd1.trade_date = sd2.max_date
           ) sd ON ss.stock_code = sd.stock_code
           WHERE ss.strategy_id=%s AND ss.status IN ('active', 'recommendation')
           ORDER BY ss.status DESC, ss.added_at DESC""",
        [strategy_id],
    )
    stocks = [dict(r) for r in (rows or [])]
    return {"stocks": stocks}


@router.post("/api/strategy/{strategy_id}/add-stock", response_class=JSONResponse)
async def api_add_stock(strategy_id: int, request: Request):
    """手动添加股票到策略"""
    data = await request.json()
    stock_code = (data.get("stock_code") or "").strip()
    stock_name = (data.get("stock_name") or "").strip()
    notes = (data.get("notes") or "").strip()

    if not stock_code:
        return JSONResponse({"ok": False, "error": "股票代码不能为空"}, status_code=400)

    # 如果没有名字，尝试从 stock_info 获取
    if not stock_name:
        info = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=%s", [stock_code])
        stock_name = info[0]["stock_name"] if info else ""

    try:
        execute_insert(
            """INSERT INTO strategy_stocks (strategy_id, stock_code, stock_name, source, status, notes)
               VALUES (%s, %s, %s, 'manual', 'active', %s)
               ON DUPLICATE KEY UPDATE status='active', notes=VALUES(notes), stock_name=VALUES(stock_name)""",
            [strategy_id, stock_code, stock_name, notes],
        )
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)

    _trigger_report_pull(stock_code)
    return {"ok": True}


@router.post("/api/strategy/{strategy_id}/remove-stock", response_class=JSONResponse)
async def api_remove_stock(strategy_id: int, request: Request):
    """移除股票"""
    data = await request.json()
    stock_code = (data.get("stock_code") or "").strip()
    execute_insert(
        "UPDATE strategy_stocks SET status='removed' WHERE strategy_id=%s AND stock_code=%s",
        [strategy_id, stock_code],
    )
    return {"ok": True}


@router.post("/api/strategy/{strategy_id}/accept-stock", response_class=JSONResponse)
async def api_accept_stock(strategy_id: int, request: Request):
    """采纳 AI 推荐"""
    data = await request.json()
    stock_code = (data.get("stock_code") or "").strip()
    execute_insert(
        "UPDATE strategy_stocks SET status='active' WHERE strategy_id=%s AND stock_code=%s",
        [strategy_id, stock_code],
    )
    _trigger_report_pull(stock_code)
    return {"ok": True}


@router.post("/api/strategy/{strategy_id}/reject-stock", response_class=JSONResponse)
async def api_reject_stock(strategy_id: int, request: Request):
    """剔除 AI 推荐"""
    data = await request.json()
    stock_code = (data.get("stock_code") or "").strip()
    execute_insert(
        "UPDATE strategy_stocks SET status='rejected' WHERE strategy_id=%s AND stock_code=%s",
        [strategy_id, stock_code],
    )
    return {"ok": True}


@router.post("/api/create-portfolio", response_class=JSONResponse)
async def api_create_portfolio(request: Request, user: TokenData = Depends(get_current_user)):
    """新建空策略组合"""
    data = await request.json()
    name = (data.get("name") or "").strip() or "新策略"
    user_id = user.user_id

    # 检查配额
    can_create, msg = check_portfolio_limit(user_id)
    if not can_create:
        return JSONResponse({"ok": False, "error": msg}, status_code=403)

    new_id = execute_insert(
        "INSERT INTO investment_strategies (strategy_name, user_id) VALUES (%s, %s)",
        [name, user_id],
    )
    return {"ok": True, "id": new_id}


# ==================== 策略筛选引擎 ====================

def _build_screening_sql(rules):
    """解析结构化条件，构建动态 SQL WHERE 子句"""
    conditions = []
    params = []

    for rule in rules:
        field = rule.get("field", "")
        op = rule.get("op", "eq")
        value = rule.get("value")

        if field == "industry_l1":
            col = "si.industry_l1"
        elif field == "industry_l2":
            col = "si.industry_l2"
        elif field == "market_cap":
            col = "si.market_cap"
        elif field == "pe_ratio":
            col = "sr.pe_ratio"
        elif field == "change_pct":
            col = "sd_latest.change_pct"
        elif field == "turnover_rate":
            col = "sd_latest.turnover_rate"
        elif field == "net_inflow_1d":
            col = "cf_1d.net"
        elif field == "net_inflow_5d":
            col = "cf_5d.net"
        elif field == "net_inflow_20d":
            col = "cf_20d.net"
        elif field == "tag_match":
            # 标签匹配走 tags_json LIKE
            if isinstance(value, list):
                for v in value:
                    conditions.append("ci_tags.tags_json LIKE %s")
                    params.append(f"%{v}%")
            else:
                conditions.append("ci_tags.tags_json LIKE %s")
                params.append(f"%{value}%")
            continue
        else:
            continue

        if op == "eq":
            conditions.append(f"{col} = %s")
            params.append(value)
        elif op == "ne":
            conditions.append(f"{col} != %s")
            params.append(value)
        elif op == "gt":
            conditions.append(f"{col} > %s")
            params.append(value)
        elif op == "gte":
            conditions.append(f"{col} >= %s")
            params.append(value)
        elif op == "lt":
            conditions.append(f"{col} < %s")
            params.append(value)
        elif op == "lte":
            conditions.append(f"{col} <= %s")
            params.append(value)
        elif op == "between" and isinstance(value, list) and len(value) == 2:
            conditions.append(f"{col} BETWEEN %s AND %s")
            params.extend(value)
        elif op == "in" and isinstance(value, list):
            placeholders = ",".join(["%s"] * len(value))
            conditions.append(f"{col} IN ({placeholders})")
            params.extend(value)
        elif op == "not_in" and isinstance(value, list):
            placeholders = ",".join(["%s"] * len(value))
            conditions.append(f"{col} NOT IN ({placeholders})")
            params.extend(value)
        elif op == "contains_any" and isinstance(value, list):
            or_parts = []
            for v in value:
                or_parts.append(f"{col} LIKE %s")
                params.append(f"%{v}%")
            if or_parts:
                conditions.append(f"({' OR '.join(or_parts)})")

    return conditions, params


@router.post("/api/strategy/{strategy_id}/run-screening", response_class=JSONResponse)
async def api_run_screening(strategy_id: int):
    """执行策略筛选（两阶段：结构化 + AI）"""
    # 获取策略
    strategy = execute_query(
        "SELECT * FROM investment_strategies WHERE id=%s AND is_active=1",
        [strategy_id],
    )
    if not strategy:
        return JSONResponse({"ok": False, "error": "策略不存在"}, status_code=404)

    strategy = dict(strategy[0])
    try:
        rules = json.loads(strategy.get("rules_json") or "[]")
    except (json.JSONDecodeError, TypeError):
        rules = []
    ai_rules = (strategy.get("ai_rules_text") or "").strip()

    # 阶段1: 结构化筛选
    base_sql = """
        SELECT DISTINCT si.stock_code, si.stock_name, si.industry_l1, si.market_cap
        FROM stock_info si
        LEFT JOIN stock_realtime sr ON si.stock_code = sr.stock_code
        LEFT JOIN (
            SELECT sd1.stock_code, sd1.close, sd1.change_pct, sd1.turnover_rate
            FROM stock_daily sd1
            INNER JOIN (SELECT stock_code, MAX(trade_date) as max_date FROM stock_daily GROUP BY stock_code) sd2
            ON sd1.stock_code = sd2.stock_code AND sd1.trade_date = sd2.max_date
        ) sd_latest ON si.stock_code = sd_latest.stock_code
        LEFT JOIN (
            SELECT stock_code, SUM(main_net_inflow) as net FROM capital_flow
            WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) GROUP BY stock_code
        ) cf_1d ON si.stock_code = cf_1d.stock_code
        LEFT JOIN (
            SELECT stock_code, SUM(main_net_inflow) as net FROM capital_flow
            WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 5 DAY) GROUP BY stock_code
        ) cf_5d ON si.stock_code = cf_5d.stock_code
        LEFT JOIN (
            SELECT stock_code, SUM(main_net_inflow) as net FROM capital_flow
            WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 20 DAY) GROUP BY stock_code
        ) cf_20d ON si.stock_code = cf_20d.stock_code
    """

    # 检查是否需要标签匹配
    has_tag_match = any(r.get("field") == "tag_match" for r in rules)
    if has_tag_match:
        base_sql += """
        LEFT JOIN (
            SELECT DISTINCT ic.stock_code, ci.tags_json
            FROM item_companies ic JOIN cleaned_items ci ON ic.cleaned_item_id = ci.id
        ) ci_tags ON si.stock_code = ci_tags.stock_code
        """

    conditions, params = _build_screening_sql(rules)

    if conditions:
        base_sql += " WHERE " + " AND ".join(conditions)

    base_sql += " LIMIT 200"

    try:
        candidates = execute_query(base_sql, params)
    except Exception as e:
        logger.error(f"策略筛选SQL失败: {e}")
        return JSONResponse({"ok": False, "error": f"筛选条件执行失败: {str(e)[:200]}"}, status_code=500)

    candidates = [dict(r) for r in (candidates or [])]

    if not candidates:
        return {"ok": True, "candidates": [], "ai_used": False, "message": "结构化筛选无结果，请检查条件"}

    # 阶段2: AI 精筛（如果有 ai_rules_text）
    if ai_rules and candidates:
        try:
            from utils.model_router import call_model as call_claude

            stock_list_text = "\n".join([
                f"{c['stock_code']} {c['stock_name']} 行业:{c.get('industry_l1','')} 市值:{c.get('market_cap','')}"
                for c in candidates[:100]
            ])

            system_prompt = f"""你是一个专业的股票筛选助手。根据以下选股原则，从候选股票中选出最符合的标的。

按 stock-recommendation Skill 的四维评分框架进行行业内相对排序：
1. 龙头属性（最高权重）：定价权/技术壁垒/定义产业进程/掌握受益资源
2. 受益直接性（高权重）：直接受益 > 间接受益 > 边际受益
3. 估值合理性（中权重）：PE相对行业中位数偏离度
4. 流动性（基础权重）：公募重仓/日成交额/资金流入

选股原则：
{ai_rules}

请返回 JSON 格式:
[{{"stock_code": "代码", "stock_name": "名称", "reason": "推荐理由", "score": 85, "tags": "龙头:技术壁垒,受益:直接,估值:合理"}}]

tags 从以下选择：龙头:定价权/龙头:技术壁垒/龙头:定义产业/龙头:受益资源/受益:直接/受益:间接/受益:边际/估值:低估/估值:合理/估值:偏高/成长:高成长/成长:转型中/成长:稳定
只返回 JSON 数组，不要其他文字。最多推荐 20 只。score 范围 0-100。"""

            user_msg = f"候选股票列表（共{len(candidates)}只）：\n{stock_list_text}"

            ai_result = call_claude('research', system_prompt, user_msg, max_tokens=4096, timeout=120)

            # 解析 AI 返回
            # 尝试提取 JSON
            import re
            json_match = re.search(r'\[.*\]', ai_result, re.DOTALL)
            if json_match:
                ai_picks = json.loads(json_match.group())
                # 存入 strategy_stocks
                for pick in ai_picks:
                    code = pick.get("stock_code", "")
                    name = pick.get("stock_name", "")
                    reason = pick.get("reason", "")
                    try:
                        execute_insert(
                            """INSERT INTO strategy_stocks (strategy_id, stock_code, stock_name, source, status, ai_reason)
                               VALUES (%s, %s, %s, 'recommendation', 'recommendation', %s)
                               ON DUPLICATE KEY UPDATE ai_reason=VALUES(ai_reason), status='recommendation', source='recommendation'""",
                            [strategy_id, code, name, reason],
                        )
                    except Exception:
                        pass

                # 筛选完成后自动创建/更新策略专属 watchlist list
                _upsert_strategy_list(strategy_id, strategy["strategy_name"], ai_picks)

                return {
                    "ok": True,
                    "candidates": ai_picks,
                    "total_candidates": len(candidates),
                    "ai_used": True,
                    "message": f"结构化筛选 {len(candidates)} 只 → AI 推荐 {len(ai_picks)} 只",
                }
        except Exception as e:
            logger.error(f"AI 筛选失败: {e}")
            # AI 失败则返回结构化结果
            return {
                "ok": True,
                "candidates": [{"stock_code": c["stock_code"], "stock_name": c["stock_name"], "reason": "", "score": 0} for c in candidates[:50]],
                "total_candidates": len(candidates),
                "ai_used": False,
                "message": f"AI 筛选失败，返回结构化结果 {len(candidates)} 只",
            }

    # 无 AI 规则，直接返回结构化结果
    return {
        "ok": True,
        "candidates": [{"stock_code": c["stock_code"], "stock_name": c["stock_name"], "reason": "", "score": 0} for c in candidates[:50]],
        "total_candidates": len(candidates),
        "ai_used": False,
        "message": f"结构化筛选结果 {len(candidates)} 只",
    }


# ==================== Watchlist List 辅助函数 ====================

def _upsert_strategy_list(strategy_id: int, strategy_name: str, picks: list):
    """筛选完成后，自动创建/更新策略专属 watchlist list"""
    try:
        existing = execute_query(
            "SELECT id FROM watchlist_lists WHERE list_type='strategy' AND strategy_id=%s",
            [strategy_id],
        )
        if existing:
            list_id = existing[0]["id"]
        else:
            list_id = execute_insert(
                "INSERT INTO watchlist_lists (list_type, list_name, strategy_id, show_on_overview) VALUES ('strategy', %s, %s, 0)",
                [strategy_name, strategy_id],
            )
        for pick in (picks or []):
            code = pick.get("stock_code", "")
            name = pick.get("stock_name", "")
            reason = pick.get("reason", "")
            if code:
                execute_insert(
                    """INSERT INTO watchlist_list_stocks (list_id, stock_code, stock_name, source, ai_reason)
                       VALUES (%s, %s, %s, 'strategy_screen', %s)
                       ON DUPLICATE KEY UPDATE ai_reason=VALUES(ai_reason), status='active'""",
                    [list_id, code, name, reason],
                )
    except Exception as e:
        logger.warning(f"_upsert_strategy_list 失败: {e}")


def _get_stock_price(stock_code: str) -> dict:
    """获取最新行情（用于 list 股票展示）"""
    try:
        rows = execute_query(
            """SELECT sd.close as price, sd.change_pct, sd.trade_date
               FROM stock_daily sd
               WHERE sd.stock_code=%s
               ORDER BY sd.trade_date DESC LIMIT 1""",
            [stock_code],
        )
        return dict(rows[0]) if rows else {}
    except Exception:
        return {}


# ==================== Watchlist Lists CRUD ====================

@router.get("/api/lists")
def api_get_lists(user: TokenData = Depends(get_optional_user)):
    """返回当前用户的所有 watchlist list 及各 list 股票数（可选登录，未登录用 user_id=1）"""
    user_id = user.user_id if user else 1
    try:
        rows = execute_query(
            """SELECT wl.*, COUNT(wls.id) as stock_count
               FROM watchlist_lists wl
               LEFT JOIN watchlist_list_stocks wls ON wl.id=wls.list_id AND wls.status='active'
               WHERE wl.user_id = %s
               GROUP BY wl.id
               ORDER BY wl.list_type, wl.sort_order, wl.id""",
            [user_id]
        ) or []
        return {"ok": True, "lists": [dict(r) for r in rows]}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]})


@router.post("/api/lists")
async def api_create_list(request: Request, user: TokenData = Depends(get_current_user)):
    """创建新 list（like 或 theme 类型）"""
    data = await request.json()
    list_type = data.get("list_type", "theme")
    list_name = (data.get("list_name") or "").strip()
    description = (data.get("description") or "").strip()
    background_info = (data.get("background_info") or "").strip()
    user_id = user.user_id

    if not list_name:
        return JSONResponse({"ok": False, "error": "list_name 不能为空"}, status_code=400)
    if list_type not in ("like", "theme"):
        return JSONResponse({"ok": False, "error": "list_type 必须为 like 或 theme"}, status_code=400)

    # 如果是theme类型（portfolio），检查配额
    if list_type == "theme":
        can_create, msg = check_portfolio_limit(user_id)
        if not can_create:
            return JSONResponse({"ok": False, "error": msg}, status_code=403)

    try:
        new_id = execute_insert(
            "INSERT INTO watchlist_lists (list_type, list_name, description, background_info, user_id) VALUES (%s, %s, %s, %s, %s)",
            [list_type, list_name, description or None, background_info or None, user_id],
        )
        return {"ok": True, "id": new_id}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


@router.put("/api/lists/{list_id}")
async def api_update_list(list_id: int, request: Request):
    """更新 list 名称/描述/背景信息/投资逻辑"""
    data = await request.json()
    fields, params = [], []
    for col in ["list_name", "description", "background_info", "investment_logic"]:
        if col in data:
            fields.append(f"{col}=%s")
            params.append(data[col])
    if not fields:
        return JSONResponse({"ok": False, "error": "没有要更新的字段"}, status_code=400)
    params.append(list_id)
    execute_insert(f"UPDATE watchlist_lists SET {', '.join(fields)}, updated_at=NOW() WHERE id=%s", params)
    return {"ok": True}


@router.delete("/api/lists/{list_id}")
def api_delete_list(list_id: int):
    """删除 list（级联删除 stocks）"""
    if list_id == 1:
        return JSONResponse({"ok": False, "error": "默认收藏组不能删除"}, status_code=400)
    execute_insert("DELETE FROM watchlist_list_stocks WHERE list_id=%s", [list_id])
    execute_insert("DELETE FROM watchlist_lists WHERE id=%s", [list_id])
    return {"ok": True}


@router.post("/api/lists/reorder")
async def api_reorder_lists(request: Request):
    """保存 tab 排序（ids 数组，按顺序写 sort_order）"""
    data = await request.json()
    ids = data.get("ids") or []
    for i, lid in enumerate(ids):
        try:
            execute_insert("UPDATE watchlist_lists SET sort_order=%s WHERE id=%s", [i, lid])
        except Exception:
            pass
    return {"ok": True}


# ==================== List 内股票管理 ====================

@router.get("/api/lists/{list_id}/stocks")
def api_get_list_stocks(list_id: int):
    """返回 list 内股票 + 最新行情（自动同步缺失数据）"""
    try:
        # 先获取列表中的股票
        rows = execute_query(
            """SELECT wls.stock_code, wls.stock_name, wls.status
               FROM watchlist_list_stocks wls
               WHERE wls.list_id=%s AND wls.status='active'""",
            [list_id],
        )

        # 检查哪些股票没有数据，自动从云端同步
        from utils.db_utils import ensure_stock_data
        for r in (rows or []):
            code = r['stock_code']
            # 检查是否有数据
            cnt = execute_query(
                "SELECT COUNT(*) as cnt FROM stock_daily WHERE stock_code=%s",
                [code]
            )
            if not cnt or cnt[0]['cnt'] == 0:
                ensure_stock_data(code, days=180)

        # 重新查询带行情数据
        rows = execute_query(
            """SELECT wls.*, si.industry_l1,
                      sd.close as price, sd.change_pct, sd.trade_date
               FROM watchlist_list_stocks wls
               LEFT JOIN stock_info si ON wls.stock_code=si.stock_code
               LEFT JOIN (
                   SELECT sd1.stock_code, sd1.close, sd1.change_pct, sd1.trade_date
                   FROM stock_daily sd1
                   INNER JOIN (SELECT stock_code, MAX(trade_date) as mx FROM stock_daily GROUP BY stock_code) sd2
                   ON sd1.stock_code=sd2.stock_code AND sd1.trade_date=sd2.mx
               ) sd ON wls.stock_code=sd.stock_code
               WHERE wls.list_id=%s AND wls.status='active'
               ORDER BY wls.added_at DESC""",
            [list_id],
        )
        return {"ok": True, "stocks": [dict(r) for r in (rows or [])]}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]})


@router.post("/api/lists/{list_id}/stocks")
async def api_add_to_list(list_id: int, request: Request):
    """添加股票到 list"""
    data = await request.json()
    stocks = data.get("stocks") or [data]  # 支持批量
    added = 0
    for item in stocks:
        code = (item.get("stock_code") or "").strip()
        if not code:
            continue
        name = (item.get("stock_name") or "").strip()
        if not name:
            info = execute_query("SELECT stock_name FROM stock_info WHERE stock_code=%s", [code])
            name = info[0]["stock_name"] if info else ""
        source = item.get("source", "manual")
        reason = (item.get("ai_reason") or "").strip()
        try:
            execute_insert(
                """INSERT INTO watchlist_list_stocks (list_id, stock_code, stock_name, source, ai_reason)
                   VALUES (%s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE status='active', ai_reason=COALESCE(VALUES(ai_reason), ai_reason)""",
                [list_id, code, name, source, reason or None],
            )
            added += 1
        except Exception:
            pass
    return {"ok": True, "added": added}


@router.delete("/api/lists/{list_id}/stocks/{stock_code}")
def api_remove_from_list(list_id: int, stock_code: str):
    """从 list 移除股票"""
    execute_insert(
        "UPDATE watchlist_list_stocks SET status='removed' WHERE list_id=%s AND stock_code=%s",
        [list_id, stock_code],
    )
    return {"ok": True}


# ==================== 主题创建 (AI 推荐) ====================

@router.post("/api/theme/recommend")
async def api_theme_recommend(request: Request):
    """根据背景信息让 AI 推荐相关股票"""
    data = await request.json()
    background_info = (data.get("background_info") or "").strip()
    list_id = data.get("list_id")

    if not background_info:
        return JSONResponse({"ok": False, "error": "background_info 不能为空"}, status_code=400)

    # 如果有 list_id，更新背景信息
    if list_id:
        try:
            execute_insert(
                "UPDATE watchlist_lists SET background_info=%s WHERE id=%s",
                [background_info, list_id],
            )
        except Exception:
            pass

    # 获取全市场股票列表（取 stock_info 中的股票，最多 3000 只）
    try:
        all_stocks = execute_query(
            "SELECT stock_code, stock_name, industry_l1 FROM stock_info LIMIT 3000"
        ) or []
    except Exception:
        all_stocks = []

    stock_text = "\n".join([
        f"{s['stock_code']} {s['stock_name']} [{s.get('industry_l1', '')}]"
        for s in all_stocks[:2000]
    ])

    system_prompt = """你是专业股票投资顾问。根据用户提供的主题背景，从候选股票列表中推荐最相关的A股标的。

按 stock-recommendation Skill 的四维评分框架评估每只股票：
1. 龙头属性（最高权重）：定价权/技术壁垒/定义产业进程/掌握受益资源，满足任一即可
2. 受益直接性（高权重）：直接受益 > 间接受益 > 边际受益
3. 估值合理性（中权重）：PE相对行业中位数偏离度
4. 流动性（基础权重）：公募重仓/日成交额/资金流入

返回 JSON 格式：
[{"stock_code":"代码","stock_name":"名称","relevance":85,"reason":"相关理由（1-2句）","tags":"龙头:定价权,受益:直接,估值:合理"}]

要求：
- 只从提供的股票列表中选择
- relevance 范围 0-100，基于四维综合评分
- tags 从以下选择：龙头:定价权/龙头:技术壁垒/龙头:定义产业/龙头:受益资源/受益:直接/受益:间接/受益:边际/估值:低估/估值:合理/估值:偏高/成长:高成长/成长:转型中/成长:稳定
- 推荐 5-15 只，按综合得分降序排列
- 只输出 JSON 数组，不要任何其他文字"""

    user_msg = f"主题背景：{background_info}\n\n候选股票列表：\n{stock_text}"

    try:
        from utils.model_router import call_model_json
        result = call_model_json('ai_recommend', system_prompt, user_msg, max_tokens=2000, timeout=120)
        if isinstance(result, list):
            return {"ok": True, "stocks": result}
        elif isinstance(result, dict) and isinstance(result.get("stocks"), list):
            return {"ok": True, "stocks": result["stocks"]}
        return {"ok": True, "stocks": []}
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"AI 推荐失败: {str(e)[:200]}"}, status_code=500)


# ==================== 概览页 Watchlist 配置 ====================

@router.post("/api/overview-lists")
async def api_set_overview_lists(request: Request):
    """设置哪些 list 在概览页显示"""
    data = await request.json()
    list_ids = data.get("list_ids", [])

    try:
        # 先全部关闭
        execute_insert("UPDATE watchlist_lists SET show_on_overview=0", [])
        # 打开选中的
        for lid in list_ids:
            execute_insert(
                "UPDATE watchlist_lists SET show_on_overview=1 WHERE id=%s",
                [lid],
            )
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


# ==================== 搜索股票（辅助） ====================

@router.get("/api/search-stocks")
def api_search_stocks(q: str = "", limit: int = 20):
    """搜索股票代码/名称"""
    if not q.strip():
        return {"stocks": []}
    try:
        rows = execute_query(
            """SELECT stock_code, stock_name, industry_l1
               FROM stock_info
               WHERE stock_code LIKE %s OR stock_name LIKE %s
               LIMIT %s""",
            [f"%{q}%", f"%{q}%", limit],
        )
        return {"stocks": [dict(r) for r in (rows or [])]}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]})

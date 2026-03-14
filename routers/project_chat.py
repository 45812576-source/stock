"""Portfolio实验室 — 项目聊天 + 上下文 API"""
import json
import logging
import threading
import time
import uuid
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from portfolio.chat_handler import (
    get_chat_history,
    submit_chat_message,
    get_pending_reply,
)
from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/portfolio", tags=["portfolio-chat"])

# ── 分析任务进度追踪（内存，重启丢失，但任务完成后写 DB，安全）──
_analysis_tasks: dict = {}
_analysis_lock = threading.Lock()


# ── 聊天 API ──────────────────────────────────────────────────

@router.post("/api/projects/{project_id}/chat")
async def api_submit_chat(project_id: int, request: Request):
    data = await request.json()
    message = (data.get("message") or "").strip()
    strategy_ids = data.get("strategy_ids") or []
    if not message:
        return JSONResponse({"ok": False, "error": "消息不能为空"}, status_code=400)
    result = submit_chat_message(project_id, message, strategy_ids)
    return result


@router.get("/api/projects/{project_id}/chat/poll")
def api_poll_chat(project_id: int):
    return get_pending_reply(project_id)


@router.get("/api/projects/{project_id}/chat/history")
def api_chat_history(project_id: int):
    history = get_chat_history(project_id)
    return {"ok": True, "messages": history}


# ── 项目上下文 API（左侧面板数据）──────────────────────────────

@router.get("/api/projects/{project_id}/context")
def api_project_context(project_id: int):
    """返回项目左侧面板所需的所有上下文数据。
    优先读 analysis_json 缓存，无缓存时 fallback 到 tag_group_research 匹配逻辑。
    """
    project = execute_query("SELECT * FROM watchlist_lists WHERE id=%s", [project_id])
    if not project:
        return JSONResponse({"ok": False, "error": "项目不存在"}, status_code=404)
    project = dict(project[0])

    project_info = {
        "id": project["id"],
        "name": project["list_name"],
        "investment_logic": project.get("investment_logic") or "",
        "description": project.get("description") or "",
        "project_type": project.get("project_type", "custom"),
        "background_info": project.get("background_info") or "",
        "source_group_id": project.get("source_group_id"),
        "has_analysis": bool(project.get("analysis_json")),
    }

    # ── 优先：读已生成的 analysis_json ──
    if project.get("analysis_json"):
        try:
            cached = json.loads(project["analysis_json"])
            return {
                "ok": True,
                "project": project_info,
                "related_news": cached.get("news_parsed") or [],
                "theme_heat": cached.get("theme_heat"),
                "macro_analysis": cached.get("macro"),
                "industry_analysis": cached.get("industry"),
                "analysis_meta": {
                    "source": cached.get("source"),
                    "generated_at": cached.get("generated_at"),
                    "tags_used": cached.get("tags_used", []),
                },
            }
        except Exception as e:
            logger.warning(f"解析 analysis_json 失败 project={project_id}: {e}")

    # ── Fallback：旧逻辑，从 tag_group_research 模糊匹配 ──
    stock_codes = _get_project_stock_codes(project_id, project.get("project_type", "custom"))
    related_news = _get_related_news(stock_codes)
    theme_heat = _get_theme_heat(stock_codes)
    macro_analysis = _get_macro_analysis(stock_codes)
    industry_analysis = _get_industry_analysis(stock_codes)

    return {
        "ok": True,
        "project": project_info,
        "related_news": related_news,
        "theme_heat": theme_heat,
        "macro_analysis": macro_analysis,
        "industry_analysis": industry_analysis,
        "analysis_meta": None,
    }


# ── 项目分析生成 API ──────────────────────────────────────────

@router.post("/api/projects/{project_id}/generate-analysis")
async def api_generate_analysis(project_id: int):
    """触发异步分析生成任务，立即返回 task_id，前端轮询进度"""
    project = execute_query("SELECT id FROM watchlist_lists WHERE id=%s", [project_id])
    if not project:
        return JSONResponse({"ok": False, "error": "项目不存在"}, status_code=404)

    # 检查是否已有运行中的任务
    with _analysis_lock:
        for task in _analysis_tasks.values():
            if task["project_id"] == project_id and task["status"] == "running":
                return {"ok": True, "task_id": task["id"], "status": "running",
                        "message": "分析任务已在运行中"}

    task_id = uuid.uuid4().hex[:8]
    with _analysis_lock:
        _analysis_tasks[task_id] = {
            "id": task_id, "project_id": project_id,
            "status": "running", "progress": 0,
            "message": "准备中...", "created_at": time.time(),
        }

    def _run():
        def cb(msg, pct=None):
            with _analysis_lock:
                if task_id in _analysis_tasks:
                    _analysis_tasks[task_id]["message"] = msg
                    if pct is not None:
                        _analysis_tasks[task_id]["progress"] = pct

        try:
            from portfolio.project_research import generate_project_analysis
            result = generate_project_analysis(project_id, progress_callback=cb)
            with _analysis_lock:
                _analysis_tasks[task_id].update({
                    "status": "done",
                    "progress": 100,
                    "message": "分析完成",
                    "ok": result.get("ok", False),
                    "error": result.get("error"),
                })
        except Exception as e:
            logger.error(f"项目分析任务失败 project={project_id}: {e}")
            with _analysis_lock:
                _analysis_tasks[task_id].update({
                    "status": "error",
                    "message": f"生成失败: {str(e)[:200]}",
                })

    threading.Thread(target=_run, daemon=True).start()
    return {"ok": True, "task_id": task_id, "status": "running", "message": "分析任务已启动"}


@router.get("/api/projects/{project_id}/generate-analysis/poll")
def api_poll_analysis(project_id: int, task_id: str = None):
    """轮询分析任务进度"""
    with _analysis_lock:
        # 找到对应任务
        task = None
        if task_id and task_id in _analysis_tasks:
            task = _analysis_tasks[task_id]
        else:
            # 没有 task_id 时找最新的
            for t in sorted(_analysis_tasks.values(), key=lambda x: x["created_at"], reverse=True):
                if t["project_id"] == project_id:
                    task = t
                    break

    if not task:
        return {"status": "no_task"}

    return {
        "task_id": task["id"],
        "status": task["status"],
        "progress": task["progress"],
        "message": task["message"],
        "ok": task.get("ok"),
        "error": task.get("error"),
    }


# ── 合并两个项目 ──────────────────────────────────────────────

@router.post("/api/projects/merge")
async def api_merge_projects(request: Request):
    """合并两个高度相关项目：股票篮子取并集，名称+逻辑重新生成，原两项目可选保留/删除"""
    data = await request.json()
    src_id = data.get("source_id")
    dst_id = data.get("target_id")
    keep_source = data.get("keep_source", False)   # 是否保留来源项目
    new_name = (data.get("new_name") or "").strip()

    if not src_id or not dst_id or src_id == dst_id:
        return JSONResponse({"ok": False, "error": "需要两个不同的项目 ID"}, status_code=400)

    from utils.db_utils import execute_insert as _insert

    # 读取两个项目
    src = execute_query("SELECT * FROM watchlist_lists WHERE id=%s", [src_id])
    dst = execute_query("SELECT * FROM watchlist_lists WHERE id=%s", [dst_id])
    if not src or not dst:
        return JSONResponse({"ok": False, "error": "项目不存在"}, status_code=404)
    src, dst = dict(src[0]), dict(dst[0])

    # 读取两边股票
    src_stocks = execute_query(
        "SELECT stock_code, stock_name, ai_reason FROM watchlist_list_stocks WHERE list_id=%s AND status='active'",
        [src_id],
    ) or []
    dst_stocks = execute_query(
        "SELECT stock_code FROM watchlist_list_stocks WHERE list_id=%s AND status='active'",
        [dst_id],
    ) or []
    dst_codes = {r["stock_code"] for r in dst_stocks}

    # 将 src 独有股票并入 dst
    added = 0
    for s in src_stocks:
        if s["stock_code"] not in dst_codes:
            try:
                _insert(
                    """INSERT INTO watchlist_list_stocks (list_id, stock_code, stock_name, source, ai_reason)
                       VALUES (%s, %s, %s, 'merge', %s)
                       ON DUPLICATE KEY UPDATE status='active'""",
                    [dst_id, s["stock_code"], s.get("stock_name", ""), s.get("ai_reason", "")],
                )
                added += 1
            except Exception:
                pass

    # 合并投资逻辑：拼接两段，供后续分析生成时精炼
    src_logic = (src.get("investment_logic") or src.get("background_info") or "").strip()
    dst_logic = (dst.get("investment_logic") or dst.get("background_info") or "").strip()
    merged_logic = " / ".join(filter(None, [dst_logic, src_logic]))

    # 更新目标项目名称和逻辑
    if new_name:
        _insert(
            "UPDATE watchlist_lists SET list_name=%s, investment_logic=%s WHERE id=%s",
            [new_name, merged_logic, dst_id],
        )
    else:
        _insert(
            "UPDATE watchlist_lists SET investment_logic=%s WHERE id=%s",
            [merged_logic, dst_id],
        )

    # 处理来源项目
    if not keep_source:
        _insert("DELETE FROM watchlist_lists WHERE id=%s", [src_id])

    return {
        "ok": True,
        "target_id": dst_id,
        "stocks_added": added,
        "message": f"已将「{src['list_name']}」合并到「{dst['list_name']}」，新增 {added} 只股票",
    }


# ── Hotspot 一键导入到 Portfolio Lab ─────────────────────────

@router.post("/api/projects/import-from-hotspot")
async def api_import_from_hotspot(request: Request):
    """从热点标签组一键创建项目，自动关联 source_group_id 并触发分析复制"""
    data = await request.json()
    group_id = data.get("group_id")
    if not group_id:
        return JSONResponse({"ok": False, "error": "缺少 group_id"}, status_code=400)

    # 读取标签组信息
    group = execute_query("SELECT * FROM tag_groups WHERE id=%s", [group_id])
    if not group:
        return JSONResponse({"ok": False, "error": "标签组不存在"}, status_code=404)
    group = dict(group[0])

    project_name = data.get("project_name") or group.get("group_name") or f"热点组 #{group_id}"
    investment_logic = data.get("investment_logic") or group.get("group_logic") or ""

    from utils.db_utils import execute_insert
    project_id = execute_insert(
        """INSERT INTO watchlist_lists
           (list_type, list_name, investment_logic, project_type, source_group_id)
           VALUES ('theme', %s, %s, 'theme', %s)""",
        [project_name, investment_logic, group_id],
    )

    # 立即触发场景A分析复制（同步，因为只是 DB 读取+写入，不调 AI）
    try:
        from portfolio.project_research import generate_project_analysis
        generate_project_analysis(project_id)
    except Exception as e:
        logger.warning(f"热点导入分析复制失败: {e}")

    return {"ok": True, "project_id": project_id, "project_name": project_name}


# ── 细分行业资金流 API ────────────────────────────────────────

@router.get("/api/projects/{project_id}/hotspot-reports")
def api_hotspot_reports(project_id: int):
    """返回与当前篮子股票相关的热点研究报告（折叠标题列表）。
    匹配逻辑：研究报告的 industry_json.benefiting_industries[].name
    与篮子股票的 stock_info.industry_l2 有交集。
    """
    # 获取篮子股票的细分行业
    stocks = execute_query(
        """SELECT wls.stock_code, si.industry_l2
           FROM watchlist_list_stocks wls
           JOIN stock_info si ON wls.stock_code = si.stock_code
           WHERE wls.list_id=%s AND wls.status='active'
             AND si.industry_l2 IS NOT NULL AND si.industry_l2 != ''""",
        [project_id],
    ) or []
    if not stocks:
        return {"ok": True, "reports": []}

    basket_industries = {s["industry_l2"] for s in stocks}
    basket_codes = {s["stock_code"] for s in stocks}

    # 拉取所有有研究的标签组
    researches = execute_query(
        """SELECT tgr.id, tgr.group_id, tg.group_name, tg.group_logic,
                  tgr.macro_json, tgr.industry_json, tgr.logic_synthesis_json,
                  tgr.news_parsed_json, tgr.industry_heat_json,
                  tgr.top10_stocks_json, tgr.research_date
           FROM tag_group_research tgr
           JOIN tag_groups tg ON tgr.group_id = tg.id
           WHERE tgr.industry_json IS NOT NULL AND tgr.industry_json != ''
           ORDER BY tgr.research_date DESC""",
        [],
    ) or []

    matched_reports = []
    seen_groups = set()
    for r in researches:
        if r["group_id"] in seen_groups:
            continue
        try:
            ind_json = json.loads(r["industry_json"])
        except Exception:
            continue
        ind_list = ind_json.get("benefiting_industries") or ind_json.get("industries") or []
        report_industries = {(ind.get("name") or "").strip() for ind in ind_list}

        # 匹配：报告行业名包含篮子细分行业名，或反之
        hit = False
        for ri in report_industries:
            for bi in basket_industries:
                if bi in ri or ri in bi:
                    hit = True
                    break
            if hit:
                break

        if hit:
            seen_groups.add(r["group_id"])
            # 解析 top10 推荐个股 → 自动加入篮子的候选
            auto_add_stocks = []
            try:
                top10 = json.loads(r.get("top10_stocks_json") or "[]")
                for s in (top10 if isinstance(top10, list) else []):
                    code = s.get("stock_code", "")
                    if code and code not in basket_codes:
                        auto_add_stocks.append({
                            "stock_code": code,
                            "stock_name": s.get("stock_name", ""),
                        })
            except Exception:
                pass

            # 构建报告摘要（不含推荐个股）
            report = {
                "research_id": r["id"],
                "group_id": r["group_id"],
                "group_name": r["group_name"],
                "group_logic": r.get("group_logic") or "",
                "research_date": str(r["research_date"]) if r.get("research_date") else "",
                "auto_add_stocks": auto_add_stocks,
            }
            # 附带各模块 JSON（前端展示用）
            for field in ["macro_json", "industry_json", "logic_synthesis_json", "news_parsed_json", "industry_heat_json"]:
                try:
                    report[field] = json.loads(r.get(field) or "null")
                except Exception:
                    report[field] = None

            matched_reports.append(report)

    return {"ok": True, "reports": matched_reports[:10]}


# ── 向量模型懒加载缓存 ─────────────────────────────────────────
_embed_model = None
_embed_model_lock = threading.Lock()


def _get_embed_model():
    global _embed_model
    if _embed_model is None:
        with _embed_model_lock:
            if _embed_model is None:
                try:
                    from sentence_transformers import SentenceTransformer
                    _embed_model = SentenceTransformer("BAAI/bge-base-zh-v1.5")
                    logger.info("向量模型 bge-base-zh-v1.5 加载完成")
                except Exception as e:
                    logger.warning(f"向量模型加载失败: {e}")
    return _embed_model


@router.get("/api/projects/{project_id}/strategy-tags")
def api_strategy_tags(project_id: int):
    """聚类篮子股票到95种预置策略标签。
    匹配方式：sentence_transformers bge-base-zh + 余弦相似度，降级到关键词匹配。
    """
    from config.stock_selection_presets import PRESET_RULES, RULE_CATEGORIES
    import numpy as np

    # 获取篮子股票
    stocks = execute_query(
        """SELECT wls.stock_code, wls.stock_name,
                  si.industry_l1, si.industry_l2, si.company_intro, si.main_business
           FROM watchlist_list_stocks wls
           LEFT JOIN stock_info si ON wls.stock_code = si.stock_code
           WHERE wls.list_id=%s AND wls.status='active'""",
        [project_id],
    ) or []
    if not stocks:
        return {"ok": True, "tags": [], "total": 0}

    codes = [s["stock_code"] for s in stocks]
    total = len(codes)
    codes_ph = ",".join(["%s"] * len(codes))

    # 拉取 content_summaries（优先）及 cleaned_items 摘要
    cs_rows = execute_query(
        f"""SELECT sm.stock_code, cs.summary
            FROM content_summaries cs
            JOIN stock_mentions sm ON cs.extracted_text_id = sm.extracted_text_id
            WHERE sm.stock_code IN ({codes_ph})
              AND cs.created_at >= DATE_SUB(NOW(), INTERVAL 90 DAY)
              AND cs.summary IS NOT NULL AND cs.summary != ''
            ORDER BY cs.created_at DESC
            LIMIT 300""",
        codes,
    ) or []

    news_rows = execute_query(
        f"""SELECT ic.stock_code, ci.summary
            FROM cleaned_items ci
            JOIN item_companies ic ON ci.id = ic.cleaned_item_id
            WHERE ic.stock_code IN ({codes_ph})
              AND ci.cleaned_at >= DATE_SUB(NOW(), INTERVAL 180 DAY)
              AND ci.summary IS NOT NULL AND ci.summary != ''
            ORDER BY ci.cleaned_at DESC
            LIMIT 200""",
        codes,
    ) or []

    # 建立 stock_code → 文本库
    stock_text: dict = {}
    for s in stocks:
        code = s["stock_code"]
        parts = [
            s.get("stock_name") or "",
            s.get("industry_l1") or "",
            s.get("industry_l2") or "",
            s.get("company_intro") or "",
            s.get("main_business") or "",
        ]
        stock_text[code] = " ".join(p for p in parts if p)

    for r in (cs_rows + news_rows):
        code = r["stock_code"]
        if code in stock_text:
            # 追加摘要，限制总长
            existing = stock_text[code]
            addition = (r.get("summary") or "").strip()
            if addition and len(existing) < 800:
                stock_text[code] = existing + " " + addition[:200]

    # ── 向量匹配（主路径） ─────────────────────────────────────
    model = _get_embed_model()
    if model is not None:
        try:
            from sklearn.metrics.pairwise import cosine_similarity

            # 股票文本向量（去重空文本）
            stock_codes_order = [c for c in codes if stock_text.get(c, "").strip()]
            if not stock_codes_order:
                stock_codes_order = codes
            stock_texts_list = [stock_text.get(c, c) for c in stock_codes_order]
            stock_vecs = model.encode(stock_texts_list, batch_size=32, show_progress_bar=False)

            # 规则文本向量
            rule_texts = [
                f"{r['rule_name']} {r.get('definition', '')}"[:300]
                for r in PRESET_RULES
            ]
            rule_vecs = model.encode(rule_texts, batch_size=32, show_progress_bar=False)

            # 余弦相似度矩阵：shape (n_rules, n_stocks)
            sim_matrix = cosine_similarity(rule_vecs, stock_vecs)

            THRESHOLD = 0.60
            tags = []
            for i, rule in enumerate(PRESET_RULES):
                cat = rule.get("category", "")
                cat_meta = RULE_CATEGORIES.get(cat, {})
                matched_stocks = []
                for j, code in enumerate(stock_codes_order):
                    sim = float(sim_matrix[i, j])
                    if sim >= THRESHOLD:
                        s = next((x for x in stocks if x["stock_code"] == code), None)
                        matched_stocks.append({
                            "stock_code": code,
                            "stock_name": (s.get("stock_name") if s else None) or code,
                            "score": round(sim, 3),
                        })

                if matched_stocks:
                    matched_stocks.sort(key=lambda x: x["score"], reverse=True)
                    tags.append({
                        "strategy_id": i,
                        "tag_name": rule["rule_name"],
                        "category": cat,
                        "category_label": cat_meta.get("label", ""),
                        "category_color": cat_meta.get("color", "slate"),
                        "match_count": len(matched_stocks),
                        "total": total,
                        "stocks": matched_stocks,
                        "is_contrarian": rule.get("is_contrarian", False),
                    })

            tags.sort(key=lambda t: t["match_count"], reverse=True)
            return {"ok": True, "tags": tags[:30], "total": total, "method": "vector"}

        except Exception as e:
            logger.warning(f"向量策略匹配失败，降级到关键词: {e}")

    # ── 降级：关键词匹配 ───────────────────────────────────────
    tags = []
    for i, rule in enumerate(PRESET_RULES):
        rule_name = rule["rule_name"]
        definition = (rule.get("definition") or "")
        cat = rule.get("category", "")
        cat_meta = RULE_CATEGORIES.get(cat, {})

        keywords = _extract_rule_keywords(rule_name, definition)
        if not keywords:
            continue

        matched_stocks = []
        for s in stocks:
            code = s["stock_code"]
            text = stock_text.get(code, "").lower()
            hit_kws = [kw for kw in keywords if kw in text]
            if hit_kws:
                matched_stocks.append({
                    "stock_code": code,
                    "stock_name": s.get("stock_name") or code,
                    "score": len(hit_kws) / len(keywords),
                })

        if matched_stocks:
            tags.append({
                "strategy_id": i,
                "tag_name": rule_name,
                "category": cat,
                "category_label": cat_meta.get("label", ""),
                "category_color": cat_meta.get("color", "slate"),
                "match_count": len(matched_stocks),
                "total": total,
                "stocks": matched_stocks,
                "is_contrarian": rule.get("is_contrarian", False),
            })

    tags.sort(key=lambda t: t["match_count"], reverse=True)
    return {"ok": True, "tags": tags[:30], "total": total, "method": "keyword"}


def _extract_rule_keywords(rule_name: str, definition: str) -> list:
    """从策略规则名和定义中提取匹配关键词（降级用）"""
    import re
    words = re.findall(r'[\u4e00-\u9fff]{2,}', rule_name)
    def_words = re.findall(r'[\u4e00-\u9fff]{2,4}', definition[:200])
    stop = {"公司", "股票", "市场", "行业", "指标", "数据", "因此", "由于", "或者",
            "根据", "进行", "以及", "通过", "同时", "具有", "包括", "属于", "相关"}
    all_words = [w for w in (words + def_words[:8]) if w not in stop and len(w) >= 2]
    seen = set()
    result = []
    for w in all_words:
        if w not in seen:
            seen.add(w)
            result.append(w)
    return result[:6]


@router.get("/api/projects/{project_id}/sector-flow")
def api_sector_flow(project_id: int):
    """返回项目细分行业近15交易日每日净流入。
    数据来源：analysis_json.industry[].name → KG industry entity →
              belongs_to_industry ← company(external_id=stock_code) →
              capital_flow 按行业分组聚合
    """
    project = execute_query("SELECT * FROM watchlist_lists WHERE id=%s", [project_id])
    if not project:
        return JSONResponse({"ok": False, "error": "项目不存在"}, status_code=404)
    project = dict(project[0])

    if not project.get("analysis_json"):
        return {"ok": True, "dates": [], "series": [], "message": "暂无行业分析，请先生成分析"}

    # ── 读 analysis_json 里的细分行业列表 ──
    try:
        cached = json.loads(project["analysis_json"])
    except Exception:
        return {"ok": True, "dates": [], "series": [], "message": "analysis_json 解析失败"}

    ind_data = cached.get("industry") or {}
    ind_list = ind_data.get("industries") or ind_data.get("benefiting_industries") or []
    if not ind_list:
        return {"ok": True, "dates": [], "series": [], "message": "行业分析中无细分行业"}

    # ── 每个行业名 → KG industry entity（前4字 LIKE） ──
    # → belongs_to_industry ← company → external_id(stock_code)
    sector_codes: dict = {}  # display_name -> [stock_code, ...]
    for ind in ind_list[:15]:
        name = (ind.get("name") or "").strip()
        if not name:
            continue
        # 取关键匹配词：优先括号前，斜杠取第一段，限4字
        keyword = name.split("（")[0].split("(")[0].split("/")[0].strip()[:6]
        if len(keyword) < 2:
            continue

        # 在 KG 里找匹配的 industry entity（用前4字 LIKE）
        kg_inds = execute_query(
            "SELECT id FROM kg_entities WHERE entity_type='industry' AND entity_name LIKE %s",
            [f"%{keyword[:4]}%"],
        ) or []

        codes = set()
        for kg_row in kg_inds:
            kg_id = kg_row["id"]
            comp_rows = execute_query(
                """SELECT ke.external_id
                   FROM kg_relationships kr
                   JOIN kg_entities ke ON kr.source_entity_id = ke.id
                   WHERE kr.target_entity_id = %s
                     AND kr.relation_type = 'belongs_to_industry'
                     AND ke.entity_type = 'company'
                     AND ke.external_id IS NOT NULL AND ke.external_id != ''""",
                [kg_id],
            ) or []
            for r in comp_rows:
                codes.add(r["external_id"])

        if codes:
            sector_codes[name] = list(codes)

    if not sector_codes:
        return {"ok": True, "dates": [], "series": [], "message": "KG 中未找到关联股票"}

    # ── 取最近 15 个有效交易日 ──
    date_rows = execute_query(
        "SELECT DISTINCT trade_date FROM capital_flow "
        "WHERE LENGTH(stock_code)=6 AND main_net_inflow != 0 "
        "ORDER BY trade_date DESC LIMIT 15",
        [],
    ) or []
    if not date_rows:
        return {"ok": True, "dates": [], "series": [], "message": "暂无资金流数据"}

    dates = sorted([r["trade_date"] for r in date_rows])
    dates_ph = ",".join(["%s"] * len(dates))

    # ── 每个细分行业按日聚合 capital_flow + 合计市值 ──
    series = []
    for sector_name, codes in sector_codes.items():
        codes_ph = ",".join(["%s"] * len(codes))
        rows = execute_query(
            f"""SELECT trade_date, ROUND(SUM(main_net_inflow) / 10000) AS net_wan
                FROM capital_flow
                WHERE stock_code IN ({codes_ph}) AND trade_date IN ({dates_ph})
                GROUP BY trade_date
                ORDER BY trade_date""",
            codes + dates,
        ) or []

        day_map = {r["trade_date"]: int(r["net_wan"] or 0) for r in rows}
        values = [day_map.get(d, 0) for d in dates]

        # 过滤全程接近 0 的（峰值绝对值 < 500万）
        if not values or max(abs(v) for v in values) < 500:
            continue

        # 合计市值：用最新日 amount / turnover_rate（单位：亿元）
        mktcap_row = execute_query(
            f"""SELECT ROUND(SUM(sd.amount / NULLIF(sd.turnover_rate, 0) / 1e8)) AS total_cap
                FROM stock_daily sd
                JOIN (SELECT stock_code, MAX(trade_date) AS mx FROM stock_daily
                      WHERE stock_code IN ({codes_ph}) GROUP BY stock_code) t
                  ON sd.stock_code = t.stock_code AND sd.trade_date = t.mx
                WHERE sd.stock_code IN ({codes_ph})
                  AND sd.turnover_rate > 0 AND sd.amount > 0""",
            codes + codes,
        ) or []
        total_cap = int(mktcap_row[0]["total_cap"] or 0) if mktcap_row else 0

        series.append({
            "name": sector_name,
            "values": values,
            "stock_count": len(codes),
            "market_cap": total_cap,  # 亿元
        })

    # 按 15 日总净流入绝对值降序
    series.sort(key=lambda s: abs(sum(s["values"])), reverse=True)

    return {"ok": True, "dates": dates, "series": series}


# ── 以下保留旧 fallback 函数（context API fallback 路径用）────

def _get_project_stock_codes(project_id: int, project_type: str) -> list:
    if project_type == 'portfolio' and project_id == 1:
        rows = execute_query("SELECT stock_code FROM holding_positions WHERE status='open'", [])
    else:
        rows = execute_query(
            "SELECT stock_code FROM watchlist_list_stocks WHERE list_id=%s AND status='active'",
            [project_id],
        )
    return [r["stock_code"] for r in (rows or [])]


def _get_latest_research_for_stocks(stock_codes: list):
    if not stock_codes:
        return None
    codes_str = ",".join(["%s"] * len(stock_codes))
    names = execute_query(
        f"SELECT stock_name FROM stock_info WHERE stock_code IN ({codes_str}) AND stock_name IS NOT NULL LIMIT 5",
        stock_codes,
    )
    if not names:
        return None
    name = names[0]["stock_name"]
    row = execute_query(
        """SELECT tgr.*, tg.group_name FROM tag_group_research tgr
           JOIN tag_groups tg ON tgr.group_id = tg.id
           WHERE tgr.top10_stocks_json LIKE %s
           ORDER BY tgr.research_date DESC LIMIT 1""",
        [f"%{name}%"],
    )
    if row:
        return dict(row[0])
    row = execute_query(
        """SELECT tgr.*, tg.group_name FROM tag_group_research tgr
           JOIN tag_groups tg ON tgr.group_id = tg.id
           WHERE tgr.news_parsed_json IS NOT NULL
           ORDER BY tgr.research_date DESC LIMIT 1""",
        [],
    )
    return dict(row[0]) if row else None


def _get_related_news(stock_codes: list, limit: int = 15) -> list:
    research = _get_latest_research_for_stocks(stock_codes)
    if research and research.get("news_parsed_json"):
        try:
            parsed = json.loads(research["news_parsed_json"])
            if isinstance(parsed, list):
                return parsed[:limit]
        except Exception:
            pass
    if not stock_codes:
        return []
    codes_str = ",".join(["%s"] * len(stock_codes))
    rows = execute_query(
        f"""SELECT ci.summary, ci.event_type, ci.sentiment, ci.importance,
                   ic.stock_code, ic.stock_name
            FROM cleaned_items ci JOIN item_companies ic ON ci.id = ic.cleaned_item_id
            WHERE ic.stock_code IN ({codes_str})
              AND ci.cleaned_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            ORDER BY ci.importance DESC, ci.cleaned_at DESC LIMIT %s""",
        stock_codes + [limit],
    )
    return [
        {"fact": r["summary"], "importance": r["importance"] or 0,
         "direction": "positive" if r["sentiment"] == "positive"
                      else "negative" if r["sentiment"] == "negative" else "neutral"}
        for r in (rows or [])
    ]


def _get_theme_heat(stock_codes: list):
    research = _get_latest_research_for_stocks(stock_codes)
    if research and research.get("theme_heat_json"):
        try:
            heat = json.loads(research["theme_heat_json"])
            if isinstance(heat, dict) and heat.get("dates"):
                return heat
        except Exception:
            pass
    return None


def _get_macro_analysis(stock_codes=None):
    research = _get_latest_research_for_stocks(stock_codes or []) if stock_codes else None
    if not research:
        row = execute_query(
            """SELECT tgr.macro_json, tgr.macro_report FROM tag_group_research tgr
               WHERE tgr.macro_json IS NOT NULL AND tgr.macro_json != ''
               ORDER BY tgr.research_date DESC LIMIT 1""", [],
        )
        research = dict(row[0]) if row else None
    if not research:
        return None
    if research.get("macro_json"):
        try:
            mj = json.loads(research["macro_json"])
            if isinstance(mj, dict):
                return mj
        except Exception:
            pass
    if research.get("macro_report"):
        return {"summary": research["macro_report"], "factors": []}
    return None


def _get_industry_analysis(stock_codes: list):
    research = _get_latest_research_for_stocks(stock_codes)
    if research and research.get("industry_json"):
        try:
            ij = json.loads(research["industry_json"])
            if isinstance(ij, dict):
                return ij
        except Exception:
            pass
    if research and research.get("industry_report"):
        return {"summary": research["industry_report"], "industries": []}
    return None

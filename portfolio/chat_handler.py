"""Portfolio实验室 — 项目聊天处理

异步模式：Web 端存消息(pending) → chat_worker 处理 → 前端轮询
"""
import json
import logging
import re
from datetime import datetime

from utils.db_utils import execute_query, execute_insert

logger = logging.getLogger(__name__)

# ── 聊天消息 CRUD ──────────────────────────────────────────────

def get_chat_history(project_id: int, limit: int = 50) -> list:
    rows = execute_query(
        "SELECT id, role, content, metadata_json, created_at FROM project_chat_messages "
        "WHERE project_id=%s AND (metadata_json NOT LIKE '%%pending%%' OR metadata_json IS NULL) "
        "ORDER BY created_at ASC LIMIT %s",
        [project_id, limit],
    )
    result = []
    for r in (rows or []):
        msg = {"id": r["id"], "role": r["role"], "content": r["content"],
               "created_at": str(r["created_at"]), "recommendations": []}
        if r.get("metadata_json"):
            try:
                meta = json.loads(r["metadata_json"])
                msg["recommendations"] = meta.get("recommendations") or []
            except Exception:
                pass
        result.append(msg)
    return result


def save_chat_message(project_id: int, role: str, content: str, metadata: dict = None) -> int:
    meta_json = json.dumps(metadata, ensure_ascii=False) if metadata else None
    return execute_insert(
        "INSERT INTO project_chat_messages (project_id, role, content, metadata_json) "
        "VALUES (%s, %s, %s, %s)",
        [project_id, role, content, meta_json],
    )


def submit_chat_message(project_id: int, user_message: str, strategy_ids: list = None) -> dict:
    """Web 端调用：存用户消息 + pending 占位"""
    user_msg_id = save_chat_message(project_id, "user", user_message)
    pending_id = save_chat_message(
        project_id, "assistant", "思考中...",
        metadata={"status": "pending", "project_id": project_id},
    )
    # 记录关联策略
    if strategy_ids:
        for sid in strategy_ids:
            try:
                execute_insert(
                    "INSERT INTO project_chat_strategies (message_id, strategy_id) VALUES (%s, %s)",
                    [user_msg_id, sid],
                )
            except Exception:
                pass
    return {"ok": True, "pending_id": pending_id, "status": "pending"}


def get_pending_reply(project_id: int) -> dict:
    """前端轮询：检查最新 assistant 消息状态"""
    rows = execute_query(
        "SELECT id, content, metadata_json FROM project_chat_messages "
        "WHERE project_id=%s AND role='assistant' ORDER BY id DESC LIMIT 1",
        [project_id],
    )
    if not rows:
        return {"status": "no_messages"}
    r = rows[0]
    metadata = {}
    if r.get("metadata_json"):
        try:
            metadata = json.loads(r["metadata_json"])
        except Exception:
            pass
    if metadata.get("status") == "pending":
        return {"status": "pending"}
    return {
        "status": "done",
        "reply": r["content"],
        "message_id": r["id"],
        "recommendations": metadata.get("recommendations") or [],
    }


# ── Worker 端：处理 pending 消息 ──────────────────────────────

CHAT_SYSTEM_PROMPT = """你是一个专业的A股选股助手，服务于Portfolio实验室。

## 当前项目
名称：{project_name}
投资逻辑：{investment_logic}

## 篮子股票
{basket_stocks}

{strategy_context}

## 最新相关信息（RAG检索）
{rag_context}

## 回复规范（严格遵守）

**原则：直接给结论，不废话。**

### 情况一：推荐新股票
直接给推荐结果，在回复末尾附加：
```推荐股票
[{{"stock_code": "代码", "stock_name": "名称", "reason": "一句话理由，含核心数据或新闻依据"}}]
```

### 情况二：分析问题
直接输出结论：

**结论**：[一句话核心判断]

**支撑**：
- [数据点/新闻依据1]
- [数据点2]

### 情况三：信息不足
直接说明：

**缺失信息**：[具体缺什么]
**可以做的**：[现有信息下能做什么]

### 禁止
- 禁止铺垫、过渡语、感谢语
- 禁止重复用户的问题
- 禁止"根据您的需求"等套话
- 禁止在没有数据支撑时编造数字"""


def process_pending_messages():
    """Worker：处理所有 pending 的 project_chat_messages"""
    from utils.model_router import call_model as _cm

    pending = execute_query(
        "SELECT id, project_id, metadata_json FROM project_chat_messages "
        "WHERE role='assistant' AND metadata_json LIKE '%%pending%%' "
        "ORDER BY id ASC",
        [],
    )
    if not pending:
        return 0

    processed = 0
    for row in pending:
        msg_id = row["id"]
        project_id = row["project_id"]
        try:
            result = _process_single_chat(project_id, _cm)
            meta_json = json.dumps({"recommendations": result.get("recommendations", [])}, ensure_ascii=False) if result.get("recommendations") else None
            execute_insert(
                "UPDATE project_chat_messages SET content=%s, metadata_json=%s WHERE id=%s",
                [result["reply"], meta_json, msg_id],
            )
            processed += 1
        except Exception as e:
            logger.error(f"处理 project chat pending #{msg_id} 失败: {e}")
            execute_insert(
                "UPDATE project_chat_messages SET content=%s, metadata_json=NULL WHERE id=%s",
                [f"处理失败: {e}", msg_id],
            )
            processed += 1
    return processed


def _process_single_chat(project_id: int, call_model_fn) -> dict:
    """处理单个项目的最新用户消息 — RAG 三场景驱动"""
    # 加载项目信息
    project = execute_query(
        "SELECT * FROM watchlist_lists WHERE id=%s", [project_id]
    )
    if not project:
        return {"reply": "项目不存在", "recommendations": []}
    project = dict(project[0])

    project_name = project.get("list_name", "")
    investment_logic = (project.get("investment_logic") or project.get("background_info") or "").strip()

    # 获取篮子股票
    basket_stocks = _get_basket_stocks(project_id, project.get("project_type", "custom"))

    # 获取最新用户消息（含拖入上下文）
    last_user_msg = execute_query(
        "SELECT content FROM project_chat_messages "
        "WHERE project_id=%s AND role='user' "
        "ORDER BY id DESC LIMIT 1",
        [project_id],
    )
    message = last_user_msg[0]["content"] if last_user_msg else ""
    if not message:
        return {"reply": "未找到用户消息", "recommendations": []}

    # 识别场景
    scenario = _detect_scenario(message)

    # RAG 检索：根据场景决定检索查询
    rag_context = _rag_retrieve(message, scenario, basket_stocks, investment_logic)

    # 策略上下文
    strategy_context = _get_strategy_context(project_id)

    # 如果是「空篮推荐」场景且未提供策略，直接返回策略选择提示
    if scenario == "empty_recommend" and not strategy_context and not _chat_has_prior_exchange(project_id):
        reply = _build_strategy_prompt()
        return {"reply": reply, "recommendations": []}

    # 获取对话历史
    history_rows = execute_query(
        "SELECT role, content FROM project_chat_messages "
        "WHERE project_id=%s AND role IN ('user','assistant') "
        "AND (metadata_json NOT LIKE '%%pending%%' OR metadata_json IS NULL) "
        "ORDER BY id DESC LIMIT 16",
        [project_id],
    )
    history = [{"role": r["role"], "content": r["content"]} for r in reversed(history_rows or [])]
    # 去掉最后一条（当前用户消息，避免重复）
    if history and history[-1]["role"] == "user" and history[-1]["content"] == message:
        history = history[:-1]

    basket_summary = _format_basket_stocks(basket_stocks)

    system_prompt = CHAT_SYSTEM_PROMPT.format(
        project_name=project_name,
        investment_logic=investment_logic or "未设置",
        basket_stocks=basket_summary or "（篮子为空）",
        strategy_context=strategy_context or "",
        rag_context=rag_context or "（暂无检索到的相关信息）",
    )

    # 构建对话历史文本
    conversation = ""
    for msg in history[-12:]:
        role_label = "用户" if msg["role"] == "user" else "AI"
        conversation += f"\n{role_label}: {msg['content']}\n"

    reply_raw = call_model_fn("hotspot", system_prompt, conversation + f"\n用户: {message}", max_tokens=2048, timeout=90)
    clean_reply, recommendations = _parse_recommendations(reply_raw)
    return {"reply": clean_reply, "recommendations": recommendations}


def _detect_scenario(message: str) -> str:
    """识别用户意图场景
    - empty_recommend: 用户直接问推荐（篮子为空或首次对话）
    - hotspot_drag: 消息包含热点报告上下文
    - tag_drag: 消息包含策略标签
    - general: 一般问题
    """
    msg_lower = message.lower()
    if "【热点报告" in message or "【热点研究" in message:
        return "hotspot_drag"
    if "【策略" in message or "策略标签" in message:
        return "tag_drag"
    # 热点报告 + 策略标签组合
    if "【" in message and "】" in message:
        return "context_drag"
    # 空篮推荐意图词
    recommend_keywords = ["推荐", "选股", "什么股票", "哪些股", "买什么", "加什么"]
    if any(kw in msg_lower for kw in recommend_keywords):
        return "empty_recommend"
    return "general"


def _rag_retrieve(message: str, scenario: str, basket_stocks: list, investment_logic: str) -> str:
    """根据场景做 hybrid_search RAG 检索"""
    try:
        from retrieval.hybrid import hybrid_search

        # 提取核心查询词
        if scenario in ("hotspot_drag", "context_drag"):
            # 从拖入内容中提取主题
            import re
            # 提取 【...】 括号内的标签
            labels = re.findall(r'【([^】]+)】', message)
            query = " ".join(labels) + " " + message[message.rfind('】')+1:].strip()[:100]
        elif scenario == "tag_drag":
            import re
            labels = re.findall(r'【([^】]+)】', message)
            query = " ".join(labels) + " 选股推荐"
        elif scenario == "empty_recommend":
            query = investment_logic or " ".join(
                [s.get("stock_name", "") for s in basket_stocks[:5]]
            ) or "A股投资推荐"
        else:
            query = message[:200]

        # 加入篮子股票名称作为锚定
        basket_names = " ".join([s.get("stock_name", "") for s in basket_stocks[:5]])
        if basket_names and basket_names not in query:
            query = basket_names[:60] + " " + query

        hr = hybrid_search(query.strip()[:300], top_k=8)
        if hr and hr.merged_context:
            return hr.merged_context[:2000]
    except Exception as e:
        logger.debug(f"Portfolio RAG 检索失败（降级）: {e}")

    # Fallback：直接查 stock_mentions
    return _fallback_stock_mentions(basket_stocks)


def _fallback_stock_mentions(basket_stocks: list) -> str:
    """RAG 降级：从 stock_mentions 拉近期主题"""
    if not basket_stocks:
        return ""
    codes = [s["stock_code"] for s in basket_stocks[:10]]
    codes_ph = ",".join(["%s"] * len(codes))
    rows = execute_query(
        f"""SELECT stock_code, related_themes
            FROM stock_mentions
            WHERE stock_code IN ({codes_ph})
              AND mention_time >= DATE_SUB(NOW(), INTERVAL 14 DAY)
            ORDER BY mention_time DESC LIMIT 20""",
        codes,
    )
    if not rows:
        return ""
    lines = []
    for r in rows:
        themes = (r.get("related_themes") or "").strip()
        if themes:
            lines.append(f"{r['stock_code']}: {themes}")
    return "\n".join(lines[:15])


def _chat_has_prior_exchange(project_id: int) -> bool:
    """检查是否已有过对话（排除 pending）"""
    rows = execute_query(
        "SELECT COUNT(*) as cnt FROM project_chat_messages "
        "WHERE project_id=%s AND role='user' "
        "AND (metadata_json NOT LIKE '%%pending%%' OR metadata_json IS NULL)",
        [project_id],
    )
    return bool(rows and (rows[0].get("cnt") or 0) > 1)


def _build_strategy_prompt() -> str:
    """当篮子为空且无策略时，引导用户选择策略偏好"""
    from config.stock_selection_presets import PRESET_RULES, RULE_CATEGORIES
    # 取每类代表性策略名
    cat_examples = {}
    for r in PRESET_RULES:
        cat = r.get("category", "other")
        if cat not in cat_examples:
            cat_examples[cat] = []
        if len(cat_examples[cat]) < 2:
            cat_examples[cat].append(r["rule_name"])

    lines = ["好的，我来帮你推荐股票。先了解一下你的选股偏好：\n"]
    for cat, meta in RULE_CATEGORIES.items():
        examples = cat_examples.get(cat, [])
        if examples:
            lines.append(f"**{meta['label']}**：{' / '.join(examples)}")
    lines.append("\n你倾向于哪类策略？或者直接描述你的选股思路也可以。")
    return "\n".join(lines)


def _get_basket_stocks(project_id: int, project_type: str) -> list:
    """获取项目篮子中的股票"""
    if project_type == 'portfolio' and project_id == 1:
        # 默认购买组合：从 holding_positions 获取
        rows = execute_query(
            """SELECT hp.stock_code, hp.stock_name, hp.buy_price, hp.quantity,
                      sd.close as price, sd.change_pct, si.industry_l1
               FROM holding_positions hp
               LEFT JOIN stock_info si ON hp.stock_code = si.stock_code
               LEFT JOIN (
                   SELECT sd1.stock_code, sd1.close, sd1.change_pct
                   FROM stock_daily sd1
                   INNER JOIN (SELECT stock_code, MAX(trade_date) as mx FROM stock_daily GROUP BY stock_code) sd2
                   ON sd1.stock_code=sd2.stock_code AND sd1.trade_date=sd2.mx
               ) sd ON hp.stock_code=sd.stock_code
               WHERE hp.status='open'
               ORDER BY hp.buy_date DESC""",
            [],
        )
        return [dict(r) for r in (rows or [])]

    # 其他项目：从 watchlist_list_stocks 获取
    rows = execute_query(
        """SELECT wls.stock_code, wls.stock_name, wls.ai_reason,
                  sd.close as price, sd.change_pct, si.industry_l1
           FROM watchlist_list_stocks wls
           LEFT JOIN stock_info si ON wls.stock_code = si.stock_code
           LEFT JOIN (
               SELECT sd1.stock_code, sd1.close, sd1.change_pct
               FROM stock_daily sd1
               INNER JOIN (SELECT stock_code, MAX(trade_date) as mx FROM stock_daily GROUP BY stock_code) sd2
               ON sd1.stock_code=sd2.stock_code AND sd1.trade_date=sd2.mx
           ) sd ON wls.stock_code=sd.stock_code
           WHERE wls.list_id=%s AND wls.status='active'
           ORDER BY wls.added_at DESC""",
        [project_id],
    )
    return [dict(r) for r in (rows or [])]


def _format_basket_stocks(stocks: list) -> str:
    if not stocks:
        return ""
    lines = []
    for s in stocks:
        parts = [s["stock_code"], s.get("stock_name", "")]
        if s.get("price"):
            parts.append(f"现价:{s['price']:.2f}")
        if s.get("change_pct") is not None:
            parts.append(f"涨跌:{s['change_pct']:+.2f}%")
        if s.get("industry_l1"):
            parts.append(f"行业:{s['industry_l1']}")
        if s.get("buy_price"):
            pnl = ((s.get("price", 0) / s["buy_price"]) - 1) * 100 if s.get("price") else 0
            parts.append(f"成本:{s['buy_price']:.2f} 盈亏:{pnl:+.1f}%")
        lines.append(" | ".join(parts))
    return "\n".join(lines)


def _get_strategy_context(project_id: int) -> str:
    """获取最近一次聊天关联的策略上下文"""
    # 找最近的用户消息关联的策略
    recent_msg = execute_query(
        "SELECT id FROM project_chat_messages WHERE project_id=%s AND role='user' ORDER BY id DESC LIMIT 1",
        [project_id],
    )
    if not recent_msg:
        return ""
    msg_id = recent_msg[0]["id"]
    strategy_rows = execute_query(
        """SELECT ist.strategy_name, ist.ai_rules_text, ist.rules_json
           FROM project_chat_strategies pcs
           JOIN investment_strategies ist ON pcs.strategy_id = ist.id
           WHERE pcs.message_id=%s""",
        [msg_id],
    )
    if not strategy_rows:
        return ""
    parts = ["选中的选股策略："]
    for s in strategy_rows:
        parts.append(f"\n策略: {s['strategy_name']}")
        if s.get("ai_rules_text"):
            parts.append(f"选股原则: {s['ai_rules_text']}")
    return "\n".join(parts)


def _assemble_data_context(stock_codes: list) -> str:
    """组装篮子股票的数据上下文"""
    if not stock_codes:
        return ""
    parts = []

    # 1. 资金流向 (5日)
    codes_str = ",".join(["%s"] * len(stock_codes))
    cf_rows = execute_query(
        f"""SELECT stock_code, SUM(main_net_inflow) as net_5d
            FROM capital_flow
            WHERE stock_code IN ({codes_str})
              AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 5 DAY)
            GROUP BY stock_code""",
        stock_codes,
    )
    if cf_rows:
        cf_text = ", ".join([f"{r['stock_code']}:{r['net_5d']/10000:.0f}万" for r in cf_rows if r.get("net_5d")])
        if cf_text:
            parts.append(f"5日主力资金流向: {cf_text}")

    # 2. 相关新闻摘要 (最近7天)
    news_rows = execute_query(
        f"""SELECT ci.summary, ic.stock_code
            FROM cleaned_items ci
            JOIN item_companies ic ON ci.id = ic.cleaned_item_id
            WHERE ic.stock_code IN ({codes_str})
              AND ci.cleaned_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            ORDER BY ci.cleaned_at DESC LIMIT 10""",
        stock_codes,
    )
    if news_rows:
        news_text = "\n".join([f"- [{r['stock_code']}] {r['summary'][:100]}" for r in news_rows if r.get("summary")])
        if news_text:
            parts.append(f"近期相关新闻:\n{news_text}")

    # 3. KG 实体关系
    kg_rows = execute_query(
        f"""SELECT DISTINCT ke.entity_name, ke.entity_type, ke.investment_logic
            FROM kg_entities ke
            WHERE ke.entity_name IN (
                SELECT si.stock_name FROM stock_info si WHERE si.stock_code IN ({codes_str})
            ) OR ke.entity_name IN ({codes_str})
            LIMIT 10""",
        stock_codes + stock_codes,
    )
    if kg_rows:
        kg_text = ", ".join([f"{r['entity_name']}({r['entity_type']})" for r in kg_rows])
        parts.append(f"知识图谱实体: {kg_text}")

    # 4. 深度研究评分
    dr_rows = execute_query(
        f"""SELECT target, overall_score, recommendation
            FROM deep_research
            WHERE target IN ({codes_str})
            ORDER BY research_date DESC LIMIT 5""",
        stock_codes,
    )
    if dr_rows:
        dr_text = ", ".join([f"{r['target']}:{r['overall_score']:.0f}分" for r in dr_rows if r.get("overall_score")])
        if dr_text:
            parts.append(f"深度研究评分: {dr_text}")

    return "\n\n".join(parts) if parts else ""


def _parse_recommendations(text: str) -> tuple:
    """解析 AI 回复中的推荐股票"""
    recommendations = []
    pattern = r'```推荐股票\s*\n(.*?)```'
    matches = re.findall(pattern, text, re.DOTALL)
    for match in matches:
        try:
            recs = json.loads(match.strip())
            if isinstance(recs, list):
                recommendations.extend(recs)
        except json.JSONDecodeError:
            logger.warning(f"解析推荐股票 JSON 失败: {match[:200]}")
    clean_text = re.sub(pattern, '', text, flags=re.DOTALL).strip()
    return clean_text, recommendations

"""热点研究 — AI 追问对话处理

架构：异步模式
- Web 端：存用户消息到 DB（status=pending），前端轮询等回复
- Worker：后台脚本 chat_worker.py 读取 pending 消息，调 Claude 生成回复写回 DB
  Worker 在本地 shell 运行，继承 Claude Code 的认证环境
"""
import json
import logging
import re
from datetime import datetime
from pathlib import Path

from utils.db_utils import execute_query, execute_insert
from utils.skill_registry import get_skill_content, SKILLS_DIR

logger = logging.getLogger(__name__)

CHAT_SYSTEM_PROMPT = """你是一位严谨的投资研究分析师，正在与用户深入讨论一个标签组的深度研究报告。

当前标签组：{group_name}
标签：{tags}
投资逻辑：{group_logic}

当前研究报告概要：
- 宏观分析：{macro_summary}
- 行业分析：{industry_summary}
- 推荐个股：{stocks_summary}
- 综合论证：{thesis_summary}

{context_section}

【你的工作原则】
1. **聚焦主题**：只回答与「{group_name}」主题相关的问题，对无关话题礼貌引回主题
2. **主动引导**：若用户没有具体问题，主动询问用户对哪个分析角度最感兴趣
3. **推理检索**：用户提出质疑时，先理解其核心关切，结合检索到的最新信息深入推理
4. **诚实纠错**：若分析中确实存在错误或遗漏，直接承认并给出修正；不要强行辩解
5. **有力反驳**：若用户的前提有误，清晰陈述你的观点，提供具体数据/事实作为支撑证据
6. **完整引用**：回复时引用报告中的具体内容作为基础，再进行扩展分析

【检索到的最新相关信息】
{retrieved_context}

【触发报告修改的条件】
如果用户的追问揭示了分析中的盲点或错误：
1. 承认问题并给出修正后的分析
2. 在回复末尾添加 JSON 标记指明需要修改的报告字段：
   ```修改报告
   {{"field": "macro_json", "action": "update", "data": {{...新的完整JSON...}}}}
   ```
   field 可选值: macro_json, industry_json, top10_stocks_json, logic_synthesis_json
   action 可选值: update（替换整个字段）, append（追加内容到现有字段）
   append 时 data 格式: {{"key": "字段路径", "value": "追加的内容或对象"}}
3. 如果这个追问具有普遍价值（不是仅针对本次分析的），在回复末尾添加：
   ```批判性思维
   {{"check_point": "...", "reason": "..."}}
   ```

【JSON 格式参考】
- macro_json: {{"summary": "...", "macro_supporting": "...", "factors": [...]}}
- industry_json: {{"summary": "...", "industry_supporting": "...", "benefiting_industries": [...]}}
- logic_synthesis_json: {{"news_digest": [...], "macro_impact": "...", "industry_impact": "...", "thesis_summary": "...", "investment_opportunity_points": [...]}}

注意：只有真正需要修改时才添加修改报告标记；回复使用中文"""


def get_chat_history(group_id: int) -> list:
    """获取标签组的聊天历史（只返回已完成的消息）"""
    rows = execute_query(
        "SELECT id, role, content, metadata_json, created_at FROM group_chat_messages "
        "WHERE group_id=? AND (metadata_json NOT LIKE '%\"status\":\"pending\"%' OR metadata_json IS NULL) "
        "ORDER BY created_at ASC",
        [group_id],
    )
    result = []
    for r in rows:
        msg = {
            "id": r["id"],
            "role": r["role"],
            "content": r["content"],
            "created_at": str(r["created_at"]),
        }
        if r.get("metadata_json"):
            try:
                msg["metadata"] = json.loads(r["metadata_json"])
            except Exception:
                pass
        result.append(msg)
    return result


def save_chat_message(group_id: int, role: str, content: str, metadata: dict = None) -> int:
    """保存聊天消息"""
    meta_json = json.dumps(metadata, ensure_ascii=False) if metadata else None
    return execute_insert(
        "INSERT INTO group_chat_messages (group_id, role, content, metadata_json) "
        "VALUES (?, ?, ?, ?)",
        [group_id, role, content, meta_json],
    )


def submit_chat_message(group_id: int, user_message: str) -> dict:
    """Web 端调用：存用户消息，标记为 pending 等待 worker 处理"""
    # 保存用户消息
    save_chat_message(group_id, "user", user_message)
    # 插入一条 pending 的 assistant 占位
    pending_id = save_chat_message(
        group_id, "assistant", "思考中...",
        metadata={"status": "pending", "group_id": group_id},
    )
    return {"ok": True, "pending_id": pending_id, "status": "pending"}


def get_pending_reply(group_id: int) -> dict:
    """前端轮询：检查是否有 pending 回复已完成"""
    rows = execute_query(
        "SELECT id, content, metadata_json FROM group_chat_messages "
        "WHERE group_id=? AND role='assistant' ORDER BY id DESC LIMIT 1",
        [group_id],
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
        "modified_fields": metadata.get("modified_fields", []),
    }


def ensure_chat_greeting(group_id: int) -> dict:
    """确保聊天面板有主动问候语。若无历史消息，插入 AI 主动问候并返回它。"""
    existing = execute_query(
        "SELECT id FROM group_chat_messages WHERE group_id=? LIMIT 1",
        [group_id],
    )
    if existing:
        return {"greeted": False}

    # 拉取标签组基本信息构造问候语
    group = execute_query("SELECT group_name, group_logic FROM tag_groups WHERE id=?", [group_id])
    if group:
        gname = group[0].get("group_name", "该主题")
        greeting = (
            f"我已完成「{gname}」的深度研究报告。\n\n"
            "想再了解点什么？你可以：\n"
            "- 质疑某个分析结论\n"
            "- 追问某只个股的推荐逻辑\n"
            "- 要求补充宏观或行业细节\n"
            "- 把报告中任意段落拖入此处提问"
        )
    else:
        greeting = "研究报告已就绪。想再了解点什么？"

    msg_id = save_chat_message(group_id, "assistant", greeting)
    return {"greeted": True, "message": {"id": msg_id, "role": "assistant", "content": greeting}}


# ── Worker 端函数（由 chat_worker.py 调用）──────────────────

def process_pending_messages():
    """Worker：处理所有 pending 的 assistant 消息"""
    from utils.model_router import call_model as _cm

    def call_claude(system_prompt, user_message, max_tokens=4096, **kwargs):
        return _cm('chat', system_prompt, user_message, max_tokens=max_tokens)

    pending = execute_query(
        "SELECT id, group_id, metadata_json FROM group_chat_messages "
        "WHERE role='assistant' AND metadata_json LIKE '%%\"status\":\"pending\"%%' "
        "ORDER BY id ASC",
        [],
    )

    if not pending:
        return 0

    processed = 0
    for row in pending:
        msg_id = row["id"]
        group_id = row["group_id"]

        try:
            result = _process_single_chat(group_id, call_claude)
            # 更新 pending 消息为实际回复
            metadata = {}
            if result["modified_fields"]:
                metadata["modified_fields"] = result["modified_fields"]
            meta_json = json.dumps(metadata, ensure_ascii=False) if metadata else None
            execute_insert(
                "UPDATE group_chat_messages SET content=?, metadata_json=? WHERE id=?",
                [result["reply"], meta_json, msg_id],
            )
            processed += 1
        except Exception as e:
            logger.error(f"处理 pending 消息 #{msg_id} 失败: {e}")
            execute_insert(
                "UPDATE group_chat_messages SET content=?, metadata_json=NULL WHERE id=?",
                [f"处理失败: {e}", msg_id],
            )
            processed += 1

    return processed


def _process_single_chat(group_id: int, call_claude_fn) -> dict:
    """处理单个标签组的最新用户消息"""
    # 加载标签组和研究数据
    group = execute_query("SELECT * FROM tag_groups WHERE id=?", [group_id])
    if not group:
        return {"reply": "标签组不存在", "modified_fields": []}
    group = group[0]

    tags = json.loads(group.get("tags_json") or "[]")
    group_logic = group.get("group_logic") or ""
    group_name = group.get("group_name") or ""

    research = execute_query(
        "SELECT * FROM tag_group_research WHERE group_id=? ORDER BY id DESC LIMIT 1",
        [group_id],
    )
    macro_json = {}
    industry_json = {}
    top10_stocks = []
    research_id = None
    logic_synthesis = {}
    if research:
        r = research[0]
        research_id = r["id"]
        try:
            macro_json = json.loads(r.get("macro_json") or "{}")
        except Exception:
            pass
        try:
            industry_json = json.loads(r.get("industry_json") or "{}")
        except Exception:
            pass
        try:
            top10_stocks = json.loads(r.get("top10_stocks_json") or "[]")
        except Exception:
            pass
        try:
            logic_synthesis = json.loads(r.get("logic_synthesis_json") or "{}")
        except Exception:
            logic_synthesis = {}

    # 拉取全部与本主题相关的对话历史（排除 pending 占位）
    history = execute_query(
        "SELECT role, content, metadata_json FROM group_chat_messages "
        "WHERE group_id=? AND (metadata_json NOT LIKE '%\"status\":\"pending\"%' OR metadata_json IS NULL) "
        "ORDER BY created_at ASC",
        [group_id],
    )

    # 取最近用户消息用于检索
    latest_user_msg = ""
    for msg in reversed(history):
        if msg["role"] == "user":
            latest_user_msg = msg["content"]
            break

    # 提取拖入上下文前缀（格式：「[标题]」内容...）
    dragged_context = ""
    if latest_user_msg.startswith("【") and "】" in latest_user_msg:
        end_bracket = latest_user_msg.index("】")
        dragged_context = latest_user_msg[:end_bracket + 1]

    # hybrid_search 检索最新相关内容（检索失败时静默降级）
    retrieved_context = "（暂无额外检索内容）"
    if latest_user_msg:
        try:
            from retrieval.hybrid import hybrid_search
            search_query = latest_user_msg[:200]
            # 加入标签组名作为锚定，提高检索精度
            if group_name and group_name not in search_query:
                search_query = group_name + " " + search_query
            hr = hybrid_search(search_query, top_k=5)
            if hr and hr.merged_context:
                retrieved_context = hr.merged_context[:1500]
        except Exception as e:
            logger.debug(f"hybrid_search 检索失败（降级）: {e}")

    # 构建上下文说明（含拖入的报告段落）
    context_section = ""
    if dragged_context:
        context_section = f"【用户正在追问报告中的这段内容】\n{latest_user_msg}\n"

    # 构建 system prompt
    system_prompt = CHAT_SYSTEM_PROMPT.format(
        group_name=group_name,
        tags=", ".join(tags),
        group_logic=group_logic,
        macro_summary=macro_json.get("summary", "暂无"),
        industry_summary=industry_json.get("summary", "暂无"),
        stocks_summary=_format_stocks_summary(top10_stocks),
        thesis_summary=logic_synthesis.get("thesis_summary", "暂无"),
        context_section=context_section,
        retrieved_context=retrieved_context,
    )

    # 构建对话历史（全量，不截断，保持完整上下文）
    conversation = ""
    for msg in history:
        role_label = "用户" if msg["role"] == "user" else "AI"
        conversation += f"\n{role_label}: {msg['content']}\n"

    # 调用 Claude
    ai_reply = call_claude_fn(system_prompt, conversation, max_tokens=4096, model="opus")

    # 解析修改报告 + 批判性思维
    clean_reply, modifications = _parse_report_modifications(ai_reply)
    clean_reply, ct_items = _parse_critical_thinking(clean_reply)

    # 应用报告修改
    modified_fields = []
    if modifications and research_id:
        for mod in modifications:
            field = mod.get("field")
            action = mod.get("action", "update")
            data = mod.get("data")
            if field and data:
                _apply_modification(research_id, field, data, action=action)
                modified_fields.append(field)

    # 积累批判性思维
    if ct_items:
        _append_critical_thinking(ct_items, group_name)

    return {"reply": clean_reply, "modified_fields": modified_fields}


def _format_stocks_summary(top10_stocks: list) -> str:
    if not top10_stocks:
        return "暂无推荐个股"
    parts = []
    for grp in top10_stocks[:3]:
        industry = grp.get("industry", "")
        stocks = [s.get("stock_name", "") for s in grp.get("stocks", [])[:3]]
        parts.append(f"{industry}: {', '.join(stocks)}")
    return "; ".join(parts)


def _parse_report_modifications(text: str) -> tuple:
    modifications = []
    pattern = r'```修改报告\s*\n(.*?)```'
    matches = re.findall(pattern, text, re.DOTALL)
    for match in matches:
        try:
            mod = json.loads(match.strip())
            modifications.append(mod)
        except json.JSONDecodeError:
            logger.warning(f"解析修改报告 JSON 失败: {match[:200]}")
    clean_text = re.sub(pattern, '', text, flags=re.DOTALL).strip()
    return clean_text, modifications


def _parse_critical_thinking(text: str) -> tuple:
    ct_items = []
    pattern = r'```批判性思维\s*\n(.*?)```'
    matches = re.findall(pattern, text, re.DOTALL)
    for match in matches:
        try:
            item = json.loads(match.strip())
            ct_items.append(item)
        except json.JSONDecodeError:
            logger.warning(f"解析批判性思维 JSON 失败: {match[:200]}")
    clean_text = re.sub(pattern, '', text, flags=re.DOTALL).strip()
    return clean_text, ct_items


def _apply_modification(research_id: int, field: str, data, action: str = "update"):
    allowed_fields = {"macro_json", "industry_json", "top10_stocks_json", "logic_synthesis_json"}
    if field not in allowed_fields:
        return

    if action == "append":
        # append: data = {"key": "路径", "value": 要追加的内容}
        # 读取现有值，在指定 key 下追加
        col_map = {
            "macro_json": "macro_json",
            "industry_json": "industry_json",
            "top10_stocks_json": "top10_stocks_json",
            "logic_synthesis_json": "logic_synthesis_json",
        }
        rows = execute_query(
            f"SELECT {col_map[field]} FROM tag_group_research WHERE id=?",
            [research_id],
        )
        existing = {}
        if rows and rows[0].get(col_map[field]):
            try:
                existing = json.loads(rows[0][col_map[field]])
            except Exception:
                existing = {}

        key = data.get("key") if isinstance(data, dict) else None
        value = data.get("value") if isinstance(data, dict) else data
        if key and isinstance(existing, dict):
            current = existing.get(key)
            if isinstance(current, list):
                if isinstance(value, list):
                    existing[key] = current + value
                else:
                    existing[key] = current + [value]
            elif isinstance(current, str):
                existing[key] = current + "\n" + str(value)
            else:
                existing[key] = value
        data = existing

    json_str = json.dumps(data, ensure_ascii=False, default=str)
    if field == "macro_json":
        summary = data.get("summary", "") if isinstance(data, dict) else ""
        execute_insert(
            "UPDATE tag_group_research SET macro_json=?, macro_report=? WHERE id=?",
            [json_str, summary, research_id],
        )
    elif field == "industry_json":
        summary = data.get("summary", "") if isinstance(data, dict) else ""
        execute_insert(
            "UPDATE tag_group_research SET industry_json=?, industry_report=? WHERE id=?",
            [json_str, summary, research_id],
        )
    elif field == "top10_stocks_json":
        execute_insert(
            "UPDATE tag_group_research SET top10_stocks_json=? WHERE id=?",
            [json_str, research_id],
        )
    elif field == "logic_synthesis_json":
        execute_insert(
            "UPDATE tag_group_research SET logic_synthesis_json=? WHERE id=?",
            [json_str, research_id],
        )
    logger.info(f"已更新研究 #{research_id} 的 {field}（action={action}）")


def _append_critical_thinking(items: list, group_name: str):
    skill_path = SKILLS_DIR / "stock-analysis-critical-thinking" / "SKILL.md"
    if not skill_path.exists():
        skill_path.parent.mkdir(parents=True, exist_ok=True)
        skill_path.write_text(
            "---\nname: stock-analysis-critical-thinking\n"
            "description: 股票分析的批判性思维清单\n---\n\n"
            "## 分析时必须检查的问题清单\n\n",
            encoding="utf-8",
        )
    today = datetime.now().strftime("%Y-%m-%d")
    content = skill_path.read_text(encoding="utf-8")
    for item in items:
        check_point = item.get("check_point", "")
        reason = item.get("reason", "")
        if check_point:
            content += (
                f"\n### [{today}] 来源：{group_name}\n"
                f"- 检查点：{check_point}\n"
                f"- 原因：{reason}\n"
            )
    skill_path.write_text(content, encoding="utf-8")
    logger.info(f"已追加 {len(items)} 条批判性思维检查点")

"""Agent 执行器 — DeepSeek function calling 驱动的 Agent Loop

流程:
  1. 构建 messages（system prompt + 用户消息 + 历史）
  2. 调用 DeepSeek API（带 tools 参数）
  3. 如果返回 tool_calls → 执行工具 → 追加结果到 messages → 回到步骤 2
  4. 如果返回普通 content → 结束，返回最终回答
"""
import json
import logging
from typing import Generator

from agent.schema_context import get_schema_context
from agent.tools import TOOL_SCHEMAS, TOOL_FUNCTIONS

logger = logging.getLogger(__name__)

MAX_TOOL_ROUNDS = 8  # 最多工具调用轮次，防止无限循环

SYSTEM_PROMPT_TEMPLATE = """你是一个专业的股票分析助手，可以查询用户的私有股票分析数据库来回答问题。

{schema_context}

## 工作方式
- 收到问题后，先判断需要查询哪些数据，然后调用相应工具获取数据
- 可以多次调用工具，逐步收集所需信息
- 基于查询到的真实数据给出分析，不要凭空捏造数据
- 如果数据库中没有相关数据，如实告知用户
- 回答使用中文，数据分析要有洞察，不只是罗列数据

## 注意事项
- stock_code 为6位数字字符串，如 '600519'（贵州茅台）
- 查询行情/资金流时，days 参数控制时间范围
- 知识图谱工具（query_kg_*）提供深度的产业链和因果分析
- execute_sql 是兜底工具，其他工具无法满足时才使用
"""


def _get_api_key() -> str:
    """从 system_config 表读取 DeepSeek API Key"""
    try:
        from utils.db_utils import execute_query
        rows = execute_query(
            "SELECT value FROM system_config WHERE config_key='deepseek_api_key'",
        )
        if rows and rows[0].get("value"):
            return rows[0]["value"]
    except Exception:
        pass
    import os
    return os.environ.get("DEEPSEEK_API_KEY", "")


def _call_tool(tool_name: str, tool_args: dict) -> str:
    """执行工具函数，返回字符串结果"""
    fn = TOOL_FUNCTIONS.get(tool_name)
    if not fn:
        return json.dumps({"error": f"未知工具: {tool_name}"}, ensure_ascii=False)
    try:
        return fn(**tool_args)
    except Exception as e:
        logger.error(f"工具 {tool_name} 执行失败: {e}")
        return json.dumps({"error": str(e)}, ensure_ascii=False)


def run_agent(user_message: str, history: list = None,
              model: str = "deepseek-chat",
              extra_context: str = "") -> str:
    """
    运行 Agent，返回最终回答字符串。

    Args:
        user_message: 用户问题
        history: 历史对话列表，格式 [{"role": "user"/"assistant", "content": "..."}]
        model: DeepSeek 模型名称
        extra_context: 额外上下文（如项目/角色信息），拼入 system prompt

    Returns:
        最终回答字符串
    """
    import openai

    api_key = _get_api_key()
    if not api_key:
        return "错误：未配置 DeepSeek API Key，请在系统设置中配置 deepseek_api_key"

    client = openai.OpenAI(
        api_key=api_key,
        base_url="https://api.deepseek.com/v1",
        timeout=120,
    )

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        schema_context=get_schema_context()
    )
    if extra_context:
        system_prompt += f"\n\n## 当前上下文\n{extra_context}"

    messages = [{"role": "system", "content": system_prompt}]
    if history:
        messages.extend(history[-10:])  # 最多保留最近10轮历史
    messages.append({"role": "user", "content": user_message})

    for round_num in range(MAX_TOOL_ROUNDS):
        logger.debug(f"Agent round {round_num + 1}, messages={len(messages)}")

        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            tools=TOOL_SCHEMAS,
            tool_choice="auto",
            max_tokens=4096,
        )

        choice = resp.choices[0]
        msg = choice.message

        # 把模型回复追加到 messages
        messages.append(msg.model_dump(exclude_none=True))

        # 没有工具调用 → 最终回答
        if not msg.tool_calls:
            return msg.content or ""

        # 执行所有工具调用
        for tc in msg.tool_calls:
            tool_name = tc.function.name
            try:
                tool_args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                tool_args = {}

            logger.info(f"调用工具: {tool_name}({tool_args})")
            result = _call_tool(tool_name, tool_args)

            # 截断过长的工具结果（防止撑爆 context）
            if len(result) > 8000:
                result = result[:8000] + "\n...(结果已截断)"

            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result,
            })

    # 超过最大轮次，强制要求模型给出最终回答
    logger.warning("Agent 达到最大工具调用轮次，强制结束")
    resp = client.chat.completions.create(
        model=model,
        messages=messages + [
            {"role": "user", "content": "请基于以上查询结果给出最终回答。"}
        ],
        max_tokens=4096,
    )
    return resp.choices[0].message.content or "抱歉，无法生成回答"


def run_agent_stream(user_message: str, history: list = None,
                     model: str = "deepseek-chat",
                     extra_context: str = "") -> Generator[str, None, None]:
    """
    流式版本的 Agent，工具调用阶段不流式，最终回答阶段流式输出。

    Yields:
        字符串片段（最终回答的流式 token）
    """
    import openai

    api_key = _get_api_key()
    if not api_key:
        yield "错误：未配置 DeepSeek API Key"
        return

    client = openai.OpenAI(
        api_key=api_key,
        base_url="https://api.deepseek.com/v1",
        timeout=120,
    )

    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        schema_context=get_schema_context()
    )
    if extra_context:
        system_prompt += f"\n\n## 当前上下文\n{extra_context}"

    messages = [{"role": "system", "content": system_prompt}]
    if history:
        messages.extend(history[-10:])
    messages.append({"role": "user", "content": user_message})

    # 工具调用阶段（非流式）
    for round_num in range(MAX_TOOL_ROUNDS):
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            tools=TOOL_SCHEMAS,
            tool_choice="auto",
            max_tokens=4096,
        )
        choice = resp.choices[0]
        msg = choice.message
        messages.append(msg.model_dump(exclude_none=True))

        if not msg.tool_calls:
            # 没有工具调用，直接 yield 最终内容
            yield msg.content or ""
            return

        for tc in msg.tool_calls:
            tool_name = tc.function.name
            try:
                tool_args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                tool_args = {}
            result = _call_tool(tool_name, tool_args)
            if len(result) > 8000:
                result = result[:8000] + "\n...(结果已截断)"
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result,
            })

    # 超过最大轮次，流式输出最终回答
    stream = client.chat.completions.create(
        model=model,
        messages=messages + [
            {"role": "user", "content": "请基于以上查询结果给出最终回答。"}
        ],
        max_tokens=4096,
        stream=True,
    )
    for chunk in stream:
        delta = chunk.choices[0].delta
        if delta.content:
            yield delta.content

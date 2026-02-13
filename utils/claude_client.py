"""Claude API封装"""
import json
import os
from datetime import datetime
from anthropic import Anthropic
from utils.db_utils import execute_insert

client = None
_last_key = None


def _get_api_key():
    """优先从 DB 读取，回退到环境变量"""
    try:
        from utils.sys_config import get_config
        db_key = get_config("claude_api_key")
        if db_key:
            return db_key
    except Exception:
        pass
    return os.getenv("ANTHROPIC_API_KEY", "") or os.getenv("ANTHROPIC_AUTH_TOKEN", "")


def _get_base_url():
    try:
        from utils.sys_config import get_config
        url = get_config("claude_base_url")
        if url:
            return url
    except Exception:
        pass
    return os.getenv("ANTHROPIC_BASE_URL", None)


def _get_model():
    try:
        from utils.sys_config import get_config
        m = get_config("claude_model")
        if m:
            return m
    except Exception:
        pass
    return "claude-sonnet-4-20250514"


def get_client():
    global client, _last_key
    key = _get_api_key()
    base_url = _get_base_url()
    # 如果 key 变了，重建 client
    if client is None or key != _last_key:
        kwargs = {"api_key": key}
        if base_url:
            kwargs["base_url"] = base_url
        client = Anthropic(**kwargs)
        _last_key = key
    return client


def call_claude(system_prompt, user_message, max_tokens=4096):
    """调用Claude API并记录用量"""
    c = get_client()
    model = _get_model()
    response = c.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system_prompt,
        messages=[{"role": "user", "content": user_message}],
    )
    # 记录API用量
    today = datetime.now().strftime("%Y-%m-%d")
    usage = response.usage
    execute_insert(
        """INSERT INTO api_usage (api_name, call_date, call_count, input_tokens, output_tokens, cost_usd)
           VALUES ('claude', ?, 1, ?, ?, 0)
           ON CONFLICT(api_name, call_date) DO UPDATE SET
             call_count = call_count + 1,
             input_tokens = input_tokens + excluded.input_tokens,
             output_tokens = output_tokens + excluded.output_tokens""",
        [today, usage.input_tokens, usage.output_tokens],
    )
    return response.content[0].text


def call_claude_json(system_prompt, user_message, max_tokens=4096):
    """调用Claude并解析JSON响应"""
    text = call_claude(system_prompt, user_message, max_tokens)
    # 尝试提取JSON
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]
    return json.loads(text.strip())

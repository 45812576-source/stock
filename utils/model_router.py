"""多厂商模型调度器 — 根据 stage 从 model_configs 表路由到对应厂商 API

支持厂商:
  claude_cli  — 通过本地 claude CLI 调用（默认，无需 API Key）
  openai      — OpenAI SDK（需 pip install openai）
  gemini      — Google Generative AI（需 pip install google-generativeai）
  groq        — Groq SDK（需 pip install groq）
  deepseek    — OpenAI 兼容接口
  minimax     — MiniMax OpenAI 兼容接口（默认模型 kimi2.5）
"""
import json
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

# ── 缓存 model_configs，减少 DB 查询 ────────────────────────────────────────

_config_cache: dict = {}


def _load_stage_config(stage: str) -> dict:
    """从 model_configs 表读取 stage 配置，带内存缓存（进程级别）"""
    if stage in _config_cache:
        return _config_cache[stage]
    try:
        from utils.db_utils import execute_query
        rows = execute_query(
            "SELECT * FROM model_configs WHERE stage=%s AND enabled=1",
            [stage],
        )
        if rows:
            _config_cache[stage] = dict(rows[0])
            return _config_cache[stage]
    except Exception as e:
        logger.warning(f"model_configs 读取失败（stage={stage}）: {e}")

    # 降级：默认 claude_cli/sonnet
    default = {"stage": stage, "provider": "claude_cli", "model_name": "sonnet",
               "api_key_ref": None, "base_url": None, "extra_json": None, "enabled": 1}
    _config_cache[stage] = default
    return default


def invalidate_config_cache(stage: str = None):
    """清除缓存（保存新配置后调用）"""
    if stage:
        _config_cache.pop(stage, None)
    else:
        _config_cache.clear()


def _get_api_key(api_key_ref: str) -> str:
    """从 system_config 表读取 API Key"""
    if not api_key_ref:
        return ""
    try:
        from utils.db_utils import execute_query
        rows = execute_query(
            "SELECT value FROM system_config WHERE config_key=%s",
            [api_key_ref],
        )
        return rows[0]["value"] if rows else ""
    except Exception:
        return ""


# ── 各厂商调用实现 ────────────────────────────────────────────────────────────

def _call_claude_cli(model_name: str, system_prompt: str, user_message: str,
                     max_tokens: int, timeout: int, retries: int) -> str:
    from utils.claude_client import call_claude
    return call_claude(system_prompt, user_message,
                       max_tokens=max_tokens, timeout=timeout,
                       retries=retries, model=model_name)



def _call_anthropic(model_name: str, api_key: str, base_url: str,
                    system_prompt: str, user_message: str,
                    max_tokens: int, timeout: int, retries: int) -> str:
    try:
        from anthropic import Anthropic
    except ImportError:
        raise RuntimeError("anthropic 包未安装，请运行: pip install anthropic")

    # 优先使用 api_key，如果为空则尝试环境变量
    if not api_key:
        import os
        api_key = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_API_KEY")
    
    if not api_key:
         raise RuntimeError("未配置 Anthropic API Key (环境变量 ANTHROPIC_API_KEY 或 system_config)")

    client = Anthropic(api_key=api_key, base_url=base_url or None, timeout=timeout, max_retries=retries)
    
    try:
        message = client.messages.create(
            model=model_name,
            max_tokens=max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}]
        )
        return message.content[0].text
    except Exception as e:
        raise RuntimeError(f"Anthropic API 调用失败: {e}")

def _call_openai(model_name: str, api_key: str, base_url: str,
                 system_prompt: str, user_message: str,
                 max_tokens: int, timeout: int, retries: int) -> str:
    try:
        import openai
    except ImportError:
        raise RuntimeError("openai 包未安装，请运行: pip install openai")

    client = openai.OpenAI(
        api_key=api_key or "placeholder",
        base_url=base_url or None,
        timeout=timeout,
        max_retries=retries,
    )
    resp = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        max_tokens=max_tokens,
    )
    return resp.choices[0].message.content or ""


def _call_gemini(model_name: str, api_key: str,
                 system_prompt: str, user_message: str,
                 max_tokens: int, timeout: int) -> str:
    try:
        import google.generativeai as genai
    except ImportError:
        raise RuntimeError("google-generativeai 包未安装，请运行: pip install google-generativeai")

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(
        model_name=model_name,
        system_instruction=system_prompt,
    )
    resp = model.generate_content(
        user_message,
        generation_config=genai.types.GenerationConfig(max_output_tokens=max_tokens),
    )
    return resp.text or ""


def _call_groq(model_name: str, api_key: str,
               system_prompt: str, user_message: str,
               max_tokens: int, timeout: int) -> str:
    try:
        from groq import Groq
    except ImportError:
        raise RuntimeError("groq 包未安装，请运行: pip install groq")

    client = Groq(api_key=api_key, timeout=timeout)
    resp = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        max_tokens=max_tokens,
    )
    return resp.choices[0].message.content or ""


def _call_deepseek(model_name: str, api_key: str, base_url: str,
                   system_prompt: str, user_message: str,
                   max_tokens: int, timeout: int, retries: int) -> str:
    """DeepSeek 使用 OpenAI 兼容接口。

    deepseek-reasoner 特殊处理：推理内容在 reasoning_content，
    实际输出在 content。当 content 为空时（max_tokens 不足），
    尝试从 reasoning_content 中提取 JSON。
    """
    try:
        import openai
    except ImportError:
        raise RuntimeError("openai 包未安装，请运行: pip install openai")

    effective_base_url = base_url or "https://api.deepseek.com/v1"

    # deepseek-reasoner 需要更大的 max_tokens（推理占用大量 token）
    effective_max_tokens = max_tokens
    if "reasoner" in model_name.lower() and max_tokens < 8000:
        effective_max_tokens = 8000
        logger.debug(f"deepseek-reasoner: 自动扩大 max_tokens {max_tokens} → {effective_max_tokens}")

    client = openai.OpenAI(
        api_key=api_key or "placeholder",
        base_url=effective_base_url,
        timeout=timeout,
        max_retries=retries,
    )
    resp = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        max_tokens=effective_max_tokens,
    )
    content = resp.choices[0].message.content or ""
    if content:
        return content

    # content 为空时（reasoner 模型 token 耗尽），尝试从 reasoning_content 提取
    reasoning = getattr(resp.choices[0].message, "reasoning_content", None) or ""
    if reasoning:
        logger.warning(f"deepseek {model_name}: content 为空，尝试从 reasoning_content 提取 JSON")
        import re
        # 找最后一个完整 JSON 块
        matches = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', reasoning, re.DOTALL)
        if matches:
            return matches[-1]
        logger.error(f"deepseek {model_name}: reasoning_content 中未找到 JSON，finish_reason={resp.choices[0].finish_reason}")
    return ""


def _call_minimax(model_name: str, api_key: str, base_url: str,
                  system_prompt: str, user_message: str,
                  max_tokens: int, timeout: int, retries: int) -> str:
    """MiniMax 使用 OpenAI 兼容接口，默认模型 kimi2.5"""
    try:
        import openai
    except ImportError:
        raise RuntimeError("openai 包未安装，请运行: pip install openai")

    effective_base_url = base_url or "https://api.minimax.chat/v1"
    effective_model = model_name or "kimi2.5"

    client = openai.OpenAI(
        api_key=api_key or "placeholder",
        base_url=effective_base_url,
        timeout=timeout,
        max_retries=retries,
    )
    resp = client.chat.completions.create(
        model=effective_model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
        max_tokens=max_tokens,
    )
    return resp.choices[0].message.content or ""


# ── 公共路由函数 ──────────────────────────────────────────────────────────────

def _log_to_pipeline(stage: str, provider: str, model_name: str, status: str,
                     pipeline_id: int = None, error_msg: str = None) -> int:
    """记录 AI 调用到 pipeline_runs，返回记录 ID"""
    try:
        from utils.db_utils import execute_insert
        if pipeline_id:
            # 更新现有记录
            execute_insert(
                """UPDATE pipeline_runs SET status=%s, finished_at=NOW(), error_message=%s WHERE id=%s""",
                [status, error_msg, pipeline_id],
            )
            return pipeline_id
        else:
            # 创建新记录，execute_insert 返回 lastrowid
            return execute_insert(
                """INSERT INTO pipeline_runs (pipeline_name, stage, status, started_at)
                   VALUES (%s, %s, 'running', NOW())""",
                [f"{provider}:{model_name}", stage],
            )
    except Exception as e:
        logger.warning(f"pipeline_runs 记录失败: {e}")
        return None


def call_model(stage: str, system_prompt: str, user_message: str,
               max_tokens: int = 4096, timeout: int = 900, retries: int = 2) -> str:
    """根据 stage 查 model_configs 表，路由到对应厂商的 API"""
    cfg = _load_stage_config(stage)
    provider = cfg.get("provider", "claude_cli")
    model_name = cfg.get("model_name", "sonnet")
    api_key = _get_api_key(cfg.get("api_key_ref") or "")
    base_url = cfg.get("base_url") or ""

    # 解析 extra_json（温度、max_tokens 覆盖等）
    extra = {}
    if cfg.get("extra_json"):
        try:
            extra = json.loads(cfg["extra_json"])
        except Exception:
            pass
    if "max_tokens" in extra:
        max_tokens = int(extra["max_tokens"])

    logger.debug(f"call_model stage={stage} provider={provider} model={model_name}")

    # 记录调用开始
    pipeline_id = _log_to_pipeline(stage, provider, model_name, "running")

    try:
        if provider == "claude_cli":
            result = _call_claude_cli(model_name, system_prompt, user_message,
                                    max_tokens, timeout, retries)
        elif provider == "anthropic":
            result = _call_anthropic(model_name, api_key, base_url,
                                   system_prompt, user_message, max_tokens, timeout, retries)
        elif provider == "openai":
            result = _call_openai(model_name, api_key, base_url,
                                system_prompt, user_message, max_tokens, timeout, retries)
        elif provider == "gemini":
            result = _call_gemini(model_name, api_key,
                                system_prompt, user_message, max_tokens, timeout)
        elif provider == "groq":
            result = _call_groq(model_name, api_key,
                              system_prompt, user_message, max_tokens, timeout)
        elif provider == "deepseek":
            result = _call_deepseek(model_name, api_key, base_url,
                                  system_prompt, user_message, max_tokens, timeout, retries)
        elif provider == "minimax":
            result = _call_minimax(model_name, api_key, base_url,
                                 system_prompt, user_message, max_tokens, timeout, retries)
        else:
            logger.warning(f"未知 provider={provider}，降级到 claude_cli")
            result = _call_claude_cli(model_name, system_prompt, user_message,
                                    max_tokens, timeout, retries)

        # 记录成功
        _log_to_pipeline(stage, provider, model_name, "success", pipeline_id)
        return result

    except Exception as e:
        # 记录失败
        _log_to_pipeline(stage, provider, model_name, "failed", pipeline_id, str(e)[:500])
        raise


def call_model_json(stage: str, system_prompt: str, user_message: str,
                    max_tokens: int = 4096, timeout: int = 900, retries: int = 2) -> dict:
    """调用模型并解析 JSON 响应"""
    text = call_model(stage, system_prompt, user_message, max_tokens, timeout, retries)
    from utils.claude_client import _extract_json
    return _extract_json(text)


def call_model_with_tools(
    stage: str,
    messages: list,
    tools: list,
    tool_executor,
    max_rounds: int = 8,
) -> dict:
    """带tool_use循环的模型调用（OpenAI兼容格式）

    Args:
        stage: model_configs stage名
        messages: OpenAI格式的messages列表
        tools: OpenAI格式的tools定义列表
        tool_executor: 执行tool call的回调函数 fn(name, args) -> str
        max_rounds: 最大tool_use循环次数

    Returns:
        {"content": str, "tool_calls_log": list}
    """
    import json as _json
    try:
        import openai
    except ImportError:
        raise RuntimeError("openai 包未安装，请运行: pip install openai")

    cfg = _load_stage_config(stage)
    provider = cfg.get("provider", "deepseek")
    model_name = cfg.get("model_name", "deepseek-chat")
    api_key = _get_api_key(cfg.get("api_key_ref") or "")
    base_url = cfg.get("base_url") or "https://api.deepseek.com/v1"

    extra = {}
    if cfg.get("extra_json"):
        try:
            extra = _json.loads(cfg["extra_json"])
        except Exception:
            pass
    max_tokens = int(extra.get("max_tokens", 4096))

    client = openai.OpenAI(api_key=api_key or "placeholder", base_url=base_url, timeout=120)

    tool_calls_log = []
    messages = list(messages)  # 复制避免修改原始列表

    pipeline_id = _log_to_pipeline(stage, provider, model_name, "running")

    try:
        for round_num in range(max_rounds):
            resp = client.chat.completions.create(
                model=model_name,
                messages=messages,
                tools=tools if tools else openai.NOT_GIVEN,
                tool_choice="auto" if tools else openai.NOT_GIVEN,
                max_tokens=max_tokens,
            )
            msg = resp.choices[0].message

            if not msg.tool_calls:
                _log_to_pipeline(stage, provider, model_name, "success", pipeline_id)
                return {"content": msg.content or "", "tool_calls_log": tool_calls_log}

            # 有tool_calls，执行每个
            # 手动构造 assistant 消息，避免 model_dump 在不同 openai 版本下的差异
            assistant_msg = {
                "role": "assistant",
                "content": msg.content or "",
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in msg.tool_calls
                ],
            }
            messages.append(assistant_msg)
            for tc in msg.tool_calls:
                args = _json.loads(tc.function.arguments)
                result = tool_executor(tc.function.name, args)
                tool_calls_log.append({
                    "round": round_num,
                    "name": tc.function.name,
                    "args": args,
                    "result_preview": str(result)[:200],
                })
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": str(result),
                })

        # 达到最大轮次，最后不带tools调用获取总结
        resp = client.chat.completions.create(
            model=model_name,
            messages=messages,
            max_tokens=max_tokens,
        )
        _log_to_pipeline(stage, provider, model_name, "success", pipeline_id)
        return {"content": resp.choices[0].message.content or "", "tool_calls_log": tool_calls_log}

    except Exception as e:
        _log_to_pipeline(stage, provider, model_name, "failed", pipeline_id, str(e)[:500])
        raise


def call_model_vision(stage: str, text_prompt: str, image_urls: list,
                      max_tokens: int = 4096, timeout: int = 120) -> str:
    """视觉模型调用 — 支持 claude_cli 和 OpenAI 兼容视觉 API（DashScope 等）

    image_urls 支持两种格式：
      - http(s):// URL → 自动下载转 base64
      - 本地文件路径 → 直接读取转 base64
    """
    cfg = _load_stage_config(stage)
    provider = cfg.get("provider", "claude_cli")
    model_name = cfg.get("model_name", "sonnet")
    api_key = _get_api_key(cfg.get("api_key_ref") or "")
    base_url = cfg.get("base_url") or ""

    if provider == "claude_cli":
        from utils.claude_client import call_claude_vision
        return call_claude_vision(text_prompt, image_urls,
                                  max_tokens=max_tokens, timeout=timeout)
    elif provider in ("openai", "dashscope", "deepseek", "minimax"):
        return _call_openai_vision(model_name, api_key, base_url,
                                   text_prompt, image_urls,
                                   max_tokens, timeout)
    else:
        logger.warning(f"vision stage={stage} provider={provider} 不支持视觉，降级到文本")
        return call_model(stage, "", text_prompt, max_tokens, timeout)


def _call_openai_vision(model_name: str, api_key: str, base_url: str,
                        text_prompt: str, image_urls: list,
                        max_tokens: int, timeout: int) -> str:
    """通过 OpenAI 兼容接口调用视觉模型（DashScope/Qwen-VL-Max 等）"""
    import base64
    import os

    try:
        import openai
    except ImportError:
        raise RuntimeError("openai 包未安装，请运行: pip install openai")

    # 构建 content 数组：图片 + 文本
    content = []
    for url in image_urls[:5]:  # 最多5张
        try:
            b64_data = _image_to_base64(url)
            content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{b64_data}"},
            })
        except Exception as e:
            logger.warning(f"视觉模型: 图片加载失败 {str(url)[:80]}: {e}")

    if not content:
        logger.warning("视觉模型: 无有效图片，降级到纯文本")
        return ""

    content.append({"type": "text", "text": text_prompt})

    client = openai.OpenAI(
        api_key=api_key or "placeholder",
        base_url=base_url or None,
        timeout=timeout,
    )
    resp = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": content}],
        max_tokens=max_tokens,
    )
    return resp.choices[0].message.content or ""


def _image_to_base64(source: str) -> str:
    """将图片来源（URL 或本地路径）转为 base64 字符串"""
    import base64
    import os

    if source.startswith("http://") or source.startswith("https://"):
        import requests
        resp = requests.get(source, timeout=30, verify=False)
        resp.raise_for_status()
        return base64.b64encode(resp.content).decode()
    elif os.path.isfile(source):
        with open(source, "rb") as f:
            return base64.b64encode(f.read()).decode()
    else:
        raise ValueError(f"无法识别的图片来源: {source[:80]}")

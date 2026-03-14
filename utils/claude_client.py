"""Claude CLI 封装 — 通过 claude 命令行工具调用，不依赖 API Key"""
import json
import logging
import os
import subprocess

logger = logging.getLogger(__name__)

# 默认模型 — 可通过环境变量 CLAUDE_MODEL 或直接修改此值来切换
DEFAULT_MODEL = os.environ.get("CLAUDE_MODEL", "sonnet")


def _clean_env():
    """构建干净的环境变量，避免嵌套 Claude Code 会话冲突"""
    env = os.environ.copy()
    env.pop("CLAUDECODE", None)
    return env


def call_claude(system_prompt, user_message, max_tokens=4096, timeout=900, retries=2, model=None):
    """通过 claude CLI 调用分析

    将 system_prompt 和 user_message 合并后通过 stdin 管道传入，
    避免长 prompt 或特殊字符导致的参数解析问题。
    自动重试间歇性 CLI 错误（Execution error / 流解析错误）。
    """
    # 限制 prompt 总长度，避免过大导致超时
    max_prompt_len = 30000
    if len(system_prompt) > max_prompt_len:
        system_prompt = system_prompt[:max_prompt_len] + "\n...(已截断)"
    prompt = f"{system_prompt}\n\n---\n\n{user_message}"

    use_model = model or DEFAULT_MODEL
    cmd = ["claude", "-p", "--output-format", "text", "--model", use_model]

    import time
    last_error = None
    for attempt in range(1 + retries):
        try:
            result = subprocess.run(
                cmd,
                input=prompt,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=_clean_env(),
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"claude CLI 调用超时（{timeout}秒）")
        except FileNotFoundError:
            raise RuntimeError("未找到 claude 命令，请确认已安装 Claude Code CLI")

        if result.returncode == 0:
            output = result.stdout.strip()
            # 检查是否为空输出或 "Execution error" 这类无效响应
            if output and output != "Execution error":
                return output

        # 间歇性错误，可重试
        err = result.stderr.strip()[:500] if result.stderr else ""
        last_error = f"code={result.returncode}, stdout={result.stdout.strip()[:100]}, stderr={err}"
        if attempt < retries:
            logger.warning(f"claude CLI 间歇性错误 (attempt {attempt+1}/{1+retries}): {last_error}")
            time.sleep(1 + attempt)  # 递增等待
            continue

    raise RuntimeError(f"claude CLI 返回错误（已重试{retries}次）: {last_error}")


def call_claude_json(system_prompt, user_message, max_tokens=4096, timeout=900):
    """通过 claude CLI 调用并解析 JSON 响应"""
    text = call_claude(system_prompt, user_message, max_tokens, timeout)
    return _extract_json(text)


def _repair_json(text):
    """尝试修复常见的 JSON 格式问题"""
    import re

    # 1. 移除控制字符（保留换行和制表符）
    text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', text)

    # 2. 修复截断的 JSON — 补齐未闭合的括号（按嵌套顺序）
    open_braces = text.count('{') - text.count('}')
    open_brackets = text.count('[') - text.count(']')
    if open_braces > 0 or open_brackets > 0:
        # 截断到最后一个完整的键值对
        last_comma = text.rfind(',')
        if last_comma > len(text) // 2:
            text = text[:last_comma]
        # 扫描确定嵌套顺序，按逆序闭合
        stack = []
        in_str = False
        for ch in text:
            if ch == '\\' and in_str:
                continue
            if ch == '"':
                in_str = not in_str
            elif not in_str:
                if ch in '{[':
                    stack.append('}' if ch == '{' else ']')
                elif ch in '}]' and stack:
                    stack.pop()
        text += ''.join(reversed(stack))

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # 3. 尝试修复未转义的双引号（在字符串值内部）
    # 策略：逐字符扫描，跟踪是否在字符串内部
    try:
        fixed = _fix_unescaped_quotes(text)
        return json.loads(fixed)
    except (json.JSONDecodeError, Exception):
        pass

    return None


def _fix_unescaped_quotes(text):
    """修复 JSON 字符串值中未转义的双引号"""
    result = []
    i = 0
    in_string = False
    while i < len(text):
        ch = text[i]
        if ch == '\\' and in_string:
            result.append(ch)
            if i + 1 < len(text):
                i += 1
                result.append(text[i])
            i += 1
            continue
        if ch == '"':
            if not in_string:
                in_string = True
                result.append(ch)
            else:
                # 判断这个引号是否是字符串结束符
                # 往后看：跳过空白后应该是 , : ] } 或字符串结束
                rest = text[i+1:].lstrip()
                if not rest or rest[0] in ',:]}':
                    in_string = False
                    result.append(ch)
                else:
                    # 这是字符串内部的未转义引号
                    result.append('\\"')
        else:
            result.append(ch)
        i += 1
    return ''.join(result)


def _extract_json(text):
    """从 Claude 响应中提取 JSON，支持多种格式"""
    import re

    candidates = []

    # 1. 尝试 ```json ... ``` 代码块
    if "```json" in text:
        block = text.split("```json")[1].split("```")[0].strip()
        candidates.append(block)

    # 2. 尝试 ``` ... ``` 代码块
    if "```" in text:
        block = text.split("```")[1].split("```")[0].strip()
        if block not in candidates:
            candidates.append(block)

    # 3. 尝试找到最外层的 { ... } 对
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        candidates.append(match.group())

    # 4. 原始文本
    candidates.append(text.strip())

    # 先尝试直接解析
    for c in candidates:
        try:
            return json.loads(c)
        except json.JSONDecodeError:
            pass

    # 再尝试修复后解析
    for c in candidates:
        result = _repair_json(c)
        if result is not None:
            return result

    raise ValueError(f"无法从响应中提取JSON，响应前500字符: {text[:500]}")


def call_claude_vision(text_prompt, image_urls, max_tokens=4096, timeout=120):
    """通过 claude CLI 的附件功能分析图片

    Args:
        text_prompt: 文本提示
        image_urls: 图片URL列表
        max_tokens: 最大输出token
        timeout: 超时秒数
    """
    import tempfile
    import requests as req

    if not image_urls:
        return call_claude("", text_prompt, max_tokens, timeout)

    # 下载图片到临时文件
    tmp_files = []
    try:
        for url in image_urls[:5]:  # 最多5张图
            try:
                resp = req.get(url, timeout=15, verify=False)
                resp.raise_for_status()
                # 根据 content-type 确定后缀
                ct = resp.headers.get("content-type", "")
                ext = ".jpg"
                if "png" in ct:
                    ext = ".png"
                elif "webp" in ct:
                    ext = ".webp"
                elif "gif" in ct:
                    ext = ".gif"
                tf = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
                tf.write(resp.content)
                tf.close()
                tmp_files.append(tf.name)
            except Exception as e:
                logger.warning(f"下载图片失败 {url[:80]}: {e}")

        if not tmp_files:
            return ""

        # 视觉模型暂不支持，直接返回空
        return ""

    finally:
        # 清理临时文件
        for f in tmp_files:
            try:
                os.unlink(f)
            except OSError:
                pass

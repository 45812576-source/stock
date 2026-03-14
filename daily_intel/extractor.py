"""daily_intel/extractor.py — DeepSeek 结构化提取

输入：文本 + 来源标题
输出：[{stock_name, stock_code, industry, business_desc, event_type, event_summary}, ...]
"""
import json
import logging
import re
import threading

logger = logging.getLogger(__name__)

# ── DeepSeek 客户端（lazy singleton）────────────────────────────────

_deepseek_client = None
_deepseek_lock = threading.Lock()


def _get_deepseek():
    global _deepseek_client
    if _deepseek_client is None:
        with _deepseek_lock:
            if _deepseek_client is None:
                from openai import OpenAI
                from utils.db_utils import execute_cloud_query
                rows = execute_cloud_query(
                    "SELECT value FROM system_config WHERE config_key='deepseek_api_key'"
                )
                if not rows:
                    raise RuntimeError("system_config 中未找到 deepseek_api_key")
                import httpx
                _deepseek_client = OpenAI(
                    api_key=rows[0]["value"],
                    base_url="https://api.deepseek.com/v1",
                    http_client=httpx.Client(trust_env=False),
                )
    return _deepseek_client


def _call_deepseek(system_prompt: str, text: str, max_tokens: int = 3000) -> str:
    if len(text) > 12000:
        text = text[:12000] + "\n\n[文本已截断]"
    client = _get_deepseek()
    resp = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text},
        ],
        max_tokens=max_tokens,
        temperature=0.2,
        timeout=120,
    )
    return resp.choices[0].message.content


# ── 提取 Prompt ──────────────────────────────────────────────────────

_EXTRACT_PROMPT = """你是专业的A股投资研究助手。以下是一篇来自知识星球的帖子或手动录入的投资信息，
请提取其中所有被明确分析或讨论的A股公司/股票。

对每家公司输出以下字段：
- stock_name: 股票简称（如"宁德时代"）
- stock_code: A股代码（如"300750"，确定时给出，不确定则为 null）
- industry: 所属行业或板块（如"新能源"、"半导体"）
- business_desc: 一句话主营业务描述（≤30字，如"锂电池PACK龙头，主攻动力电池和储能"）
- event_type: 从以下枚举中选一个最匹配的：
    "题材组合推荐" / "公司动态" / "行业动态" / "宏观市场动态" / "宏观政策动态"
- event_summary: 用 FOE 法（Fact-Opinion-Evidence）写200字内的摘要：
    先陈述核心事实，再给出观点/逻辑，最后列关键数据佐证。保留具体数字、百分比等关键数据。

输出严格的 JSON 数组（不含 markdown 代码块），示例：
[
  {
    "stock_name": "宁德时代",
    "stock_code": "300750",
    "industry": "新能源",
    "business_desc": "全球锂电池龙头，专注动力电池和储能系统",
    "event_type": "公司动态",
    "event_summary": "【事实】宁德时代2024年Q4净利润同比增长42%，达89亿元。【观点】固态电池技术领先优势+海外产能扩张是核心逻辑，维持高成长预期。【佐证】海外客户BMW/特斯拉合同续签，欧洲工厂Q2投产，产能利用率回升至91%。"
  }
]

只输出 JSON 数组，不要有任何其他文字。如果文本中没有具体股票分析（如纯市场情绪、宏观数据），
但提到了宏观政策或市场动态，则输出一个带 stock_name="宏观" 且 stock_code=null 的条目，
event_type 选"宏观市场动态"或"宏观政策动态"，event_summary 描述核心内容。
如果完全没有可提取内容，输出空数组 []。"""


# ── 主提取函数 ───────────────────────────────────────────────────────

def extract_stocks_from_text(text: str, source_title: str = "") -> list[dict]:
    """DeepSeek 提取：输入文本 → 结构化公司列表

    Returns:
        [{stock_name, stock_code, industry, business_desc, event_type, event_summary}, ...]
        发生错误时返回空列表
    """
    if not text or len(text.strip()) < 50:
        return []

    input_text = f"来源标题：{source_title}\n\n{text}" if source_title else text

    try:
        raw = _call_deepseek(_EXTRACT_PROMPT, input_text)
        # 提取 JSON 数组（兼容 markdown 代码块）
        m = re.search(r"\[.*\]", raw, re.DOTALL)
        if not m:
            logger.warning(f"[Extractor] DeepSeek 未返回有效 JSON: {raw[:200]}")
            return []
        stocks = json.loads(m.group(0))
        if not isinstance(stocks, list):
            return []

        # 清洗每个字段
        result = []
        for s in stocks:
            if not isinstance(s, dict):
                continue
            sname = (s.get("stock_name") or "").strip()
            if not sname:
                continue
            result.append({
                "stock_name": sname[:50],
                "stock_code": (s.get("stock_code") or "").strip()[:20] or None,
                "industry": (s.get("industry") or "").strip()[:100],
                "business_desc": (s.get("business_desc") or "").strip()[:200],
                "event_type": (s.get("event_type") or "").strip()[:50],
                "event_summary": (s.get("event_summary") or "").strip(),
            })
        return result

    except json.JSONDecodeError as e:
        logger.warning(f"[Extractor] JSON 解析失败: {e}")
        return []
    except Exception as e:
        logger.error(f"[Extractor] DeepSeek 调用失败: {e}")
        return []

"""股票提及提取器 — Pipeline B2
从 extracted_texts 读取文本，调用 DeepSeek 提取股票提及，
写入 stock_mentions（云端），并同步到本地。
"""
import json
import logging
from datetime import datetime
from utils.db_utils import execute_cloud_query, execute_cloud_insert, sync_mentions_to_local

logger = logging.getLogger(__name__)

STOCK_MENTIONS_PROMPT = """你是专业的金融信息分析专家。请从以下文本中提取所有被**明确提及**的股票/上市公司标的。

严格规则：
1. 只提取文本中**直接出现**的具体股票名称或代码，不推断、不联想相关公司
2. 股票名称必须是具体的上市公司名称（如"宁德时代"、"比亚迪"），不接受模糊描述
3. 数据提供商、指数编制机构（如万得资讯、中证指数）不算投资标的
4. 如果文本中没有明确提及任何具体股票，直接返回空数组 []

输出 JSON 数组，每个元素：
{
  "stock_name": "股票/标的名称（具体公司名）",
  "stock_code": "代码（如有，如 600519.SH）",
  "related_themes": "相关题材/概念（逗号分隔）",
  "related_events": "文本中提及的相关事件",
  "theme_logic": "为什么这个股票和这个题材相关（基于文本内容）",
  "mention_time": "报道发布时间（如有，格式 YYYY-MM-DD HH:MM:SS，否则留空）"
}"""


def _get_deepseek_client():
    from openai import OpenAI
    rows = execute_cloud_query(
        "SELECT value FROM system_config WHERE config_key='deepseek_api_key'"
    )
    if not rows:
        raise RuntimeError("system_config 中未找到 deepseek_api_key")
    return OpenAI(api_key=rows[0]["value"], base_url="https://api.deepseek.com/v1")


def _call_deepseek(client, text: str) -> list:
    if len(text) > 12000:
        text = text[:12000] + "\n\n[文本已截断]"
    resp = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": STOCK_MENTIONS_PROMPT},
            {"role": "user", "content": text},
        ],
        max_tokens=2048,
        temperature=0.3,
    )
    raw = resp.choices[0].message.content
    # 解析 JSON
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        result = json.loads(raw)
        return result if isinstance(result, list) else []
    except Exception:
        return []


def extract_mentions_single(extracted_text_id: int, client=None) -> int:
    """对单条 extracted_text 提取股票提及

    Returns:
        写入的 stock_mentions 条数，失败返回 0
    """
    rows = execute_cloud_query(
        "SELECT id, full_text, publish_time FROM extracted_texts WHERE id=%s",
        [extracted_text_id],
    )
    if not rows:
        return 0
    row = rows[0]
    full_text = row["full_text"] or ""
    if not full_text.strip():
        return 0

    if client is None:
        client = _get_deepseek_client()

    try:
        mentions = _call_deepseek(client, full_text)
    except Exception as e:
        logger.error(f"DeepSeek 提取失败 id={extracted_text_id}: {e}")
        return 0

    if not mentions:
        return 0

    inserted_ids = []
    for m in mentions:
        if not m.get("stock_name"):
            continue
        # mention_time: 优先用模型返回的，否则用 publish_time
        mt = m.get("mention_time") or ""
        if not mt and row.get("publish_time"):
            mt = str(row["publish_time"])
        if not mt:
            mt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        mid = execute_cloud_insert(
            """INSERT INTO stock_mentions
               (extracted_text_id, stock_name, stock_code,
                related_themes, related_events, theme_logic, mention_time)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            [extracted_text_id,
             m.get("stock_name", ""),
             m.get("stock_code", "") or None,
             m.get("related_themes", "") or None,
             m.get("related_events", "") or None,
             m.get("theme_logic", "") or None,
             mt],
        )
        if mid:
            inserted_ids.append(mid)

    if inserted_ids:
        try:
            sync_mentions_to_local(inserted_ids)
        except Exception as e:
            logger.warning(f"同步 stock_mentions 到本地失败: {e}")

    return len(inserted_ids)

"""三管线验证测试 — Summary / Stock Mentions / KG

从 extracted_texts 每种 source_format 选 5 篇（共 20 条），
直接调 DeepSeek Chat 跑三条管线，输出对照 CSV 到桌面。
"""
import csv
import json
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_utils import execute_cloud_query
from cleaning.content_summarizer import SUMMARY_SYSTEM_PROMPT
from knowledge_graph.kg_updater import KG_EXTRACTION_PROMPT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ==================== Prompts ====================

STOCK_MENTIONS_PROMPT = """你是专业的金融信息分析专家。请从以下文本中提取所有被**明确提及**的股票/上市公司标的。

严格规则：
1. 只提取文本中**直接出现**的具体股票名称或代码，不推断、不联想相关公司
2. 股票名称必须是具体的上市公司名称（如"宁德时代"、"比亚迪"），不接受模糊描述（如"XX集团相关公司"、"航天板块"）
3. 数据提供商、指数编制机构（如万得资讯、中证指数）不算投资标的，除非文本明确讨论其股票
4. 如果文本中没有明确提及任何具体股票，直接返回空数组 []

输出 JSON 数组，每个元素：
{
  "stock_name": "股票/标的名称（必须是具体公司名）",
  "stock_code": "代码（如有，如 600519.SH）",
  "related_themes": "相关题材/概念",
  "related_events": "文本中提及的相关事件",
  "theme_logic": "为什么这个股票和这个题材相关（基于文本内容，不推断）",
  "mention_time": "该报道/文章的发布时间或文本中明确提到的时间（如有）"
}"""

# ==================== DeepSeek Client ====================

OUTPUT_PATH = os.path.expanduser("~/Desktop/three_pipeline_test.csv")


def get_deepseek_client():
    """从 system_config 读取 deepseek_api_key，返回 OpenAI 客户端"""
    from openai import OpenAI
    rows = execute_cloud_query(
        "SELECT value FROM system_config WHERE config_key='deepseek_api_key'"
    )
    if not rows:
        raise RuntimeError("system_config 中未找到 deepseek_api_key")
    api_key = rows[0]["value"]
    return OpenAI(api_key=api_key, base_url="https://api.deepseek.com/v1")


def call_deepseek(client, system_prompt: str, user_text: str, max_tokens=4096, model="deepseek-chat") -> str:
    """调用 DeepSeek，返回原始文本响应"""
    # 截断超长文本
    if len(user_text) > 12000:
        user_text = user_text[:12000] + "\n\n[文本已截断]"
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text},
        ],
        max_tokens=max_tokens,
        temperature=0.3,
    )
    return resp.choices[0].message.content


def parse_json_safe(text: str):
    """尝试从响应中解析 JSON，容忍 markdown 代码块"""
    text = text.strip()
    if text.startswith("```"):
        # 去掉 ```json ... ```
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text  # 返回原始文本


# ==================== 选样本 ====================

def select_samples() -> list:
    """每种 source_format 选 5 条记录，优先 extract_quality='pass'，不足则取任意"""
    samples = []
    formats = ["text", "pdf", "image", "mixed"]
    for fmt in formats:
        # 优先选 pass
        rows = execute_cloud_query(
            """SELECT id, full_text, source, source_format, publish_time
               FROM extracted_texts
               WHERE source_format=%s AND extract_quality='pass'
               ORDER BY RAND() LIMIT 5""",
            [fmt],
        )
        # 不足 5 条则补充任意 quality
        if len(rows) < 5:
            existing_ids = [r["id"] for r in rows]
            placeholder = ",".join(["%s"] * len(existing_ids)) if existing_ids else "0"
            extra = execute_cloud_query(
                f"""SELECT id, full_text, source, source_format, publish_time
                    FROM extracted_texts
                    WHERE source_format=%s AND id NOT IN ({placeholder})
                    ORDER BY RAND() LIMIT %s""",
                [fmt] + existing_ids + [5 - len(rows)],
            )
            rows.extend(extra)
        logger.info(f"source_format={fmt}: 选中 {len(rows)} 条")
        samples.extend(rows)
    if not samples:
        logger.warning("各 format 均无数据，尝试不限 format 选取")
        samples = execute_cloud_query(
            """SELECT id, full_text, source, source_format, publish_time
               FROM extracted_texts ORDER BY RAND() LIMIT 20"""
        )
    return samples


# ==================== 管线执行 ====================

def run_pipeline_a(client, text: str) -> dict:
    """Pipeline A: Summary — deepseek-chat"""
    raw = call_deepseek(client, SUMMARY_SYSTEM_PROMPT, text, model="deepseek-chat")
    result = parse_json_safe(raw)
    if isinstance(result, dict):
        return result
    return {"summary": str(result), "fact_summary": "", "opinion_summary": ""}


def run_pipeline_b2(client, text: str) -> list:
    """Pipeline B2: Stock Mentions — deepseek-chat"""
    raw = call_deepseek(client, STOCK_MENTIONS_PROMPT, text, model="deepseek-chat")
    result = parse_json_safe(raw)
    if isinstance(result, list):
        return result
    return []


def run_pipeline_c(client, text: str) -> dict:
    """Pipeline C: KG Extraction — deepseek-chat，纯三元组输出"""
    raw = call_deepseek(client, KG_EXTRACTION_PROMPT, text, max_tokens=4096, model="deepseek-chat")
    result = parse_json_safe(raw)
    # 新格式：直接是三元组数组
    if isinstance(result, list):
        return {"triples": result}
    # 兼容旧格式
    if isinstance(result, dict):
        return result
    return {"triples": []}


# ==================== 主流程 ====================

def main():
    logger.info("=== 三管线验证测试开始 ===")

    # 1. 选样本
    samples = select_samples()
    logger.info(f"共选中 {len(samples)} 条样本")
    if not samples:
        logger.error("没有可用样本，请检查 extracted_texts 表")
        return

    # 2. 初始化 DeepSeek 客户端
    client = get_deepseek_client()
    logger.info("DeepSeek 客户端初始化成功")

    # 3. 逐条跑三管线
    results = []
    for i, sample in enumerate(samples):
        sid = sample["id"]
        text = sample["full_text"] or ""
        logger.info(f"[{i+1}/{len(samples)}] id={sid} format={sample['source_format']} len={len(text)}")

        row = {
            "id": sid,
            "source": sample.get("source", ""),
            "source_format": sample.get("source_format", ""),
            "full_text_preview": text[:200].replace("\n", " "),
        }

        # Pipeline A: Summary
        try:
            a = run_pipeline_a(client, text)
            row["doc_type"] = a.get("doc_type", "")
            row["summary"] = a.get("summary", "")
            row["fact_summary"] = a.get("fact_summary", "")
            row["opinion_summary"] = a.get("opinion_summary", "")
            logger.info(f"  Pipeline A done")
        except Exception as e:
            logger.error(f"  Pipeline A failed: {e}")
            row["doc_type"] = ""
            row["summary"] = row["fact_summary"] = row["opinion_summary"] = f"ERROR: {e}"
        time.sleep(1)

        # Pipeline B2: Stock Mentions
        try:
            b2 = run_pipeline_b2(client, text)
            row["stock_mentions_json"] = json.dumps(b2, ensure_ascii=False)
            logger.info(f"  Pipeline B2 done, {len(b2)} mentions")
        except Exception as e:
            logger.error(f"  Pipeline B2 failed: {e}")
            row["stock_mentions_json"] = f"ERROR: {e}"
        time.sleep(1)

        # Pipeline C: KG
        try:
            c = run_pipeline_c(client, text)
            triples = c.get("triples") or c.get("entities", [])
            row["kg_triples_json"] = json.dumps(triples, ensure_ascii=False)
            logger.info(f"  Pipeline C done, {len(triples)} triples")
        except Exception as e:
            logger.error(f"  Pipeline C failed: {e}")
            row["kg_triples_json"] = f"ERROR: {e}"
        time.sleep(1)

        results.append(row)

    # 4. 写 CSV
    fieldnames = [
        "id", "source", "source_format", "full_text_preview",
        "doc_type", "summary", "fact_summary", "opinion_summary",
        "stock_mentions_json",
        "kg_triples_json",
    ]
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    logger.info(f"=== 完成！CSV 已保存到 {OUTPUT_PATH}，共 {len(results)} 行 ===")


if __name__ == "__main__":
    main()

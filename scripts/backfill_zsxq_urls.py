#!/usr/bin/env python3
"""回溯补采知识星球附件下载链接 + 重新提取

针对 source_documents 中 extract_status='failed' 且 oss_url 为空的记录：
1. 从 title 中解析出 topic_id 和文件名
2. 调用知识星球 API 获取帖子详情 → 匹配 file_id
3. 调用 /v2/files/{file_id}/download_url 获取临时下载链接
4. 更新 oss_url → 触发现有提取流程重新提取

用法:
    python scripts/backfill_zsxq_urls.py               # 正式运行
    python scripts/backfill_zsxq_urls.py --dry-run     # 仅预览不执行
    python scripts/backfill_zsxq_urls.py --limit 10    # 限制处理条数
"""

import sys
import os
import re
import time
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.db_utils import execute_cloud_query, execute_cloud_insert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"scripts/backfill_zsxq_urls_{datetime.now():%Y%m%d}.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─── 知识星球 API ───
ZSXQ_API_BASE = "https://api.zsxq.com/v2"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Origin": "https://wx.zsxq.com",
    "Referer": "https://wx.zsxq.com/",
}


def get_zsxq_token():
    """从云端 system_config 读取 zsxq token"""
    row = execute_cloud_query(
        "SELECT value FROM system_config WHERE config_key LIKE '%%zsxq%%' LIMIT 1"
    ) or []
    if not row:
        raise RuntimeError("未找到 zsxq token")
    return row[0]["value"]


def get_topic_detail(session, topic_id):
    """获取单个帖子详情"""
    resp = session.get(f"{ZSXQ_API_BASE}/topics/{topic_id}", timeout=15)
    data = resp.json()
    if data.get("succeeded"):
        return data.get("resp_data", {}).get("topic", {})
    logger.warning(f"获取 topic {topic_id} 失败: {data}")
    return None


def get_download_url(session, file_id):
    """获取文件临时下载链接"""
    resp = session.get(f"{ZSXQ_API_BASE}/files/{file_id}/download_url", timeout=10)
    data = resp.json()
    if data.get("succeeded"):
        return data.get("resp_data", {}).get("download_url", "")
    logger.warning(f"获取 download_url 失败 file_id={file_id}: {data}")
    return ""


def parse_title(title):
    """从 title 中解析 topic_id 和文件名: [ZSXQ:topic_id] filename"""
    m = re.match(r'\[ZSXQ:(\d+)\]\s*(.+)', title or "")
    if m:
        return m.group(1), m.group(2).strip()
    return None, None


def find_file_in_topic(topic, target_filename):
    """在 topic 的 files 中按文件名匹配找到 file_id"""
    talk = topic.get("talk", {})
    files = talk.get("files", [])
    # 精确匹配
    for f in files:
        if f.get("name", "").strip() == target_filename:
            return f.get("file_id")
    # 模糊匹配（去掉扩展名对比）
    target_stem = os.path.splitext(target_filename)[0].lower()
    for f in files:
        stem = os.path.splitext(f.get("name", ""))[0].lower()
        if stem == target_stem:
            return f.get("file_id")
    # 部分匹配（文件名包含）
    for f in files:
        if target_stem in f.get("name", "").lower() or f.get("name", "").lower() in target_filename.lower():
            return f.get("file_id")
    return None


def extract_single(doc_id, file_type, oss_url, title):
    """调用现有提取流程提取单条"""
    try:
        from ingestion.source_extractor import _extract_audio, _extract_pdf_with_meta, _semantic_clean
        from config.doc_types import classify_doc_type

        row = {"id": doc_id, "file_type": file_type, "oss_url": oss_url, "title": title, "text_content": ""}

        if file_type in ("audio", "mp3"):
            raw_text = _extract_audio(row)
        elif file_type == "pdf":
            raw_text, _ = _extract_pdf_with_meta(row)
        else:
            from ingestion.source_extractor import _extract_single_with_meta
            raw_text, _ = _extract_single_with_meta(row)

        if not raw_text or len(raw_text.strip()) < 20:
            return None

        cleaned = _semantic_clean(raw_text, file_type, doc_id, needs_understanding=False)
        if not cleaned or len(cleaned.strip()) < 20:
            return None

        new_doc_type = classify_doc_type(title, cleaned[:200])
        return {"cleaned": cleaned, "doc_type": new_doc_type}
    except Exception as e:
        logger.warning(f"提取失败 id={doc_id}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="回溯补采知识星球附件下载链接")
    parser.add_argument("--dry-run", action="store_true", help="仅预览不执行")
    parser.add_argument("--limit", type=int, default=100, help="处理条数上限")
    parser.add_argument("--skip-extract", action="store_true", help="仅补URL不提取")
    args = parser.parse_args()

    logger.info(f"=== 回溯补采启动 | dry_run={args.dry_run} limit={args.limit} ===")

    # 1. 查 failed 且 oss_url 为空的记录
    rows = execute_cloud_query("""
        SELECT id, title, file_type, oss_url, review_notes
        FROM source_documents
        WHERE extract_status = 'failed' AND source = 'zsxq'
          AND file_type IN ('audio', 'mp3', 'pdf', 'xlsx')
          AND (oss_url IS NULL OR oss_url = '')
        ORDER BY publish_date DESC
        LIMIT %s
    """, [args.limit]) or []
    logger.info(f"查到 {len(rows)} 条待补采记录")

    if not rows:
        return

    # 2. 按 topic_id 分组减少 API 调用
    import requests
    token = get_zsxq_token()
    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies.set("zsxq_access_token", token)

    # 分组
    topic_groups = {}  # topic_id -> [row, ...]
    skipped_parse = 0
    for row in rows:
        topic_id, filename = parse_title(row["title"])
        if not topic_id:
            skipped_parse += 1
            continue
        row["_topic_id"] = topic_id
        row["_filename"] = filename
        topic_groups.setdefault(topic_id, []).append(row)

    logger.info(f"共 {len(topic_groups)} 个 topic, 跳过解析失败 {skipped_parse} 条")

    # 3. 逐 topic 处理
    stats = {"url_found": 0, "url_missing": 0, "extracted": 0, "extract_fail": 0, "api_fail": 0}

    for i, (topic_id, group_rows) in enumerate(topic_groups.items()):
        # 限速
        if i > 0:
            time.sleep(1.5)

        topic = get_topic_detail(session, topic_id)
        if not topic:
            stats["api_fail"] += len(group_rows)
            continue

        talk_files = topic.get("talk", {}).get("files", [])
        logger.info(f"[{i+1}/{len(topic_groups)}] topic={topic_id} 含 {len(talk_files)} 个文件, 待处理 {len(group_rows)} 条")

        for row in group_rows:
            filename = row["_filename"]
            file_id = find_file_in_topic(topic, filename)

            if not file_id:
                logger.warning(f"  ✗ 未匹配到 file_id: {filename}")
                stats["url_missing"] += 1
                continue

            time.sleep(0.5)  # 限速
            download_url = get_download_url(session, file_id)
            if not download_url:
                stats["url_missing"] += 1
                continue

            stats["url_found"] += 1
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

            if args.dry_run:
                logger.info(f"  [DRY] id={row['id']} | {row['file_type']} | file_id={file_id} | url OK")
                continue

            # 更新 oss_url
            execute_cloud_insert(
                "UPDATE source_documents SET oss_url=%s WHERE id=%s",
                [download_url, row["id"]],
            )

            if args.skip_extract:
                # 仅补 URL，把状态改为 pending 等待下次定时提取
                execute_cloud_insert(
                    "UPDATE source_documents SET extract_status='pending', review_notes=%s WHERE id=%s",
                    [f"[backfill] {now_str} URL已补采, 待重新提取 (file_id={file_id})", row["id"]],
                )
                logger.info(f"  ✓ id={row['id']} URL已补采 → pending")
                continue

            # 立即提取
            result = extract_single(row["id"], row["file_type"], download_url, row["title"])
            if result:
                execute_cloud_insert(
                    "UPDATE source_documents SET extracted_text=%s, extract_status='extracted', "
                    "doc_type=%s, oss_url=NULL, review_notes=%s WHERE id=%s",
                    [result["cleaned"], result["doc_type"],
                     f"[recovered] {now_str} 回溯补采成功 ({len(result['cleaned'])}字, file_id={file_id})",
                     row["id"]],
                )
                stats["extracted"] += 1
                logger.info(f"  ✓ id={row['id']} 提取成功 ({len(result['cleaned'])}字)")
                # 推入管线
                try:
                    from ingestion.source_extractor import push_to_extracted_texts_by_ids
                    push_to_extracted_texts_by_ids([row["id"]])
                except Exception:
                    pass
            else:
                execute_cloud_insert(
                    "UPDATE source_documents SET review_notes=%s WHERE id=%s",
                    [f"[backfill_extract_fail] {now_str} URL已补采但提取失败 (file_id={file_id})", row["id"]],
                )
                stats["extract_fail"] += 1
                logger.info(f"  ✗ id={row['id']} 提取失败")

    logger.info(f"\n=== 完成 === {json.dumps(stats, ensure_ascii=False)}")
    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}结果: {json.dumps(stats, ensure_ascii=False, indent=2)}")


if __name__ == "__main__":
    main()

# 摘要 Chunk 实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将族2（研报/策略/路演/专题）的 content_summaries 结构化字段展开为自然语言段落，写入向量库，使"评级变化/目标价/核心论点"等查询能精准命中高密度摘要。

**Architecture:** 新增独立 Milvus collection `summary_chunks`，避免修改现有 `text_chunks` collection schema（不停机）；MySQL `text_chunks` 加 `chunk_type` 列区分原文/摘要 chunk；新建 `retrieval/summary_chunker.py` 负责渲染+写入；`unified_pipeline.py` 在 Pipeline A 完成后追加调用；`hybrid_search()` 同时搜两个 collection。

**Tech Stack:** Python, pymysql, pymilvus, BGEM3FlagModel (FlagEmbedding), langchain_text_splitters, 现有 `execute_query`/`execute_insert` 工具函数

---

## 准备：理解现有代码结构

在开始前，熟悉以下文件：
- `retrieval/vector_store.py` — `ensure_collection()`, `upsert_chunks()`, `search()` 的参数格式
- `retrieval/chunker.py` — `chunk_and_index()` 的写库模式
- `retrieval/embedding.py` — `embed_texts()`, `embed_query()` 接口
- `retrieval/semantic.py` — `semantic_search()` 的返回结构
- `retrieval/hybrid.py` — `hybrid_search()` 当前实现，要在这里加合并逻辑
- `cleaning/unified_pipeline.py` — `process_single()` 返回 `{"summary_id": ...}` 的位置

---

## Task 1：MySQL 加 chunk_type 列

**Files:**
- Modify: `db/migration_v3_chunks.sql`（追加注释说明）
- Execute SQL on local MySQL

**Step 1: 在本地 MySQL 执行 ALTER TABLE**

```bash
cd /Users/liaoxia/stock-analysis-system
mysql -h 127.0.0.1 -P 3306 -u root stock_analysis -e "
ALTER TABLE text_chunks
  ADD COLUMN IF NOT EXISTS chunk_type VARCHAR(20) DEFAULT 'raw'
  COMMENT 'raw=原文切片, summary=摘要chunk';
"
```

Expected: Query OK (或"Duplicate column name" 说明已存在也可以)

**Step 2: 验证**

```bash
mysql -h 127.0.0.1 -P 3306 -u root stock_analysis -e "DESCRIBE text_chunks;" | grep chunk_type
```

Expected: 看到 `chunk_type  varchar(20)  YES  ...  raw`

**Step 3: 不需要 commit（纯 SQL 操作）**

---

## Task 2：新建 `retrieval/summary_chunker.py`

**Files:**
- Create: `retrieval/summary_chunker.py`

### Step 1: 写文件

```python
"""摘要 Chunk — 将族2 content_summaries 展开为自然语言，写入 MySQL + Milvus summary_chunks"""
import json
import logging
from typing import Optional

from utils.db_utils import execute_cloud_query, execute_insert, execute_query

logger = logging.getLogger(__name__)

SUMMARY_COLLECTION = "summary_chunks"

# ── 模板渲染 ─────────────────────────────────────────────────────────────────

def render_summary_text(cs_row: dict) -> str:
    """将 content_summaries 一行展开为可读自然语言段落"""
    doc_type = cs_row.get("doc_type", "")
    summary = cs_row.get("summary") or ""
    fact_summary = cs_row.get("fact_summary") or ""
    opinion_summary = cs_row.get("opinion_summary") or ""
    evidence_assessment = cs_row.get("evidence_assessment") or ""
    info_gaps = cs_row.get("info_gaps") or ""

    raw_tf = cs_row.get("type_fields")
    if isinstance(raw_tf, str):
        try:
            tf = json.loads(raw_tf)
        except Exception:
            tf = {}
    elif isinstance(raw_tf, dict):
        tf = raw_tf
    else:
        tf = {}

    parts = []

    if doc_type == "research_report":
        parts.append(f"[研报摘要] {summary}")
        institution = tf.get("institution") or ""
        analyst = tf.get("analyst") or ""
        if institution or analyst:
            parts.append(f"机构：{institution}，分析师：{analyst}")
        rating = tf.get("rating") or ""
        target_price = tf.get("target_price") or ""
        current_price = tf.get("current_price") or ""
        if rating or target_price:
            parts.append(f"评级：{rating}，目标价：{target_price}，当前价：{current_price}")
        if fact_summary:
            parts.append(f"核心事实：{fact_summary}")
        if opinion_summary:
            parts.append(f"核心观点：{opinion_summary}")
        # key_arguments: list of {claim, evidence, strength}
        args = tf.get("key_arguments") or []
        if args:
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except Exception:
                    args = []
            arg_parts = []
            for a in args[:3]:
                if isinstance(a, dict):
                    claim = a.get("claim") or ""
                    evidence = a.get("evidence") or ""
                    strength = a.get("strength") or ""
                    arg_parts.append(f"{claim}（证据：{evidence}，强度：{strength}）")
            if arg_parts:
                parts.append(f"核心论点：{'；'.join(arg_parts)}")
        valuation = tf.get("valuation_method") or ""
        if valuation:
            parts.append(f"估值方法：{valuation}")
        risk = tf.get("risk_factors") or ""
        if risk:
            parts.append(f"风险提示：{risk}")
        if evidence_assessment:
            parts.append(f"证据评估：{evidence_assessment}")
        if info_gaps:
            parts.append(f"信息缺口：{info_gaps}")

    elif doc_type == "strategy_report":
        parts.append(f"[策略摘要] {summary}")
        market_view = tf.get("market_view") or ""
        if market_view:
            parts.append(f"市场观点：{market_view}")
        sector = tf.get("sector_allocation") or ""
        if sector:
            parts.append(f"推荐行业：{sector}")
        themes = tf.get("key_themes") or ""
        if themes:
            parts.append(f"投资主题：{themes}")
        args = tf.get("key_arguments") or []
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except Exception:
                args = []
        if args:
            arg_parts = []
            for a in args[:3]:
                if isinstance(a, dict):
                    claim = a.get("claim") or ""
                    evidence = a.get("evidence") or ""
                    arg_parts.append(f"{claim}（依据：{evidence}）")
            if arg_parts:
                parts.append(f"核心论点：{'；'.join(arg_parts)}")
        horizon = tf.get("time_horizon") or ""
        if horizon:
            parts.append(f"投资周期：{horizon}")
        risk = tf.get("risk_factors") or ""
        if risk:
            parts.append(f"风险提示：{risk}")

    elif doc_type == "roadshow_notes":
        parts.append(f"[纪要摘要] {summary}")
        company = tf.get("company") or ""
        if company:
            parts.append(f"公司：{company}")
        guidance = tf.get("management_guidance") or ""
        if guidance:
            parts.append(f"管理层指引：{guidance}")
        disclosures = tf.get("new_disclosures") or ""
        if disclosures:
            parts.append(f"首次披露：{disclosures}")
        qa = tf.get("key_qa") or []
        if isinstance(qa, str):
            try:
                qa = json.loads(qa)
            except Exception:
                qa = []
        if qa:
            qa_parts = []
            for item in qa[:2]:
                if isinstance(item, dict):
                    q = item.get("q") or item.get("question") or ""
                    a = item.get("a") or item.get("answer") or ""
                    qa_parts.append(f"Q: {q} A: {a}")
            if qa_parts:
                parts.append(f"核心问答：{'；'.join(qa_parts)}")
        if fact_summary:
            parts.append(f"核心事实：{fact_summary}")
        if opinion_summary:
            parts.append(f"核心观点：{opinion_summary}")

    elif doc_type == "feature_news":
        parts.append(f"[专题摘要] {summary}")
        level = tf.get("news_level") or ""
        if level:
            parts.append(f"层级：{level}")
        chain = tf.get("industry_chain") or ""
        if chain:
            parts.append(f"产业链影响：{chain}")
        perspectives = tf.get("multiple_perspectives") or ""
        if perspectives:
            parts.append(f"多方观点：{perspectives}")
        background = tf.get("background") or ""
        if background:
            parts.append(f"背景：{background}")
        if fact_summary:
            parts.append(f"核心事实：{fact_summary}")
        if opinion_summary:
            parts.append(f"核心观点：{opinion_summary}")

    else:
        # 兜底：通用拼接
        parts.append(f"[摘要] {summary}")
        if fact_summary:
            parts.append(f"核心事实：{fact_summary}")
        if opinion_summary:
            parts.append(f"核心观点：{opinion_summary}")

    return "\n".join(p for p in parts if p.strip())


# ── Milvus summary_chunks collection ─────────────────────────────────────────

def ensure_summary_collection():
    """创建 summary_chunks collection（若不存在），HNSW + COSINE"""
    from pymilvus import (
        Collection, CollectionSchema, FieldSchema, DataType, utility, connections
    )
    from config import MILVUS_HOST, MILVUS_PORT, EMBEDDING_DIM

    try:
        connections.connect(alias="default", host=MILVUS_HOST, port=MILVUS_PORT)
    except Exception:
        pass  # 已连接

    if utility.has_collection(SUMMARY_COLLECTION):
        col = Collection(SUMMARY_COLLECTION)
        col.load()
        return col

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=EMBEDDING_DIM),
        FieldSchema(name="content_summary_id", dtype=DataType.INT64),
        FieldSchema(name="extracted_text_id", dtype=DataType.INT64),
        FieldSchema(name="doc_type", dtype=DataType.VARCHAR, max_length=50),
        FieldSchema(name="publish_time", dtype=DataType.VARCHAR, max_length=20),
    ]
    schema = CollectionSchema(fields, description="summary_chunks 摘要向量索引")
    col = Collection(SUMMARY_COLLECTION, schema)
    col.create_index(
        field_name="embedding",
        index_params={
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 16, "efConstruction": 256},
        },
    )
    col.load()
    logger.info(f"Collection '{SUMMARY_COLLECTION}' 创建完成")
    return col


def _upsert_summary_embedding(
    chunk_id: int,
    embedding: list[float],
    content_summary_id: int,
    extracted_text_id: int,
    doc_type: str,
    publish_time: str,
):
    """写入 Milvus summary_chunks"""
    from pymilvus import Collection
    col = Collection(SUMMARY_COLLECTION)
    data = [
        [chunk_id],
        [embedding],
        [content_summary_id],
        [extracted_text_id],
        [doc_type[:50] if doc_type else ""],
        [publish_time[:20] if publish_time else ""],
    ]
    col.upsert(data)


# ── 主要接口 ──────────────────────────────────────────────────────────────────

def index_summary_chunk(content_summary_id: int) -> bool:
    """单条 content_summaries → 展开 → 写 text_chunks(summary) + Milvus summary_chunks

    Returns: True 成功，False 跳过或失败
    """
    # 读 content_summaries（云端）
    rows = execute_cloud_query(
        """SELECT cs.id, cs.extracted_text_id, cs.doc_type, cs.family,
                  cs.summary, cs.fact_summary, cs.opinion_summary,
                  cs.evidence_assessment, cs.info_gaps, cs.type_fields,
                  et.publish_time
           FROM content_summaries cs
           JOIN extracted_texts et ON cs.extracted_text_id = et.id
           WHERE cs.id = %s""",
        [content_summary_id],
    )
    if not rows:
        logger.warning(f"content_summaries id={content_summary_id} 不存在")
        return False

    cs = rows[0]
    family = cs.get("family")
    if family != 2:
        # 目前只处理族2
        return False

    rendered = render_summary_text(cs)
    if not rendered or len(rendered.strip()) < 20:
        logger.warning(f"渲染结果过短，跳过 cs_id={content_summary_id}")
        return False

    extracted_text_id = cs["extracted_text_id"]
    doc_type = cs.get("doc_type") or ""
    pt = cs.get("publish_time")
    pt_str = str(pt)[:10] if pt else ""

    # 写 text_chunks（本地 MySQL），chunk_type='summary'
    # chunk_index 用 content_summary_id 的负值区分，避免与原文 chunk 冲突
    # 实际使用一个固定大 offset，让 summary chunk 的 index 不与原文冲突
    SUMMARY_INDEX_OFFSET = 100000
    summary_chunk_index = SUMMARY_INDEX_OFFSET + content_summary_id

    chunk_id = execute_insert(
        """INSERT INTO text_chunks
           (extracted_text_id, chunk_index, chunk_text, chunk_type,
            doc_type, publish_time, source_doc_title, metadata_json)
           VALUES (%s, %s, %s, 'summary', %s, %s, %s, %s)
           ON DUPLICATE KEY UPDATE
             chunk_text=VALUES(chunk_text),
             chunk_type='summary',
             doc_type=VALUES(doc_type)""",
        [
            extracted_text_id,
            summary_chunk_index,
            rendered,
            doc_type or None,
            pt_str or None,
            None,
            json.dumps({"content_summary_id": content_summary_id}, ensure_ascii=False),
        ],
    )

    if not chunk_id:
        # ON DUPLICATE KEY：查回 id
        existing = execute_query(
            "SELECT id FROM text_chunks WHERE extracted_text_id=%s AND chunk_index=%s",
            [extracted_text_id, summary_chunk_index],
        )
        if existing:
            chunk_id = existing[0]["id"]

    if not chunk_id:
        logger.error(f"写 text_chunks 失败 cs_id={content_summary_id}")
        return False

    # 生成 embedding + 写 Milvus
    try:
        ensure_summary_collection()
        from retrieval.embedding import embed_texts
        embeddings = embed_texts([rendered])
        if not embeddings:
            return False
        _upsert_summary_embedding(
            chunk_id=chunk_id,
            embedding=embeddings[0],
            content_summary_id=content_summary_id,
            extracted_text_id=extracted_text_id,
            doc_type=doc_type,
            publish_time=pt_str,
        )
        logger.info(f"摘要 chunk 写入完成 cs_id={content_summary_id} chunk_id={chunk_id}")
        return True
    except Exception as e:
        logger.error(f"Milvus 写入失败 cs_id={content_summary_id}: {e}")
        return False


def search_summary_chunks(
    query_embedding: list[float],
    top_k: int = 5,
) -> list[dict]:
    """在 summary_chunks collection 中做向量搜索

    Returns:
        [{"chunk_id", "score", "content_summary_id", "extracted_text_id", "doc_type", "publish_time"}]
    """
    from pymilvus import Collection
    try:
        ensure_summary_collection()
        col = Collection(SUMMARY_COLLECTION)
        results = col.search(
            data=[query_embedding],
            anns_field="embedding",
            param={"metric_type": "COSINE", "params": {"ef": 128}},
            limit=top_k,
            output_fields=["content_summary_id", "extracted_text_id", "doc_type", "publish_time"],
        )
        hits = []
        for hit in results[0]:
            hits.append({
                "chunk_id": hit.id,
                "score": hit.score,
                "content_summary_id": hit.entity.get("content_summary_id"),
                "extracted_text_id": hit.entity.get("extracted_text_id"),
                "doc_type": hit.entity.get("doc_type"),
                "publish_time": hit.entity.get("publish_time"),
            })
        return hits
    except Exception as e:
        logger.warning(f"summary_chunks 搜索失败: {e}")
        return []


def backfill_family2(batch_size: int = 100, dry_run: bool = False) -> dict:
    """回填：对所有族2 content_summaries 生成摘要 chunk

    Args:
        batch_size: 每批处理数量
        dry_run: True 时只统计不写入
    Returns:
        {"total": int, "ok": int, "skip": int, "fail": int}
    """
    rows = execute_cloud_query(
        """SELECT id FROM content_summaries
           WHERE family = 2
           ORDER BY id
           LIMIT %s""",
        [batch_size],
    )
    total = len(rows)
    ok = skip = fail = 0

    for row in rows:
        cs_id = row["id"]
        if dry_run:
            logger.info(f"[dry_run] 会处理 cs_id={cs_id}")
            ok += 1
            continue
        try:
            result = index_summary_chunk(cs_id)
            if result:
                ok += 1
            else:
                skip += 1
        except Exception as e:
            logger.error(f"backfill 失败 cs_id={cs_id}: {e}")
            fail += 1

    logger.info(f"backfill_family2 完成: total={total} ok={ok} skip={skip} fail={fail}")
    return {"total": total, "ok": ok, "skip": skip, "fail": fail}
```

**Step 2: 快速冒烟测试（单函数）**

```bash
cd /Users/liaoxia/stock-analysis-system
python3 -c "
from retrieval.summary_chunker import render_summary_text
row = {
    'doc_type': 'research_report',
    'summary': '宁德时代2025年业绩超预期，维持买入评级',
    'fact_summary': '营收同比+15%',
    'opinion_summary': '储能需求旺盛',
    'evidence_assessment': '数据充分',
    'info_gaps': '海外产能进度待确认',
    'type_fields': '{\"institution\": \"中金公司\", \"analyst\": \"张三\", \"rating\": \"买入\", \"target_price\": \"200\", \"key_arguments\": [{\"claim\": \"储能放量\", \"evidence\": \"订单数据\", \"strength\": \"高\"}]}',
}
print(render_summary_text(row))
"
```

Expected: 打印出格式化的摘要文本，包含评级、机构、论点等字段

**Step 3: Commit**

```bash
cd /Users/liaoxia/stock-analysis-system
git add retrieval/summary_chunker.py
git commit -m "feat: add summary_chunker for family2 content_summaries vectorization"
```

---

## Task 3：接入 unified_pipeline — 管线 A 完成后写摘要 chunk

**Files:**
- Modify: `cleaning/unified_pipeline.py`，在 `process_single()` 的 results 收集之后

**Step 1: 找到修改位置**

在 `unified_pipeline.py` 的 `process_single()` 里，找到这段（约 480-492 行）：

```python
    for key, fut in futures.items():
        try:
            val = fut.result()
            if key == "a":
                results["summary_id"] = val
            elif key == "b":
                ...
```

在这个 for 循环结束、`return results` 之前，追加摘要 chunk 写入：

**Step 2: 插入代码**

在 `return results` 之前（约 492 行 `return results` 改为）：

```python
    # 摘要 chunk：族2文档的 content_summaries 展开写向量库
    if results.get("summary_id"):
        try:
            from retrieval.summary_chunker import index_summary_chunk
            index_summary_chunk(results["summary_id"])
        except Exception as e:
            logger.warning(f"摘要 chunk 写入失败 id={extracted_text_id}: {e}")

    return results
```

**Step 3: 验证改动不破坏现有逻辑**

```bash
cd /Users/liaoxia/stock-analysis-system
python3 -c "
from cleaning.unified_pipeline import process_single
import inspect
src = inspect.getsource(process_single)
assert 'index_summary_chunk' in src, 'index_summary_chunk 未注入'
print('OK: index_summary_chunk 已注入 process_single')
"
```

Expected: `OK: index_summary_chunk 已注入 process_single`

**Step 4: Commit**

```bash
git add cleaning/unified_pipeline.py
git commit -m "feat: call index_summary_chunk after pipeline A in unified_pipeline"
```

---

## Task 4：搜索端合并 summary_chunks 结果

**Files:**
- Modify: `retrieval/hybrid.py`

**Step 1: 理解当前 hybrid_search 结构**

当前 `hybrid_search()` 在 `retrieval/hybrid.py` 第 10-72 行。
它调用 `semantic_search()` 得到 `chunks`（ChunkResult 列表），然后可选调 KG。
最后 `_merge_context()` 拼成字符串。

**Step 2: 修改 `hybrid_search()` 加入 summary_chunks 合并**

在 `hybrid_search()` 中，`chunks = semantic_search(...)` 之后追加：

```python
    # 合并摘要 chunks（summary_chunks collection）
    try:
        from retrieval.embedding import embed_query as _embed_query
        from retrieval.summary_chunker import search_summary_chunks
        from retrieval.chunker import get_chunks_by_ids

        query_vec = _embed_query(query)
        summary_hits = search_summary_chunks(query_vec, top_k=5)

        if summary_hits:
            # 从 MySQL 取 chunk 文本
            s_chunk_ids = [h["chunk_id"] for h in summary_hits]
            s_score_map = {h["chunk_id"]: h["score"] * 1.2 for h in summary_hits}  # 摘要加权 1.2x
            s_rows = get_chunks_by_ids(s_chunk_ids)
            s_row_map = {r["id"]: r for r in s_rows}

            from retrieval.models import ChunkResult
            for cid in s_chunk_ids:
                row = s_row_map.get(cid)
                if not row:
                    continue
                chunks.append(ChunkResult(
                    chunk_id=cid,
                    text=row["chunk_text"],
                    score=s_score_map.get(cid, 0.0),
                    extracted_text_id=row["extracted_text_id"],
                    doc_type=row.get("doc_type") or "",
                    file_type="summary",
                    publish_time=str(row.get("publish_time") or ""),
                    source_doc_title=row.get("source_doc_title") or "",
                ))

            # 按 score 重新排序（摘要 chunk 因加权 1.2x 会上浮）
            chunks.sort(key=lambda c: c.score, reverse=True)
            result.chunks = chunks
    except Exception as e:
        logger.warning(f"summary_chunks 合并失败: {e}")
```

注意插入位置：在 `result.chunks = chunks` 这行之后（约第 43 行），且在 KG 查询之前。

**Step 3: 验证 import 不循环**

```bash
cd /Users/liaoxia/stock-analysis-system
python3 -c "from retrieval.hybrid import hybrid_search; print('import OK')"
```

Expected: `import OK`（不报循环 import 错误）

**Step 4: Commit**

```bash
git add retrieval/hybrid.py
git commit -m "feat: merge summary_chunks results in hybrid_search with 1.2x score boost"
```

---

## Task 5：新增 API 端点 — 手动触发回填

**Files:**
- Modify: `routers/datacollect.py`（追加一个路由）

**Step 1: 找到文件中合适的追加位置**

在 `routers/datacollect.py` 末尾，追加：

```python
@router.post("/api/backfill-summary-chunks")
async def backfill_summary_chunks(
    batch_size: int = 100,
    dry_run: bool = False,
):
    """回填族2摘要 chunk（research_report/strategy_report/roadshow_notes/feature_news）"""
    try:
        from retrieval.summary_chunker import backfill_family2
        result = backfill_family2(batch_size=batch_size, dry_run=dry_run)
        return {"status": "ok", "result": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}
```

**Step 2: 冒烟验证（dry_run 模式）**

先用 dry_run 验证查询逻辑不报错：

```bash
cd /Users/liaoxia/stock-analysis-system
python3 -c "
from retrieval.summary_chunker import backfill_family2
result = backfill_family2(batch_size=5, dry_run=True)
print('dry_run result:', result)
"
```

Expected: 打印出 `dry_run result: {'total': N, 'ok': N, 'skip': 0, 'fail': 0}`（N 可以为 0 如果云端没有族2数据）

**Step 3: Commit**

```bash
git add routers/datacollect.py
git commit -m "feat: add /api/backfill-summary-chunks endpoint"
```

---

## Task 6：端到端验证

**Step 1: 验证 ensure_summary_collection 可执行（Milvus 连接）**

```bash
cd /Users/liaoxia/stock-analysis-system
python3 -c "
from retrieval.summary_chunker import ensure_summary_collection
col = ensure_summary_collection()
print('summary_chunks collection 状态 OK，实体数:', col.num_entities)
"
```

Expected: 打印 collection 状态（连接失败时 Milvus 不可用则 warning，不 crash）

**Step 2: 选一条实际的族2 content_summaries 测试完整写入**

```bash
cd /Users/liaoxia/stock-analysis-system
python3 -c "
from utils.db_utils import execute_cloud_query
rows = execute_cloud_query(
    'SELECT id FROM content_summaries WHERE family=2 LIMIT 1'
)
if rows:
    cs_id = rows[0]['id']
    print(f'测试 cs_id={cs_id}')
    from retrieval.summary_chunker import index_summary_chunk
    ok = index_summary_chunk(cs_id)
    print('写入结果:', ok)
else:
    print('云端暂无族2摘要数据，跳过')
"
```

Expected: `写入结果: True`（或"暂无数据"）

**Step 3: 验证搜索命中摘要 chunk**

```bash
cd /Users/liaoxia/stock-analysis-system
python3 -c "
from retrieval.hybrid import hybrid_search
result = hybrid_search('宁德时代评级目标价', top_k=5)
print('chunks 数量:', len(result.chunks))
for c in result.chunks[:3]:
    print(f'  score={c.score:.3f} type={c.file_type} text={c.text[:60]}')
"
```

Expected: 结果中出现 `file_type='summary'` 的 chunk，且 score 较高

---

## 附录：数据库表结构参考

**text_chunks（本地 MySQL）新增字段后：**
```sql
chunk_type VARCHAR(20) DEFAULT 'raw'  -- 'raw' | 'summary'
```

**summary_chunks（Milvus）字段：**
```
id              INT64 (primary)
embedding       FLOAT_VECTOR(1024)
content_summary_id  INT64
extracted_text_id   INT64
doc_type        VARCHAR(50)
publish_time    VARCHAR(20)
```

**chunk_index 设计约定（避免 UNIQUE KEY 冲突）：**
- 原文 chunk：`chunk_index = 0, 1, 2, ...`
- 摘要 chunk：`chunk_index = 100000 + content_summary_id`（远离原文 index 范围）

---

## 执行顺序

```
Task 1  ─→  Task 2  ─→  Task 3  ─→  Task 4  ─→  Task 5  ─→  Task 6
(SQL)     (新文件)     (接管线)     (改搜索)    (API端点)    (验证)
```

Task 1 必须先做（数据库 schema）。Task 2 是核心，Task 3/4/5 依赖 Task 2，可并行。

"""向量+KG 混合检索冒烟测试

验证端到端：
  L1: semantic_search → Milvus 有返回结果
  L2: kg_enhanced_search → KG 实体查询 + chunk 溯源
  L3: hybrid_search → merged_context 可读

用法:
    python scripts/test_retrieval.py
    python scripts/test_retrieval.py --skip-l2  # 跳过 KG 测试
"""
import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def test_milvus_connection():
    print("\n=== Milvus 连接测试 ===")
    from retrieval.vector_store import ensure_collection, get_collection_stats
    col = ensure_collection()
    stats = get_collection_stats()
    print(f"Collection 状态: exists={stats['exists']}, count={stats['count']}")
    assert stats["exists"], "Collection 不存在！请先运行 backfill_chunks.py"
    assert stats["count"] > 0, "Collection 为空！请先运行 backfill_chunks.py"
    print(f"✓ Milvus 正常，{stats['count']} 条向量")
    return True


def test_l1_semantic_search():
    print("\n=== L1 语义检索测试 ===")
    from retrieval.semantic import semantic_search

    queries = [
        "AI服务器出货量",
        "碳酸锂价格对电池产业链影响",
        "宁德时代成本结构",
    ]

    for query in queries:
        results = semantic_search(query, top_k=5)
        print(f"\n查询: '{query}'")
        print(f"  结果数: {len(results)}")
        if results:
            top = results[0]
            print(f"  Top1 score={top.score:.4f} doc_type={top.doc_type}")
            print(f"  文本预览: {top.text[:100]}...")
        assert len(results) > 0, f"L1 查询无结果: {query}"

    print("✓ L1 语义检索正常")
    return True


def test_l1_with_filters():
    print("\n=== L1 带过滤条件测试 ===")
    from retrieval.semantic import semantic_search

    results = semantic_search(
        "新能源汽车",
        top_k=5,
        filters={"doc_types": ["research_report", "flash_news"]},
    )
    print(f"  带 doc_type 过滤结果数: {len(results)}")

    results2 = semantic_search(
        "锂电池",
        top_k=5,
        filters={"date_range": ("2025-01-01", "2026-12-31")},
    )
    print(f"  带 date_range 过滤结果数: {len(results2)}")
    print("✓ L1 过滤功能正常")
    return True


def test_l2_kg_enhanced(skip: bool = False):
    print("\n=== L2 KG 增强检索测试 ===")
    if skip:
        print("  (跳过)")
        return True

    from retrieval.kg_enhanced import kg_enhanced_search

    # 测试 context 模式
    result = kg_enhanced_search("宁德时代", mode="context", with_chunks=True)
    print(f"  context 模式 text 长度: {len(result.text)}")
    print(f"  evidence_chunks 数: {len(result.evidence_chunks)}")
    if result.text:
        print(f"  KG 文本预览: {result.text[:150]}...")

    # 测试 company 模式
    result2 = kg_enhanced_search("宁德时代", mode="company", with_chunks=False)
    print(f"  company 模式 text 长度: {len(result2.text)}")

    print("✓ L2 KG 增强检索正常")
    return True


def test_l3_hybrid_search():
    print("\n=== L3 混合检索测试 ===")
    from retrieval.hybrid import hybrid_search

    result = hybrid_search(
        "碳酸锂对电池产业链影响",
        strategy="auto",
        max_context_chars=3000,
    )
    print(f"  chunks 数: {len(result.chunks)}")
    print(f"  KG 结果: {'有' if result.kg and result.kg.text else '无'}")
    print(f"  merged_context 长度: {len(result.merged_context)}")
    print(f"\n  merged_context 预览:\n{result.merged_context[:500]}")
    assert len(result.merged_context) > 0, "merged_context 为空"

    print("✓ L3 混合检索正常")
    return True


def main():
    parser = argparse.ArgumentParser(description="检索系统冒烟测试")
    parser.add_argument("--skip-l2", action="store_true", help="跳过 KG 测试（KG 为空时用）")
    args = parser.parse_args()

    tests = [
        ("Milvus 连接", test_milvus_connection),
        ("L1 语义检索", test_l1_semantic_search),
        ("L1 带过滤", test_l1_with_filters),
        ("L2 KG 增强", lambda: test_l2_kg_enhanced(skip=args.skip_l2)),
        ("L3 混合检索", test_l3_hybrid_search),
    ]

    passed = 0
    failed = 0

    for name, fn in tests:
        try:
            fn()
            passed += 1
        except Exception as e:
            print(f"✗ {name} 失败: {e}")
            failed += 1

    print(f"\n{'='*40}")
    print(f"测试结果: {passed} 通过, {failed} 失败")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

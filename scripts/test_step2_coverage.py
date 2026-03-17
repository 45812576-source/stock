"""
Step1→Step2 内部Chunk打捞覆盖率测试
用法: python scripts/test_step2_coverage.py --code 603993
"""
import argparse
import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def load_step1_indicators(step1_json_path: str) -> tuple[list, list]:
    """从已有Step1结果文件加载指标列表"""
    with open(step1_json_path, encoding="utf-8") as f:
        data = json.load(f)
    tl = data.get("topline_indicators") or []
    bl = data.get("bottomline_indicators") or []
    return tl, bl


def _fetch_texts_local(stock_code: str, days: int = 180) -> list[str]:
    """从本地库拉该股相关文本（绕开 rag_context 中 cleaned_items.created_at bug）"""
    from utils.db_utils import execute_query
    texts = []
    # content_summaries via stock_mentions
    cs_rows = execute_query(
        """SELECT cs.summary
           FROM content_summaries cs
           JOIN stock_mentions sm ON cs.extracted_text_id = sm.extracted_text_id
           WHERE sm.stock_code = %s
             AND cs.summary IS NOT NULL AND cs.summary != ''
           ORDER BY cs.created_at DESC
           LIMIT 200""",
        [stock_code]
    ) or []
    texts += [r["summary"] for r in cs_rows if r.get("summary")]
    # cleaned_items via item_companies
    ci_rows = execute_query(
        """SELECT ci.summary
           FROM cleaned_items ci
           JOIN item_companies ic ON ci.id = ic.cleaned_item_id
           WHERE ic.stock_code = %s
             AND ci.summary IS NOT NULL AND ci.summary != ''
           ORDER BY ci.cleaned_at DESC
           LIMIT 200""",
        [stock_code]
    ) or []
    texts += [r["summary"] for r in ci_rows if r.get("summary")]
    return texts


def check_indicator_coverage(stock_code: str, stock_name: str, indicators: list, side: str, top_k: int = 8) -> list:
    """对每个指标，用内部RAG检索，判断是否有置信度数据"""
    from research.rag_context import _fallback_search

    # 拉全量文本
    texts = _fetch_texts_local(stock_code, days=180)
    print(f"\n  {side} - 内部文档数: {len(texts)} 条")

    results = []
    for ind in indicators:
        name = ind.get("name", "")
        category = ind.get("category", "")
        data_available_step1 = ind.get("data_available", False)

        query = f"{stock_name} {name} {ind.get('current_status', '')}"
        top_texts = _fallback_search(texts, query, top_k) if texts else []

        # 提取指标名的关键词（去掉括号注释，按 / 空格 、切分，≥2字）
        import re
        kw_raw = re.sub(r'[（(].*?[)）]', '', name)
        kw_parts = [w for w in re.split(r'[/\s、，,（(]', kw_raw) if len(w) >= 2]
        # 再追加单字切分（如 铜/价/量 这类）作为宽松备用
        kw_single = [c for c in kw_raw if '\u4e00' <= c <= '\u9fff']

        # 判断是否有实质性数据：文本中含≥2个指标关键词 且 含数字
        has_data = False
        has_number = False
        best_text = ""
        for t in top_texts:
            if len(t) < 30:
                continue
            # 严格：任意 kw_parts 词命中
            strict_hit = any(kw in t for kw in kw_parts) if kw_parts else False
            # 宽松：≥2个单字命中
            loose_hit = sum(1 for c in kw_single if c in t) >= 2
            kw_hit = strict_hit or loose_hit
            if kw_hit:
                has_data = True
                if any(c.isdigit() for c in t[:300]):
                    has_number = True
                    best_text = t
                    break
                elif not best_text:
                    best_text = t

        confidence = "high" if (has_data and has_number) else ("low" if has_data else "none")

        results.append({
            "name": name,
            "category": category,
            "data_available_step1": data_available_step1,
            "rag_hit": has_data,
            "has_number": has_number,
            "confidence": confidence,
            "top_snippet": best_text[:120] if best_text else (top_texts[0][:120] if top_texts else ""),
        })

    return results


def check_milvus_coverage(stock_code: str, stock_name: str, indicators: list, side: str, top_k: int = 8) -> list:
    """用 Milvus semantic_search 拿真实 chunk 列表，再做关键词置信度判断"""
    import re
    try:
        from retrieval.semantic import semantic_search
    except Exception as e:
        print(f"  Milvus不可用: {e}")
        return []

    results = []
    for ind in indicators:
        name = ind.get("name", "")
        query = f"{stock_name} {name} {ind.get('current_status', '')}"

        # 构建关键词
        kw_raw = re.sub(r'[（(].*?[)）]', '', name)
        kw_parts = [w for w in re.split(r'[/\s、，,（(]', kw_raw) if len(w) >= 2]
        kw_single = [c for c in kw_raw if '\u4e00' <= c <= '\u9fff']

        has_data = False
        has_number = False
        best_text = ""
        try:
            chunks = semantic_search(query, top_k=top_k)
            for chunk in chunks:
                t = getattr(chunk, "text", "") or ""
                if len(t) < 30:
                    continue
                strict_hit = any(kw in t for kw in kw_parts) if kw_parts else False
                loose_hit = sum(1 for c in kw_single if c in t) >= 2
                if strict_hit or loose_hit:
                    has_data = True
                    if any(c.isdigit() for c in t[:300]):
                        has_number = True
                        best_text = t
                        break
                    elif not best_text:
                        best_text = t
        except Exception as e:
            pass

        confidence = "high" if (has_data and has_number) else ("low" if has_data else "none")
        results.append({
            "name": name,
            "category": ind.get("category", ""),
            "rag_hit": has_data,
            "has_number": has_number,
            "confidence": confidence,
            "top_snippet": best_text[:120] if best_text else "",
        })
    return results


def print_results(results: list, side: str, method: str):
    total = len(results)
    high = sum(1 for r in results if r["confidence"] == "high")
    low  = sum(1 for r in results if r["confidence"] == "low")
    none = sum(1 for r in results if r["confidence"] == "none")

    print(f"\n{'='*60}")
    print(f"[{side.upper()}] {method} 覆盖率  ({total} 个指标)")
    print(f"{'='*60}")
    print(f"  高置信度 (有文本+有数字): {high}/{total} = {high/total*100:.1f}%")
    print(f"  低置信度 (有文本无数字): {low}/{total} = {low/total*100:.1f}%")
    print(f"  未命中:                 {none}/{total} = {none/total*100:.1f}%")
    print()

    for r in results:
        icon = "✓" if r["confidence"] == "high" else ("△" if r["confidence"] == "low" else "✗")
        print(f"  {icon} [{r['category']}] {r['name']}")
        if r["top_snippet"]:
            print(f"      → {r['top_snippet'][:100]}")
    return high, low, none, total


def run_test(stock_code: str, stock_name: str, step1_path: str = None, use_milvus: bool = False):
    print(f"\n{'#'*60}")
    print(f"# 603993 {stock_name} Step1→Step2 内部Chunk打捞覆盖率")
    print(f"{'#'*60}")

    if step1_path:
        print(f"\n从文件加载Step1结果: {step1_path}")
        tl_inds, bl_inds = load_step1_indicators(step1_path)
    else:
        print("\n从数据库拉取最新深度报告Step1结果...")
        from utils.db_utils import execute_query
        rows = execute_query(
            """SELECT report_json FROM deep_research
               WHERE target=%s AND research_type='stock'
               ORDER BY created_at DESC LIMIT 1""",
            [stock_code]
        ) or []
        if rows:
            data = json.loads(rows[0]["report_json"] or "{}")
            bm = data.get("business_model") or {}
            tl_inds = bm.get("topline_indicators") or []
            bl_inds = bm.get("bottomline_indicators") or []
        if not tl_inds and not bl_inds:
            print("未找到Step1指标，使用内置603993洛阳钼业标准指标集...")
            tl_inds, bl_inds = _default_luoyang_indicators()

    print(f"\n  topline_indicators: {len(tl_inds)} 个")
    print(f"  bottomline_indicators: {len(bl_inds)} 个")

    method = "Milvus hybrid_search" if use_milvus else "sentence_transformers fallback"
    check_fn = check_milvus_coverage if use_milvus else check_indicator_coverage

    tl_results = check_fn(stock_code, stock_name, tl_inds, "topline", top_k=8)
    bl_results = check_fn(stock_code, stock_name, bl_inds, "bottomline", top_k=8)

    tl_high, tl_low, tl_none, tl_total = print_results(tl_results, "topline", method)
    bl_high, bl_low, bl_none, bl_total = print_results(bl_results, "bottomline", method)

    total_all = tl_total + bl_total
    high_all = tl_high + bl_high
    low_all = tl_low + bl_low
    none_all = tl_none + bl_none

    print(f"\n{'='*60}")
    print(f"汇总 ({method})")
    print(f"{'='*60}")
    print(f"  总指标数:              {total_all}")
    print(f"  高置信度命中:          {high_all}/{total_all} = {high_all/total_all*100:.1f}%")
    print(f"  低置信度命中:          {low_all}/{total_all} = {low_all/total_all*100:.1f}%")
    print(f"  完全未命中:            {none_all}/{total_all} = {none_all/total_all*100:.1f}%")
    print(f"\n  => 内部Chunk数据能支撑Step2高置信度答案的比例: {high_all/total_all*100:.1f}%")


def _default_luoyang_indicators():
    """603993洛阳钼业标准指标集（无法从DB拿Step1时的兜底）"""
    tl = [
        {"name": "铜产量/出货量", "category": "volume", "current_status": "刚果金TFM矿扩产", "data_available": False},
        {"name": "钴产量/出货量", "category": "volume", "current_status": "刚果金钴产量", "data_available": False},
        {"name": "钼产量/出货量", "category": "volume", "current_status": "洛阳钼矿产量", "data_available": False},
        {"name": "铜金属价格", "category": "price", "current_status": "LME铜价", "data_available": False},
        {"name": "钴金属价格", "category": "price", "current_status": "MB钴价", "data_available": False},
        {"name": "钼金属价格", "category": "price", "current_status": "国内钼铁价格", "data_available": False},
        {"name": "铌产量/出货量", "category": "volume", "current_status": "巴西铌铁产量", "data_available": False},
        {"name": "磷化工产品销量", "category": "volume", "current_status": "磷肥出货量", "data_available": False},
        {"name": "新能源车钴需求", "category": "market_share", "current_status": "锂电池钴需求量", "data_available": False},
        {"name": "公司铜市场份额", "category": "market_share", "current_status": "全球铜矿产量排名", "data_available": False},
    ]
    bl = [
        {"name": "刚果金矿业税/特许权使用费", "category": "原材料", "current_status": "刚果金税率政策", "data_available": False},
        {"name": "刚果金工业电价", "category": "能源", "current_status": "矿山电力成本", "data_available": False},
        {"name": "铜精矿加工费TC/RC", "category": "原材料", "current_status": "TC费率下行", "data_available": False},
        {"name": "采矿直接人工成本", "category": "人工", "current_status": "刚果金员工工资", "data_available": False},
        {"name": "矿山AISC综合维持成本", "category": "原材料", "current_status": "AISC美元/磅", "data_available": False},
        {"name": "汇率（人民币/美元）", "category": "其他", "current_status": "RMB/USD汇率", "data_available": False},
        {"name": "运输/物流成本", "category": "其他", "current_status": "刚果金陆路运输", "data_available": False},
        {"name": "选矿产率/回收率", "category": "原材料", "current_status": "TFM铜钴回收率", "data_available": False},
    ]
    return tl, bl


def main():
    parser = argparse.ArgumentParser(description="Step1→Step2 内部chunk打捞覆盖率测试")
    parser.add_argument("--code", default="603993", help="股票代码")
    parser.add_argument("--name", default="洛阳钼业", help="股票名称")
    parser.add_argument("--step1", default=None, help="Step1结果JSON文件路径（可选）")
    parser.add_argument("--milvus", action="store_true", help="使用Milvus检索（默认用fallback）")
    args = parser.parse_args()

    run_test(args.code, args.name, args.step1, args.milvus)


if __name__ == "__main__":
    main()

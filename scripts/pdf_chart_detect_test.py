"""PDF 图表检测探测脚本 — 分析每页的文字量、矢量元素数量、嵌入图片数"""
import sys
import pdfplumber

def analyze_pdf(pdf_path: str):
    print(f"\n{'='*80}")
    print(f"文件: {pdf_path}")
    print(f"{'='*80}")

    with pdfplumber.open(pdf_path) as pdf:
        page_count = len(pdf.pages)
        print(f"总页数: {page_count}\n")

        char_counts = []

        print(f"{'页码':>4} | {'字数':>6} | {'rects':>5} | {'lines':>5} | {'curves':>6} | {'images':>6} | {'矢量合计':>8} | 判断")
        print("-" * 80)

        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            char_count = len(text.strip())
            char_counts.append(char_count)

            n_rects = len(page.rects or [])
            n_lines = len(page.lines or [])
            n_curves = len(page.curves or [])
            n_images = len(page.images or [])
            n_vectors = n_rects + n_lines + n_curves

            print(f"{i+1:>4} | {char_count:>6} | {n_rects:>5} | {n_lines:>5} | {n_curves:>6} | {n_images:>6} | {n_vectors:>8} | ", end="")

            # 简单标注
            tags = []
            if i < 2:
                tags.append("封面/目录?")
            if n_images > 0:
                tags.append(f"嵌入图{n_images}张")
            if n_vectors > 20:
                tags.append("矢量元素多")
            if char_count < 200:
                tags.append("字极少")

            print(", ".join(tags) if tags else "-")

        # 统计
        if char_counts:
            import statistics
            median_chars = statistics.median(char_counts)
            print(f"\n--- 统计 ---")
            print(f"每页字数: min={min(char_counts)}, max={max(char_counts)}, "
                  f"median={median_chars:.0f}, mean={sum(char_counts)/len(char_counts):.0f}")

            print(f"\n--- 用 '字数 < median*0.6 且 矢量>20 且 非前2页' 规则检测 ---")
            threshold = median_chars * 0.6
            print(f"字数阈值: {threshold:.0f} (median {median_chars:.0f} × 0.6)")
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                char_count = len(text.strip())
                n_vectors = len(page.rects or []) + len(page.lines or []) + len(page.curves or [])
                n_images = len(page.images or [])

                hit = (char_count < threshold and n_vectors > 20 and i >= 2)
                has_embedded_img = n_images > 0 and i >= 2

                if hit or has_embedded_img:
                    reason = []
                    if hit:
                        reason.append(f"矢量图表(字{char_count}<{threshold:.0f}, 矢量{n_vectors})")
                    if has_embedded_img:
                        reason.append(f"嵌入图片({n_images}张)")
                    print(f"  → 第{i+1}页: {', '.join(reason)}")


if __name__ == "__main__":
    pdfs = sys.argv[1:] or []
    if not pdfs:
        print("用法: python pdf_chart_detect_test.py <pdf1> [pdf2] ...")
        sys.exit(1)
    for p in pdfs:
        analyze_pdf(p)

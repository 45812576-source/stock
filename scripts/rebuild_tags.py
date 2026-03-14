"""一次性脚本：从 structured_json 重新提取 tags_json，过滤泛标签"""
import json
import sys
sys.path.insert(0, ".")

from utils.db_utils import execute_query, execute_insert

# 与 claude_processor._is_generic_tag 保持一致
_GENERIC = {
    "涨价", "降价", "涨停", "跌停", "上涨", "下跌", "反弹", "回调",
    "放量", "缩量", "异动", "拉升", "跳水", "分化", "调整", "震荡", "突破",
    "利好", "利空", "机会", "风险", "热点", "题材", "概念", "板块",
    "龙头", "强势", "弱势", "趋势", "走势",
    "资金", "流入", "流出", "加仓", "减仓", "买入", "卖出", "持仓", "配置",
    "业绩", "增长", "下滑", "超预期", "不及预期", "景气", "复苏", "回暖",
    "改革", "创新", "转型", "升级", "发展", "推进", "落地", "出台",
    "市场", "政策", "技术", "重组", "融资", "其他",
    "科技创新", "消费升级", "产业升级", "政策红利", "周期复苏",
    "策略", "综合", "固收", "基金",
}

def _is_generic_tag(tag):
    if not tag or len(tag) <= 1:
        return True
    return tag in _GENERIC


def rebuild_tags():
    batch_size = 500
    offset = 0
    total_updated = 0

    while True:
        rows = execute_query(
            "SELECT id, structured_json FROM cleaned_items WHERE structured_json IS NOT NULL ORDER BY id LIMIT %s OFFSET %s",
            [batch_size, offset],
        )
        if not rows:
            break

        for row in rows:
            cid = row["id"]
            raw = row["structured_json"]
            if not raw:
                continue
            try:
                result = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue

            tags_obj = result.get("tags", {})
            if not tags_obj or not isinstance(tags_obj, dict):
                continue

            tag_list = []
            for k in ["sw_industry_l1", "sw_industry_l2"]:
                v = tags_obj.get(k)
                if v and v != "null":
                    tag_list.append(v)
            for st in tags_obj.get("sub_theme", []) or []:
                if st and not _is_generic_tag(st):
                    tag_list.append(st)

            new_tags_json = json.dumps(tag_list, ensure_ascii=False)

            execute_insert(
                "UPDATE cleaned_items SET tags_json=%s WHERE id=%s",
                [new_tags_json, cid],
            )
            total_updated += 1

        print(f"  已处理 {offset + len(rows)} 条，更新 {total_updated} 条")
        offset += batch_size

    print(f"\n完成，共更新 {total_updated} 条 tags_json")

    # 同步清理 dashboard_tag_frequency 中的泛标签
    print("\n清理 dashboard_tag_frequency 中的泛标签...")
    all_tags = execute_query("SELECT DISTINCT tag_name FROM dashboard_tag_frequency")
    removed = 0
    for r in (all_tags or []):
        tag = r["tag_name"]
        if _is_generic_tag(tag):
            execute_insert("DELETE FROM dashboard_tag_frequency WHERE tag_name=%s", [tag])
            print(f"  删除: {tag}")
            removed += 1
    print(f"清理完成，删除 {removed} 个泛标签")


if __name__ == "__main__":
    rebuild_tags()

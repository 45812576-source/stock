"""报告生成器 — 研究报告查询 + 格式化输出"""
import json
import re
from utils.db_utils import execute_query, execute_insert


# ── 读取时后处理：修正 value_chain ──────────────────────────

_TOPLINE_KW = {"价格", "售价", "销售", "需求", "销量", "市场份额", "规模",
               "出货", "订单", "收入", "贸易", "库存", "供需", "供给",
               "产量", "配额", "金属价格", "铜价", "铝价", "镍价", "锡价",
               "储备", "下游需求", "上游供给"}
_BOTTOMLINE_KW = {"成本", "费用", "原材料", "能源", "电价", "运费", "折旧",
                  "人工", "汇率", "利率", "税", "摊销", "环保", "利润",
                  "成本结构", "冶炼成本"}

_UNIT_HINTS = [
    (re.compile(r"CAPEX|资本开支|投资额|市值|规模|产值"), "亿美元"),
    (re.compile(r"产量|需求|供应|供给|缺口|库存|出口|进口|配额|产能"), "万吨"),
    (re.compile(r"价格|均价|完全成本|单价|售价|成本"), "美元/吨"),
    (re.compile(r"CAGR|增速|增长率|占比|比例|利润率|毛利率|份额"), "%"),
]


def _fuzzy_match(name, driver_set):
    clean = re.sub(r'[（(][^)）]*[)）]', '', name).strip()
    for dn in driver_set:
        dn_c = re.sub(r'[（(][^)）]*[)）]', '', dn).strip()
        if clean in dn_c or dn_c in clean:
            return True
        for kw in re.split(r'[、/·，,\s]+', clean):
            if len(kw) >= 2 and kw in dn_c:
                return True
    return False


def _classify(names, tl_drivers, bl_drivers):
    if isinstance(names, str):
        names = [names] if names else []
    if not names:
        return None
    # 精确匹配 bm driver
    hit_tl = any(_fuzzy_match(n, tl_drivers) for n in names)
    hit_bl = any(_fuzzy_match(n, bl_drivers) for n in names)
    if hit_tl and hit_bl:
        return "both"
    if hit_tl:
        return "topline"
    if hit_bl:
        return "bottomline"
    # 通用关键词
    text = " ".join(names)
    kw_tl = any(k in text for k in _TOPLINE_KW)
    kw_bl = any(k in text for k in _BOTTOMLINE_KW)
    if kw_tl and kw_bl:
        return "both"
    if kw_tl:
        return "topline"
    if kw_bl:
        return "bottomline"
    return None


def _fixup_value_chain(vc, bm):
    """读取时后处理 value_chain：修正 line_type + 补单位"""
    if isinstance(vc, str):
        try:
            vc = json.loads(vc)
        except Exception:
            return vc
    if not isinstance(vc, dict):
        return vc
    if isinstance(bm, str):
        try:
            bm = json.loads(bm)
        except Exception:
            bm = {}

    tl = {d.get("name", "") for d in (bm.get("topline_drivers") or []) if d.get("name")}
    bl = {d.get("name", "") for d in (bm.get("bottomline_drivers") or []) if d.get("name")}

    # 修正 industry_data line_type + 补单位
    for d in (vc.get("industry_data") or []):
        lt = _classify(d.get("related_driver", ""), tl, bl)
        if lt:
            d["line_type"] = lt
        elif d.get("line_type") not in ("topline", "bottomline"):
            lt2 = _classify(d.get("metric", ""), tl, bl)
            if lt2:
                d["line_type"] = lt2
        # 补单位
        val = str(d.get("value", ""))
        stripped = re.sub(r'^[约~><=≈不足超]+', '', val.strip())
        if stripped and re.match(r'^[\d,.–-]+$', stripped):
            metric = d.get("metric", "")
            for pat, unit in _UNIT_HINTS:
                if pat.search(metric):
                    d["value"] = val + unit
                    break
            else:
                d["value"] = val + " (缺单位)"

    # 修正 industry_news line_type
    # AI 判定为 context 的不覆盖（宏观背景不应被关键词硬改）
    for n in (vc.get("industry_news") or []):
        if n.get("line_type") == "context":
            continue
        tagged = n.get("tagged_drivers") or []
        lt = _classify(tagged, tl, bl)
        if lt:
            n["line_type"] = lt

    return vc


def get_research_report(research_id):
    """获取研究报告完整内容"""
    rows = execute_query("SELECT * FROM deep_research WHERE id=?", [research_id])
    if not rows:
        return None
    r = rows[0]
    report_data = json.loads(r["report_json"]) if r.get("report_json") else {}

    # 个股研究：新6板块结构，只用 overall_score
    if r["research_type"] == "stock":
        report_body = report_data.get("report", report_data) if isinstance(report_data, dict) else report_data
        # 读取时后处理：修正 value_chain 的 line_type 和 value 单位
        if isinstance(report_body, dict) and report_body.get("value_chain") and report_body.get("business_model"):
            try:
                report_body["value_chain"] = _fixup_value_chain(
                    report_body["value_chain"], report_body["business_model"]
                )
            except Exception:
                pass
        return {
            "id": r["id"],
            "type": r["research_type"],
            "target": r["target"],
            "date": r["research_date"],
            "scores": {
                "overall": r.get("overall_score"),
            },
            "recommendation": r.get("recommendation"),
            "report": report_body,
        }
    else:
        # 行业/宏观研究
        return {
            "id": r["id"],
            "type": r["research_type"],
            "target": r["target"],
            "date": r["research_date"],
            "scores": {"overall": r.get("overall_score")},
            "recommendation": r.get("recommendation"),
            "report": report_data.get("report", report_data),
            "extra": {k: v for k, v in report_data.items() if k != "report"},
        }


def list_research_records(research_type=None, limit=20):
    """列出研究记录"""
    sql = """SELECT id, research_type, target, research_date,
                    overall_score, recommendation, created_at
             FROM deep_research"""
    params = []
    if research_type:
        sql += " WHERE research_type=?"
        params.append(research_type)
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    return execute_query(sql, params)


def get_research_stats():
    """获取研究统计"""
    total = execute_query("SELECT COUNT(*) as cnt FROM deep_research")
    by_type = execute_query(
        "SELECT research_type, COUNT(*) as cnt FROM deep_research GROUP BY research_type"
    )
    by_rec = execute_query(
        """SELECT recommendation, COUNT(*) as cnt FROM deep_research
           WHERE recommendation IS NOT NULL GROUP BY recommendation"""
    )
    avg_score = execute_query(
        "SELECT AVG(overall_score) as avg_score FROM deep_research WHERE overall_score IS NOT NULL"
    )
    return {
        "total": total[0]["cnt"] if total else 0,
        "by_type": {r["research_type"]: r["cnt"] for r in by_type},
        "by_recommendation": {r["recommendation"]: r["cnt"] for r in by_rec},
        "avg_score": round(avg_score[0]["avg_score"], 1) if avg_score and avg_score[0]["avg_score"] else 0,
    }

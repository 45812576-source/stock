"""报告生成器 — 研究报告查询 + 格式化输出"""
import json
from utils.db_utils import execute_query, execute_insert


def get_research_report(research_id):
    """获取研究报告完整内容"""
    rows = execute_query("SELECT * FROM deep_research WHERE id=?", [research_id])
    if not rows:
        return None
    r = rows[0]
    report_data = json.loads(r["report_json"]) if r.get("report_json") else {}

    # 个股研究有6维评分
    if r["research_type"] == "stock":
        return {
            "id": r["id"],
            "type": r["research_type"],
            "target": r["target"],
            "date": r["research_date"],
            "scores": {
                "financial": r.get("financial_score"),
                "valuation": r.get("valuation_score"),
                "technical": r.get("technical_score"),
                "sentiment": r.get("sentiment_score"),
                "catalyst": r.get("catalyst_score"),
                "risk": r.get("risk_score"),
                "overall": r.get("overall_score"),
            },
            "recommendation": r.get("recommendation"),
            "report": report_data.get("report", report_data),
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

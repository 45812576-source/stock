"""榜单1：宏观利好利空"""
from utils.db_utils import execute_query


def generate_macro_highlights(date_str):
    """生成宏观利好利空榜单"""
    positives = execute_query(
        """SELECT ci.id, ci.summary, ci.importance, ci.impact_analysis, ci.tags_json
           FROM cleaned_items ci
           WHERE ci.event_type='macro_policy' AND ci.sentiment='positive'
           AND date(ci.cleaned_at)=?
           ORDER BY ci.importance DESC LIMIT 10""",
        [date_str],
    )
    negatives = execute_query(
        """SELECT ci.id, ci.summary, ci.importance, ci.impact_analysis, ci.tags_json
           FROM cleaned_items ci
           WHERE ci.event_type='macro_policy' AND ci.sentiment='negative'
           AND date(ci.cleaned_at)=?
           ORDER BY ci.importance DESC LIMIT 10""",
        [date_str],
    )
    return {"positives": positives, "negatives": negatives}

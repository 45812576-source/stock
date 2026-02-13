"""榜单2：行业重大利好Top10"""
from utils.db_utils import execute_query


def generate_industry_news(date_str):
    """生成行业重大利好榜单"""
    return execute_query(
        """SELECT ii.industry_name, ci.summary, ci.importance, ci.tags_json
           FROM item_industries ii
           JOIN cleaned_items ci ON ii.cleaned_item_id=ci.id
           WHERE ci.sentiment='positive' AND date(ci.cleaned_at)=?
           ORDER BY ci.importance DESC LIMIT 10""",
        [date_str],
    )

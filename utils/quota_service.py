"""配额服务 - 基于角色的权限和配额管理"""
from datetime import datetime, date
from typing import Optional, Tuple
from utils.db_utils import execute_query, execute_insert


# 角色默认配额配置
ROLE_QUOTAS = {
    'super_admin': {
        'portfolio_limit': 999,
        'ai_chat_monthly_limit': 999999,
        'tag_group_limit': 999999,
        'research_limit': 999999,
        'hotspot_access': True,
        'chart_analysis': True,
    },
    'data_admin': {
        'portfolio_limit': 10,
        'ai_chat_monthly_limit': 1000,
        'tag_group_limit': 100,
        'research_limit': 50,
        'hotspot_access': True,
        'chart_analysis': True,
    },
    'free_user': {
        'portfolio_limit': 2,
        'ai_chat_monthly_limit': 20,
        'tag_group_limit': 0,
        'research_limit': 0,
        'hotspot_access': False,
        'chart_analysis': False,
    },
    'subscriber': {
        'portfolio_limit': 999,
        'ai_chat_monthly_limit': 100,
        'tag_group_limit': 5,
        'research_limit': 10,
        'hotspot_access': True,
        'chart_analysis': True,
    },
}

# 积分消耗配置
POINTS_COST = {
    'tag_group': 10,          # 标签组成组
    'research': 30,           # 个股深度研究
    'chart_analysis': 20,     # K线阶段分析
    'ai_chat_bundle': 10,     # 10积分 = 50轮对话
}


def get_user_quota(user_id: int) -> dict:
    """获取用户配额信息"""
    result = execute_query("""
        SELECT role, portfolio_limit, ai_chat_monthly_limit, ai_chat_used,
               tag_group_limit, tag_group_used, research_limit, research_used,
               points_balance, last_reset_date, is_active
        FROM users WHERE id = %s
    """, (user_id,))

    if not result:
        return None

    user = result[0]

    # 检查是否需要重置月度配额
    today = date.today()
    last_reset = user.get('last_reset_date')
    if last_reset and last_reset < today.replace(day=1):
        # 新月份，重置配额
        execute_insert("""
            UPDATE users SET
                ai_chat_used = 0,
                tag_group_used = 0,
                research_used = 0,
                last_reset_date = %s
            WHERE id = %s
        """, (today, user_id))
        user['ai_chat_used'] = 0
        user['tag_group_used'] = 0
        user['research_used'] = 0
        user['last_reset_date'] = today

    role = user['role']
    quotas = ROLE_QUOTAS.get(role, ROLE_QUOTAS['free_user'])

    return {
        'role': role,
        'is_active': user['is_active'],
        **quotas,
        'ai_chat_used': user.get('ai_chat_used', 0),
        'tag_group_used': user.get('tag_group_used', 0),
        'research_used': user.get('research_used', 0),
        'points_balance': user.get('points_balance', 0),
    }


def check_quota(user_id: int, quota_type: str) -> Tuple[bool, str]:
    """检查配额是否足够

    Returns:
        (can_proceed, message)
    """
    quota = get_user_quota(user_id)
    if not quota:
        return False, "用户不存在"

    if not quota.get('is_active'):
        return False, "账号已被禁用"

    role = quota['role']

    # super_admin 和 data_admin 无限制
    if role in ['super_admin', 'data_admin']:
        return True, ""

    if quota_type == 'portfolio':
        # 检查portfolio数量（需要外部查询当前数量）
        return True, ""

    elif quota_type == 'ai_chat':
        used = quota.get('ai_chat_used', 0)
        limit = quota.get('ai_chat_monthly_limit', 0)
        if used >= limit:
            return False, f"AI对话次数已达上限({limit}/月)"
        return True, f"剩余{limit - used}次"

    elif quota_type == 'tag_group':
        if role == 'free_user':
            return False, "免费用户无标签组权限，请升级订阅"
        used = quota.get('tag_group_used', 0)
        limit = quota.get('tag_group_limit', 0)
        if used >= limit:
            # 尝试用积分
            if quota.get('points_balance', 0) >= POINTS_COST['tag_group']:
                return True, f"积分足够，可消耗{POINTS_COST['tag_group']}积分"
            return False, f"标签组次数已达上限({limit}/月)，可用积分兑换"
        return True, f"剩余{limit - used}次"

    elif quota_type == 'research':
        if role == 'free_user':
            return False, "免费用户无深度研究权限，请升级订阅"
        used = quota.get('research_used', 0)
        limit = quota.get('research_limit', 0)
        if used >= limit:
            if quota.get('points_balance', 0) >= POINTS_COST['research']:
                return True, f"积分足够，可消耗{POINTS_COST['research']}积分"
            return False, f"深度研究次数已达上限({limit}/月)，可用积分兑换"
        return True, f"剩余{limit - used}次"

    elif quota_type == 'chart_analysis':
        if role == 'free_user':
            return False, "免费用户无K线分析权限，请升级订阅"
        return True, ""

    elif quota_type == 'hotspot':
        if not quota.get('hotspot_access'):
            return False, "免费用户无热点发现权限，请升级订阅"
        return True, ""

    return True, ""


def consume_quota(user_id: int, quota_type: str, quantity: int = 1, points: int = 0) -> bool:
    """消耗配额

    Args:
        user_id: 用户ID
        quota_type: 配额类型 (ai_chat, tag_group, research, chart_analysis)
        quantity: 消耗数量
        points: 如果配额用完，尝试用积分抵扣的点数

    Returns:
        是否成功
    """
    quota = get_user_quota(user_id)
    if not quota or not quota.get('is_active'):
        return False

    role = quota['role']

    # super_admin 和 data_admin 不消耗配额
    if role in ['super_admin', 'data_admin']:
        # 仍然记录日志
        _log_usage(user_id, quota_type, quantity, 0)
        return True

    if quota_type == 'ai_chat':
        execute_insert("""
            UPDATE users SET ai_chat_used = ai_chat_used + %s WHERE id = %s
        """, (quantity, user_id))
        _log_usage(user_id, quota_type, quantity, 0)
        return True

    elif quota_type == 'tag_group':
        used = quota.get('tag_group_used', 0)
        limit = quota.get('tag_group_limit', 0)

        if used < limit:
            # 使用免费次数
            execute_insert("""
                UPDATE users SET tag_group_used = tag_group_used + %s WHERE id = %s
            """, (quantity, user_id))
            _log_usage(user_id, quota_type, quantity, 0)
            return True
        elif points > 0 or quota.get('points_balance', 0) >= POINTS_COST['tag_group']:
            # 使用积分
            cost = points or POINTS_COST['tag_group']
            execute_insert("""
                UPDATE users SET points_balance = points_balance - %s WHERE id = %s
            """, (cost, user_id))
            _log_usage(user_id, quota_type, quantity, cost)
            return True
        return False

    elif quota_type == 'research':
        used = quota.get('research_used', 0)
        limit = quota.get('research_limit', 0)

        if used < limit:
            execute_insert("""
                UPDATE users SET research_used = research_used + %s WHERE id = %s
            """, (quantity, user_id))
            _log_usage(user_id, quota_type, quantity, 0)
            return True
        elif points > 0 or quota.get('points_balance', 0) >= POINTS_COST['research']:
            cost = points or POINTS_COST['research']
            execute_insert("""
                UPDATE users SET points_balance = points_balance - %s WHERE id = %s
            """, (cost, user_id))
            _log_usage(user_id, quota_type, quantity, cost)
            return True
        return False

    elif quota_type == 'chart_analysis':
        # K线分析暂时不单独计数，随研究一起
        _log_usage(user_id, quota_type, quantity, 0)
        return True

    return True


def add_points(user_id: int, points: int, description: str = "积分充值") -> bool:
    """添加积分"""
    execute_insert("""
        UPDATE users SET
            points_balance = points_balance + %s,
            points_total = points_total + %s
        WHERE id = %s
    """, (points, points, user_id))

    # 记录日志
    execute_insert("""
        INSERT INTO user_usage_logs (user_id, usage_type, quantity, points_cost, description)
        VALUES (%s, 'points_purchase', 1, -%s, %s)
    """, (user_id, points, description))

    return True


def get_usage_logs(user_id: int, usage_type: str = None, limit: int = 50) -> list:
    """获取使用记录"""
    if usage_type:
        return execute_query("""
            SELECT * FROM user_usage_logs
            WHERE user_id = %s AND usage_type = %s
            ORDER BY created_at DESC LIMIT %s
        """, (user_id, usage_type, limit))
    return execute_query("""
        SELECT * FROM user_usage_logs
        WHERE user_id = %s
        ORDER BY created_at DESC LIMIT %s
    """, (user_id, limit))


def _log_usage(user_id: int, usage_type: str, quantity: int, points_cost: int):
    """记录使用日志"""
    execute_insert("""
        INSERT INTO user_usage_logs (user_id, usage_type, quantity, points_cost, description)
        VALUES (%s, %s, %s, %s, %s)
    """, (user_id, usage_type, quantity, points_cost, f"自动记录:{usage_type}"))


def get_portfolio_count(user_id: int) -> int:
    """获取用户的portfolio数量"""
    # 统计 watchlist_lists 中 list_type='theme' 的数量 + investment_strategies 数量
    result = execute_query("""
        SELECT (
            SELECT COUNT(*) FROM watchlist_lists WHERE user_id = %s AND list_type = 'theme'
        ) + (
            SELECT COUNT(*) FROM investment_strategies WHERE user_id = %s AND is_active = 1
        ) as cnt
    """, (user_id, user_id))
    return result[0]['cnt'] if result else 0


def check_portfolio_limit(user_id: int) -> Tuple[bool, str]:
    """检查portfolio数量限制"""
    quota = get_user_quota(user_id)
    if not quota:
        return False, "用户不存在"

    if not quota.get('is_active'):
        return False, "账号已被禁用"

    role = quota['role']
    if role in ['super_admin', 'data_admin']:
        return True, ""

    current = get_portfolio_count(user_id)
    limit = quota.get('portfolio_limit', 2)

    if current >= limit:
        return False, f"Portfolio数量已达上限({limit}个)"

    return True, f"还可创建{limit - current}个"

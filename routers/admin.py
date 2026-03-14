"""系统管理路由 - 用户管理/配额监控"""
from datetime import datetime
from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from utils.auth_deps import require_super_admin, TokenData
from utils.db_utils import execute_query, execute_insert

router = APIRouter(prefix="/admin", tags=["admin"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("", response_class=HTMLResponse)
def admin_page(request: Request, user: TokenData = Depends(require_super_admin)):
    """管理后台首页"""
    # 统计
    total_users = execute_query("SELECT COUNT(*) as cnt FROM users")
    total_users = total_users[0]['cnt'] if total_users else 0

    role_stats = execute_query("""
        SELECT role, COUNT(*) as cnt FROM users GROUP BY role
    """)
    role_distribution = {r['role']: r['cnt'] for r in role_stats}

    total_points = execute_query("SELECT SUM(points_balance) as total FROM users")
    total_points = total_points[0]['total'] or 0

    active_today = execute_query("""
        SELECT COUNT(DISTINCT user_id) as cnt FROM user_usage_logs
        WHERE DATE(created_at) = CURDATE()
    """)

    ctx = {
        "request": request,
        "active_page": "admin",
        "total_users": total_users,
        "role_distribution": role_distribution,
        "total_points": total_points,
        "active_today": active_today[0]['cnt'] if active_today else 0,
        "today": datetime.now().strftime("%Y-%m-%d"),
    }
    return templates.TemplateResponse("admin.html", ctx)


@router.get("/users", response_class=HTMLResponse)
def admin_users_page(request: Request, user: TokenData = Depends(require_super_admin)):
    """用户管理页面"""
    users = execute_query("""
        SELECT id, username, role, is_active,
               portfolio_limit, ai_chat_monthly_limit, ai_chat_used,
               tag_group_limit, tag_group_used, research_limit, research_used,
               points_balance, points_total, created_at
        FROM users ORDER BY id DESC LIMIT 100
    """)

    ctx = {
        "request": request,
        "active_page": "admin",
        "users": users,
    }
    return templates.TemplateResponse("admin_users.html", ctx)


@router.get("/packages", response_class=HTMLResponse)
def admin_packages_page(request: Request, user: TokenData = Depends(require_super_admin)):
    """积分包管理页面"""
    packages = execute_query("SELECT * FROM points_packages ORDER BY price")

    ctx = {
        "request": request,
        "active_page": "admin",
        "packages": packages,
    }
    return templates.TemplateResponse("admin_packages.html", ctx)

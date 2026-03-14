"""用户认证模块"""
import hashlib
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Cookie, Response, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from jose import JWTError, jwt
from pathlib import Path

from utils.db_utils import execute_query, execute_insert
from utils.auth_deps import get_current_user, TokenData, require_super_admin
from utils.quota_service import get_user_quota, ROLE_QUOTAS

# 模板
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))

router = APIRouter(prefix="/auth", tags=["认证"])

# JWT 配置
SECRET_KEY = "de0d5c7b8e4a9f2d6c8b1a4e7d9f3c5a8b2d6e4f7a9c3b5d8e2f6a4c7b9d1"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 24 * 7  # 7天过期

# Token 数据模型
class TokenData(BaseModel):
    user_id: int
    username: str
    role: str

# 请求模型
class LoginRequest(BaseModel):
    username: str
    password: str

class RegisterRequest(BaseModel):
    username: str
    password: str
    role: str = "free_user"

class UpdateUserRequest(BaseModel):
    role: str = None
    portfolio_limit: int = None
    ai_chat_monthly_limit: int = None
    tag_group_limit: int = None
    research_limit: int = None
    points_balance: int = None
    is_active: bool = None


def hash_password(password: str) -> str:
    """SHA256 哈希密码"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """验证密码"""
    return hash_password(plain_password) == hashed_password

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """创建 JWT token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def decode_token(token: str) -> Optional[TokenData]:
    """解码 token"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return TokenData(
            user_id=payload.get("user_id"),
            username=payload.get("username"),
            role=payload.get("role")
        )
    except JWTError:
        return None


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """登录页面"""
    return templates.TemplateResponse("login.html", {"request": request})


@router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    """注册页面"""
    return templates.TemplateResponse("register.html", {"request": request})


@router.get("/subscription", response_class=HTMLResponse)
async def subscription_page(request: Request):
    """订阅升级页面"""
    from utils.auth_deps import get_optional_user
    user = get_optional_user(request.cookies.get("access_token"))

    # 获取积分包
    packages = execute_query("""
        SELECT id, name, points, price, bonus_points, description
        FROM points_packages WHERE is_active = TRUE ORDER BY price
    """)

    ctx = {
        "request": request,
        "user": user,
        "packages": packages,
    }
    return templates.TemplateResponse("subscription.html", ctx)


@router.post("/login")
async def login(request: LoginRequest, response: Response):
    """登录接口"""
    # 查询用户
    result = execute_query(
        "SELECT id, username, password_hash, role, is_active FROM users WHERE username = %s",
        (request.username,)
    )

    if not result:
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    user = result[0]
    user_id, username, password_hash, role, is_active = user['id'], user['username'], user['password_hash'], user['role'], user.get('is_active', True)

    # 检查账号是否启用
    if is_active is False:
        raise HTTPException(status_code=403, detail="账号已被禁用，请联系管理员")

    # 验证密码
    if not verify_password(request.password, password_hash):
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    # 创建 token
    token_data = {"user_id": user_id, "username": username, "role": role}
    access_token = create_access_token(token_data)

    # 设置 cookie
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        max_age=60 * 60 * ACCESS_TOKEN_EXPIRE_HOURS,
        samesite="lax"
    )

    # 获取配额信息
    quota = get_user_quota(user_id)

    return {
        "success": True,
        "user": {"id": user_id, "username": username, "role": role},
        "quota": quota,
        "token": access_token
    }


@router.post("/logout")
async def logout(response: Response):
    """登出接口"""
    response.delete_cookie(key="access_token")
    return {"success": True}


@router.post("/register")
async def register(request: RegisterRequest):
    """注册接口 - 默认注册为 free_user"""
    # 检查用户名是否已存在
    result = execute_query(
        "SELECT id FROM users WHERE username = %s",
        (request.username,)
    )

    if result:
        raise HTTPException(status_code=400, detail="用户名已存在")

    # 验证角色
    valid_roles = ["free_user", "subscriber"]
    if request.role not in valid_roles:
        raise HTTPException(status_code=400, detail=f"无效的角色，仅支持: {', '.join(valid_roles)}")

    # 插入用户
    password_hash = hash_password(request.password)
    user_id = execute_insert(
        "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s)",
        (request.username, password_hash, request.role)
    )

    return {
        "success": True,
        "user": {"id": user_id, "username": request.username, "role": request.role}
    }


@router.get("/me")
async def get_current_user_info(user: TokenData = Depends(get_current_user)):
    """获取当前登录用户详细信息（包含配额）"""
    # 获取配额信息
    quota = get_user_quota(user.user_id)

    return {
        "user_id": user.user_id,
        "username": user.username,
        "role": user.role,
        "quota": quota
    }


@router.get("/quota")
async def get_quota(user: TokenData = Depends(get_current_user)):
    """获取当前用户配额"""
    quota = get_user_quota(user.user_id)
    return quota


# ========== 管理员接口 ==========

@router.get("/users")
async def list_users(
    page: int = 1,
    page_size: int = 20,
    user: TokenData = Depends(require_super_admin)
):
    """用户列表（仅超级管理员）"""
    offset = (page - 1) * page_size
    result = execute_query("""
        SELECT id, username, role, is_active,
               portfolio_limit, ai_chat_monthly_limit, ai_chat_used,
               tag_group_limit, tag_group_used, research_limit, research_used,
               points_balance, points_total, created_at
        FROM users
        ORDER BY id DESC
        LIMIT %s OFFSET %s
    """, (page_size, offset))

    total = execute_query("SELECT COUNT(*) as cnt FROM users")
    total = total[0]['cnt'] if total else 0

    return {
        "users": result,
        "total": total,
        "page": page,
        "page_size": page_size
    }


@router.get("/users/{user_id}")
async def get_user(user_id: int, user: TokenData = Depends(require_super_admin)):
    """获取用户详情"""
    result = execute_query("""
        SELECT id, username, role, is_active,
               portfolio_limit, ai_chat_monthly_limit, ai_chat_used,
               tag_group_limit, tag_group_used, research_limit, research_used,
               points_balance, points_total, created_at, last_reset_date
        FROM users WHERE id = %s
    """, (user_id,))

    if not result:
        raise HTTPException(status_code=404, detail="用户不存在")

    return result[0]


@router.put("/users/{user_id}")
async def update_user(
    user_id: int,
    request: UpdateUserRequest,
    admin: TokenData = Depends(require_super_admin)
):
    """更新用户（仅超级管理员）"""
    # 构建更新语句
    updates = []
    params = []

    if request.role is not None:
        if request.role not in ["super_admin", "data_admin", "free_user", "subscriber"]:
            raise HTTPException(status_code=400, detail="无效的角色")
        updates.append("role = %s")
        params.append(request.role)

    if request.portfolio_limit is not None:
        updates.append("portfolio_limit = %s")
        params.append(request.portfolio_limit)

    if request.ai_chat_monthly_limit is not None:
        updates.append("ai_chat_monthly_limit = %s")
        params.append(request.ai_chat_monthly_limit)

    if request.tag_group_limit is not None:
        updates.append("tag_group_limit = %s")
        params.append(request.tag_group_limit)

    if request.research_limit is not None:
        updates.append("research_limit = %s")
        params.append(request.research_limit)

    if request.points_balance is not None:
        updates.append("points_balance = points_balance + %s")
        params.append(request.points_balance)

    if request.is_active is not None:
        updates.append("is_active = %s")
        params.append(request.is_active)

    if not updates:
        raise HTTPException(status_code=400, detail="没有需要更新的字段")

    params.append(user_id)
    sql = f"UPDATE users SET {', '.join(updates)} WHERE id = %s"
    execute_insert(sql, tuple(params))

    return {"success": True}


@router.delete("/users/{user_id}")
async def delete_user(user_id: int, admin: TokenData = Depends(require_super_admin)):
    """删除用户（仅超级管理员）"""
    if user_id == admin.user_id:
        raise HTTPException(status_code=400, detail="不能删除自己")

    execute_insert("DELETE FROM users WHERE id = %s", (user_id,))
    return {"success": True}


@router.post("/users/{user_id}/reset-usage")
async def reset_usage(user_id: int, admin: TokenData = Depends(require_super_admin)):
    """重置用户使用量"""
    execute_insert("""
        UPDATE users SET
            ai_chat_used = 0,
            tag_group_used = 0,
            research_used = 0
        WHERE id = %s
    """, (user_id,))
    return {"success": True, "message": "使用量已重置"}


@router.get("/packages")
async def list_packages(user: TokenData = Depends(require_super_admin)):
    """积分包列表"""
    result = execute_query("SELECT * FROM points_packages WHERE is_active = TRUE ORDER BY price")
    return {"packages": result}


@router.post("/packages")
async def create_package(
    name: str,
    points: int,
    price: float,
    bonus_points: int = 0,
    description: str = "",
    admin: TokenData = Depends(require_super_admin)
):
    """创建积分包"""
    package_id = execute_insert("""
        INSERT INTO points_packages (name, points, price, bonus_points, description)
        VALUES (%s, %s, %s, %s, %s)
    """, (name, points, price, bonus_points, description))
    return {"success": True, "package_id": package_id}


@router.put("/packages/{package_id}")
async def update_package(
    package_id: int,
    name: str = None,
    points: int = None,
    price: float = None,
    bonus_points: int = None,
    is_active: bool = None,
    admin: TokenData = Depends(require_super_admin)
):
    """更新积分包"""
    updates = []
    params = []

    if name is not None:
        updates.append("name = %s")
        params.append(name)
    if points is not None:
        updates.append("points = %s")
        params.append(points)
    if price is not None:
        updates.append("price = %s")
        params.append(price)
    if bonus_points is not None:
        updates.append("bonus_points = %s")
        params.append(bonus_points)
    if is_active is not None:
        updates.append("is_active = %s")
        params.append(is_active)

    if not updates:
        raise HTTPException(status_code=400, detail="没有需要更新的字段")

    params.append(package_id)
    sql = f"UPDATE points_packages SET {', '.join(updates)} WHERE id = %s"
    execute_insert(sql, tuple(params))

    return {"success": True}


# ========== 订阅管理 ==========

@router.post("/subscription/upgrade")
async def upgrade_subscription(
    target_role: str,
    user: TokenData = Depends(get_current_user)
):
    """订阅升级（免费版 -> 订阅版）"""
    if target_role not in ["subscriber"]:
        raise HTTPException(status_code=400, detail="无效的目标角色")

    # 检查当前角色
    if user.role == "subscriber":
        return {"success": False, "message": "您已经是订阅用户"}

    if user.role not in ["free_user"]:
        raise HTTPException(status_code=400, detail="当前角色不支持升级")

    # 更新角色和配额
    execute_insert("""
        UPDATE users SET role = %s,
            portfolio_limit = 999,
            ai_chat_monthly_limit = 100,
            tag_group_limit = 5,
            research_limit = 10
        WHERE id = %s
    """, (target_role, user.user_id))

    return {
        "success": True,
        "message": f"已升级为{target_role}，请重新登录生效"
    }


@router.post("/subscription/downgrade")
async def downgrade_subscription(user: TokenData = Depends(get_current_user)):
    """订阅降级（订阅版 -> 免费版）"""
    if user.role != "subscriber":
        raise HTTPException(status_code=400, detail="当前不是订阅用户")

    # 降级为免费版
    execute_insert("""
        UPDATE users SET role = 'free_user',
            portfolio_limit = 2,
            ai_chat_monthly_limit = 20,
            tag_group_limit = 0,
            research_limit = 0
        WHERE id = %s
    """, (user.user_id,))

    return {
        "success": True,
        "message": "已降级为免费版，请重新登录生效"
    }


# ========== 积分购买 ==========

@router.get("/packages/public")
async def list_packages_public():
    """积分包列表（公开，用户可见）"""
    result = execute_query("""
        SELECT id, name, points, price, bonus_points, description
        FROM points_packages WHERE is_active = TRUE ORDER BY price
    """)
    return {"packages": result}


@router.post("/points/purchase")
async def purchase_points(
    package_id: int,
    user: TokenData = Depends(get_current_user)
):
    """购买积分包（模拟支付流程）"""
    # 获取套餐
    package = execute_query("""
        SELECT * FROM points_packages WHERE id = %s AND is_active = TRUE
    """, (package_id,))

    if not package:
        raise HTTPException(status_code=404, detail="积分包不存在")

    package = package[0]
    total_points = package['points'] + package['bonus_points']

    # 模拟支付成功，添加积分
    execute_insert("""
        UPDATE users SET
            points_balance = points_balance + %s,
            points_total = points_total + %s
        WHERE id = %s
    """, (total_points, total_points, user.user_id))

    # 记录日志
    execute_insert("""
        INSERT INTO user_usage_logs (user_id, usage_type, quantity, points_cost, description)
        VALUES (%s, 'points_purchase', %s, 0, %s)
    """, (user.user_id, total_points, f"购买积分包: {package['name']}"))

    return {
        "success": True,
        "message": f"购买成功，获得{total_points}积分",
        "points_added": total_points,
        "new_balance": execute_query("SELECT points_balance FROM users WHERE id = %s", (user.user_id,))[0]['points_balance']
    }


@router.get("/usage")
async def get_usage(
    usage_type: str = None,
    limit: int = 50,
    user: TokenData = Depends(get_current_user)
):
    """获取当前用户使用记录"""
    if usage_type:
        result = execute_query("""
            SELECT * FROM user_usage_logs
            WHERE user_id = %s AND usage_type = %s
            ORDER BY created_at DESC LIMIT %s
        """, (user.user_id, usage_type, limit))
    else:
        result = execute_query("""
            SELECT * FROM user_usage_logs
            WHERE user_id = %s
            ORDER BY created_at DESC LIMIT %s
        """, (user.user_id, limit))

    return {"usage": result}

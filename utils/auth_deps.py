"""认证依赖 - 用于保护需要登录的路由"""
from typing import Optional
from fastapi import Depends, HTTPException, Cookie, Request
from jose import JWTError, jwt
from pydantic import BaseModel


# JWT 配置（与 auth.py 保持一致）
SECRET_KEY = "de0d5c7b8e4a9f2d6c8b1a4e7d9f3c5a8b2d6e4f7a9c3b5d8e2f6a4c7b9d1"
ALGORITHM = "HS256"


class TokenData(BaseModel):
    user_id: int
    username: str
    role: str


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


def get_current_user(access_token: Optional[str] = Cookie(None)) -> TokenData:
    """获取当前登录用户 - 必须登录"""
    if not access_token:
        raise HTTPException(status_code=401, detail="请先登录", headers={"Location": "/auth/login"})

    token_data = decode_token(access_token)
    if not token_data:
        raise HTTPException(status_code=401, detail="登录已过期，请重新登录", headers={"Location": "/auth/login"})

    return token_data


def get_optional_user(access_token: Optional[str] = Cookie(None)) -> Optional[TokenData]:
    """获取当前登录用户 - 可选（未登录返回 None）"""
    if not access_token:
        return None

    return decode_token(access_token)


def require_role(*allowed_roles: str):
    """角色权限依赖 - 检查用户是否有指定角色"""
    def role_checker(user: TokenData = Depends(get_current_user)) -> TokenData:
        if user.role not in allowed_roles:
            raise HTTPException(
                status_code=403,
                detail=f"需要 {', '.join(allowed_roles)} 角色，当前角色: {user.role}"
            )
        return user
    return role_checker


def require_super_admin(user: TokenData = Depends(get_current_user)) -> TokenData:
    """超级管理员权限依赖"""
    if user.role != 'super_admin':
        raise HTTPException(status_code=403, detail="需要超级管理员权限")
    return user


def require_data_admin(user: TokenData = Depends(get_current_user)) -> TokenData:
    """数据管理员权限依赖"""
    if user.role not in ['super_admin', 'data_admin']:
        raise HTTPException(status_code=403, detail="需要数据管理员权限")
    return user


def require_annotator(user: TokenData = Depends(get_current_user)) -> TokenData:
    """标注权限（data_admin 或 super_admin）"""
    if user.role not in ['super_admin', 'data_admin']:
        raise HTTPException(status_code=403, detail="需要标注权限")
    return user


def require_subscription(user: TokenData = Depends(get_current_user)) -> TokenData:
    """订阅用户权限（subscriber 或更高）"""
    if user.role not in ['super_admin', 'data_admin', 'subscriber']:
        raise HTTPException(status_code=403, detail="需要订阅用户权限")
    return user


# 保留旧函数名以兼容现有代码
def require_admin(user: TokenData = Depends(get_current_user)) -> TokenData:
    """管理员权限依赖（兼容旧代码）"""
    if user.role not in ['super_admin', 'data_admin']:
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user

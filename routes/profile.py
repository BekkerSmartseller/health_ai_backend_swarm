# health_ai_backend_swarm/routes/profile.py
"""
Profile controller — эндпоинты профиля пользователя.
"""
import logging
from typing import Optional
from litestar import Controller, get, put, Request
from litestar.exceptions import HTTPException
from litestar.connection import ASGIConnection
from litestar.handlers.base import BaseRouteHandler
from pydantic import BaseModel


logger = logging.getLogger(__name__)


# Guard'ы (дублируются для избежания циклического импорта)
async def _get_user_from_token(connection: ASGIConnection):
    import jwt
    from config import config as cfg
    token = connection.cookies.get("token")
    if not token:
        return None
    try:
        payload = jwt.decode(token, cfg.JWT_SECRET, algorithms=["HS256"])
        user_id = payload.get("sub")
        if user_id:
            from services.user_db import get_user_by_id
            user = await get_user_by_id(user_id)
            if user and user.get("is_active"):
                return user
    except Exception:
        pass
    return None


async def require_auth(connection: ASGIConnection, handler: BaseRouteHandler):
    from litestar.exceptions import NotAuthorizedException
    user = await _get_user_from_token(connection)
    if not user:
        raise NotAuthorizedException("Authentication required")
    connection.scope["user"] = user


class ProfileUpdate(BaseModel):
    name: Optional[str] = None
    age: Optional[int] = None
    user_type: Optional[str] = None  # 'user' или 'doctor'


class ProfileController(Controller):
    path = "/profile"

    @get("/", guards=[require_auth])
    async def get_profile(self, request: Request) -> dict:
        user = request.user
        return {
            "id": user["id"],
            "email": user["email"],
            "name": user.get("name"),
            "age": user.get("age"),
            "user_type": user.get("user_type"),
            "is_admin": user.get("is_admin", False),
            "auth_provider": user.get("auth_provider"),
            "created_at": user.get("created_at"),
        }

    @put("/", guards=[require_auth])
    async def update_profile(self, request: Request, data: ProfileUpdate) -> dict:
        from services.user_db import get_user_by_id, update_user_profile_by_id
        
        # Получаем пользователя из токена
        token = request.cookies.get("token")
        if not token:
            raise HTTPException(status_code=401, detail="Not authenticated")
        import jwt
        from config import config
        try:
            payload = jwt.decode(token, config.JWT_SECRET, algorithms=["HS256"])
            user_id = payload.get("sub")
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid token")

        updated = await update_user_profile_by_id(
            user_id,
            name=data.name,
            age=data.age,
            user_type=data.user_type
        )
        if not updated:
            raise HTTPException(status_code=404, detail="User not found")
        return updated
# health_ai_backend_swarm/routes/admin_blog.py
"""
Admin blog controller — эндпоинты администрирования блога.
"""
import uuid
import logging
from pathlib import Path
from litestar import Controller, get, post, put, delete, Request
from litestar.exceptions import HTTPException
from litestar.datastructures import UploadFile
from litestar.connection import ASGIConnection
from litestar.handlers.base import BaseRouteHandler

from services.blog_db import get_all_posts, get_total_posts_count

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


async def require_admin(connection: ASGIConnection, handler: BaseRouteHandler):
    from litestar.exceptions import NotAuthorizedException
    user = await _get_user_from_token(connection)
    if not user or not user.get("is_admin"):
        raise NotAuthorizedException("Admin access required")
    connection.scope["user"] = user


class AdminBlogController(Controller):
    path = "/admin/blog"

    @get("/posts", guards=[require_admin])
    async def admin_list_posts(self, request: Request, page: int = 1, limit: int = 20) -> dict:
        offset = (page - 1) * limit
        posts = await get_all_posts(limit=limit, offset=offset)
        total = await get_total_posts_count()
        return {"posts": posts, "total": total, "page": page, "limit": limit}

    @post("/posts", guards=[require_admin])
    async def admin_create_post(self, request: Request, data: dict) -> dict:
        from services.blog_db import create_post
        try:
            post = await create_post(data)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {"success": True, "post": post}

    @put("/posts/{slug:str}", guards=[require_admin])
    async def admin_update_post(self, slug: str, data: dict) -> dict:
        from services.blog_db import update_post
        try:
            post = await update_post(slug, data)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        if not post:
            raise HTTPException(status_code=404, detail="Post not found")
        return {"success": True, "post": post}

    @delete("/posts/{slug:str}", status_code=200, guards=[require_admin])
    async def admin_delete_post(self, slug: str) -> dict:
        from services.blog_db import delete_post
        deleted = await delete_post(slug)
        if not deleted:
            raise HTTPException(status_code=404, detail="Post not found")
        return {"success": True}

    @post("/upload-image", guards=[require_admin])
    async def upload_image(self, request: Request, file: UploadFile) -> dict:
        if not file.content_type.startswith("image/"):
            raise HTTPException(status_code=400, detail="Only images allowed")
        ext = Path(file.filename).suffix
        new_name = f"{uuid.uuid4().hex}{ext}"
        upload_dir = Path("static/uploads/blog")
        upload_dir.mkdir(parents=True, exist_ok=True)
        file_path = upload_dir / new_name
        content = await file.read()
        file_path.write_bytes(content)
        url = f"/uploads/blog/{new_name}"
        return {"url": url}
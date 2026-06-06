# health_ai_backend_swarm/routes/blog.py
"""
Blog controller — все публичные эндпоинты блога, лайков, комментариев.
"""
import json
import markdown
import bleach
import uuid
import logging
from pathlib import Path
from typing import List, Optional
from bs4 import BeautifulSoup
from litestar import Controller, get, post, put, delete, Request
from litestar.exceptions import HTTPException, NotAuthorizedException
from litestar.datastructures import UploadFile
from litestar.connection import ASGIConnection
from litestar.handlers.base import BaseRouteHandler

from services.blog_db import (
    get_all_posts,
    get_post_by_slug,
    get_all_tags,
    get_total_posts_count,
)
from services.user_db import (
    get_comments, create_comment,
    add_like, remove_like, get_like_count, get_comments_count,
    get_user_like, get_user_comment_reaction, set_comment_reaction,
    remove_comment_reaction as db_remove_comment_reaction,
)

logger = logging.getLogger(__name__)

# Глобальный пул БД (устанавливается из main.py после инициализации)
BLOG_PG_POOL = None

# Guard'ы
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
    user = await _get_user_from_token(connection)
    if not user:
        raise NotAuthorizedException("Authentication required")
    connection.scope["user"] = user


async def require_admin(connection: ASGIConnection, handler: BaseRouteHandler):
    user = await _get_user_from_token(connection)
    if not user or not user.get("is_admin"):
        raise NotAuthorizedException("Admin access required")
    connection.scope["user"] = user

# JSON fallback
BLOG_DATA_FILE = Path(__file__).parent.parent / "blog_data" / "blog_posts.json"


def load_blog_posts() -> List[dict]:
    with open(BLOG_DATA_FILE, "r", encoding="utf-8") as f:
        posts = json.load(f)
    for post in posts:
        if "content_html" not in post and "content_markdown" in post:
            raw_html = markdown.markdown(
                post["content_markdown"],
                extensions=["extra", "codehilite"]
            )
            post["content_html"] = bleach.clean(
                raw_html,
                tags=['p', 'br', 'strong', 'em', 'u', 'a', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
                       'ul', 'ol', 'li', 'pre', 'code', 'blockquote', 'table', 'thead', 'tbody',
                       'tr', 'th', 'td', 'hr', 'img', 'div', 'span'],
                attributes={
                    'a': ['href', 'title', 'target', 'rel'],
                    'img': ['src', 'alt', 'title', 'width', 'height'],
                    'td': ['colspan', 'rowspan'],
                    'th': ['colspan', 'rowspan'],
                    'div': ['class'],
                    'span': ['class'],
                    'table': ['class'],
                },
                strip=True
            )
        if "content_html" not in post:
            post["content_html"] = "<p>Содержимое статьи временно недоступно.</p>"
        if not post.get("excerpt") and "content_html" in post:
            soup = BeautifulSoup(post["content_html"], "html.parser")
            text = soup.get_text()
            post["excerpt"] = text[:160] + ("..." if len(text) > 160 else "")
        if not post.get("reading_time") and "content_markdown" in post:
            word_count = len(post["content_markdown"].split())
            post["reading_time"] = max(1, round(word_count / 200))
    return posts


async def _get_user_from_request(request: Request, get_user_by_id):
    import jwt
    from config import config
    token = request.cookies.get("token")
    if not token:
        return None
    try:
        payload = jwt.decode(token, config.JWT_SECRET, algorithms=["HS256"])
        user_id = payload.get("sub")
        if user_id:
            user = await get_user_by_id(user_id)
            if user and user.get("is_active"):
                return user
    except Exception:
        pass
    return None


# ==================== ПУБЛИЧНЫЙ КОНТРОЛЛЕР БЛОГА ====================

class BlogController(Controller):
    path = "/blog"

    @get("/posts")
    async def blog_posts(
        self, request: Request, page: int = 1, limit: int = 10, tag: Optional[str] = None
    ) -> dict:
        if BLOG_PG_POOL is None:
            all_posts = load_blog_posts()
            if tag:
                filtered = [p for p in all_posts if tag in p.get("tags", [])]
            else:
                filtered = all_posts
            total = len(filtered)
            total_pages = (total + limit - 1) // limit if limit > 0 else 1
            start = (page - 1) * limit
            end = start + limit
            paginated = filtered[start:end]
            for p in paginated:
                p.pop("content_markdown", None)
                p.pop("content_html", None)
            return {"posts": paginated, "total": total, "page": page, "limit": limit, "total_pages": total_pages}
        offset = (page - 1) * limit
        posts = await get_all_posts(limit=limit, offset=offset, tag=tag)
        total = await get_total_posts_count(tag=tag)
        total_pages = (total + limit - 1) // limit
        for p in posts:
            p.pop("content_markdown", None)
            p.pop("content_html", None)
        return {"posts": posts, "total": total, "page": page, "limit": limit, "total_pages": total_pages}

    @get("/posts/{slug:str}")
    async def blog_post(self, request: Request, slug: str) -> dict:
        from services.user_db import get_user_by_id
        post = await get_post_by_slug(slug)
        if not post:
            raise HTTPException(status_code=404, detail="Post not found")
        post.pop("content_html", None)
        if not post.get("content_markdown"):
            post["content_markdown"] = "Контент временно недоступен"
        post["related_posts"] = []
        post["likes_count"] = await get_like_count(slug)
        post["comments_count"] = await get_comments_count(slug)
        user = await _get_user_from_request(request, get_user_by_id)
        post["user_liked"] = await get_user_like(user["id"], slug) if user else False
        return post

    @get("/tags")
    async def blog_tags(self, request: Request) -> List[dict]:
        return await get_all_tags()

    # ==================== КОММЕНТАРИИ ====================

    @get("/{slug:str}/comments")
    async def get_post_comments(self, request: Request, slug: str, limit: int = 50, offset: int = 0) -> list:
        from services.user_db import get_user_by_id
        user = await _get_user_from_request(request, get_user_by_id)
        comments = await get_comments(slug, limit, offset)
        if user:
            for c in comments:
                reaction = await get_user_comment_reaction(user["id"], c["id"])
                c["user_reaction"] = reaction
        return comments

    @post("/{slug:str}/comments", guards=[require_auth])
    async def add_post_comment(self, request: Request, slug: str, data: dict) -> dict:
        user = request.user
        content = data.get("content")
        if not content:
            raise HTTPException(status_code=400, detail="Content required")
        parent_id = data.get("parent_id")
        comment = await create_comment(user["id"], slug, content, parent_id)
        return comment

    # ==================== ЛАЙКИ ====================

    @post("/{slug:str}/like", guards=[require_auth])
    async def like_post(self, request: Request, slug: str) -> dict:
        user = request.user
        await add_like(user["id"], slug)
        likes_count = await get_like_count(slug)
        return {"likes_count": likes_count}

    @delete("/{slug:str}/like", guards=[require_auth], status_code=200)
    async def unlike_post(self, request: Request, slug: str) -> dict:
        user = request.user
        await remove_like(user["id"], slug)
        likes_count = await get_like_count(slug)
        return {"likes_count": likes_count}

    @get("/{slug:str}/like-count")
    async def get_like_counts(self, slug: str) -> dict:
        return {"count": await get_like_count(slug)}

    @get("/{slug:str}/user-like")
    async def get_user_like_endpoint(self, request: Request, slug: str) -> dict:
        from services.user_db import get_user_by_id
        user = await _get_user_from_request(request, get_user_by_id)
        if not user:
            return {"liked": False}
        liked = await get_user_like(user["id"], slug)
        return {"liked": liked}

    # ==================== РЕАКЦИИ НА КОММЕНТАРИИ ====================

    @post("/comments/{comment_id:str}/like", guards=[require_auth])
    async def like_comment(self, request: Request, comment_id: str) -> dict:
        user = request.user
        counts = await set_comment_reaction(user["id"], comment_id, "like")
        return counts

    @post("/comments/{comment_id:str}/dislike", guards=[require_auth])
    async def dislike_comment(self, request: Request, comment_id: str) -> dict:
        user = request.user
        counts = await set_comment_reaction(user["id"], comment_id, "dislike")
        return counts

    @delete("/comments/{comment_id:str}/reaction", guards=[require_auth], status_code=200)
    async def remove_comment_reaction(self, request: Request, comment_id: str) -> dict:
        user = request.user
        counts = await db_remove_comment_reaction(user["id"], comment_id)
        return counts

# health_ai_backend_swarm/services/user_db.py
import uuid
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
import asyncpg
from config import config

logger = logging.getLogger(__name__)

_pool = None
SCHEMA = config.DB_SCHEMA

async def init_user_db(pool: asyncpg.Pool):
    global _pool
    _pool = pool
    async with pool.acquire() as conn:
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        # Пользователи
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email TEXT UNIQUE NOT NULL,
                name TEXT,
                age INTEGER,
                user_type TEXT CHECK (user_type IN ('user', 'doctor')) DEFAULT 'user',
                auth_provider TEXT NOT NULL CHECK (auth_provider IN ('email', 'google', 'yandex')),
                provider_id TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                is_admin BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now()
            )
        """)
        # Коды верификации (можно в Redis, но для простоты – таблица)
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.email_verification_codes (
                email TEXT PRIMARY KEY,
                code TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL
            )
        """)
        # Лайки
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.blog_likes (
                user_id UUID REFERENCES {SCHEMA}.users(id) ON DELETE CASCADE,
                post_slug TEXT REFERENCES {SCHEMA}.blog_posts(slug) ON DELETE CASCADE,
                created_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (user_id, post_slug)
            )
        """)
        # Комментарии
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.blog_comments (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                post_slug TEXT REFERENCES {SCHEMA}.blog_posts(slug) ON DELETE CASCADE,
                user_id UUID REFERENCES {SCHEMA}.users(id),
                parent_id UUID REFERENCES {SCHEMA}.blog_comments(id) ON DELETE CASCADE,
                content TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now()
            )
        """)
        # Отзывы
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.user_feedback (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID REFERENCES {SCHEMA}.users(id),
                rating INTEGER CHECK (rating >= 1 AND rating <= 5),
                comment TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            )
        """)
        # Лайки к коментариям
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.comment_reactions (
                user_id UUID REFERENCES {SCHEMA}.users(id) ON DELETE CASCADE,
                comment_id UUID REFERENCES {SCHEMA}.blog_comments(id) ON DELETE CASCADE,
                reaction_type TEXT CHECK (reaction_type IN ('like', 'dislike')) NOT NULL,
                created_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (user_id, comment_id)
            )
        """)
    logger.info("User tables ensured in schema medexpertai.")

# ---- CRUD пользователей ----
async def create_or_get_user(email: str, auth_provider: str, provider_id: str = None, name: str = None) -> Dict:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT * FROM {SCHEMA}.users WHERE email = $1", email)
        if row:
            return dict(row)
        # Создаём нового
        user_id = uuid.uuid4()
        await conn.execute(
            f"""
            INSERT INTO {SCHEMA}.users (id, email, name, auth_provider, provider_id)
            VALUES ($1, $2, $3, $4, $5)
            """,
            user_id, email, name, auth_provider, provider_id
        )
        row = await conn.fetchrow(f"SELECT * FROM {SCHEMA}.users WHERE id = $1", user_id)
        return dict(row)

async def get_user_by_id(user_id: str) -> Optional[Dict]:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT * FROM {SCHEMA}.users WHERE id = $1", user_id)
        return dict(row) if row else None

async def get_user_by_email(email: str) -> Optional[Dict]:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT * FROM {SCHEMA}.users WHERE email = $1", email)
        return dict(row) if row else None

async def update_user_profile(email: str, name: str = None, age: int = None, user_type: str = None) -> Optional[Dict]:
    async with _pool.acquire() as conn:
        updates = []
        params = []
        idx = 1
        if name is not None:
            updates.append(f"name = ${idx}")
            params.append(name)
            idx += 1
        if age is not None:
            updates.append(f"age = ${idx}")
            params.append(age)
            idx += 1
        if user_type is not None:
            updates.append(f"user_type = ${idx}")
            params.append(user_type)
            idx += 1

        if not updates:
            # Нечего обновлять – просто вернуть текущего пользователя
            user = await get_user_by_email(email)
            return user

        params.append(email)
        set_clause = ", ".join(updates)
        query = f"UPDATE {SCHEMA}.users SET {set_clause} WHERE email = ${idx} RETURNING *"
        row = await conn.fetchrow(query, *params)
        return dict(row) if row else None

async def set_user_admin(email: str, is_admin: bool) -> bool:
    async with _pool.acquire() as conn:
        res = await conn.execute(
            f"UPDATE {SCHEMA}.users SET is_admin = $1 WHERE email = $2",
            is_admin, email
        )
        return res == "UPDATE 1"

# ---- Коды верификации (если храним в БД, но лучше в Redis) ----
# Для простоты оставим в БД, но в auth.py будем использовать Redis (как в задании).
# Оставим заглушки, но фактически используем Redis напрямую.

# ---- Лайки ----
async def add_like(user_id: str, post_slug: str):
    async with _pool.acquire() as conn:
        await conn.execute(
            f"INSERT INTO {SCHEMA}.blog_likes (user_id, post_slug) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            user_id, post_slug
        )

async def remove_like(user_id: str, post_slug: str):
    async with _pool.acquire() as conn:
        await conn.execute(
            f"DELETE FROM {SCHEMA}.blog_likes WHERE user_id = $1 AND post_slug = $2",
            user_id, post_slug
        )

async def get_like_count(post_slug: str) -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT COUNT(*) FROM {SCHEMA}.blog_likes WHERE post_slug = $1",
            post_slug
        )
        return row[0] if row else 0

async def get_user_like(user_id: str, post_slug: str) -> bool:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT 1 FROM {SCHEMA}.blog_likes WHERE user_id = $1 AND post_slug = $2",
            user_id, post_slug
        )
        return row is not None

# ---- Комментарии ----
async def create_comment(user_id: str, post_slug: str, content: str, parent_id: str = None) -> Dict:
    comment_id = uuid.uuid4()
    async with _pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {SCHEMA}.blog_comments (id, user_id, post_slug, parent_id, content)
            VALUES ($1, $2, $3, $4, $5)
            """,
            comment_id, user_id, post_slug, parent_id, content
        )
        # Возвращаем созданный комментарий с именем пользователя
        row = await conn.fetchrow(
            f"""
            SELECT c.id, c.content, c.created_at, c.parent_id,
                   u.id as user_id, u.name as user_name
            FROM {SCHEMA}.blog_comments c
            JOIN {SCHEMA}.users u ON c.user_id = u.id
            WHERE c.id = $1
            """,
            comment_id
        )
    return dict(row)

async def get_comments(post_slug: str, limit: int = 50, offset: int = 0) -> List[Dict]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT c.id, c.content, c.created_at, c.parent_id,
                   u.id as user_id, u.name as user_name
            FROM {SCHEMA}.blog_comments c
            JOIN {SCHEMA}.users u ON c.user_id = u.id
            WHERE c.post_slug = $1
            ORDER BY c.created_at ASC
            LIMIT $2 OFFSET $3
            """,
            post_slug, limit, offset
        )
    comments = [dict(row) for row in rows]
    # Добавляем счётчики реакций для каждого комментария
    for c in comments:
        counts = await get_comment_reaction_counts(c["id"])
        c["likes"] = counts["likes"]
        c["dislikes"] = counts["dislikes"]
        c["user_reaction"] = None  # заполним позже, если передан user_id
    return comments

async def get_comments_count(post_slug: str) -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT COUNT(*) FROM {SCHEMA}.blog_comments WHERE post_slug = $1", post_slug)
        return row[0] if row else 0
    
# ---- Отзывы ----
async def save_feedback(user_id: str, rating: int, comment: str):
    async with _pool.acquire() as conn:
        await conn.execute(
            f"INSERT INTO {SCHEMA}.user_feedback (user_id, rating, comment) VALUES ($1, $2, $3)",
            user_id, rating, comment
        )

# ПРофиль пользователя
async def update_user_profile_by_id(user_id: str, name: str = None, age: int = None, user_type: str = None) -> Optional[Dict]:
    async with _pool.acquire() as conn:
        updates = []
        params = []
        idx = 1
        if name is not None:
            updates.append(f"name = ${idx}")
            params.append(name)
            idx += 1
        if age is not None:
            updates.append(f"age = ${idx}")
            params.append(age)
            idx += 1
        if user_type is not None:
            updates.append(f"user_type = ${idx}")
            params.append(user_type)
            idx += 1
        if not updates:
            user = await get_user_by_id(user_id)
            return user
        params.append(user_id)
        set_clause = ", ".join(updates)
        query = f"UPDATE {SCHEMA}.users SET {set_clause}, updated_at = now() WHERE id = ${idx} RETURNING *"
        row = await conn.fetchrow(query, *params)
        return dict(row) if row else None

# Функции для работы с реакциями комментариев
async def set_comment_reaction(user_id: str, comment_id: str, reaction: str) -> dict:
    """Устанавливает реакцию (like/dislike) на комментарий. Если реакция уже была, обновляет."""
    async with _pool.acquire() as conn:
        # Удаляем старую реакцию, если есть
        await conn.execute(
            f"DELETE FROM {SCHEMA}.comment_reactions WHERE user_id = $1 AND comment_id = $2",
            user_id, comment_id
        )
        # Вставляем новую
        await conn.execute(
            f"INSERT INTO {SCHEMA}.comment_reactions (user_id, comment_id, reaction_type) VALUES ($1, $2, $3)",
            user_id, comment_id, reaction
        )
        # Возвращаем обновлённые счётчики
        return await get_comment_reaction_counts(comment_id)

async def remove_comment_reaction(user_id: str, comment_id: str) -> dict:
    """Удаляет реакцию пользователя на комментарий."""
    async with _pool.acquire() as conn:
        await conn.execute(
            f"DELETE FROM {SCHEMA}.comment_reactions WHERE user_id = $1 AND comment_id = $2",
            user_id, comment_id
        )
        return await get_comment_reaction_counts(comment_id)

async def get_comment_reaction_counts(comment_id: str) -> dict:
    """Возвращает количество лайков и дизлайков для комментария."""
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT 
                COUNT(*) FILTER (WHERE reaction_type = 'like') AS likes,
                COUNT(*) FILTER (WHERE reaction_type = 'dislike') AS dislikes
            FROM {SCHEMA}.comment_reactions
            WHERE comment_id = $1
            """,
            comment_id
        )
        return {"likes": row["likes"] or 0, "dislikes": row["dislikes"] or 0}

async def get_user_comment_reaction(user_id: str, comment_id: str) -> Optional[str]:
    """Возвращает реакцию пользователя (like/dislike) или None."""
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT reaction_type FROM {SCHEMA}.comment_reactions WHERE user_id = $1 AND comment_id = $2",
            user_id, comment_id
        )
        return row["reaction_type"] if row else None
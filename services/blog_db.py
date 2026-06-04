# health_ai_backend_swarm/services/blog_db.py
import json
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import uuid4

import asyncpg
import markdown
from bs4 import BeautifulSoup

from config import config

logger = logging.getLogger(__name__)

# Глобальный пул соединений (инициализируется в lifespan)
_pool: Optional[asyncpg.Pool] = None
SCHEMA = config.DB_SCHEMA

async def init_db(pool: asyncpg.Pool):
    global _pool
    _pool = pool
    async with pool.acquire() as conn:
        # Создаём схему, если нет
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        # Таблица блога
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.blog_posts (
                id SERIAL PRIMARY KEY,
                slug TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                content_markdown TEXT NOT NULL,
                content_html TEXT NOT NULL,
                excerpt TEXT,
                featured_image TEXT,
                og_image TEXT,
                published_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                tags JSONB NOT NULL DEFAULT '[]'::jsonb,
                reading_time INTEGER NOT NULL
            )
        """)
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_blog_posts_slug ON {SCHEMA}.blog_posts(slug)")
        await conn.execute(f"CREATE INDEX IF NOT EXISTS idx_blog_posts_published_at ON {SCHEMA}.blog_posts(published_at DESC)")
    logger.info("Table blog_posts ensured in schema medexpertai.")


def _compute_reading_time(markdown_text: str) -> int:
    words = len(markdown_text.split())
    return max(1, round(words / 200))


def _generate_slug(title: str) -> str:
    """Генерирует slug из заголовка (транслитерация + нижний регистр)."""
    import re
    from unidecode import unidecode
    slug = unidecode(title).lower()
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    slug = slug.strip('-')
    return slug


async def get_all_posts(limit: int = 100, offset: int = 0, tag: Optional[str] = None) -> List[Dict]:
    async with _pool.acquire() as conn:
        if tag:
            rows = await conn.fetch(
                f"""
                SELECT p.id, p.slug, p.title, p.excerpt, p.featured_image, p.og_image,
                       p.published_at, p.updated_at, p.tags, p.reading_time,
                       COALESCE(l.likes_count, 0) AS likes_count,
                       COALESCE(c.comments_count, 0) AS comments_count
                FROM {SCHEMA}.blog_posts p
                LEFT JOIN (
                    SELECT post_slug, COUNT(*) as likes_count
                    FROM {SCHEMA}.blog_likes
                    GROUP BY post_slug
                ) l ON p.slug = l.post_slug
                LEFT JOIN (
                    SELECT post_slug, COUNT(*) as comments_count
                    FROM {SCHEMA}.blog_comments
                    GROUP BY post_slug
                ) c ON p.slug = c.post_slug
                WHERE p.tags ? $1
                ORDER BY p.published_at DESC
                LIMIT $2 OFFSET $3
                """,
                tag, limit, offset
            )
        else:
            rows = await conn.fetch(
                f"""
                SELECT p.id, p.slug, p.title, p.excerpt, p.featured_image, p.og_image,
                       p.published_at, p.updated_at, p.tags, p.reading_time,
                       COALESCE(l.likes_count, 0) AS likes_count,
                       COALESCE(c.comments_count, 0) AS comments_count
                FROM {SCHEMA}.blog_posts p
                LEFT JOIN (
                    SELECT post_slug, COUNT(*) as likes_count
                    FROM {SCHEMA}.blog_likes
                    GROUP BY post_slug
                ) l ON p.slug = l.post_slug
                LEFT JOIN (
                    SELECT post_slug, COUNT(*) as comments_count
                    FROM {SCHEMA}.blog_comments
                    GROUP BY post_slug
                ) c ON p.slug = c.post_slug
                ORDER BY p.published_at DESC
                LIMIT $1 OFFSET $2
                """,
                limit, offset
            )
    return [dict(row) for row in rows]


async def get_post_by_slug(slug: str) -> Optional[Dict]:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT * FROM {SCHEMA}.blog_posts WHERE slug = $1",
            slug
        )
    return dict(row) if row else None


async def create_post(data: Dict[str, Any]) -> Dict:
    """Создаёт новую статью. Ожидает поля: title, content_markdown, excerpt, featured_image, og_image, published_at, tags."""
    # Генерируем slug, если не передан
    slug = data.get('slug') or _generate_slug(data['title'])
    # Генерируем HTML из Markdown
    content_html = markdown.markdown(
        data['content_markdown'],
        extensions=['extra', 'codehilite']
    )
    # Вычисляем чтение
    reading_time = _compute_reading_time(data['content_markdown'])
    # Извлекаем excerpt, если не передан
    excerpt = data.get('excerpt')
    if not excerpt and content_html:
        soup = BeautifulSoup(content_html, 'html.parser')
        text = soup.get_text()
        excerpt = text[:160] + ('...' if len(text) > 160 else '')

    raw_published = data.get('published_at')
    if isinstance(raw_published, str):
        # Преобразуем ISO-строку с 'Z' в datetime
        raw_published = raw_published.replace('Z', '+00:00')
        published_dt = datetime.fromisoformat(raw_published)
    else:
        published_dt = raw_published or datetime.utcnow()    

    async with _pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {SCHEMA}.blog_posts
                (slug, title, content_markdown, content_html, excerpt,
                featured_image, og_image, published_at, updated_at, tags, reading_time)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9, $10)
                RETURNING *
                """,
                slug,
                data['title'],
                data['content_markdown'],
                content_html,
                excerpt,
                data.get('featured_image', ''),
                data.get('og_image', ''),
                published_dt,
                data.get('tags', []),
                reading_time
            )
        except asyncpg.UniqueViolationError:
            # 🔥 Ловим ошибку дубликата и кидаем ValueError
            raise ValueError(f"Статья с таким URL (slug: '{slug}') уже существует. Измените заголовок или укажите другой slug.")
        except asyncpg.PostgresError as e:
            # Ловим любые другие ошибки БД
            logger.error(f"DB Error in create_post: {e}")
            raise ValueError(f"Ошибка базы данных: {e.detail or str(e)}")    
    return dict(row)


async def update_post(slug: str, data: Dict[str, Any]) -> Optional[Dict]:
    """Обновляет существующую статью."""
    # Получаем текущую запись
    existing = await get_post_by_slug(slug)
    if not existing:
        return None

    # Подготавливаем обновлённые поля
    new_title = data.get('title', existing['title'])
    new_content_md = data.get('content_markdown', existing['content_markdown'])
    new_content_html = markdown.markdown(new_content_md, extensions=['extra', 'codehilite'])
    new_excerpt = data.get('excerpt')
    if not new_excerpt and new_content_html:
        soup = BeautifulSoup(new_content_html, 'html.parser')
        text = soup.get_text()
        new_excerpt = text[:160] + ('...' if len(text) > 160 else '')
    elif not new_excerpt:
        new_excerpt = existing['excerpt']

    new_reading_time = _compute_reading_time(new_content_md)
    new_tags = data.get('tags', existing['tags'])
    new_featured = data.get('featured_image', existing['featured_image'])
    new_og = data.get('og_image', existing['og_image'])
    new_published = data.get('published_at', existing['published_at'])

    if isinstance(new_published, str):
        # Преобразуем ISO-строку с 'Z' в datetime
        new_published = new_published.replace('Z', '+00:00')
        new_published_dt = datetime.fromisoformat(new_published)
    else:
        new_published_dt = new_published or datetime.utcnow() 

    async with _pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                f"""
                UPDATE {SCHEMA}.blog_posts
                SET title = $1,
                    content_markdown = $2,
                    content_html = $3,
                    excerpt = $4,
                    featured_image = $5,
                    og_image = $6,
                    published_at = $7,
                    updated_at = NOW(),
                    tags = $8,
                    reading_time = $9
                WHERE slug = $10
                RETURNING *
                """,
                new_title,
                new_content_md,
                new_content_html,
                new_excerpt,
                new_featured,
                new_og,
                new_published_dt,
                new_tags,
                new_reading_time,
                slug
            )
        except asyncpg.PostgresError as e:
            logger.error(f"DB Error in update_post: {e}")
            raise ValueError(f"Ошибка базы данных: {e.detail or str(e)}")    
    return dict(row) if row else None


async def delete_post(slug: str) -> bool:
    async with _pool.acquire() as conn:
        result = await conn.execute(f"DELETE FROM {SCHEMA}.blog_posts WHERE slug = $1", slug)
        return result == "DELETE 1"


async def get_all_tags() -> List[Dict[str, Any]]:
    """Возвращает список всех тегов с количеством статей."""
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT DISTINCT jsonb_array_elements_text(tags) AS tag, COUNT(*) AS count
            FROM {SCHEMA}.blog_posts
            GROUP BY tag
            ORDER BY tag
            """
        )
    return [{"name": row["tag"], "slug": row["tag"].lower().replace(" ", "-"), "count": row["count"]} for row in rows]

async def get_total_posts_count(tag: Optional[str] = None) -> int:
    async with _pool.acquire() as conn:
        if tag:
            row = await conn.fetchrow(f"SELECT COUNT(*) FROM {SCHEMA}.blog_posts WHERE tags ? $1", tag)
        else:
            row = await conn.fetchrow(f"SELECT COUNT(*) FROM {SCHEMA}.blog_posts")
    return row[0] if row else 0
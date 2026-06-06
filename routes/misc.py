# health_ai_backend_swarm/routes/misc.py
"""
Misc controller — эндпоинты health, sitemap.xml, llms.txt.
"""
import logging
import html
from typing import List
from litestar import Controller, get, Response

logger = logging.getLogger(__name__)

# Глобальный blog_pg_pool (устанавливается из main.py)
blog_pg_pool = None


def init_misc_pool(pool):
    global blog_pg_pool
    blog_pg_pool = pool


class MiscController(Controller):
    path = ""

    @get("/health")
    async def health(self) -> dict:
        return {"status": "ok"}

    @get("/sitemap.xml", status_code=200)
    async def sitemap_xml(self) -> Response:
        from config import config
        PUBLIC_SITE_URL = getattr(config, 'PUBLIC_SITE_URL', 'https://medexpertai.ru')
        global blog_pg_pool

        static_paths = ['/', '/chat', '/blog', '/privacy', '/terms', '/offer', '/security', '/refund']

        xml_parts = ['<?xml version="1.0" encoding="UTF-8"?>']
        xml_parts.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

        for path in static_paths:
            priority = '1.0' if path == '/' else '0.6'
            xml_parts.append(f'''
  <url>
    <loc>{PUBLIC_SITE_URL}{path}</loc>
    <priority>{priority}</priority>
  </url>''')

        if blog_pg_pool is not None:
            try:
                async with blog_pg_pool.acquire(timeout=5.0) as conn:
                    rows = await conn.fetch(
                        "SELECT slug, updated_at, published_at FROM blog_posts ORDER BY published_at DESC",
                        timeout=5.0
                    )
                for row in rows:
                    slug = row['slug']
                    lastmod = row['updated_at'] or row['published_at']
                    lastmod_str = lastmod.strftime('%Y-%m-%d') if hasattr(lastmod, 'strftime') else str(lastmod)[:10]
                    safe_slug = html.escape(str(slug))
                    xml_parts.append(f'''
  <url>
    <loc>{PUBLIC_SITE_URL}/blog/{safe_slug}</loc>
    <lastmod>{lastmod_str}</lastmod>
    <priority>0.8</priority>
  </url>''')
            except Exception as e:
                logger.error(f"[sitemap] DB error: {e}")
        else:
            logger.warning("[sitemap] blog_pg_pool is None, skipping posts")

        xml_parts.append('\n</urlset>')
        return Response(
            content=''.join(xml_parts),
            media_type="application/xml",
            headers={"Cache-Control": "public, max-age=3600"}
        )

    @get("/llms.txt", status_code=200)
    async def llms_txt(self) -> Response:
        from config import config
        PUBLIC_SITE_URL = getattr(config, 'PUBLIC_SITE_URL', 'https://medexpertai.ru')
        global blog_pg_pool

        lines = ["# MedExpert AI — информационная интерпретация анализов крови\n"]
        lines.append("> Сервис расшифровки медицинских анализов крови с помощью ИИ. Учитывает пол, возраст, беременность, образ жизни. **Не ставит диагнозы**, не заменяет врача. Предоставляет справочную информацию.\n\n")
        lines.append("## Основные страницы\n")
        lines.append(f"- [Главная]({PUBLIC_SITE_URL}/)\n")
        lines.append(f"- [Чат с AI-помощником]({PUBLIC_SITE_URL}/chat)\n")
        lines.append(f"- [Блог со статьями]({PUBLIC_SITE_URL}/blog)\n\n")
        lines.append("## Статьи блога\n")
        if blog_pg_pool is not None:
            try:
                async with blog_pg_pool.acquire(timeout=30.0) as conn:
                    count_row = await conn.fetchval("SELECT COUNT(*) FROM blog_posts", timeout=5.0)
                    total = count_row or 0
                    batch_size = 1000
                    offset = 0
                    while offset < total:
                        rows = await conn.fetch(
                            "SELECT slug, title FROM blog_posts ORDER BY published_at DESC LIMIT $1 OFFSET $2",
                            batch_size, offset, timeout=15.0
                        )
                        if not rows:
                            break
                        for row in rows:
                            lines.append(f"- [{row['title']}]({PUBLIC_SITE_URL}/blog/{row['slug']})\n")
                        offset += batch_size
            except Exception as e:
                logger.error(f"[llms.txt] DB error: {e}")
                lines.append("  (статьи временно недоступны)\n")
        else:
            lines.append("  (база данных блога недоступна)\n")
        lines.append("\n")
        lines.append("## Юридическая информация\n")
        lines.append(f"- [Политика конфиденциальности]({PUBLIC_SITE_URL}/privacy)\n")
        lines.append(f"- [Условия использования]({PUBLIC_SITE_URL}/terms)\n")
        lines.append(f"- [Публичная оферта]({PUBLIC_SITE_URL}/offer)\n")

        return Response(
            content=''.join(lines),
            media_type="text/plain; charset=utf-8",
            headers={"Cache-Control": "public, max-age=3600"}
        )
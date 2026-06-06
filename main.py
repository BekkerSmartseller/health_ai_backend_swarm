# health_ai_backend_swarm/main.py
"""
Новый сервер с Socket.IO и swarm‑агентами.
Поддерживает потоковую передачу (streaming) через события socket.io.
Интегрирован HindsightMemoryLayer и PostgreSQL checkpointer.
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict
import json
import socketio

from litestar import Litestar
from litestar.config.cors import CORSConfig
from litestar.connection import ASGIConnection
from litestar.security.jwt import JWTCookieAuth, Token

from langfuse.langchain import CallbackHandler

import asyncpg
from config import config

from services.hindsight_memory import HindsightMemoryLayer
from graph.swarm_workflow import get_compiled_graph

from services.user_db import init_user_db, get_user_by_id
from services.blog_db import init_db as init_blog_db

from routes.auth import AuthController, init_redis
from routes.blog import BlogController
from routes.admin_blog import AdminBlogController
from routes.chat import ChatController, init_chat_memory
from routes.profile import ProfileController
from routes.misc import MiscController, init_misc_pool

from services.swarm_runner import init_swarm_runner
from services.socket_handlers import init_socket_handlers, register_handlers

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from dotenv import load_dotenv
load_dotenv()

# ==================== ГЛОБАЛЬНЫЕ ОБЪЕКТЫ ====================
swarm_graph = None
memory_layer = None
blog_pg_pool = None
sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')
active_sessions: Dict[str, str] = {}


# ---------- JWT ----------

async def retrieve_user(token: Token, conn: ASGIConnection):
    sub = token.sub
    if sub == "admin":
        return {"id": "admin", "is_admin": True}
    user = await get_user_by_id(sub)
    if user and user.get("is_active"):
        return user
    return None

jwt_auth = JWTCookieAuth[dict](
    retrieve_user_handler=retrieve_user,
    token_secret=config.JWT_SECRET,
    key="token",
    exclude=[
        "/admin/login", "/health",
        r"^/blog",
        r"^/chat", r"^/uploads",
        r"^/auth/request-code", r"^/auth/verify-code", r"^/auth/complete-profile",
        r"^/auth/google", r"^/auth/google/callback",
        r"^/auth/yandex", r"^/auth/yandex/callback",
        "/auth/logout", "/sitemap.xml", "/llms.txt", "/auth/me",
        r"^/admin", "/profile"
    ],
)


# ==================== LIFESPAN ====================
def init_connection(conn):
    async def set_type_codec(conn):
        await conn.set_type_codec('jsonb', encoder=json.dumps, decoder=json.loads, schema='pg_catalog')
    return set_type_codec(conn)


@asynccontextmanager
async def lifespan(app: Litestar):
    global swarm_graph, memory_layer, blog_pg_pool
    memory_layer = HindsightMemoryLayer(config.HINDSIGHT_URL)
    langfuse_handler = CallbackHandler()
    swarm_graph = await get_compiled_graph()

    # Инициализация PostgreSQL для блога
    conn_string = config.POSTGRES
    if conn_string:
        blog_pg_pool = await asyncpg.create_pool(conn_string, min_size=1, max_size=5, init=init_connection)
        if blog_pg_pool:
            await init_blog_db(blog_pg_pool)
            await init_user_db(blog_pg_pool)
            import routes.blog as blog_module
            blog_module.BLOG_PG_POOL = blog_pg_pool
            init_misc_pool(blog_pg_pool)
            logger.info("Blog PostgreSQL pool initialized")
    else:
        logger.warning("POSTGRES connection string missing, blog will use JSON fallback")

    # Инициализация Redis
    init_redis(app)

    # Передаём зависимости в вынесенные модули
    init_chat_memory(memory_layer)
    init_swarm_runner(swarm_graph, memory_layer, langfuse_handler, sio)

    # Импортируем run_swarm_and_emit после инициализации swarm_runner
    from services.swarm_runner import run_swarm_and_emit
    init_socket_handlers(memory_layer, run_swarm_and_emit)

    # Регистрируем Socket.IO обработчики
    register_handlers(sio)

    yield
    if blog_pg_pool:
        await blog_pg_pool.close()


# ==================== LITESTAR APP ====================
litestar_app = Litestar(
    route_handlers=[
        AuthController,
        BlogController,
        AdminBlogController,
        ChatController,
        ProfileController,
        MiscController,
    ],
    cors_config=CORSConfig(
        allow_origins=config.ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization"],
    ),
    debug=getattr(config, 'DEBUG', True),
    lifespan=[lifespan],
    on_app_init=[jwt_auth.on_app_init],
)

asgi_app = socketio.ASGIApp(sio, other_asgi_app=litestar_app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        asgi_app,
        host=getattr(config, 'HOST', '0.0.0.0'),
        port=getattr(config, 'PORT', 6575),
        log_level="info",
        workers=config.WORKERS
    )
# health_ai_backend_swarm/main.py
"""
Новый сервер с Socket.IO и swarm‑агентами.
Поддерживает потоковую передачу (streaming) через события socket.io.
Интегрирован HindsightMemoryLayer и PostgreSQL checkpointer.
"""
import asyncio
import base64
import logging
import uuid
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Dict, Optional
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import re
import json
import socketio
import jwt

from litestar import Litestar, get, Request, Response
from litestar.exceptions import NotAuthorizedException
from litestar.config.cors import CORSConfig
from litestar.connection import ASGIConnection
from litestar.handlers.base import BaseRouteHandler
from litestar.security.jwt import JWTCookieAuth, Token

from langfuse.langchain import CallbackHandler

import asyncpg
import redis.asyncio as aioredis
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

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from dotenv import load_dotenv
load_dotenv()

# ==================== ГЛОБАЛЬНЫЕ ОБЪЕКТЫ ====================
swarm_graph = None
memory_layer = None
langfuse_handler = None
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
    global swarm_graph, memory_layer, langfuse_handler, blog_pg_pool
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
            # Передаём пул в blog-контроллер
            import routes.blog as blog_module
            blog_module.BLOG_PG_POOL = blog_pg_pool
            init_misc_pool(blog_pg_pool)
            logger.info("Blog PostgreSQL pool initialized")
    else:
        logger.warning("POSTGRES connection string missing, blog will use JSON fallback")

    # Инициализация Redis
    init_redis(app)

    # Передаём memory_layer в chat-контроллер
    init_chat_memory(memory_layer)

    yield
    if blog_pg_pool:
        await blog_pg_pool.close()


# ==================== MERMAID ====================
def fix_mermaid_blocks(text: str) -> str:
    pattern = r'(```mermaid\n)(.*?)(```)'

    def replace(match):
        content = match.group(2)
        content = re.sub(r'<br\s*/?>', '\n', content)
        return match.group(1) + content + match.group(3)
    return re.sub(pattern, replace, text, flags=re.DOTALL)


# ==================== SWARM ====================
async def run_swarm_and_emit(
    thread_id: str,
    user_message: str,
    timezone: str = "UTC",
    locale: str = "en",
    location: dict | None = None,
):
    room = thread_id
    try:
        await memory_layer.save_message(thread_id, thread_id, "user", user_message)
        try:
            tz = ZoneInfo(timezone)
        except Exception:
            tz = ZoneInfo("UTC")
        now_local = datetime.now(tz)
        local_time = now_local.strftime("%H:%M:%S")
        local_date = now_local.strftime("%Y-%m-%d")
        day_of_week = now_local.strftime("%A")
        context = (
            f"[Системный контекст: Текущее локальное время: {local_time}, "
            f"дата: {local_date}, день недели: {day_of_week}, "
            f"часовой пояс: {timezone}"
        )
        if location:
            context += f", местоположение: широта {location['lat']}, долгота {location['lon']}"
        context += "]\n"
        augmented_message = context + user_message

        history = await memory_layer.get_conversation_history(thread_id, thread_id, limit=20)
        user_profile = await memory_layer.extract_user_facts(thread_id)

        messages = []
        if user_profile:
            messages.append({"role": "system", "content": f"Информация о пользователе: {user_profile}"})
        for msg in history:
            messages.append({"role": msg["role"], "content": msg["content"]})
        messages.append({"role": "user", "content": augmented_message})

        configurable = {"configurable": {"thread_id": thread_id}}
        full_config = {**configurable, "recursion_limit": 30, "callbacks": [langfuse_handler]}

        reasoning_buffer = ""
        last_send_time = asyncio.get_event_loop().time()
        stop_flusher = asyncio.Event()

        async def flush_reasoning():
            nonlocal reasoning_buffer
            if reasoning_buffer:
                await sio.emit("reasoning_chunk", {"content": reasoning_buffer}, room=room)
                reasoning_buffer = ""

        async def periodic_flusher():
            nonlocal last_send_time
            while not stop_flusher.is_set():
                await asyncio.sleep(0.05)
                if reasoning_buffer:
                    await flush_reasoning()
                    last_send_time = asyncio.get_event_loop().time()

        flusher_task = asyncio.create_task(periodic_flusher())

        try:
            async for event in swarm_graph.astream_events(
                {"messages": messages}, config=full_config, version="v2"
            ):
                kind = event.get("event")
                if kind == "on_chat_model_stream":
                    chunk = event["data"]["chunk"]
                    reasoning = None
                    if hasattr(chunk, "additional_kwargs"):
                        reasoning = chunk.additional_kwargs.get("reasoning_content")
                    if reasoning:
                        reasoning_buffer += reasoning
                        now = asyncio.get_event_loop().time()
                        if len(reasoning_buffer) >= 10 or (now - last_send_time) >= 0.05:
                            await flush_reasoning()
                            last_send_time = now
                elif kind == "on_tool_start":
                    tool_name = event.get("name", "инструмент")
                    tool_input = event.get("data", {}).get("input", {})
                    msg = ""
                    if "handoff" in tool_name:
                        msg = f"🔄 **Переключаюсь на агента: {tool_name}**\n\n"
                    elif tool_name == "web_search":
                        query = tool_input.get("query", "")
                        msg = f"🔍 **Ищу в интернете:** {query}\n\n"
                    elif tool_name == "fact_check":
                        statement = tool_input.get("statement", "")
                        msg = f"✅ **Проверяю достоверность:**\n{statement}\n\n"
                    else:
                        if "handoff" not in tool_name:
                            msg = f"🔧 **Вызываю инструмент:** {tool_name}\n\n"
                    if msg:
                        await flush_reasoning()
                        await sio.emit("reasoning_chunk", {"content": msg}, room=room)
                elif kind == "on_tool_end":
                    tool_name = event.get("name", "инструмент")
                    output = event.get("data", {}).get("output")
                    output_str = output.content if hasattr(output, "content") else str(output)
                    msg = ""
                    if tool_name == "web_search":
                        link_count = output_str.count("**1.") if "**1." in output_str else output_str.count("🔗")
                        loaded_pages = output_str.count("📄 Текст со страницы:")
                        msg = f"✅ **{tool_name} завершён**\n- Найдено ссылок: {link_count}\n- Загружено страниц: {loaded_pages}\n\n"
                    elif tool_name == "fact_check":
                        try:
                            data = json.loads(output_str)
                            verdict = data.get("verdict", "неизвестно")
                            confidence = data.get("confidence", 0.0)
                            msg = f"✅ **{tool_name} завершён**\n- Вердикт: {verdict}\n- Уверенность: {confidence:.0%}\n\n"
                        except Exception:
                            msg = f"✅ **{tool_name} завершён**\n\n"
                    else:
                        msg = f"✅ **{tool_name} завершён**\n\n"
                    if msg:
                        await flush_reasoning()
                        await sio.emit("reasoning_chunk", {"content": msg}, room=room)
        finally:
            stop_flusher.set()
            flusher_task.cancel()
            try:
                await flusher_task
            except asyncio.CancelledError:
                pass
            await flush_reasoning()

        final_answer = ""
        try:
            final_state = await swarm_graph.aget_state(configurable)
            final_msgs = final_state.values.get("messages", [])
            if final_msgs:
                last = final_msgs[-1]
                if hasattr(last, "content") and last.content:
                    final_answer = last.content
                elif isinstance(last, dict) and last.get("content"):
                    final_answer = last["content"]
        except Exception as e:
            logger.error(f"Failed to get final state: {e}")

        if final_answer.strip():
            import unicodedata
            final_answer = unicodedata.normalize('NFC', final_answer)
            final_answer = fix_mermaid_blocks(final_answer)
            await sio.emit("stream_start", room=room)
            chunk_size = 20
            for i in range(0, len(final_answer), chunk_size):
                chunk = final_answer[i:i + chunk_size]
                await sio.emit("stream_chunk", {"content": chunk}, room=room)
                await asyncio.sleep(0.02)
            await sio.emit("stream_end", room=room)
            await memory_layer.save_message(thread_id, thread_id, "assistant", final_answer)
        else:
            await sio.emit("error", {"message": "Пустой ответ от ассистента"}, room=room)
    except Exception as e:
        logger.error(f"Swarm streaming error: {e}", exc_info=True)
        await sio.emit("error", {"message": f"Ошибка: {str(e)}"}, room=room)


# ==================== SOCKET.IO ====================
@sio.event
async def connect(sid, environ, auth):
    logger.info(f"Socket.IO connect: {sid}")
    await sio.save_session(sid, {"thread_id": None})


@sio.event
async def join(sid, data):
    thread_id = data.get("thread_id")
    if not thread_id:
        await sio.disconnect(sid)
        return
    await sio.enter_room(sid, thread_id)
    await sio.emit('join_success', to=sid)
    async with sio.session(sid) as session:
        session["thread_id"] = thread_id
    active_sessions[sid] = thread_id

    history = await memory_layer.get_conversation_history(thread_id, thread_id, limit=1)
    if not history:
        welcome = "Здравствуйте! Я многоагентный помощник. Чем могу помочь?"
        await sio.emit("chat_message", {"role": "assistant", "content": welcome, "type": "text"}, room=thread_id)
        await memory_layer.save_message(thread_id, thread_id, "assistant", welcome)


@sio.event
async def chat_message(sid, data):
    session = await sio.get_session(sid)
    thread_id = session.get("thread_id")
    if not thread_id:
        await sio.emit("error", {"message": "Not joined"}, to=sid)
        return
    user_text = data.get("content")
    if not user_text:
        return
    timezone = data.get("timezone", "UTC")
    locale = data.get("locale", "en")
    location = data.get("location")
    asyncio.create_task(run_swarm_and_emit(
        thread_id, user_text, timezone=timezone, locale=locale, location=location,
    ))


@sio.event
async def file_upload(sid, data):
    session = await sio.get_session(sid)
    thread_id = session.get("thread_id")
    if not thread_id:
        await sio.emit("error", {"message": "Not joined"}, to=sid)
        return
    filename = data.get("filename")
    file_b64 = data.get("file")
    if not filename or not file_b64:
        return
    upload_dir = Path("/tmp/swarm_uploads")
    upload_dir.mkdir(exist_ok=True)
    file_path = upload_dir / filename
    file_path.write_bytes(base64.b64decode(file_b64))
    user_message = f"Пользователь загрузил файл {filename}. Файл сохранён по пути {file_path}. Проанализируй его содержимое."
    asyncio.create_task(run_swarm_and_emit(thread_id, user_message))


@sio.event
async def disconnect(sid):
    logger.info(f"Disconnect: {sid}")
    if sid in active_sessions:
        del active_sessions[sid]


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
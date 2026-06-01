# health_ai_backend_swarm/main.py
"""
Новый сервер с Socket.IO и swarm‑агентами.
Поддерживает потоковую передачу (streaming) через события socket.io.
Интегрирован HindsightMemoryLayer и PostgreSQL checkpointer.
ИСПРАВЛЕНИЯ:
- Буферизация reasoning-чанков для плавного отображения на фронте.
- Обработка handoff-переключений агентов через полный цикл astream.
- Увеличен recursion_limit до 30.
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
import markdown
from typing import List, Optional,Any
from datetime import datetime
from bs4 import BeautifulSoup

import socketio
from litestar import Litestar, post,put, get,delete, Request,Response
from litestar.exceptions import HTTPException,NotAuthorizedException
from litestar.config.cors import CORSConfig
from litestar.datastructures import Cookie
from litestar.response import Redirect
from litestar.middleware.session.client_side import ClientSideSessionBackend
from litestar.middleware.session import SessionMiddleware
from litestar.middleware.session.server_side import ServerSideSessionConfig
from litestar.connection import ASGIConnection
from litestar.handlers.base import BaseRouteHandler
from litestar.exceptions import NotAuthorizedException
from litestar.di import Provide
from litestar.security.jwt import JWTCookieAuth, Token
from litestar.datastructures import UploadFile


from langfuse.langchain import CallbackHandler

from litestar.dto import DTOData
from pydantic import BaseModel


from services.hindsight_memory import HindsightMemoryLayer
from graph.swarm_workflow import get_compiled_graph
from config import config

import asyncpg
from asyncpg.pool import Pool
from services.blog_db import (
    init_db, get_all_posts, get_post_by_slug, create_post,
    update_post, delete_post, get_all_tags,get_total_posts_count
)

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


# ==================== LIFESPAN ====================
def init_connection(conn):
    """Регистрирует кодек для автоматического преобразования JSONB."""
    async def set_type_codec(conn):
        await conn.set_type_codec(
            'jsonb',
            encoder=json.dumps,      # Python list/dict -> JSON string
            decoder=json.loads,      # JSON string -> Python list/dict
            schema='pg_catalog'
        )
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
        blog_pg_pool = await asyncpg.create_pool(conn_string, min_size=1, max_size=5,init=init_connection)
        await init_db(blog_pg_pool)
        logger.info("Blog PostgreSQL pool initialized")
    else:
        logger.warning("POSTGRES connection string missing, blog will use JSON fallback")
    
    yield
    if blog_pg_pool:
        await blog_pg_pool.close()

# ==================== АДМИН АУТЕНТИФИКАЦИЯ ====================

class AdminUser(BaseModel):
    id: str          # всегда 'admin'
    name: str = "Administrator"

async def retrieve_admin_user(token: Token, conn: ASGIConnection) -> Optional[AdminUser]:
    # Токен считается валидным, если subject == 'admin'
    return AdminUser(id="admin") if token.sub == "admin" else None

jwt_admin_auth = JWTCookieAuth[AdminUser](
    retrieve_user_handler=retrieve_admin_user,
    token_secret=config.JWT_SECRET,
    exclude=[
        "/admin/login",
        "/health",
        # "/admin/logout",
        
        # 🔥 ВАЖНО: Используем r"^/..." (raw string с якорем начала строки)
        r"^/blog",        # Матчит /blog, /blog/posts, /blog/posts/slug, /blog/tags
                          # НО НЕ матчит /admin/blog/... !
                          
        r"^/chat",        # Матчит публичные эндпоинты чата (/chat/thread, /chat/upload)
        
        # Служебные пути Litestar/Swagger
        "/openapi.json",
        "/schema",
        "/docs",
        "/sitemap.xml",
    ],
)

async def admin_guard(connection: ASGIConnection, handler: BaseRouteHandler) -> None:
    if connection.route_handler.path.startswith("/admin") and connection.user is None:
        raise NotAuthorizedException("Authentication required")

# ==================== 
def fix_mermaid_blocks(text: str) -> str:
    """Заменяет <br/> на \n внутри блоков ```mermaid ... ```"""
    pattern = r'(```mermaid\n)(.*?)(```)'
    def replace(match):
        content = match.group(2)
        # Заменяем <br/> и <br> на \n
        content = re.sub(r'<br\s*/?>', '\n', content)
        # Экранируем специальные символы, если нужно (например, фигурные скобки)
        return match.group(1) + content + match.group(3)
    return re.sub(pattern, replace, text, flags=re.DOTALL)

# async def run_swarm_and_emit(
#     thread_id: str,
#     user_message: str,
#     timezone: str = "UTC",
#     locale: str = "en",
#     location: dict | None = None,
# ):
#     """
#     Запускает swarm-граф с потоковой передачей событий клиенту.
#     Корректно обрабатывает handoff-переключения между агентами.
#     """
#     room = thread_id
#     try:
#         # 1. Сохраняем оригинальное сообщение в Hindsight
#         await memory_layer.save_message(thread_id, thread_id, "user", user_message)

#         # 2. Формируем контекст (время, место)
#         try:
#             tz = ZoneInfo(timezone)
#         except Exception:
#             tz = ZoneInfo("UTC")
#         now_local = datetime.now(tz)
#         local_time = now_local.strftime("%H:%M:%S")
#         local_date = now_local.strftime("%Y-%m-%d")
#         day_of_week = now_local.strftime("%A")
#         context = (
#             f"[Системный контекст: Текущее локальное время: {local_time}, "
#             f"дата: {local_date}, день недели: {day_of_week}, "
#             f"часовой пояс: {timezone}"
#         )
#         if location:
#             context += f", местоположение: широта {location['lat']}, долгота {location['lon']}"
#         context += "]\n"
#         augmented_message = context + user_message
#         logger.info(f"Augmented message: {augmented_message}")

#         # 3. Загружаем историю и профиль пользователя
#         history = await memory_layer.get_conversation_history(thread_id, thread_id, limit=20)
#         user_profile = await memory_layer.extract_user_facts(thread_id)

#         messages = []
#         if user_profile:
#             messages.append({"role": "system", "content": f"Информация о пользователе: {user_profile}"})
#         for msg in history:
#             messages.append({"role": msg["role"], "content": msg["content"]})
#         messages.append({"role": "user", "content": augmented_message})

#         # 4. Конфигурация графа
#         configurable = {"configurable": {"thread_id": thread_id}}
#         full_config = {**configurable, "recursion_limit": 30, "callbacks": [langfuse_handler]}

#         # Буфер для reasoning (для плавного стриминга)
#         reasoning_buffer = ""
#         last_send_time = asyncio.get_event_loop().time()

#         async def flush_reasoning():
#             nonlocal reasoning_buffer
#             if reasoning_buffer:
#                 await sio.emit("reasoning_chunk", {"content": reasoning_buffer}, room=room)
#                 reasoning_buffer = ""

#         # 5. Запускаем граф в режиме values – получаем полное состояние после каждого шага
#         final_answer = ""
#         async for event in swarm_graph.astream(
#             {"messages": messages}, config=full_config, stream_mode="values"
#         ):
#             # event — словарь с ключом "messages" (список всех сообщений на данный момент)
#             if not event.get("messages"):
#                 continue
#             last_msg = event["messages"][-1]

#             # Извлекаем reasoning (если есть)
#             if hasattr(last_msg, "additional_kwargs"):
#                 reasoning = last_msg.additional_kwargs.get("reasoning_content")
#                 if reasoning:
#                     reasoning_buffer += reasoning
#                     now = asyncio.get_event_loop().time()
#                     if len(reasoning_buffer) >= 10 or (now - last_send_time) >= 0.1:
#                         await flush_reasoning()
#                         last_send_time = now

#             # Уведомления о вызове инструментов
#             if hasattr(last_msg, "tool_calls") and last_msg.tool_calls:
#                 for tc in last_msg.tool_calls:
#                     tool_name = tc.get("name", "")
#                     if "handoff" in tool_name or "transfer" in tool_name:
#                         await flush_reasoning()
#                         await sio.emit("reasoning_chunk", {"content": f"🔄 **Переключаюсь на агента: {tool_name}**\n\n"}, room=room)
#                     elif tool_name == "web_search":
#                         await flush_reasoning()
#                         query = tc.get("args", {}).get("query", "")
#                         await sio.emit("reasoning_chunk", {"content": f"🔍 **Ищу в интернете:** {query}\n\n"}, room=room)
#                     elif tool_name == "fact_check":
#                         await flush_reasoning()
#                         await sio.emit("reasoning_chunk", {"content": f"✅ **Проверяю достоверность...**\n\n"}, room=room)
#                     # другие инструменты по желанию

#             # Уведомления о завершении инструментов (ToolMessage)
#             if last_msg.type == "tool":
#                 tool_name = getattr(last_msg, "name", "инструмент")
#                 if tool_name == "web_search":
#                     await flush_reasoning()
#                     await sio.emit("reasoning_chunk", {"content": f"✅ **Поиск завершён**\n\n"}, room=room)
#                 elif tool_name == "fact_check":
#                     await flush_reasoning()
#                     await sio.emit("reasoning_chunk", {"content": f"✅ **Проверка достоверности завершена**\n\n"}, room=room)
#                 # handoff-инструменты возвращают ToolMessage с текстом "Successfully transferred..."
#                 # их тоже можно показать, но лучше пропустить, чтобы не засорять

#             # Запоминаем последнее сообщение ассистента (если это AIMessage с контентом)
#             if last_msg.type == "ai" and last_msg.content:
#                 final_answer = last_msg.content

#             # Небольшая задержка для имитации реального времени (опционально)
#             await asyncio.sleep(0.01)

#         # 6. После завершения цикла отправляем финальный ответ
#         await flush_reasoning()

#         if final_answer.strip():
#             await sio.emit("stream_start", room=room)
#             chunk_size = 50
#             for i in range(0, len(final_answer), chunk_size):
#                 chunk = final_answer[i:i+chunk_size]
#                 await sio.emit("stream_chunk", {"content": chunk}, room=room)
#                 await asyncio.sleep(0.03)
#             await sio.emit("stream_end", room=room)
#             await memory_layer.save_message(thread_id, thread_id, "assistant", final_answer)
#         else:
#             # Страховка: пробуем ещё раз получить состояние
#             final_state = await swarm_graph.aget_state(configurable)
#             final_msgs = final_state.values.get("messages", [])
#             if final_msgs:
#                 last = final_msgs[-1]
#                 if hasattr(last, "content") and last.content:
#                     final_answer = last.content
#                 elif isinstance(last, dict) and last.get("content"):
#                     final_answer = last["content"]       
#             if final_answer.strip():
#                 final_answer = fix_mermaid_blocks(final_answer)
#                 await sio.emit("stream_start", room=room)
#                 for i in range(0, len(final_answer), 50):
#                     await sio.emit("stream_chunk", {"content": final_answer[i:i+50]}, room=room)
#                     await asyncio.sleep(0.03)
#                 await sio.emit("stream_end", room=room)
#                 await memory_layer.save_message(thread_id, thread_id, "assistant", final_answer)
#             else:
#                 await sio.emit("error", {"message": "Пустой ответ от ассистента"}, room=room)

#     except Exception as e:
#         logger.error(f"Swarm streaming error: {e}", exc_info=True)
#         await sio.emit("error", {"message": f"Ошибка: {str(e)}"}, room=room)
async def run_swarm_and_emit(
    thread_id: str,
    user_message: str,
    timezone: str = "UTC",
    locale: str = "en",
    location: dict | None = None,
):
    room = thread_id
    try:
        # 1. Сохраняем оригинальное сообщение
        await memory_layer.save_message(thread_id, thread_id, "user", user_message)

        # 2. Контекст
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
        logger.info(f"Augmented message: {augmented_message}")

        # 3. История и профиль
        history = await memory_layer.get_conversation_history(thread_id, thread_id, limit=20)
        user_profile = await memory_layer.extract_user_facts(thread_id)

        messages = []
        if user_profile:
            messages.append({"role": "system", "content": f"Информация о пользователе: {user_profile}"})
        for msg in history:
            messages.append({"role": msg["role"], "content": msg["content"]})
        messages.append({"role": "user", "content": augmented_message})

        # 4. Конфигурация графа
        configurable = {"configurable": {"thread_id": thread_id}}
        full_config = {**configurable, "recursion_limit": 30, "callbacks": [langfuse_handler]}

        # ✅ ИСПРАВЛЕНО: буферизация с фоновым периодическим flush'ем
        reasoning_buffer = ""
        last_send_time = asyncio.get_event_loop().time()
        stop_flusher = asyncio.Event()

        async def flush_reasoning():
            nonlocal reasoning_buffer
            if reasoning_buffer:
                await sio.emit("reasoning_chunk", {"content": reasoning_buffer}, room=room)
                reasoning_buffer = ""

        # ✅ НОВОЕ: фоновый flusher — отправляет буфер каждые 50мс,
        # даже если новых событий не приходит
        async def periodic_flusher():
            nonlocal last_send_time
            while not stop_flusher.is_set():
                await asyncio.sleep(0.05)  # 50мс
                if reasoning_buffer:
                    await flush_reasoning()
                    last_send_time = asyncio.get_event_loop().time()

        flusher_task = asyncio.create_task(periodic_flusher())

        try:
            # 5. Стриминг через astream_events
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
                        # ✅ ИСПРАВЛЕНО: снижен порог до 10 символов
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
                            import json
                            data = json.loads(output_str)
                            verdict = data.get("verdict", "неизвестно")
                            confidence = data.get("confidence", 0.0)
                            msg = f"✅ **{tool_name} завершён**\n- Вердикт: {verdict}\n- Уверенность: {confidence:.0%}\n\n"
                        except:
                            msg = f"✅ **{tool_name} завершён**\n\n"
                    else:
                        msg = f"✅ **{tool_name} завершён**\n\n"
                    if msg:
                        await flush_reasoning()
                        await sio.emit("reasoning_chunk", {"content": msg}, room=room)

        finally:
            # ✅ Останавливаем flusher и отправляем остатки
            stop_flusher.set()
            flusher_task.cancel()
            try:
                await flusher_task
            except asyncio.CancelledError:
                pass
            await flush_reasoning()

        # 6. Получить финальный ответ
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

        # 7. Отправить финальный ответ
        if final_answer.strip():
            import unicodedata
            final_answer = unicodedata.normalize('NFC', final_answer)
            final_answer = fix_mermaid_blocks(final_answer)
            await sio.emit("stream_start", room=room)
            # ✅ ИСПРАВЛЕНО: уменьшен размер чанка для более плавного стриминга
            chunk_size = 20
            for i in range(0, len(final_answer), chunk_size):
                chunk = final_answer[i:i+chunk_size]
                await sio.emit("stream_chunk", {"content": chunk}, room=room)
                await asyncio.sleep(0.02)
            await sio.emit("stream_end", room=room)
            await memory_layer.save_message(thread_id, thread_id, "assistant", final_answer)
        else:
            await sio.emit("error", {"message": "Пустой ответ от ассистента"}, room=room)

    except Exception as e:
        logger.error(f"Swarm streaming error: {e}", exc_info=True)
        await sio.emit("error", {"message": f"Ошибка: {str(e)}"}, room=room)

# ==================== SOCKET.IO EVENT HANDLERS ====================
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
    logger.info(f"Client {sid} joined room {thread_id}")

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
        thread_id, user_text,
        timezone=timezone,
        locale=locale,
        location=location,
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

# ==================== HTTP ENDPOINTS ====================
@get("/chat/{thread_id:str}/history")
async def get_chat_history(thread_id: str, timezone: str = "UTC", limit: int = 10, offset: int = 0) -> dict:
    all_history = await memory_layer.get_conversation_history(thread_id, thread_id, limit=200)
    total = len(all_history)
    start = max(0, total - offset - limit)
    end = max(0, total - offset)
    sliced = all_history[start:end]
    return {"messages": sliced, "total": total}

@post("/chat/upload")
async def upload_file(request: Request) -> dict:
    form = await request.form()
    file = form.get('file')
    if not file:
        return {"error": "No file"}
    upload_dir = Path("/tmp/swarm_uploads")
    upload_dir.mkdir(exist_ok=True)
    file_path = upload_dir / file.filename
    content = await file.read()
    file_path.write_bytes(content)
    thread_id = str(uuid.uuid4())
    return {"thread_id": thread_id}

@get("/chat/thread")
async def create_thread() -> dict:
    """Создаёт новый thread_id и возвращает его."""
    thread_id = str(uuid.uuid4())
    return {"thread_id": thread_id}


# ==================== БЛОГ ====================
# BLOG_DATA_FILE = Path("blog_data/blog_posts.json")
BLOG_DATA_FILE = Path(__file__).parent / "blog_data" / "blog_posts.json"
BLOG_POSTS_CACHE = []
LAST_MODIFIED = 0

def load_blog_posts(force_reload: bool = False) -> List[dict]:
    """Загружает статьи из JSON, принудительно генерируя content_html."""
    with open(BLOG_DATA_FILE, "r", encoding="utf-8") as f:
        posts = json.load(f)
    for post in posts:
        # Генерируем content_html, если его нет, но есть content_markdown
        if "content_html" not in post and "content_markdown" in post:
            post["content_html"] = markdown.markdown(
                post["content_markdown"],
                extensions=["extra", "codehilite"]
            )
            print(f"✅ Сгенерирован HTML для статьи: {post['slug']}")
        # Если нет content_html и нет content_markdown – ошибка
        if "content_html" not in post:
            post["content_html"] = "<p>Содержимое статьи временно недоступно.</p>"
            print(f"⚠️ Нет контента для статьи: {post['slug']}")
        # Генерация excerpt (если нет)
        if not post.get("excerpt") and "content_html" in post:
            soup = BeautifulSoup(post["content_html"], "html.parser")
            text = soup.get_text()
            post["excerpt"] = text[:160] + ("..." if len(text) > 160 else "")
        # Генерация reading_time (если нет)
        if not post.get("reading_time") and "content_markdown" in post:
            word_count = len(post["content_markdown"].split())
            post["reading_time"] = max(1, round(word_count / 200))
    return posts

def get_related_posts(current_post: dict, all_posts: List[dict], limit: int = 3) -> List[dict]:
    """Возвращает похожие статьи по совпадению тегов."""
    current_tags = set(current_post.get("tags", []))
    scored = []
    for p in all_posts:
        if p["slug"] == current_post["slug"]:
            continue
        score = len(current_tags.intersection(set(p.get("tags", []))))
        if score > 0:
            scored.append((score, p))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [{
        "slug": p["slug"],
        "title": p["title"],
        "featured_image": p.get("featured_image")
    } for _, p in scored[:limit]]


@get("/blog/posts")
async def blog_posts(request: Request, page: int = 1, limit: int = 10, tag: Optional[str] = None) -> dict:
    """
    Возвращает список статей с пагинацией и фильтром по тегу.
    """
    if blog_pg_pool is None:
        # fallback на JSON
        all_posts = load_blog_posts()
        print("all_posts", all_posts)
        # Фильтрация по тегу
        if tag:
            filtered = [p for p in all_posts if tag in p.get("tags", [])]
        else:
            filtered = all_posts
        
        # Пагинация
        total = len(filtered)
        total_pages = (total + limit - 1) // limit if limit > 0 else 1
        start = (page - 1) * limit
        end = start + limit
        paginated = filtered[start:end]
        
        # Убираем полное содержимое (content_markdown, content_html) – оставляем только метаданные
        for p in paginated:
            p.pop("content_markdown", None)
            p.pop("content_html", None)
        
        return {
            "posts": paginated,
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": total_pages
        }
    offset = (page - 1) * limit
    posts = await get_all_posts(limit=limit, offset=offset, tag=tag)
    total = await get_total_posts_count(tag=tag)  # нужно добавить эту функцию в blog_db
    total_pages = (total + limit - 1) // limit
    # Убираем полное содержимое для списка
    for p in posts:
        p.pop("content_markdown", None)
        p.pop("content_html", None)
    return {
        "posts": posts,
        "total": total,
        "page": page,
        "limit": limit,
        "total_pages": total_pages
    }

@get("/blog/posts/{slug:str}")
async def blog_post(request: Request, slug: str) -> dict:
    post = await get_post_by_slug(slug)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    
    # Удаляем content_html, чтобы фронт использовал MarkdownRenderer
    post.pop("content_html", None)
    
    # Убедимся, что content_markdown есть
    if not post.get("content_markdown"):
        post["content_markdown"] = "Контент временно недоступен"
    
    # Добавляем пустые related_posts (можно реализовать позже)
    post["related_posts"] = []
    
    return post


@get("/blog/tags")
async def blog_tags(request: Request) -> List[dict]:
    return await get_all_tags()

# ==================== БЛОГ ADMIN ====================

@get("/admin/blog/posts", guards=jwt_admin_auth.guards)
async def admin_list_posts(request: Request[AdminUser, Token, Any], page: int = 1, limit: int = 20) -> dict:
    print("🚨 ADMIN_LIST_POSTS CALLED! User:", request.scope.get("user"))
    offset = (page - 1) * limit
    posts = await get_all_posts(limit=limit, offset=offset)
    total = await get_total_posts_count()
    return {"posts": posts, "total": total, "page": page, "limit": limit}

@post("/admin/blog/posts", guards=jwt_admin_auth.guards)
async def admin_create_post(request: Request[AdminUser, Token, Any], data: dict) -> dict:
    try:
        post = await create_post(data)
    except ValueError as e:
        # 🔥 Возвращаем 400 Bad Request с текстом ошибки
        raise HTTPException(status_code=400, detail=str(e))
    return {"success": True, "post": post}

@put("/admin/blog/posts/{slug:str}", guards=jwt_admin_auth.guards)
async def admin_update_post(slug: str, data: dict) -> dict:
    try:
        post = await update_post(slug, data)
    except ValueError as e:
        # 🔥 Возвращаем 400 Bad Request с текстом ошибки
        raise HTTPException(status_code=400, detail=str(e))
        
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return {"success": True, "post": post}

@delete("/admin/blog/posts/{slug:str}", status_code=200, guards=jwt_admin_auth.guards)
async def admin_delete_post(slug: str) -> dict:
    deleted = await delete_post(slug)
    if not deleted:
        raise HTTPException(status_code=404, detail="Post not found")
    return {"success": True}

@post("/admin/upload-image", guards=jwt_admin_auth.guards)
async def upload_image(request: Request[AdminUser, Token, Any], file: UploadFile) -> dict:
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

# ==================== SECURITY ====================

@post("/admin/login")
async def admin_login(data: dict) -> Response:
    password = data.get("password")
    if password == config.ADMIN_PASSWORD:
        # login() возвращает Response с установленной cookie
        return jwt_admin_auth.login(identifier="admin", response_body={"success": True})
    raise HTTPException(status_code=401, detail="Invalid password")

@post("/admin/logout")
async def admin_logout() -> Response:
    # Создаем пустой ответ
    response = Response({"success": True})
    
    # Принудительно удаляем куку с явным указанием всех параметров
    response.delete_cookie(
        key="token",
        path="/",
    )
    return response

# ==================== LITESTAR APP + SOCKET.IO ASGI ====================

@get("/health")
async def health() -> dict:
    return {"status": "ok"}

litestar_app = Litestar(
    route_handlers=[
        get_chat_history,
        upload_file,
        create_thread,
        blog_posts,
        blog_post,
        blog_tags,
        admin_login,
        admin_logout,
        admin_list_posts,
        admin_create_post,
        admin_update_post,
        admin_delete_post,
        upload_image,
        health
    ],
    cors_config = CORSConfig(
        allow_origins=config.ALLOWED_ORIGINS,
        allow_credentials=True,   # ← критически важно
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization"],
    ),
    debug=getattr(config, 'DEBUG', True),
    lifespan=[lifespan],
    on_app_init=[jwt_admin_auth.on_app_init],
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
# health_ai_backend_swarm/services/socket_handlers.py
"""
Socket.IO event handlers — connect, join, chat_message, file_upload, disconnect.
"""
import asyncio
import base64
import logging
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)

# Глобальные объекты (устанавливаются из main.py)
memory_layer = None
active_sessions: Dict[str, str] = {}
run_swarm_and_emit_fn = None


def init_socket_handlers(ml, run_fn):
    global memory_layer, run_swarm_and_emit_fn
    memory_layer = ml
    run_swarm_and_emit_fn = run_fn


def register_handlers(sio):
    """Регистрирует все Socket.IO обработчики на сервере."""

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
        asyncio.create_task(run_swarm_and_emit_fn(
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
        asyncio.create_task(run_swarm_and_emit_fn(thread_id, user_message))

    @sio.event
    async def disconnect(sid):
        logger.info(f"Disconnect: {sid}")
        if sid in active_sessions:
            del active_sessions[sid]
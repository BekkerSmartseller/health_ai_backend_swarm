# health_ai_backend_swarm/routes/chat.py
"""
Chat controller — эндпоинты для работы с чатом (история, загрузка файлов, создание thread).
"""
import uuid
import logging
from pathlib import Path
from litestar import Controller, get, post, Request

logger = logging.getLogger(__name__)

# Глобальный memory_layer (устанавливается из main.py)
memory_layer = None


def init_chat_memory(ml):
    global memory_layer
    memory_layer = ml


class ChatController(Controller):
    path = "/chat"

    @get("/{thread_id:str}/history")
    async def get_chat_history(self, thread_id: str, timezone: str = "UTC", limit: int = 10, offset: int = 0) -> dict:
        global memory_layer
        all_history = await memory_layer.get_conversation_history(thread_id, thread_id, limit=200)
        total = len(all_history)
        start = max(0, total - offset - limit)
        end = max(0, total - offset)
        sliced = all_history[start:end]
        return {"messages": sliced, "total": total}

    @post("/upload")
    async def upload_file(self, request: Request) -> dict:
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

    @get("/thread")
    async def create_thread(self) -> dict:
        """Создаёт новый thread_id и возвращает его."""
        thread_id = str(uuid.uuid4())
        return {"thread_id": thread_id}
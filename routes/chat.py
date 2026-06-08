# health_ai_backend_swarm/routes/chat.py
"""
Chat controller — эндпоинты для работы с чатом (история, загрузка файлов, создание thread, анализы).
"""
import uuid
import json
import logging
from pathlib import Path
from typing import List
from litestar import Controller, get, post, Request

logger = logging.getLogger(__name__)

# Глобальные объекты (устанавливается из main.py)
memory_layer = None
hindsight_client = None
file_processor = None


def init_chat_memory(ml):
    global memory_layer
    memory_layer = ml


def init_chat_services(hc, fp):
    global hindsight_client, file_processor
    hindsight_client = hc
    file_processor = fp


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
        """Загружает один файл. Возвращает thread_id и путь к файлу."""
        form = await request.form()
        file = form.get('file')
        if not file:
            return {"error": "No file"}
        thread_id = form.get('thread_id', str(uuid.uuid4()))
        upload_dir = Path("/tmp/swarm_uploads")
        upload_dir.mkdir(exist_ok=True)
        file_path = upload_dir / file.filename
        content = await file.read()
        file_path.write_bytes(content)
        return {"thread_id": thread_id, "file_path": str(file_path), "file_name": file.filename}

    @post("/upload/batch")
    async def upload_files_batch(self, request: Request) -> dict:
        """Загружает несколько файлов. Возвращает список загруженных файлов."""
        form = await request.form()
        files = form.multi_items('files')
        if not files:
            # Если не 'files', пробуем 'file'
            files = form.multi_items('file')
        if not files:
            return {"error": "No files", "files": []}
        
        thread_id = form.get('thread_id', str(uuid.uuid4()))
        upload_dir = Path("/tmp/swarm_uploads")
        upload_dir.mkdir(exist_ok=True)
        
        uploaded = []
        for file in files:
            file_path = upload_dir / file.filename
            content = await file.read()
            file_path.write_bytes(content)
            uploaded.append({
                "file_name": file.filename,
                "file_path": str(file_path),
                "size": len(content),
            })
        
        return {"thread_id": thread_id, "files": uploaded, "count": len(uploaded)}

    @get("/{thread_id:str}/analyses")
    async def get_analyses(self, thread_id: str) -> dict:
        """Возвращает список всех анализов для thread_id."""
        global hindsight_client
        if not hindsight_client:
            return {"analyses": [], "error": "Hindsight not initialized"}
        bank_id = f"user_{thread_id}"
        try:
            analyses = await hindsight_client.get_analyses_list(bank_id)
            return {"analyses": analyses, "total": len(analyses)}
        except Exception as e:
            logger.error(f"Failed to get analyses: {e}")
            return {"analyses": [], "error": str(e)}

    @get("/{thread_id:str}/patient")
    async def get_patient_info(self, thread_id: str) -> dict:
        """Возвращает метаданные пациента для thread_id."""
        global hindsight_client
        if not hindsight_client:
            return {"patient": None, "error": "Hindsight not initialized"}
        bank_id = f"user_{thread_id}"
        try:
            patient = await hindsight_client.get_patient_metadata(bank_id)
            return {"patient": patient}
        except Exception as e:
            logger.error(f"Failed to get patient info: {e}")
            return {"patient": None, "error": str(e)}

    @get("/thread")
    async def create_thread(self) -> dict:
        """Создаёт новый thread_id и возвращает его."""
        thread_id = str(uuid.uuid4())
        return {"thread_id": thread_id}

# health_ai_backend_swarm/services/socket_handlers.py
"""
Socket.IO event handlers — connect, join, chat_message, file_upload, disconnect.
"""
import asyncio
import base64
import json
import logging
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)

# Глобальные объекты (устанавливаются из main.py)
memory_layer = None
active_sessions: Dict[str, str] = {}
run_swarm_and_emit_fn = None
run_medical_swarm_and_emit_fn = None
file_processor = None
hindsight_client = None
# Множество thread_id'ов, для которых уже отправлено приветствие
welcomed_threads: set = set()

# Банки Hindsight
FAQ_BANK_ID = "faq-assistant"
NORM_BLOOD_BANK_ID = "norm-blood"


def init_socket_handlers(ml, run_fn, run_medical_fn=None, fp=None, hc=None):
    global memory_layer, run_swarm_and_emit_fn, run_medical_swarm_and_emit_fn, file_processor, hindsight_client
    memory_layer = ml
    run_swarm_and_emit_fn = run_fn
    run_medical_swarm_and_emit_fn = run_medical_fn
    file_processor = fp
    hindsight_client = hc


async def ensure_faq_and_norm_banks():
    """Инициализирует FAQ и norm-blood банки при старте."""
    try:
        if hindsight_client:
            await hindsight_client.init_faq_bank(FAQ_BANK_ID)
            await hindsight_client.init_norm_blood_bank(NORM_BLOOD_BANK_ID)
            logger.info("FAQ and norm-blood banks initialized")
    except Exception as e:
        logger.warning(f"Failed to init banks: {e}")


ALLOWED_ORIGINS = ['http://localhost:5173', 'https://medexpertai.ru']


def register_handlers(sio):
    """Регистрирует все Socket.IO обработчики на сервере."""

    @sio.event
    async def connect(sid, environ, auth):
        origin = environ.get('HTTP_ORIGIN', '') or environ.get('HTTP_REFERER', '')
        if origin and not any(allowed in origin for allowed in ALLOWED_ORIGINS):
            logger.warning(f'Blocked Socket.IO connection from origin: {origin}')
            return False
        logger.info(f"Socket.IO connect: {sid} from {origin or 'unknown'}")
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

        # Приветствие на фронте (Chat.svelte onMount)
        await ensure_faq_and_norm_banks()

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
        
        asyncio.create_task(run_medical_swarm_and_emit_fn(
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
        asyncio.create_task(run_medical_swarm_and_emit_fn(thread_id, user_message))

    @sio.event
    async def upload_files(sid, data):
        """Принимает массив файлов для медицинской обработки."""
        session = await sio.get_session(sid)
        thread_id = session.get("thread_id")
        if not thread_id:
            await sio.emit("error", {"message": "Not joined"}, to=sid)
            return
        
        files = data.get("files", [])
        if not files:
            await sio.emit("error", {"message": "No files provided"}, to=sid)
            return
        
        upload_dir = Path("/tmp/swarm_uploads")
        upload_dir.mkdir(exist_ok=True)
        saved_files = []
        
        for file_data in files:
            filename = file_data.get("filename", f"file_{len(saved_files)}")
            file_b64 = file_data.get("file")
            if not file_b64:
                continue
            file_path = upload_dir / filename
            file_path.write_bytes(base64.b64decode(file_b64))
            saved_files.append(str(file_path))
        
        if not saved_files:
            await sio.emit("error", {"message": "No valid files"}, to=sid)
            return
        
        # Запускаем асинхронную обработку файлов
        async def process_and_notify():
            try:
                # Прогресс: начало
                await sio.emit("file_processing_progress", {
                    "total": len(saved_files),
                    "current": 0,
                    "status": "processing"
                }, room=thread_id)
                
                # Обрабатываем файлы
                results = await file_processor.process_files_batch(saved_files)
                
                # Прогресс: завершено
                await sio.emit("file_processing_progress", {
                    "total": len(saved_files),
                    "current": len(saved_files),
                    "status": "completed"
                }, room=thread_id)
                
                # Отправляем результат обработки
                aggregated = file_processor.aggregate_analyses(results)
                await sio.emit("file_processed", aggregated, room=thread_id)
                
                # Сохраняем анализы в Hindsight
                bank_id = f"user_{thread_id}"
                for analysis in aggregated.get("analyses", []):
                    try:
                        await hindsight_client.save_analysis(bank_id, analysis)
                    except Exception as e:
                        logger.error(f"Failed to save analysis: {e}")
                
                # Если есть информация о пациенте, сохраняем
                if aggregated.get("patient"):
                    try:
                        await hindsight_client.save_patient_metadata(bank_id, aggregated["patient"])
                    except Exception as e:
                        logger.error(f"Failed to save patient metadata: {e}")
                
                # Отправляем сообщение в чат о результате
                summary = (
                    f"📊 **Обработка завершена!**\n"
                    f"- Всего файлов: {aggregated['total_files']}\n"
                    f"- Обработано: {aggregated['processed']}\n"
                    f"- Распознано анализов: {aggregated['total_analyses']}\n"
                    f"- Пропущено (не анализы): {aggregated['total_ignored']}\n"
                )
                if aggregated.get("errors"):
                    summary += f"- Ошибки: {', '.join(aggregated['errors'])}\n"
                summary += "\nЧто делаем дальше?\n"
                
                # Сохраняем результат в Hindsight как сообщение ассистента
                await hindsight_client.retain(
                    bank_id,
                    f"[assistant]: {summary}",
                    metadata={"role": "assistant", "thread_id": thread_id, "type": "file_processing_result"}
                )
                
                # Отправляем в swarm для дальнейшей обработки
                asyncio.create_task(run_medical_swarm_and_emit_fn(
                    thread_id, 
                    f"Файлы загружены и обработаны. {summary} "
                    f"Данные: {json.dumps(aggregated, ensure_ascii=False)[:1000]}",
                ))
                    
            except Exception as e:
                logger.error(f"File processing error: {e}", exc_info=True)
                await sio.emit("error", {"message": f"Ошибка обработки файлов: {str(e)}"}, room=thread_id)
        
        asyncio.create_task(process_and_notify())

    @sio.event
    async def button_click(sid, data):
        """Обрабатывает нажатие кнопки в чате."""
        session = await sio.get_session(sid)
        thread_id = session.get("thread_id")
        if not thread_id:
            await sio.emit("error", {"message": "Not joined"}, to=sid)
            return
        
        button_id = data.get("button_id")
        payload = data.get("payload", {})
        
        # Маппинг кнопок на действия
        if button_id == "upload_analyses":
            # Симулируем сообщение пользователя "Загрузить анализы"
            asyncio.create_task(run_medical_swarm_and_emit_fn(
                thread_id, "Загрузить анализы"
            ))
        elif button_id == "upload_format":
            asyncio.create_task(run_medical_swarm_and_emit_fn(
                thread_id, "В каком формате загружать анализы?"
            ))
        elif button_id == "unique_features":
            asyncio.create_task(run_medical_swarm_and_emit_fn(
                thread_id, "Какие мои уникальные возможности?"
            ))
        elif button_id == "start_decode":
            asyncio.create_task(run_medical_swarm_and_emit_fn(
                thread_id, "Расшифровать анализы"
            ))
        elif button_id == "files_selected":
            # Пользователь выбрал файлы (пришли через upload_files)
            pass  # обработано в upload_files
        else:
            # Любая другая кнопка — передаём её текст как сообщение
            text = payload.get("text", button_id)
            asyncio.create_task(run_medical_swarm_and_emit_fn(
                thread_id, text
            ))

    @sio.event
    async def disconnect(sid):
        logger.info(f"Disconnect: {sid}")
        if sid in active_sessions:
            del active_sessions[sid]

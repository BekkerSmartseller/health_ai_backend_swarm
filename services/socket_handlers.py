# health_ai_backend_swarm/services/socket_handlers.py
"""
Socket.IO event handlers — connect, join, chat_message, file_upload, disconnect.
"""
import asyncio
import base64
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)

# Глобальные объекты (устанавливаются из main.py)
memory_layer = None
active_sessions: Dict[str, str] = {}
run_swarm_and_emit_fn = None
file_processor = None
hindsight_client = None
# Множество thread_id'ов, для которых уже отправлено приветствие
welcomed_threads: set = set()

# Банки Hindsight
FAQ_BANK_ID = "faq-assistant"
NORM_BLOOD_BANK_ID = "norm-blood"


def init_socket_handlers(ml, run_fn, fp=None, hc=None):
    global memory_layer, run_swarm_and_emit_fn, file_processor, hindsight_client
    memory_layer = ml
    run_swarm_and_emit_fn = run_fn
    file_processor = fp
    hindsight_client = hc


async def ensure_faq_and_norm_banks():
    """Инициализирует FAQ банк при старте."""
    try:
        if hindsight_client:
            await hindsight_client.init_faq_bank(FAQ_BANK_ID)
            logger.info("FAQ bank initialized")
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

                # Проверка: если анализы не найдены — файл не медицинский
                total_analyses = aggregated.get("total_analyses", 0)
                if total_analyses == 0:
                    await sio.emit("chat_message", {
                        "role": "assistant",
                        "content": "❌ Загруженный файл не содержит медицинских анализов. Пожалуйста, загрузите файл с результатами лабораторных исследований.",
                    }, room=thread_id)
                    return

                # Сохраняем анализы в Hindsight — одним батчем с метаданными
                bank_id = f"user_{thread_id}"
                analysis_id = str(uuid.uuid4())
                
                # Определяем дату анализа из метаданных пациента или текущую
                patient_data = aggregated.get("patient") or {}
                analysis_date = patient_data.get("date", "") or datetime.now().strftime("%Y-%m-%d")
                
                # Собираем типы анализов
                analyses_list = aggregated.get("analyses") or []
                analysis_types = list(set(
                    a.get("test_name", "").split("(")[0].strip() or "Общий анализ"
                    for a in analyses_list
                    if a.get("test_name")
                ))
                
                # Формируем полный документ со всеми метаданными
                full_data_doc = {
                    "type": "full_analyses_batch",
                    "analysis_id": analysis_id,
                    "analysis_date": analysis_date,
                    "analysis_types": analysis_types,
                    "user_id": thread_id,
                    "total_files": aggregated["total_files"],
                    "processed": aggregated["processed"],
                    "total_analyses": aggregated["total_analyses"],
                    "total_ignored": aggregated["total_ignored"],
                    "errors": aggregated.get("errors", []),
                    "analyses": aggregated["analyses"],
                    "ignored": aggregated.get("ignored", []),
                    "patient": patient_data,
                    "timestamp": datetime.now().isoformat(),
                }
                await hindsight_client.retain(
                    bank_id,
                    json.dumps(full_data_doc, ensure_ascii=False),
                    metadata={
                        "type": "full_analyses_batch",
                        "analysis_id": analysis_id,
                        "analysis_date": analysis_date,
                        "total_analyses": str(aggregated["total_analyses"]),
                        "thread_id": thread_id,
                        "user_id": thread_id,
                    }
                )
                
                # Если есть информация о пациенте, сохраняем метаданные
                if patient_data:
                    try:
                        await hindsight_client.save_patient_metadata(bank_id, {
                            **patient_data,
                            "analysis_id": analysis_id,
                            "analysis_date": analysis_date,
                        })
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
                
                # Отправляем в swarm — передаём полные данные напрямую
                patient_str = ""
                patient_needs_info = False
                missing_fields = []
                if patient_data:
                    patient_str = f"Пациент: {json.dumps(patient_data, ensure_ascii=False)}"
                    for field in ["name", "age", "sex"]:
                        if not patient_data.get(field):
                            missing_fields.append(field)
                            patient_needs_info = True
                
                asyncio.create_task(run_swarm_and_emit_fn(
                    thread_id, 
                    f"Файлы загружены и обработаны. {summary} "
                    f"{patient_str}\n\n"
                    f"ID анализа: {analysis_id}\n"
                    f"Дата анализа: {analysis_date}\n\n"
                    f"ДАННЫЕ АНАЛИЗОВ (всего {aggregated['total_analyses']} шт.):\n"
                    f"{json.dumps(full_data_doc, ensure_ascii=False)}\n"
                    f"{'Обрати внимание: у пациента не хватает данных: ' + ', '.join(missing_fields) if missing_fields else 'Данные о пациенте полные.'}\n\n"
                    f"Вызови check_results_analysis с этими данными для валидации и расшифровки. "
                    f"Если данных о пациенте не хватает — используй get_analyses_data('{bank_id}') для поиска в Hindsight.",
                    save_to_history=False,
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
        print("button_click: ",button_id)
        if button_id == "upload_analyses":
            # Симулируем сообщение пользователя "Загрузить анализы"
            asyncio.create_task(run_swarm_and_emit_fn(
                thread_id, "Загрузить анализы"
            ))
        elif button_id == "upload_format":
            asyncio.create_task(run_swarm_and_emit_fn(
                thread_id, "В каком формате загружать анализы?"
            ))
        elif button_id == "unique_features":
            asyncio.create_task(run_swarm_and_emit_fn(
                thread_id, "Какие мои уникальные возможности?"
            ))
        elif button_id == "start_decode":
            asyncio.create_task(run_swarm_and_emit_fn(
                thread_id, "Расшифровать анализы"
            ))
        elif button_id == "files_selected":
            # Пользователь выбрал файлы (пришли через upload_files)
            pass  # обработано в upload_files
        else:
            # Любая другая кнопка — передаём её текст как сообщение
            text = payload.get("text", button_id)
            asyncio.create_task(run_swarm_and_emit_fn(
                thread_id, text
            ))

    @sio.event
    async def disconnect(sid):
        logger.info(f"Disconnect: {sid}")
        if sid in active_sessions:
            del active_sessions[sid]

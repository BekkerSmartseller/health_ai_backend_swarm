# health_ai_backend_swarm/services/swarm_runner.py
"""
Swarm runner — запуск swarm-графа с потоковой передачей событий через Socket.IO.
"""
import asyncio
import json
import re
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# Глобальные объекты (устанавливаются из main.py)
swarm_graph = None
memory_layer = None
langfuse_handler = None
sio = None


def init_swarm_runner(sg, ml, lf, socket_io):
    global swarm_graph, memory_layer, langfuse_handler, sio
    swarm_graph = sg
    memory_layer = ml
    langfuse_handler = lf
    sio = socket_io


def fix_mermaid_blocks(text: str) -> str:
    """Заменяет <br/> на \n внутри блоков ```mermaid ... ```"""
    pattern = r'(```mermaid\n)(.*?)(```)'

    def replace(match):
        content = match.group(2)
        content = re.sub(r'<br\s*/?>', '\n', content)
        return match.group(1) + content + match.group(3)
    return re.sub(pattern, replace, text, flags=re.DOTALL)


async def run_swarm_and_emit(
    thread_id: str,
    user_message: str,
    timezone: str = "UTC",
    locale: str = "en",
    location: dict | None = None,
):
    """Запускает swarm-граф и отправляет события через Socket.IO."""
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
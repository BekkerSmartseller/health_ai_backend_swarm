# health_ai_backend_swarm/graph/medical_swarm_workflow.py
"""
Медицинский многоагентный граф для расшифровки анализов.
Использует явный StateGraph ручную маршрутизацию — без LLM-супервизора,
чтобы исключить KeyError от сторонних агентов.
"""
import asyncio
import json
import logging
import re
from typing import Dict, List, Optional, Any, Literal

from clients.deepseek_client_class import ChatDeepSeek
from langchain.agents import create_agent
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph, MessagesState, START, END
from typing_extensions import TypedDict

from config import config as app_config
from services.hindsight_client import HindsightClient
from services.file_processor import FileProcessor
from services.web_search_tavily import TavilySearchClient

logger = logging.getLogger(__name__)

# ==================== Глобальные ссылки ====================
hindsight_client: Optional[HindsightClient] = None
file_processor: Optional[FileProcessor] = None
tavily_client: Optional[TavilySearchClient] = None
sio = None

FAQ_BANK_ID = "faq-assistant"
NORM_BLOOD_BANK_ID = "norm-blood"


def init_medical_globals(hc, fp, tc, socket_io):
    global hindsight_client, file_processor, tavily_client, sio
    hindsight_client = hc
    file_processor = fp
    tavily_client = tc
    sio = socket_io


# ==================== LLM ====================
llm = ChatDeepSeek(
    base_url=app_config.DEEPSEEK_BASE_URL,
    api_key=app_config.DEEPSEEK_API_KEY,
    model=app_config.DEEPSEEK_MODEL,
    temperature=0.0,
    max_tokens=20000,
    extra_body={"thinking": {"type": "enabled"}}
)


# ==================== Tools ====================

async def assistant_faq_tool(query: str) -> str:
    """Ищет ответы на вопросы о возможностях ассистента в FAQ-банке Hindsight."""
    try:
        results = await hindsight_client.faq_query(FAQ_BANK_ID, query, limit=3)
        if results:
            for r in results:
                try:
                    data = json.loads(r)
                    if isinstance(data, dict) and data.get("answer"):
                        return data["answer"]
                except (json.JSONDecodeError, TypeError):
                    pass
            return results[0]
        return "Информация не найдена."
    except Exception as e:
        logger.error(f"FAQ tool error: {e}")
        return "Ошибка при поиске в FAQ."


async def check_results_analysis(raw_data: str) -> str:
    """Фильтрует медицинские анализы: отделяет тесты от мусора, проверяет метаданные пациента."""
    try:
        data = json.loads(raw_data)
    except (json.JSONDecodeError, TypeError):
        data = {"analyses": [], "raw_text": str(raw_data)}
    analyses = data.get("analyses", [])
    patient = data.get("patient")
    valid = [a for a in analyses if isinstance(a, dict) and a.get("test_name")]
    ignored = [a for a in analyses if a not in valid]
    patient_info = patient or {}
    result = {
        "total_found": len(analyses), "valid_count": len(valid), "ignored_count": len(ignored),
        "analyses": valid, "ignored_items": ignored, "patient": patient_info,
        "needs_patient_info": not (patient_info.get("name") and patient_info.get("age") and patient_info.get("sex")),
        "missing_fields": [k for k in ["name", "age", "sex", "date"] if not patient_info.get(k)]
    }
    return json.dumps(result, ensure_ascii=False)


async def verify_interpretation(report: str) -> str:
    """Проверяет качество интерпретации анализов."""
    try:
        data = json.loads(report)
    except (json.JSONDecodeError, TypeError):
        data = {"raw": str(report)}
    checks = {
        "has_recommendations": bool(data.get("recommendations")),
        "has_deviations": bool(data.get("deviations")),
        "is_complete": len(data.get("deviations", [])) > 0 or data.get("status") == "normal",
    }
    issues = []
    if not checks["has_recommendations"]: issues.append("Нет рекомендаций")
    if not checks["has_deviations"] and data.get("status") != "normal": issues.append("Нет отклонений")
    if not checks["is_complete"]: issues.append("Неполная")
    quality = "approved" if len(issues) <= 1 else "needs_review"
    return json.dumps({"quality": quality, "checks": checks, "issues": issues}, ensure_ascii=False)


async def web_search_tavily_tool(query: str) -> str:
    """Ищет информацию о препаратах через Tavily API."""
    try:
        result = await tavily_client.search_medical(query)
        try:
            await hindsight_client.save_web_search_result(NORM_BLOOD_BANK_ID, query, result)
        except Exception as e:
            logger.warning(f"Save web result: {e}")
        return result
    except Exception as e:
        logger.error(f"Web search error: {e}")
        return f"Ошибка поиска: {str(e)}"


# ==================== Агенты ====================
dispatcher_agent = create_agent(
    model=llm,
    tools=[assistant_faq_tool],
    name="med_dispatcher",
    system_prompt=(
        "Ты главный диспетчер медицинского ассистента.\n"
        "Правила:\n"
        "1. Приветствуй пользователя с кнопками: Загрузить анализы | Формат загрузки | Мои возможности\n"
        "2. Если вопрос о возможностях ассистента → используй assistant_faq_tool\n"
        "3. Если 'Загрузить анализы' → дай инструкцию\n"
        "4. Если просят расшифровать → ответь что передашь аналитику\n"
        "5. Если о препарате/нутрицевтике → ответь что передашь веб-поиску\n"
        "6. На общие вопросы отвечай сам\n"
        "Кнопки в формате: [Кнопка: текст]\n"
        "Не извиняйся. Используй русский язык."
    )
)

analyst_agent = create_agent(
    model=llm,
    tools=[check_results_analysis],
    name="med_analyst",
    system_prompt=(
        "Ты медицинский аналитик.\n"
        "1. Запусти check_results_analysis с данными от диспетчера\n"
        "2. Сообщи сколько найдено/пропущено\n"
        "3. Если нужны метаданные пациента → запроси\n"
        "Не извиняйся. Используй русский язык."
    )
)

interpreter_agent = create_agent(
    model=llm,
    tools=[verify_interpretation],
    name="med_interpreter",
    system_prompt=(
        "Ты медицинский интерпретатор.\n"
        "1. Сравни каждый показатель с нормой\n"
        "2. Объясни отклонения\n"
        "3. Дай общие рекомендации\n"
        "4. Запусти verify_interpretation\n"
        "5. Выдай итог\n"
        "Не ставь диагнозы. Рекомендуй врача.\n"
        "Не извиняйся. Используй русский язык."
    )
)

web_agent = create_agent(
    model=llm,
    tools=[web_search_tavily_tool],
    name="med_web",
    system_prompt=(
        "Ты агент веб-поиска.\n"
        "1. Найди информацию о препарате/нутрицевтике\n"
        "2. Составь описание с источниками\n"
        "Не давай рекомендаций.\n"
        "Не извиняйся. Используй русский язык."
    )
)


# ==================== Состояние + routing ====================
class AgentState(TypedDict):
    messages: list
    next_agent: Optional[str]


def router(state: AgentState) -> str:
    """Определяет следующий агент на основе контента последнего сообщения."""
    messages = state.get("messages", [])
    if not messages:
        return "med_dispatcher"
    last = messages[-1]
    content = ""
    if hasattr(last, "content") and last.content:
        content = last.content
    elif isinstance(last, dict) and last.get("content"):
        content = last["content"]
    
    content_lower = content.lower()
    
    # Аналитик — когда речь об обработке файлов/анализов
    if "анализ" in content_lower or "загруж" in content_lower or "файл" in content_lower:
        return "med_analyst"
    
    # Веб-поиск — когда о препарате или нутрицевтике
    if "препарат" in content_lower or "нутрицевтик" in content_lower or "лекарств" in content_lower:
        return "med_web"
    
    # Интерпретатор — когда расшифровка
    if "расшифр" in content_lower or "интерпрет" in content_lower:
        return "med_interpreter"
    
    # По умолчанию — диспетчер
    return "med_dispatcher"


# ==================== Сборка графа ====================
from graph.checkpointer import get_postgres_checkpointer

async def get_compiled_medical_graph():
    """Компилирует медицинский граф с PostgreSQL checkpointer в схеме medexpertai_checkpointer."""
    checkpointer = await get_postgres_checkpointer(
        max_stored_messages=100,
        max_context_messages=20
    )
    
    builder = StateGraph(AgentState)
    
    builder.add_node("med_dispatcher", dispatcher_agent)
    builder.add_node("med_analyst", analyst_agent)
    builder.add_node("med_interpreter", interpreter_agent)
    builder.add_node("med_web", web_agent)
    
    # Из START → dispatcher
    builder.add_conditional_edges(START, router)
    # Каждый агент завершает обработку — возвращаемся к концу
    for agent in ["med_dispatcher", "med_analyst", "med_interpreter", "med_web"]:
        builder.add_edge(agent, END)
    
    graph = builder.compile(checkpointer=checkpointer)
    logger.info("Medical graph compiled with StateGraph (no LLM supervisor, single pass).")
    return graph


# ==================== Запуск с streaming ====================
async def run_medical_swarm_and_emit(
    thread_id: str,
    user_message: str,
    timezone: str = "UTC",
    locale: str = "en",
    location: dict | None = None,
):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    room = thread_id
    graph = await get_compiled_medical_graph()

    try:
        try:
            tz = ZoneInfo(timezone)
        except Exception:
            tz = ZoneInfo("UTC")
        now_local = datetime.now(tz)
        context = f"[Контекст: {now_local.strftime('%H:%M:%S')} {now_local.strftime('%Y-%m-%d')} ЧП: {timezone}]"
        if location:
            context += f" локация: {location['lat']},{location['lon']}"
        context += "\n"

        bank_id = f"user_{thread_id}"
        
        # Сохраняем сообщение пользователя
        try:
            await hindsight_client.retain(bank_id, f"[user]: {user_message}", metadata={
                "role": "user", "thread_id": thread_id, "timestamp": datetime.now().isoformat()
            })
        except Exception as e:
            logger.warning(f"Failed to save user message: {e}")

        configurable = {"configurable": {"thread_id": thread_id}}
        input_data = {"messages": [{"role": "user", "content": context + user_message}], "next_agent": None}

        reasoning_buffer = ""
        last_send_time = asyncio.get_event_loop().time()

        async def flush_reasoning():
            nonlocal reasoning_buffer
            if reasoning_buffer and sio:
                await sio.emit("reasoning_chunk", {"content": reasoning_buffer}, room=room)
                reasoning_buffer = ""

        try:
            async for event in graph.astream_events(input_data, config=configurable, version="v2"):
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
                    name = event.get("name", "")
                    msg = ""
                    if name == "assistant_faq_tool":
                        msg = "📖 **Ищу в базе знаний...**\n\n"
                    elif name == "check_results_analysis":
                        msg = "🔬 **Анализирую результаты...**\n\n"
                    elif name == "verify_interpretation":
                        msg = "✅ **Проверяю интерпретацию...**\n\n"
                    elif name == "web_search_tavily_tool":
                        msg = "🌐 **Ищу в интернете...**\n\n"
                    else:
                        msg = f"🔧 **{name}**\n\n"
                    if msg and sio:
                        await flush_reasoning()
                        await sio.emit("reasoning_chunk", {"content": msg}, room=room)

        finally:
            await flush_reasoning()

        # Получаем финальный ответ
        final_state = await graph.aget_state(configurable)
        final_msgs = final_state.values.get("messages", [])
        final_answer = ""
        if final_msgs:
            last = final_msgs[-1]
            if hasattr(last, "content") and last.content:
                final_answer = last.content
            elif isinstance(last, dict) and last.get("content"):
                final_answer = last["content"]

        if final_answer.strip() and sio:
            import unicodedata
            final_answer = unicodedata.normalize('NFC', final_answer)
            final_answer = re.sub(r'<br\s*/?>', '\n', final_answer)

            await sio.emit("stream_start", room=room)
            for i in range(0, len(final_answer), 20):
                await sio.emit("stream_chunk", {"content": final_answer[i:i+20]}, room=room)
                await asyncio.sleep(0.02)
            await sio.emit("stream_end", room=room)

            try:
                await hindsight_client.retain(bank_id, f"[assistant]: {final_answer}", metadata={
                    "role": "assistant", "thread_id": thread_id, "timestamp": datetime.now().isoformat()
                })
            except Exception as e:
                logger.warning(f"Save error: {e}")
        elif sio:
            await sio.emit("error", {"message": "Пустой ответ"}, room=room)

    except Exception as e:
        logger.error(f"Medical swarm error: {e}", exc_info=True)
        if sio:
            await sio.emit("error", {"message": f"Ошибка: {str(e)}"}, room=room)
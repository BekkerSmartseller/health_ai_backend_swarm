# ================================
# graph/swarm_workflow.py
# ================================
"""
Multi-Agent System with Aggregation, Web Search, Langfuse Tracing,
Automatic Fact-Checking and Recursion Limit (Async Version)
- Добавлена автоматическая проверка достоверности через инструмент fact_check
- Ограничение числа шагов (recursion_limit = 12)
- Полностью асинхронное выполнение
"""

import asyncio
import functools
import logging
import re
import os
import urllib.parse
from typing import List, Optional, Any
from dotenv import load_dotenv

# DeepSeek импорты
from clients.deepseek_client_class import ChatDeepSeek

from langchain.agents import create_agent
from langgraph_swarm import create_handoff_tool, create_swarm
from langgraph.checkpoint.memory import InMemorySaver

from ddgs import DDGS
from langchain_community.document_loaders import AsyncChromiumLoader
from langchain_community.document_transformers import Html2TextTransformer

from langfuse.langchain import CallbackHandler

from config import config as app_config

os.environ['USER_AGENT'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
load_dotenv()

# ===================== НАСТРОЙКА ЛОГИРОВАНИЯ =====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ===================== ДЕКОРАТОРЫ ЛОГИРОВАНИЯ =====================
def log_tool_async(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        tool_name = func.__name__
        arg_str = str(args[0]) if args else str(kwargs)
        preview = arg_str[:100] + "..." if len(arg_str) > 100 else arg_str
        logger.info(f"🔧 ASYNC TOOL: {tool_name}({preview})")
        try:
            result = await func(*args, **kwargs)
            result_str = str(result)
            preview_res = result_str[:100] + "..." if len(result_str) > 100 else result_str
            logger.info(f"✅ ASYNC TOOL {tool_name} returned: {preview_res}")
            return result
        except Exception as e:
            logger.error(f"Ошибка в {tool_name}: {e}")
            return str(e)
    return wrapper

# ===================== ИНИЦИАЛИЗАЦИЯ LANGFUSE =====================
langfuse_handler = CallbackHandler()

# ===================== МОДЕЛЬ =====================
llm = ChatDeepSeek(
    base_url=app_config.DEEPSEEK_BASE_URL,
    api_key=app_config.DEEPSEEK_API_KEY,
    model=app_config.DEEPSEEK_MODEL,
    temperature=0.0,
    max_tokens=20000,
    extra_body={"thinking": {"type": "enabled"}}
)

# ===================== ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ =====================
def extract_language_and_text(request: str) -> tuple[str, Optional[str]]:
    """Извлекает текст и целевой язык из запроса перевода."""
    request_lower = request.lower()
    patterns = [" to ", " in ", " into ", " на ", " переведи на ", " как будет на "]
    target_language = None
    text = request_lower
    for pattern in patterns:
        if pattern in request_lower:
            parts = request_lower.split(pattern)
            if len(parts) == 2:
                text = parts[0].strip()
                target_language = parts[1].strip()
                break
    for word in ["переведи", "translate", "скажи", "say", "как будет"]:
        text = text.replace(word, "")
    text = text.strip()
    if not text:
        text = request
        for word in ["переведи", "translate"]:
            text = text.replace(word, "").strip()
    return text, target_language

# ===================== АСИНХРОННЫЕ ИНСТРУМЕНТЫ =====================
@log_tool_async
async def translate_text_tool(request: str) -> str:
    """Переводит текст. Если язык не указан, просит уточнить."""
    if any(x in request.lower() for x in ["help", "помоги"]):
        return "Форматы перевода:\n- 'translate текст to язык'\n- 'текст in язык'\n- 'переведи текст на язык'"
    text, target_language = extract_language_and_text(request)
    if not target_language:
        return f"Укажите целевой язык. Пример: 'translate {text or 'текст'} to english'"
    if not text:
        return "Укажите текст для перевода."
    prompt = f"Translate '{text}' to {target_language}. Return only the translation."
    response = await llm.ainvoke(prompt)
    return response.content

@log_tool_async
async def scientific_explanation_tool(text: str) -> str:
    """Научное обоснование фразы/понятия."""
    prompt = (
        f'Дай научное обоснование фразе/понятию: "{text}" с точки зрения биологии, физики, химии, нейронауки.\n'
        "Ответь на русском, структурированно, с подзаголовками."
    )
    response = await llm.ainvoke(prompt)
    return response.content

@log_tool_async
async def ask_science(question: str) -> str:
    """Вызов научного агента (как инструмент)."""
    system_prompt = (
        "Ты научный эксперт по физике, химии, биологии, астрономии. "
        "Отвечай подробно, с фактами и примерами. Никогда не извиняйся."
    )
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": question}
    ]
    response = await llm.ainvoke(messages)
    return response.content

@log_tool_async
async def ask_qa(question: str) -> str:
    """Общий вопрос (как инструмент)."""
    system_prompt = "Ты полезный ассистент по общим вопросам. Отвечай кратко и по делу."
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": question}
    ]
    response = await llm.ainvoke(messages)
    return response.content

# ===================== АСИНХРОННЫЙ ВЕБ-ПОИСК =====================
SKIP_DOMAINS = {
    'wikipedia.org', 'wikimedia.org', 'wikidata.org', 'mediawiki.org',
    'youtube.com', 'youtu.be', 'tiktok.com', 'facebook.com', 'instagram.com'
}

@log_tool_async
async def web_search(query: str, num_results: int = 3) -> str:
    """Поиск через DuckDuckGo (в потоке) и асинхронная загрузка страниц."""
    def _search():
        ddgs = DDGS()
        try:
            return list(ddgs.text(query, max_results=num_results * 2))
        except Exception:
            return []

    loop = asyncio.get_running_loop()
    results = await loop.run_in_executor(None, _search)

    if not results:
        return f"По запросу '{query}' ничего не найдено."

    filtered = [r for r in results if not any(domain in r.get('href', '') for domain in SKIP_DOMAINS)]
    results = filtered[:num_results]
    if not results:
        return "Результаты поиска содержат только нежелательные сайты. Попробуйте другой запрос."

    urls = [r['href'] for r in results]
    loaders = [
        AsyncChromiumLoader([url], user_agent=os.environ.get('USER_AGENT', 'Mozilla/5.0'))
        for url in urls
    ]
    loaded_docs = await asyncio.gather(*[loader.aload() for loader in loaders], return_exceptions=True)

    transformer = Html2TextTransformer()
    output = f"🔍 Поиск: {query}\n\n"
    for i, r in enumerate(results):
        output += f"**{i+1}. {r['title']}**\n"
        output += f"🔗 {r['href']}\n"
        output += f"📌 Краткий сниппет: {r['body'][:200]}\n"
        if i < len(loaded_docs) and not isinstance(loaded_docs[i], Exception):
            docs = loaded_docs[i]
            if docs:
                try:
                    transformed = transformer.transform_documents(docs)
                    if transformed:
                        raw_text = transformed[0].page_content
                        if raw_text.count('{') > 50:
                            output += "⚠️ Страница содержит динамический контент, не извлечённый в читаемый текст.\n"
                        else:
                            clean_text = re.sub(r'\n{3,}', '\n\n', raw_text)[:1500]
                            output += f"📄 Текст со страницы:\n{clean_text}\n"
                    else:
                        output += "⚠️ Не удалось извлечь текст.\n"
                except Exception as e:
                    output += f"⚠️ Ошибка обработки страницы: {e}\n"
        else:
            output += "⚠️ Страница не загрузилась.\n"
        output += "\n---\n\n"
    return output

# ===================== НОВЫЙ ИНСТРУМЕНТ: ПРОВЕРКА ДОСТОВЕРНОСТИ =====================
@log_tool_async
async def fact_check(statement: str, search_results_summary: str) -> str:
    """Автоматическая проверка достоверности утверждения на основе результатов поиска."""
    prompt = f"""
Ты — критический факт-чекер. Оцени достоверность следующего утверждения на основе предоставленного контекста (результаты поиска).

УТВЕРЖДЕНИЕ: {statement}

КОНТЕКСТ (выдержки из источников):
{search_results_summary}

Оцени по следующим критериям:
- Авторитетность источников (домены .gov, .edu, известные научные журналы – высокое доверие)
- Свежесть данных (если старше 3 лет для быстро меняющихся тем – снижает доверие)
- Наличие противоречий между источниками
- Внутренняя согласованность

Ответь строго в формате JSON (без лишнего текста):
{{
  "verdict": "достоверно|сомнительно|ложно|недостаточно данных",
  "confidence": 0.0-1.0,
  "reasons": ["причина1", "причина2"],
  "suggested_next_search": "уточняющий запрос, если нужно (или пустая строка)"
}}
"""
    response = await llm.ainvoke(prompt)
    return response.content

# ===================== ИНСТРУМЕНТЫ HANDOFF =====================
handoff_to_science = create_handoff_tool(agent_name="science_agent")
handoff_to_translator = create_handoff_tool(agent_name="translator_agent")
handoff_to_qa = create_handoff_tool(agent_name="question_answering_agent")

# ===================== СОЗДАНИЕ АГЕНТОВ =====================
question_answering_agent = create_agent(
    model=llm,
    tools=[web_search, fact_check, handoff_to_science, handoff_to_translator],
    name="question_answering_agent",
    system_prompt=(
        "Ты — главный помощник, который отвечает на общие вопросы и помогает с поиском актуальной информации. "
        "Ты НЕ отвечаешь на научные вопросы и не занимаешься переводами — для этого есть другие агенты. "
        "Твоя задача — быть максимально полезным и прозрачным: объяснять пользователю, как ты ищешь информацию, и показывать свои рассуждения.\n\n"
        "## Правила работы\n"
        "1. Если вопрос требует актуальных данных, используй `web_search`.\n"
        "2. Перед поиском озвучь план пользователю.\n"
        "3. Ты можешь выполнять несколько поисков с разными запросами.\n"
        "4. **ОБЯЗАТЕЛЬНО после каждого вызова `web_search` вызывай `fact_check`**, передавая туда ключевое утверждение из найденного текста и сами результаты.\n"
        "5. Анализируй результаты `fact_check`: если `confidence` < 0.7 или `verdict` = 'сомнительно'/'недостаточно данных' — сделай дополнительный поиск, используя `suggested_next_search`.\n"
        "6. Сообщай о количестве найденных страниц и сколько откроешь.\n"
        "7. Составляй развёрнутый структурированный ответ с таблицами.\n"
        "8. Указывай источники и обоснование выбора.\n"
        "9. На запрос «топ 100» дай лучшие 10–15 позиций.\n"
        "10. Прогноз погоды смотри прежде всего на Яндекс Погоде и Gismeteo.\n\n"
        "11. При поиске погоды и любых других данных всегда используй дату и локацию из контекста, указанного в сообщении пользователя. Но учти, что пользователь может использовать прокси и его нахождение может быть не реальным и соответственно часовой пояс.\n\n"
        "12. Не придумывай год и не полагайся на внутренние знания о дате.\n\n"
        "## Важные замечания\n"
        "- Не извиняйся за передачу другому агенту — просто делай handoff.\n"
        "- Если вопрос не требует поиска, отвечай кратко и по делу.\n"
        "- Используй форматирование: таблицы, заголовки, списки.\n"
        "- Будь полезным и понятным."
    )
)

science_agent = create_agent(
    model=llm,
    tools=[ask_qa, web_search, fact_check, handoff_to_qa, handoff_to_translator],
    system_prompt=(
        "Ты научный эксперт. Отвечаешь на научные вопросы.\n"
        "## Правила работы\n"
        "1. Если вопрос требует актуальных данных, используй `web_search`.\n"
        "2. Перед поиском озвучь план пользователю.\n"
        "3. Ты можешь выполнять несколько поисков с разными запросами.\n"
        "4. **ОБЯЗАТЕЛЬНО после каждого вызова `web_search` вызывай `fact_check`**.\n"
        "5. Анализируй результаты `fact_check`: при низкой достоверности делай дополнительный поиск.\n"
        "6. Сообщай о количестве найденных страниц и сколько откроешь.\n"
        "7. Составляй развёрнутый структурированный ответ с таблицами.\n"
        "8. Указывай источники и обоснование выбора.\n"
        "9. Для чисто научных вопросов отвечай сам. Не извиняйся.\n"
        "10. Ты НЕ отвечаешь на перевод текста — для этого есть другой агент.\n"
        "## Важные замечания\n"
        "- Не извиняйся за передачу другому агенту — просто делай handoff.\n"
    ),
    name="science_agent"
)

translator_agent = create_agent(
    model=llm,
    tools=[
        translate_text_tool, scientific_explanation_tool,
        ask_science, ask_qa, fact_check,
        handoff_to_qa, handoff_to_science
    ],
    system_prompt=(
        "Ты агент перевода. Если просят перевод + научное обоснование: "
        "сначала вызови translate_text_tool, затем scientific_explanation_tool (с оригинальным текстом), "
        "объедини результаты. Для науки вызывай ask_science, только для общих вопросов — есть другой агент. "
        "Ты НЕ отвечаешь на научные вопросы и не отвечаешь на общие вопросы — для этого есть другие агенты.\n"
        "Если в процессе перевода встречаешь неоднозначный термин, используй `fact_check` для проверки.\n"
        "## Важные замечания\n"
        "- Не извиняйся за передачу другому агенту — просто делай handoff.\n"
    ),
    name="translator_agent"
)

# ===================== КОМПИЛЯЦИЯ SWARM (АСИНХРОННАЯ, С POSTGRES) =====================
# Удаляем старую глобальную компиляцию с InMemorySaver.
# Вместо этого создаём фабрику.

from graph.checkpointer import get_postgres_checkpointer

_compiled_graph = None

async def get_compiled_graph():
    """Асинхронно создаёт и возвращает скомпилированный swarm-граф с PostgreSQL checkpointer."""
    global _compiled_graph
    if _compiled_graph is None:
        checkpointer = await get_postgres_checkpointer()
        supervisor = create_swarm(
            agents=[question_answering_agent, science_agent, translator_agent],
            default_active_agent="question_answering_agent",
        )
        _compiled_graph = supervisor.compile(checkpointer=checkpointer)
        logger.info("Swarm graph compiled with PostgreSQL checkpointer")
    return _compiled_graph

def get_swarm_graph():
    """
    Синхронная обёртка для использования в старом коде.
    В новом асинхронном сервере используйте await get_compiled_graph().
    """
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        if _compiled_graph is not None:
            return _compiled_graph
        raise RuntimeError("Cannot call get_swarm_graph() from async context. Use await get_compiled_graph() instead.")
    return asyncio.run(get_compiled_graph())

# ===================== АСИНХРОННАЯ ОБЁРТКА С РЕКУРСИВНЫМ ЛИМИТОМ =====================
async def invoke_with_logging(app, input_messages, config_wo_callbacks):
    """Вызов swarm-приложения с логированием и ограничением числа шагов."""
    user_msg = input_messages["messages"][0]["content"]
    logger.info(f"👤 ЗАПРОС: {user_msg[:150]}")
    full_config = {
        **config_wo_callbacks,
        "callbacks": [langfuse_handler],
        "recursion_limit": 12
    }
    try:
        result = await app.ainvoke(input_messages, full_config)
    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        return {"messages": [type('msg', (), {'type': 'ai', 'name': 'system', 'content': f'Произошла ошибка: {e}'})]}
    last_msg = result["messages"][-1]
    if hasattr(last_msg, "name") and last_msg.name:
        logger.info(f"🤖 ОТВЕТИЛ АГЕНТ: {last_msg.name}")
    tool_calls = set()
    for msg in result["messages"]:
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            for tc in msg.tool_calls:
                tool_calls.add(tc["name"])
    if tool_calls:
        logger.info(f"📞 ИСПОЛЬЗОВАНЫ ИНСТРУМЕНТЫ: {', '.join(tool_calls)}")
    return result

# ===================== ГЛАВНЫЙ ЦИКЛ ДЛЯ ТЕСТИРОВАНИЯ =====================
async def main():
    graph = await get_compiled_graph()
    thread_config = {"configurable": {"thread_id": "1"}}
    print("🚀 Система с агрегацией, поиском, проверкой достоверности и ограничением шагов запущена (async).")
    print("   (максимум 12 итераций агент-инструменты, автоматический fact-check после поиска)")
    print("   Введите 'exit' для выхода.\n")
    while True:
        try:
            user_input = input("Вы: ")
        except (EOFError, KeyboardInterrupt):
            print("\nДо свидания!")
            break
        if user_input.lower() == "exit":
            print("До свидания!")
            break
        result = await invoke_with_logging(
            graph,
            {"messages": [{"role": "user", "content": user_input}]},
            thread_config
        )
        last_message = result["messages"][-1]
        if last_message.type == "ai":
            print(f"\n{last_message.name}: {last_message.content}\n")
        else:
            print(f"\n{last_message.content}\n")

if __name__ == "__main__":
    asyncio.run(main())
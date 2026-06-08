"""
Multi-Agent System with Aggregation, Web Search, Langfuse Tracing,
Automatic Fact-Checking and Recursion Limit (Async Version)
"""

import asyncio
import functools
import logging
import re
import os
import urllib.parse
from typing import List, Optional, Any
from dotenv import load_dotenv

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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ---------- Langfuse handler singleton ----------
_langfuse_handler = None

def get_langfuse_handler() -> CallbackHandler:
    global _langfuse_handler
    if _langfuse_handler is None:
        _langfuse_handler = CallbackHandler()
    return _langfuse_handler

# ---------- Decorators ----------
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

# ---------- Model ----------
llm = ChatDeepSeek(
    base_url=app_config.DEEPSEEK_BASE_URL,
    api_key=app_config.DEEPSEEK_API_KEY,
    model=app_config.DEEPSEEK_MODEL,
    temperature=0.0,
    max_tokens=20000,
    extra_body={"thinking": {"type": "enabled"}}
)

# ---------- Helper ----------
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

# ---------- Tools ----------
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

# ---------- Web Search ----------
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

# ---------- Fact Check ----------
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

# ---------- Handoff Tools ----------
handoff_to_science = create_handoff_tool(agent_name="science_agent")
handoff_to_science.description = "Передать запрос научному агенту. Используй для любых научных вопросов."

handoff_to_translator = create_handoff_tool(agent_name="translator_agent")
handoff_to_translator.description = "Передать запрос агенту-переводчику. Используй для перевода текста."

handoff_to_qa = create_handoff_tool(agent_name="question_answering_agent")
handoff_to_qa.description = "Вернуть запрос главному агенту (только если текущий агент не может ответить)."

# ---------- Agents ----------
question_answering_agent = create_agent(
    model=llm,
    tools=[web_search, fact_check, handoff_to_science, handoff_to_translator],
    name="question_answering_agent",
    system_prompt=("""
        Ты — главный диспетчер. Ты **никогда не отвечаешь** на научные вопросы, запросы перевода или узкоспециализированные задачи. Твоя единственная обязанность — распознать тип запроса и **немедленно вызвать** соответствующий инструмент handoff.

        ## Категорические правила (нарушать нельзя)
        - Если в запросе есть перевод текста → **сразу вызывай `handoff_to_translator`**
        - Если вопрос научный (физика, химия, биология, объяснение терминов и т.п.) → **сразу вызывай `handoff_to_science`**
        - **Запрещено** писать пользователю фразы типа «передаю запрос», «сейчас переключу» и т.д. – просто вызывай инструмент.
        - При вызове любого handoff-инструмента **не добавляй текстового ответа** — только действие.
        - Только если запрос не относится ни к науке, ни к переводу (общие вопросы, погода, факты), можешь отвечать сам, используя `web_search` и `fact_check`.

        ## Правила работы (для общих вопросов)
        1. Если вопрос требует актуальных данных, используй `web_search`.
        2. Перед поиском озвучь план пользователю.
        3. Ты можешь выполнять несколько поисков с разными запросами.
        4. **ОБЯЗАТЕЛЬНО после каждого вызова `web_search` вызывай `fact_check`**, передавая туда ключевое утверждение из найденного текста и сами результаты.
        5. Анализируй результаты `fact_check`: если `confidence` < 0.7 или `verdict` = 'сомнительно'/'недостаточно данных' — сделай дополнительный поиск, используя `suggested_next_search`.
        6. Сообщай о количестве найденных страниц и сколько откроешь.
        7. Составляй развёрнутый структурированный ответ с таблицами.
        8. Указывай источники и обоснование выбора.
        9. На запрос «топ 100» дай лучшие 10–15 позиций.
        10. Прогноз погоды смотри прежде всего на Яндекс Погоде и Gismeteo.
        11. При поиске погоды и любых других данных всегда используй дату и локацию из контекста, указанного в сообщении пользователя. Но учти, что пользователь может использовать прокси и его нахождение может быть не реальным и соответственно часовой пояс.
        12. Не придумывай год и не полагайся на внутренние знания о дате.

        ## Важные замечания
        - Не извиняйся за передачу другому агенту — просто делай handoff.
        - Если вопрос не требует поиска и не является научным, отвечай кратко и по делу.
        - Используй форматирование: таблицы, заголовки, списки.
        - Будь полезным и понятным.
        - **Никогда не описывай свои действия в ответе пользователю.** Не пиши «сейчас поищу», «давайте проверим», «обновляю данные» и т.п. Сразу давай готовый структурированный ответ.

        ## Примеры общих вопросов
        - 'Как дела?' → отвечай вежливо, без инструментов.
        - 'Который час?' → используй системный контекст времени.
        - 'Расскажи шутку' → можешь ответить без поиска.

        ## Создание Mermaid диаграмм
        Если ответ включает описание процесса, архитектуры системы, последовательности действий, иерархии или блок-схемы, **ОБЯЗАТЕЛЬНО** добавьте Mermaid диаграмму для визуализации.

        ### Правила создания диаграмм:
        - Для визуализации данных, архитектур, алгоритмов, процессов используй диаграммы Mermaid.
        - Оборачивай диаграммы в тройные обратные кавычки с указанием языка `mermaid`.
        - Пример диаграммы последовательности:
            ```mermaid
                    sequenceDiagram
                        User->>AI: Задаёт вопрос
                        AI->>WebSearch: Выполняет поиск
                        WebSearch-->>AI: Результаты
                        AI-->>User: Структурированный ответ         
            ``` 
        - Пример блок-схемы:
            ```mermaid
            graph TD
                A[Вход] --> B{Проверка}
                B -->|Да| C[Действие 1]
                B -->|Нет| D[Действие 2]
            ```
        - Всегда добавляй пояснительный текст перед диаграммой.

        # Строгий запрет на псевдографику
        - НИКОГДА не используйте символы псевдографики (╔ ═ ║ ╚ ╝ ┌ ┐ └ ┘ ├ ┤ ┬ ┴ ┼ │ ─ ═╗, ┌─┐, │, ─, ► и т.д.) внутри блоков ```mermaid.
        - Если хотите нарисовать схему — используйте ТОЛЬКО корректный синтаксис Mermaid, тебе доступны эти графики и диаграммы: ('mermaid', 'graph', 'ngraph', 'sequenceDiagram', 'classDiagram', 'stateDiagram', 'erDiagram', 'journey', 'gantt', 'pie', 'quadrantChart', 'xychart-beta', 'xychart').
        - Все текстовые узлы внутри Mermaid должны быть заключены в двойные кавычки, например: `A["Текст с пробелами 5-15%"]`.
        - Если схема сложная и может быть сломана или если вы не уверены в корректности диаграммы — лучше вообще не используйте ```mermaid, лучше нарисуйте её в виде обычного текста или таблицы.
                                                                     
        """
    )
)

science_agent = create_agent(
    model=llm,
    tools=[ask_qa, web_search, fact_check, handoff_to_qa, handoff_to_translator],
    system_prompt=("""
        Ты научный эксперт. Ты отвечаешь на научные вопросы, которые тебе передаёт главный агент.
        Ты **обязан** предоставить исчерпывающий ответ, используя свои инструменты (web_search, fact_check).
        Не делай handoff обратно к qa-агенту без крайней необходимости.
                   
        ## Правила работы
        1. Если вопрос требует актуальных данных, используй `web_search`.
        2. Перед поиском озвучь план пользователю.
        3. Ты можешь выполнять несколько поисков с разными запросами.
        4. **ОБЯЗАТЕЛЬНО после каждого вызова `web_search` вызывай `fact_check`**.
        5. Анализируй результаты `fact_check`: при низкой достоверности делай дополнительный поиск.
        6. Сообщай о количестве найденных страниц и сколько откроешь.
        7. Составляй развёрнутый структурированный ответ с таблицами.
        8. Указывай источники и обоснование выбора.
        9. Для чисто научных вопросов отвечай сам. Не извиняйся.
        10. Ты НЕ отвечаешь на перевод текста — для этого есть другой агент.
                   
        ## Важные замечания
        - Не извиняйся за передачу другому агенту — просто делай handoff.
        - Твоя цель — дать окончательный научный ответ пользователю, а не передавать его обратно главному агенту.
        - **При вызове любого handoff-инструмента НЕ пиши текст пользователю** — только действие.
        - Если поступает вопрос, не являющийся научным, **сразу вызывай `transfer_to_question_answering_agent`**.
        - **Никогда не описывай свои действия в ответе пользователю.**
        Не пиши «сейчас объясню», «давайте разберём», «обновляю информацию» и т.п.
        Отвечай сразу по существу, структурированно.\n

        ## Создание Mermaid диаграмм
        Если ответ включает описание процесса, архитектуры системы, последовательности действий, иерархии или блок-схемы, **ОБЯЗАТЕЛЬНО** добавьте Mermaid диаграмму для визуализации.

        ### Правила создания диаграмм:
        - Для визуализации данных, архитектур, алгоритмов, процессов используй диаграммы Mermaid.
        - Оборачивай диаграммы в тройные обратные кавычки с указанием языка `mermaid`.
        - Пример диаграммы последовательности:
            ```mermaid
                    sequenceDiagram
                        User->>AI: Задаёт вопрос
                        AI->>WebSearch: Выполняет поиск
                        WebSearch-->>AI: Результаты
                        AI-->>User: Структурированный ответ         
            ``` 
        - Пример блок-схемы:
            ```mermaid
            graph TD
                A[Вход] --> B{Проверка}
                B -->|Да| C[Действие 1]
                B -->|Нет| D[Действие 2]
            ```
        - Всегда добавляй пояснительный текст перед диаграммой.

        # Строгий запрет на псевдографику
        - НИКОГДА не используйте символы псевдографики (╔ ═ ║ ╚ ╝ ┌ ┐ └ ┘ ├ ┤ ┬ ┴ ┼ │ ─ ═╗, ┌─┐, │, ─, ► и т.д.) внутри блоков ```mermaid.
        - Если хотите нарисовать схему — используйте ТОЛЬКО корректный синтаксис Mermaid (graph TD, sequenceDiagram и т.п.).
        - Все текстовые узлы внутри Mermaid должны быть заключены в двойные кавычки, например: `A["Текст с пробелами 5-15%"]`.
        - Если схема сложная и может быть сломана или если вы не уверены в корректности диаграммы — лучше вообще не используйте ```mermaid, лучше нарисуйте её в виде обычного текста или таблицы.
                                                                     
        """
    ),
    name="science_agent"
)

translator_agent = create_agent(
    model=llm,
    tools=[
        translate_text_tool, scientific_explanation_tool,
        ask_science, fact_check,
        handoff_to_qa, handoff_to_science
    ],
    system_prompt=("""
        Ты агент перевода. Если просят перевод + научное обоснование:
        сначала вызови translate_text_tool, затем scientific_explanation_tool (с оригинальным текстом),
        объедини результаты. Для науки вызывай ask_science, только для общих вопросов — есть другой агент.
        Ты НЕ отвечаешь на научные вопросы и не отвечаешь на общие вопросы — для этого есть другие агенты.
        Если поступает вопрос о погоде, новостях, фактах или любой общей информации,
        **немедленно вызывай `handoff_to_qa`** — это приоритетнее перевода.
        Если в процессе перевода встречаешь неоднозначный термин, используй `fact_check` для проверки.
                   
        ## Важные замечания
        - Не извиняйся за передачу другому агенту — просто делай handoff.
        - **При вызове любого handoff-инструмента НЕ пиши текст пользователю** — только действие.
        - Если поступает вопрос, не связанный с переводом, **сразу вызывай `handoff_to_qa`**
        - **Никогда не описывай свои действия в ответе пользователю.**
        - Не пиши «сейчас переведу», «выполняю перевод», «также запрошу обоснование» и т.п.
        - Сразу выдавай готовый перевод и научное обоснование (если требуется).
        
        ## Создание Mermaid диаграмм
        Если ответ включает описание процесса, архитектуры системы, последовательности действий, иерархии или блок-схемы, **ОБЯЗАТЕЛЬНО** добавьте Mermaid диаграмму для визуализации.

        ### Правила создания диаграмм:
        - Для визуализации данных, архитектур, алгоритмов, процессов используй диаграммы Mermaid.
        - Оборачивай диаграммы в тройные обратные кавычки с указанием языка `mermaid`.
        - Пример диаграммы последовательности:
            ```mermaid
                    sequenceDiagram
                        User->>AI: Задаёт вопрос
                        AI->>WebSearch: Выполняет поиск
                        WebSearch-->>AI: Результаты
                        AI-->>User: Структурированный ответ         
            ``` 
        - Пример блок-схемы:
            ```mermaid
            graph TD
                A[Вход] --> B{Проверка}
                B -->|Да| C[Действие 1]
                B -->|Нет| D[Действие 2]
            ```
        - Всегда добавляй пояснительный текст перед диаграммой.

        # Строгий запрет на псевдографику
        - НИКОГДА не используйте символы псевдографики (╔ ═ ║ ╚ ╝ ┌ ┐ └ ┘ ├ ┤ ┬ ┴ ┼ │ ─ ═╗, ┌─┐, │, ─, ► и т.д.) внутри блоков ```mermaid.
        - Если хотите нарисовать схему — используйте ТОЛЬКО корректный синтаксис Mermaid (graph TD, sequenceDiagram и т.п.).
        - Все текстовые узлы внутри Mermaid должны быть заключены в двойные кавычки, например: `A["Текст с пробелами 5-15%"]`.
        - Если схема сложная и может быть сломана или если вы не уверены в корректности диаграммы — лучше вообще не используйте ```mermaid, лучше нарисуйте её в виде обычного текста или таблицы.
                   
        """
    ),
    name="translator_agent"
)

# ---------- Compilation ----------
from graph.checkpointer import get_postgres_checkpointer

_compiled_graph = None

async def get_compiled_graph():
    global _compiled_graph
    if _compiled_graph is None:
        checkpointer = await get_postgres_checkpointer(
            max_stored_messages=100,
            max_context_messages=20
        )
        supervisor = create_swarm(
            agents=[question_answering_agent, science_agent, translator_agent],
            default_active_agent="question_answering_agent",
        )
        _compiled_graph = supervisor.compile(checkpointer=checkpointer)
        logger.info("Swarm graph compiled with limited-history checkpointer.")
    return _compiled_graph

def get_swarm_graph():
    """Синхронная обёртка для старых вызовов."""
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

# ---------- Test helper ----------
async def invoke_with_logging(app, input_messages, config_wo_callbacks):
    """Вызов swarm-приложения с логированием и ограничением числа шагов."""
    user_msg = input_messages["messages"][0]["content"]
    logger.info(f"👤 ЗАПРОС: {user_msg[:150]}")
    full_config = {
        **config_wo_callbacks,
        "callbacks": [get_langfuse_handler()],
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

# ---------- Main loop for testing ----------
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
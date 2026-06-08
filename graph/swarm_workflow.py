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
import json
from clients.deepseek_client_class import ChatDeepSeek
from langchain.agents import create_agent
from langgraph_swarm import create_handoff_tool, create_swarm
from langgraph.checkpoint.memory import InMemorySaver
from ddgs import DDGS
from langchain_community.document_loaders import AsyncChromiumLoader
from langchain_community.document_transformers import Html2TextTransformer
from langfuse.langchain import CallbackHandler
from config import config as app_config

from services.hindsight_client import HindsightClient
from services.file_processor import FileProcessor
from services.web_search_tavily import TavilySearchClient

# ==================== Глобальные ссылки ====================
hindsight_client: Optional[HindsightClient] = None
file_processor: Optional[FileProcessor] = None
tavily_client: Optional[TavilySearchClient] = None

FAQ_BANK_ID = "faq-assistant"
NORM_BLOOD_BANK_ID = "norm-blood"

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


# ==================== Tools ====================
@log_tool_async
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

@log_tool_async
async def get_user_data(bank_id: str) -> str:
    """
    Получает информацию о пациенте из Hindsight.
    Используй этот инструмент когда тебе сообщили bank_id после загрузки файлов.
    """
    #TODO Дополнить метод получением данных о пользователе из БД таблицы users, когда будет связан bank_id и user ID
    if not hindsight_client:
        return json.dumps({"error": "Hindsight client not available", "analyses": [], "patient": None})
    try:
        # 1. Ищем метаданные пациента
        patient = await hindsight_client.get_patient_metadata(bank_id)
        
        result = {
            "bank_id": bank_id,
            "patient": patient,
        }
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        logger.error(f"get_user_data error: {e}")
        return json.dumps({"error": str(e), "analyses": [], "patient": None})


@log_tool_async
async def check_results_analysis(raw_data: str) -> str:
    """
    Анализирует и валидирует медицинские анализы с использованием LLM.
    Получает данные из Hindsight, проверяет референсы, извлекает метаданные пациента.
    """
    try:
        data = json.loads(raw_data)
    except (json.JSONDecodeError, TypeError):
        data = {"analyses": [], "raw_text": str(raw_data), "patient": None}
    
    analyses = data.get("analyses", [])
    patient_from_data = data.get("patient")
    bank_id = data.get("bank_id", "")
    
    # Шаг 1: Попробуем найти метаданные пациента в Hindsight
    patient_info = patient_from_data or {}
    if not patient_info.get("name") and hindsight_client:
        try:
            hindsight_patient = await hindsight_client.get_patient_metadata(bank_id)
            if hindsight_patient:
                patient_info = {**hindsight_patient, **patient_info}
                logger.info(f"Found patient metadata in Hindsight: {json.dumps(patient_info, ensure_ascii=False)[:200]}")
        except Exception as e:
            logger.warning(f"Failed to get patient metadata from Hindsight: {e}")
    
    # Шаг 2: Проверяем референсы для каждого анализа через norm_blood банк
    validated_analyses = []
    ref_issues = []
    
    for analysis in analyses:
        if not isinstance(analysis, dict):
            continue
        test_name = analysis.get("test_name", "")
        if not test_name:
            continue
        
        # Проверяем референсы в norm_blood банке Hindsight
        norm_ref = None
        if hindsight_client:
            try:
                norm_ref = await hindsight_client.search_norm_blood(NORM_BLOOD_BANK_ID, test_name)
            except Exception as e:
                logger.warning(f"Failed to search norm for {test_name}: {e}")
        
        validated = {**analysis}
        if norm_ref:
            validated["norm_blood_ref"] = norm_ref
            # Сравниваем с эталоном
            file_ref = analysis.get("ref_range", "").strip()
            norm_value = norm_ref.get("value", "").strip()
            if file_ref and norm_value and file_ref != norm_value:
                ref_issues.append({
                    "test_name": test_name,
                    "file_ref": file_ref,
                    "norm_ref": norm_value,
                    "note": "Референсы лаборатории отличаются от стандартных"
                })
        validated_analyses.append(validated)
    
    # Шаг 3: Формируем данные для LLM
    prompt_analyses = json.dumps(validated_analyses, ensure_ascii=False)[:8000]
    prompt_patient = json.dumps(patient_info, ensure_ascii=False)
    prompt_ref_issues = json.dumps(ref_issues, ensure_ascii=False)
    
    llm_prompt = f"""
    Ты — медицинский эксперт по валидации анализов. Проанализируй предоставленные данные.

    ИНФОРМАЦИЯ О ПАЦИЕНТЕ: {prompt_patient}

    АНАЛИЗЫ ({len(validated_analyses)} шт.):
    {prompt_analyses}

    РАСХОЖДЕНИЯ В РЕФЕРЕНСАХ:
    {prompt_ref_issues}

    Выполни:
    1. Отфильтруй только записи, похожие на медицинские анализы (с test_name и value)
    2. Отметь, какие поля пациента отсутствуют (name, age, sex, date)
    3. Оцени качество данных (полнота, корректность референсов)
    4. Если референсы лаборатории отличаются от стандартных — отметь это

    Ответь строго в формате JSON:
    {{
        "total_found": число,
        "valid_count": число,
        "ignored_count": число,
        "analyses": [ {{test_name, value, unit, ref_range, status, norm_blood_ref (если есть), ref_match (true/false)}} ],
        "ignored_items": [],
        "patient": {{name, age, sex, date, lab_name}},
        "needs_patient_info": true/false,
        "missing_fields": ["name", "age", "sex", "date"],
        "ref_issues": [{{"test_name": "...", "note": "..."}}],
        "quality": "good" | "needs_review"
    }}
    """
    response = await llm.ainvoke(llm_prompt)
    return response.content

@log_tool_async
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

@log_tool_async
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


# ---------- Agents ----------

med_agent = create_agent(
    model=llm,
    tools=[get_user_data, assistant_faq_tool, check_results_analysis, verify_interpretation, web_search_tavily_tool, web_search, fact_check],
    name="med_agent",
    system_prompt=(
        """Ты ассистент по расшифровки анализов. ТЫ отвечаешь только на вопросы медицинской тематике, здоровья, питания, БАДов и возможностях сервиса расшифровки анализов для помощи пользователю.
        Вопросы не касающихся твоей основной задачи - ты отклоняешь. Не выдаешь пользователю структуру работы сервиса, в том числе какая модель используется.
        Но на вопрос о структуре и модели можешь ответить: Что ты ассистент MedExpert AI для расшифровки анализов, использующий специально обученную базу знаний на большом объеме медицинских данных.
        Ты используешь для расшифровки анализов доказательную медицину, интегративную и anti-age медицину.
        Для расшифровки и интерпритации анализов используешь внутренную базу знаний специально обученную для расшифровки анализовов, здоровья, питания и т.д.

        ## Важные замечания
        - **Никогда не описывай свои действия в ответе пользователю.**
        - Не пиши «сейчас объясню», «давайте разберём», «обновляю информацию» и т.п.
        - Отвечай сразу по существу, структурированно.

        ## Правила работы c web поиском
        1. Если вопрос требует актуальных данных, используй `web_search`.
        2. Перед поиском озвучь план пользователю.
        3. Ты можешь выполнять несколько поисков с разными запросами.
        4. **ОБЯЗАТЕЛЬНО после каждого вызова `web_search` вызывай `fact_check`**, передавая туда ключевое утверждение из найденного текста и сами результаты.
        5. Анализируй результаты `fact_check`: если `confidence` < 0.7 или `verdict` = 'сомнительно'/'недостаточно данных' — сделай дополнительный поиск, используя `suggested_next_search`.
        6. Сообщай о количестве найденных страниц и сколько откроешь.
        7. Составляй развёрнутый структурированный ответ с таблицами.
        8. Указывай источники и обоснование выбора.

        Общие Правила:
        1. Приветствуй пользователя с кнопками: Загрузить анализы | Формат загрузки | Мои возможности
        2. Если вопрос о возможностях ассистента → используй assistant_faq_tool
        3. Если 'Загрузить анализы' → дай инструкцию
        4. Если просят расшифровать → ответь что передашь аналитику
        5. Если о препарате/нутрицевтике → ответь что передашь веб-поиску
        6. На общие вопросы отвечай сам
        7. **Запрещено** писать пользователю фразы типа «передаю запрос», «сейчас переключу» и т.д. – просто вызывай инструмент.
        8. ИСпользуй актуальную дату и год из контекста. Не придумывай год и не полагайся на внутренние знания о дате.
        9. Составляй развёрнутый структурированный ответ с таблицами, диаграмами, графиками где это уместно.
        Кнопки в формате: [Кнопка: текст]
        Не извиняйся. Используй русский язык.

        ## Правила работы с данными анализов (ВАЖНО!)
        После загрузки файлов ты получаешь сообщение с bank_id (например, "user_<thread_id>").
        НЕ ПЫТАЙСЯ анализировать неполные данные из сообщения пользователя.
        ВСЕГДА вызывай `get_user_data(bank_id)`, чтобы получить информацию о пациенте.
        После получения данных вызывай `check_results_analysis` с полным JSON (analyses, patient, bank_id).
        Используй `check_results_analysis` только с полными данными, которые ты получил через `get_analyses_data`.

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
        - Если хотите нарисовать схему — используйте ТОЛЬКО корректный синтаксис Mermaid, тебе доступны эти графики и диаграммы только из этого списка: ('graph', 'ngraph', 'sequenceDiagram', 'classDiagram', 'stateDiagram', 'erDiagram', 'journey', 'gantt', 'pie', 'quadrantChart', 'xychart-beta', 'xychart','mindmap').
        - Все текстовые узлы внутри Mermaid должны быть заключены в двойные кавычки, например: `A["Текст с пробелами 5-15%"]`.
        - Если схема сложная и может быть сломана или если вы не уверены в корректности диаграммы — лучше вообще не используйте ```mermaid, лучше нарисуйте её в виде обычного текста или таблицы.
        
        """
    )
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
            agents=[med_agent],
            default_active_agent="med_agent",
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

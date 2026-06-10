# services/knowledge_search.py
"""
Умный поиск референсных значений в Hindsight через LLM.
LLM группирует показатели, формирует запросы, оценивает результаты,
предлагает уточняющие запросы, извлекает референсы из документов.
"""
import asyncio
import json
import logging
from typing import Dict, List, Optional, Any
from services.hindsight_client import HindsightClient

logger = logging.getLogger(__name__)

# LLM будет инициализирован из main.py
_llm = None
_max_attempts_per_group = 10
_max_total_documents = 50


def init_search_llm(llm):
    global _llm
    _llm = llm


async def _call_llm(prompt: str, system: str = "") -> str:
    """Вызывает LLM с заданным промптом."""
    if not _llm:
        raise RuntimeError("LLM not initialized. Call init_search_llm() first.")
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    response = await _llm.ainvoke(messages)
    return response.content


# ==================== ШАГ 1: LLM группирует показатели ====================

GROUP_PROMPT = """Ты — медицинский аналитик данных. Твоя задача — сгруппировать показатели анализов по типам исследований и сформировать точные поисковые запросы для получения референсных (нормальных) значений.

{patient_info}

Правила группировки:
- Каждая группа должна содержать логически связанные показатели (например: печёночные пробы, почечные пробы, липидный профиль, маркеры воспаления).
- Не смешивай в одной группе более 7-8 показателей — это ухудшает качество поиска.
- Если показателей много (более 15), разбей на 3-4 группы.

Формирование поискового запроса для группы:
- Запрос должен быть на русском языке, кратким (не более 10 слов).
- Всегда включай слова: "референсные значения", "норма", "диапазон".
- Обязательно используй пол и возраст пациента, например: "референсные значения биохимии крови для мужчин 21 год".
- НИКОГДА не включай в запрос название лаборатории, ФИО пациента или дату сдачи анализов.
- Добавь теги для поиска: как минимум "референсные интервалы", а также специфичные для группы (например "липидный профиль", "печёночные пробы").

Пример правильного запроса: "референсные значения печёночных проб АЛТ АСТ билирубин для мужчин 35 лет"
Пример неправильного: "нормы из лаборатории Инвитро для пациента Иванова"

Формат ответа (строго JSON):
{{
  "groups": [
    {{
      "group_name": "название группы (например, Биохимия печени)",
      "tests": ["точное название показателя1", "показатель2"],
      "search_query": "референсные значения ... для [пол] [возраст]",
      "search_tags": ["референсные интервалы", "дополнительный тег"]
    }}
  ]
}}

Список показателей для группировки:
{test_list}

Верни ТОЛЬКО JSON, без пояснений."""


async def llm_group_analyses(analyses: List[Dict], patient_info: Optional[Dict] = None) -> List[Dict]:
    """LLM группирует показатели и формирует поисковые запросы."""
    test_names = [a.get("test_name", "") for a in analyses if a.get("test_name")]
    patient_str = ""
    if patient_info:
        patient_str = f"\nПАЦИЕНТ: Пол={patient_info.get('sex', 'не указан')}, Возраст={patient_info.get('age', 'не указан')}"
    logger.info(f"\n{'='*60}")
    logger.info(f"🔬 ШАГ 1: Группировка {len(test_names)} показателей через LLM")
    logger.info(f"   Показатели: {test_names}")
    if patient_info:
        logger.info(f"   Пациент: пол={patient_info.get('sex')}, возраст={patient_info.get('age')}")

    prompt = GROUP_PROMPT.format(
        test_list=json.dumps(test_names, ensure_ascii=False, indent=2),
        patient_info=patient_str,
    )

    try:
        response = await _call_llm(prompt)
        logger.info(f"📥 Ответ LLM (первые 500): {response[:500]}")

        # Извлекаем JSON
        json_str = response.strip()
        if "```json" in json_str:
            json_str = json_str.split("```json")[1].split("```")[0].strip()
        elif "```" in json_str:
            json_str = json_str.split("```")[1].split("```")[0].strip()

        data = json.loads(json_str)
        groups = data.get("groups", [])

        logger.info(f"📊 Сформировано {len(groups)} групп:")
        for g in groups:
            logger.info(f"   - {g.get('group_name')}: {len(g.get('tests', []))} тестов")
            logger.info(f"     запрос: '{g.get('search_query')}'")
            logger.info(f"     тэги: {g.get('search_tags')}")

        return groups
    except Exception as e:
        logger.error(f"❌ Ошибка группировки LLM: {e}", exc_info=True)
        # Fallback: все в одну группу
        fallback = [{
            "group_name": "Все показатели",
            "tests": test_names,
            "search_query": "референсные значения медицинских анализов",
            "search_tags": ["референсные интервалы"],
        }]
        logger.info(f"⚠️ Использую fallback: 1 группа со всеми показателями")
        return fallback


# ==================== ШАГ 2: LLM оценивает результаты ====================

EVALUATE_PROMPT = """Ты — медицинский эксперт по проверке референсных интервалов. Оцени результаты поиска в базе знаний.

ЗАПРОС: {query}
{patient_info}
ИЩЕМ РЕФЕРЕНСНЫЕ ЗНАЧЕНИЯ ДЛЯ ПОКАЗАТЕЛЕЙ:
{test_names}

РЕЗУЛЬТАТЫ ПОИСКА (каждый результат содержит id и текст):
{results}

Критерии отбора:
1. Документ должен содержать ЧИСЛОВЫЕ ДИАПАЗОНЫ (например, "3.5-5.1", "менее 5.0") и единицы измерения.
2. Документ ДОЛЖЕН БЫТЬ СПРАВОЧНЫМ (таблицы норм, клинические рекомендации) — НЕ результатом анализов конкретного пациента.
3. Если в тексте есть слова "пациент", "сдал анализ", "результат", "повышен", "понижен" без указания "норма X-Y" — скорее всего, это не справочный документ.
4. Учитывай пол и возраст: если в документе указано "для взрослых" или нет возрастных ограничений — допустимо. Если указан другой пол или возраст — не подходит.

Что делать:
- Если найден документ с референсами для ВСЕХ или БОЛЬШИНСТВА показателей — укажи его ID и покрытые тесты.
- Если референсы найдены только для части показателей — отметь это в covered_tests, а для остальных предложи refined_query.
- Если НЕТ ни одного подходящего документа — found=false и предложи УТОЧНЁННЫЙ запрос, исключив уже найденные показатели.

Формат ответа JSON:
{{
  "found": true/false,
  "document_ids": ["id1"],
  "covered_tests": ["список показателей, для которых есть референсы"],
  "confidence": 0.0-1.0 (насколько уверены, что документ корректен и полон),
  "refined_query": "новый поисковый запрос для недостающих показателей (если нужно)",
  "refined_tags": ["тег1", "тег2"],
  "reason": "почему этот документ подходит или не подходит"
}}

Важно: Если found=false, refined_query ОБЯЗАТЕЛЕН.
Если покрыты не все показатели, refined_query должен содержать запрос для недостающих (с учётом пола и возраста)."""


async def llm_evaluate_results(query: str, test_names: List[str],
                                results: list, attempt: int,
                                patient_info: Optional[Dict] = None) -> Dict:
    """LLM оценивает, есть ли в результатах референсные значения."""
    patient_str = ""
    if patient_info:
        patient_str = f"\nПАЦИЕНТ: Пол={patient_info.get('sex', 'не указан')}, Возраст={patient_info.get('age', 'не указан')}"
    logger.info(f"\n   --- Шаг 2.{attempt}: Оценка результатов LLM ---")
    logger.info(f"   Запрос: {query}")
    logger.info(f"   Результатов: {len(results)}")
    if patient_info:
        logger.info(f"   Пациент: пол={patient_info.get('sex')}, возраст={patient_info.get('age')}")

    # Форматируем результаты для LLM
    results_text = []
    for i, r in enumerate(results):
        doc_id = r.get("document_id", r.get("id", f"result_{i}"))
        text = r.get("text", "")
        results_text.append(f"[{i+1}] ID={doc_id}\nТекст: {text}\n")
    results_str = "\n---\n".join(results_text)

    prompt = EVALUATE_PROMPT.format(
        query=query,
        patient_info=patient_str,
        test_names=json.dumps(test_names, ensure_ascii=False),
        results=results_str,
    )

    try:
        response = await _call_llm(prompt)
        logger.info(f"   📥 Оценка LLM (первые 300): {response[:300]}")

        json_str = response.strip()
        if "```json" in json_str:
            json_str = json_str.split("```json")[1].split("```")[0].strip()
        elif "```" in json_str:
            json_str = json_str.split("```")[1].split("```")[0].strip()

        evaluation = json.loads(json_str)
        logger.info(f"   📊 Оценка: found={evaluation.get('found')}, "
                    f"confidence={evaluation.get('confidence')}, "
                    f"docs={evaluation.get('document_ids')}")
        if evaluation.get("refined_query"):
            logger.info(f"   🔄 Уточнённый запрос: {evaluation.get('refined_query')}")
        return evaluation
    except Exception as e:
        logger.error(f"   ❌ Ошибка оценки LLM: {e}")
        return {"found": False, "refined_query": None, "document_ids": []}


# ==================== ШАГ 3: LLM извлекает референсы из документа ====================

EXTRACT_PROMPT = """Ты — медицинский эксперт по извлечению референсных значений из текста.

Из документа нужно извлечь числовые референсные интервалы (нормальные диапазоны) для указанных показателей, а также любые другие, которые там есть.

Требования к извлечению:
- Ищи паттерны: "норма X–Y", "референсные значения: от X до Y", "менее X", "X–Y (норма)".
- Обязательно извлекай единицы измерения (г/л, ммоль/л, ед/л, мкмоль/л и т.д.).
- Если для одного показателя указаны отдельные диапазоны для мужчин и женщин, заполни поля male и female.
- Если диапазон общий — заполни поле value.
- Если в документе нет референса для показателя — НЕ включай его в результат.
- Игнорируй текст, описывающий результаты конкретного пациента (например, "у пациента АлАт повышен до 65").

Пример корректного извлечения:
Показатель "АлАт": норма 0-40 ед/л → {"test_name": "АлАт", "value": "0-40", "unit": "ед/л"}

Документ (текст):
{document_text}

Показатели, которые нас интересуют в первую очередь:
{requested_tests}

Но извлеки ВСЕ референсы, которые есть в документе.

Формат ответа JSON:
{{
  "references": [
    {{
      "test_name": "точное название показателя",
      "male": "диапазон для мужчин (если есть)",
      "female": "диапазон для женщин (если есть)",
      "value": "общий диапазон",
      "unit": "единица измерения",
      "source": "источник (если указан)"
    }}
  ],
  "notes": "примечания (например, если референс дан для взрослых без разбивки по полу)"
}}

Верни ТОЛЬКО JSON."""


async def llm_extract_references(requested_tests: List[str], document_text: str) -> List[Dict]:
    """LLM извлекает референсные значения из полного текста документа."""
    logger.info(f"   --- Шаг 3: Извлечение референсов из документа ---")
    logger.info(f"   Ищем для {len(requested_tests)} показателей")

    prompt = EXTRACT_PROMPT.format(
        requested_tests=json.dumps(requested_tests, ensure_ascii=False),
        document_text=document_text[:8000],  # ограничение на токены
    )

    try:
        response = await _call_llm(prompt)
        json_str = response.strip()
        if "```json" in json_str:
            json_str = json_str.split("```json")[1].split("```")[0].strip()
        elif "```" in json_str:
            json_str = json_str.split("```")[1].split("```")[0].strip()

        data = json.loads(json_str)
        refs = data.get("references", [])
        logger.info(f"   📊 Извлечено {len(refs)} референсов")
        for r in refs:
            logger.info(f"      {r.get('test_name')}: {r.get('value') or r.get('male') or '?'} {r.get('unit', '')}")
        return refs
    except Exception as e:
        logger.error(f"   ❌ Ошибка извлечения референсов: {e}")
        return []


# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================

async def search_all_references(
    client: HindsightClient,
    bank_id: str,
    analyses: List[Dict],
    patient_info: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Главная функция: ищет референсные значения через LLM.
    """
    global _max_attempts_per_group, _max_total_documents

    logger.info(f"\n{'='*70}")
    logger.info(f"🔬 ЗАПУСК LLM-ПОИСКА РЕФЕРЕНСНЫХ ЗНАЧЕНИЙ")
    logger.info(f"   Банк: {bank_id}")
    logger.info(f"   Всего показателей: {len(analyses)}")
    logger.info(f"   Max попыток на группу: {_max_attempts_per_group}")
    logger.info(f"{'='*70}")

    # Шаг 1: LLM группирует
    groups = await llm_group_analyses(analyses, patient_info)
    if not groups:
        logger.error("❌ LLM не смогла сгруппировать показатели")
        return {"success": False, "error": "grouping_failed", "groups": []}

    # Шаг 2-3: Для каждой группы выполняем поиск и оценку
    all_group_results = []
    total_attempts = 0

    for group_idx, group in enumerate(groups):
        logger.info(f"\n{'='*60}")
        logger.info(f"📋 Группа {group_idx + 1}: {group.get('group_name')}")
        logger.info(f"{'='*60}")

        test_names = group.get("tests", [])
        query = group.get("search_query", "")
        tags = group.get("search_tags", ["референсные интервалы"])
        found_references = []
        used_attempts = 0
        current_query = query
        current_tags = tags

        for attempt in range(_max_attempts_per_group):
            if total_attempts >= _max_total_documents:
                logger.warning(f"⚠️ Достигнут лимит попыток ({_max_total_documents})")
                break

            used_attempts += 1
            total_attempts += 1

            logger.info(f"\n--- Попытка {attempt + 1}/{_max_attempts_per_group} ---")
            logger.info(f"   Запрос: {current_query}")
            logger.info(f"   Тэги: {current_tags}")

            # Выполняем recall
            try:
                results = await client.recall_raw(
                    bank_id,
                    query=current_query,
                    tags=current_tags if current_tags else None,
                    types=["world"],
                    max_tokens=4096 + attempt * 1000,
                    budget="high" if attempt > 3 else "mid",
                    include_entities=attempt > 1,
                    limit=25,
                )
            except Exception as e:
                logger.error(f"   ❌ Ошибка recall: {e}")
                continue

            if not results:
                logger.info(f"   ⚠️ Пустой ответ")
                # Пробуем другой запрос
                current_query = f"нормальные значения {group.get('group_name')} {' '.join(test_names[:3])}"
                current_tags = ["лабораторные референсные значения"]
                continue

            logger.info(f"   ✅ Получено {len(results)} результатов")

            # LLM оценивает результаты
            evaluation = await llm_evaluate_results(
                current_query, test_names, results, attempt + 1,
                patient_info=patient_info,
            )

            if evaluation.get("found") and evaluation.get("document_ids"):
                # Загружаем полные документы для найденных ID
                for doc_id in evaluation["document_ids"]: 
                    try:
                        full_doc = await client.get_document(bank_id, doc_id)
                        full_text = full_doc.get("original_text", "")
                        if full_text:
                            refs = await llm_extract_references(test_names, full_text)
                            found_references.extend(refs)
                    except Exception as e:
                        logger.error(f"   ❌ Ошибка загрузки документа {doc_id}: {e}")

                # Проверяем, покрыты ли все показатели
                covered = set(evaluation.get("covered_tests", []))
                missing = set(test_names) - covered
                if not missing:
                    logger.info(f"   ✅ Все показатели группы покрыты!")
                    break
                else:
                    logger.info(f"   ⚠️ Не хватает: {missing}")
                    # Уточняем запрос
                    current_query = evaluation.get("refined_query") or \
                        f"референсные значения {' '.join(list(missing))}"
                    current_tags = evaluation.get("refined_tags", ["референсные интервалы"])
            else:
                # LLM сказала, что не нашла. Используем уточнённый запрос
                refined = evaluation.get("refined_query")
                if refined and refined != current_query:
                    logger.info(f"   🔄 Уточняем запрос: {refined}")
                    current_query = refined
                    current_tags = evaluation.get("refined_tags", ["референсные интервалы"])
                else:
                    # Пробуем альтернативный запрос
                    current_query = f"норма {group.get('group_name')} референсы таблица"
                    current_tags = ["лабораторные референсные значения"]

        # Сохраняем результаты группы
        all_group_results.append({
            "group_name": group.get("group_name"),
            "tests": test_names,
            "found_references": found_references,
            "attempts_used": used_attempts,
        })

        logger.info(f"\n✅ Группа '{group.get('group_name')}': "
                    f"найдено {len(found_references)}/{len(test_names)} референсов "
                    f"за {used_attempts} попыток")

    # Итог
    total_found = sum(
        len(g["found_references"]) for g in all_group_results
    )
    total_requested = sum(len(g["tests"]) for g in all_group_results)

    logger.info(f"\n{'='*70}")
    logger.info(f"📊 ИТОГ LLM-ПОИСКА РЕФЕРЕНСНЫХ ЗНАЧЕНИЙ")
    logger.info(f"   Найдено референсов: {total_found}/{total_requested}")
    logger.info(f"   Всего попыток: {total_attempts}")
    logger.info(f"{'='*70}")

    return {
        "success": total_found > 0,
        "total_found": total_found,
        "total_requested": total_requested,
        "total_attempts": total_attempts,
        "groups": all_group_results,
    }
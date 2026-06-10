# services/hindsight_client.py
import aiohttp
import asyncio
import json
import logging
from typing import Dict, List, Optional, Any
from config import config

logger = logging.getLogger(__name__)

class HindsightClient:
    def __init__(self, base_url: str = "http://91.122.158.124:8888"):
        self.base_url = base_url.rstrip("/")

    async def _api_get(self, path: str, params: dict = None) -> Any:
        url = f"{self.base_url}{path}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    else:
                        logger.error(f"GET {url} error {resp.status}: {await resp.text()}")
        except Exception as e:
            logger.error(f"GET {url} exception: {e}")
        return None

    async def _api_post(self, path: str, json: dict = None) -> Any:
        url = f"{self.base_url}{path}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=json) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    else:
                        logger.error(f"POST {url} error {resp.status}: {await resp.text()}")
        except Exception as e:
            logger.error(f"POST {url} exception: {e}")
        return None

    async def bank_exists(self, bank_id: str) -> bool:
        data = await self._api_get(f"/v1/default/banks/{bank_id}/documents", params={"limit": 1})
        # logger.info(f"bank_exists data: {data}")
        if data is None:
            return False
        items = data.get("items", [])
        total = data.get("total", 0)
        return bool(items) or total > 0

    async def import_bank(self, bank_id: str, payload: dict) -> bool:
        result = await self._api_post(f"/v1/default/banks/{bank_id}/import", json=payload)
        if result is not None:
            logger.info(f"Bank {bank_id} imported successfully")
            return True
        return False

    async def retain(self, bank_id: str, content: str, metadata: Dict[str, Any] = None) -> str:
        """Сохраняет документ в банк памяти."""
        url = f"{self.base_url}/v1/default/banks/{bank_id}/memories"
        payload = {
            "async": True,
            "items": [{
                "content": content,
                "metadata": metadata or {}
            }]
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("id", "")
                    else:
                        logger.error(f"Retain error {resp.status}: {await resp.text()}")
        except Exception as e:
            logger.error(f"Retain exception: {e}")
        return ""

    async def recall(self, bank_id: str, query: str, limit: int = 5) -> List[str]:
        """Семантический поиск (возвращает тексты)."""
        results = await self.recall_raw(bank_id, None, query=query, limit=limit)
        texts = []
        for r in results:
            if isinstance(r, str):
                texts.append(r)
            elif isinstance(r, dict):
                texts.append(r.get("content", r.get("text", json.dumps(r))))
        return texts[:limit]

    async def recall_raw(self, bank_id: str, payload: dict = None, *,
                         query: str = None, limit: int = 5,
                         types: list = None, tags: list = None,
                         max_tokens: int = 4096, budget: str = "mid",
                         include_entities: bool = False) -> list:
        """
        Семантический поиск. Возвращает сырые результаты (list диктов).
        Можно передать готовый payload или параметры по отдельности.
        """
        url = f"{self.base_url}/v1/default/banks/{bank_id}/memories/recall"

        if payload is None:
            payload = {
                "query": query or "",
                "budget": budget,
                "max_tokens": max_tokens,
                "types": types or ["world"],
            }
            if tags:
                payload["tags"] = tags
                payload["tags_match"] = "any"
            if include_entities:
                payload["include"] = {"entities": {"max_tokens": 500}}

        logger.info(f"🔍 RECALL_RAW: bank={bank_id}, query='{payload.get('query', '')}', "
                    f"types={payload.get('types')}, tags={payload.get('tags')}, "
                    f"budget={payload.get('budget')}, max_tokens={payload.get('max_tokens')}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    response_text = await resp.text()
                    if resp.status == 200:
                        try:
                            data = json.loads(response_text)
                        except json.JSONDecodeError as e:
                            logger.error(f"❌ RECALL_RAW JSON parse error: {e}")
                            return []
                        results = data.get("results", [])
                        entities = data.get("entities") or {}
                        logger.info(f"📊 RECALL_RAW: {len(results)} results, {len(entities) if isinstance(entities, dict) else 0} entities")
                        # Форматируем результат
                        out = []
                        for r in results:
                            if isinstance(r, dict):
                                r["_entities"] = entities
                                out.append(r)
                            elif isinstance(r, str):
                                out.append({"text": r, "_entities": entities})
                        return out[:limit] if limit else out
                    else:
                        logger.error(f"❌ RECALL_RAW non-200: status={resp.status}, body={response_text[:500]}")
                        return []
        except Exception as e:
            logger.error(f"❌ RECALL_RAW exception: {e}", exc_info=True)
            return []

    async def get_documents(self, bank_id: str, limit: int = 100, offset: int = 0) -> List[Dict]:
        data = await self._api_get(f"/v1/default/banks/{bank_id}/documents", params={"limit": limit, "offset": offset}) or []
        # logger.info(f"get_documents: {data}")
        return data.get("items", [])

    async def get_document(self, bank_id: str, document_id: str) -> Dict:
        return await self._api_get(f"/v1/default/banks/{bank_id}/documents/{document_id}") or {}

    async def get_mental_model(self, bank_id: str, mental_model_id: str) -> str:
        data = await self._api_get(f"/v1/default/banks/{bank_id}/mental-models/{mental_model_id}")
        if data and isinstance(data, dict):
            return data.get("model", data.get("content", str(data)))
        return ""

    # ==================== Медицинские методы ====================

    async def save_analysis(self, bank_id: str, analysis_data: dict) -> str:
        """
        Сохраняет структурированный анализ в Hindsight.
        analysis_data: {patient_name, age, sex, date, test_name, value, unit, ref_range, raw_text}
        Возвращает ID сохранённого документа.
        """
        content = json.dumps(analysis_data, ensure_ascii=False)
        metadata = {
            "type": "medical_analysis",
            "test_name": analysis_data.get("test_name", ""),
            "date": analysis_data.get("date", ""),
            "patient_name": analysis_data.get("patient_name", ""),
        }
        return await self.retain(bank_id, content, metadata=metadata)

    async def save_patient_metadata(self, bank_id: str, metadata: dict) -> str:
        """
        Сохраняет метаданные пациента: name, age, sex, birth_date, birth_time, birth_place.
        """
        content = json.dumps(metadata, ensure_ascii=False)
        meta = {
            "type": "patient_metadata",
            "patient_name": metadata.get("name", ""),
        }
        return await self.retain(bank_id, content, metadata=meta)

    async def get_patient_metadata(self, bank_id: str) -> Optional[Dict]:
        """Ищет метаданные пациента в банке."""
        results = await self.recall(bank_id, "Имя пользователя, возраст и пол", limit=5)
        for r in results:
            try:
                data = json.loads(r)
                if "name" in data or "age" in data:
                    return data
            except (json.JSONDecodeError, TypeError):
                pass
        # Fallback: ищем документы с метаданными
        docs = await self.get_documents(bank_id, limit=50)
        for doc in docs:
            meta = doc.get("document_metadata", {})
            if meta.get("type") == "patient_metadata":
                full = await self.get_document(bank_id, doc["id"])
                try:
                    text = full.get("original_text", "")
                    if text:
                        return json.loads(text)
                except (json.JSONDecodeError, TypeError):
                    pass
        return None

    async def get_analyses_list(self, bank_id: str) -> List[Dict]:
        """Возвращает список всех анализов в банке."""
        docs = await self.get_documents(bank_id, limit=200)
        analyses = []
        for doc in docs:
            meta = doc.get("document_metadata", {})
            if meta.get("type") == "medical_analysis":
                full = await self.get_document(bank_id, doc["id"])
                try:
                    text = full.get("original_text", "")
                    if text:
                        data = json.loads(text)
                        data["doc_id"] = doc["id"]
                        analyses.append(data)
                except (json.JSONDecodeError, TypeError):
                    pass
        return analyses

    async def faq_query(self, bank_id: str, query: str, limit: int = 3) -> List[str]:
        """Поиск по FAQ-банку."""
        return await self.recall(bank_id, query, limit=limit)

    async def init_faq_bank(self, bank_id: str) -> bool:
        """Инициализирует FAQ-банк с предзагруженными вопросами/ответами."""
        exists = await self.bank_exists(bank_id)
        if exists:
            return True
        faq_config = {
            "version": "1",
            "bank": {
                "retain_mission": "Храните вопросы и ответы о возможностях медицинского ассистента.",
                "enable_observations": False,
            },
            "mental_models": []
        }
        success = await self.import_bank(bank_id, faq_config)
        if not success:
            return False
        # Загружаем стандартные FAQ
        faq_items = [
            {
                "content": json.dumps({
                    "question": "Какие ваши уникальные возможности?",
                    "answer": "Я — медицинский AI-ассистент на базе многоагентной системы. "
                              "Мои возможности включают:\n"
                              "1. ✅ **Расшифровка анализов** — загрузите PDF или фото анализов, я извлеку показатели и сравню с референсами\n"
                              "2. 🔍 **Поиск в интернете** — могу найти информацию о любых препаратах и нутрицевтиках\n"
                              "3. 🧠 **Долговременная память** — помню ваши предыдущие обращения и предпочтения\n"
                              "4. 📊 **Интерпретация отклонений** — объясню, что означает каждый показатель\n"
                              "5. 🔄 **Проверка достоверности** — факт-чекинг найденной информации"
                }, ensure_ascii=False),
                "metadata": {"type": "faq", "question": "unique_capabilities"}
            },
            {
                "content": json.dumps({
                    "question": "В каком формате загружать анализы?",
                    "answer": "Вы можете загружать анализы в следующих форматах:\n"
                              "- 📄 **PDF** — скан или электронный файл\n"
                              "- 🖼️ **Изображения** — JPG, JPEG, PNG\n"
                              "Я поддерживаю загрузку нескольких файлов одновременно. "
                              "Для лучшего распознавания рекомендуется:\n"
                              "- Использовать чёткие сканы/фото\n"
                              "- Избегать бликов и перекосов\n"
                              "- Если файл большой, разделите на страницы"
                }, ensure_ascii=False),
                "metadata": {"type": "faq", "question": "upload_format"}
            },
            {
                "content": json.dumps({
                    "question": "Как загрузить анализы?",
                    "answer": "Нажмите кнопку **«Загрузить анализы»**, затем выберите файлы на своём устройстве. "
                              "После загрузки я обработаю файлы, извлеку показатели и сохраню их. "
                              "Затем вы сможете запросить расшифровку."
                }, ensure_ascii=False),
                "metadata": {"type": "faq", "question": "how_to_upload"}
            }
        ]
        for item in faq_items:
            await self.retain(bank_id, item["content"], metadata=item["metadata"])
        logger.info(f"FAQ bank {bank_id} initialized with {len(faq_items)} items")
        return True

    async def search_norm_blood(self, bank_id: str, test_name: str) -> Optional[Dict]:
        """Ищет эталонный референс по названию теста."""
        results = await self.recall(bank_id, test_name, limit=3)
        for r in results:
            try:
                data = json.loads(r)
                if isinstance(data, dict) and data.get("test_name", "").lower() in test_name.lower():
                    return data
            except (json.JSONDecodeError, TypeError):
                pass
        return None

    async def save_web_search_result(self, bank_id: str, query: str, content: str) -> str:
        """Сохраняет результат веб-поиска как непроверенный референс."""
        data = {
            "source": "web",
            "verified": False,
            "query": query,
            "content": content,
        }
        return await self.retain(bank_id, json.dumps(data, ensure_ascii=False), metadata={
            "type": "norm_blood",
            "source": "web",
            "verified": False,
        })
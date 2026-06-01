# services/hindsight_client.py
import aiohttp
import asyncio
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

    async def bank_exists(self, bank_id: str) -> bool:
        # проверим, есть ли банк, запросив документы с limit=1
        data = await self._api_get(f"/v1/default/banks/{bank_id}/documents", params={"limit": 1})
        return data is not None

    async def import_bank(self, bank_id: str, payload: dict) -> bool:
        result = await self._api_post(f"/v1/default/banks/{bank_id}/import", json=payload)
        if result is not None:
            logger.info(f"Bank {bank_id} imported successfully")
            return True
        return False    

    async def retain(self, bank_id: str, content: str, metadata: Dict[str, Any] = None) -> str:
        """Сохраняет документ в банк памяти."""
        logger.info(f"Сохраняет документ в банк памяти.")
        logger.info(f"Retain bank_id {bank_id},Retain content {content},Retain metadata {metadata}")
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
                        logger.info(f"Retain data {data}")
                        return data.get("id", "")
                    else:
                        logger.error(f"Retain error {resp.status}: {await resp.text()}")
        except Exception as e:
            logger.error(f"Retain exception: {e}")
        return ""

    async def recall(self, bank_id: str, query: str, limit: int = 5) -> List[str]:
        """Семантический поиск (оставляем для извлечения фактов)."""
        url = f"{self.base_url}/v1/default/banks/{bank_id}/memories/recall"
        payload = {"query": query, "budget": "mid"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # logger.info(f"recall bank_id {bank_id},recall content {query},recall data {data}")

                        results = []
                        for r in data.get("results", []):
                            if isinstance(r, str):
                                results.append(r)
                            elif isinstance(r, dict) and "content" in r:
                                results.append(r["content"])
                            elif hasattr(r, "content"):
                                results.append(r.content)
                        return results[:limit]
        except Exception as e:
            logger.error(f"Recall error: {e}")
        return []

    
    async def get_documents(self, bank_id: str, limit: int = 100, offset: int = 0) -> List[Dict]:
        data =  await self._api_get(f"/v1/default/banks/{bank_id}/documents", params={"limit": limit, "offset": offset}) or []
        return data.get("items", [])

    async def get_document(self, bank_id: str, document_id: str) -> Dict:
        return await self._api_get(f"/v1/default/banks/{bank_id}/documents/{document_id}") or {}

    async def get_mental_model(self, bank_id: str, mental_model_id: str) -> str:
        """Возвращает содержимое ментальной модели как текст (Markdown)."""
        data = await self._api_get(f"/v1/default/banks/{bank_id}/mental-models/{mental_model_id}")
        if data and isinstance(data, dict):
            # Ожидаем поле 'model' или 'content'
            return data.get("model", data.get("content", str(data)))
        return ""

    async def __aexit__(self, *args):
        pass
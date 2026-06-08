# ================================
# services/hindsight_memory.py
# ================================
import json
import logging
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from services.hindsight_client import HindsightClient
from config import config

logger = logging.getLogger(__name__)

# Конфигурация банка (карта) – может быть вынесена в config.py, для простоты здесь
BANK_CONFIG = {
  "version": "1",
  "bank": {
    "retain_mission": "Извлеките предпочтения пользователя, анализы, его распорядок дня, заявленные факты о себе, сделанные им запросы, интересующие его темы, запланированные события, обязательства, упомянутых им людей и любую личную информацию, которой он делится,а также любые обязательства или последующие действия. Отслеживайте, что он запрашивает неоднократно и что для него важно.Игнорировать пустую болтовню и лишние слова.",
    "enable_observations": True,
    "observations_mission": "Отслеживайте стабильные предпочтения пользователя, повторяющиеся действия,стиль общения,  важных для него людей и отношения, а также то, как меняются его приоритеты и потребности с течением времени."
  },
#   "mental_models": [
#     {
#       "id": "user-profile",
#       "name": "User Profile",
#       "source_query": "Что нам известно об этом пользователе? Какой стиль общения у пользователя? Какой предположительно тип личности(например INFJ (Заступник/Провидец))? Как нужно с ним общаться в диалоге? Заполняй его профиль: Имя, Дата рожения, Время рождения, Место рождения,Солнечный знак, Тип личности",
#       "max_tokens": 3000,
#       "trigger": {
#         "refresh_after_consolidation": True
#       }
#     },
#     {
#       "id": "active-tasks",
#       "name": "Active Tasks & Commitments",
#       "source_query": "Какие задачи, обязательства или последующие действия пользователь отслеживает в данный момент? Какие сроки или обещания были даны? Какие темы, задачи или последующие действия остаются открытыми или нерешенными из прошлых разговоров?",
#       "max_tokens": 8000,
#       "trigger": {
#         "refresh_after_consolidation": True
#       }
#     },
#     {
#       "id": "personality-type",
#       "name": "Personality type",
#       "source_query": "Каковы его предпочтения, распорядок дня, кто ему важен и как он предпочитает получать помощь?",
#       "max_tokens": 8000,
#       "trigger": {
#         "refresh_after_consolidation": True
#       }
#     }

#   ]
}

class HindsightMemoryLayer:
    def __init__(self, base_url: str):
        self.client = HindsightClient(base_url)
        # кэш инициализированных банков (в памяти на время работы)
        self._initialized_banks = set()

    async def _get_bank_id(self, user_id: str) -> str:
        return f"user_{user_id}"

    async def _ensure_bank_initialized(self, user_id: str):
        bank_id = await self._get_bank_id(user_id)

        if bank_id in self._initialized_banks:
            return
        exists = await self.client.bank_exists(bank_id)

        if not exists:
            success = await self.client.import_bank(bank_id, BANK_CONFIG)
            if success:
                self._initialized_banks.add(bank_id)
            else:
                logger.error(f"Failed to import bank {bank_id}")
        else:
            self._initialized_banks.add(bank_id) 

    async def save_message(self, user_id: str, thread_id: str, role: str, content: str, metadata: Optional[Dict] = None):
        await self._ensure_bank_initialized(user_id)
        bank_id = await self._get_bank_id(user_id)
        doc_content = f"[{role}]: {content}"
        meta = {
            "thread_id": thread_id,
            "user_id": user_id,
            "role": role,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **(metadata or {})
        }
        try:
            await self.client.retain(bank_id, doc_content, meta)
            logger.info(f"Saved message to Hindsight: {doc_content[:50]}...")
        except Exception as e:
            logger.error(f"Failed to save message to Hindsight: {e}")

    async def recall_relevant(self, user_id: str, query: str, limit: int = 5) -> List[str]:
        await self._ensure_bank_initialized(user_id)
        bank_id = await self._get_bank_id(user_id)
        try:
            return await self.client.recall(bank_id, query, limit=limit)
        except Exception as e:
            logger.error(f"Recall failed: {e}")
            return []

    async def get_conversation_history(self, user_id: str, thread_id: str, limit: int = 20) -> List[Dict]:
        await self._ensure_bank_initialized(user_id)
        bank_id = await self._get_bank_id(user_id)
        logger.info(f"get_conversation_history bank_id={bank_id} thread_id={thread_id}")
        try:
            docs_meta = await self.client.get_documents(bank_id, limit=200, offset=0)
            logger.info(f"get_conversation_history docs_meta count={len(docs_meta)}")
            if not docs_meta:
                return []

            filtered = [
                doc for doc in docs_meta
                if doc.get("document_metadata", {}).get("thread_id") == thread_id
            ]
            logger.info(f"get_conversation_history filtered by thread_id count={len(filtered)} from {len(docs_meta)}")
            if not filtered:
                return []

            async def fetch_full(doc):
                doc_id = doc["id"]
                return await self.client.get_document(bank_id, doc_id)

            full_docs = await asyncio.gather(*[fetch_full(d) for d in filtered])
            messages = []
            for doc in full_docs:
                meta = doc.get("document_metadata", {})
                role = meta.get("role")
                if role not in ("user", "assistant"):
                    continue
                text = doc.get("original_text", "")
                prefix = f"[{role}]: "
                if text.startswith(prefix):
                    content = text[len(prefix):]
                else:
                    content = text
                timestamp = meta.get("timestamp")
                messages.append({
                    "role": role,
                    "content": content,
                    "timestamp": timestamp
                })
            messages.sort(key=lambda x: x.get("timestamp", ""))
            logger.info(f"get_conversation_history messages count={len(messages)}")
            return messages[-limit:] if limit > 0 else messages
        except Exception as e:
            logger.error(f"Failed to get conversation history: {e}", exc_info=True)
            return []

    async def extract_user_facts(self, user_id: str) -> Dict[str, Any]:
        await self._ensure_bank_initialized(user_id)
        bank_id = await self._get_bank_id(user_id)
        try:
            results = await self.client.recall(bank_id, "user profile facts", limit=10)
            facts = {}
            for r in results:
                content = r if isinstance(r, str) else getattr(r, 'content', str(r))
                try:
                    fact = json.loads(content)
                    facts[fact.get("fact_type", "")] = fact.get("value")
                except:
                    pass
            return facts
        except Exception as e:
            logger.error(f"Extract user facts failed: {e}")
            return {}

    async def save_user_fact(self, user_id: str, key: str, value: Any):
        await self._ensure_bank_initialized(user_id)
        bank_id = await self._get_bank_id(user_id)
        content = json.dumps({key: value}, ensure_ascii=False)
        await self.client.retain(bank_id, content, metadata={"type": "user_fact", "fact_key": key, "timestamp": datetime.now(timezone.utc).isoformat()})

    async def get_mental_models(self, user_id: str, model_ids: List[str]) -> Dict[str, str]:
        """Возвращает словарь {model_id: текст_модели}."""
        await self._ensure_bank_initialized(user_id)
        bank_id = await self._get_bank_id(user_id)
        models = {}
        async def fetch_model(mid):
            text = await self.client.get_mental_model(bank_id, mid)
            if text:
                models[mid] = text
        await asyncio.gather(*[fetch_model(mid) for mid in model_ids])
        return models
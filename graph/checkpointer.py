# ================================
# graph/checkpointer.py
# ================================
# import logging
# from psycopg.rows import dict_row
# from psycopg_pool import AsyncConnectionPool
# from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
# from langgraph.checkpoint.memory import MemorySaver
# from config import config

# logger = logging.getLogger(__name__)

# async def get_postgres_checkpointer():
#     """Возвращает AsyncPostgresSaver с пулом соединений, устойчивым к обрывам."""
#     conn_string = config.POSTGRES
#     if not conn_string:
#         logger.error("POSTGRES connection string is not set in config")
#         raise RuntimeError("POSTGRES connection string is not set in config")

#     try:
#         pool = AsyncConnectionPool(
#             conninfo=conn_string,
#             kwargs={
#                 "autocommit": True,
#                 "row_factory": dict_row,
#                 "keepalives": 1,
#                 "keepalives_idle": 30,
#                 "keepalives_interval": 10,
#                 "keepalives_count": 5,
#             },
#             min_size=1,
#             max_size=5,
#             max_idle=300,
#             open=False,                     # не открываем в конструкторе
#         )
#         await pool.open()                  # явное открытие
#         checkpointer = AsyncPostgresSaver(pool)
#         await checkpointer.setup()
#         logger.info("AsyncPostgresSaver initialized successfully with connection pool.")
#         return checkpointer
#     except Exception as e:
#         logger.error(f"Failed to initialize AsyncPostgresSaver: {e}. Falling back to MemorySaver.")
#         logger.warning("Using MemorySaver – checkpoints will not persist across restarts.")
#         return MemorySaver()

import logging
import asyncio
from typing import Optional
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.checkpoint.memory import MemorySaver
from config import config

# Схема для чекпоинтов
CHECKPOINTER_SCHEMA = "medexpertai_checkpointer"

logger = logging.getLogger(__name__)


class LimitedHistoryCheckpointer(BaseCheckpointSaver):
    """
    Обёртка над чекпоинтером LangGraph с:
    - ограничением истории (храним только последние N сообщений)
    - ограничением контекста (отдаём только последние M сообщений)
    - автоматическими повторами при обрыве соединения с БД
    """

    def __init__(
        self,
        wrapped: BaseCheckpointSaver,
        max_stored_messages: int = 100,
        max_context_messages: int = 20,
    ):
        super().__init__()
        self.wrapped = wrapped
        self.max_stored_messages = max_stored_messages
        self.max_context_messages = max_context_messages

    async def _with_retry(self, coro, max_retries=3, delay=0.5):
        """Повторяет выполнение корутины при ошибках соединения с БД."""
        last_exc = None
        for attempt in range(max_retries):
            try:
                return await coro
            except Exception as e:
                last_exc = e
                # Распознаём ошибки, связанные с разрывом соединения
                err_msg = str(e).lower()
                if any(keyword in err_msg for keyword in [
                    "idlesessiontimeout", "connection was closed", "bad", "server closed"
                ]):
                    logger.warning(
                        f"Database connection error, retry {attempt+1}/{max_retries}: {e}"
                    )
                    # Если пул поддерживает, принудительно сбросим все соединения
                    if hasattr(self.wrapped, "pool"):
                        try:
                            await self.wrapped.pool.close()
                            await self.wrapped.pool.open()
                        except Exception:
                            pass
                    await asyncio.sleep(delay * (attempt + 1))
                    continue
                raise
        raise last_exc

    async def aput(self, config: dict, checkpoint, metadata, new_versions):
        # Обрезаем сообщения перед сохранением
        messages = checkpoint.get("channel_values", {}).get("messages", [])
        if len(messages) > self.max_stored_messages:
            checkpoint["channel_values"]["messages"] = messages[-self.max_stored_messages:]
        return await self._with_retry(
            self.wrapped.aput(config, checkpoint, metadata, new_versions)
        )

    async def aget_tuple(self, config: dict) -> Optional[CheckpointTuple]:
        async def _get():
            return await self.wrapped.aget_tuple(config)

        checkpoint_tuple = await self._with_retry(_get())
        if checkpoint_tuple is None:
            return None

        # Обрезаем сообщения для контекста LLM
        checkpoint = checkpoint_tuple.checkpoint
        messages = checkpoint.get("channel_values", {}).get("messages", [])
        if len(messages) > self.max_context_messages:
            checkpoint["channel_values"]["messages"] = messages[-self.max_context_messages:]
        return checkpoint_tuple

    async def aput_writes(self, config: dict, writes, task_id):
        return await self._with_retry(
            self.wrapped.aput_writes(config, writes, task_id)
        )

    async def alist(self, config: dict, *, limit=None, before=None, filter=None):
        return await self._with_retry(
            self.wrapped.alist(config, limit=limit, before=before, filter=filter)
        )

    def get_next_version(self, current, channel):
        return self.wrapped.get_next_version(current, channel)


async def get_postgres_checkpointer(
    max_stored_messages: int = 100,
    max_context_messages: int = 20,
):
    """Создаёт чекпоинтер с пулом соединений, устойчивым к простою."""
    conn_string = config.POSTGRES
    if not conn_string:
        logger.error("POSTGRES connection string is not set")
        raise RuntimeError("POSTGRES connection string is not set")

    try:
        pool = AsyncConnectionPool(
            conninfo=conn_string,
            kwargs={
                "autocommit": True,
                "row_factory": dict_row,
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5,
            },
            min_size=1,
            max_size=5,
            max_idle=300,
            # Проверять соединение перед использованием
            check=AsyncConnectionPool.check_connection,
            open=False,
        )
        await pool.open()
        base = AsyncPostgresSaver(pool)
        await base.setup()
        wrapped = LimitedHistoryCheckpointer(
            base,
            max_stored_messages=max_stored_messages,
            max_context_messages=max_context_messages,
        )
        logger.info(
            f"Checkpointer ready: stored ≤ {max_stored_messages}, context ≤ {max_context_messages} messages."
        )
        return wrapped
    except Exception as e:
        logger.error(f"PostgreSQL failed ({e}). Falling back to MemorySaver.")
        return LimitedHistoryCheckpointer(
            MemorySaver(),
            max_stored_messages=max_stored_messages,
            max_context_messages=max_context_messages,
        )
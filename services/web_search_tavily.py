# services/web_search_tavily.py
"""
Веб-поиск через Tavily API для медицинских запросов.
Результаты сохраняются в Hindsight как непроверенные референсы.
"""
import json
import logging
from typing import List, Dict, Optional
from config import config

logger = logging.getLogger(__name__)


class TavilySearchClient:
    """Клиент для поиска через Tavily API."""

    def __init__(self):
        self.api_key = config.TAVILY_API_KEY
        self.base_url = "https://api.tavily.com"

    async def search(self, query: str, max_results: int = 5, search_depth: str = "basic") -> List[Dict]:
        """
        Выполняет поиск через Tavily API.
        Возвращает список результатов с полями: title, url, content, score.
        """
        import aiohttp

        payload = {
            "api_key": self.api_key,
            "query": query,
            "search_depth": search_depth,
            "max_results": max_results,
            "include_answer": True,
            "include_raw_content": False,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/search",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        results = data.get("results", [])
                        answer = data.get("answer", "")
                        logger.info(f"Tavily search '{query}' returned {len(results)} results")
                        return {
                            "results": [
                                {
                                    "title": r.get("title", ""),
                                    "url": r.get("url", ""),
                                    "content": r.get("content", ""),
                                    "score": r.get("score", 0),
                                }
                                for r in results
                            ],
                            "answer": answer,
                            "query": query,
                        }
                    else:
                        error_text = await resp.text()
                        logger.error(f"Tavily API error {resp.status}: {error_text}")
                        return {"results": [], "answer": "", "query": query}
        except Exception as e:
            logger.error(f"Tavily search exception: {e}")
            return {"results": [], "answer": "", "query": query}

    async def search_medical(self, query: str) -> str:
        """
        Медицинский поиск. Возвращает форматированный текст для агента.
        """
        result = await self.search(query, max_results=5, search_depth="advanced")

        if not result["results"]:
            return f"По запросу '{query}' ничего не найдено."

        output = f"🔍 **Результаты поиска: {query}**\n\n"

        if result.get("answer"):
            output += f"📌 **Краткий ответ:** {result['answer']}\n\n"

        for i, r in enumerate(result["results"], 1):
            output += f"**{i}. {r['title']}**\n"
            output += f"🔗 {r['url']}\n"
            output += f"📄 {r['content'][:500]}\n\n"

        return output
# services/file_processor.py
"""
Обработка медицинских файлов: конвертация PDF/изображений, OCR через Gemini (CometAPI),
извлечение структурированных медицинских показателей.
"""
import asyncio
import base64
import json
import logging
import os
from pathlib import Path
from typing import List, Dict, Optional, Callable
import io
import re

from config import config
from services.hindsight_client import HindsightClient

logger = logging.getLogger(__name__)

# Разрешённые расширения файлов
ALLOWED_EXTENSIONS = {'.pdf', '.jpg', '.jpeg', '.png', '.dcm'}

# Промпт для извлечения медицинских показателей
MEDICAL_OCR_PROMPT = """You are a medical data extraction specialist. Extract all medical test results from this document/image.
For each test result, extract the following fields in JSON format:
- test_name: name of the medical test (in Russian)
- value: the numerical value or result
- unit: measurement units
- ref_range: reference range if available
- status: "normal", "elevated", "decreased", or "unknown" based on comparison with reference range

Also extract patient information if available:
- patient_name: full name
- age: age in years
- sex: male/female
- date: date of analysis (YYYY-MM-DD format)
- lab_name: name of the laboratory

Return ONLY a valid JSON array of objects. Do not include any other text or markdown formatting. If no medical data found, return empty array [].
Example format:
[
  {
    "test_name": "Гемоглобин",
    "value": "145",
    "unit": "г/л",
    "ref_range": "130-160",
    "status": "normal"
  }
]
Also extract patient info as a separate object with key "_patient":
{
  "_patient": {
    "name": "Иванов Иван",
    "age": 35,
    "sex": "male",
    "date": "2024-01-15",
    "lab_name": "Инвитро"
  }
}
"""


class FileProcessor:
    """Обработчик медицинских файлов с OCR через CometAPI."""

    def __init__(self):
        self.api_key = config.COMET_API_KEY
        self.base_url = config.COMET_BASE_URL
        self.proxy_url = config.PROXY_URL
        self.max_retries = getattr(config, 'COMET_MAX_RETRIES', 3)
        self.timeout = getattr(config, 'COMET_TIMEOUT', 300)
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.model = "gemini-3.1-flash-lite-preview"
        self.hindsight = HindsightClient(config.HINDSIGHT_URL)
        self._progress_callback: Optional[Callable] = None

    def set_progress_callback(self, callback: Callable):
        """Устанавливает callback для уведомлений о прогрессе."""
        self._progress_callback = callback

    async def _report_progress(self, file_name: str, progress: int, status: str, data: dict = None):
        """Отправляет уведомление о прогрессе, если callback установлен."""
        if self._progress_callback:
            await self._progress_callback({
                "file_name": file_name,
                "progress": progress,
                "status": status,
                "data": data or {}
            })

    def _get_connector(self):
        """Создаёт прокси-коннектор если нужен."""
        if not self.proxy_url:
            return None
        from urllib.parse import urlparse
        parsed = urlparse(self.proxy_url)
        if parsed.scheme == "socks5":
            from aiohttp_socks import ProxyConnector, ProxyType
            return ProxyConnector(
                proxy_type=ProxyType.SOCKS5,
                host=parsed.hostname,
                port=parsed.port,
                username=parsed.username,
                password=parsed.password,
                rdns=True
            )
        return None

    def validate_extension(self, file_path: str) -> bool:
        """Проверяет расширение файла."""
        ext = Path(file_path).suffix.lower()
        return ext in ALLOWED_EXTENSIONS

    def pdf_to_images_base64(self, pdf_path: str, dpi: int = 150) -> List[str]:
        """Конвертирует PDF в список base64-строк (каждая страница PNG)."""
        import fitz  # PyMuPDF
        images_base64 = []
        doc = fitz.open(pdf_path)
        try:
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                zoom = dpi / 72
                mat = fitz.Matrix(zoom, zoom)
                pix = page.get_pixmap(matrix=mat, alpha=False)
                img_data = pix.tobytes("png")
                b64_str = base64.b64encode(img_data).decode("utf-8")
                images_base64.append(b64_str)
        finally:
            doc.close()
        return images_base64

    def image_to_base64(self, image_path: str) -> str:
        """Читает изображение и возвращает base64 (PNG)."""
        from PIL import Image
        with Image.open(image_path) as img:
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")
            buffer = io.BytesIO()
            img.save(buffer, format="PNG")
            return base64.b64encode(buffer.getvalue()).decode("utf-8")

    async def _send_to_gemini(self, images_base64: List[str], prompt: str = MEDICAL_OCR_PROMPT) -> Dict:
        """Отправляет изображения в Gemini через CometAPI."""
        import aiohttp

        image_parts = [
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img}"}}
            for img in images_base64
        ]
        messages = [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                *image_parts
            ]
        }]

        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": 8000,
            "temperature": 0.1,
            "stream": False
        }

        connector = self._get_connector()
        async with aiohttp.ClientSession(connector=connector) as session:
            for attempt in range(self.max_retries):
                try:
                    async with session.post(
                        f"{self.base_url}/chat/completions",
                        headers=self.headers,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=self.timeout)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            content = data["choices"][0]["message"]["content"]
                            usage = data.get("usage", {})
                            return {"content": content, "usage": usage}
                        else:
                            error_text = await resp.text()
                            logger.error(f"API error {resp.status}: {error_text}")
                            if attempt < self.max_retries - 1:
                                await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    logger.error(f"Attempt {attempt+1} failed: {e}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
        raise Exception(f"Gemini OCR failed after {self.max_retries} retries")

    def _parse_medical_data(self, raw_text: str) -> Dict:
        """
        Парсит JSON из ответа Gemini.
        Возвращает { "analyses": [...], "patient": {...}, "ignored": [] }
        """
        result = {"analyses": [], "patient": None, "ignored": [], "raw_response": raw_text}

        # Пробуем извлечь JSON из ответа
        json_str = raw_text.strip()
        # Убираем markdown-обёртку ```json ... ```
        json_match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', json_str)
        if json_match:
            json_str = json_match.group(1)

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError:
            # Пробуем найти массив или объект в тексте
            array_match = re.search(r'(\[.*\])', json_str, re.DOTALL)
            if array_match:
                try:
                    data = json.loads(array_match.group(1))
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse JSON from Gemini response: {raw_text[:200]}")
                    return result
            else:
                logger.error(f"No JSON found in Gemini response: {raw_text[:200]}")
                return result

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    if "_patient" in item:
                        result["patient"] = item["_patient"]
                    elif "test_name" in item and item.get("test_name"):
                        result["analyses"].append(item)
                    else:
                        result["ignored"].append(item)
        elif isinstance(data, dict):
            # Может быть {"analyses": [...], "patient": {...}}
            if "analyses" in data:
                result["analyses"] = data.get("analyses", [])
            if "patient" in data:
                result["patient"] = data.get("patient")
            if "_patient" in data:
                result["patient"] = data["_patient"]

        return result

    async def process_file(self, file_path: str, file_name: str = None) -> Dict:
        """Обрабатывает один файл: конвертация → OCR → парсинг."""
        if not file_name:
            file_name = Path(file_path).name

        await self._report_progress(file_name, 0, "pending")

        # 1. Валидация расширения
        if not self.validate_extension(file_path):
            await self._report_progress(file_name, 100, "error", {"error": "Неподдерживаемый формат файла"})
            return {"file_name": file_name, "status": "error", "error": "Неподдерживаемый формат файла"}

        await self._report_progress(file_name, 10, "converting")

        # 2. Конвертация в base64
        try:
            if file_path.lower().endswith('.pdf'):
                images_b64 = self.pdf_to_images_base64(file_path)
            elif file_path.lower().endswith(('.jpg', '.jpeg', '.png')):
                images_b64 = [self.image_to_base64(file_path)]
            else:
                await self._report_progress(file_name, 100, "error", {"error": "Формат не поддерживается для OCR"})
                return {"file_name": file_name, "status": "error", "error": "Формат не поддерживается для OCR"}
        except Exception as e:
            logger.error(f"Failed to convert file {file_path}: {e}")
            await self._report_progress(file_name, 100, "error", {"error": f"Ошибка конвертации: {str(e)}"})
            return {"file_name": file_name, "status": "error", "error": f"Ошибка конвертации: {str(e)}"}

        if not images_b64:
            await self._report_progress(file_name, 100, "error", {"error": "Пустой файл или не удалось прочитать"})
            return {"file_name": file_name, "status": "error", "error": "Пустой файл"}

        await self._report_progress(file_name, 30, "ocr")

        # 3. OCR через Gemini
        try:
            ocr_result = await self._send_to_gemini(images_b64)
        except Exception as e:
            logger.error(f"OCR failed for {file_path}: {e}")
            await self._report_progress(file_name, 100, "error", {"error": f"Ошибка OCR: {str(e)}"})
            return {"file_name": file_name, "status": "error", "error": f"Ошибка OCR: {str(e)}"}

        await self._report_progress(file_name, 70, "parsing")

        # 4. Парсинг медицинских данных
        parsed = self._parse_medical_data(ocr_result["content"])

        result = {
            "file_name": file_name,
            "status": "processed",
            "analyses": parsed["analyses"],
            "patient": parsed["patient"],
            "ignored": parsed["ignored"],
            "raw_text": parsed["raw_response"][:500],  # только превью
            "tokens_used": ocr_result.get("usage", {})
        }

        await self._report_progress(file_name, 100, "completed", result)
        return result

    async def process_files_batch(self, file_paths: List[str]) -> List[Dict]:
        """Обрабатывает пачку файлов параллельно."""
        tasks = []
        for fp in file_paths:
            tasks.append(self.process_file(fp))
        results = await asyncio.gather(*tasks, return_exceptions=True)

        final_results = []
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                final_results.append({
                    "file_name": Path(file_paths[i]).name,
                    "status": "error",
                    "error": str(r)
                })
            else:
                final_results.append(r)
        return final_results

    def aggregate_analyses(self, results: List[Dict]) -> Dict:
        """Агрегирует результаты обработки нескольких файлов."""
        all_analyses = []
        all_ignored = []
        patient_info = None
        total_files = len(results)
        processed = 0
        errors = []

        for r in results:
            if r["status"] == "error":
                errors.append(r.get("file_name", "unknown"))
                continue
            processed += 1
            all_analyses.extend(r.get("analyses", []))
            all_ignored.extend(r.get("ignored", []))
            if r.get("patient") and not patient_info:
                patient_info = r["patient"]

        return {
            "total_files": total_files,
            "processed": processed,
            "errors": errors,
            "total_analyses": len(all_analyses),
            "total_ignored": len(all_ignored),
            "analyses": all_analyses,
            "ignored": all_ignored,
            "patient": patient_info,
        }
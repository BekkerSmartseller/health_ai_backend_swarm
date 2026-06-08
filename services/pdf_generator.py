# health_ai_backend_swarm/services/pdf_generator.py
"""
Генерация PDF через Playwright (headless Chromium).
Получает готовый HTML с инлайновыми стилями и возвращает PDF.
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class PDFGenerator:
    def __init__(self):
        self._playwright = None
        self._browser = None

    async def start(self):
        """Запускает Playwright и открывает headless браузер."""
        from playwright.async_api import async_playwright
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
            ]
        )
        logger.info("Playwright browser started")

    async def generate(self, html: str, filename: str = "response.pdf") -> bytes:
        """
        Принимает полный HTML-документ, рендерит его в headless браузере
        и возвращает PDF как bytes.
        """
        if not self._browser:
            raise RuntimeError("PDFGenerator not started. Call start() first.")

        context = await self._browser.new_context(
            viewport={"width": 1200, "height": 800},
            device_scale_factor=2,
        )
        page = await context.new_page()

        try:
            await page.set_content(html, wait_until="networkidle")
            # Ждём дополнительно, чтобы шрифты и стили точно применились
            await page.wait_for_timeout(500)

            pdf_bytes = await page.pdf(
                format="A4",
                margin={
                    "top": "20px",
                    "bottom": "20px",
                    "left": "20px",
                    "right": "20px",
                },
                print_background=True,
                prefer_css_page_size=False,
            )
            logger.info(f"PDF generated: {len(pdf_bytes)} bytes")
            return pdf_bytes
        except Exception as e:
            logger.error(f"PDF generation error: {e}", exc_info=True)
            raise
        finally:
            await page.close()
            await context.close()

    async def stop(self):
        """Закрывает браузер и Playwright."""
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
        logger.info("Playwright browser stopped")
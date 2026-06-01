# Используем официальный образ uv с Python 3.13
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

# Копируем только requirements.txt
COPY requirements.txt .

# Создаём виртуальное окружение и устанавливаем зависимости
RUN uv venv /app/.venv && \
    uv pip install --no-cache -r requirements.txt && \
    playwright install chromium && \
    playwright install-deps chromium

# Финальный образ
FROM python:3.13-slim-bookworm

WORKDIR /app

# Копируем виртуальное окружение и браузеры Playwright из билдера
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /root/.cache/ms-playwright /root/.cache/ms-playwright

# Копируем весь исходный код
COPY . .

# Настраиваем PATH
ENV PATH="/app/.venv/bin:$PATH"

# Переменные окружения
ENV MODE=PROD
ENV PYTHONUNBUFFERED=1
ENV PLAYWRIGHT_BROWSERS_PATH=/root/.cache/ms-playwright

# Создаём директорию для загружаемых изображений
RUN mkdir -p /app/static/uploads/blog

# Открываем порт
EXPOSE 6575

# Healthcheck (требует наличия curl в финальном образе)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:6575/health || exit 1

# Запуск
CMD ["uvicorn", "main:asgi_app", "--host", "0.0.0.0", "--port", "6575"]
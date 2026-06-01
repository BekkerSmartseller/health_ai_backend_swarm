# Сборочный этап: устанавливаем Python-зависимости и браузеры
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

# Копируем файл зависимостей
COPY requirements.txt .

# Создаём виртуальное окружение и устанавливаем пакеты
RUN uv venv /app/.venv && \
    uv pip install --no-cache -r requirements.txt

# Устанавливаем браузер Chromium и его системные зависимости
# Используем полный путь к playwright внутри .venv
RUN /app/.venv/bin/playwright install chromium && \
    /app/.venv/bin/playwright install-deps chromium

# Финальный образ
FROM python:3.13-slim-bookworm

WORKDIR /app

# Копируем виртуальное окружение и браузеры
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /root/.cache/ms-playwright /root/.cache/ms-playwright

# Устанавливаем системные зависимости для Chromium (необходимы в финальном образе)
RUN /app/.venv/bin/playwright install-deps chromium

# Копируем исходный код приложения
COPY . .

# Добавляем .venv/bin в PATH
ENV PATH="/app/.venv/bin:$PATH"

# Переменные окружения для продакшена
ENV MODE=PROD
ENV PYTHONUNBUFFERED=1
ENV PLAYWRIGHT_BROWSERS_PATH=/root/.cache/ms-playwright

# Создаём директорию для загружаемых изображений
RUN mkdir -p /app/static/uploads/blog

# Устанавливаем curl для healthcheck (уже может быть, но на всякий случай)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

EXPOSE 6575

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:6575/health || exit 1

CMD ["uvicorn", "main:asgi_app", "--host", "0.0.0.0", "--port", "6575"]
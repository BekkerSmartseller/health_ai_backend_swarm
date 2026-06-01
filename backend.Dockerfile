# health_ai_backend_swarm/Dockerfile
FROM ghcr.io/astral-sh/uv:python3.11 AS builder

WORKDIR /app

# Копируем зависимости
COPY pyproject.toml uv.lock* requirements.txt ./

# Устанавливаем зависимости в виртуальное окружение
RUN uv venv /app/.venv && \
    uv pip install --no-cache -r requirements.txt && \
    playwright install chromium && \
    playwright install-deps chromium

# Финальный образ
FROM python:3.11-slim

WORKDIR /app

# Копируем виртуальное окружение из билдера
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /root/.cache/ms-playwright /root/.cache/ms-playwright

# Копируем исходники
COPY . .

# Настройка PATH
ENV PATH="/app/.venv/bin:$PATH"

# Переменные окружения (можно переопределить через docker-compose)
ENV MODE=PROD
ENV PYTHONUNBUFFERED=1
ENV PLAYWRIGHT_BROWSERS_PATH=/root/.cache/ms-playwright

# Создаём директорию для загруженных изображений
RUN mkdir -p /app/static/uploads/blog

EXPOSE 6575

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:6575/health || exit 1

CMD ["uvicorn", "main:asgi_app", "--host", "0.0.0.0", "--port", "6575"]
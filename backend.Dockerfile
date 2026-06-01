# Используем официальный образ uv с предустановленным Python 3.13
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

# Копируем файлы зависимостей
COPY pyproject.toml uv.lock* README.md ./

# Устанавливаем зависимости проекта в виртуальное окружение
# Флаг --frozen гарантирует, что uv не будет пытаться изменить файл uv.lock
RUN uv sync --frozen --no-dev

# Финальный образ
FROM python:3.13-slim-bookworm

WORKDIR /app

# Копируем виртуальное окружение из образа builder
COPY --from=builder /app/.venv /app/.venv

# Копируем исходный код приложения
COPY . .

# Добавляем виртуальное окружение в PATH
ENV PATH="/app/.venv/bin:$PATH"

# Переменные окружения для продакшена
ENV MODE=PROD
ENV PYTHONUNBUFFERED=1

# Создаём директорию для загруженных изображений
RUN mkdir -p /app/static/uploads/blog

# Устанавливаем рабочую директорию (опционально)
# WORKDIR /app

# Открываем порт
EXPOSE 6575

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:6575/health || exit 1

# Команда для запуска
CMD ["uvicorn", "main:asgi_app", "--host", "0.0.0.0", "--port", "6575"]
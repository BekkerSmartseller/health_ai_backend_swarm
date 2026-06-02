# config.py
import os
from urllib.parse import urlparse
from typing import Optional, List
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class ProjectConfig(BaseSettings):
    # Основные настройки
    MODE: str = os.environ.get('MODE', 'DEV')
    HOST: str = '0.0.0.0'
    PORT: int = 6575
    DEBUG: bool = False
    WORKERS: int =1

    PUBLIC_SITE_URL: str = "https://medexpertai.ru"

    HINDSIGHT_URL: str = "http://localhost:8888"

    # JWT настройки
    JWT_SECRET: str = '4EK0FG+o8+c7tzBNVfjpMkNDi5yARAAKzQlk1O7IKoxLu8nF2EdAh6s5TwpHwrdWT0R'
    JWT_ALGORITHM: str = 'HS256'
    JWT_EXPIRATION: int = 3600*24*90  # 90 дней
    JWT_REFRESH_EXPIRATION: int = 86400*180  # 180 дней
    ADMIN_PASSWORD: str = "4L9GZYuvUQ"

    # Секретные данные (сохранены все для совместимости)
    SANIC_JWT_SECRET: str = '4EK0FG+o8+c7tzBNVfjpMkNDi5yARAAKzQlk1O7IKoxLu8nF2EdAh6s5TwpHwrdWT0R'
    REDIS_CACHE_PASSWORD: str = "f7a9cd93-e976-4b13-a3fb-dcac9ete0cf5"

    # DeepSeek AI настройки
    DEEPSEEK_API_KEY: str = "sk-6db2a26cf2e646ddb58a8e8ca63bfefd"
    DEEPSEEK_BASE_URL: str = "https://api.deepseek.com"
    DEEPSEEK_MODEL: str = "deepseek-v4-flash"
    DEEPSEEK_MAX_RETRIES: int = 3
    DEEPSEEK_TIMEOUT: int = 300

    # CometApi настройки
    COMET_API_KEY: str = "sk-gPcEKuN8g3VOaAAVqjroOiWHW8AUj7N98lZop52LTZMHyOGC"
    COMET_BASE_URL: str = "https://api.cometapi.com/v1"
    COMET_FLUX_URL: str = "https://api.cometapi.com/flux/v1/flux-2-flex"
    COMET_MAX_RETRIES: int = 3
    COMET_TIMEOUT: int = 300
    PROXY_URL: str = "socks5://M0MMzF:EeEvuC@192.166.153.24:8000"  # socks5://user:pass@host:port, если нужен

    # Redis
    REDIS_MQ: str = ""
    REDIS_CACHE_HOST: str = ""
    REDIS_CACHE_PORT: int = 0
    REDIS_PROXY: str = ""

    # База данных
    POSTGRES: str = ""
    POSTGRES_DICT: Optional[dict] = None

    # Deployments (оставлено для совместимости)
    LIST_OF_PRODUCT_CARD_DEPLOYMENT_ID: str = "cc893258-ac4b-48ca-8a00-6b0238c618b4"

    # Добавленные поля для ментальных моделей
    # Модели, которые загружаются для всех агентов
    DEFAULT_MENTAL_MODEL_IDS: List[str] = ["user-profile", "active-tasks", "personality-type"]

    ALLOWED_ORIGINS: List[str] = ["http://localhost:5173","http://192.168.1.100:5173", "http://192.168.1.100:6585", "http://localhost:5174", "http://localhost:3000","https://medexpertai.ru","https://api.medexpertai.ru", "http://backend:6575","http://frontend:3002", "https://medexpertai" ]
    

    @model_validator(mode='after')
    def set_dynamic_config(self) -> 'ProjectConfig':
        if self.MODE == 'PROD':
            self.REDIS_MQ = "redis://default:f7a9cd93-e976-4b13-a3fb-dcac9ete0cf5@192.168.1.100/2"
            self.HINDSIGHT_URL: str = "http://192.168.1.100:8888"
            self.REDIS_CACHE_HOST = "192.168.1.100"
            self.REDIS_CACHE_PORT = 6379
            self.REDIS_PROXY = "redis://default:f7a9cd93-e976-4b13-a3fb-dcac9ete0cf5@192.168.1.100/0"
            self.POSTGRES = "postgres://postgres:cZejbGF7WE5Xr4KQsD83@192.168.1.51:5432/postgres"
            self.DEBUG = True
            self.WORKERS = 4
        else:
            self.REDIS_MQ = "redis://default:f7a9cd93-e976-4b13-a3fb-dcac9ete0cf5@91.122.158.124:63798/2"
            self.HINDSIGHT_URL: str = "http://91.122.158.124:8888"
            self.REDIS_CACHE_HOST = "91.122.158.124"
            self.REDIS_CACHE_PORT = 63799
            self.REDIS_PROXY = "redis://default:f7a9cd93-e976-4b13-a3fb-dcac9ete0cf5@91.122.158.124:63799/0"
            self.POSTGRES = "postgres://postgres:cZejbGF7WE5Xr4KQsD83@91.122.158.124:54329/postgres"
            self.DEBUG = True
            self.WORKERS = 1

        if self.POSTGRES:
            parsed = urlparse(self.POSTGRES)
            self.POSTGRES_DICT = {
                'host': parsed.hostname or 'localhost',
                'port': parsed.port or 5432,
                'user': parsed.username or 'postgres',
                'password': parsed.password or '',
                'database': parsed.path.lstrip('/') or 'postgres'
            }
        return self

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )

config = ProjectConfig()
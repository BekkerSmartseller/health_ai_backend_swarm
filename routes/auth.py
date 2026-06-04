# health_ai_backend_swarm/routes/auth.py
import uuid
import random
import jwt
import aiohttp
from datetime import datetime, timedelta
from urllib.parse import urlencode
from litestar import Controller, post, get, Request, Response
from litestar.response import Redirect
from litestar.exceptions import HTTPException
from litestar.datastructures import Cookie
from litestar.di import Provide
from config import config
from services.user_db import create_or_get_user, update_user_profile, get_user_by_email, get_user_by_id
import redis.asyncio as aioredis
import logging

logger = logging.getLogger(__name__)

redis_client = None

def init_redis(app):
    global redis_client
    redis_client = aioredis.from_url(config.REDIS_CACHE_URL, decode_responses=True)

class AuthController(Controller):
    path = "/auth"

    async def _send_email_code(self, email: str, code: str):
        """Отправка кода через SendPulse (aiohttp)."""
        async with aiohttp.ClientSession() as session:
            # Получить токен
            auth_resp = await session.post(
                "https://api.sendpulse.com/oauth/access_token",
                data={"grant_type": "client_credentials",
                      "client_id": config.SENDPULSE_API_KEY,
                      "client_secret": config.SENDPULSE_API_SECRET}
            )
            token_data = await auth_resp.json()
            access_token = token_data.get("access_token")
            if not access_token:
                raise HTTPException(status_code=500, detail="SendPulse auth failed")
            # Отправить email
            resp = await session.post(
                "https://api.sendpulse.com/smtp/emails",
                headers={"Authorization": f"Bearer {access_token}"},
                json={
                    "email": {
                        "html": f"<p>Ваш код подтверждения: <b>{code}</b></p><p>Код действителен 10 минут.</p>",
                        "subject": "Код входа на MedExpertAI",
                        "from": {"name": "MedExpertAI", "email": "noreply@medexpertai.ru"},
                        "to": [{"email": email}]
                    }
                }
            )
            if resp.status != 200:
                raise HTTPException(status_code=500, detail="Email send failed")

    @post("/request-code")
    async def request_code(self, data: dict) -> dict:
        email = data.get("email")
        if not email:
            raise HTTPException(status_code=400, detail="Email required")
        code = str(random.randint(1000, 9999))
        await redis_client.setex(f"verify_code:{email}", 600, code)
        await self._send_email_code(email, code)
        return {"message": "Code sent"}

    @post("/verify-code")
    async def verify_code(self, data: dict, response: Response) -> dict:
        email = data.get("email")
        code = data.get("code")
        if not email or not code:
            raise HTTPException(status_code=400, detail="Email and code required")
        stored = await redis_client.get(f"verify_code:{email}")
        if stored != code:
            raise HTTPException(status_code=401, detail="Invalid code")
        await redis_client.delete(f"verify_code:{email}")
        user = await create_or_get_user(email, auth_provider="email")
        if not user.get("name") or not user.get("age") or not user.get("user_type"):
            return {"require_profile": True, "user": {k: user[k] for k in ("id", "email", "name", "age", "user_type")}}
        token = self._generate_token(user["id"], email)
        response.set_cookie("token", token, max_age=3600*24*90, path="/", httponly=True, secure=True, samesite="lax")
        return {"success": True, "user": user}

    @post("/complete-profile")
    async def complete_profile(self, data: dict) -> Response:
        email = data.get("email")
        name = data.get("name")
        age = data.get("age")
        user_type = data.get("user_type", "user")
        if not email:
            raise HTTPException(status_code=400, detail="Email required")
        user = await update_user_profile(email, name=name, age=age, user_type=user_type)
        token = self._generate_token(user["id"], email)
        response = Response({"success": True, "user": user})
        response.set_cookie("token", token, max_age=3600*24*90, path="/", httponly=True, secure=True, samesite="lax")
        return response

    @get("/google")
    async def auth_google(self) -> Response:
        params = {
            "client_id": config.GOOGLE_CLIENT_ID,
            "redirect_uri": f"{config.BACKEND_URL}/auth/google/callback",
            "response_type": "code",
            "scope": "email profile",
            "state": str(uuid.uuid4())
        }
        url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)
        return Redirect(url)
    
    @get("/google/callback")
    async def auth_google_callback(self, request: Request) -> Redirect:
        code = request.query_params.get("code")
        if not code:
            raise HTTPException(status_code=400, detail="Missing code")
        
        async with aiohttp.ClientSession() as session:
            token_resp = await session.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "code": code,
                    "client_id": config.GOOGLE_CLIENT_ID,
                    "client_secret": config.GOOGLE_CLIENT_SECRET,
                    "redirect_uri": f"{config.BACKEND_URL}/auth/google/callback",
                    "grant_type": "authorization_code"
                }
            )
            token_data = await token_resp.json()
            userinfo = await session.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {token_data['access_token']}"}
            )
            profile = await userinfo.json()
        
        email = profile.get("email")
        name = profile.get("name")
        if not email:
            raise HTTPException(status_code=400, detail="No email from Google")
        
        user = await create_or_get_user(email, auth_provider="google", provider_id=profile["id"], name=name)
        
        # Если профиль неполный – редирект на фронт без куки
        if user.get("age") is None or user.get("user_type") is None:
            return Redirect(f"{config.PUBLIC_SITE_URL}/auth/callback?require_profile=true&user_id={user['id']}&email={email}&name={name or ''}")
        
        # Полный профиль – создаём токен и редиректим с кукой
        token = self._generate_token(user["id"], email)
    
        # ВСЕГДА ставим куку, даже при неполном профиле
        redirect_response = Redirect(
            f"{config.PUBLIC_SITE_URL}/auth/callback" +
            (f"?require_profile=true&user_id={user['id']}&email={email}&name={name or ''}" 
            if not user.get("age") or not user.get("user_type") else "")
        )
        redirect_response.set_cookie(
            "token", token,
            max_age=3600*24*90, path="/", httponly=True, secure=True, samesite="lax"
        )
        return redirect_response

    @get("/yandex")
    async def auth_yandex(self) -> Response:
        params = {
            "client_id": config.YANDEX_CLIENT_ID,
            "redirect_uri": f"{config.BACKEND_URL}/auth/yandex/callback",
            "response_type": "code",
            "scope": "login:email login:info"
        }
        url = "https://oauth.yandex.ru/authorize?" + urlencode(params)
        return Redirect(url)
    
    @get("/yandex/callback")
    async def auth_yandex_callback(self, request: Request) -> Redirect:
        code = request.query_params.get("code")
        if not code:
            raise HTTPException(status_code=400, detail="Missing code")
        
        async with aiohttp.ClientSession() as session:
            token_resp = await session.post(
                "https://oauth.yandex.ru/token",
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "client_id": config.YANDEX_CLIENT_ID,
                    "client_secret": config.YANDEX_CLIENT_SECRET
                }
            )
            token_data = await token_resp.json()
            access_token = token_data.get("access_token")
            userinfo = await session.get(
                "https://login.yandex.ru/info",
                params={"format": "json"},
                headers={"Authorization": f"OAuth {access_token}"}
            )
            profile = await userinfo.json()
        
        email = profile.get("default_email")
        name = profile.get("real_name") or profile.get("display_name")
        if not email:
            raise HTTPException(status_code=400, detail="No email from Yandex")
        
        user = await create_or_get_user(email, auth_provider="yandex", provider_id=profile["id"], name=name)
        
        if not user.get("age") or not user.get("user_type"):
            return Redirect(f"{config.PUBLIC_SITE_URL}/auth/callback?require_profile=true&user_id={user['id']}&email={email}&name={name or ''}")
        
        token = self._generate_token(user["id"], email)
        redirect_response = Redirect(f"{config.PUBLIC_SITE_URL}/auth/callback")
        redirect_response.set_cookie(
            "token", token,
            max_age=3600 * 24 * 90,
            path="/",
            httponly=True,
            secure=True,
            samesite="lax"
        )
        return redirect_response

    @get("/me")
    async def get_current_user(self, request: Request) -> dict:
        token = request.cookies.get("token")
        if not token:
            raise HTTPException(status_code=401, detail="Not authenticated")
        
        try:
            payload = jwt.decode(token, config.JWT_SECRET, algorithms=["HS256"])
            user_id = payload.get("sub")
            if not user_id:
                raise HTTPException(status_code=401, detail="Invalid token")
            
            # Получаем пользователя из БД
            from services.user_db import get_user_by_id
            user = await get_user_by_id(user_id)
            if not user or not user.get("is_active"):
                raise HTTPException(status_code=401, detail="User not found")
            
            # Не возвращаем чувствительные поля (пароль и т.п.)
            return {"user": user}
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.InvalidTokenError as e:
            raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")

    @post("/logout")
    async def logout(self) -> Response:
        response = Response({"success": True})
        response.delete_cookie(
            key="token",
            path="/",
        )
        return response

    def _generate_token(self, user_id: str, email: str) -> str:
        # user_id может быть UUID, преобразуем в строку
        payload = {
            "sub": str(user_id),
            "email": email,
            "exp": datetime.utcnow() + timedelta(days=90)
        }
        return jwt.encode(payload, config.JWT_SECRET, algorithm="HS256")
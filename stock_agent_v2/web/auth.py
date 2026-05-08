"""세션 + 비번 검증.

세션은 itsdangerous 로 서명한 쿠키(`session`)에 username 만 저장.
서명 키: WEB_SECRET_KEY 환경변수 (없으면 임시 키 — 재시작 시 모두 로그아웃됨).
"""
from __future__ import annotations

import base64
import hashlib
import os
import secrets

import bcrypt
from itsdangerous import BadSignature, URLSafeSerializer

from database import load_user

COOKIE_NAME = "stock_agent_session"

_SECRET = os.getenv("WEB_SECRET_KEY") or secrets.token_urlsafe(48)
_serializer = URLSafeSerializer(_SECRET, salt="stock-agent-web")


def _prehash(plain: str) -> bytes:
    """SHA-256 후 base64 → 항상 44바이트 ASCII.
    bcrypt 의 72바이트 제한 + NUL/멀티바이트 이슈 동시 우회. 한글 비번 안전.
    """
    digest = hashlib.sha256(plain.encode("utf-8")).digest()
    return base64.b64encode(digest)


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(_prehash(plain), bcrypt.gensalt()).decode("ascii")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(_prehash(plain), hashed.encode("ascii"))
    except Exception:
        return False


def authenticate(username: str, password: str) -> dict | None:
    user = load_user(username)
    if not user:
        return None
    if not verify_password(password, user["password_hash"]):
        return None
    return user


def make_session_cookie(username: str) -> str:
    return _serializer.dumps({"u": username})


def parse_session_cookie(value: str) -> str | None:
    try:
        data = _serializer.loads(value)
        return data.get("u")
    except BadSignature:
        return None
    except Exception:
        return None

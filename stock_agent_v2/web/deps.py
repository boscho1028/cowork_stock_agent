"""FastAPI 의존성: 로그인 가드 / 현재 사용자 조회."""
from __future__ import annotations

from fastapi import Cookie, Request
from fastapi.responses import RedirectResponse

from database import load_user
from web.auth import COOKIE_NAME, parse_session_cookie


class _LoginRequired(Exception):
    """라우트가 직접 raise — 미들웨어가 RedirectResponse 로 변환."""


def current_user_optional(stock_agent_session: str | None = Cookie(default=None)):
    """로그인 안 됐으면 None. 컨텐츠 페이지에선 require_user 쓸 것."""
    if not stock_agent_session:
        return None
    username = parse_session_cookie(stock_agent_session)
    if not username:
        return None
    return load_user(username)


def require_user(request: Request):
    """로그인 안 됐으면 /login?next=... 으로 리다이렉트되는 응답을 raise.
    FastAPI dependency 로 사용할 땐 RedirectResponse 를 직접 raise 할 수 없어
    예외로 던지고 핸들러가 잡는 패턴 대신, 본 함수는 None 일 때 RedirectResponse
    를 *반환* 하지 못하므로 라우트 안에서 직접 분기하기 위한 옵셔널 형태로 둠.
    실제 라우트에선 `user = require_user_or_redirect(request)` 의 분기를 사용.
    """
    raise NotImplementedError("use require_user_or_redirect() inside a route")


def require_user_or_redirect(request: Request):
    """로그인 사용자 dict 반환. 없으면 RedirectResponse 를 반환 (라우트가 그대로
    return 하도록). 호출 패턴:

        u = require_user_or_redirect(request)
        if isinstance(u, RedirectResponse):
            return u
        # 이후 user 사용
    """
    cookie = request.cookies.get(COOKIE_NAME)
    if cookie:
        username = parse_session_cookie(cookie)
        if username:
            user = load_user(username)
            if user:
                return user
    nxt = request.url.path or "/"
    return RedirectResponse(url=f"/login?next={nxt}", status_code=303)

"""시장 경고 브리핑 — market_warnings 테이블."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from database import load_market_warnings
from web.deps import require_user_or_redirect

router = APIRouter()


@router.get("/warnings")
def warnings_list(request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    items = load_market_warnings(limit=30)
    return request.app.state.templates.TemplateResponse(
        request, "warnings/list.html",
        {"items": items, "user": u},
    )

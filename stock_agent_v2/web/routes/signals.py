"""시그널 스캔 결과 — signals 테이블."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from database import load_signals_grouped_by_date
from web.deps import require_user_or_redirect

router = APIRouter()


@router.get("/signals")
def signals_list(request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    groups = load_signals_grouped_by_date(limit_days=30)
    return request.app.state.templates.TemplateResponse(
        request, "signals/list.html",
        {"groups": groups, "user": u},
    )

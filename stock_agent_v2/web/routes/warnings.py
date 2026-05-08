"""시장 경고 브리핑 — F&G 레벨 색상 카드."""
from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from database import load_market_warnings
from web.deps import require_user_or_redirect

router = APIRouter()


def _fg_band(score) -> str:
    """CNN F&G 점수 → 색상 밴드 클래스."""
    if score is None:
        return ""
    try:
        s = int(score)
    except (TypeError, ValueError):
        return ""
    if s < 25:  return "extreme-fear"
    if s < 45:  return "fear"
    if s < 55:  return "neutral"
    if s < 75:  return "greed"
    return "extreme-greed"


def _today_iso() -> str:
    return date.today().isoformat()


@router.get("/warnings")
def warnings_list(request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    raw = load_market_warnings(limit=30)
    today = _today_iso()
    items: list[dict] = []
    for it in raw:
        asof = it.get("asof") or ""
        items.append({
            **it,
            "fg_band":  _fg_band(it.get("fg_score")),
            "is_today": asof.startswith(today),
        })
    return request.app.state.templates.TemplateResponse(
        request, "warnings/list.html",
        {"items": items, "user": u},
    )

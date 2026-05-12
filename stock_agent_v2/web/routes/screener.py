"""자연어 스크리너 라우트 — 시그널 정의·실행·관리.

GET  /screener                 : 저장된 시그널 리스트 + 새 시그널 폼
POST /screener/new             : 새 시그널 저장 (옵션: 즉시 실행)
POST /screener/{id}/run        : 저장된 시그널 즉시 실행 → 결과 페이지
POST /screener/{id}/toggle     : enable/disable (morning batch 포함 여부)
POST /screener/{id}/delete     : 삭제
POST /screener/adhoc           : 저장 없이 즉시 실행 (테스트용)
"""
from __future__ import annotations

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import RedirectResponse

from database import (
    create_nl_signal, list_nl_signals, get_nl_signal,
    set_nl_signal_enabled, delete_nl_signal,
)
from web.deps import require_user_or_redirect

router = APIRouter()


def _render(request: Request, template: str, ctx: dict):
    u = ctx.get("user") or {}
    return request.app.state.templates.TemplateResponse(
        request, template, {**ctx, "user": u},
    )


@router.get("/screener")
def screener_page(request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    signals = list_nl_signals()
    return _render(request, "screener/index.html",
                   {"signals": signals, "user": u})


@router.post("/screener/new")
def screener_new(
    request: Request,
    name:   str = Form(...),
    prompt: str = Form(...),
    scope:  str = Form("portfolio"),
    enabled: str = Form("on"),
    run_now: str = Form(""),
):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    name = name.strip()
    prompt = prompt.strip()
    if not name or not prompt:
        raise HTTPException(400, "name/prompt required")
    if scope not in ("portfolio", "universe"):
        scope = "portfolio"
    sid = create_nl_signal(name, prompt, scope, enabled=bool(enabled))
    if run_now:
        return RedirectResponse(url=f"/screener/{sid}/run", status_code=307)
    return RedirectResponse(url="/screener", status_code=303)


@router.post("/screener/{signal_id}/run")
def screener_run_saved(signal_id: int, request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    sig = get_nl_signal(signal_id)
    if not sig:
        raise HTTPException(404, "signal not found")
    from nl_screener import run_nl_signal, save_matches_to_signals
    from database import update_nl_signal_run
    results = run_nl_signal(sig["prompt"], scope=sig["scope"])
    matched = [r for r in results if r.match]
    save_matches_to_signals(sig["id"], sig["name"], sig["scope"], results)
    update_nl_signal_run(sig["id"], len(matched))
    return _render(request, "screener/results.html", {
        "signal": sig,
        "results": [{"ticker": r.ticker, "name": r.name,
                     "match": r.match, "reason": r.reason} for r in results],
        "matched_count": len(matched),
        "total":         len(results),
        "user":          u,
    })


@router.post("/screener/adhoc")
def screener_run_adhoc(
    request: Request,
    name:   str = Form(""),
    prompt: str = Form(...),
    scope:  str = Form("portfolio"),
):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    prompt = prompt.strip()
    if not prompt:
        raise HTTPException(400, "prompt required")
    if scope not in ("portfolio", "universe"):
        scope = "portfolio"
    from nl_screener import run_nl_signal, save_matches_to_signals
    results = run_nl_signal(prompt, scope=scope)
    matched = [r for r in results if r.match]
    # ad-hoc 도 signals 에 저장 (rule='nl:adhoc') — 추후 /signals 페이지에서 검토 가능
    save_matches_to_signals(None, name or "임시", scope, results)
    return _render(request, "screener/results.html", {
        "signal": {"id": None, "name": name or "(임시)",
                   "prompt": prompt, "scope": scope},
        "results": [{"ticker": r.ticker, "name": r.name,
                     "match": r.match, "reason": r.reason} for r in results],
        "matched_count": len(matched),
        "total":         len(results),
        "user":          u,
    })


@router.post("/screener/{signal_id}/toggle")
def screener_toggle(signal_id: int, request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    sig = get_nl_signal(signal_id)
    if not sig:
        raise HTTPException(404, "signal not found")
    set_nl_signal_enabled(signal_id, not bool(sig.get("enabled")))
    return RedirectResponse(url="/screener", status_code=303)


@router.post("/screener/{signal_id}/delete")
def screener_delete(signal_id: int, request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    delete_nl_signal(signal_id)
    return RedirectResponse(url="/screener", status_code=303)

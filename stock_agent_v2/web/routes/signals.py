"""시그널 스캔 결과 — 시장별 (KR/US) 카드 그리드, 우선순위·오늘 강조."""
from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

import config
from database import get_conn
from web.deps import require_user_or_redirect

router = APIRouter()


# 우선순위 정렬 키 — 🔴 > 🟠 > 🟡
_PRIO_ORDER = {"🔴": 0, "🟠": 1, "🟡": 2}


def _build_signals_grid(limit_days: int = 30) -> dict:
    """signals 테이블 → {kr: [...], us: [...]} 카드 리스트 (각각 최신순+우선순위순)."""
    portfolio = config.get_portfolio_detail()
    universe  = config.get_universe_detail()
    today_iso = date.today().isoformat()

    kr: list[dict] = []
    us: list[dict] = []

    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT scan_date, ticker, name, rule, title, detail, priority
            FROM signals
            WHERE scan_date >= date('now', ?)
            ORDER BY scan_date DESC, id DESC
            """,
            (f"-{int(limit_days)} days",),
        )
        for scan_date, ticker, name, rule, title, detail, priority in cur.fetchall():
            info = universe.get(ticker) or portfolio.get(ticker) or {}
            is_overseas = info.get("is_overseas",
                                   not (ticker or "").isdigit())
            card = {
                "scan_date":    scan_date or "",
                "ticker":       ticker,
                "name":         (name or info.get("name", "") or "").strip(),
                "rule":         rule or "",
                "title":        (title or "").strip(),
                "detail":       (detail or "").strip(),
                "priority":     priority or "🟡",
                "is_portfolio": ticker in portfolio,
                "is_today":     (scan_date or "") == today_iso,
            }
            (us if is_overseas else kr).append(card)

    # 같은 날짜 안에서는 우선순위 (🔴 → 🟠 → 🟡) 순으로
    def _key(c: dict):
        return (c["scan_date"] or "",
                -_PRIO_ORDER.get(c["priority"], 9))   # 빨강이 위로 오게 음수

    kr.sort(key=_key, reverse=True)
    us.sort(key=_key, reverse=True)
    return {"kr": kr, "us": us}


@router.get("/signals")
def signals_list(request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    grid = _build_signals_grid(limit_days=30)
    return request.app.state.templates.TemplateResponse(
        request, "signals/list.html",
        {"grid": grid, "user": u},
    )

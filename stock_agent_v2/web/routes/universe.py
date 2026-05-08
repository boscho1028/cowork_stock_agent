"""Universe / portfolio 관리 API.

/reports 페이지의 별표·X 버튼·종목 추가 폼이 호출하는 JSON 엔드포인트.
- POST /api/universe/add            : 새 종목을 universe 에 추가
- POST /api/universe/{ticker}/remove: universe (와 portfolio) 에서 제거
- POST /api/portfolio/{ticker}/toggle: portfolio 편입/제외 토글
"""
from __future__ import annotations

import re

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

import config
from web.deps import require_user_or_redirect

router = APIRouter()

VALID_EXCHANGES = {"KRX", "KOSPI", "KOSDAQ", "NASDAQ", "NYSE", "AMEX"}
TICKER_RE = re.compile(r"^[A-Za-z0-9.\-]{1,16}$")


def _validate_ticker(t: str) -> str:
    t = (t or "").strip().upper()
    if not TICKER_RE.match(t):
        raise HTTPException(400, "bad ticker")
    return t


def _require_login(request: Request):
    """API 라우트용 — 로그인 안 됐으면 401."""
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        raise HTTPException(401, "login required")
    return u


@router.post("/api/universe/add")
async def universe_add(request: Request):
    _require_login(request)
    body = await request.json()
    ticker = _validate_ticker(body.get("ticker", ""))
    name = (body.get("name") or "").strip() or ticker
    exchange = (body.get("exchange") or "").strip().upper() or None
    if exchange and exchange not in VALID_EXCHANGES:
        raise HTTPException(400, f"bad exchange: {exchange}")
    if not config.append_universe_row(ticker, name, exchange):
        raise HTTPException(409, "already in universe")
    info = config.get_universe_detail().get(ticker, {})
    return {
        "ok":           True,
        "ticker":       ticker,
        "name":         info.get("name", name),
        "exchange":     info.get("exchange", exchange or ""),
        "is_overseas":  info.get("is_overseas", False),
        "is_portfolio": ticker in config.get_portfolio_detail(),
    }


@router.post("/api/universe/{ticker}/remove")
async def universe_remove(ticker: str, request: Request):
    _require_login(request)
    ticker = _validate_ticker(ticker)
    if not config.remove_universe_row(ticker):
        raise HTTPException(404, "not in universe")
    return {"ok": True, "ticker": ticker}


@router.post("/api/portfolio/{ticker}/toggle")
async def portfolio_toggle(ticker: str, request: Request):
    _require_login(request)
    ticker = _validate_ticker(ticker)
    if ticker in config.get_portfolio_detail():
        config.remove_portfolio_row(ticker)
        return {"ok": True, "ticker": ticker, "in_portfolio": False}
    if ticker not in config.get_universe_detail():
        raise HTTPException(404, "not in universe")
    if not config.append_portfolio_row(ticker):
        raise HTTPException(500, "could not add")
    return {"ok": True, "ticker": ticker, "in_portfolio": True}

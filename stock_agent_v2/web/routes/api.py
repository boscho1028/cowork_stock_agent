"""/api/* — read-only JSON endpoints for sibling services (my_palantir 등).

세션 쿠키 검증 후 portfolio/universe 등 ticker-level 데이터를 노출.
HTML 페이지와 달리 401 을 JSON 으로 반환해서 httpx 클라이언트가 처리 가능.

확장 시 추가 후보:
  GET /api/candles/{ticker}     — 캔들 OHLCV
  GET /api/filings/{ticker}     — 최근 DART/SEC 공시
  GET /api/signals/{ticker}     — 현재 시그널 상태
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

import config as _cfg
from web.deps import require_user_or_401

router = APIRouter(prefix="/api", tags=["api"])


@router.get("/universe")
def list_universe(request: Request, _user=Depends(require_user_or_401)):
    """Universe + portfolio 합집합. config.get_universe_detail() 이 이미 머지함."""
    return [
        {"ticker": t, "name": v["name"], "exchange": v["exchange"],
         "is_overseas": v["is_overseas"]}
        for t, v in _cfg.get_universe_detail().items()
    ]


@router.get("/portfolio")
def list_portfolio(request: Request, _user=Depends(require_user_or_401)):
    """실제 보유 종목 + 수량."""
    return [
        {"ticker": t, "name": v["name"], "qty": v["qty"],
         "exchange": v["exchange"], "is_overseas": v["is_overseas"]}
        for t, v in _cfg.get_portfolio_detail().items()
    ]

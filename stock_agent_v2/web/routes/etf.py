"""ETF 모멘텀 스크리닝 결과 — D:\\momentum_etf 가 매일 08:50 Turso 에 쓰는
etf_screen_unified / etf_screen_kr / etf_screen_us 테이블을 그대로 읽어 표시.
별도 코드 머지 없이 같은 Turso replica DB 를 공유하므로 stock_agent 웹에서
즉시 노출 가능.
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from database import get_conn
from web.banner import build_status_banner
from web.deps import require_user_or_redirect

router = APIRouter()


def _latest_date(conn) -> str | None:
    """3 테이블 중 가장 최근 screen_date — 한 페이지로 같은 시점 데이터 보여주기."""
    row = conn.execute("""
        SELECT MAX(d) FROM (
            SELECT MAX(screen_date) AS d FROM etf_screen_unified
            UNION ALL
            SELECT MAX(screen_date) AS d FROM etf_screen_kr
            UNION ALL
            SELECT MAX(screen_date) AS d FROM etf_screen_us
        )
    """).fetchone()
    return row[0] if row and row[0] else None


def _rows(conn, sql: str, params: tuple) -> list[dict]:
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


@router.get("/etf")
def etf_page(request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    banner = build_status_banner("etf_screen", "ETF 모멘텀 스크리닝")
    with get_conn() as conn:
        asof = _latest_date(conn)
        if not asof:
            return request.app.state.templates.TemplateResponse(
                request, "etf/index.html",
                {"asof": None, "unified": [], "kr": [], "us": [],
                 "banner": banner, "user": u},
            )
        unified = _rows(conn, """
            SELECT theme, category, us_ticker,
                   us_return_1w, us_return_1m, us_return_3m,
                   kr_ticker, kr_ticker_name,
                   kr_return_1w, kr_return_1m, kr_return_3m,
                   match_score, discount_rate, stop_loss
            FROM etf_screen_unified
            WHERE screen_date = ?
            ORDER BY match_score DESC, theme
        """, (asof,))
        kr = _rows(conn, """
            SELECT ticker, name,
                   return_1d, return_1w, return_1m, return_3m,
                   momentum_score, avg_trading_value,
                   current_price, atr14, stop_loss
            FROM etf_screen_kr
            WHERE screen_date = ?
            ORDER BY momentum_score DESC
            LIMIT 30
        """, (asof,))
        us = _rows(conn, """
            SELECT ticker, name,
                   return_1d, return_1w, return_1m, return_3m,
                   momentum_score, avg_volume_usd,
                   current_price, atr14, stop_loss
            FROM etf_screen_us
            WHERE screen_date = ?
            ORDER BY momentum_score DESC
            LIMIT 30
        """, (asof,))
    return request.app.state.templates.TemplateResponse(
        request, "etf/index.html",
        {"asof": asof, "unified": unified, "kr": kr, "us": us,
         "banner": banner, "user": u},
    )

"""수급 동향 — 외국인·기관 1D/3D/1W/1M 누적 순매수.

오후 evening 배치가 텔레그램으로 보내는 수급 차트·요약을 웹에도 노출한다.
데이터는 `investor_trend` 테이블 (KIS API 가 KR universe 전체에 대해 수집).
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import RedirectResponse

import config
from database import load_investor_trend
from web.banner import build_status_banner
from web.deps import require_user_or_redirect

router = APIRouter()

_PERIOD_DAYS = (("1D", 1), ("3D", 3), ("1W", 5), ("1M", 20))
_chart_cache: dict[str, bytes] = {}


def _fmt_amt_mkrw(v: int) -> str:
    """백만원 단위 → '+123억' / '-4.5조' 등 사람이 읽기 좋게."""
    if not v:
        return "±0"
    sign = "+" if v > 0 else "-"
    a = abs(v)
    if a >= 1_000_000:
        return f"{sign}{a/1_000_000:.1f}조"
    if a >= 100:
        return f"{sign}{a/100:.0f}억"
    return f"{sign}{a}백만"


def _kr_portfolio() -> list[str]:
    """portfolio 의 국내 종목 — 오후 텔레그램 수급 차트와 동일한 범위."""
    return [t for t in config.PORTFOLIO if not config.is_overseas(t)]


def _build_rows(tickers: list[str]) -> tuple[list[dict], str]:
    """투자자 동향 행을 ticker 별로 집계.
    반환: (rows, asof) — rows = [{ticker, name, periods: {"1D": (f, i), ...}}, ...]
    """
    rows_data: list[dict] = []
    asof = ""
    for t in tickers:
        rows = load_investor_trend(t, days=20)
        if not rows:
            continue
        if not asof:
            asof = rows[0]["trade_date"]
        periods: dict[str, tuple[int, int]] = {}
        for lbl, n in _PERIOD_DAYS:
            f = sum((r.get("foreign_amt") or 0) for r in rows[:n])
            i = sum((r.get("inst_amt")    or 0) for r in rows[:n])
            periods[lbl] = (f, i)
        info = (config.get_portfolio_detail().get(t)
             or config.get_universe_detail().get(t)
             or {})
        rows_data.append({
            "ticker":  t,
            "name":    info.get("name", t),
            "periods": periods,
        })
    return rows_data, asof


def _periods_line(periods: dict, idx: int) -> str:
    """idx=0 외국인, idx=1 기관."""
    return (f"1D {_fmt_amt_mkrw(periods['1D'][idx])}"
            f"  3D {_fmt_amt_mkrw(periods['3D'][idx])}"
            f"  1W {_fmt_amt_mkrw(periods['1W'][idx])}"
            f"  1M {_fmt_amt_mkrw(periods['1M'][idx])}")


@router.get("/supply")
def supply_page(request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    rows_data, asof = _build_rows(_kr_portfolio())
    # 1M (외국인+기관) 합 큰 순 → 차트와 동일한 정렬 유지
    rows_data.sort(
        key=lambda r: (r["periods"]["1M"][0] or 0) + (r["periods"]["1M"][1] or 0),
        reverse=True,
    )
    items = []
    for r in rows_data:
        items.append({
            "ticker":  r["ticker"],
            "name":    r["name"],
            "foreign": _periods_line(r["periods"], 0),
            "inst":    _periods_line(r["periods"], 1),
        })
    banner = build_status_banner("kr_evening", "한국 저녁 분석 (수급 포함)")
    return request.app.state.templates.TemplateResponse(
        request, "supply/index.html",
        {"items": items, "asof": asof, "banner": banner, "user": u},
    )


@router.get("/charts/supply.png")
def supply_chart_png(request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    rows_data, asof = _build_rows(_kr_portfolio())
    if not rows_data:
        raise HTTPException(404, "no data")
    # asof 기준 메모리 캐시 — 수급 데이터는 하루 1회 갱신.
    if asof and asof in _chart_cache:
        return Response(content=_chart_cache[asof], media_type="image/png")

    from chart_generator import generate_supply_chart
    chart_input = [(r["ticker"], r["name"], r["periods"]) for r in rows_data]
    png = generate_supply_chart(chart_input, asof=asof)
    if not png:
        raise HTTPException(404, "no data")
    if asof:
        _chart_cache.clear()
        _chart_cache[asof] = png
    return Response(
        content=png, media_type="image/png",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )

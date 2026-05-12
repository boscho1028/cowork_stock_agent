"""한국 시장 현황 페이지 — stockeasy.intellio.kr 무인증 API 직접 호출.

texts:
- 시장 단계 (KOSPI/KOSDAQ status + 분배일)
- 지수 + 시장 신호 (단기/장기)
- 신용잔고 추이 (최근 20일)
- 원자재 시세 (귀금속/에너지/비철금속/...)
- DRAM ETF (메모리 사이클 indicator)

stockeasy 가 일시 장애여도 페이지는 partial 로 렌더 (없는 섹션 자동 숨김).
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from stockeasy_client import (
    fetch_big_picture, fetch_indices, fetch_credit_balance,
    fetch_commodity_quotes, fetch_dram_etf_overview, STATUS_LABEL,
)
from web.deps import require_user_or_redirect

router = APIRouter()


def _build_market_status() -> list[dict]:
    """KOSPI/KOSDAQ 시장 단계 (big_picture) → 카드용."""
    bp = fetch_big_picture() or {}
    cards = []
    for key in ("kospi", "kosdaq"):
        d = bp.get(key) or {}
        status = d.get("status")
        label, exposure = STATUS_LABEL.get(
            status, (status or "정보 없음", "?")
        )
        cards.append({
            "market":   key.upper(),
            "status":   status,
            "label":    label,
            "exposure": exposure,
            "rally_day_count":   d.get("rally_day_count", 0),
            "active_distribution_count": d.get("active_distribution_count", 0),
        })
    return cards


def _build_indices_summary() -> dict | None:
    """코스피/코스닥 지수 + 단·장기 시장 신호."""
    idx = fetch_indices()
    if not idx:
        return None
    out = {"indices": [], "short_term": idx.get("short_term_signal"),
           "long_term": idx.get("long_term_signal")}
    for it in (idx.get("indices") or []):
        out["indices"].append({
            "code":        it.get("index_code"),
            "name":        it.get("index_name"),
            "value":       it.get("current_value"),
            "change":      it.get("change_amount"),
            "change_pct":  it.get("price_change_percent"),
            "advance":     it.get("rising_stocks"),
            "decline":     it.get("falling_stocks"),
            "unchanged":   it.get("unchanged_stocks"),
            "upper_limit": it.get("upper_limit_stocks"),
            "lower_limit": it.get("lower_limit_stocks"),
            "listed":      it.get("listed_stocks"),
        })
    return out


def _build_credit_balance() -> dict | None:
    """신용잔고 추이 — 처음/끝 비교 + 시리즈 (그래프용)."""
    cb = fetch_credit_balance(days=20)
    if not cb or not cb.get("data"):
        return None
    series = cb["data"]
    first = series[0]
    last = series[-1]
    return {
        "start_date": cb.get("start_date"),
        "end_date":   cb.get("end_date"),
        "total_count": cb.get("total_count"),
        "first":      first,
        "last":       last,
        "delta":      (last.get("total_credit", 0) or 0)
                       - (first.get("total_credit", 0) or 0),
        "series":     series,
    }


def _build_commodities() -> list[dict]:
    """원자재 카테고리별 그룹화."""
    q = fetch_commodity_quotes() or {}
    cats = q.get("categories") or []
    items = q.get("commodities") or []
    # API 는 camelCase (changePercentage) — 템플릿에서 쓰기 좋게 snake_case 로 정규화
    by_cat: dict[str, list[dict]] = {}
    for it in items:
        norm = {
            **it,
            "change_percent": it.get("changePercentage"),
        }
        by_cat.setdefault(it.get("category", "etc"), []).append(norm)
    cat_labels = {c["key"]: c.get("label") for c in cats}
    out = []
    for key, lst in by_cat.items():
        # 키 이름은 'rows' — Jinja 가 dict 의 `.items` 메서드와 헷갈리는 충돌 피함.
        out.append({
            "key":   key,
            "label": cat_labels.get(key, key),
            "rows":  sorted(lst, key=lambda x: -(x.get("tier") or 0)),
        })
    out.sort(key=lambda c: -sum(1 for x in c["rows"] if x.get("featured")))
    return out


def _build_dram_etf() -> dict | None:
    """DRAM ETF — 가격 + 시리즈 + holdings 상위."""
    d = fetch_dram_etf_overview()
    if not d:
        return None
    holdings = d.get("holdings") or []
    # 시가총액(가중치) 큰 순 상위 10
    holdings_top = sorted(
        holdings, key=lambda h: -(h.get("weight") or 0),
    )[:10]
    return {
        "ticker":      d.get("ticker"),
        "fund_name":   d.get("fund_name"),
        "latest_date": d.get("latest_date"),
        "current_nav":   d.get("current_nav"),
        "current_price": d.get("current_price"),
        "current_aum":   d.get("current_aum"),
        "series":      d.get("series") or [],
        "holdings":    holdings_top,
    }


@router.get("/market")
def market_page(request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u

    ctx = {
        "user":        u,
        "status":      _build_market_status(),
        "indices":     _build_indices_summary(),
        "credit":      _build_credit_balance(),
        "commodities": _build_commodities(),
        "dram":        _build_dram_etf(),
    }
    return request.app.state.templates.TemplateResponse(
        request, "market/index.html", ctx,
    )

"""분석 리포트 — universe 종목 그리드 + 분석 상세."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

import config
from database import latest_analysis_per_ticker, load_analysis
from web.deps import require_user_or_redirect

router = APIRouter()


def _build_grid() -> dict:
    """한국/미국 두 섹션, 각 섹션은 종목 카드 리스트.
    portfolio 종목엔 ⭐ 마크.
    """
    portfolio = config.get_portfolio_detail()
    universe  = config.get_universe_detail()

    # 합집합 (universe 가 portfolio 포함인 게 보통이지만 안전하게)
    all_tickers = {}
    for src in (universe, portfolio):
        for tk, info in src.items():
            if tk not in all_tickers:
                all_tickers[tk] = dict(info)

    latest = latest_analysis_per_ticker()
    kr, us = [], []
    for tk, info in all_tickers.items():
        rec = latest.get(tk)
        card = {
            "ticker":       tk,
            "name":         info.get("name", tk),
            "exchange":     info.get("exchange", ""),
            "is_portfolio": tk in portfolio,
            "qty":          (portfolio.get(tk) or {}).get("qty", 0),
            "latest_id":    rec["id"]          if rec else None,
            "latest_at":    rec["analyzed_at"] if rec else None,
            "preview":      (rec["preview"]    if rec else "").replace("\n", " ").strip(),
        }
        (us if info.get("is_overseas") else kr).append(card)

    # 정렬: 분석 있는 것 먼저(최신순), 그 다음 무분석 (티커순)
    def _sort_key(c):
        return (0, c["latest_at"] or "") if c["latest_at"] else (1, c["ticker"])
    kr.sort(key=_sort_key, reverse=False)
    kr.sort(key=lambda c: c["latest_at"] or "", reverse=True)
    us.sort(key=lambda c: c["latest_at"] or "", reverse=True)
    # 분석 없는 카드는 뒤로
    kr = sorted(kr, key=lambda c: c["latest_at"] is None)
    us = sorted(us, key=lambda c: c["latest_at"] is None)

    return {"kr": kr, "us": us}


@router.get("/reports")
def reports_list(request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    grid = _build_grid()
    return request.app.state.templates.TemplateResponse(
        request, "reports/list.html",
        {"grid": grid, "user": u},
    )


def _build_sections(rec: dict) -> tuple[list[dict], str | None]:
    """rec.result_text 를 ===MONTHLY===/===WEEKLY===/===DAILY=== 마커로 분할 +
    chart_files 또는 live URL 매핑하여 [월·주·일] 섹션 리스트 반환.
    분할 실패 시 (sections=[], fallback_text=원문) 반환.
    """
    from telegram_bot import _split_analysis

    blocks = _split_analysis(rec["result_text"] or "")
    if not blocks:
        return [], rec.get("result_text", "")

    # chart_files: [{interval, file_path}, ...] → {interval: url}
    chart_url: dict[str, str] = {}
    for c in (rec.get("charts") or []):
        rel = c["file_path"]
        if rel.startswith("data/"):
            chart_url[c["interval"]] = "/" + rel[len("data/"):]
        else:
            chart_url[c["interval"]] = rel

    ticker = rec["ticker"]
    # 저장된 게 없으면 live URL 폴백 (D/W/M 만 — 엘리엇은 저장본 있을 때만)
    def url_for(iv: str) -> str | None:
        if iv in chart_url:
            return chart_url[iv]
        if iv in ("D", "W", "M"):
            return f"/charts/live/{ticker}/{iv}"
        return None

    sections = [
        {
            "key":         "M",
            "icon":        "📅",
            "label":       "월봉 (장기)",
            "text":        blocks.get("MONTHLY", ""),
            "chart_url":   url_for("M"),
            "elliott_url": url_for("E_M"),
        },
        {
            "key":         "W",
            "icon":        "📊",
            "label":       "주봉 (중기)",
            "text":        blocks.get("WEEKLY", ""),
            "chart_url":   url_for("W"),
            "elliott_url": url_for("E_W"),
        },
        {
            "key":         "D",
            "icon":        "📈",
            "label":       "일봉 (단기)",
            "text":        blocks.get("DAILY", ""),
            "chart_url":   url_for("D"),
            "elliott_url": url_for("E"),
        },
    ]
    return sections, None


@router.get("/reports/{analysis_id}")
def reports_detail(analysis_id: int, request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    rec = load_analysis(analysis_id)
    if not rec:
        return request.app.state.templates.TemplateResponse(
            request, "reports/detail.html",
            {"rec": None, "user": u},
            status_code=404,
        )
    sections, fallback_text = _build_sections(rec)
    return request.app.state.templates.TemplateResponse(
        request, "reports/detail.html",
        {
            "rec":           rec,
            "sections":      sections,
            "fallback_text": fallback_text,
            "user":          u,
        },
    )

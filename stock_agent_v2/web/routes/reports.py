"""분석 리포트 — universe 종목 그리드 + 분석 상세."""
from __future__ import annotations

from datetime import date
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

import config
from database import latest_analysis_per_ticker, load_analysis
from web.deps import require_user_or_redirect

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LIVE_CHART_DIR = PROJECT_ROOT / "data" / "charts" / "_live"

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


import re as _re

# 옛 한국어 헤더 패턴 — LLM 출력 형식이 마커 이전 시기인 경우 폴백.
# 라인 시작에서 매칭 (이모지/공백/대괄호 허용).
_KO_HEADER_PATTERNS = {
    "MONTHLY": _re.compile(
        r'^[\W_]*(?:큰그림\s*\(월봉\)|\[MONTHLY\]|장기\s*\(월봉\)|월봉\s*전략|월간\s*리포트)',
        _re.MULTILINE,
    ),
    "WEEKLY": _re.compile(
        r'^[\W_]*(?:중기\s*\(주봉\)|\[WEEKLY\]|주봉\s*전략|주간\s*리포트)',
        _re.MULTILINE,
    ),
    "DAILY": _re.compile(
        r'^[\W_]*(?:단기\s*\(일봉\)|\[SINGLE\]|\[DAILY\]|일봉\s*전략|일간\s*리포트)',
        _re.MULTILINE,
    ),
}


def _split_korean_headers(text: str) -> dict:
    """옛 분석 텍스트의 한국어 헤더 (🔭 큰그림(월봉), [WEEKLY], [SINGLE] 등) 로 분할 시도."""
    matches: list[tuple[int, str, int]] = []  # (start, label, line_end)
    for label, pat in _KO_HEADER_PATTERNS.items():
        m = pat.search(text)
        if m:
            line_end = text.find("\n", m.start())
            if line_end == -1:
                line_end = m.end()
            matches.append((m.start(), label, line_end))
    if len(matches) < 2:
        return {}
    matches.sort()
    blocks: dict[str, str] = {}
    for i, (_pos, label, line_end) in enumerate(matches):
        next_start = matches[i + 1][0] if i + 1 < len(matches) else len(text)
        body = text[line_end:next_start].strip()
        if body:
            blocks[label] = body
    return blocks


def _build_sections(rec: dict) -> tuple[list[dict], str | None]:
    """rec.result_text 를 인터벌별 섹션으로 분할 + 차트 URL 매핑.
    1) 새 마커 (===MONTHLY=== 등) 시도
    2) 실패 시 옛 한국어 헤더 시도
    3) 둘 다 실패 시 (sections=[], fallback_text=원문)
    """
    from telegram_bot import _split_analysis

    raw = rec["result_text"] or ""
    blocks = _split_analysis(raw)
    if not blocks:
        blocks = _split_korean_headers(raw)
    if not blocks:
        return [], raw

    # chart_files: [{interval, file_path}, ...] → {interval: url}
    chart_url: dict[str, str] = {}
    for c in (rec.get("charts") or []):
        rel = c["file_path"]
        if rel.startswith("data/"):
            chart_url[c["interval"]] = "/" + rel[len("data/"):]
        else:
            chart_url[c["interval"]] = rel

    ticker = rec["ticker"]
    # 저장된 게 없으면 live URL 폴백 (D/W/M + D_I/W_I/M_I — 엘리엇은 저장본 있을 때만)
    def url_for(iv: str) -> str | None:
        if iv in chart_url:
            return chart_url[iv]
        if iv in ("D", "W", "M", "D_I", "W_I", "M_I"):
            return f"/charts/live/{ticker}/{iv}"
        return None

    sections = [
        {
            "key":          "M",
            "icon":         "📅",
            "label":        "월봉 (장기)",
            "text":         blocks.get("MONTHLY", ""),
            "ichi_text":    blocks.get("MONTHLY_ICHI", ""),
            "chart_url":    url_for("M"),
            "ichi_url":     url_for("M_I"),
            "elliott_url":  url_for("E_M"),
        },
        {
            "key":          "W",
            "icon":         "📊",
            "label":        "주봉 (중기)",
            "text":         blocks.get("WEEKLY", ""),
            "ichi_text":    blocks.get("WEEKLY_ICHI", ""),
            "chart_url":    url_for("W"),
            "ichi_url":     url_for("W_I"),
            "elliott_url":  url_for("E_W"),
        },
        {
            "key":          "D",
            "icon":         "📈",
            "label":        "일봉 (단기)",
            "text":         blocks.get("DAILY", ""),
            "ichi_text":    blocks.get("DAILY_ICHI", ""),
            "chart_url":    url_for("D"),
            "ichi_url":     url_for("D_I"),
            "elliott_url":  url_for("E"),
        },
    ]
    return sections, None


@router.post("/reports/{analysis_id}/refresh")
def reports_refresh(analysis_id: int, request: Request):
    """수동 갱신 — KIS 에서 해당 종목의 최신 캔들을 받아서 차트 캐시 무효화.
    완료 후 detail 페이지로 redirect."""
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    rec = load_analysis(analysis_id)
    if not rec:
        return RedirectResponse(url="/reports", status_code=303)
    ticker = rec["ticker"]

    # 1) KIS 캔들 update
    try:
        from kis_collector import KISCollector
        kis = KISCollector()
        if kis.login():
            kis.run_daily_update([ticker])
        else:
            print(f"[refresh] {ticker}: KIS 로그인 실패, 캔들 update 스킵")
    except Exception as e:
        print(f"[refresh] {ticker} KIS update 실패: {e}")

    # 2) 디스크 캐시 무효화 — 오늘 폴더의 해당 종목 PNG 삭제 (캔들 + 엘리엇)
    today = date.today().strftime("%Y%m%d")
    today_dir = LIVE_CHART_DIR / today
    removed = 0
    if today_dir.exists():
        for p in today_dir.glob(f"{ticker}_*.png"):
            try:
                p.unlink()
                removed += 1
            except Exception:
                pass
    print(f"[refresh] {ticker}: 캐시 {removed}장 삭제")

    # 3) 메모리 캐시도 무효화 — 새 요청에서 디스크/생성 단계로 가도록
    try:
        from web.app import _wipe_chart_cache_for
        _wipe_chart_cache_for(ticker)
    except Exception:
        pass

    return RedirectResponse(url=f"/reports/{analysis_id}", status_code=303)


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

"""분석 리포트 — universe 종목 그리드 + 분석 상세."""
from __future__ import annotations

from datetime import date
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

import config
from database import latest_analysis_per_ticker, load_analysis, get_latest_candle_date
from web.banner import build_multi_banner
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
    banner = build_multi_banner([
        ("kr_evening",    "한국 저녁 분석"),
        ("morning_brief", "모닝 브리핑"),
    ])
    return request.app.state.templates.TemplateResponse(
        request, "reports/list.html",
        {"grid": grid, "banner": banner, "user": u},
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


def _refresh_make_charts(ticker: str, name: str) -> dict:
    """일/주/월 기술 + 일목 + 엘리엇 차트 (PNG bytes dict).
    main.py 의 _make_chart 와 동일한 로직이지만 main 임포트 시 stdout 가
    파일 로그로 tee 되는 사이드이펙트를 피하려고 인라인."""
    from database import load_candles
    from chart_generator import generate_chart, generate_elliott_chart
    from elliott_wave import compute_elliott_wave
    charts: dict = {}
    df_by_iv: dict = {}
    ichi_key = {"D": "D_I", "W": "W_I", "M": "M_I"}
    for interval, limit in [("D", 400), ("W", 260), ("M", 60)]:
        try:
            df = load_candles(ticker, interval, limit=limit)
            if df.empty:
                continue
            charts[interval] = generate_chart(
                df, ticker, name, config.INDICATOR_CONFIG,
                interval=interval, mode="tech",
            )
            charts[ichi_key[interval]] = generate_chart(
                df, ticker, name, config.INDICATOR_CONFIG,
                interval=interval, mode="ichi",
            )
            df_by_iv[interval] = df
        except Exception as e:
            print(f"[refresh] {ticker} {interval} 차트 실패: {e}")
    elliott_key = {"D": "E", "W": "E_W", "M": "E_M"}
    for interval, df in df_by_iv.items():
        try:
            ec = config.get_elliott_config(interval)
            elliott = compute_elliott_wave(df, ec)
            if not elliott.get("available"):
                continue
            img = generate_elliott_chart(df, ticker, name, elliott, interval=interval)
            if img:
                charts[elliott_key[interval]] = img
        except Exception as e:
            print(f"[refresh] {ticker} 엘리엇 {interval} 실패: {e}")
    return charts


def _refresh_persist_charts(analysis_id: int, ticker: str, charts: dict) -> None:
    """data/charts/YYYYMMDD/ 에 PNG 저장 + chart_files DB 업데이트."""
    from database import save_chart_files
    if not charts:
        return
    today = date.today().strftime("%Y%m%d")
    out_dir = PROJECT_ROOT / "data" / "charts" / today
    out_dir.mkdir(parents=True, exist_ok=True)
    saved: dict[str, str] = {}
    for interval, blob in charts.items():
        if not blob:
            continue
        fpath = out_dir / f"{ticker}_{interval}.png"
        try:
            fpath.write_bytes(blob)
            saved[interval] = str(fpath.relative_to(PROJECT_ROOT)).replace("\\", "/")
        except Exception as e:
            print(f"[refresh] {ticker} {interval} 디스크 저장 실패: {e}")
    if saved:
        save_chart_files(analysis_id, saved)


@router.post("/reports/analyze/{ticker}")
def reports_analyze_new(ticker: str, request: Request):
    """신규 분석 트리거 — universe 종목 처음 분석할 때.
    1) KIS 캔들 update (최신 종가 반영)
    2) AI 분석 → DB 저장
    3) 차트 생성 → 디스크 저장
    4) 새 analysis_id 의 detail 페이지로 redirect (30~60초 소요)
    """
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    import re as _re
    if not _re.match(r'^[A-Za-z0-9.\-]{1,16}$', ticker):
        return RedirectResponse(url="/reports", status_code=303)
    ticker = ticker.upper()
    info = (config.get_portfolio_detail().get(ticker)
         or config.get_universe_detail().get(ticker))
    if not info:
        return RedirectResponse(url="/reports", status_code=303)
    name = info.get("name", ticker)

    # 1) KIS 캔들 update — 현재 종가 반영
    try:
        from kis_collector import KISCollector
        kis = KISCollector()
        if kis.login():
            kis.run_daily_update([ticker])
    except Exception as e:
        print(f"[analyze-new] {ticker} KIS update 실패: {e}")

    # 2) AI 분석 + 차트
    new_id = None
    try:
        from analyzer import StockAnalyzer
        from database import save_analysis
        text = StockAnalyzer().analyze(ticker)
        new_id = save_analysis(ticker, text)
        charts = _refresh_make_charts(ticker, name)
        _refresh_persist_charts(new_id, ticker, charts)
        print(f"[analyze-new] {ticker} 완료 → analysis_id={new_id}, 차트 {len(charts)}장")
    except Exception as e:
        import traceback
        print(f"[analyze-new] {ticker} AI 분석 실패: {e}")
        traceback.print_exc()

    if new_id:
        return RedirectResponse(url=f"/reports/{new_id}", status_code=303)
    return RedirectResponse(url="/reports", status_code=303)


@router.post("/reports/{analysis_id}/refresh")
def reports_refresh(analysis_id: int, request: Request):
    """수동 갱신 — KIS 캔들 update + AI 재분석 + 차트 재생성 + 캐시 무효화.
    완료 후 NEW analysis_id 의 detail 페이지로 redirect (실패 시 기존 id).
    텔레그램은 보내지 않음 (웹 수동 액션이라 별도 알림 불필요)."""
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    rec = load_analysis(analysis_id)
    if not rec:
        return RedirectResponse(url="/reports", status_code=303)
    ticker = rec["ticker"]
    info = (config.get_portfolio_detail().get(ticker)
         or config.get_universe_detail().get(ticker)
         or {})
    name = info.get("name", ticker)

    # 1) KIS 캔들 update — 최신 D/W/M 캔들을 DB 에 받아오기
    try:
        from kis_collector import KISCollector
        kis = KISCollector()
        if kis.login():
            kis.run_daily_update([ticker])
        else:
            print(f"[refresh] {ticker}: KIS 로그인 실패, 캔들 update 스킵")
    except Exception as e:
        print(f"[refresh] {ticker} KIS update 실패: {e}")

    # 2) AI 재분석 (Claude/Gemini ~30-60초) + 차트 재생성
    new_id = analysis_id
    try:
        from analyzer import StockAnalyzer
        from database import save_analysis
        text = StockAnalyzer().analyze(ticker)
        new_id = save_analysis(ticker, text)
        charts = _refresh_make_charts(ticker, name)
        _refresh_persist_charts(new_id, ticker, charts)
        print(f"[refresh] {ticker} 재분석 완료 → new id={new_id}, 차트 {len(charts)}장")
    except Exception as e:
        import traceback
        print(f"[refresh] {ticker} AI 재분석 실패 (KIS update 는 완료): {e}")
        traceback.print_exc()

    # 3) 디스크 차트 캐시 무효화 — 오늘 _live 폴더의 해당 종목 PNG 삭제
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
    print(f"[refresh] {ticker}: _live 캐시 {removed}장 삭제")

    # 4) 메모리 캐시도 무효화
    try:
        from web.app import _wipe_chart_cache_for
        _wipe_chart_cache_for(ticker)
    except Exception:
        pass

    return RedirectResponse(url=f"/reports/{new_id}", status_code=303)


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
    last_candles = {
        iv: get_latest_candle_date(rec["ticker"], iv) for iv in ("D", "W", "M")
    }
    banner = build_multi_banner([
        ("kr_evening",    "한국 저녁 분석"),
        ("morning_brief", "모닝 브리핑"),
    ])
    return request.app.state.templates.TemplateResponse(
        request, "reports/detail.html",
        {
            "rec":           rec,
            "sections":      sections,
            "fallback_text": fallback_text,
            "last_candles":  last_candles,
            "banner":        banner,
            "user":          u,
        },
    )

"""FastAPI 앱 팩토리.

마운트:
- /static            : web/static (CSS, JS)
- /charts            : data/charts (분석 차트 PNG, 로그인 가드)
- /login, /logout    : 인증
- /reports, /signals, /warnings, /filings : 컨텐츠
"""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from web.auth import COOKIE_NAME, authenticate, make_session_cookie, parse_session_cookie
from web.deps import require_user_or_redirect
from database import load_user

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"
CHARTS_DIR = PROJECT_ROOT / "data" / "charts"


def create_app() -> FastAPI:
    app = FastAPI(title="Stock Agent (private share)")

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    # 정적 자원 캐시 버스팅 — {{ static('css/style.css') }} 가 mtime 쿼리 부착
    def _static(rel: str) -> str:
        path = STATIC_DIR / rel
        try:
            v = int(path.stat().st_mtime)
        except OSError:
            v = 0
        return f"/static/{rel}?v={v}"
    templates.env.globals["static"] = _static

    app.state.templates = templates

    # 정적 자원
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # 모든 페이지에 username 노출 (로그인 페이지 제외)
    @app.middleware("http")
    async def attach_user(request: Request, call_next):
        username = None
        cookie = request.cookies.get(COOKIE_NAME)
        if cookie:
            username = parse_session_cookie(cookie)
        request.state.username = username
        return await call_next(request)

    # ── 로그인 / 로그아웃 ───────────────────────────────────────
    @app.get("/login")
    def login_form(request: Request, next: str = "/"):
        return templates.TemplateResponse(
            request, "login.html",
            {"next": next, "error": None, "username": ""},
        )

    @app.post("/login")
    def login_submit(
        request: Request,
        username: str = Form(...),
        password: str = Form(...),
        next: str = Form("/"),
    ):
        user = authenticate(username, password)
        if not user:
            return templates.TemplateResponse(
                request, "login.html",
                {
                    "next":     next,
                    "error":    "아이디 또는 비밀번호가 잘못되었습니다.",
                    "username": username,
                },
                status_code=401,
            )
        # next 경로 검증 — open redirect 방지
        if not next.startswith("/") or next.startswith("//"):
            next = "/"
        resp = RedirectResponse(url=next, status_code=303)
        resp.set_cookie(
            COOKIE_NAME,
            make_session_cookie(user["username"]),
            httponly=True,
            samesite="lax",
            max_age=60 * 60 * 24 * 30,   # 30일
        )
        return resp

    @app.post("/logout")
    def logout():
        resp = RedirectResponse(url="/login", status_code=303)
        resp.delete_cookie(COOKIE_NAME)
        return resp

    # ── 차트 PNG 서빙 (로그인 가드) ─────────────────────────────
    @app.get("/charts/{date}/{filename}")
    def serve_chart(date: str, filename: str, request: Request):
        u = require_user_or_redirect(request)
        if isinstance(u, RedirectResponse):
            return u
        # 경로 traversal 방지
        if "/" in date or "\\" in date or ".." in date:
            raise HTTPException(400, "bad date")
        if "/" in filename or "\\" in filename or ".." in filename:
            raise HTTPException(400, "bad filename")
        if not filename.endswith(".png"):
            raise HTTPException(400, "png only")
        fpath = CHARTS_DIR / date / filename
        if not fpath.exists():
            raise HTTPException(404, "chart not found")
        return FileResponse(str(fpath), media_type="image/png")

    # ── On-demand 차트 (저장된 PNG 없는 분석에도 차트 보이게) ───
    # 2단 캐시: 메모리 (가장 빠름) → 디스크 (서버 재시작 후에도 유지) → 생성
    _chart_cache: dict[tuple[str, str, str], bytes] = {}
    _elliott_cache: dict[tuple[str, str, str], bytes] = {}
    LIVE_CHART_DIR = PROJECT_ROOT / "data" / "charts" / "_live"
    from fastapi import Response

    def _live_chart_path(ticker: str, interval: str, today: str):
        return LIVE_CHART_DIR / today / f"{ticker}_{interval}.png"

    def _live_chart(ticker: str, interval: str, today: str) -> bytes | None:
        key = (ticker, interval, today)
        if key in _chart_cache:
            return _chart_cache[key]

        # 디스크 캐시 (오늘 같은 차트가 이미 만들어졌으면 그대로 사용)
        cache_path = _live_chart_path(ticker, interval, today)
        if cache_path.exists():
            try:
                data = cache_path.read_bytes()
                if data:
                    _chart_cache[key] = data
                    return data
            except Exception as e:
                print(f"[charts/live] {ticker} {interval} 캐시 읽기 실패: {e}")

        import config as _cfg
        from database import load_candles
        from chart_generator import generate_chart
        import traceback

        limit = {"D": 400, "W": 260, "M": 60}.get(interval, 200)
        df = load_candles(ticker, interval, limit=limit)
        if df.empty:
            print(f"[charts/live] {ticker} {interval}: candles 없음")
            return None
        info = (_cfg.get_portfolio_detail().get(ticker)
             or _cfg.get_universe_detail().get(ticker)
             or {})
        name = info.get("name", ticker)
        try:
            png = generate_chart(df, ticker, name, _cfg.INDICATOR_CONFIG, interval=interval)
            if png:
                _chart_cache[key] = png
                # 디스크에도 저장 — cleanup_old_charts 가 일자 기준 정리
                try:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.write_bytes(png)
                except Exception as e:
                    print(f"[charts/live] {ticker} {interval} 캐시 쓰기 실패: {e}")
                # 메모리 폭주 방지
                if len(_chart_cache) > 256:
                    for k in list(_chart_cache.keys())[:128]:
                        del _chart_cache[k]
            return png
        except Exception as e:
            print(f"[charts/live] {ticker} {interval} 생성 실패: {type(e).__name__}: {e}")
            traceback.print_exc()
            return None

    def _validate_ticker(ticker: str):
        if not ticker.replace("-", "").replace(".", "").isalnum() or len(ticker) > 16:
            raise HTTPException(400, "bad ticker")

    def _today_key() -> str:
        """디스크 캐시 폴더명. YYYYMMDD (cleanup_old_charts / prebuild_charts 와 동일 포맷)."""
        from datetime import date as _date
        return _date.today().strftime("%Y%m%d")

    @app.get("/charts/live/{ticker}/{interval}")
    def chart_live(ticker: str, interval: str, request: Request):
        u = require_user_or_redirect(request)
        if isinstance(u, RedirectResponse):
            return u
        if interval not in ("D", "W", "M"):
            raise HTTPException(400, "bad interval")
        _validate_ticker(ticker)
        png = _live_chart(ticker.upper(), interval, _today_key())
        if png is None:
            raise HTTPException(404, "no candle data")
        # max-age 짧게 — 데이터 변경 시 빠른 갱신
        return Response(
            content=png, media_type="image/png",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )

    # ── 메모리 캐시 무효화 (수동 갱신 라우트가 호출) ────────────────
    def _wipe_chart_cache_for(ticker: str):
        ticker = ticker.upper()
        for k in [k for k in _chart_cache if k[0] == ticker]:
            del _chart_cache[k]
        for k in [k for k in _elliott_cache if k[0] == ticker]:
            del _elliott_cache[k]
    app.state.wipe_chart_cache_for = _wipe_chart_cache_for
    # routes 모듈에서 import 가능하도록 모듈 전역에도 노출
    globals()["_wipe_chart_cache_for"] = _wipe_chart_cache_for

    # ── 엘리엇 파동 차트 (검출되면 응답, 안 되면 404) ─────────────

    def _live_elliott(ticker: str, interval: str, today: str) -> bytes | None:
        key = (ticker, interval, today)
        if key in _elliott_cache:
            return _elliott_cache[key]

        cache_path = LIVE_CHART_DIR / today / f"{ticker}_E_{interval}.png"
        if cache_path.exists():
            try:
                data = cache_path.read_bytes()
                if data:
                    _elliott_cache[key] = data
                    return data
            except Exception:
                pass

        import config as _cfg
        from database import load_candles
        from chart_generator import generate_elliott_chart
        from elliott_wave import compute_elliott_wave

        limit = {"D": 400, "W": 260, "M": 60}.get(interval, 200)
        df = load_candles(ticker, interval, limit=limit)
        if df.empty:
            return None
        info = (_cfg.get_portfolio_detail().get(ticker)
             or _cfg.get_universe_detail().get(ticker)
             or {})
        name = info.get("name", ticker)
        try:
            ec = _cfg.get_elliott_config(interval)
            elliott = compute_elliott_wave(df, ec)
            if not elliott.get("available"):
                return None
            png = generate_elliott_chart(df, ticker, name, elliott, interval=interval)
            if png:
                _elliott_cache[key] = png
                try:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.write_bytes(png)
                except Exception:
                    pass
                if len(_elliott_cache) > 256:
                    for k in list(_elliott_cache.keys())[:128]:
                        del _elliott_cache[k]
            return png
        except Exception as e:
            print(f"[charts/live] elliott {ticker} {interval}: {type(e).__name__}: {e}")
            return None

    @app.get("/charts/live/elliott/{ticker}/{interval}")
    def chart_live_elliott(ticker: str, interval: str, request: Request):
        u = require_user_or_redirect(request)
        if isinstance(u, RedirectResponse):
            return u
        if interval not in ("D", "W", "M"):
            raise HTTPException(400, "bad interval")
        _validate_ticker(ticker)
        png = _live_elliott(ticker.upper(), interval, _today_key())
        if png is None:
            raise HTTPException(404, "no elliott")
        return Response(
            content=png, media_type="image/png",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )

    # ── 컨텐츠 라우트 ────────────────────────────────────────────
    from web.routes import reports, signals, warnings, filings, universe
    app.include_router(reports.router)
    app.include_router(signals.router)
    app.include_router(warnings.router)
    app.include_router(filings.router)
    app.include_router(universe.router)

    @app.get("/")
    def root(request: Request):
        u = require_user_or_redirect(request)
        if isinstance(u, RedirectResponse):
            return u
        return RedirectResponse(url="/reports", status_code=303)

    return app

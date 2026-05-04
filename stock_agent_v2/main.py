"""
main.py - KIS API 버전 실행 진입점

데이터 흐름:
  - 수집 대상 : universe.csv (관찰 종목 전체)
  - 분석 대상 : portfolio.csv (매일 리포트 생성 종목)
  - 시그널   : universe 전체 스캔, Claude 호출 없이 규칙 기반 발동 종목만 요약 전송
  - portfolio ⊆ universe (portfolio-only 종목은 자동 편입)

사용법:
  python main.py --init                    # 최초 1회: DB생성 + 5년치 캔들(universe) + 공시
  python main.py --update                  # 증분 업데이트 (universe 전체, 느림)
  python main.py --analyze                 # portfolio 업데이트 + 분석 + 채널 전송 (빠름)
  python main.py --analyze --ticker 005930 # 단일 종목 업데이트 + 분석
  python main.py --analyze --ticker TSLA --exchange NASDAQ   # 미등록 해외 종목
  python main.py --analyze --ticker 9988 --exchange HKEX     # 홍콩 등 특정 거래소
  python main.py --dart-only               # portfolio 공시 확인만
  python main.py --weekly                  # portfolio 주간 리포트
  python main.py --signals                 # universe 업데이트 + 시그널 스캔
  python main.py --signals --ticker portfolio  # portfolio 업데이트 + 시그널 스캔
  python main.py --signals --ticker 005930 # 단일 종목 업데이트 + 시그널 스캔

AI 모델 선택 (전역 기본값은 .env 의 AI_PRIMARY, 런타임 오버라이드는 --model):
  python main.py --analyze --model claude  # Claude 우선(실패 시 Gemini 폴백)
  python main.py --analyze --model gemini  # Gemini 우선(실패 시 Claude 폴백)
  python main.py                           # 스케줄 모드 (주중 08:00 → portfolio 업데이트 + 분석)
"""

import os
import sys
import time
import schedule
from datetime import datetime, timedelta
from pathlib import Path


# ── 파일 로그 (콘솔 + 파일 tee) ────────────────────────────────────
# 반드시 config 등 다른 모듈 import 전에 호출해야 그 모듈들의 print 도 캡처된다.
class _TeeStream:
    """stdout/stderr 를 콘솔과 파일에 동시 write. 인코딩 실패 시 다른 스트림은 영향 없음."""
    def __init__(self, *streams):
        self._streams = streams
    def write(self, data):
        for s in self._streams:
            try:
                s.write(data)
            except UnicodeEncodeError:
                enc = getattr(s, "encoding", None) or "utf-8"
                s.write(data.encode(enc, errors="replace").decode(enc, errors="replace"))
            except Exception:
                pass
        self.flush()
    def flush(self):
        for s in self._streams:
            try:
                s.flush()
            except Exception:
                pass
    def isatty(self):
        return any(getattr(s, "isatty", lambda: False)() for s in self._streams)


def _setup_file_log() -> Path:
    """logs/main_YYYYMMDD_HHMMSS.log 로 stdout/stderr 를 tee."""
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"main_{ts}.log"
    # 파일은 utf-8, 라인 버퍼(1) 로 열어 실시간 저장
    log_file = open(log_path, "a", encoding="utf-8", errors="replace", buffering=1)
    # 콘솔 인코딩 오류 방지 (Windows cp949 에서 이모지·한자 등 깨짐 방지)
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    sys.stdout = _TeeStream(sys.__stdout__, log_file)
    sys.stderr = _TeeStream(sys.__stderr__, log_file)
    print(f"[LOG] 로그 파일: {log_path}")
    return log_path


_LOG_PATH = _setup_file_log()


import config
from database         import init_db, save_analysis, mark_sent, load_candles
from kis_collector    import KISCollector          # KIS REST API
from dart_collector   import DartCollector
from sec_collector    import SECCollector
from analyzer         import StockAnalyzer
from telegram_bot     import TelegramNotifier
from chart_generator  import generate_chart, generate_elliott_chart
from elliott_wave     import compute_elliott_wave


def _get_arg(args, key, default=None):
    """--key value 형태 인자 추출"""
    if key in args:
        idx = args.index(key)
        if idx + 1 < len(args):
            return args[idx + 1]
    return default


# ── 커맨드 함수 ───────────────────────────────────────────────────────

def cmd_init():
    """최초 1회 전체 초기화 (universe 전체에 대해 5년치 캔들 + 공시 적재)"""
    print("=" * 55)
    print("  KIS 초기화 시작 (5년치 캔들 + DART 전체 공시)")
    print("=" * 55)
    init_db()

    kis = KISCollector()
    if not kis.login():
        print("[ERROR] KIS 토큰 발급 실패 — App Key/Secret 확인")
        return

    kis.run_initial_load(config.UNIVERSE, years=5)

    dart = DartCollector()
    print("\n[DART] 초기 공시 수집 시작...")
    dart.fetch_all_tickers(config.UNIVERSE, days_back=365 * 5)

    print("\n[DART] 재무보고서 수집 시작...")
    for ticker in config.UNIVERSE:
        name = config.UNIVERSE_DETAIL.get(ticker, (ticker,))[0]
        print(f"  [{ticker}] {name} 재무 수집 중...")
        dart.fetch_financial_report(ticker)
        time.sleep(0.5)

    # SEC EDGAR 초기 적재 (해외 종목)
    sec = SECCollector()
    sec.fetch_initial(config.UNIVERSE, days_back=365)

    print("\n[OK] 초기화 완료. python main.py --analyze 로 분석 시작하세요.")


def cmd_update(tickers=None):
    """증분 업데이트: 전 영업일 캔들 + T-1 공시. 기본 타겟 = universe."""
    targets = tickers or config.UNIVERSE

    kis = KISCollector()
    if not kis.login():
        print("[ERROR] KIS 토큰 발급 실패")
        return
    kis.run_daily_update(targets)

    dart = DartCollector()
    dart.fetch_all_tickers(targets, days_back=3)   # T-1: 국내

    sec = SECCollector()
    sec.fetch_all_tickers(targets, days_back=3)    # T-1: 해외 (SEC EDGAR)


def _make_chart(ticker: str, name: str) -> dict:
    """일/주/월 차트 생성 + 인터벌별 엘리엇 검출 시 E/E_W/E_M 차트 추가.
    반환: {"D": bytes, "W": bytes, "M": bytes,
            ["E": bytes, "E_W": bytes, "E_M": bytes]}"""
    charts = {}
    df_by_interval: dict = {}
    for interval, limit in [("D", 400), ("W", 260), ("M", 60)]:
        try:
            df = load_candles(ticker, interval, limit=limit)
            if not df.empty:
                charts[interval] = generate_chart(
                    df, ticker, name, config.INDICATOR_CONFIG, interval=interval
                )
                df_by_interval[interval] = df
        except Exception as e:
            lbl = {"D": "일봉", "W": "주봉", "M": "월봉"}[interval]
            print(f"  [WARN] {ticker} {lbl} 차트 생성 실패: {e}")

    # 엘리엇 차트는 인터벌별로 각각 검출 시도. 인터벌별 임계값은 ELLIOTT_CONFIG_OVERRIDES.
    # 결과 키: D → "E", W → "E_W", M → "E_M".
    elliott_key = {"D": "E", "W": "E_W", "M": "E_M"}
    for interval, df in df_by_interval.items():
        try:
            ec = config.get_elliott_config(interval)
            elliott = compute_elliott_wave(df, ec)
            if not elliott.get("available"):
                continue
            img = generate_elliott_chart(df, ticker, name, elliott, interval=interval)
            if img:
                charts[elliott_key[interval]] = img
        except Exception as e:
            lbl = {"D": "일봉", "W": "주봉", "M": "월봉"}[interval]
            print(f"  [WARN] {ticker} 엘리엇 {lbl} 차트 생성 실패: {e}")

    return charts


def cmd_analyze(tickers=None, header="", primary=None, header_photo=None):
    """
    분석 실행 + 텔레그램 채널 전송
    primary: 'claude' | 'gemini' | None(config.AI_PRIMARY 따름)
    header_photo: 헤더 텍스트 직후 발송할 추가 이미지(예: 수급 차트). None 이면 생략.

    에러 처리 방침:
    - 정상 종목: 분석 결과 전송
    - 에러 종목: [WARN] 에러 알림 포함해서 전송 (누락 없이 파악 가능)
    - 전체 에러: 헤더에 실패 현황 표시
    """
    targets  = tickers or config.PORTFOLIO
    notifier = TelegramNotifier(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID)
    analyzer = StockAnalyzer(primary=primary)

    ok_results  = []   # 정상 분석 결과
    err_results = []   # 에러 결과

    for ticker in targets:
        info = (config.get_portfolio_detail().get(ticker)
             or config.get_universe_detail().get(ticker)
             or {})
        name = info.get("name", ticker)
        print(f"  ▶ [{ticker}] {name} 분석 중...")
        try:
            text = analyzer.analyze(ticker)
            save_analysis(ticker, text)
            charts = _make_chart(ticker, name)
            ok_results.append({"ticker": ticker, "analysis": text, "ok": True, "charts": charts})
            print(f"  [OK] {ticker} {name}")
        except Exception as e:
            import traceback
            err_detail = traceback.format_exc().strip().split("\n")[-1]
            print(f"  [ERROR] {ticker}: {e}")
            err_results.append({
                "ticker":   ticker,
                "name":     name,
                "error":    str(e),
                "detail":   err_detail,
            })

    total   = len(targets)
    ok_cnt  = len(ok_results)
    err_cnt = len(err_results)

    # 헤더 구성 (에러 현황 포함)
    if err_cnt == 0:
        full_header = header
    else:
        err_names = ", ".join(r["name"] for r in err_results)
        full_header = (
            f"{header}\n" if header else ""
        ) + f"[WARN] {ok_cnt}/{total}종목 정상 | 분석 실패: {err_names}"

    # 정상 결과 전송
    all_results = ok_results[:]

    # 에러 종목도 알림으로 포함
    for r in err_results:
        err_msg = (
            f"[WARN] [{r['ticker']}] {r['name']} 분석 실패\n"
            f"오류: {r['error']}\n"
            f"➡️ 다음 실행 시 자동 재시도됩니다."
        )
        all_results.append({"ticker": r["ticker"], "analysis": err_msg})

    if all_results:
        notifier.send_batch(all_results, header=full_header, header_photo=header_photo)

    for r in ok_results:
        mark_sent(r["ticker"])

    print(f"[OK] 전송 완료: 정상 {ok_cnt}종목 | 에러 {err_cnt}종목")


def cmd_dart_only(tickers=None):
    """T-1 특별 공시만 확인 후 채널 전송"""
    targets  = tickers or config.PORTFOLIO
    dart     = DartCollector()
    notifier = TelegramNotifier(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID)

    lines = [f"[DART] T-1 특별 공시 확인\n🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}"]
    for ticker in targets:
        dart.fetch_special_disclosures(ticker, days_back=3)
        summary = dart.get_disclosure_summary(ticker, limit=5)
        name = config.PORTFOLIO_DETAIL.get(ticker, (ticker,))[0]
        lines.append(f"─── {name}({ticker}) ───\n{summary}")

    notifier.send("\n\n".join(lines))
    print("[OK] 공시 확인 완료")


def cmd_signals(tickers=None):
    """
    시그널 스캔 → 발동 시그널 요약 텔레그램 전송 (Claude 호출 없음)
    스캔 전에 대상 종목을 증분 업데이트하여 신선도 보장.

    tickers=None               : universe 전체
    tickers=["portfolio"]      : portfolio 종목만
    tickers=["universe"]       : universe 전체 (명시)
    tickers=["005930"]         : 지정 종목만
    """
    from signals import evaluate_signals, format_report

    # ── 타겟 결정 ─────────────────────────────────────────────────────
    if tickers is None:
        targets = list(config.UNIVERSE)
        mode_label = f"universe {len(targets)}종목"
        header = ""
    else:
        first = tickers[0].strip().lower() if tickers else ""
        if len(tickers) == 1 and first == "portfolio":
            targets = list(config.PORTFOLIO)
            mode_label = f"portfolio {len(targets)}종목"
            header = f"[SIGNAL] portfolio 시그널 스캔"
        elif len(tickers) == 1 and first == "universe":
            targets = list(config.UNIVERSE)
            mode_label = f"universe {len(targets)}종목"
            header = ""
        else:
            targets = [t.strip().upper() for t in tickers]
            mode_label = ",".join(targets)
            header = f"[SIGNAL] {mode_label} 시그널 스캔"

    notifier = TelegramNotifier(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID)

    if not targets:
        notifier.send("[SIGNAL] 대상 종목 없음")
        return

    # ── 스캔 전 증분 업데이트 (신선도 보장) ───────────────────────────
    print(f"[SIGNAL] {mode_label} 업데이트 + 스캔 시작")
    try:
        cmd_update(targets)
    except Exception as e:
        print(f"  [WARN] 업데이트 실패 (기존 DB로 진행): {e}")

    all_sigs = []
    for ticker in targets:
        info = config.UNIVERSE_DETAIL.get(ticker) or config.PORTFOLIO_DETAIL.get(ticker) or (ticker,)
        name = info[0] if isinstance(info, tuple) else ticker
        try:
            sigs = evaluate_signals(ticker, name)
            if sigs:
                print(f"  ▶ {ticker} {name}: {len(sigs)}건")
            all_sigs.extend(sigs)
        except Exception as e:
            print(f"  [WARN] {ticker}: {e}")

    text = format_report(all_sigs, len(targets), header=header)
    notifier.send(text)
    print(f"[OK] 시그널 전송 완료 (발동 {len(all_sigs)}건)")


def cmd_weekly_report(primary=None):
    """주봉·월봉 중심 주간 리포트 + 파일 저장 + 채널 전송"""
    notifier = TelegramNotifier(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID)
    analyzer = StockAnalyzer(primary=primary)
    ts       = datetime.now().strftime("%Y%m%d")
    lines    = [f"[WEEKLY] 주간 전략 리포트 [{datetime.now().strftime('%Y-%m-%d')} 기준]"]
    results  = []

    for ticker in config.PORTFOLIO:
        name = config.PORTFOLIO_DETAIL.get(ticker, (ticker,))[0]
        print(f"  ▶ [{ticker}] {name} 주간 분석 중...")
        try:
            text = analyzer.analyze(ticker)
            results.append({"ticker": ticker, "analysis": text})
            lines.append(f"{'─'*30}\n{text}")
        except Exception as e:
            lines.append(f"[ERROR] {ticker} {name}: {e}")

    # 파일 저장
    report_dir = os.path.join(os.path.dirname(__file__), "..", "output", "reports")
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"{ts}_주간전략.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n\n".join(lines))
    print(f"  [저장] {report_path}")

    notifier.send_batch(results, header="[WEEKLY] 주간 전략 리포트")
    print("[OK] 주간 리포트 완료")


# ── 스케줄 래퍼 ───────────────────────────────────────────────────────

def _split_portfolio() -> tuple[list, list]:
    """포트폴리오를 (해외, 국내) 티커 리스트로 분리."""
    detail = config.get_portfolio_detail()
    us = [t for t, v in detail.items() if v.get("is_overseas")]
    kr = [t for t, v in detail.items() if not v.get("is_overseas")]
    return us, kr


def _notify_batch_error(label: str, exc: Exception) -> None:
    import traceback
    tb = traceback.format_exc()
    print(f"\n[ERROR] {label} 실패: {exc}\n{tb}")
    try:
        TelegramNotifier(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID).send_error(
            f"{label} 실패\n{datetime.now():%Y-%m-%d %H:%M}\n"
            f"오류: {exc}\n로그: {_LOG_PATH.name}"
        )
    except Exception as te:
        print(f"[ERROR] Telegram 알림도 실패: {te}")


# 공시 요약 창: 당일 + T-3 캘린더일치 (주말·공휴일 판단 회피)
_BRIEF_LOOKBACK_DAYS = 3
_BRIEF_SUMMARY_LIMIT = 50  # 창 안이면 사실상 전부 포함


def _format_summary_lines(raw: str) -> list[str]:
    """공시 요약 텍스트를 개별 공시 라인으로 분해. '없음' 안내는 빈 리스트."""
    if not raw or "없음" in raw:
        return []
    return [ln for ln in raw.splitlines() if ln.strip()]


def _fetch_news_for_block(name: str, items: list[str], max_per_disclosure: int = 2) -> list[dict]:
    """중요도 🔴/🟠 이거나 주요 키워드(실적·배당·자사주 등)인 공시에 한해
    네이버 뉴스를 검색해 돌려준다. 종목당 중복 제거해 평탄화."""
    import naver_news
    seen_links: set = set()
    collected: list[dict] = []
    for line in items:
        if not naver_news.should_fetch_news(line):
            continue
        # line 예: "🟡 2026-04-23  연결재무제표기준영업(잠정)실적(공정공시)"
        # 제목부: 날짜 뒤 부분
        parts = line.split(None, 2)
        title = parts[-1] if len(parts) >= 3 else line
        q = naver_news.query_for(name, title)
        hits = naver_news.search(q, display=max_per_disclosure, sort="date")
        for h in hits:
            if h["link"] in seen_links:
                continue
            seen_links.add(h["link"])
            collected.append(h)
            if len(collected) >= 4:   # 종목당 상한 (텔레그램 4096자 대비)
                return collected
    return collected


# SEC 양식별 영문 뉴스 검색어 보조어. ticker 와 함께 결합해 검색.
_SEC_FORM_QUERY_TERMS = {
    "10-K":    "annual report",
    "10-Q":    "earnings results",
    "8-K":     "",                 # 일반 사건 — ticker 만으로 최근 뉴스 잡힘
    "SC 13D":  "stake",
    "SC 13G":  "stake",
    "DEF 14A": "shareholder",
    "S-1":     "offering",
}


def _should_fetch_us_news(line: str) -> bool:
    """SEC 공시 라인이 영문 뉴스 보강 대상인지 판단.
    - 🔴/🟠 중요도 → 항상 포함
    - 핵심 양식 (8-K / 10-Q / 10-K / S-1) → 포함
    """
    if not line:
        return False
    if "🔴" in line or "🟠" in line:
        return True
    return any(form in line for form in ("8-K", "10-Q", "10-K", "S-1", "SC 13"))


def _us_news_query(ticker: str, line: str) -> str:
    """SEC 공시 라인 → ticker + 검색 보조어.
    우선순위: [item_label] (8-K) > 양식별 보조어 > "news" 폴백.
    ticker 단독으로 검색하면 Yahoo Finance/Seeking Alpha 등 시세 페이지만 잡혀
    의미 있는 뉴스가 안 나옴 — 보조어를 강제로 한 단어 이상 붙임.
    """
    import re
    m = re.search(r"\[([^\]]+)\]", line)
    if m:
        return f"{ticker} {m.group(1).lower().strip()}"
    for form, term in _SEC_FORM_QUERY_TERMS.items():
        if form in line and term:
            return f"{ticker} {term}"
    return f"{ticker} news"


def _fetch_news_for_us_block(ticker: str, items: list[str]) -> list[dict]:
    """미국 SEC 공시 보강용 영문 뉴스. 첫 should-fetch 라인의 양식으로 검색.
    market_warning 의 hybrid (Serper.dev → Google News RSS) 를 재사용.
    """
    if not any(_should_fetch_us_news(ln) for ln in items):
        return []
    # 첫 매치 라인만 검색 — 종목당 한 카테고리로 충분
    query = ticker
    for ln in items:
        if _should_fetch_us_news(ln):
            query = _us_news_query(ticker, ln)
            break
    from market_warning import fetch_news_snippets
    hits = fetch_news_snippets(query, limit=3, days=2)
    # 모닝 브리핑 렌더링은 'title' + 'link' 키만 사용 — 호환되게 매핑
    return [
        {"title": h.get("title", ""), "link": h.get("link", "")}
        for h in hits if h.get("title") and h.get("link")
    ][:2]


def run_morning_brief():
    """월~금 07:30 — 미국 포트폴리오 가격·SEC 공시 업데이트 + 한국 DART
    공시 수집 → 당일+T-3 캘린더일치 공시를 규칙 기반 목록 + LLM 한줄 요약
    으로 전송.

    미국 포트폴리오가 비어 있으면 해당 섹션은 조용히 스킵.
    US·KR 둘 다 비면 전송도 스킵.
    """
    print(f"\n[{datetime.now():%Y-%m-%d %H:%M}] [BRIEF] 모닝 브리핑 시작")
    try:
        us_tickers, kr_tickers = _split_portfolio()
        sections: list[list[str]] = []
        blocks_for_llm: list[dict] = []

        cutoff      = datetime.today() - timedelta(days=_BRIEF_LOOKBACK_DAYS)
        sec_since   = cutoff.strftime("%Y-%m-%d")  # SEC filed_date 포맷
        dart_since  = cutoff.strftime("%Y%m%d")    # DART rcept_dt 포맷

        # ── 미국 섹션 ─────────────────────────────────────────────────
        if us_tickers:
            kis = KISCollector()
            if kis.login():
                kis.run_daily_update(us_tickers)
            sec = SECCollector()
            sec.fetch_all_tickers(us_tickers, days_back=_BRIEF_LOOKBACK_DAYS)

            us_lines = [f"🇺🇸 미국 포트폴리오 (최근 {_BRIEF_LOOKBACK_DAYS}일)"]
            for t in us_tickers:
                name = (config.get_portfolio_detail().get(t) or {}).get("name", t)
                summary = sec.get_filing_summary(
                    t, limit=_BRIEF_SUMMARY_LIMIT, since_date=sec_since
                )
                us_lines.append(f"─── {name}({t}) ───\n{summary}")
                items = _format_summary_lines(summary)
                # SEC 공시 → ticker + 양식 키워드로 영문 뉴스 매칭
                # (Serper.dev hybrid 사용, Google News RSS 폴백)
                news = _fetch_news_for_us_block(t, items) if items else []
                blocks_for_llm.append({
                    "ticker": t, "name": name, "market": "US",
                    "items": items, "news": news,
                })
            sections.append(us_lines)

        # ── 한국 DART 섹션 ────────────────────────────────────────────
        if kr_tickers:
            dart = DartCollector()
            dart.fetch_all_tickers(kr_tickers, days_back=_BRIEF_LOOKBACK_DAYS)

            kr_lines = [f"🇰🇷 한국 DART 공시 (최근 {_BRIEF_LOOKBACK_DAYS}일)"]
            for t in kr_tickers:
                name = (config.get_portfolio_detail().get(t) or {}).get("name", t)
                summary = dart.get_disclosure_summary(
                    t, limit=_BRIEF_SUMMARY_LIMIT, since_date=dart_since
                )
                kr_lines.append(f"─── {name}({t}) ───\n{summary}")
                items = _format_summary_lines(summary)
                news = _fetch_news_for_block(name, items) if items else []
                blocks_for_llm.append({
                    "ticker": t, "name": name, "market": "KR",
                    "items": items, "news": news,
                })
            sections.append(kr_lines)

        if not sections:
            print("[BRIEF] 대상 포트폴리오 없음 — 스킵")
            return

        # ── LLM 요약 (공시 있는 종목에 한해 종목당 한 줄) ─────────────
        ticker_summary: dict[str, str] = {}
        if any(b["items"] for b in blocks_for_llm):
            ticker_summary = StockAnalyzer().summarize_disclosures(blocks_for_llm)

        # ── 섹션 렌더링: LLM 요약 + 뉴스 링크를 각 종목 블록 아래 삽입 ─
        import re
        news_by_ticker = {b["ticker"]: b.get("news") or [] for b in blocks_for_llm}
        rendered_sections: list[str] = []
        for lines in sections:
            out = [lines[0]]  # 섹션 헤더 ("🇺🇸 ..." / "🇰🇷 ...")
            for block in lines[1:]:
                out.append(block)
                # block = "─── NAME(TICKER) ───\n<요약>"
                head = block.split("\n", 1)[0]
                m = re.search(r"\(([^()]+)\)", head)
                ticker = m.group(1) if m else ""
                if ticker in ticker_summary:
                    out.append(f"💡 {ticker_summary[ticker]}")
                for n in news_by_ticker.get(ticker, [])[:2]:
                    t = n.get("title", "").strip()
                    l = n.get("link", "")
                    if t and l:
                        out.append(f"📰 {t}\n   {l}")
            rendered_sections.append("\n".join(out))

        header = (f"[BRIEF] 모닝 브리핑 | "
                  f"{datetime.now().strftime('%m/%d %H:%M')}")
        TelegramNotifier(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID).send(
            "\n\n".join([header] + rendered_sections)
        )
        print("[OK] 모닝 브리핑 완료")

        # ── 미국 AI 분석 (장 마감 후 전일 종가 기준, 포트폴리오 US 전종목) ──
        if us_tickers:
            us_header = (f"[REPORT] 미국 AI 전략 | "
                         f"{datetime.now():%m/%d} {config.MORNING_BRIEF_TIME}")
            cmd_analyze(tickers=us_tickers, header=us_header)
    except Exception as e:
        _notify_batch_error("모닝 브리핑", e)


def _fmt_amt_mkrw(v_mil_krw: int) -> str:
    """백만원 단위 순매수 정수 → '+123억' / '-4.5조' 처럼 읽기 편하게."""
    if v_mil_krw == 0:
        return "±0"
    sign = "+" if v_mil_krw > 0 else "-"
    a    = abs(v_mil_krw)
    # 백만원 → 억(1억=100백만) / 조(1조=1,000,000백만)
    if a >= 1_000_000:
        return f"{sign}{a/1_000_000:.1f}조"
    if a >= 100:
        return f"{sign}{a/100:.0f}억"
    return f"{sign}{a}백만"


def _build_supply_chart(kr_tickers: list):
    """portfolio 국내 종목별 1M 외국인·기관 누적 순매수 차트 (PNG bytes).
    DB 데이터 없는 종목은 스킵. 데이터가 하나도 없으면 None.
    """
    from database import load_investor_trend
    from chart_generator import generate_supply_chart

    rows_data = []
    asof = ""
    for t in kr_tickers:
        rows = load_investor_trend(t, days=20)
        if not rows:
            continue
        if not asof:
            asof = rows[0]["trade_date"]
        f_sum = sum((r.get("foreign_amt") or 0) for r in rows[:20])
        i_sum = sum((r.get("inst_amt")    or 0) for r in rows[:20])
        info = (config.get_portfolio_detail().get(t)
             or config.get_universe_detail().get(t)
             or {})
        name = info.get("name", t)
        rows_data.append((t, name, f_sum, i_sum))
    if not rows_data:
        return None
    return generate_supply_chart(rows_data, asof=asof)


def _build_supply_summary(kr_tickers: list) -> str:
    """portfolio 국내 종목별 외국인·기관 수급 요약 (저녁 분석 헤더용).
    DB 에 데이터 없는 종목은 스킵. 반환: 멀티라인 문자열.

    종목당 3줄:
      헤더   — 티커/이름
      외국인 — 1D / 3D / 1W / 1M 기간별 순매수 누계
      기관   — 1D / 3D / 1W / 1M 기간별 순매수 누계
    (영업일 기준: 1D=당일, 3D=3영업일, 1W=5영업일, 1M=20영업일)
    """
    from database import load_investor_trend
    lines = ["🌏 수급 동향 (외국인·기관 순매수)"]
    had_data = False
    for t in kr_tickers:
        rows = load_investor_trend(t, days=20)
        if not rows:
            continue
        had_data = True

        def _agg(field: str, n: int) -> int:
            return sum((r.get(field) or 0) for r in rows[:n])

        def _periods(field: str) -> str:
            return (f"1D {_fmt_amt_mkrw(_agg(field, 1))}"
                    f"  3D {_fmt_amt_mkrw(_agg(field, 3))}"
                    f"  1W {_fmt_amt_mkrw(_agg(field, 5))}"
                    f"  1M {_fmt_amt_mkrw(_agg(field, 20))}")

        info = (config.get_portfolio_detail().get(t)
             or config.get_universe_detail().get(t)
             or {})
        name = info.get("name", t)
        lines.append(
            f"· {t} {name}\n"
            f"  외국인  {_periods('foreign_amt')}\n"
            f"  기관    {_periods('inst_amt')}"
        )
    return "\n".join(lines) if had_data else ""


def run_universe_signal_scan():
    """매일 universe 전체 시그널 스캔 → 발동 종목만 텔레그램 별도 메시지.
    포함 시그널: RSI/MACD/일목/MA10주봉/캔들반전/거래량급증/MA5풀백/공시.
    발동 0건이면 '발동 시그널 없음' 한 줄로 발송.
    """
    print(f"\n[{datetime.now():%Y-%m-%d %H:%M}] [SIGNAL] universe 자동 스캔 시작")
    try:
        cmd_signals(tickers=None)  # universe 전체 스캔 (cmd_update 자체 포함)
    except Exception as e:
        _notify_batch_error("시그널 스캔", e)


def run_kr_evening():
    """월~금 17:00 — 한국 장 마감 후 국내 포트폴리오 가격·공시 업데이트 +
    universe 전체 외국인·기관 수급 수집 + portfolio AI 분석 + 차트 전송.
    국내 포트폴리오가 비면 조용히 스킵.
    """
    print(f"\n[{datetime.now():%Y-%m-%d %H:%M}] [REPORT] 한국 저녁 분석 시작")
    try:
        _, kr_tickers = _split_portfolio()
        if not kr_tickers:
            print("[REPORT] 국내 포트폴리오 없음 — 스킵")
            return
        cmd_update(kr_tickers)

        # universe 전체 외국인·기관 매매동향 (≈67종목 × 1초 ≈ 2분).
        # 수급 수집이 실패해도 AI 분석은 진행 — 수급 블록은 전일 DB 데이터로 대체.
        from investor_collector import InvestorCollector
        universe_kr = [t for t in config.UNIVERSE
                       if not config.is_overseas(t)]
        try:
            InvestorCollector().fetch_all_tickers(universe_kr)
        except Exception as e:
            print(f"[INVESTOR] 수급 수집 중단 (저녁 분석은 계속 진행): {e}")

        supply       = _build_supply_summary(kr_tickers)
        supply_chart = _build_supply_chart(kr_tickers)
        header = (f"[REPORT] AI 주식 전략 | "
                  f"{datetime.now().strftime('%m/%d')} "
                  f"{config.EVENING_ANALYZE_TIME}")
        if supply:
            header = f"{header}\n\n{supply}"

        cmd_analyze(tickers=kr_tickers, header=header, header_photo=supply_chart)
    except Exception as e:
        _notify_batch_error("한국 저녁 분석", e)


# ── 진입점 ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args         = sys.argv[1:]
    ticker_arg   = _get_arg(args, "--ticker")
    exchange_arg = _get_arg(args, "--exchange")  # 미등록 종목일 때 거래소 명시 (NASDAQ/NYSE/KRX/HKEX/TSE 등)
    header       = _get_arg(args, "--header", default="")
    model_arg    = _get_arg(args, "--model")  # claude | gemini | None(.env 값)

    # 미등록 종목 자동 임시 등록 (KIS 해외거래소 코드·국내여부 판단에 필요)
    if ticker_arg:
        ticker_arg = ticker_arg.strip().upper()
        if ticker_arg not in config.get_portfolio_detail() \
           and ticker_arg not in config.get_universe_detail():
            config.register_temp(ticker_arg, exchange=exchange_arg)
            print(f"[{ticker_arg}] 미등록 종목 → 임시 등록 "
                  f"({exchange_arg or ('KRX' if ticker_arg.isdigit() else 'NASDAQ')})")
    tickers = [ticker_arg] if ticker_arg else None

    if   "--init"      in args: cmd_init()
    elif "--update"    in args: cmd_update(tickers)
    elif "--analyze"   in args:
        # --analyze 는 portfolio 분석이 목적 → 업데이트도 portfolio 만 (빠름).
        # --ticker 가 지정되면 해당 종목만.
        update_targets = tickers or list(config.PORTFOLIO)
        cmd_update(update_targets)
        cmd_analyze(tickers, header, primary=model_arg)
    elif "--dart-only" in args: cmd_dart_only(tickers)
    elif "--weekly"    in args: cmd_weekly_report(primary=model_arg)
    elif "--signals"   in args: cmd_signals(tickers)
    else:
        # 스케줄 모드: 주중(월~금) 3회
        #   · MARKET_WARNING_TIME — 시장 경고 브리핑 (F&G + 시세 + 신용/AI 뉴스)
        #   · MORNING_BRIEF_TIME  — 미국 업데이트 + 한국 DART 공시 요약
        #   · EVENING_ANALYZE_TIME — 한국 포트폴리오 업데이트 + AI 분석
        from market_warning import run_market_warning
        print(f"스케줄 모드 | 주중(월~금) "
              f"{config.MARKET_WARNING_TIME} 시장경고, "
              f"{config.MORNING_BRIEF_TIME} 모닝브리핑, "
              f"{config.EVENING_ANALYZE_TIME} 저녁분석, "
              f"{config.SIGNAL_SCAN_TIME} 시그널스캔")
        print("Ctrl+C 로 종료\n")
        for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
            getattr(schedule.every(), day).at(config.MARKET_WARNING_TIME).do(run_market_warning)
            getattr(schedule.every(), day).at(config.MORNING_BRIEF_TIME).do(run_morning_brief)
            getattr(schedule.every(), day).at(config.EVENING_ANALYZE_TIME).do(run_kr_evening)
            getattr(schedule.every(), day).at(config.SIGNAL_SCAN_TIME).do(run_universe_signal_scan)
        while True:
            schedule.run_pending()
            time.sleep(30)

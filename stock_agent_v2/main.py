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
from datetime import datetime
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
from chart_generator  import generate_chart


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
    """일/주/월 차트를 각각 생성해 반환. {"D": bytes, "W": bytes, "M": bytes}."""
    charts = {}
    for interval, limit in [("D", 400), ("W", 260), ("M", 60)]:
        try:
            df = load_candles(ticker, interval, limit=limit)
            if not df.empty:
                charts[interval] = generate_chart(
                    df, ticker, name, config.INDICATOR_CONFIG, interval=interval
                )
        except Exception as e:
            lbl = {"D": "일봉", "W": "주봉", "M": "월봉"}[interval]
            print(f"  [WARN] {ticker} {lbl} 차트 생성 실패: {e}")
    return charts


def cmd_analyze(tickers=None, header="", primary=None):
    """
    분석 실행 + 텔레그램 채널 전송
    primary: 'claude' | 'gemini' | None(config.AI_PRIMARY 따름)

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
        notifier.send_batch(all_results, header=full_header)

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

def run_daily():
    """주중 08:00 — portfolio 업데이트 + portfolio 분석 + 채널 전송.
    universe 전체 업데이트는 Telegram /update 로 수동 실행.
    예외 발생 시 traceback 을 로그로 남기고 Telegram 으로도 알림.
    """
    import traceback
    print(f"\n[{datetime.now():%Y-%m-%d %H:%M}] [REPORT] 일일 분석 시작")
    try:
        cmd_update(list(config.PORTFOLIO))
        cmd_analyze(header=f"[REPORT] AI 주식 전략 | {datetime.now().strftime('%m/%d')} 08:00")
    except Exception as e:
        tb = traceback.format_exc()
        print(f"\n[ERROR] run_daily 실패: {e}\n{tb}")
        try:
            TelegramNotifier().send_error(
                f"일일 배치 실패\n{datetime.now():%Y-%m-%d %H:%M}\n"
                f"오류: {e}\n로그: {_LOG_PATH.name}"
            )
        except Exception as te:
            print(f"[ERROR] Telegram 알림도 실패: {te}")


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
        # 스케줄 모드: 주중(월~금) 08:00 단 1회
        print(f"스케줄 모드 | 주중(월~금) {config.SCHEDULE_TIME} 자동 실행")
        print("Ctrl+C 로 종료\n")
        for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
            getattr(schedule.every(), day).at(config.SCHEDULE_TIME).do(run_daily)
        while True:
            schedule.run_pending()
            time.sleep(30)

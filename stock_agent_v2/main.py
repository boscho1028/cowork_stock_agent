"""
main.py - KIS API 버전 실행 진입점

사용법:
  python main.py --init                    # 최초 1회: DB생성 + 5년치 + DART
  python main.py --update                  # 증분 업데이트만
  python main.py --analyze                 # 전체 분석 + 채널 전송
  python main.py --analyze --ticker 005930 # 단일 종목 분석
  python main.py --dart-only               # 공시 확인만
  python main.py --weekly                  # 주간 리포트
  python main.py                           # 스케줄 모드 (주중 08:00)
"""

import os
import sys
import time
import schedule
from datetime import datetime

import config
from database       import init_db, save_analysis, mark_sent
from kis_collector  import KISCollector          # KIS REST API
from dart_collector import DartCollector
from sec_collector  import SECCollector
from analyzer       import StockAnalyzer
from telegram_bot   import TelegramNotifier


def _get_arg(args, key, default=None):
    """--key value 형태 인자 추출"""
    if key in args:
        idx = args.index(key)
        if idx + 1 < len(args):
            return args[idx + 1]
    return default


# ── 커맨드 함수 ───────────────────────────────────────────────────────

def cmd_init():
    """최초 1회 전체 초기화"""
    print("=" * 55)
    print("  KIS 초기화 시작 (5년치 캔들 + DART 전체 공시)")
    print("=" * 55)
    init_db()

    kis = KISCollector()
    if not kis.login():
        print("[ERROR] KIS 토큰 발급 실패 — App Key/Secret 확인")
        return

    kis.run_initial_load(config.PORTFOLIO, years=5)

    dart = DartCollector()
    print("\n[DART] 초기 공시 수집 시작...")
    dart.fetch_all_tickers(config.PORTFOLIO, days_back=365 * 5)

    print("\n[DART] 재무보고서 수집 시작...")
    for ticker in config.PORTFOLIO:
        name = config.PORTFOLIO_DETAIL.get(ticker, (ticker,))[0]
        print(f"  [{ticker}] {name} 재무 수집 중...")
        dart.fetch_financial_report(ticker)
        time.sleep(0.5)

    # SEC EDGAR 초기 적재 (해외 종목)
    sec = SECCollector()
    sec.fetch_initial(config.PORTFOLIO, days_back=365)

    print("\n✅ 초기화 완료. python main.py --analyze 로 분석 시작하세요.")


def cmd_update(tickers=None):
    """증분 업데이트: 전 영업일 캔들 + T-1 공시"""
    targets = tickers or config.PORTFOLIO

    kis = KISCollector()
    if not kis.login():
        print("[ERROR] KIS 토큰 발급 실패")
        return
    kis.run_daily_update(targets)

    dart = DartCollector()
    dart.fetch_all_tickers(targets, days_back=3)   # T-1: 국내

    sec = SECCollector()
    sec.fetch_all_tickers(targets, days_back=3)    # T-1: 해외 (SEC EDGAR)


def cmd_analyze(tickers=None, header=""):
    """
    분석 실행 + 텔레그램 채널 전송

    에러 처리 방침:
    - 정상 종목: 분석 결과 전송
    - 에러 종목: ⚠️ 에러 알림 포함해서 전송 (누락 없이 파악 가능)
    - 전체 에러: 헤더에 실패 현황 표시
    """
    targets  = tickers or config.PORTFOLIO
    notifier = TelegramNotifier(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID)
    analyzer = StockAnalyzer()

    ok_results  = []   # 정상 분석 결과
    err_results = []   # 에러 결과

    for ticker in targets:
        info = config.get_portfolio_detail().get(ticker, {})
        name = info.get("name", ticker)
        print(f"  ▶ [{ticker}] {name} 분석 중...")
        try:
            text = analyzer.analyze(ticker)
            save_analysis(ticker, text)
            ok_results.append({"ticker": ticker, "analysis": text, "ok": True})
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
        ) + f"⚠️ {ok_cnt}/{total}종목 정상 | 분석 실패: {err_names}"

    # 정상 결과 전송
    all_results = ok_results[:]

    # 에러 종목도 알림으로 포함
    for r in err_results:
        err_msg = (
            f"⚠️ [{r['ticker']}] {r['name']} 분석 실패\n"
            f"오류: {r['error']}\n"
            f"➡️ 다음 실행 시 자동 재시도됩니다."
        )
        all_results.append({"ticker": r["ticker"], "analysis": err_msg})

    if all_results:
        notifier.send_batch(all_results, header=full_header)

    for r in ok_results:
        mark_sent(r["ticker"])

    print(f"✅ 전송 완료: 정상 {ok_cnt}종목 | 에러 {err_cnt}종목")


def cmd_dart_only(tickers=None):
    """T-1 특별 공시만 확인 후 채널 전송"""
    targets  = tickers or config.PORTFOLIO
    dart     = DartCollector()
    notifier = TelegramNotifier(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID)

    lines = [f"📋 T-1 특별 공시 확인\n🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}"]
    for ticker in targets:
        dart.fetch_special_disclosures(ticker, days_back=3)
        summary = dart.get_disclosure_summary(ticker, limit=5)
        name = config.PORTFOLIO_DETAIL.get(ticker, (ticker,))[0]
        lines.append(f"─── {name}({ticker}) ───\n{summary}")

    notifier.send("\n\n".join(lines))
    print("✅ 공시 확인 완료")


def cmd_weekly_report():
    """주봉·월봉 중심 주간 리포트 + 파일 저장 + 채널 전송"""
    notifier = TelegramNotifier(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID)
    analyzer = StockAnalyzer()
    ts       = datetime.now().strftime("%Y%m%d")
    lines    = [f"📅 주간 전략 리포트 [{datetime.now().strftime('%Y-%m-%d')} 기준]"]
    results  = []

    for ticker in config.PORTFOLIO:
        name = config.PORTFOLIO_DETAIL.get(ticker, (ticker,))[0]
        print(f"  ▶ [{ticker}] {name} 주간 분석 중...")
        try:
            text = analyzer.analyze(ticker)
            results.append({"ticker": ticker, "analysis": text})
            lines.append(f"{'─'*30}\n{text}")
        except Exception as e:
            lines.append(f"❌ {ticker} {name}: {e}")

    # 파일 저장
    report_dir = os.path.join(os.path.dirname(__file__), "..", "output", "reports")
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"{ts}_주간전략.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n\n".join(lines))
    print(f"  [저장] {report_path}")

    notifier.send_batch(results, header="📅 주간 전략 리포트")
    print("✅ 주간 리포트 완료")


# ── 스케줄 래퍼 ───────────────────────────────────────────────────────

def run_daily():
    """주중 08:00 — 업데이트 + 전체 분석 + 채널 전송"""
    print(f"\n[{datetime.now():%Y-%m-%d %H:%M}] 📊 일일 분석 시작")
    cmd_update()
    cmd_analyze(header=f"📊 AI 주식 전략 | {datetime.now().strftime('%m/%d')} 08:00")


# ── 진입점 ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args       = sys.argv[1:]
    ticker_arg = _get_arg(args, "--ticker")
    tickers    = [ticker_arg] if ticker_arg else None
    header     = _get_arg(args, "--header", default="")

    if   "--init"      in args: cmd_init()
    elif "--update"    in args: cmd_update(tickers)
    elif "--analyze"   in args:
        cmd_update(tickers)
        cmd_analyze(tickers, header)
    elif "--dart-only" in args: cmd_dart_only(tickers)
    elif "--weekly"    in args: cmd_weekly_report()
    else:
        # 스케줄 모드: 주중(월~금) 08:00 단 1회
        print(f"스케줄 모드 | 주중(월~금) {config.SCHEDULE_TIME} 자동 실행")
        print("Ctrl+C 로 종료\n")
        for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
            getattr(schedule.every(), day).at(config.SCHEDULE_TIME).do(run_daily)
        while True:
            schedule.run_pending()
            time.sleep(30)

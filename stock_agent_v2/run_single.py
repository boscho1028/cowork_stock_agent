"""
run_single.py - 단일 종목 즉시 분석
사용: python run_single.py 005930
     python run_single.py NVDA
     python run_single.py 9988 HKEX      # 포트폴리오 외 종목 + 거래소 지정

미등록 종목 + 시세 수집 성공 시 universe.csv 에 자동 영구 등록
→ 다음 저녁 배치부터 외국인·기관 수급 수집에 포함됨.
"""
import os
import sys

os.environ["PYTHONUTF8"]       = "1"
os.environ["PYTHONIOENCODING"] = "utf-8"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from main import cmd_update, cmd_analyze
from database import get_conn


def _has_candles(ticker: str) -> bool:
    """DB 에 해당 티커 캔들이 한 건이라도 있으면 True."""
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT 1 FROM candles WHERE ticker=? LIMIT 1", (ticker,)
        )
        return cur.fetchone() is not None


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python run_single.py [종목코드] [거래소?]")
        print("예시:   python run_single.py 005930")
        print("        python run_single.py TSLA NASDAQ")
        sys.exit(1)

    ticker   = sys.argv[1].strip().upper()
    exchange = sys.argv[2].strip().upper() if len(sys.argv) > 2 else None

    # 이 subprocess 시작 시점의 상태 기준으로 "새 종목" 판정.
    was_new = (ticker not in config.get_portfolio_detail()
               and ticker not in config.get_universe_detail())
    if was_new:
        config.register_temp(ticker, exchange=exchange)
        resolved = exchange or ("KRX" if ticker.isdigit() else "NASDAQ")
        print(f"[{ticker}] 미등록 종목 → 임시 등록 ({resolved})")

    print(f"[{ticker}] 단일 종목 분석 시작...")
    cmd_update(tickers=[ticker])

    # KIS 가 시세를 한 건도 못 내려주면 = 미상장·상폐·코드 오인식.
    # 이 경우 빈 DB 로 분석을 돌리면 모든 지표가 N/A 인 무의미한 리포트가 나가므로,
    # 명시적 에러 알림 후 즉시 종료한다 (universe 자동 등록도 자연히 차단됨).
    if not _has_candles(ticker):
        from telegram_bot import TelegramNotifier
        msg = (
            f"[ERROR] [단일분석] {ticker} 시세 데이터를 가져올 수 없습니다.\n"
            f"- KIS 가 해당 종목코드를 인식하지 못합니다.\n"
            f"- 미상장·상장폐지·코드 오인식 가능성이 높습니다.\n"
            f"- 종목코드를 다시 확인해 주세요."
        )
        print(msg)
        try:
            TelegramNotifier(
                config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID
            ).send(msg)
        except Exception as e:
            print(f"[WARN] 텔레그램 알림 실패: {e}")
        sys.exit(2)

    # 새 종목이고 시세 수집에 실제로 성공했다면 universe.csv 영구 등록.
    if was_new:
        resolved = exchange or ("KRX" if ticker.isdigit() else "NASDAQ")
        if config.append_universe_row(ticker, exchange=resolved):
            print(f"[{ticker}] universe.csv 자동 추가 "
                  f"({resolved}, 다음 저녁 배치부터 수급 수집)")

    cmd_analyze(tickers=[ticker], header=f"[단일분석] {ticker}")

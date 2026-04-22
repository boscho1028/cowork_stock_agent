"""
run_single.py - 단일 종목 즉시 분석
사용: python run_single.py 005930
     python run_single.py NVDA
     python run_single.py 9988 HKEX      # 포트폴리오 외 종목 + 거래소 지정
"""
import os
import sys

# UTF-8 환경변수 설정 (reconfigure 사용 안 함 - 파일핸들 충돌 방지)
os.environ["PYTHONUTF8"]       = "1"
os.environ["PYTHONIOENCODING"] = "utf-8"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from main import cmd_update, cmd_analyze

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python run_single.py [종목코드] [거래소?]")
        print("예시:   python run_single.py 005930")
        print("        python run_single.py TSLA NASDAQ")
        sys.exit(1)

    ticker   = sys.argv[1].strip().upper()
    exchange = sys.argv[2].strip().upper() if len(sys.argv) > 2 else None

    # 포트폴리오/유니버스에 없으면 임시 등록 (거래소 코드·국내여부 판단에 필요)
    if ticker not in config.get_portfolio_detail() \
       and ticker not in config.get_universe_detail():
        config.register_temp(ticker, exchange=exchange)
        print(f"[{ticker}] 미등록 종목 → 임시 등록 "
              f"({exchange or ('KRX' if ticker.isdigit() else 'NASDAQ')})")

    print(f"[{ticker}] 단일 종목 분석 시작...")
    cmd_update(tickers=[ticker])
    cmd_analyze(tickers=[ticker], header=f"[단일분석] {ticker}")

"""
run_single.py
단일 종목 즉시 분석

사용:
  python run_single.py 005930
  python run_single.py 000660
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import cmd_update, cmd_analyze

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python run_single.py [종목코드]")
        print("예시:   python run_single.py 005930")
        sys.exit(1)

    ticker = sys.argv[1].strip()
    print(f"[{ticker}] 단일 종목 분석 시작...")
    cmd_update(tickers=[ticker])
    cmd_analyze(tickers=[ticker], header=f"🔍 {ticker} 즉시 분석")

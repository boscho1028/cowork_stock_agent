"""
run_signals.py - 시그널 스캔 래퍼
사용:
  python run_signals.py            # universe 전체
  python run_signals.py 005930     # 단일 종목만
"""
import os
import sys

os.environ["PYTHONUTF8"]       = "1"
os.environ["PYTHONIOENCODING"] = "utf-8"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from main import cmd_signals

if __name__ == "__main__":
    ticker  = sys.argv[1].strip().upper() if len(sys.argv) >= 2 else None
    tickers = [ticker] if ticker else None
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] 시그널 스캔 시작"
          + (f" ({ticker})" if ticker else ""))
    cmd_signals(tickers=tickers)

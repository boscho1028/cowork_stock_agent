"""run_market_warning.py — 매일 07:30 시장 경고 브리핑 실행 래퍼.
사용:
  python run_market_warning.py
"""
import os
import sys

os.environ["PYTHONUTF8"]       = "1"
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from market_warning import run_market_warning


if __name__ == "__main__":
    run_market_warning()

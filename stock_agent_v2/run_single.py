"""
run_single.py - 단일 종목 즉시 분석
사용: python run_single.py 005930
     python run_single.py NVDA
"""
import os
import sys

# UTF-8 환경변수 설정 (reconfigure 사용 안 함 - 파일핸들 충돌 방지)
os.environ["PYTHONUTF8"]       = "1"
os.environ["PYTHONIOENCODING"] = "utf-8"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import cmd_update, cmd_analyze

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python run_single.py [종목코드]")
        print("예시:   python run_single.py 005930")
        sys.exit(1)

    ticker = sys.argv[1].strip().upper()
    print(f"[{ticker}] 단일 종목 분석 시작...")
    cmd_update(tickers=[ticker])
    cmd_analyze(tickers=[ticker], header=f"[단일분석] {ticker}")

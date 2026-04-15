"""
run_daily.py - 일일 분석 래퍼
Windows 작업 스케줄러 / 텔레그램 봇에서 호출
"""
import os
import sys

os.environ["PYTHONUTF8"]       = "1"
os.environ["PYTHONIOENCODING"] = "utf-8"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from main import cmd_update, cmd_analyze

if __name__ == "__main__":
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] 일일 분석 시작")
    cmd_update()
    cmd_analyze(header=f"[AI 주식 전략] {datetime.now().strftime('%m/%d')} 08:00")

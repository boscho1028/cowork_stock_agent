"""
run_daily.py
Cowork 스케줄 래퍼 — 주중(월~금) 08:00 단 1회

Cowork 스케줄 설정:
  실행 파일: C:\stock_agent\stock_agent_v2\venv_kis\Scripts\python.exe
  스크립트:  C:\stock_agent\stock_agent_v2\run_daily.py
  시간: 08:00 / 평일만
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from main import cmd_update, cmd_analyze

if __name__ == "__main__":
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] 일일 분석 시작")
    cmd_update()
    cmd_analyze(header=f"📊 AI 주식 전략 | {datetime.now().strftime('%m/%d')} 08:00")

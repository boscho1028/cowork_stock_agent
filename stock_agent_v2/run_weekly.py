"""
run_weekly.py
주봉·월봉 중심 주간 전략 리포트

사용:
  python run_weekly.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import cmd_weekly_report

if __name__ == "__main__":
    cmd_weekly_report()

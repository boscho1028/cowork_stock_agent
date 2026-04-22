"""
run_update_universe.py - universe 전체 증분 업데이트 (분석·전송 없음)

Telegram /update 명령으로 트리거. 최근 5봉 재조회는 kis_collector.REFRESH_DAYS 로 자동 적용.
분석/Telegram 전송은 하지 않음 (업데이트만).
"""
import os
import sys

os.environ["PYTHONUTF8"]       = "1"
os.environ["PYTHONIOENCODING"] = "utf-8"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from main import cmd_update

if __name__ == "__main__":
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] universe 전체 업데이트 시작")
    cmd_update(None)  # None → config.UNIVERSE 전체
    print(f"[{datetime.now():%Y-%m-%d %H:%M}] universe 업데이트 완료")

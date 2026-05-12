"""Docker 컨테이너용 cron-like 스케줄러.

Linux/GX10 에서는 Windows Task Scheduler 대신 이 파일을 별도 컨테이너에서
띄워 일일 배치를 실행한다. (Windows PC 는 기존 Task Scheduler 그대로 사용)

평일 KST 기준:
  07:25  시장 경고 브리핑
  07:30  모닝 브리핑 (US update + DART/SEC 공시 요약)
  17:00  한국 저녁 분석 (cmd_update + 수급 + AI + ETF chained)

ETF/시그널/차트 prebuild 등은 run_kr_evening 내부 체이닝으로 함께 돌아감.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

os.environ.setdefault("PYTHONUTF8", "1")
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

import config
from main import run_morning_brief, run_kr_evening
from market_warning import run_market_warning


KST = pytz.timezone("Asia/Seoul")


def _add_weekday(sched: BlockingScheduler, hhmm: str, fn, name: str):
    """평일(월-금) hh:mm KST 에 fn 실행 등록."""
    hh, mm = hhmm.split(":")
    sched.add_job(
        fn,
        CronTrigger(day_of_week="mon-fri",
                    hour=int(hh), minute=int(mm),
                    timezone=KST),
        id=name, name=name, replace_existing=True,
        max_instances=1, coalesce=True,
        misfire_grace_time=300,  # 5분 늦게 일어나도 1회는 실행
    )


def main():
    sched = BlockingScheduler(timezone=KST)

    _add_weekday(sched, config.MARKET_WARNING_TIME,  run_market_warning,
                  "market_warning")
    _add_weekday(sched, config.MORNING_BRIEF_TIME,   run_morning_brief,
                  "morning_brief")
    _add_weekday(sched, config.EVENING_ANALYZE_TIME, run_kr_evening,
                  "kr_evening")

    print(f"[SCHEDULER] 시작 — KST 기준 평일 잡:")
    for job in sched.get_jobs():
        print(f"  · {job.name}: {job.trigger}")
    print(f"[SCHEDULER] 부팅 완료 — {datetime.now(KST):%Y-%m-%d %H:%M:%S}")

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        print("[SCHEDULER] 종료")


if __name__ == "__main__":
    main()

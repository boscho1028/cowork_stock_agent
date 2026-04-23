"""
run_daily.py — Windows 작업 스케줄러 래퍼

사용:
  run_daily.py morning  월~금 07:30 — 미국 업데이트 + SEC/DART 공시 모닝 브리핑
  run_daily.py evening  월~금 17:00 — 한국 업데이트 + 외국인·기관 수급 + AI 분석
"""
import os
import sys

os.environ["PYTHONUTF8"]       = "1"
os.environ["PYTHONIOENCODING"] = "utf-8"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import run_morning_brief, run_kr_evening

_DISPATCH = {
    "morning": run_morning_brief,
    "evening": run_kr_evening,
}

if __name__ == "__main__":
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else ""
    fn   = _DISPATCH.get(mode)
    if fn is None:
        print(f"Usage: run_daily.py {{{'|'.join(_DISPATCH)}}}", file=sys.stderr)
        sys.exit(2)
    fn()

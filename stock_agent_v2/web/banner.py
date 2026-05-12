"""웹 페이지 상단 상태 배너 빌더.

`batch_runs` 테이블의 최신 실행 상태를 보고 (실패/지연/실행중) 배너 dict 반환.
각 페이지 라우트에서 `build_status_banner(name, label)` 로 호출.
여러 배치에 의존하는 페이지는 `build_multi_banner` 로 가장 심각한 것 선택.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from database import latest_batch_run

_KST = timezone(timedelta(hours=9))

_LEVEL_RANK = {"error": 0, "warn": 1, "running": 2, "info": 3}


def _parse(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return datetime.fromisoformat(ts)
        except Exception:
            return None


def build_status_banner(batch_name: str,
                         label: str | None = None) -> dict | None:
    """배치 실행 상태 → 배너 dict | None.

    레벨:
      error   — 24시간 이내 실패
      warn    — 30분+ running 중 (hang 의심)
      running — 정상 실행 중
      None    — 24h 이내 성공 (배너 안 보임)
    """
    label = label or batch_name
    run = latest_batch_run(batch_name)
    if not run:
        return None

    now = datetime.now()

    # 1) 실행 중
    if run["status"] == "running" and not run["finished_at"]:
        started = _parse(run["started_at"])
        if started and (now - started) > timedelta(minutes=30):
            return {
                "level":  "warn",
                "title":  f"{label} 30분 이상 응답 없음",
                "detail": f"시작 {run['started_at']} KST — 멈춰있을 수 있음",
            }
        return {
            "level":  "running",
            "title":  f"{label} 실행 중",
            "detail": f"시작 {run['started_at']} KST · 완료 후 자동 갱신",
        }

    # 2) 24시간 이내 실패
    if run["status"] == "failure" and run["finished_at"]:
        finished = _parse(run["finished_at"])
        if finished and (now - finished) < timedelta(hours=24):
            return {
                "level":  "error",
                "title":  f"마지막 {label} 실패",
                "detail": f"{run['finished_at']} KST · {run['message'] or '원인 불명'}",
            }

    # 3) 24시간 이내 partial (KR 만 됐고 US 실패 같은 경우)
    if run["status"] == "partial" and run["finished_at"]:
        finished = _parse(run["finished_at"])
        if finished and (now - finished) < timedelta(hours=24):
            return {
                "level":  "warn",
                "title":  f"{label} 일부만 성공",
                "detail": f"{run['finished_at']} KST · {run['message'] or ''}",
            }

    return None


def last_refresh_at(batch_name: str) -> str | None:
    """가장 최근 success/partial 종료 시각 (KST). 없으면 None."""
    run = latest_batch_run(batch_name)
    if not run:
        return None
    if run["status"] in ("success", "partial") and run["finished_at"]:
        return run["finished_at"]
    return None


def build_multi_banner(batches: list[tuple[str, str]]) -> dict | None:
    """여러 배치를 함께 보고 가장 심각한 레벨의 배너 반환.
    batches: [(name, label), ...]"""
    worst: dict | None = None
    for name, label in batches:
        b = build_status_banner(name, label)
        if not b:
            continue
        if (worst is None or
                _LEVEL_RANK[b["level"]] < _LEVEL_RANK[worst["level"]]):
            worst = b
    return worst

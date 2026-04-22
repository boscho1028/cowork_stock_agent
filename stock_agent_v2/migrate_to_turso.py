"""
migrate_to_turso.py - 로컬 SQLite(data/stock_agent.db) → Turso 일회성 마이그레이션

실행 방법:
  1) .env 에 TURSO_DATABASE_URL / TURSO_AUTH_TOKEN 설정 완료
  2) 기존 로컬 DB 를 data/stock_agent_backup.db 로 복사(안전장치)
  3) python migrate_to_turso.py

동작:
  - 기존 data/stock_agent.db 를 "원본(소스)"으로 읽기 전용 열기
  - Turso embedded replica(= database.get_conn) 로 init 후 테이블별로 INSERT
  - 마지막에 conn.sync() 로 클라우드로 push
  - 다른 PC 는 첫 실행 시 init_db() → sync() 로 자동 수신
"""

import shutil
import sqlite3
import sys
from pathlib import Path

import config
from database import DB_PATH, init_db, _new_conn, _try_sync

BASE_DIR   = Path(__file__).parent
SOURCE_DB  = BASE_DIR / "data" / "stock_agent_source.db"  # 마이그레이션 소스 (원본 복사본)
BACKUP_DB  = BASE_DIR / "data" / "stock_agent_backup.db"  # 사용자 안전장치

# 이관할 테이블(기본키·UNIQUE 제약은 스키마에 이미 있음 → INSERT OR IGNORE 로 중복 방지)
TABLES_WITH_COLS = {
    "candles": [
        "ticker", "interval", "date", "open", "high", "low", "close", "volume", "created_at",
    ],
    "dart_disclosures": [
        "rcept_no", "ticker", "corp_name", "report_nm", "rcept_dt", "rm", "flr_nm", "created_at",
    ],
    "dart_reports": [
        "rcept_no", "ticker", "report_type", "period_end",
        "revenue", "op_income", "net_income", "total_assets", "total_equity",
        "per", "pbr", "roe", "debt_ratio", "summary_text", "created_at",
    ],
    "analysis_log": [
        "ticker", "analyzed_at", "result_text", "sent_telegram",
    ],
    "sec_filings": [
        "ticker", "cik", "accession", "form_type", "filed_date",
        "description", "items", "importance", "url", "created_at",
    ],
    "sec_cik_map": [
        "ticker", "cik", "cik_padded", "company", "updated_at",
    ],
}


def _prepare_source() -> Path:
    """
    소스 결정 순서:
      1) data/stock_agent_legacy.db  (init_db 가 자동 비켜 둔 구버전 파일)
      2) data/stock_agent.db          (아직 init_db 안 돌린 상태)
      3) data/stock_agent_source.db  (이전 실행에서 만든 소스)
    백업본을 stock_agent_backup.db 로 복사해 안전장치 확보.
    """
    legacy = BASE_DIR / "data" / "stock_agent_legacy.db"
    candidates = [legacy, DB_PATH, SOURCE_DB]
    origin = next((p for p in candidates if p.exists() and p.stat().st_size > 0), None)

    if origin is None:
        print(f"[마이그레이션] 이관할 원본 DB 를 찾지 못함 (확인 경로: {[str(p) for p in candidates]})")
        sys.exit(0)

    print(f"[마이그레이션] 소스 후보 발견: {origin}")

    if not BACKUP_DB.exists():
        print(f"[마이그레이션] 백업을 {BACKUP_DB.name} 로 복사")
        shutil.copy2(origin, BACKUP_DB)

    # init_db() 가 DB_PATH 를 replica 로 덮어쓰기 전에 소스를 별도 파일로 고정
    if not SOURCE_DB.exists() or origin != SOURCE_DB:
        print(f"[마이그레이션] 원본을 {SOURCE_DB.name} 로 복사")
        shutil.copy2(origin, SOURCE_DB)

    return SOURCE_DB


BATCH_SIZE = 1000


def _copy_table(src: sqlite3.Connection, dst, table: str, cols: list) -> int:
    # 원본에 테이블이 없으면 skip
    has = src.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if not has:
        print(f"  [{table}] 원본에 테이블 없음 -> skip", flush=True)
        return 0

    # 실제 컬럼 교집합만 사용 (원본 스키마가 구버전일 수 있음)
    src_cols = {r[1] for r in src.execute(f"PRAGMA table_info({table})").fetchall()}
    use_cols = [c for c in cols if c in src_cols]
    if not use_cols:
        print(f"  [{table}] 사용 가능한 컬럼 없음 -> skip", flush=True)
        return 0

    # 총 건수 먼저 확인 (진행도 표시용)
    total = src.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if total == 0:
        print(f"  [{table}] 0건", flush=True)
        return 0

    col_list     = ",".join(use_cols)
    placeholders = ",".join(["?"] * len(use_cols))
    sql = f"INSERT OR IGNORE INTO {table} ({col_list}) VALUES ({placeholders})"

    print(f"  [{table}] {total:,}건 이관 시작 (배치 {BATCH_SIZE})...", flush=True)

    # iter_chunk: fetchmany 로 스트리밍 (fetchall 로 메모리 폭발 방지)
    cur = src.execute(f"SELECT {col_list} FROM {table}")
    copied = 0
    while True:
        chunk = cur.fetchmany(BATCH_SIZE)
        if not chunk:
            break
        dst.executemany(sql, chunk)
        dst.commit()  # 배치별 commit 으로 WAL 정리
        copied += len(chunk)
        print(f"    ...{copied:,}/{total:,}", flush=True)
    print(f"  [{table}] 완료: {copied:,}건", flush=True)
    return copied


def main():
    if not (config.TURSO_DATABASE_URL and config.TURSO_AUTH_TOKEN):
        print("[마이그레이션] TURSO_DATABASE_URL / TURSO_AUTH_TOKEN 미설정 → .env 확인")
        sys.exit(1)

    source = _prepare_source()
    print(f"\n[마이그레이션] 소스: {source}")
    print(f"[마이그레이션] 대상: Turso ({config.TURSO_DATABASE_URL})\n")

    # 1) 스키마 초기화 (replica 파일 생성 + 클라우드 sync)
    print("[마이그레이션] 스키마 초기화 중...")
    init_db()

    # 2) 원본에서 읽고 replica 로 쓰기
    src = sqlite3.connect(source)
    src.row_factory = None  # 튜플로 받기
    dst = _new_conn()

    total = 0
    try:
        for table, cols in TABLES_WITH_COLS.items():
            total += _copy_table(src, dst, table, cols)
        dst.commit()
        # 3) 클라우드로 push
        print("\n[마이그레이션] Turso 클라우드로 push (sync) 중...")
        _try_sync(dst)
    finally:
        src.close()
        dst.close()

    print(f"\n[마이그레이션] 완료. 총 {total:,}건 이관됨.")
    print(f"  - 백업본: {BACKUP_DB}")
    print(f"  - 소스본(삭제해도 됨): {SOURCE_DB}")
    print("\n다른 PC 에서는 .env 설정 후 python -c \"import database; database.init_db()\" 실행만 하면 자동 동기화됩니다.")


if __name__ == "__main__":
    main()

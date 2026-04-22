"""
test_setup.py - Turso DB + KIS 토큰 공유 세팅 smoke test

체크 항목:
  1) .env 환경변수가 제대로 로드되는가
  2) libsql 패키지 import 가능한가
  3) Turso 연결 + 스키마 초기화 + sync() 동작하는가
  4) 각 테이블 row 수 확인 (이관 결과 검증)
  5) KIS 토큰 캐시 폴더가 쓰기 가능한가
  6) (선택) KIS API 로 실제 토큰 1회 발급/캐시 확인

실행: python test_setup.py
"""
import os
import sys
from pathlib import Path


def _hr():
    print("-" * 60)


def step(n: int, title: str):
    print(f"\n[{n}] {title}")
    _hr()


def ok(msg: str):
    print(f"  [OK]   {msg}")


def fail(msg: str):
    print(f"  [FAIL] {msg}")


def warn(msg: str):
    print(f"  [WARN] {msg}")


def main():
    all_ok = True

    # ── 1. 환경변수 ──────────────────────────────────────────────────
    step(1, ".env 환경변수 확인")
    import config
    checks = [
        ("TURSO_DATABASE_URL", config.TURSO_DATABASE_URL),
        ("TURSO_AUTH_TOKEN",   config.TURSO_AUTH_TOKEN),
        ("KIS_APP_KEY",        config.KIS_APP_KEY),
        ("KIS_APP_SECRET",     config.KIS_APP_SECRET),
    ]
    for name, val in checks:
        if val:
            # 토큰은 앞 8자리만
            preview = val[:20] + "..." if len(val) > 20 else val
            ok(f"{name} = {preview}")
        else:
            warn(f"{name} 비어 있음 (로컬 폴백 모드로 동작)")
    ok(f"KIS_TOKEN_CACHE_DIR = {config.KIS_TOKEN_CACHE_DIR}")

    # ── 2. libsql 패키지 ────────────────────────────────────────────
    step(2, "libsql 패키지")
    try:
        import libsql  # noqa: F401
        ok("libsql import 성공")
    except ImportError:
        fail("libsql 미설치 → pip install -r requirements.txt")
        all_ok = False

    # ── 3. DB 초기화 + sync ──────────────────────────────────────────
    step(3, "DB 연결 + 스키마 초기화 + Turso sync")
    try:
        import database
        database.init_db()
        mode = "Turso embedded replica" if database._TURSO_ENABLED else "로컬 SQLite (Turso 비활성)"
        ok(f"모드: {mode}")
    except Exception as e:
        fail(f"init_db 실패: {e}")
        all_ok = False
        return all_ok

    # ── 4. 테이블 row 수 ─────────────────────────────────────────────
    step(4, "테이블별 row 수")
    tables = [
        "candles", "dart_disclosures", "dart_reports",
        "analysis_log", "sec_filings", "sec_cik_map",
    ]
    try:
        with database.get_conn() as conn:
            for t in tables:
                try:
                    n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    ok(f"{t:20s} {n:>10,} rows")
                except Exception as e:
                    warn(f"{t}: 조회 실패 ({e})")
    except Exception as e:
        fail(f"DB 조회 실패: {e}")
        all_ok = False

    # ── 5. 쓰기 + sync 테스트 ───────────────────────────────────────
    step(5, "쓰기 → sync() 라운드트립 테스트")
    try:
        with database.get_conn(sync_after=True) as conn:
            conn.execute(
                "INSERT INTO analysis_log (ticker, analyzed_at, result_text) "
                "VALUES (?, datetime('now','localtime'), ?)",
                ("__SMOKE_TEST__", "setup test row"),
            )
        # 다시 읽어서 확인
        with database.get_conn() as conn:
            cur = conn.execute(
                "SELECT COUNT(*) FROM analysis_log WHERE ticker=?",
                ("__SMOKE_TEST__",),
            )
            n = cur.fetchone()[0]
        if n >= 1:
            ok(f"__SMOKE_TEST__ row {n}건 확인")
        else:
            fail("쓰기 후 읽기 실패")
            all_ok = False
        # 테스트 row 정리
        with database.get_conn(sync_after=True) as conn:
            conn.execute("DELETE FROM analysis_log WHERE ticker=?", ("__SMOKE_TEST__",))
        ok("테스트 row 정리 완료")
    except Exception as e:
        fail(f"쓰기/sync 실패: {e}")
        all_ok = False

    # ── 6. KIS 토큰 캐시 폴더 ───────────────────────────────────────
    step(6, "KIS 토큰 캐시 폴더")
    token_dir = Path(config.KIS_TOKEN_CACHE_DIR)
    try:
        token_dir.mkdir(parents=True, exist_ok=True)
        probe = token_dir / ".write_test"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        ok(f"쓰기 가능: {token_dir}")
    except Exception as e:
        fail(f"폴더 접근 실패: {e}")
        all_ok = False

    # ── 7. (선택) 실제 KIS 토큰 발급·캐시 확인 ──────────────────────
    step(7, "KIS 토큰 발급·공유 캐시 동작")
    if not (config.KIS_APP_KEY and config.KIS_APP_SECRET):
        warn("KIS_APP_KEY/SECRET 비어 있음 → 스킵")
    else:
        try:
            from kis_collector import KISCollector
            kc = KISCollector()
            token = kc._get_token()  # 메모리 → 파일 → 신규발급
            if token:
                ok(f"토큰 발급 OK (길이: {len(token)})")
            cache_file = kc._token_cache_file()
            if cache_file.exists():
                ok(f"공유 캐시 파일 생성됨: {cache_file.name}")
            else:
                warn(f"공유 캐시 파일 미생성: {cache_file}")
        except Exception as e:
            fail(f"KIS 토큰 발급 실패: {e}")
            all_ok = False

    # ── 요약 ────────────────────────────────────────────────────────
    print()
    _hr()
    if all_ok:
        print("[SUCCESS] 모든 체크 통과 - 2PC 공유 환경 준비 완료")
    else:
        print("[FAILED]  일부 항목 실패 - 위 로그 확인 후 재실행")
    _hr()
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()

"""
database.py - libSQL (Turso) embedded replica + SQLite 폴백
- 평상시: 로컬 replica 파일(data/stock_agent.db)에서 읽고,
          쓰기 후 Turso 클라우드로 sync → 다른 PC와 공유
- 환경변수 TURSO_DATABASE_URL/TURSO_AUTH_TOKEN이 없으면 로컬 SQLite만 사용(폴백)
테이블: candles / dart_disclosures / dart_reports / analysis_log / sec_filings / sec_cik_map
"""

import pandas as pd
from pathlib import Path
from contextlib import contextmanager

import config

# libsql 바인딩 (Turso 공식, Windows prebuilt wheel 지원).
# 미설치/미지원 환경이면 sqlite3로 자동 폴백.
try:
    import libsql  # type: ignore
    _HAS_LIBSQL = True
except Exception:
    import sqlite3 as libsql  # type: ignore
    _HAS_LIBSQL = False

DB_PATH = Path(__file__).parent / "data" / "stock_agent.db"

_TURSO_ENABLED = bool(config.TURSO_DATABASE_URL and config.TURSO_AUTH_TOKEN and _HAS_LIBSQL)


def _new_conn():
    """새 연결 생성. Turso 설정이 있으면 embedded replica, 아니면 순수 로컬."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if _TURSO_ENABLED:
        return libsql.connect(
            str(DB_PATH),
            sync_url=config.TURSO_DATABASE_URL,
            auth_token=config.TURSO_AUTH_TOKEN,
        )
    return libsql.connect(str(DB_PATH))


def _try_sync(conn):
    """Turso 모드일 때만 sync (실패해도 무시 — 로컬은 이미 커밋됨)."""
    if not _TURSO_ENABLED:
        return
    try:
        conn.sync()
    except Exception as e:
        print(f"[DB] Turso sync 실패(로컬 커밋은 완료): {e}")


def _migrate_legacy_sqlite():
    """
    Turso 모드인데 DB_PATH 가 구버전 순수 SQLite 파일이면 replica 로 열 수 없다.
    → 자동으로 stock_agent_legacy.db 로 옮겨 빈 replica 로 시작.
    기존 데이터는 migrate_to_turso.py 로 이관해야 함.
    """
    if not _TURSO_ENABLED or not DB_PATH.exists():
        return
    legacy = DB_PATH.with_name(DB_PATH.stem + "_legacy.db")
    # 이미 legacy 가 있고 DB_PATH 가 그대로라면 → 이미 처리됨
    if legacy.exists() and DB_PATH.stat().st_size == 0:
        return
    try:
        probe = libsql.connect(
            str(DB_PATH),
            sync_url=config.TURSO_DATABASE_URL,
            auth_token=config.TURSO_AUTH_TOKEN,
        )
        probe.sync()
        probe.close()
    except Exception as e:
        msg = str(e)
        if ("metadata file does not" in msg) or ("invalid local state" in msg):
            if legacy.exists():
                legacy.unlink()  # 중복 방지
            print(f"[DB] 기존 순수 SQLite 감지 → {legacy.name} 으로 이동")
            print("[DB]   (데이터 이관이 필요하면 python migrate_to_turso.py 실행)")
            DB_PATH.rename(legacy)
            # WAL/SHM 도 같이 정리
            for suffix in ("-wal", "-shm"):
                side = DB_PATH.with_name(DB_PATH.name + suffix)
                if side.exists():
                    side.unlink()
        else:
            raise


def init_db():
    """DB 및 테이블 초기화 (없으면 생성). Turso 설정 시 최초 pull도 수행."""
    _migrate_legacy_sqlite()
    conn = _new_conn()
    try:
        # 클라우드에서 최신 스키마·데이터 pull
        _try_sync(conn)
        conn.executescript("""
        -- ── 캔들 (일/주/월봉 통합) ─────────────────────────────────
        CREATE TABLE IF NOT EXISTS candles (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker     TEXT NOT NULL,
            interval   TEXT NOT NULL,        -- 'D' | 'W' | 'M'
            date       TEXT NOT NULL,        -- YYYY-MM-DD
            open       REAL NOT NULL,
            high       REAL NOT NULL,
            low        REAL NOT NULL,
            close      REAL NOT NULL,
            volume     REAL NOT NULL,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            UNIQUE(ticker, interval, date)
        );
        CREATE INDEX IF NOT EXISTS idx_candles
            ON candles(ticker, interval, date DESC);

        -- ── DART 특별 공시 목록 ──────────────────────────────────────
        CREATE TABLE IF NOT EXISTS dart_disclosures (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            rcept_no   TEXT UNIQUE NOT NULL,
            ticker     TEXT NOT NULL,
            corp_name  TEXT,
            report_nm  TEXT,
            rcept_dt   TEXT,
            rm         TEXT,
            flr_nm     TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_dart_disc
            ON dart_disclosures(ticker, rcept_dt DESC);

        -- ── DART 재무보고서 요약 ─────────────────────────────────────
        CREATE TABLE IF NOT EXISTS dart_reports (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            rcept_no     TEXT UNIQUE NOT NULL,
            ticker       TEXT NOT NULL,
            report_type  TEXT,
            period_end   TEXT,
            revenue      REAL,
            op_income    REAL,
            net_income   REAL,
            total_assets REAL,
            total_equity REAL,
            per          REAL,
            pbr          REAL,
            roe          REAL,
            debt_ratio   REAL,
            summary_text TEXT,
            created_at   TEXT DEFAULT (datetime('now','localtime'))
        );

        -- ── 분석 로그 ────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS analysis_log (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker        TEXT NOT NULL,
            analyzed_at   TEXT NOT NULL,
            result_text   TEXT,
            sent_telegram INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_log
            ON analysis_log(ticker, analyzed_at DESC);

        -- ── SEC EDGAR 공시 목록 (미국 주식) ─────────────────────────
        CREATE TABLE IF NOT EXISTS sec_filings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT NOT NULL,
            cik         TEXT NOT NULL,
            accession   TEXT UNIQUE NOT NULL,
            form_type   TEXT NOT NULL,
            filed_date  TEXT NOT NULL,
            description TEXT,
            items       TEXT,
            importance  TEXT DEFAULT '🔵',
            url         TEXT,
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_sec_ticker_date
            ON sec_filings(ticker, filed_date DESC);

        -- ── SEC CIK 매핑 캐시 ────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS sec_cik_map (
            ticker     TEXT PRIMARY KEY,
            cik        TEXT NOT NULL,
            cik_padded TEXT NOT NULL,
            company    TEXT,
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );
        """)
        conn.commit()
        # 스키마 생성분을 클라우드로 push
        _try_sync(conn)
    finally:
        conn.close()
    mode = "Turso(embedded replica)" if _TURSO_ENABLED else "로컬 SQLite"
    print(f"[DB] 초기화 완료 ({mode}): {DB_PATH}")


@contextmanager
def get_conn(sync_after: bool = False):
    """
    커넥션 컨텍스트 매니저.
    sync_after=True: 쓰기 성공 후 Turso 클라우드로 push (다른 PC와 공유)
    """
    conn = _new_conn()
    # sqlite3 폴백일 때만 row_factory 설정 (libsql-experimental 은 자체 row 지원)
    if not _HAS_LIBSQL:
        try:
            import sqlite3 as _sq
            conn.row_factory = _sq.Row
        except Exception:
            pass
    try:
        yield conn
        conn.commit()
        if sync_after:
            _try_sync(conn)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _fetch_df(conn, sql: str, params: list) -> pd.DataFrame:
    """
    DB-API 커서로 SELECT → DataFrame.
    pd.read_sql 이 libsql 연결에서 동작 안 할 수 있어 범용 경로로 통일.
    """
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


# ── 캔들 CRUD ────────────────────────────────────────────────────────

def upsert_candles(
    df: pd.DataFrame,
    ticker: str,
    interval: str,
    replace_all: bool = False,
) -> int:
    """
    DataFrame → candles 테이블 upsert
    replace_all=True : Corporate Event 발생 시 기존 삭제 후 수정주가 재적재
    replace_all=False: 일반 증분 업데이트
    """
    if df.empty:
        return 0
    rows = [
        (ticker, interval, str(idx.date()),
         float(row.open), float(row.high),
         float(row.low),  float(row.close), float(row.volume))
        for idx, row in df.iterrows()
    ]
    with get_conn(sync_after=True) as conn:
        if replace_all:
            conn.execute(
                "DELETE FROM candles WHERE ticker=? AND interval=?",
                (ticker, interval)
            )
        conn.executemany("""
            INSERT OR REPLACE INTO candles
                (ticker, interval, date, open, high, low, close, volume)
            VALUES (?,?,?,?,?,?,?,?)
        """, rows)
    return len(rows)


def load_candles(
    ticker: str,
    interval: str,
    limit: int = 300,
    start_date: str = None,
) -> pd.DataFrame:
    """candles 테이블 → DataFrame (index=datetime)"""
    sql = """
        SELECT date, open, high, low, close, volume
        FROM   candles
        WHERE  ticker=? AND interval=?
    """
    params = [ticker, interval]
    if start_date:
        sql += " AND date >= ?"
        params.append(start_date)
    sql += f" ORDER BY date DESC LIMIT {int(limit)}"

    with get_conn() as conn:
        df = _fetch_df(conn, sql, params)

    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date").sort_index()


def get_latest_candle_date(ticker: str, interval: str):
    """마지막 저장 날짜 반환 (증분 업데이트용)"""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT MAX(date) FROM candles WHERE ticker=? AND interval=?",
            (ticker, interval)
        ).fetchone()
    return row[0] if row and row[0] else None


# ── DART CRUD ────────────────────────────────────────────────────────

def upsert_disclosures(rows: list) -> int:
    if not rows:
        return 0
    # libsql 은 dict 바인딩(:name) 미지원 → 튜플 리스트로 변환
    cols = ["rcept_no", "ticker", "corp_name", "report_nm", "rcept_dt", "rm", "flr_nm"]
    tuples = [tuple(r.get(c) for c in cols) for r in rows]
    with get_conn(sync_after=True) as conn:
        conn.executemany("""
            INSERT OR IGNORE INTO dart_disclosures
                (rcept_no, ticker, corp_name, report_nm, rcept_dt, rm, flr_nm)
            VALUES (?,?,?,?,?,?,?)
        """, tuples)
    return len(rows)


def load_disclosures(ticker: str, limit: int = 10, since_date: str = None) -> list:
    """
    since_date: YYYYMMDD (DART rcept_dt 포맷). None이면 전체.
    """
    sql    = "SELECT rcept_no, report_nm, rcept_dt, flr_nm, rm FROM dart_disclosures WHERE ticker=?"
    params = [ticker]
    if since_date:
        sql += " AND rcept_dt >= ?"
        params.append(since_date.replace("-", ""))
    sql += " ORDER BY rcept_dt DESC LIMIT ?"
    params.append(limit)
    with get_conn() as conn:
        cur  = conn.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return [dict(zip(cols, r)) for r in rows]


def upsert_report(data: dict):
    # libsql 은 dict 바인딩(:name) 미지원 → 튜플로 변환
    cols = ["rcept_no", "ticker", "report_type", "period_end",
            "revenue", "op_income", "net_income",
            "total_assets", "total_equity",
            "per", "pbr", "roe", "debt_ratio", "summary_text"]
    values = tuple(data.get(c) for c in cols)
    with get_conn(sync_after=True) as conn:
        conn.execute("""
            INSERT OR REPLACE INTO dart_reports
                (rcept_no, ticker, report_type, period_end,
                 revenue, op_income, net_income,
                 total_assets, total_equity,
                 per, pbr, roe, debt_ratio, summary_text)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, values)


def load_latest_report(ticker: str):
    with get_conn() as conn:
        cur  = conn.execute("""
            SELECT * FROM dart_reports
            WHERE ticker=?
            ORDER BY period_end DESC LIMIT 1
        """, (ticker,))
        cols = [d[0] for d in cur.description]
        row  = cur.fetchone()
    return dict(zip(cols, row)) if row else None


# ── 분석 로그 ────────────────────────────────────────────────────────

def save_analysis(ticker: str, result_text: str):
    with get_conn(sync_after=True) as conn:
        conn.execute("""
            INSERT INTO analysis_log (ticker, analyzed_at, result_text)
            VALUES (?, datetime('now','localtime'), ?)
        """, (ticker, result_text))


def mark_sent(ticker: str):
    with get_conn(sync_after=True) as conn:
        conn.execute("""
            UPDATE analysis_log SET sent_telegram=1
            WHERE ticker=? AND sent_telegram=0
        """, (ticker,))

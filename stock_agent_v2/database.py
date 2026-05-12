"""
database.py - libSQL (Turso) embedded replica + SQLite 폴백
- 평상시: 로컬 replica 파일(data/stock_agent.db)에서 읽고,
          쓰기 후 Turso 클라우드로 sync → 다른 PC와 공유
- 환경변수 TURSO_DATABASE_URL/TURSO_AUTH_TOKEN이 없으면 로컬 SQLite만 사용(폴백)
테이블: candles / dart_disclosures / dart_reports / analysis_log / sec_filings / sec_cik_map
"""

from datetime import datetime, timezone, timedelta

import pandas as pd
from pathlib import Path
from contextlib import contextmanager

import config

_KST = timezone(timedelta(hours=9))


def _now_kst() -> str:
    """현재 시각을 KST 'YYYY-MM-DD HH:MM:SS' 문자열로.
    SQL 의 datetime('now','localtime') 은 Turso 클라우드(UTC) 에서 실행되면
    UTC 가 되어버려 KST 와 9시간 어긋난다. 타임스탬프는 항상 Python 에서
    이 함수로 만들어 파라미터로 넣는다.
    """
    return datetime.now(_KST).strftime("%Y-%m-%d %H:%M:%S")

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

        -- ── 투자자별 매매동향 (외국인·기관, KIS inquire-investor) ──
        -- foreign_amt / inst_amt: 순매수 거래대금 (KIS raw, 백만원 단위)
        CREATE TABLE IF NOT EXISTS investor_trend (
            ticker      TEXT NOT NULL,
            trade_date  TEXT NOT NULL,          -- YYYY-MM-DD
            foreign_amt INTEGER,
            inst_amt    INTEGER,
            created_at  TEXT DEFAULT (datetime('now','localtime')),
            PRIMARY KEY (ticker, trade_date)
        );
        CREATE INDEX IF NOT EXISTS idx_investor_ticker_date
            ON investor_trend(ticker, trade_date DESC);

        -- ── 웹 사용자 (나/친구 공유용) ──────────────────────────────
        CREATE TABLE IF NOT EXISTS users (
            username      TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            display_name  TEXT,
            created_at    TEXT DEFAULT (datetime('now','localtime'))
        );

        -- ── 시그널 로그 (run_signals.py 가 적재) ───────────────────
        -- payload_json: Signal dataclass 직렬화 (rule, title, detail, priority)
        CREATE TABLE IF NOT EXISTS signals (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date    TEXT NOT NULL,           -- YYYY-MM-DD
            ticker       TEXT NOT NULL,
            name         TEXT,
            rule         TEXT NOT NULL,
            title        TEXT,
            detail       TEXT,
            priority     TEXT,                    -- 🔴/🟠/🟡
            created_at   TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_signals_date
            ON signals(scan_date DESC, ticker);

        -- ── 시장 경고 브리핑 (run_market_warning.py) ───────────────
        CREATE TABLE IF NOT EXISTS market_warnings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            asof        TEXT NOT NULL,            -- YYYY-MM-DD HH:MM
            body        TEXT NOT NULL,            -- LLM 출력 본문 (markdown)
            fg_score    REAL,                    -- 추출되면 저장
            fg_rating   TEXT,
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_warnings_date
            ON market_warnings(asof DESC);

        -- ── 분석 차트 파일 메타 (실제 PNG는 data/charts/ 아래) ─────
        CREATE TABLE IF NOT EXISTS chart_files (
            analysis_id  INTEGER NOT NULL,
            interval     TEXT NOT NULL,            -- D/W/M/E/E_W/E_M
            file_path    TEXT NOT NULL,            -- 프로젝트 root 기준 상대경로
            created_at   TEXT DEFAULT (datetime('now','localtime')),
            PRIMARY KEY (analysis_id, interval),
            FOREIGN KEY (analysis_id) REFERENCES analysis_log(id) ON DELETE CASCADE
        );

        -- ── 자연어 시그널 정의 (사용자가 자연어로 등록, batch/즉시 실행) ──
        CREATE TABLE IF NOT EXISTS nl_signals (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            name             TEXT NOT NULL,
            prompt           TEXT NOT NULL,
            scope            TEXT NOT NULL DEFAULT 'portfolio',  -- portfolio|universe
            enabled          INTEGER NOT NULL DEFAULT 1,         -- morning batch 포함 여부
            created_at       TEXT NOT NULL,
            last_run_at      TEXT,
            last_match_count INTEGER
        );

        -- ── 종목 스냅샷 (NL 스크리너 캐시) ───────────────────────────
        -- 일봉 60봉 → 지표 + 최근 5봉을 JSON 으로 캐싱.
        -- 캔들 update 후 첫 접근 시 갱신 (last_date != 최신 일봉 날짜일 때).
        CREATE TABLE IF NOT EXISTS ticker_snapshot (
            ticker      TEXT PRIMARY KEY,
            last_date   TEXT NOT NULL,   -- 최근 일봉 날짜
            payload     TEXT NOT NULL,   -- JSON
            updated_at  TEXT NOT NULL
        );

        -- ── 배치 실행 로그 (UI 에 지연·에러 배너 노출용) ─────────────
        CREATE TABLE IF NOT EXISTS batch_runs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,        -- etf_screen / kr_evening / morning_brief / ...
            started_at  TEXT NOT NULL,        -- KST
            finished_at TEXT,                 -- KST (NULL=running)
            status      TEXT,                 -- running / success / failure / partial
            message     TEXT                  -- 요약 / 에러 메세지
        );
        CREATE INDEX IF NOT EXISTS idx_batch_runs_name
            ON batch_runs(name, started_at DESC);
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

    # 월봉은 KIS API가 그 달의 '마지막 거래일'을 date로 반환한다.
    # 진행 중인 달을 매일 호출하면 매일 다른 date(=호출 당일)로 새 행이 INSERT 되어
    # UNIQUE(ticker, interval, date) 가 중복을 차단하지 못한다.
    # → interval='M' 행은 항상 'YYYY-MM-01' 로 정규화해서 한 달에 한 행만 유지.
    def _norm(idx) -> str:
        if interval == "M":
            return f"{idx.year:04d}-{idx.month:02d}-01"
        return str(idx.date())

    rows = [
        (ticker, interval, _norm(idx),
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


# ── 투자자 매매동향 CRUD ─────────────────────────────────────────────

def upsert_investor_trend(rows: list) -> int:
    """
    rows: [(ticker, trade_date, foreign_amt, inst_amt), ...]
    trade_date 포맷 'YYYY-MM-DD'. 기본키 (ticker, trade_date) 충돌 시 덮어씀.
    """
    if not rows:
        return 0
    with get_conn(sync_after=True) as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO investor_trend
                (ticker, trade_date, foreign_amt, inst_amt)
            VALUES (?,?,?,?)
        """, rows)
    return len(rows)


def load_investor_trend(ticker: str, days: int = 30) -> list:
    """해당 ticker 최신 N영업일 행. [{trade_date, foreign_amt, inst_amt}, ...]
    최신일자부터 역순."""
    with get_conn() as conn:
        cur = conn.execute("""
            SELECT trade_date, foreign_amt, inst_amt
            FROM   investor_trend
            WHERE  ticker=?
            ORDER  BY trade_date DESC
            LIMIT  ?
        """, (ticker, int(days)))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ── 자연어 시그널 (NL signals) ──────────────────────────────────────

def create_nl_signal(name: str, prompt: str, scope: str = "portfolio",
                     enabled: bool = True) -> int:
    """자연어 시그널 저장. 새 id 반환."""
    scope = scope if scope in ("portfolio", "universe") else "portfolio"
    with get_conn(sync_after=True) as conn:
        cur = conn.execute("""
            INSERT INTO nl_signals (name, prompt, scope, enabled, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (name, prompt, scope, 1 if enabled else 0, _now_kst()))
        rid = cur.lastrowid
        if rid is None:
            row = conn.execute(
                "SELECT id FROM nl_signals WHERE name=? ORDER BY id DESC LIMIT 1",
                (name,),
            ).fetchone()
            rid = row[0] if row else 0
    return int(rid)


def list_nl_signals() -> list[dict]:
    with get_conn() as conn:
        cur = conn.execute("""
            SELECT id, name, prompt, scope, enabled, created_at,
                   last_run_at, last_match_count
            FROM nl_signals
            ORDER BY id DESC
        """)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def get_nl_signal(signal_id: int) -> dict | None:
    with get_conn() as conn:
        cur = conn.execute("""
            SELECT id, name, prompt, scope, enabled, created_at,
                   last_run_at, last_match_count
            FROM nl_signals WHERE id=?
        """, (signal_id,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


def update_nl_signal_run(signal_id: int, match_count: int) -> None:
    with get_conn(sync_after=True) as conn:
        conn.execute("""
            UPDATE nl_signals SET last_run_at=?, last_match_count=? WHERE id=?
        """, (_now_kst(), int(match_count), signal_id))


def set_nl_signal_enabled(signal_id: int, enabled: bool) -> None:
    with get_conn(sync_after=True) as conn:
        conn.execute("UPDATE nl_signals SET enabled=? WHERE id=?",
                     (1 if enabled else 0, signal_id))


def update_nl_signal(signal_id: int, name: str, prompt: str,
                      scope: str = "portfolio", enabled: bool = True) -> None:
    """저장된 NL 시그널 편집."""
    scope = scope if scope in ("portfolio", "universe") else "portfolio"
    with get_conn(sync_after=True) as conn:
        conn.execute("""
            UPDATE nl_signals
            SET name=?, prompt=?, scope=?, enabled=?
            WHERE id=?
        """, (name, prompt, scope, 1 if enabled else 0, signal_id))


def delete_nl_signal(signal_id: int) -> None:
    with get_conn(sync_after=True) as conn:
        conn.execute("DELETE FROM nl_signals WHERE id=?", (signal_id,))


# ── 종목 스냅샷 (NL 스크리너 인디케이터 캐시) ─────────────────────────

def save_ticker_snapshot(ticker: str, last_date: str, payload: dict) -> None:
    """ticker_snapshot upsert. payload 는 dict — JSON 직렬화."""
    import json
    with get_conn(sync_after=True) as conn:
        conn.execute("""
            INSERT INTO ticker_snapshot (ticker, last_date, payload, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                last_date  = excluded.last_date,
                payload    = excluded.payload,
                updated_at = excluded.updated_at
        """, (ticker, last_date,
              json.dumps(payload, ensure_ascii=False),
              _now_kst()))


def load_ticker_snapshot(ticker: str) -> dict | None:
    """반환: {last_date, payload(dict), updated_at} or None."""
    import json
    with get_conn() as conn:
        row = conn.execute("""
            SELECT last_date, payload, updated_at
            FROM ticker_snapshot WHERE ticker=?
        """, (ticker,)).fetchone()
    if not row:
        return None
    try:
        return {
            "last_date":  row[0],
            "payload":    json.loads(row[1]),
            "updated_at": row[2],
        }
    except Exception:
        return None


# ── 배치 실행 로그 ───────────────────────────────────────────────────

def batch_start(name: str) -> int:
    """배치 실행 시작 기록 → row id 반환."""
    with get_conn(sync_after=True) as conn:
        cur = conn.execute("""
            INSERT INTO batch_runs (name, started_at, status)
            VALUES (?, ?, 'running')
        """, (name, _now_kst()))
        rid = cur.lastrowid
        if rid is None:
            row = conn.execute(
                "SELECT id FROM batch_runs WHERE name=? ORDER BY id DESC LIMIT 1",
                (name,),
            ).fetchone()
            rid = row[0] if row else 0
    return int(rid)


def batch_finish(run_id: int, status: str, message: str = "") -> None:
    """배치 종료 — status: success|failure|partial."""
    if status not in ("success", "failure", "partial"):
        status = "failure"
    with get_conn(sync_after=True) as conn:
        conn.execute("""
            UPDATE batch_runs
            SET finished_at=?, status=?, message=?
            WHERE id=?
        """, (_now_kst(), status, (message or "")[:1000], run_id))


def latest_batch_run(name: str) -> dict | None:
    """가장 최근 run row → {id, name, started_at, finished_at, status, message}."""
    with get_conn() as conn:
        cur = conn.execute("""
            SELECT id, name, started_at, finished_at, status, message
            FROM batch_runs
            WHERE name=?
            ORDER BY started_at DESC, id DESC
            LIMIT 1
        """, (name,))
        row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0], "name": row[1],
        "started_at": row[2], "finished_at": row[3],
        "status": row[4], "message": row[5],
    }


# ── 분석 로그 ────────────────────────────────────────────────────────

def save_analysis(ticker: str, result_text: str) -> int:
    """analysis_log 삽입 후 새 row의 id 반환 (chart_files 연결용)."""
    with get_conn(sync_after=True) as conn:
        cur = conn.execute("""
            INSERT INTO analysis_log (ticker, analyzed_at, result_text)
            VALUES (?, ?, ?)
        """, (ticker, _now_kst(), result_text))
        # libsql / sqlite3 양쪽에서 lastrowid 동작
        rid = cur.lastrowid
        if rid is None:
            row = conn.execute(
                "SELECT id FROM analysis_log WHERE ticker=? ORDER BY id DESC LIMIT 1",
                (ticker,),
            ).fetchone()
            rid = row[0] if row else 0
    return int(rid)


def mark_sent(ticker: str):
    with get_conn(sync_after=True) as conn:
        conn.execute("""
            UPDATE analysis_log SET sent_telegram=1
            WHERE ticker=? AND sent_telegram=0
        """, (ticker,))


# ── 웹 사용자 ────────────────────────────────────────────────────────

def upsert_user(username: str, password_hash: str, display_name: str | None = None):
    """username PK 기준 upsert. 비번 변경 시도 동일 함수로."""
    with get_conn(sync_after=True) as conn:
        conn.execute("""
            INSERT INTO users (username, password_hash, display_name)
            VALUES (?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
                password_hash = excluded.password_hash,
                display_name  = COALESCE(excluded.display_name, users.display_name)
        """, (username, password_hash, display_name))


def load_user(username: str) -> dict | None:
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT username, password_hash, display_name FROM users WHERE username=?",
            (username,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "username":      row[0],
            "password_hash": row[1],
            "display_name":  row[2] or row[0],
        }


# ── 시그널 로그 ──────────────────────────────────────────────────────

def save_signals(scan_date: str, signals: list) -> int:
    """signals: list[Signal dataclass-like]. (rule/title/detail/priority/ticker/name 속성)"""
    if not signals:
        return 0
    rows = [
        (scan_date, s.ticker, s.name, s.rule, s.title, s.detail, s.priority)
        for s in signals
    ]
    with get_conn(sync_after=True) as conn:
        conn.executemany("""
            INSERT INTO signals (scan_date, ticker, name, rule, title, detail, priority)
            VALUES (?,?,?,?,?,?,?)
        """, rows)
    return len(rows)


def load_signals_grouped_by_date(limit_days: int = 30) -> list[dict]:
    """최근 N일치 시그널 — 날짜별로 그룹핑된 리스트 반환.
    반환: [{"scan_date": ..., "items": [{ticker,name,rule,title,detail,priority}, ...]}, ...]"""
    with get_conn() as conn:
        cur = conn.execute("""
            SELECT scan_date, ticker, name, rule, title, detail, priority
            FROM signals
            WHERE scan_date >= date('now', ?)
            ORDER BY scan_date DESC, priority ASC, ticker ASC
        """, (f"-{int(limit_days)} days",))
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    grouped: dict[str, list] = {}
    for r in rows:
        grouped.setdefault(r["scan_date"], []).append(r)
    return [{"scan_date": d, "items": items} for d, items in grouped.items()]


# ── 시장 경고 ────────────────────────────────────────────────────────

def save_market_warning(asof: str, body: str, fg_score: float | None = None,
                         fg_rating: str | None = None):
    with get_conn(sync_after=True) as conn:
        conn.execute("""
            INSERT INTO market_warnings (asof, body, fg_score, fg_rating)
            VALUES (?,?,?,?)
        """, (asof, body, fg_score, fg_rating))


def load_market_warnings(limit: int = 30) -> list[dict]:
    with get_conn() as conn:
        cur = conn.execute("""
            SELECT id, asof, body, fg_score, fg_rating
            FROM market_warnings
            ORDER BY asof DESC LIMIT ?
        """, (int(limit),))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# ── 분석 리포트 (웹용 조회) ──────────────────────────────────────────

def list_analyses(limit: int = 200) -> list[dict]:
    """최근 분석 리포트 목록. ticker별 최신부터."""
    with get_conn() as conn:
        cur = conn.execute("""
            SELECT id, ticker, analyzed_at, substr(result_text, 1, 200) AS preview
            FROM analysis_log
            ORDER BY analyzed_at DESC
            LIMIT ?
        """, (int(limit),))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def latest_analysis_per_ticker() -> dict[str, dict]:
    """ticker → {id, analyzed_at, preview} (가장 최근 1건). 없으면 키 없음.
    universe 종목 그리드에서 각 종목의 최신 분석 링크 만들 때 사용.
    """
    with get_conn() as conn:
        # 각 ticker 의 max(analyzed_at) row 만 추출.
        cur = conn.execute("""
            SELECT a.ticker, a.id, a.analyzed_at,
                   substr(a.result_text, 1, 160) AS preview
            FROM analysis_log a
            JOIN (
                SELECT ticker, MAX(analyzed_at) AS m
                FROM analysis_log
                GROUP BY ticker
            ) t ON t.ticker = a.ticker AND t.m = a.analyzed_at
        """)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return {r["ticker"]: r for r in rows}


def load_analysis(analysis_id: int) -> dict | None:
    with get_conn() as conn:
        cur = conn.execute("""
            SELECT id, ticker, analyzed_at, result_text
            FROM analysis_log WHERE id=?
        """, (int(analysis_id),))
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        rec = dict(zip(cols, row))
        # 차트 파일 첨부
        cur2 = conn.execute("""
            SELECT interval, file_path FROM chart_files
            WHERE analysis_id=? ORDER BY interval
        """, (int(analysis_id),))
        rec["charts"] = [
            {"interval": r[0], "file_path": r[1]} for r in cur2.fetchall()
        ]
        return rec


# ── 차트 파일 메타 ───────────────────────────────────────────────────

def save_chart_files(analysis_id: int, charts: dict[str, str]):
    """charts: {interval: file_path}. file_path 는 프로젝트 root 기준 상대경로 권장."""
    if not charts:
        return
    rows = [(analysis_id, interval, path) for interval, path in charts.items()]
    with get_conn(sync_after=True) as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO chart_files (analysis_id, interval, file_path)
            VALUES (?,?,?)
        """, rows)


def cleanup_old_charts(keep_days: int = 30) -> int:
    """N일 이전 차트 파일 + chart_files 메타 + 빈 디렉토리 삭제. 삭제 파일 수 반환."""
    from pathlib import Path
    import datetime as _dt

    cutoff = _dt.date.today() - _dt.timedelta(days=int(keep_days))
    deleted = 0

    # 1) 메타 + 파일 삭제
    with get_conn(sync_after=True) as conn:
        cur = conn.execute("""
            SELECT cf.analysis_id, cf.interval, cf.file_path
            FROM chart_files cf
            JOIN analysis_log al ON al.id = cf.analysis_id
            WHERE date(al.analyzed_at) < date(?)
        """, (cutoff.isoformat(),))
        rows = cur.fetchall()
        for aid, interval, fpath in rows:
            try:
                p = Path(__file__).parent / fpath
                if p.exists():
                    p.unlink()
                    deleted += 1
            except Exception as e:
                print(f"[charts] {fpath} 삭제 실패: {e}")
            conn.execute(
                "DELETE FROM chart_files WHERE analysis_id=? AND interval=?",
                (aid, interval),
            )

    # 2) 빈 날짜 디렉토리 정리
    charts_root = Path(__file__).parent / "data" / "charts"
    if charts_root.exists():
        for d in charts_root.iterdir():
            if d.is_dir() and not any(d.iterdir()):
                try:
                    d.rmdir()
                except Exception:
                    pass

    return deleted

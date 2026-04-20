"""
database.py - SQLite 스키마 정의 및 CRUD
테이블: candles / dart_disclosures / dart_reports / analysis_log
"""

import sqlite3
import pandas as pd
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime

DB_PATH = Path(__file__).parent / "data" / "stock_agent.db"


def init_db():
    """DB 및 테이블 초기화 (없으면 생성)"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_conn() as conn:
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
    print(f"[DB] 초기화 완료: {DB_PATH}")


@contextmanager
def get_conn():
    """SQLite 커넥션 컨텍스트 매니저"""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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
    with get_conn() as conn:
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
    sql += f" ORDER BY date DESC LIMIT {limit}"

    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=params, parse_dates=["date"])

    if df.empty:
        return df
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
    with get_conn() as conn:
        conn.executemany("""
            INSERT OR IGNORE INTO dart_disclosures
                (rcept_no, ticker, corp_name, report_nm, rcept_dt, rm, flr_nm)
            VALUES (:rcept_no,:ticker,:corp_name,:report_nm,:rcept_dt,:rm,:flr_nm)
        """, rows)
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
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def upsert_report(data: dict):
    with get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO dart_reports
                (rcept_no, ticker, report_type, period_end,
                 revenue, op_income, net_income,
                 total_assets, total_equity,
                 per, pbr, roe, debt_ratio, summary_text)
            VALUES
                (:rcept_no,:ticker,:report_type,:period_end,
                 :revenue,:op_income,:net_income,
                 :total_assets,:total_equity,
                 :per,:pbr,:roe,:debt_ratio,:summary_text)
        """, data)


def load_latest_report(ticker: str):
    with get_conn() as conn:
        row = conn.execute("""
            SELECT * FROM dart_reports
            WHERE ticker=?
            ORDER BY period_end DESC LIMIT 1
        """, (ticker,)).fetchone()
    return dict(row) if row else None


# ── 분석 로그 ────────────────────────────────────────────────────────

def save_analysis(ticker: str, result_text: str):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO analysis_log (ticker, analyzed_at, result_text)
            VALUES (?, datetime('now','localtime'), ?)
        """, (ticker, result_text))


def mark_sent(ticker: str):
    with get_conn() as conn:
        conn.execute("""
            UPDATE analysis_log SET sent_telegram=1
            WHERE ticker=? AND sent_telegram=0
        """, (ticker,))

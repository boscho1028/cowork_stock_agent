"""
sec_collector.py - SEC EDGAR 공식 API 공시 수집
API 키 불필요, 완전 무료 (data.sec.gov)

수집 대상:
  8-K   주요사건 즉시공시 (실적·M&A·CEO교체·소송 등)  ← 핵심
  10-K  연간보고서
  10-Q  분기보고서
  Form4 내부자거래 (임원 매수/매도)                   ← 투자 시그널

API 엔드포인트:
  티커→CIK: https://www.sec.gov/files/company_tickers.json
  공시목록:  https://data.sec.gov/submissions/CIK{10자리}.json
  재무데이터: https://data.sec.gov/api/xbrl/companyfacts/CIK{10자리}.json
"""

import time
import json
import requests
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import contextmanager

# DB 경로 (database.py와 동일 위치)
DB_PATH = Path(__file__).parent / "data" / "stock_agent.db"

# SEC EDGAR API (인증 불필요)
BASE_URL    = "https://data.sec.gov"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

# SEC EDGAR API 정책:
# - User-Agent 에 이름+이메일 포함 필수
# - Host 헤더는 절대 직접 설정하지 말 것 (requests가 URL에서 자동 설정)
# - 초과 시 429, Host 헤더 수동 지정 시 403 발생
SEC_USER_AGENT = "StockAnalysisAgent contact@stockagent.com"

# 모든 요청에 공통 사용 (Host 헤더 없음)
HEADERS = {
    "User-Agent": SEC_USER_AGENT,
}

# 하위 호환
TICKERS_HEADERS = HEADERS
DATA_HEADERS    = HEADERS

# 중요 공시 유형 및 중요도
FORM_PRIORITY = {
    "8-K":   "🔴",   # 주요사건 즉시공시
    "10-K":  "🔵",   # 연간보고서
    "10-Q":  "🔵",   # 분기보고서
    "4":     "🟡",   # 내부자거래 (Form 4)
    "SC 13G":"🟡",   # 5% 이상 지분 보고
    "SC 13D":"🟠",   # 5% 이상 지분 + 경영 참여
    "DEF 14A":"🔵",  # 주주총회 위임장
    "S-1":   "🟠",   # 신주 발행 (희석 위험)
}

# 8-K 세부 아이템 → 중요도 매핑
ITEM_KEYWORDS = {
    "🔴": ["결과", "실적", "earnings", "results", "revenue",
           "merger", "합병", "acquisition", "인수",
           "bankruptcy", "파산", "restatement", "재작성",
           "CEO", "CFO", "officer", "director", "임원"],
    "🟠": ["lawsuit", "소송", "settlement", "합의",
           "SEC investigation", "조사", "fine", "penalty",
           "dividend", "배당", "buyback", "자사주"],
    "🟡": ["agreement", "계약", "partnership", "파트너십",
           "guidance", "전망", "outlook"],
}

# 제외할 일상적 8-K 아이템 (투자 판단에 덜 중요)
EXCLUDE_ITEMS = ["8.01", "7.01"]  # Other Events, Regulation FD


@contextmanager
def _get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _init_sec_tables():
    """SEC 공시 테이블 생성 (없으면)"""
    with _get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS sec_filings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT NOT NULL,
            cik         TEXT NOT NULL,
            accession   TEXT UNIQUE NOT NULL,   -- SEC 고유 접수번호
            form_type   TEXT NOT NULL,          -- 8-K / 10-K / 10-Q / 4 등
            filed_date  TEXT NOT NULL,          -- YYYY-MM-DD
            description TEXT,                  -- 공시 제목/설명
            items       TEXT,                  -- 8-K 아이템 번호 (쉼표 구분)
            importance  TEXT DEFAULT '🔵',     -- 🔴🟠🟡🔵
            url         TEXT,                  -- EDGAR 직접 링크
            created_at  TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_sec_ticker_date
            ON sec_filings(ticker, filed_date DESC);
        CREATE INDEX IF NOT EXISTS idx_sec_form
            ON sec_filings(ticker, form_type, filed_date DESC);

        CREATE TABLE IF NOT EXISTS sec_cik_map (
            ticker     TEXT PRIMARY KEY,
            cik        TEXT NOT NULL,
            cik_padded TEXT NOT NULL,           -- 10자리 0패딩
            company    TEXT,
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        );
        """)


class SECCollector:
    """
    SEC EDGAR 공식 REST API 래퍼
    - 티커 → CIK 매핑 (로컬 캐시)
    - 8-K / 10-K / 10-Q / Form 4 수집
    - 중요도 자동 분류
    - DB 저장 및 요약 텍스트 생성
    """

    def __init__(self):
        self.sess = requests.Session()
        # Host 헤더는 절대 직접 설정하지 않음 — requests가 URL에서 자동 설정
        self.sess.headers.update({"User-Agent": SEC_USER_AGENT})
        self._cik_cache: dict = {}
        _init_sec_tables()
        self._load_cik_cache()

    # ── CIK 관리 ─────────────────────────────────────────────────────

    def _load_cik_cache(self):
        """DB에서 CIK 캐시 로드"""
        with _get_conn() as conn:
            rows = conn.execute(
                "SELECT ticker, cik_padded FROM sec_cik_map"
            ).fetchall()
        self._cik_cache = {r["ticker"]: r["cik_padded"] for r in rows}

    def _get_cik(self, ticker: str) -> str | None:
        """티커 → CIK 10자리 반환 (캐시 우선, 없으면 API 조회)"""
        ticker_upper = ticker.upper()

        if ticker_upper in self._cik_cache:
            return self._cik_cache[ticker_upper]

        # SEC 전체 티커 맵 다운로드 (약 1MB, 한 번만 받음)
        # www.sec.gov 는 별도 Host 헤더 필요
        try:
            resp = self.sess.get(TICKERS_URL, timeout=15)
            if resp.status_code != 200:
                print(f"  [SEC] CIK 조회 HTTP {resp.status_code}")
                return None
            resp.encoding = "utf-8"
            data = resp.json()
        except Exception as e:
            print(f"  [SEC] CIK 조회 실패: {e}")
            return None

        # {0: {cik_str, entity_type, ticker, title}, ...}
        found = {}
        for item in data.values():
            t = item.get("ticker", "").upper()
            cik_raw = str(item.get("cik_str", ""))
            cik_padded = cik_raw.zfill(10)
            found[t] = {
                "cik":     cik_raw,
                "padded":  cik_padded,
                "company": item.get("title", ""),
            }

            # 전체를 DB에 저장 (한 번만 다운로드)
        with _get_conn() as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO sec_cik_map
                   (ticker, cik, cik_padded, company)
                   VALUES (?, ?, ?, ?)""",
                [(t, v["cik"], v["padded"], v["company"])
                 for t, v in found.items()]
            )
        self._cik_cache = {t: v["padded"] for t, v in found.items()}

        result = self._cik_cache.get(ticker_upper)
        if not result:
            print(f"  [SEC] {ticker}: CIK를 찾을 수 없음 (미국 상장 종목인지 확인)")
        return result

    # ── 공시 수집 ─────────────────────────────────────────────────────

    def fetch_filings(
        self,
        ticker: str,
        days_back: int = 3,
        forms: list = None,
    ) -> list:
        """
        종목의 최근 공시 수집 → DB 저장

        ticker   : 미국 주식 티커 (AAPL, NVDA 등)
        days_back: 몇 일 전까지 (T-1 기본값 3)
        forms    : 수집할 공시 유형 (None = 전체)
        """
        cik = self._get_cik(ticker)
        if not cik:
            return []

        target_forms = forms or list(FORM_PRIORITY.keys())
        cutoff_date  = (datetime.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        # EDGAR 제출 이력 조회 (data.sec.gov)
        url = f"{BASE_URL}/submissions/CIK{cik}.json"
        try:
            resp = self.sess.get(url, timeout=15)
            if resp.status_code != 200:
                print(f"  [SEC] {ticker} 공시 조회 HTTP {resp.status_code}")
                return []
            if not resp.text or not resp.text.strip():
                print(f"  [SEC] {ticker}: 빈 응답")
                return []
            resp.encoding = "utf-8"
            data = resp.json()
        except Exception as e:
            print(f"  [SEC] {ticker} 공시 조회 실패: {e}")
            return []

        recent = data.get("filings", {}).get("recent", {})
        if not recent:
            return []

        # 필드 추출
        accessions  = recent.get("accessionNumber", [])
        form_types  = recent.get("form", [])
        filed_dates = recent.get("filingDate", [])
        descriptions= recent.get("primaryDocument", [])
        items_list  = recent.get("items", [])          # 8-K 아이템

        collected = []
        for i, acc in enumerate(accessions):
            try:
                form      = form_types[i] if i < len(form_types) else ""
                filed     = filed_dates[i] if i < len(filed_dates) else ""
                desc      = descriptions[i] if i < len(descriptions) else ""
                items_str = items_list[i]   if i < len(items_list)  else ""
            except IndexError:
                continue

            # 날짜 필터
            if filed < cutoff_date:
                break   # 날짜 내림차순이므로 이후는 모두 오래됨

            # 유형 필터
            if form not in target_forms:
                continue

            # 8-K 필터: 중요하지 않은 아이템 제외
            if form == "8-K" and items_str:
                item_nums = [x.strip() for x in str(items_str).split(",")]
                if all(n in EXCLUDE_ITEMS for n in item_nums if n):
                    continue

            # 중요도 판단
            importance = self._classify_importance(form, desc, items_str)

            # EDGAR 링크
            acc_clean = acc.replace("-", "")
            edgar_url = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{int(cik)}/{acc_clean}/{desc}"
            )

            collected.append({
                "ticker":      ticker,
                "cik":         cik,
                "accession":   acc,
                "form_type":   form,
                "filed_date":  filed,
                "description": desc,
                "items":       str(items_str),
                "importance":  importance,
                "url":         edgar_url,
            })

        # DB 저장
        if collected:
            with _get_conn() as conn:
                conn.executemany("""
                    INSERT OR IGNORE INTO sec_filings
                        (ticker, cik, accession, form_type, filed_date,
                         description, items, importance, url)
                    VALUES
                        (:ticker, :cik, :accession, :form_type, :filed_date,
                         :description, :items, :importance, :url)
                """, collected)
            print(f"  [SEC] {ticker}: {len(collected)}건 저장 ({cutoff_date} 이후)")

        time.sleep(0.2)   # SEC rate limit 준수 (초당 10회)
        return collected

    def fetch_all_tickers(self, tickers: list, days_back: int = 3):
        """포트폴리오 전체 해외 종목 공시 수집"""
        overseas_tickers = [
            t for t in tickers if _is_overseas(t)
        ]
        if not overseas_tickers:
            return

        print(f"\n[SEC] {len(overseas_tickers)}개 해외 종목 공시 수집...")
        for ticker in overseas_tickers:
            self.fetch_filings(ticker, days_back=days_back)

    def fetch_initial(self, tickers: list, days_back: int = 365):
        """초기 적재: 최근 1년치 공시"""
        overseas_tickers = [t for t in tickers if _is_overseas(t)]
        if not overseas_tickers:
            return
        print(f"\n[SEC] 초기 적재: {len(overseas_tickers)}개 종목 × {days_back}일")
        for ticker in overseas_tickers:
            print(f"  [{ticker}] SEC 공시 수집 중...")
            self.fetch_filings(ticker, days_back=days_back)
            time.sleep(0.5)

    # ── 공시 요약 텍스트 생성 (AI 프롬프트용) ────────────────────────

    def get_filing_summary(self, ticker: str, limit: int = 5) -> str:
        """DB에서 최근 공시 → 프롬프트용 텍스트"""
        with _get_conn() as conn:
            rows = conn.execute("""
                SELECT form_type, filed_date, description, items, importance
                FROM   sec_filings
                WHERE  ticker = ?
                ORDER  BY filed_date DESC
                LIMIT  ?
            """, (ticker.upper(), limit)).fetchall()

        if not rows:
            return "최근 SEC 공시 없음"

        lines = []
        for r in rows:
            date_str = r["filed_date"]
            form     = r["form_type"]
            items    = r["items"] or ""
            imp      = r["importance"] or "🔵"
            desc     = r["description"] or ""

            # 8-K 아이템 번호를 사람이 읽기 쉬운 이름으로
            item_label = _item_label(items) if form == "8-K" and items else ""
            suffix = f" [{item_label}]" if item_label else ""

            lines.append(f"{imp} {date_str}  {form}{suffix}")

        return "\n".join(lines)

    # ── 중요도 분류 ───────────────────────────────────────────────────

    @staticmethod
    def _classify_importance(form: str, desc: str, items: str) -> str:
        # Form 기본 중요도
        base = FORM_PRIORITY.get(form, "🔵")

        if form == "8-K":
            combined = (desc + " " + str(items)).lower()
            for imp, keywords in ITEM_KEYWORDS.items():
                if any(kw.lower() in combined for kw in keywords):
                    return imp

        if form in ("SC 13D", "S-1"):
            return "🟠"
        if form == "4":
            return "🟡"

        return base


# ── 헬퍼 ─────────────────────────────────────────────────────────────

def _is_overseas(ticker: str) -> bool:
    """config 없이 간단히 판단 (숫자 6자리면 국내)"""
    try:
        import config
        return config.is_overseas(ticker)
    except Exception:
        return not ticker.isdigit()


def _item_label(items_str: str) -> str:
    """8-K 아이템 번호 → 한국어 레이블"""
    ITEM_NAMES = {
        "1.01": "중요계약 체결",
        "1.02": "중요계약 종료",
        "1.03": "파산/법정관리",
        "2.01": "자산 취득/처분",
        "2.02": "실적발표",
        "2.03": "부채 발생",
        "2.04": "의무 촉진",
        "2.05": "임원 퇴임",
        "2.06": "자산 손상",
        "3.01": "상장폐지 통보",
        "3.02": "주식 미등록 매도",
        "4.01": "회계법인 교체",
        "4.02": "재무제표 신뢰 불가",
        "5.01": "지배구조 변경",
        "5.02": "임원 선임/해임",
        "5.03": "정관 변경",
        "5.07": "주주총회 결과",
        "5.08": "이사회 임기",
        "7.01": "Regulation FD",
        "8.01": "기타 사건",
        "9.01": "재무제표 첨부",
    }
    nums = [x.strip() for x in items_str.split(",") if x.strip()]
    labels = [ITEM_NAMES.get(n, n) for n in nums if n not in EXCLUDE_ITEMS]
    return ", ".join(labels) if labels else ""

"""
config.py - KIS API 버전
- universe.csv : 데이터 수집 대상 전체 (ticker, name, exchange)
- portfolio.csv: 매일 분석/리포트 대상 (ticker, name, quantity, exchange)
portfolio ⊆ universe (portfolio에만 있고 universe에 없는 종목은 자동 편입)
"""
import os
import csv
import time
import threading
from dotenv import load_dotenv

load_dotenv()

# ── AI 모델 ──────────────────────────────────────────────────────────
# Primary 선택 (claude | gemini). 기본 claude. 실패 시 반대 모델로 폴백.
AI_PRIMARY        = os.getenv("AI_PRIMARY", "claude").lower()

# Claude (Anthropic)
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL      = "claude-sonnet-4-5"

# Gemini (Google)
GOOGLE_API_KEY    = os.getenv("GOOGLE_API_KEY", "")
GEMINI_MODEL      = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
# Gemini 2.5 Flash 추론 토큰 예산. 0=OFF(가장 빠름·저품질), 512=가벼운 추론,
# 1024=중간(권장), 2048=Pro에 근접. 높일수록 품질↑ 지연·비용↑.
GEMINI_THINKING_BUDGET = int(os.getenv("GEMINI_THINKING_BUDGET", "1024"))

# ── 텔레그램 ─────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "")

# ── DART ─────────────────────────────────────────────────────────────
DART_API_KEY = os.getenv("DART_API_KEY", "")

# ── 네이버 뉴스 (모닝 브리핑 공시 보강) ──────────────────────────────
# https://developers.naver.com — 검색 API (25,000 req/day 무료)
# 값 미설정 시 모닝 브리핑은 뉴스 보강 없이 동작 (graceful degrade)
NAVER_CLIENT_ID     = os.getenv("NAVER_CLIENT_ID",     "")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "")

# ── KIS API ──────────────────────────────────────────────────────────
KIS_APP_KEY       = os.getenv("KIS_APP_KEY",       "")
KIS_APP_SECRET    = os.getenv("KIS_APP_SECRET",    "")
KIS_ACCOUNT_NO    = os.getenv("KIS_ACCOUNT_NO",    "")
KIS_PAPER_TRADING = os.getenv("KIS_PAPER_TRADING", "false").lower() == "true"

# KIS 토큰 공유 캐시 경로 (PC마다 Google Drive 마운트 경로가 다를 수 있음)
# .env 에 PC별로 KIS_TOKEN_CACHE_DIR 를 지정 — 미지정이면 후보 경로 자동 탐색
_TOKEN_DIR_CANDIDATES = [
    r"G:\내 드라이브\03_Cloud\KIS_token",
    r"H:\내 드라이브\03_Cloud\KIS_token",
    os.path.expanduser(r"~\Google Drive\내 드라이브\03_Cloud\KIS_token"),
    os.path.expanduser(r"~\My Drive\03_Cloud\KIS_token"),
]
KIS_TOKEN_CACHE_DIR = os.getenv("KIS_TOKEN_CACHE_DIR", "") or next(
    (p for p in _TOKEN_DIR_CANDIDATES if os.path.isdir(p)),
    _TOKEN_DIR_CANDIDATES[0],  # 어느 것도 없으면 첫 후보(생성 시도용)
)

# ── Turso (libSQL) ───────────────────────────────────────────────────
# embedded replica: 로컬 SQLite 파일을 읽고, 변경분만 Turso 클라우드와 동기화
# 두 PC가 같은 Turso DB에 쓰기·읽기하여 데이터 공유.
# URL/TOKEN 이 비어 있으면 database.py는 순수 로컬 모드로 동작(폴백).
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "")
TURSO_AUTH_TOKEN   = os.getenv("TURSO_AUTH_TOKEN",   "")

# ── 스케줄 ───────────────────────────────────────────────────────────
# 월~금 2회 배치.
#   · MORNING_BRIEF_TIME: 미국 포트폴리오 업데이트 + SEC 공시 + 한국 DART T-1
#     공시를 규칙 기반으로 요약 전송 (AI 호출 없음, 빠름)
#   · EVENING_ANALYZE_TIME: 한국 장 마감 후 종가·일봉 반영된 뒤
#     한국 포트폴리오 업데이트 + AI 분석 + 차트 전송
MORNING_BRIEF_TIME    = os.getenv("MORNING_BRIEF_TIME",    "07:30")
EVENING_ANALYZE_TIME  = os.getenv("EVENING_ANALYZE_TIME",  "17:00")
# 시장 경고 브리핑 (CNN F&G + 시세 + 신용/유동성·AI 우려 뉴스).
# 모닝 브리핑보다 5분 먼저 보내서 두 알림이 겹치지 않게 함.
MARKET_WARNING_TIME   = os.getenv("MARKET_WARNING_TIME",   "07:25")

# ── 기술적 지표 파라미터 ─────────────────────────────────────────────
# ── 시그널 규칙 파라미터 ─────────────────────────────────────────────
# signals.py 에서 참조. 임계값만 여기서 조정.
SIGNAL_CONFIG = {
    "rsi_oversold":        30,     # 과매도 기준
    "rsi_overbought":      70,     # 과매수 기준
    "volume_spike_mult":   3.0,    # 거래량 MA20 대비 배수
    "min_priority":        "🟡",   # 🔴 > 🟠 > 🟡 (이 미만은 전송 제외)
    "enabled": [
        "rsi_cross",
        "macd_cross",
        "ichimoku_cloud_break",
        "ichimoku_tk_cross",
        "ma10_weekly_break",
        "candle_reversal",
        "volume_spike",
        "disclosure",
    ],
}

INDICATOR_CONFIG = {
    "ma_periods":         [5, 20, 60, 120],
    "ma_periods_weekly":  [5, 10, 20, 60],
    "ma_periods_monthly": [5, 10, 20, 40],
    "rsi_period":         14,
    "rsi_oversold":       30,
    "rsi_overbought":     70,
    "macd_fast":          12,
    "macd_slow":          26,
    "macd_signal":         9,
    "bb_period":          20,
    "bb_std":              2,
    "atr_period":         14,
    "volume_ma_period":   20,
    "ichimoku_tenkan":     9,
    "ichimoku_kijun":     26,
    "ichimoku_span_b":    52,
    "ichimoku_offset":    26,
}

# ── 엘리엇 파동 (PoC: 추진 5파만, 일봉 기준) ─────────────────────────
# ZigZag 스윙 감지 → 5파 후보 → 4축(피보·거래량·RSI 다이버전스·추세선) 검증.
# A-B-C 조정파와 차트 시각화는 v2 로 분리.
ELLIOTT_CONFIG = {
    # ZigZag 파라미터 (둘 중 큰 값을 임계값으로 사용)
    "swing_min_pct":   3.0,   # 직전 스윙 대비 최소 변동률 (%)
    "swing_atr_mult":  2.0,   # ATR(14) 배수
    "min_bars":      120,     # 최소 캔들 수 (5파 사이클 1회 식별 가능)
    # 신선도 기준 — P5 가 마지막 봉으로부터 이 봉수 이내일 때만 매매 시그널로 인정.
    # 일봉 기준 60봉 ≈ 3개월. 이보다 오래된 5파는 현재 추세와 무관할 가능성이 높아
    # 차트와 리포트에서 모두 "검출 안 됨" 으로 처리.
    "max_p5_age":     60,
    # 점수 만점 (각 검증 축 raw score 상한)
    "max_fib":        75,
    "max_volume":     60,
    "max_rsi":        30,
    "max_trend":      30,
}

# 인터벌별 ELLIOTT_CONFIG 오버라이드 — get_elliott_config(interval) 로 병합.
# 일봉 3% 변동이 주봉/월봉에선 너무 작아 노이즈 스윙이 잡힘 → 임계값을 키우고
# min_bars / max_p5_age 도 시간프레임에 맞게 축소.
ELLIOTT_CONFIG_OVERRIDES = {
    "D": {},  # 기본값 그대로
    "W": {
        "swing_min_pct":  6.0,   # 주봉 1개당 변동이 일봉보다 큼
        "min_bars":      60,     # 60주 ≈ 14개월 (1.2년)
        "max_p5_age":    26,     # 26주 ≈ 6개월
    },
    "M": {
        "swing_min_pct": 12.0,   # 월봉은 더 큰 변동만 의미 있는 스윙
        "min_bars":      24,     # 24개월
        "max_p5_age":    12,     # 12개월
    },
}


def get_elliott_config(interval: str = "D") -> dict:
    """인터벌별 Elliott 설정 반환. 기본값에 인터벌 오버라이드 병합."""
    cfg = dict(ELLIOTT_CONFIG)
    cfg.update(ELLIOTT_CONFIG_OVERRIDES.get(interval, {}))
    return cfg

# ── 거래소 코드 매핑 (해외주식 KIS EXCD) ─────────────────────────────
# KIS 해외주식 거래소 코드
EXCHANGE_TO_EXCD = {
    "NASDAQ": "NAS",
    "NYSE":   "NYS",
    "AMEX":   "AMS",
    "TOKYO":  "TSE",
    "HONG_KONG": "HKS",
    "SHANGHAI":  "SHS",
    "SHENZHEN":  "SZS",
}

# ═══════════════════════════════════════════════════════════════════════
# 포트폴리오 CSV 관리 (평단가 없음)
# ═══════════════════════════════════════════════════════════════════════

_BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
CSV_PATH       = os.path.join(_BASE_DIR, "portfolio.csv")
UNIVERSE_PATH  = os.path.join(_BASE_DIR, "universe.csv")

_portfolio_detail: dict = {}
_portfolio_lock = threading.Lock()

_universe_detail: dict = {}
_universe_lock = threading.Lock()


def _load_csv(path: str, has_qty: bool) -> dict:
    """CSV 공용 로더. has_qty=True면 portfolio 스키마(quantity 포함)."""
    result = {}
    if not os.path.exists(path):
        return result
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ticker   = row.get("ticker",   "").strip()
                name     = row.get("name",     "").strip()
                exchange = row.get("exchange", "KRX").strip().upper()
                if not ticker:
                    continue
                if has_qty:
                    qty_raw = row.get("quantity", "0").strip() or "0"
                    qty = int(float(qty_raw))
                else:
                    qty = 0
                result[ticker] = {
                    "name":        name,
                    "qty":         qty,
                    "exchange":    exchange,
                    "is_overseas": exchange not in ("KRX", "KOSPI", "KOSDAQ"),
                }
    except Exception as e:
        print(f"[CSV] {path} 로드 실패: {e}")
    return result


def load_portfolio(path: str = CSV_PATH) -> dict:
    """portfolio.csv 읽기 (ticker, name, quantity, exchange)."""
    if not os.path.exists(path):
        print(f"[Portfolio] 파일 없음: {path}")
        return {}
    result = _load_csv(path, has_qty=True)
    exch_list = ", ".join(t + "(" + v["exchange"] + ")" for t, v in result.items())
    print("[Portfolio] " + str(len(result)) + "종목 로드: " + exch_list)
    return result


def load_universe(path: str = UNIVERSE_PATH) -> dict:
    """universe.csv 읽기 (ticker, name, exchange)."""
    if not os.path.exists(path):
        print(f"[Universe] 파일 없음: {path} (portfolio만 수집 대상이 됩니다)")
        return {}
    result = _load_csv(path, has_qty=False)
    print(f"[Universe] {len(result)}종목 로드")
    return result


def reload_portfolio():
    global _portfolio_detail
    new_data = load_portfolio()
    with _portfolio_lock:
        _portfolio_detail = new_data
    print(f"[Portfolio] 리로드 완료 → {len(_portfolio_detail)}종목")


def reload_universe():
    global _universe_detail
    new_data = load_universe()
    with _universe_lock:
        _universe_detail = new_data
    print(f"[Universe] 리로드 완료 → {len(_universe_detail)}종목")


def append_universe_row(ticker: str, name: str = "", exchange: str = None) -> bool:
    """universe.csv 에 종목 한 줄 추가. 이미 있으면 False, 추가하면 True.
    추가 직후 in-memory `_universe_detail` 도 갱신해 FileWatcher reload 전에도
    즉시 조회 가능.
    """
    ticker = ticker.upper()
    if exchange is None:
        exchange = "KRX" if ticker.isdigit() else "NASDAQ"
    exchange = exchange.upper()
    name = name or ticker

    with _universe_lock:
        if ticker in _universe_detail:
            return False
        created_new = not os.path.exists(UNIVERSE_PATH)
        mode = "w" if created_new else "a"
        with open(UNIVERSE_PATH, mode, encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            if created_new:
                w.writerow(["ticker", "name", "exchange"])
            w.writerow([ticker, name, exchange])
        _universe_detail[ticker] = {
            "name":        name,
            "qty":         0,
            "exchange":    exchange,
            "is_overseas": exchange not in ("KRX", "KOSPI", "KOSDAQ"),
        }
    return True


def get_portfolio_detail() -> dict:
    with _portfolio_lock:
        return dict(_portfolio_detail)


def get_portfolio() -> list:
    with _portfolio_lock:
        return list(_portfolio_detail.keys())


def get_universe_detail() -> dict:
    """Universe + portfolio 병합. portfolio-only 종목도 수집에 포함."""
    with _universe_lock:
        merged = dict(_universe_detail)
    with _portfolio_lock:
        for t, v in _portfolio_detail.items():
            if t not in merged:
                merged[t] = dict(v)
    return merged


def get_universe() -> list:
    return list(get_universe_detail().keys())


def _lookup(ticker: str) -> dict:
    """portfolio → universe 순서로 메타 조회 (없으면 빈 dict)."""
    with _portfolio_lock:
        info = _portfolio_detail.get(ticker)
    if info:
        return info
    with _universe_lock:
        info = _universe_detail.get(ticker)
    return info or {}


def is_overseas(ticker: str) -> bool:
    """
    해외 종목 여부
    - portfolio → universe → 티커 형태 자동 판단
    """
    info = _lookup(ticker)
    if info:
        return info.get("is_overseas", False)
    return not ticker.isdigit()


def register_temp(ticker: str, name: str = "", exchange: str = None):
    """
    portfolio/universe에 없는 종목을 임시로 등록 (분석용)
    exchange: 'KRX' | 'NASDAQ' | 'NYSE' | None(자동판단)
    """
    if exchange is None:
        exchange = "KRX" if ticker.isdigit() else "NASDAQ"
    with _portfolio_lock:
        _portfolio_detail[ticker] = {
            "name":        name or ticker,
            "qty":         0,
            "exchange":    exchange.upper(),
            "is_overseas": exchange.upper() not in ("KRX", "KOSPI", "KOSDAQ"),
        }


def get_excd(ticker: str) -> str:
    """KIS 해외거래소 코드 반환 (portfolio→universe→기본값 순으로 판단)"""
    info     = _lookup(ticker)
    exchange = info.get("exchange") if info else ("NASDAQ" if not ticker.isdigit() else "KRX")
    return EXCHANGE_TO_EXCD.get(exchange.upper(), "NAS")


# ── CSV 변경 감지 ────────────────────────────────────────────────────

class _FileWatcher(threading.Thread):
    """파일 mtime 변경을 감지해 reload 콜백을 호출."""
    def __init__(self, path: str, reload_fn, label: str):
        super().__init__(daemon=True)
        self.path      = path
        self.reload_fn = reload_fn
        self.label     = label
        self._last_mtime = self._mtime()

    def _mtime(self):
        try:
            return os.path.getmtime(self.path)
        except FileNotFoundError:
            return 0.0

    def run(self):
        print(f"[{self.label}] CSV 감시 시작")
        while True:
            time.sleep(2)
            m = self._mtime()
            if m != self._last_mtime:
                print(f"\n[{self.label}] 변경 감지! 리로드 중...")
                self.reload_fn()
                self._last_mtime = m


# ── 초기 로드 ────────────────────────────────────────────────────────
_portfolio_detail = load_portfolio()
_universe_detail  = load_universe()

_FileWatcher(CSV_PATH,      reload_portfolio, "Portfolio").start()
_FileWatcher(UNIVERSE_PATH, reload_universe,  "Universe").start()


# portfolio가 universe에 없는 경우 경고
_missing = [t for t in _portfolio_detail if t not in _universe_detail]
if _universe_detail and _missing:
    print(f"[WARN] portfolio에만 있고 universe에 없는 종목: {_missing} "
          f"(수집 시 자동 포함)")


# ── 하위 호환 프록시 ─────────────────────────────────────────────────
def _dict_proxy_factory(get_detail_fn):
    class _DictProxy:
        def __getitem__(self, key):
            info = get_detail_fn()[key]
            return (info["name"], info["qty"], 0)

        def get(self, key, default=None):
            info = get_detail_fn().get(key)
            if info is None:
                return default
            return (info["name"], info["qty"], 0)

        def keys(self):   return get_detail_fn().keys()
        def items(self):  return {k: (v["name"], v["qty"], 0) for k, v in get_detail_fn().items()}.items()
        def __len__(self): return len(get_detail_fn())
        def __contains__(self, key): return key in get_detail_fn()
        def __repr__(self): return repr(get_detail_fn())
    return _DictProxy()


def _list_proxy_factory(get_list_fn):
    class _ListProxy(list):
        def __iter__(self):       return iter(get_list_fn())
        def __len__(self):        return len(get_list_fn())
        def __getitem__(self, i): return get_list_fn()[i]
        def __repr__(self):       return repr(get_list_fn())
    return _ListProxy()


PORTFOLIO_DETAIL = _dict_proxy_factory(get_portfolio_detail)
PORTFOLIO        = _list_proxy_factory(get_portfolio)
UNIVERSE_DETAIL  = _dict_proxy_factory(get_universe_detail)
UNIVERSE         = _list_proxy_factory(get_universe)

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

# ── Claude AI ────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL      = "claude-sonnet-4-5"

# ── 텔레그램 ─────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "")

# ── DART ─────────────────────────────────────────────────────────────
DART_API_KEY = os.getenv("DART_API_KEY", "")

# ── KIS API ──────────────────────────────────────────────────────────
KIS_APP_KEY       = os.getenv("KIS_APP_KEY",       "")
KIS_APP_SECRET    = os.getenv("KIS_APP_SECRET",    "")
KIS_ACCOUNT_NO    = os.getenv("KIS_ACCOUNT_NO",    "")
KIS_PAPER_TRADING = os.getenv("KIS_PAPER_TRADING", "false").lower() == "true"

# ── 스케줄 ───────────────────────────────────────────────────────────
SCHEDULE_TIME = "08:00"

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

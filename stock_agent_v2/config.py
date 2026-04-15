"""
config.py - KIS API 버전
포트폴리오: portfolio.csv 로 관리 (ticker, name, quantity, exchange)
평단가 제외
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

CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portfolio.csv")

_portfolio_detail: dict = {}
_portfolio_lock = threading.Lock()


def load_portfolio(path: str = CSV_PATH) -> dict:
    """
    portfolio.csv 읽기
    형식: ticker, name, quantity, exchange
    반환: {
        "005930": {"name":"삼성전자", "qty":100, "exchange":"KRX"},
        "AAPL":   {"name":"애플",     "qty":10,  "exchange":"NASDAQ"},
    }
    """
    result = {}
    if not os.path.exists(path):
        print(f"[Portfolio] 파일 없음: {path}")
        return result
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ticker   = row.get("ticker",   "").strip()
                name     = row.get("name",     "").strip()
                qty      = row.get("quantity", "0").strip()
                exchange = row.get("exchange", "KRX").strip().upper()
                if not ticker:
                    continue
                result[ticker] = {
                    "name":     name,
                    "qty":      int(float(qty)),
                    "exchange": exchange,
                    "is_overseas": exchange not in ("KRX", "KOSPI", "KOSDAQ"),
                }
        exch_list = ", ".join(t + "(" + v["exchange"] + ")" for t, v in result.items())
        print("[Portfolio] " + str(len(result)) + "종목 로드: " + exch_list)
    except Exception as e:
        print(f"[Portfolio] 로드 실패: {e}")
    return result


def reload_portfolio():
    global _portfolio_detail
    new_data = load_portfolio()
    with _portfolio_lock:
        _portfolio_detail = new_data
    print(f"[Portfolio] 리로드 완료 → {len(_portfolio_detail)}종목")


def get_portfolio_detail() -> dict:
    with _portfolio_lock:
        return dict(_portfolio_detail)


def get_portfolio() -> list:
    with _portfolio_lock:
        return list(_portfolio_detail.keys())


def is_overseas(ticker: str) -> bool:
    """해외 종목 여부"""
    with _portfolio_lock:
        info = _portfolio_detail.get(ticker, {})
        return info.get("is_overseas", False)


def get_excd(ticker: str) -> str:
    """KIS 해외거래소 코드 반환"""
    with _portfolio_lock:
        info     = _portfolio_detail.get(ticker, {})
        exchange = info.get("exchange", "KRX")
        return EXCHANGE_TO_EXCD.get(exchange, "NAS")


# ── CSV 변경 감지 ────────────────────────────────────────────────────

class _PortfolioWatcher(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self._last_mtime = self._mtime()

    @staticmethod
    def _mtime():
        try:
            return os.path.getmtime(CSV_PATH)
        except FileNotFoundError:
            return 0.0

    def run(self):
        print(f"[Portfolio] CSV 감시 시작")
        while True:
            time.sleep(2)
            m = self._mtime()
            if m != self._last_mtime:
                print(f"\n[Portfolio] 변경 감지! 리로드 중...")
                reload_portfolio()
                self._last_mtime = m


# ── 초기 로드 ────────────────────────────────────────────────────────
_portfolio_detail = load_portfolio()
_watcher = _PortfolioWatcher()
_watcher.start()


# ── 하위 호환 프록시 ─────────────────────────────────────────────────
class _PortfolioProxy:
    def __getitem__(self, key):
        info = get_portfolio_detail()[key]
        # 기존 코드가 (name, qty, avg_cost) 튜플을 기대하는 경우를 위해
        return (info["name"], info["qty"], 0)

    def get(self, key, default=None):
        info = get_portfolio_detail().get(key)
        if info is None:
            return default
        return (info["name"], info["qty"], 0)

    def keys(self):   return get_portfolio_detail().keys()
    def items(self):  return {k: (v["name"], v["qty"], 0) for k, v in get_portfolio_detail().items()}.items()
    def __len__(self): return len(get_portfolio_detail())
    def __contains__(self, key): return key in get_portfolio_detail()
    def __repr__(self): return repr(get_portfolio_detail())


class _PortfolioListProxy(list):
    def __iter__(self):     return iter(get_portfolio())
    def __len__(self):      return len(get_portfolio())
    def __getitem__(self, i): return get_portfolio()[i]
    def __repr__(self):     return repr(get_portfolio())


PORTFOLIO_DETAIL = _PortfolioProxy()
PORTFOLIO        = _PortfolioListProxy()

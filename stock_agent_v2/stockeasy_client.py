"""stockeasy.intellio.kr 무인증 API 래퍼.

브라우저 클라이언트가 호출하는 reverse-proxy 경로(/stockdata/api/v1/...)를
서버 측에서 그대로 호출. Referer 헤더만 맞추면 통과.
인증 필요한 엔드포인트(industry-reports, high52, rs)는 여기서 처리하지 않음.
"""
from __future__ import annotations

import requests

BASE = "https://stockeasy.intellio.kr/stockdata/api/v1"
_HEADERS = {
    "Origin":     "https://stockeasy.intellio.kr",
    "Referer":    "https://stockeasy.intellio.kr/market-analysis",
    "User-Agent": "Mozilla/5.0",
    "Accept":     "application/json",
}
_TIMEOUT = 8

# 시장 단계 코드 → 한국어 라벨 + 권장 익스포저
# 출처: 사이트 클라이언트 JS 번들에서 추출한 라벨 사전
STATUS_LABEL = {
    "confirmed_uptrend":      ("상승 추세", "80-100%"),
    "uptrend_under_pressure": ("상승 둔화", "40-60%"),
    "market_in_correction":   ("조정 국면", "0-20%"),
    "rally_attempt":          ("반등 시도", "20-40%"),
}


def _get(path: str, params: dict | None = None) -> dict | None:
    try:
        r = requests.get(f"{BASE}{path}", headers=_HEADERS,
                         params=params, timeout=_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[stockeasy] {path} 실패: {type(e).__name__}: {e}")
        return None


def fetch_big_picture() -> dict | None:
    """KOSPI/KOSDAQ 시장 단계 + 분배일."""
    return _get("/market/big-picture")


def fetch_indices() -> dict | None:
    """코스피·코스닥 지수, 등락 종목 수, 단기·장기 시장 신호."""
    return _get("/market/indices")


def fetch_credit_balance(days: int = 20) -> dict | None:
    """신용잔고 추이."""
    return _get("/market/credit-balance", {"days": days})


def fetch_commodity_quotes() -> dict | None:
    """원자재 시세 (구리·유가·금 등)."""
    return _get("/commodity/quotes")


def fetch_dram_etf_overview() -> dict | None:
    """DRAM ETF + 메모리 가격 개요 — 반도체 종목 펀더멘털 컨텍스트."""
    return _get("/memory-prices/dram-etf/overview")


def korea_market_lines() -> list[str]:
    """build_warning_message 용 — 한국 시장 한두 줄 요약.
    실패 시 빈 리스트 반환(브리핑 자체는 정상 발송)."""
    bp = fetch_big_picture()
    idx = fetch_indices()
    if not bp and not idx:
        return []

    out: list[str] = []

    # indices: 코스피/코스닥 가격 + 단기·장기 신호
    idx_map: dict[str, dict] = {}
    if idx and idx.get("indices"):
        for it in idx["indices"]:
            code = it.get("index_code")
            if code == "001":
                idx_map["KOSPI"] = it
            elif code == "101":
                idx_map["KOSDAQ"] = it

    for mkt in ("KOSPI", "KOSDAQ"):
        bp_part = (bp or {}).get(mkt.lower(), {}) if bp else {}
        idx_part = idx_map.get(mkt, {})
        status = bp_part.get("status")
        label, exposure = STATUS_LABEL.get(status, (status or "정보 없음", "?"))
        dd_count = bp_part.get("active_distribution_count", 0)
        if idx_part:
            price = idx_part.get("current_value")
            chg = idx_part.get("price_change_percent")
            head = f"- {mkt} {price:,.2f} ({chg:+.2f}%)" if price else f"- {mkt}"
        else:
            head = f"- {mkt}"
        out.append(f"{head} — {label} (분배일 {dd_count}개, 권장 {exposure})")

    if idx and idx.get("short_term_signal"):
        sig = {"green": "🟢", "yellow": "🟡", "red": "🔴"}
        s_st = sig.get(idx.get("short_term_signal"), idx.get("short_term_signal"))
        s_lt = sig.get(idx.get("long_term_signal"),  idx.get("long_term_signal"))
        out.append(f"- 한국 시장 신호: 단기 {s_st} / 장기 {s_lt}")

    return out


if __name__ == "__main__":
    # 빠른 점검용
    for line in korea_market_lines():
        print(line)

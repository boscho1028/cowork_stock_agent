"""
signals.py - 시그널 생성 규칙 (universe 스캔용)

- Claude 호출 없이 지표·공시 데이터만으로 판정 → 빠르고 저렴함.
- 각 시그널 함수는 발동된 경우 Signal 객체 리스트를 반환.
- 새 시그널 추가: _signal_* 함수 만들고 evaluate_signals() 에 등록 + config.SIGNAL_CONFIG["enabled"] 에 이름 추가.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import numpy as np
import pandas as pd

import config
from database       import load_candles, load_disclosures
from dart_collector import URGENCY_MAP as DART_URGENCY_MAP


PRIORITY_ORDER = {"🔴": 0, "🟠": 1, "🟡": 2}


@dataclass
class Signal:
    ticker:   str
    name:     str
    rule:     str      # 규칙 식별자 (e.g. "rsi_cross")
    title:    str      # 한글 제목
    detail:   str      # 숫자·맥락 정보
    priority: str      # 🔴 / 🟠 / 🟡


# ═══════════════════════════════════════════════════════════════════════
# 유틸
# ═══════════════════════════════════════════════════════════════════════

def _t_minus_1_bday() -> datetime:
    d = datetime.today()
    found = 0
    while True:
        if d.weekday() < 5:
            found += 1
            if found >= 2:
                return d
        d -= timedelta(days=1)


def _rsi_series(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    return 100 - 100 / (1 + gain / loss.replace(0, np.nan))


def _priority_ok(p: str, threshold: str) -> bool:
    return PRIORITY_ORDER.get(p, 9) <= PRIORITY_ORDER.get(threshold, 9)


# ═══════════════════════════════════════════════════════════════════════
# 개별 시그널 규칙
# ═══════════════════════════════════════════════════════════════════════

def _signal_rsi_cross(ticker, name, daily, ind_cfg, sig_cfg) -> List[Signal]:
    rp   = ind_cfg["rsi_period"]
    osv  = sig_cfg["rsi_oversold"]
    obv  = sig_cfg["rsi_overbought"]
    rsi  = _rsi_series(daily["close"], rp)
    if len(rsi) < 2:
        return []
    prev, curr = rsi.iloc[-2], rsi.iloc[-1]
    if np.isnan(prev) or np.isnan(curr):
        return []
    out = []
    if prev >= osv and curr < osv:
        out.append(Signal(ticker, name, "rsi_cross", "RSI 과매도 진입",
                          f"{prev:.1f}→{curr:.1f} (<{osv})", "🟠"))
    elif prev < osv and curr >= osv:
        out.append(Signal(ticker, name, "rsi_cross", "RSI 과매도 탈출",
                          f"{prev:.1f}→{curr:.1f} (>{osv})", "🟡"))
    if prev < obv and curr >= obv:
        out.append(Signal(ticker, name, "rsi_cross", "RSI 과매수 진입",
                          f"{prev:.1f}→{curr:.1f} (>{obv})", "🟡"))
    return out


def _signal_macd_cross(ticker, name, daily, ind_cfg, sig_cfg) -> List[Signal]:
    c = daily["close"]
    macd = c.ewm(span=ind_cfg["macd_fast"],   adjust=False).mean() \
         - c.ewm(span=ind_cfg["macd_slow"],   adjust=False).mean()
    sig  = macd.ewm(span=ind_cfg["macd_signal"], adjust=False).mean()
    hist = macd - sig
    if len(hist) < 2:
        return []
    prev, curr = hist.iloc[-2], hist.iloc[-1]
    if np.isnan(prev) or np.isnan(curr):
        return []
    if prev < 0 and curr > 0:
        return [Signal(ticker, name, "macd_cross", "MACD 골든크로스",
                       f"hist {prev:.2f}→{curr:.2f}", "🟠")]
    if prev > 0 and curr < 0:
        return [Signal(ticker, name, "macd_cross", "MACD 데드크로스",
                       f"hist {prev:.2f}→{curr:.2f}", "🟠")]
    return []


def _signal_ichimoku_cloud_break(ticker, name, daily, ind_cfg, sig_cfg) -> List[Signal]:
    t_n  = ind_cfg["ichimoku_tenkan"]
    k_n  = ind_cfg["ichimoku_kijun"]
    sb_n = ind_cfg["ichimoku_span_b"]
    off  = ind_cfg["ichimoku_offset"]
    if len(daily) < sb_n + off:
        return []

    h, l, c = daily["high"], daily["low"], daily["close"]
    tenk  = (h.rolling(t_n).max() + l.rolling(t_n).min()) / 2
    kij   = (h.rolling(k_n).max() + l.rolling(k_n).min()) / 2
    span_a = ((tenk + kij) / 2).shift(off)
    span_b = ((h.rolling(sb_n).max() + l.rolling(sb_n).min()) / 2).shift(off)

    prev_sa, curr_sa = span_a.iloc[-2], span_a.iloc[-1]
    prev_sb, curr_sb = span_b.iloc[-2], span_b.iloc[-1]
    prev_c,  curr_c  = c.iloc[-2], c.iloc[-1]
    if any(np.isnan(v) for v in [prev_sa, prev_sb, curr_sa, curr_sb]):
        return []

    prev_top, prev_bot = max(prev_sa, prev_sb), min(prev_sa, prev_sb)
    curr_top, curr_bot = max(curr_sa, curr_sb), min(curr_sa, curr_sb)

    prev_pos = "above" if prev_c > prev_top else ("below" if prev_c < prev_bot else "inside")
    curr_pos = "above" if curr_c > curr_top else ("below" if curr_c < curr_bot else "inside")

    if prev_pos in ("below", "inside") and curr_pos == "above":
        return [Signal(ticker, name, "ichimoku_cloud_break", "일목구름 상향돌파",
                       f"종가 {curr_c:,.2f} > 구름상단 {curr_top:,.2f}", "🟠")]
    if prev_pos in ("above", "inside") and curr_pos == "below":
        return [Signal(ticker, name, "ichimoku_cloud_break", "일목구름 하향이탈",
                       f"종가 {curr_c:,.2f} < 구름하단 {curr_bot:,.2f}", "🔴")]
    return []


def _signal_ichimoku_tk_cross(ticker, name, daily, ind_cfg, sig_cfg) -> List[Signal]:
    t_n = ind_cfg["ichimoku_tenkan"]
    k_n = ind_cfg["ichimoku_kijun"]
    if len(daily) < k_n + 1:
        return []
    h, l = daily["high"], daily["low"]
    tenk = (h.rolling(t_n).max() + l.rolling(t_n).min()) / 2
    kij  = (h.rolling(k_n).max() + l.rolling(k_n).min()) / 2
    pt, ct = tenk.iloc[-2], tenk.iloc[-1]
    pk, ck = kij.iloc[-2],  kij.iloc[-1]
    if any(np.isnan(v) for v in [pt, ct, pk, ck]):
        return []
    if pt < pk and ct >= ck:
        return [Signal(ticker, name, "ichimoku_tk_cross", "일목 전환선 상향돌파",
                       f"전환 {ct:,.2f} > 기준 {ck:,.2f}", "🟡")]
    if pt > pk and ct <= ck:
        return [Signal(ticker, name, "ichimoku_tk_cross", "일목 전환선 하향돌파",
                       f"전환 {ct:,.2f} < 기준 {ck:,.2f}", "🟡")]
    return []


def _signal_ma10_weekly_break(ticker, name, weekly, ind_cfg, sig_cfg) -> List[Signal]:
    if weekly.empty or len(weekly) < 11:
        return []
    c    = weekly["close"]
    ma10 = c.rolling(10).mean()
    if np.isnan(ma10.iloc[-2]) or np.isnan(ma10.iloc[-1]):
        return []
    prev_above = c.iloc[-2] >= ma10.iloc[-2]
    curr_above = c.iloc[-1] >= ma10.iloc[-1]
    gap = (c.iloc[-1] / ma10.iloc[-1] - 1) * 100
    if not prev_above and curr_above:
        return [Signal(ticker, name, "ma10_weekly_break", "주봉 10선 상향돌파",
                       f"종가 {c.iloc[-1]:,.2f}  10선 {ma10.iloc[-1]:,.2f}  괴리 {gap:+.2f}%", "🟠")]
    if prev_above and not curr_above:
        return [Signal(ticker, name, "ma10_weekly_break", "주봉 10선 하향이탈",
                       f"종가 {c.iloc[-1]:,.2f}  10선 {ma10.iloc[-1]:,.2f}  괴리 {gap:+.2f}%", "🟠")]
    return []


def _signal_candle_reversal(ticker, name, daily, ind_cfg, sig_cfg) -> List[Signal]:
    from analyzer import detect_candle_patterns
    pat = detect_candle_patterns(daily, n=3)
    if not pat["patterns"]:
        return []
    if "반전상승" in pat["signal"]:
        detail = " / ".join(p.split("—")[0].strip() for p in pat["patterns"][:2])
        return [Signal(ticker, name, "candle_reversal", "반전상승 캔들 패턴", detail, "🟡")]
    if "반전하락" in pat["signal"]:
        detail = " / ".join(p.split("—")[0].strip() for p in pat["patterns"][:2])
        return [Signal(ticker, name, "candle_reversal", "반전하락 캔들 패턴", detail, "🟡")]
    return []


def _signal_volume_spike(ticker, name, daily, ind_cfg, sig_cfg) -> List[Signal]:
    vm   = ind_cfg["volume_ma_period"]
    mult = sig_cfg["volume_spike_mult"]
    if len(daily) < vm + 1:
        return []
    v     = daily["volume"]
    v_ma  = v.rolling(vm).mean().iloc[-1]
    v_now = v.iloc[-1]
    if np.isnan(v_ma) or v_ma <= 0:
        return []
    ratio = v_now / v_ma
    if ratio >= mult:
        return [Signal(ticker, name, "volume_spike", "거래량 급증",
                       f"{ratio:.1f}배 (평균대비)", "🟡")]
    return []


def _signal_ma5_pullback(ticker, name, daily, ind_cfg, sig_cfg) -> List[Signal]:
    """5일선 풀백 진입 후보 — Round 4 backtest 검증 매매 룰.

    gap_atr = (close - MA5) / ATR14 임계값:
      ≥ 4.0 → STRONG 🔴 (검증된 sweet spot, n=27 mean +8.85% PF 8.77 p=0.006)
      ≥ 3.0 → MODERATE 🟠 (보조 강도, 표본 多 mean 약함)
      ≥ 2.0 → WATCH (노이즈 많아 시그널 제외, 분석 prompt 에만 표시)
    """
    from analyzer import compute_pullback_signal

    sig = compute_pullback_signal(daily, ind_cfg)
    level = sig.get("level")
    if level not in ("STRONG", "MODERATE"):
        return []

    icon = sig.get("icon", "")
    gap  = sig["gap_atr"]
    wait = sig["entry_in"]
    sl   = abs(ind_cfg.get("pullback_sl_pct", -10.0))
    tp   = ind_cfg.get("pullback_tp_pct", 20.0)

    if level == "STRONG":
        priority = "🔴"
        title    = f"{icon} MA5 풀백 STRONG (검증 sweet spot)"
    else:
        priority = "🟠"
        title    = f"{icon} MA5 풀백 MODERATE (보조)"

    detail = (f"gap {gap:+.2f} ATR · {wait}일 대기 후 진입 후보 · "
              f"SL -{sl:.0f}% / TP +{tp:.0f}% / close<MA5 청산")
    return [Signal(ticker, name, "ma5_pullback", title, detail, priority)]


def _signal_disclosure(ticker, name, overseas) -> List[Signal]:
    """T-0/T-1 공시 중 🔴 급을 시그널로."""
    t1 = _t_minus_1_bday()

    if overseas:
        # SEC: importance 컬럼 그대로 사용 (database.get_conn 사용 → Turso replica)
        from database import get_conn as _get_conn
        with _get_conn() as conn:
            cur = conn.execute("""
                SELECT form_type, filed_date, items, importance, description
                FROM sec_filings
                WHERE ticker=? AND filed_date >= ?
                ORDER BY filed_date DESC LIMIT 10
            """, (ticker.upper(), t1.strftime("%Y-%m-%d")))
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        out = []
        for r in rows:
            if r["importance"] == "🔴":
                out.append(Signal(ticker, name, "disclosure", "SEC 중요 공시",
                                  f"{r['filed_date']} {r['form_type']} {(r['description'] or '')[:40]}", "🔴"))
        return out

    # DART: report_nm 키워드로 🔴 판정
    rows = load_disclosures(ticker, limit=20, since_date=t1.strftime("%Y%m%d"))
    red_kws = DART_URGENCY_MAP.get("🔴", [])
    out = []
    for r in rows:
        nm = r.get("report_nm", "")
        if any(kw in nm for kw in red_kws):
            dt = r.get("rcept_dt", "")
            dstr = f"{dt[:4]}-{dt[4:6]}-{dt[6:]}" if len(dt) == 8 else dt
            out.append(Signal(ticker, name, "disclosure", "DART 중요 공시",
                              f"{dstr} {nm[:50]}", "🔴"))
    return out


# 규칙 레지스트리: config.SIGNAL_CONFIG["enabled"] 이름 → 함수
_RULES = {
    "rsi_cross":              _signal_rsi_cross,
    "macd_cross":             _signal_macd_cross,
    "ichimoku_cloud_break":   _signal_ichimoku_cloud_break,
    "ichimoku_tk_cross":      _signal_ichimoku_tk_cross,
    "ma10_weekly_break":      _signal_ma10_weekly_break,
    "candle_reversal":        _signal_candle_reversal,
    "volume_spike":           _signal_volume_spike,
    "ma5_pullback":           _signal_ma5_pullback,
    # "disclosure" 는 따로 처리 (weekly 인자 없이)
}


# ═══════════════════════════════════════════════════════════════════════
# 평가 + 포맷
# ═══════════════════════════════════════════════════════════════════════

def evaluate_signals(ticker: str, name: str) -> List[Signal]:
    ind_cfg = config.INDICATOR_CONFIG
    sig_cfg = config.SIGNAL_CONFIG
    enabled = set(sig_cfg.get("enabled", []))

    daily  = load_candles(ticker, "D", limit=400)
    if daily.empty or len(daily) < 30:
        return []
    weekly = load_candles(ticker, "W", limit=260)

    results: List[Signal] = []

    for rule, fn in _RULES.items():
        if rule not in enabled:
            continue
        try:
            if rule == "ma10_weekly_break":
                results.extend(fn(ticker, name, weekly, ind_cfg, sig_cfg))
            else:
                results.extend(fn(ticker, name, daily, ind_cfg, sig_cfg))
        except Exception as e:
            print(f"  [WARN] {ticker} {rule}: {e}")

    if "disclosure" in enabled:
        try:
            overseas = config.is_overseas(ticker)
            results.extend(_signal_disclosure(ticker, name, overseas))
        except Exception as e:
            print(f"  [WARN] {ticker} disclosure: {e}")

    # 우선순위 필터
    threshold = sig_cfg.get("min_priority", "🟡")
    return [s for s in results if _priority_ok(s.priority, threshold)]


def format_report(all_signals: List[Signal], universe_size: int, header: str = "") -> str:
    """우선순위·종목별로 그룹핑한 텔레그램 요약."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    if not all_signals:
        prefix = f"{header}\n" if header else ""
        return (f"{prefix}📡 시그널 스캔 | {ts}\n"
                f"universe {universe_size}종목\n"
                f"발동 시그널 없음 ✅")

    # 우선순위 → 종목 순 정렬
    all_signals.sort(key=lambda s: (PRIORITY_ORDER.get(s.priority, 9), s.ticker))

    # 종목별 그룹핑
    by_ticker: dict = {}
    for s in all_signals:
        by_ticker.setdefault((s.ticker, s.name), []).append(s)

    tickers_hit = len(by_ticker)
    prefix = f"{header}\n" if header else ""
    lines  = [f"{prefix}🚨 시그널 스캔 | {ts}",
              f"universe {universe_size}종목 중 {tickers_hit}종목 · 시그널 {len(all_signals)}건",
              "─────────────"]

    for (tk, nm), sigs in by_ticker.items():
        # 가장 높은 우선순위 이모지로 헤더
        top = min(sigs, key=lambda s: PRIORITY_ORDER.get(s.priority, 9))
        lines.append(f"{top.priority} {tk} {nm}")
        for s in sigs:
            lines.append(f"  · {s.title} — {s.detail}")
        lines.append("")

    return "\n".join(lines).rstrip()

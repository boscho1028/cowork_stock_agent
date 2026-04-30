"""
analyzer.py - 기술적 지표 + Claude AI 분석
수정사항:
  - 일목균형표 데이터 부족 문제 해결 (limit 확대, 최소 기준 완화)
  - 평단가 제거 (보유수량만 표시)
  - 해외주식 대응 (통화 표시, 공시 스킵)
"""

import time
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import anthropic
import config
from database import load_candles, load_latest_report
from dart_collector import DartCollector
from sec_collector  import SECCollector
from elliott_wave   import compute_elliott_wave

# Gemini는 선택 의존성. 미설치 시 Claude 전용 모드로 폴백.
try:
    from google import genai as _genai
    from google.genai import types as _genai_types
    _GENAI_AVAILABLE = True
except ImportError:
    _GENAI_AVAILABLE = False


def _t_minus_1_business_day() -> datetime:
    """오늘 기준 T-1 영업일(= 오늘 포함 최근 2영업일의 시작일)."""
    d = datetime.today()
    found = 0
    while True:
        if d.weekday() < 5:
            found += 1
            if found >= 2:
                return d
        d -= timedelta(days=1)


# ═══════════════════════════════════════════════════════════════════════
# 1. 기술적 지표 계산
# ═══════════════════════════════════════════════════════════════════════

def compute_indicators(df: pd.DataFrame, ma_periods: list, cfg: dict) -> dict:
    if df.empty or len(df) < 5:
        return {}

    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    R = {}
    R["current"]    = c.iloc[-1]
    R["prev"]       = c.iloc[-2] if len(c) > 1 else c.iloc[-1]
    R["change_pct"] = (R["current"] / R["prev"] - 1) * 100

    # 이동평균
    for p in ma_periods:
        if len(df) >= p:
            R[f"ma{p}"] = c.rolling(p).mean().iloc[-1]

    mas = [R[f"ma{p}"] for p in ma_periods if f"ma{p}" in R]
    if len(mas) >= 2:
        R["ma_align"] = (
            "정배열 [OK]" if all(mas[i] > mas[i+1] for i in range(len(mas)-1)) else
            "역배열 [ERROR]" if all(mas[i] < mas[i+1] for i in range(len(mas)-1)) else
            "혼조"
        )

    # 10선 돌파
    if 10 in ma_periods and len(df) >= 11:
        ma10 = c.rolling(10).mean()
        if ma10.iloc[-2] > 0:
            if c.iloc[-2] < ma10.iloc[-2] and c.iloc[-1] >= ma10.iloc[-1]:
                R["ma10_cross"] = "10선 상향돌파 🟢"
            elif c.iloc[-2] > ma10.iloc[-2] and c.iloc[-1] <= ma10.iloc[-1]:
                R["ma10_cross"] = "10선 하향돌파 🔴"
            else:
                above = c.iloc[-1] >= ma10.iloc[-1]
                R["ma10_cross"] = f"{'10선 위' if above else '10선 아래'} (돌파 없음)"
            R["ma10_gap_pct"] = (c.iloc[-1] / ma10.iloc[-1] - 1) * 100

    # RSI
    rp    = cfg["rsi_period"]
    delta = c.diff()
    gain  = delta.clip(lower=0).rolling(rp).mean()
    loss  = (-delta.clip(upper=0)).rolling(rp).mean()
    rsi_s = 100 - 100 / (1 + gain / loss.replace(0, np.nan))
    R["rsi"]      = rsi_s.iloc[-1]
    R["rsi_prev"] = rsi_s.iloc[-2] if len(rsi_s) > 1 else R["rsi"]

    oversold   = cfg["rsi_oversold"]
    overbought = cfg["rsi_overbought"]
    pr, cr     = R["rsi_prev"], R["rsi"]
    if pr >= oversold and cr < oversold:
        R["rsi_signal"] = f"[WARN] RSI {oversold} 하향돌파! (과매도 진입)"
    elif pr < oversold and cr >= oversold:
        R["rsi_signal"] = f"🟢 RSI {oversold} 상향돌파! (과매도 탈출)"
    elif cr < oversold:
        R["rsi_signal"] = f"🔴 과매도 유지 (RSI {cr:.1f})"
    elif cr >= overbought:
        R["rsi_signal"] = f"🟡 과매수 (RSI {cr:.1f})"
    else:
        R["rsi_signal"] = f"중립 (RSI {cr:.1f})"

    # MACD
    f_, s_, g_ = cfg["macd_fast"], cfg["macd_slow"], cfg["macd_signal"]
    macd_line = c.ewm(span=f_, adjust=False).mean() - c.ewm(span=s_, adjust=False).mean()
    sig_line  = macd_line.ewm(span=g_, adjust=False).mean()
    hist      = macd_line - sig_line
    R["macd_hist"]  = hist.iloc[-1]
    prev_h = hist.iloc[-2] if len(hist) > 1 else 0
    R["macd_cross"] = (
        "골든크로스 🟢" if prev_h < 0 and R["macd_hist"] > 0 else
        "데드크로스 🔴" if prev_h > 0 and R["macd_hist"] < 0 else
        f"없음 ({'상승' if R['macd_hist'] > 0 else '하락'} 구간)"
    )

    # 볼린저밴드
    bp     = cfg["bb_period"]
    bb_mid = c.rolling(bp).mean()
    bb_std = c.rolling(bp).std()
    bb_up  = bb_mid + cfg["bb_std"] * bb_std
    bb_dn  = bb_mid - cfg["bb_std"] * bb_std
    R["bb_upper"]  = bb_up.iloc[-1]
    R["bb_middle"] = bb_mid.iloc[-1]
    R["bb_lower"]  = bb_dn.iloc[-1]
    R["bb_pct"]    = (R["current"] - bb_dn.iloc[-1]) / (bb_up.iloc[-1] - bb_dn.iloc[-1] + 1e-9)
    R["bb_state"]  = (
        "상단 돌파 🔴" if R["current"] > bb_up.iloc[-1] else
        "하단 이탈 🟢" if R["current"] < bb_dn.iloc[-1] else
        "밴드 내"
    )

    # ATR / 거래량
    tr = pd.concat(
        [h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1
    ).max(axis=1)
    R["atr"]       = tr.rolling(cfg["atr_period"]).mean().iloc[-1]
    R["atr_pct"]   = R["atr"] / R["current"] * 100
    vm             = cfg["volume_ma_period"]
    R["vol"]       = v.iloc[-1]
    R["vol_ma"]    = v.rolling(vm).mean().iloc[-1]
    R["vol_ratio"] = R["vol"] / (R["vol_ma"] + 1e-9)

    # 52주 고/저
    n = min(len(c), 252)
    R["high_52w"]  = c.iloc[-n:].max()
    R["low_52w"]   = c.iloc[-n:].min()
    R["from_high"] = (R["current"] / R["high_52w"] - 1) * 100

    return R


# ═══════════════════════════════════════════════════════════════════════
# 2. 캔들 추세전환 패턴
# ═══════════════════════════════════════════════════════════════════════

def detect_candle_patterns(df: pd.DataFrame, n: int = 5) -> dict:
    if len(df) < 3:
        return {"patterns": [], "candles": "", "signal": "데이터 부족"}

    tail = df.tail(n)
    o = tail["open"].values
    h = tail["high"].values
    l = tail["low"].values
    c = tail["close"].values
    patterns = []

    if len(tail) >= 3:
        if (c[-3] < o[-3] and abs(c[-2]-o[-2]) < (h[-2]-l[-2])*0.3 and
                c[-1] > o[-1] and c[-1] > (o[-3]+c[-3])/2):
            patterns.append("🌟 샛별형 (Morning Star) — 반전상승 신호")
        if (c[-3] > o[-3] and abs(c[-2]-o[-2]) < (h[-2]-l[-2])*0.3 and
                c[-1] < o[-1] and c[-1] < (o[-3]+c[-3])/2):
            patterns.append("🌆 저녁별형 (Evening Star) — 반전하락 신호")
        if (all(c[i] < o[i] for i in [-3,-2,-1]) and c[-2] < c[-3] and c[-1] < c[-2]):
            patterns.append("🐦 흑삼병 (Three Black Crows) — 강한 하락 전환")
        if (all(c[i] > o[i] for i in [-3,-2,-1]) and c[-2] > c[-3] and c[-1] > c[-2]):
            patterns.append("🏹 적삼병 (Three White Soldiers) — 강한 상승 전환")

    if len(tail) >= 2:
        body2 = abs(c[-1] - o[-1])
        wick2 = h[-1] - l[-1]
        if (c[-2] < o[-2] and c[-1] > o[-1] and o[-1] <= c[-2] and c[-1] >= o[-2]):
            patterns.append("🟢 상승장악형 (Bullish Engulfing) — 반전상승")
        if (c[-2] > o[-2] and c[-1] < o[-1] and o[-1] >= c[-2] and c[-1] <= o[-2]):
            patterns.append("🔴 하락장악형 (Bearish Engulfing) — 반전하락")
        lower_wick = min(o[-1], c[-1]) - l[-1]
        upper_wick = h[-1] - max(o[-1], c[-1])
        if lower_wick > body2*2 and upper_wick < body2*0.5 and c[-2] < o[-2]:
            patterns.append("🔨 망치형 (Hammer) — 하락 후 반전상승 가능")
        if upper_wick > body2*2 and lower_wick < body2*0.5 and c[-2] > o[-2]:
            patterns.append("💫 유성형 (Shooting Star) — 상승 후 반전하락 가능")
        if body2 < wick2*0.1 and wick2 > 0:
            patterns.append("➕ 도지 (Doji) — 추세 전환 가능성")

    lines = []
    for dt, row in tail.iterrows():
        body = row["close"] - row["open"]
        wick = row["high"] - row["low"]
        d = "양봉" if body > 0 else "음봉"
        s = "도지" if abs(body) < wick*0.1 else ("장대" if abs(body) > wick*0.6 else "보통")
        lines.append(
            f"{dt.strftime('%m/%d')} {s}{d} "
            f"O:{row['open']:,.2f} H:{row['high']:,.2f} "
            f"L:{row['low']:,.2f} C:{row['close']:,.2f}"
        )

    up_kw   = ["상승", "반전상승", "Bullish", "Soldiers", "Morning", "Hammer"]
    down_kw = ["하락", "반전하락", "Bearish", "Crows",    "Evening", "Shooting"]
    up   = sum(1 for p in patterns if any(k in p for k in up_kw))
    down = sum(1 for p in patterns if any(k in p for k in down_kw))

    return {
        "patterns": patterns,
        "candles":  "\n".join(lines),
        "signal":   ("반전상승 신호 🟢" if up > down else
                     "반전하락 신호 🔴" if down > up else "패턴 없음"),
    }


# ═══════════════════════════════════════════════════════════════════════
# 3. 일목균형표 계산
#    핵심 수정: 데이터 부족 기준 완화 + limit 확대
# ═══════════════════════════════════════════════════════════════════════

def compute_ichimoku(df: pd.DataFrame, cfg: dict) -> dict:
    """
    일목균형표 계산
    - 전환선(9): 최소 9봉
    - 기준선(26): 최소 26봉
    - 선행스팬B(52): 최소 52봉 → 구름 계산 가능
    - 최소 26봉만 있어도 전환선/기준선/크로스는 분석 가능
    """
    tenkan_n = cfg["ichimoku_tenkan"]   # 9
    kijun_n  = cfg["ichimoku_kijun"]    # 26
    span_b_n = cfg["ichimoku_span_b"]   # 52
    offset   = cfg["ichimoku_offset"]   # 26

    if len(df) < tenkan_n:
        return {"available": False, "reason": f"데이터 {len(df)}봉 (최소 {tenkan_n}봉 필요)"}

    h, l, c = df["high"], df["low"], df["close"]

    def mid(n):
        if len(df) < n:
            return pd.Series([np.nan] * len(df), index=df.index)
        return (h.rolling(n).max() + l.rolling(n).min()) / 2

    def safe(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        return round(float(v), 2)

    tenkan = mid(tenkan_n)
    kijun  = mid(kijun_n)
    span_a = ((tenkan + kijun) / 2).shift(offset) if len(df) >= kijun_n else None
    span_b = mid(span_b_n).shift(offset) if len(df) >= span_b_n else None

    price = c.iloc[-1]
    t_val = safe(tenkan.iloc[-1])
    k_val = safe(kijun.iloc[-1]) if len(df) >= kijun_n else None
    sa_val = safe(span_a.iloc[-1]) if span_a is not None else None
    sb_val = safe(span_b.iloc[-1]) if span_b is not None else None

    # 전환/기준선 크로스 (26봉 이상이면 계산)
    tk_cross = "N/A (기준선 데이터 부족)"
    if k_val is not None and len(tenkan) > 1 and len(kijun) > 1:
        prev_t = tenkan.iloc[-2]
        prev_k = kijun.iloc[-2]
        if not (np.isnan(prev_t) or np.isnan(prev_k)):
            if prev_t < prev_k and tenkan.iloc[-1] >= kijun.iloc[-1]:
                tk_cross = "전환선 기준선 상향돌파 🟢 (호전)"
            elif prev_t > prev_k and tenkan.iloc[-1] <= kijun.iloc[-1]:
                tk_cross = "전환선 기준선 하향돌파 🔴 (역전)"
            else:
                above = tenkan.iloc[-1] >= kijun.iloc[-1]
                tk_cross = f"크로스 없음 ({'전환>기준' if above else '전환<기준'})"

    # 구름 위치 (52봉 이상이면 계산)
    cloud_pos = "구름 계산 중 (52봉 이상 필요)"
    cloud_top = cloud_bot = None
    if sa_val and sb_val:
        cloud_top = max(sa_val, sb_val)
        cloud_bot = min(sa_val, sb_val)
        cloud_pos = (
            "구름 위 [OK] (강세)" if price > cloud_top else
            "구름 아래 [ERROR] (약세)" if price < cloud_bot else
            "구름 안 [WARN] (중립)"
        )

    # 지지/저항
    supports    = sorted([lv for lv in [k_val, cloud_bot] if lv and price > lv], reverse=True)
    resistances = sorted([lv for lv in [k_val, cloud_top] if lv and price < lv])

    return {
        "available":   True,
        "data_count":  len(df),
        "tenkan":      t_val,
        "kijun":       k_val,
        "span_a":      sa_val,
        "span_b":      sb_val,
        "cloud_pos":   cloud_pos,
        "cloud_top":   cloud_top,
        "cloud_bot":   cloud_bot,
        "tk_cross":    tk_cross,
        "support":     supports,
        "resistance":  resistances,
    }


# ═══════════════════════════════════════════════════════════════════════
# 3.5. 5일선 풀백 시그널 (Round 4 backtest 검증)
# ═══════════════════════════════════════════════════════════════════════

def compute_pullback_signal(df: pd.DataFrame, cfg: dict) -> dict:
    """gap_atr = (close - MA5) / ATR14 가 임계값 초과 시 풀백 매매 후보. (3단계)

    검증된 매매 룰 (Round 4 backtest, sweet spot = STRONG):
      gap≥4.0 ATR → 평균 1~2일 -1.9% 조정 → 진입 → streak 깨짐 / 익절 +20% /
      손절 -10% / 보유 max 10일.   n=27, mean +8.85%, PF 8.77, p=0.006.
    MODERATE/WATCH 는 동일 가이드, 강도만 약함 (LLM 의사결정에 반영).

    반환:
      level    : "STRONG" | "MODERATE" | "WATCH" | None
      gap_atr  : 현재 ATR 단위 이격률 (음수면 MA5 아래)
      message  : 한 줄 시그널 메시지
      entry_in : 진입까지 대기 영업일 수 (시그널 없으면 None)
      sl_price / tp_price : 진입 가정 시 손절·익절 가격 (현재가 기준 추정)
    """
    if len(df) < 14:
        return {"level": None, "gap_atr": None, "message": "", "entry_in": None}

    c, h, l = df["close"], df["high"], df["low"]
    ma5 = c.rolling(5).mean().iloc[-1]
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()],
                   axis=1).max(axis=1)
    atr14 = tr.rolling(14).mean().iloc[-1]
    if pd.isna(ma5) or pd.isna(atr14) or atr14 == 0:
        return {"level": None, "gap_atr": None, "message": "", "entry_in": None}

    price = float(c.iloc[-1])
    gap_atr = float((price - ma5) / atr14)

    strong   = cfg.get("pullback_atr_strong",   4.0)
    moderate = cfg.get("pullback_atr_moderate", 3.0)
    watch    = cfg.get("pullback_atr_watch",    2.0)
    wait     = cfg.get("pullback_wait_days",    2)
    sl_pct   = cfg.get("pullback_sl_pct",      -10.0) / 100
    tp_pct   = cfg.get("pullback_tp_pct",       20.0) / 100

    # 진입가는 wait일 후 종가로 unknown — 현재가를 placeholder 로
    sl_price = price * (1 + sl_pct)
    tp_price = price * (1 + tp_pct)

    base = {
        "gap_atr":  gap_atr,
        "ma5":      float(ma5),
        "atr14":    float(atr14),
        "entry_in": wait,
        "sl_price": sl_price,
        "tp_price": tp_price,
    }

    if gap_atr >= strong:
        return {**base,
            "level":   "STRONG",
            "icon":    "🔥",
            "message": f"🔥 STRONG 풀백 시그널 — gap {gap_atr:+.2f} ATR ≥ {strong} (검증된 sweet spot)",
        }
    if gap_atr >= moderate:
        return {**base,
            "level":   "MODERATE",
            "icon":    "📈",
            "message": f"📈 MODERATE 풀백 시그널 — gap {gap_atr:+.2f} ATR ≥ {moderate} (보조 강도)",
        }
    if gap_atr >= watch:
        return {**base,
            "level":   "WATCH",
            "icon":    "👁️",
            "message": f"👁️ WATCH 풀백 시그널 — gap {gap_atr:+.2f} ATR ≥ {watch} (관찰 단계)",
        }
    return {
        "level":    None,
        "gap_atr":  gap_atr,
        "ma5":      float(ma5),
        "atr14":    float(atr14),
        "message":  f"gap {gap_atr:+.2f} ATR — 시그널 없음 "
                    f"(관찰≥{watch}, 보조≥{moderate}, 강력≥{strong})",
        "entry_in": None,
    }


# ═══════════════════════════════════════════════════════════════════════
# 4. 포맷 헬퍼
# ═══════════════════════════════════════════════════════════════════════

def _f(v, d=2, suf=""):
    """소수점 2자리 기본 (해외주식 달러 표시용)"""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "N/A"
    return f"{v:,.{d}f}{suf}"


def _fp(v, is_overseas=False):
    """가격 포맷 (국내=0자리, 해외=2자리)"""
    d = 2 if is_overseas else 0
    return _f(v, d)


# ═══════════════════════════════════════════════════════════════════════
# 5. 메인 분석 엔진
# ═══════════════════════════════════════════════════════════════════════

class StockAnalyzer:

    # Claude API 529(Overloaded)·429·5xx 발생 시 재시도 간격(초)
    _RETRY_DELAYS    = (5, 15, 45)
    _PROVIDER_LABEL  = {"claude": "Claude", "gemini": "Gemini"}

    def __init__(self, primary: str = None):
        """
        primary: 'claude' | 'gemini' | None(None이면 config.AI_PRIMARY 사용)
        실패 시 반대 모델로 단일 폴백.
        """
        p = (primary or config.AI_PRIMARY or "claude").lower()
        if p not in ("claude", "gemini"):
            print(f"[WARN] 알 수 없는 primary '{p}' → claude 로 설정")
            p = "claude"
        self.primary = p

        # Claude client (SDK 기본 재시도를 5회로)
        self.claude_client = anthropic.Anthropic(
            api_key=config.ANTHROPIC_API_KEY, max_retries=5,
        )

        # Gemini client (선택)
        self.gemini_client = None
        if _GENAI_AVAILABLE and config.GOOGLE_API_KEY:
            try:
                self.gemini_client = _genai.Client(api_key=config.GOOGLE_API_KEY)
            except Exception as e:
                print(f"[WARN] Gemini 클라이언트 생성 실패: {e}")
        elif not _GENAI_AVAILABLE:
            print("[INFO] google-genai 미설치 → Gemini 폴백 비활성")
        elif not config.GOOGLE_API_KEY:
            print("[INFO] GOOGLE_API_KEY 미설정 → Gemini 폴백 비활성")

        print(f"[AI] primary = {self.primary}"
              + ("" if self.gemini_client else " (Gemini 비활성)"))

        self.dart = DartCollector()
        self.sec  = SECCollector()
        self.cfg  = config.INDICATOR_CONFIG

    # ── Provider 호출 ────────────────────────────────────────────────

    def _call_claude(self, prompt: str) -> str:
        """Overloaded/Rate-limit/5xx 에 대해 백오프 재시도."""
        last_err = None
        for attempt in range(len(self._RETRY_DELAYS) + 1):
            try:
                resp = self.claude_client.messages.create(
                    model=config.CLAUDE_MODEL,
                    max_tokens=1200,
                    messages=[{"role": "user", "content": prompt}],
                )
                return resp.content[0].text
            except anthropic.APIStatusError as e:
                status    = getattr(e, "status_code", None)
                msg       = str(e).lower()
                retryable = (status in (429, 500, 502, 503, 504, 529)
                             or "overloaded" in msg or "rate" in msg)
                last_err = e
                if not retryable or attempt >= len(self._RETRY_DELAYS):
                    raise
                wait = self._RETRY_DELAYS[attempt]
                print(f"  [RETRY] Claude {status} → {wait}s 후 재시도 "
                      f"({attempt+1}/{len(self._RETRY_DELAYS)})")
                time.sleep(wait)
        if last_err:
            raise last_err

    def _call_gemini(self, prompt: str) -> str:
        if not self.gemini_client:
            raise RuntimeError("Gemini 사용 불가 — google-genai 또는 GOOGLE_API_KEY 확인")
        # thinking_budget 을 config에서 받아 품질·비용 밸런스 조정 가능.
        # 0(OFF) / 512(가벼움) / 1024(권장·기본) / 2048(Pro에 근접)
        resp = self.gemini_client.models.generate_content(
            model=config.GEMINI_MODEL,
            contents=prompt,
            config=_genai_types.GenerateContentConfig(
                max_output_tokens=4000,
                temperature=0.7,
                thinking_config=_genai_types.ThinkingConfig(
                    thinking_budget=config.GEMINI_THINKING_BUDGET
                ),
            ),
        )
        text = resp.text if hasattr(resp, "text") else ""
        if not text:
            # 원인 진단 (finish_reason = MAX_TOKENS / SAFETY / RECITATION 등)
            reason = None
            try:
                reason = getattr(resp.candidates[0], "finish_reason", None)
            except Exception:
                pass
            raise RuntimeError(f"Gemini 빈 응답 (finish_reason={reason})")
        return text.strip()

    def _invoke(self, provider: str, prompt: str) -> str:
        if provider == "claude":
            return self._call_claude(prompt)
        return self._call_gemini(prompt)

    def _call_ai(self, prompt: str):
        """primary → 반대 모델 순으로 호출. (text, provider) 반환."""
        secondary = "gemini" if self.primary == "claude" else "claude"
        try:
            return self._invoke(self.primary, prompt), self.primary
        except Exception as e_primary:
            print(f"  [FALLBACK] {self.primary} 실패 → {secondary} 시도 "
                  f"({type(e_primary).__name__}: {str(e_primary)[:120]})")
            try:
                return self._invoke(secondary, prompt), secondary
            except Exception as e_secondary:
                print(f"  [FAIL] {secondary}도 실패: "
                      f"{type(e_secondary).__name__}: {str(e_secondary)[:120]}")
                raise e_primary

    # ── 공시 요약 (모닝 브리핑용) ────────────────────────────────────
    def summarize_disclosures(self, blocks: list) -> dict:
        """
        공시 목록을 LLM 으로 한번에 요약. 중요 공시에는 뉴스 기사(제목+
        스니펫) 가 함께 주어지므로 구체 수치·배경까지 반영한다.

        blocks: [{"ticker": str, "name": str, "market": "KR"|"US",
                  "items": list[str],
                  "news": list[dict] (선택: title/description/link)},
                 ...]
        반환: {ticker: 한 줄 요약} — items 비어있던 종목은 제외.
        """
        filled = [b for b in blocks if b.get("items")]
        if not filled:
            return {}

        lines = []
        for b in filled:
            flag = "🇺🇸" if b["market"] == "US" else "🇰🇷"
            lines.append(f"[{b['ticker']} | {b['name']} {flag}]")
            lines.append("공시:")
            lines.extend(f"  {it}" for it in b["items"])
            news = b.get("news") or []
            if news:
                lines.append("관련 뉴스:")
                for n in news:
                    t = n.get("title", "").strip()
                    d = n.get("description", "").strip()
                    if t:
                        lines.append(f"  - {t}")
                    if d:
                        lines.append(f"    {d[:200]}")
            lines.append("")
        listing = "\n".join(lines).strip()

        prompt = (
            "아래는 포트폴리오 종목의 최근 공시 목록과 관련 뉴스다. 각 종목에 "
            "대해 **한 줄(최대 2문장)** 로 요약하라. 뉴스 스니펫에 구체 수치/"
            "배경이 있으면 반영하고, 없으면 공시 제목·중요도(🔴>🟠>🟡>🔵) 만으로 "
            "판단. 본문 수치가 필요하면 '본문 확인 필요' 명시. 뉴스가 공시와 "
            "무관해 보이면 무시. 추측·과장·투자권유 금지.\n\n"
            "응답 형식 (엄격):\n"
            "TICKER | 요약문\n"
            "TICKER | 요약문\n"
            "...\n"
            "종목당 정확히 한 줄. 파이프(|) 앞뒤 공백 1칸.\n\n"
            "입력:\n"
            f"{listing}"
        )

        try:
            text, provider = self._call_ai(prompt)
        except Exception as e:
            print(f"  [WARN] 공시 LLM 요약 실패: {type(e).__name__}: {e}")
            return {}

        result: dict[str, str] = {}
        for ln in text.splitlines():
            if "|" not in ln:
                continue
            k, _, v = ln.partition("|")
            k, v = k.strip(), v.strip()
            if k and v:
                result[k] = v
        print(f"  [AI] 공시 요약 ({provider}) — {len(result)}종목")
        return result

    def analyze(self, ticker: str) -> str:
        cfg      = self.cfg
        overseas = config.is_overseas(ticker)

        # 캔들 로드 — 일목균형표를 위해 limit을 충분히 크게
        daily   = load_candles(ticker, "D", limit=400)   # 52+26 이상 확보
        weekly  = load_candles(ticker, "W", limit=260)
        monthly = load_candles(ticker, "M", limit=60)

        # 지표 계산
        d_ind = compute_indicators(daily,   cfg["ma_periods"],         cfg)
        w_ind = compute_indicators(weekly,  cfg["ma_periods_weekly"],  cfg)
        m_ind = compute_indicators(monthly, cfg["ma_periods_monthly"], cfg)

        # 캔들 패턴
        d_pat = detect_candle_patterns(daily,   n=5)
        w_pat = detect_candle_patterns(weekly,  n=4)
        m_pat = detect_candle_patterns(monthly, n=3)

        # 일목균형표 (일봉 기준, 400봉으로 충분)
        ichi = compute_ichimoku(daily, cfg)

        # 5일선 풀백 시그널 (Round 4 backtest 검증)
        pullback = compute_pullback_signal(daily, cfg)

        # 엘리엇 5파 추진파 검출 (일봉 PoC)
        elliott = compute_elliott_wave(daily, config.ELLIOTT_CONFIG)

        # T-0/T-1 공시 (오늘 + 전 영업일). 휴일 대비 fetch는 5일치로 넉넉히.
        t1         = _t_minus_1_business_day()
        since_dart = t1.strftime("%Y%m%d")
        since_sec  = t1.strftime("%Y-%m-%d")
        range_lbl  = f"{t1.strftime('%m/%d')}~{datetime.today().strftime('%m/%d')}"

        if overseas:
            self.sec.fetch_filings(ticker, days_back=5)
            disc_text  = self.sec.get_filing_summary(ticker, since_date=since_sec, limit=15)
            disc_label = f"SEC 공시 ({range_lbl}, T-0/T-1)"
        else:
            self.dart.fetch_special_disclosures(ticker, days_back=5)
            disc_text  = self.dart.get_disclosure_summary(ticker, since_date=since_dart, limit=15)
            disc_label = f"DART 특별공시 ({range_lbl}, T-0/T-1)"

        # 재무 (국내만)
        report = load_latest_report(ticker) if not overseas else None

        # 보유 정보 (portfolio → universe 순 조회, 평단가 없음)
        info = (config.get_portfolio_detail().get(ticker)
             or config.get_universe_detail().get(ticker)
             or {})
        name = info.get("name", ticker)
        qty  = info.get("qty", 0)
        curr = d_ind.get("current", 0)
        currency = "$" if overseas else "₩"

        prompt = self._build_prompt(
            ticker, name, qty, curr, currency, overseas,
            d_ind, w_ind, m_ind,
            d_pat, w_pat, m_pat,
            ichi, pullback, elliott, disc_text, disc_label, report,
        )

        text, provider = self._call_ai(prompt)
        return f"{text.rstrip()}\n\n🤖 AI: {self._PROVIDER_LABEL[provider]}"

    def _build_prompt(
        self, ticker, name, qty, curr, currency, overseas,
        d, w, m, d_pat, w_pat, m_pat, ichi, pullback, elliott, disc, disc_label, report
    ) -> str:

        fp = lambda v: _fp(v, overseas)

        # 전일비 변동률 (일봉 기준)
        chg_pct  = d.get("change_pct")
        if chg_pct is None or (isinstance(chg_pct, float) and np.isnan(chg_pct)):
            chg_str = ""
        else:
            sign_ch = "+" if chg_pct >= 0 else ""
            chg_str = f" ({sign_ch}{chg_pct:.2f}%)"

        # 재무 요약 (국내만)
        fin = ""
        if report:
            fin = (
                f"\n═══ 최근 재무 ({report.get('period_end','')}) ═══\n"
                f"매출 {_f(report.get('revenue'),1)}억  "
                f"영업이익 {_f(report.get('op_income'),1)}억  "
                f"순이익 {_f(report.get('net_income'),1)}억  "
                f"부채비율 {_f(report.get('debt_ratio'),1)}%"
            )

        # 풀백 시그널 텍스트 (Round 4 backtest 검증된 매매 룰)
        if pullback.get("level"):
            pull_lines = [
                pullback["message"],
                f"MA5: {fp(pullback['ma5'])}  ATR(14): {_f(pullback['atr14'],2)}",
                f"진입: {pullback['entry_in']}거래일 후 종가 (검증된 단기 조정 -1.9% 활용)",
                f"손절: {fp(pullback['sl_price'])} (-{abs(self.cfg['pullback_sl_pct']):.0f}%)  "
                f"익절: {fp(pullback['tp_price'])} (+{self.cfg['pullback_tp_pct']:.0f}%)  "
                f"청산 보조: close < MA5 또는 max {self.cfg['pullback_max_hold']}일 보유",
                f"검증 성과: 평균 +8.85%/trade, profit factor 8.77, max DD -7.2%, p=0.006 (n=27)",
            ]
            pullback_txt = "\n".join(pull_lines)
        else:
            pullback_txt = pullback.get("message", "데이터 부족")

        # 일목균형표 텍스트
        if ichi.get("available"):
            data_count = ichi.get("data_count", 0)
            ichi_lines = [
                f"전환선({self.cfg['ichimoku_tenkan']}): {fp(ichi['tenkan'])}  "
                f"기준선({self.cfg['ichimoku_kijun']}): {fp(ichi['kijun'])}",
                f"선행스팬A: {fp(ichi['span_a'])}  선행스팬B: {fp(ichi['span_b'])}",
                f"구름 위치: {ichi['cloud_pos']}",
                f"전환/기준 크로스: {ichi['tk_cross']}",
                f"일목 지지: {[fp(v) for v in ichi['support']]}  "
                f"일목 저항: {[fp(v) for v in ichi['resistance']]}",
                f"(분석 기준: {data_count}봉)",
            ]
            ichi_txt = "\n".join(ichi_lines)
        else:
            reason = ichi.get("reason", "데이터 부족")
            ichi_txt = f"계산 불가: {reason}"

        # 엘리엇 파동 텍스트 (PoC: 일봉 추진 5파만, 모든 후보 노출)
        if elliott.get("available"):
            ell_pts = " → ".join(
                f"{p['wave']}({p['date'][5:]}@{fp(p['price'])})"
                for p in elliott["points"]
            )
            sc = elliott["scores"]
            ratios = elliott.get("ratios", {})
            ell_lines = [
                f"방향: {'상승 추진' if elliott['direction']=='up' else '하락 추진'}  "
                f"상태: {elliott['current_wave']}",
                f"신뢰도: {elliott['confidence']}/100 (등급 {elliott['grade']})  "
                f"raw {sc['raw']}/{sc['max']}",
                f"세부 점수 — 피보 {sc['fib']}/75  거래량 {sc['volume']}/60  "
                f"RSI {sc['rsi']}/30  추세선 {sc['trend']}/30",
                f"파동: {ell_pts}",
                f"피보 비율 — 2파 {ratios.get('w2',0):.2f}  3파 {ratios.get('w3',0):.2f}  "
                f"4파 {ratios.get('w4',0):.2f}  5파 {ratios.get('w5',0):.2f}",
            ]
            if elliott.get("warnings"):
                for wn in elliott["warnings"]:
                    ell_lines.append(f"⚠ {wn}")
            elliott_txt = "\n".join(ell_lines)
        else:
            elliott_txt = f"검출 안 됨: {elliott.get('reason', '데이터 부족')}"

        def fmt_pat(pat):
            lines = pat["patterns"] if pat["patterns"] else ["특이 패턴 없음"]
            return "\n".join(lines) + f"\n→ 종합: {pat['signal']}"

        # 프롬프트 내에서 config 값 직접 참조
        t_n = self.cfg["ichimoku_tenkan"]
        k_n = self.cfg["ichimoku_kijun"]

        market_tag = "해외주식" if overseas else "국내주식"

        return f"""당신은 베테랑 퀀트 트레이더입니다. 아래 데이터로 {name}({ticker}) [{market_tag}] 심층 분석을 해주세요.

═══ 보유 현황 ═══
보유: {qty:,}주  현재가: {currency}{fp(curr)}

═══ 일봉 지표 ═══
현재가: {currency}{fp(d.get('current'))}  전일비: {_f(d.get('change_pct'),2)}%
MA5: {fp(d.get('ma5'))} / MA20: {fp(d.get('ma20'))} / MA60: {fp(d.get('ma60'))} / MA120: {fp(d.get('ma120'))}
MA배열: {d.get('ma_align','N/A')}
RSI: {d.get('rsi_signal','N/A')}
MACD: {d.get('macd_cross','N/A')}  히스토그램: {_f(d.get('macd_hist'),2)}
볼린저밴드: {d.get('bb_state','N/A')}  %B: {_f(d.get('bb_pct'),2)}
거래량: 평균대비 {_f(d.get('vol_ratio'),1)}배  ATR: {_f(d.get('atr_pct'),2)}%
52주 고점대비: {_f(d.get('from_high'),1)}%

═══ 일봉 캔들 패턴 (최근 5봉) ═══
{d_pat['candles']}
{fmt_pat(d_pat)}

═══ 주봉 지표 ═══
MA5: {fp(w.get('ma5'))} / MA10: {fp(w.get('ma10'))} / MA20: {fp(w.get('ma20'))} / MA60: {fp(w.get('ma60'))}
10선 돌파: {w.get('ma10_cross','N/A')}  괴리율: {_f(w.get('ma10_gap_pct'),2)}%
MA배열: {w.get('ma_align','N/A')}
RSI: {w.get('rsi_signal','N/A')}
MACD: {w.get('macd_cross','N/A')}

═══ 주봉 캔들 패턴 (최근 4봉) ═══
{w_pat['candles']}
{fmt_pat(w_pat)}

═══ 월봉 지표 ═══
MA5: {fp(m.get('ma5'))} / MA10: {fp(m.get('ma10'))} / MA20: {fp(m.get('ma20'))} / MA40: {fp(m.get('ma40'))}
10선 돌파: {m.get('ma10_cross','N/A')}  괴리율: {_f(m.get('ma10_gap_pct'),2)}%
MA배열: {m.get('ma_align','N/A')}
RSI: {m.get('rsi_signal','N/A')}

═══ 월봉 캔들 패턴 (최근 3봉) ═══
{m_pat['candles']}
{fmt_pat(m_pat)}

═══ 일목균형표 (일봉 기준) ═══
{ichi_txt}

═══ 5일선 풀백 시그널 (검증된 매매 룰) ═══
{pullback_txt}

═══ 엘리엇 파동 (일봉, 추진 5파 검출 PoC) ═══
{elliott_txt}
{fin}

═══ {disc_label} ═══
{disc}

─────────────────────────────────────────────────────────
아래 형식으로 텔레그램 채널 메시지를 작성하세요 (이모지 포함, 1100자 이내).
**모든 섹션은 필수**입니다. 길이 맞추려고 섹션을 누락하지 말고, 각 섹션 내용을 짧게 압축하세요.

[REPORT] {name}({ticker}) [{market_tag}]
💼 {qty:,}주 | 현재 {currency}{fp(curr)}{chg_str}

🔭 큰그림(월봉)
· MA10: {{월봉 10선 상황 + 돌파 여부}}
· 패턴: {{월봉 추세전환 신호}}

[WEEKLY] 중기(주봉)
· MA10: {{주봉 10선 상황 + 돌파 여부}}
· 패턴: {{주봉 추세전환 신호}}

[SINGLE] 단기(일봉)
· RSI: {{과매도 진입/탈출 여부}}
· 패턴: {{일봉 추세전환 신호}}
· MACD: {{크로스 여부}}

☁️ 일목균형표
· {{구름 위치}} | 전환 {fp(ichi.get('tenkan'))} / 기준 {fp(ichi.get('kijun'))}
· 지지: {{지지레벨}} | 저항: {{저항레벨}}
· {{전환/기준선 크로스 여부}}

🎯 5일선 풀백 시그널
· {{시그널 발생 시: 강도(🔥STRONG≥4.0 / 📈MODERATE≥3.0 / 👁️WATCH≥2.0) + 현재 gap_atr 값.
   예: "🔥 STRONG — gap +4.21 ATR (검증된 sweet spot)" /
       "📈 MODERATE — gap +3.15 ATR (보조)" /
       "👁️ WATCH — gap +2.34 ATR (관찰 단계)"}}
· {{시그널 발생 시: 진입·청산 가이드 한 줄로.
   예: "{self.cfg['pullback_wait_days']}거래일 대기 후 종가 진입 / 손절 -{abs(self.cfg['pullback_sl_pct']):.0f}% / 익절 +{self.cfg['pullback_tp_pct']:.0f}% / close<MA5 시 청산"}}
· {{시그널 없으면: "현재 gap X.XX ATR (시그널 없음)" 한 줄로만.
   강도별 매매 가중: STRONG → 🎯 전략 적극 반영,  MODERATE → 보조 참고,
   WATCH → 관찰만 (단독 진입 근거 안 됨)}}

🌊 엘리엇 파동(일봉)
· {{검출됐으면: "방향 + 상태 (등급 X, 신뢰도 XX/100)" 한 줄.
   예: "상승 추진 5파 종료 임박 (등급 B, 74.4/100)"}}
· {{검출됐으면: 다이버전스/연장 경고 있으면 한 줄로 명시. 없으면 생략}}
· {{미검출이면: "명확한 5파 카운트 없음" 한 줄로만}}
· {{매매 가중: B등급 이상이면 🎯 전략에 반영, C/D 는 참고만}}

[DART] {disc_label}
· {{최근 2영업일(T-0/T-1) 공시가 없으면 "특이 없음" 한 줄.
   있으면 각 공시를 한 줄씩 중요도 이모지와 함께 나열하고,
   바로 아래에 2~3줄로 핵심 내용·투자 관점 의미를 요약}}

🎯 전략: 매수고려 / 관망 / 비중축소 중 하나
📍 진입: {{가격}}  🛑 손절: {{가격 또는 조건}}
💰 목표: 1차 {{가격}} / 2차 {{가격}}
확신도: {{XX%}}
"""

    @property
    def cfg(self):
        return self._cfg

    @cfg.setter
    def cfg(self, v):
        self._cfg = v

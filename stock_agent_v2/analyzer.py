"""
analyzer.py - 기술적 지표 + Claude AI 분석
수정사항:
  - 일목균형표 데이터 부족 문제 해결 (limit 확대, 최소 기준 완화)
  - 평단가 제거 (보유수량만 표시)
  - 해외주식 대응 (통화 표시, 공시 스킵)
"""

import numpy as np
import pandas as pd
import anthropic
import config
from database import load_candles, load_latest_report
from dart_collector import DartCollector
from sec_collector  import SECCollector


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

    def __init__(self):
        self.client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        self.dart   = DartCollector()
        self.sec    = SECCollector()
        self.cfg    = config.INDICATOR_CONFIG

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

        # T-1 공시 (국내=DART, 해외=SEC EDGAR)
        if overseas:
            self.sec.fetch_filings(ticker, days_back=3)
            disc_text = self.sec.get_filing_summary(ticker, limit=5)
            disc_label = "SEC EDGAR 공시 (T-1)"
        else:
            self.dart.fetch_special_disclosures(ticker, days_back=3)
            disc_text  = self.dart.get_disclosure_summary(ticker, limit=5)
            disc_label = "DART 특별 공시 (T-1)" 

        # 재무 (국내만)
        report = load_latest_report(ticker) if not overseas else None

        # 보유 정보 (평단가 없음)
        info = config.get_portfolio_detail().get(ticker, {})
        name = info.get("name", ticker)
        qty  = info.get("qty", 0)
        curr = d_ind.get("current", 0)
        currency = "$" if overseas else "₩"

        prompt = self._build_prompt(
            ticker, name, qty, curr, currency, overseas,
            d_ind, w_ind, m_ind,
            d_pat, w_pat, m_pat,
            ichi, disc_text, disc_label, report,
        )

        resp = self.client.messages.create(
            model=config.CLAUDE_MODEL,
            max_tokens=1200,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text

    def _build_prompt(
        self, ticker, name, qty, curr, currency, overseas,
        d, w, m, d_pat, w_pat, m_pat, ichi, disc, disc_label, report
    ) -> str:

        fp = lambda v: _fp(v, overseas)

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
{fin}

═══ {disc_label} ═══
{disc}

─────────────────────────────────────────────────────────
아래 형식으로 텔레그램 채널 메시지를 작성하세요 (이모지 포함, 900자 이내):

[REPORT] {name}({ticker}) [{market_tag}]
💼 {qty:,}주 | 현재 {currency}{fp(curr)}

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

[DART] {disc_label}: {{주요 공시 또는 "특이 없음"}}

🎯 전략: 매수고려 / 관망 / 비중축소 중 하나
📍 진입: {{가격}}  🛑 손절: {{가격 또는 조건}}
💰 목표: 1차 {{가격}} / 2차 {{가격}}
확신도: {{XX%}}

⚡ 최종 매매 결정은 본인이 직접 판단하세요.
"""

    @property
    def cfg(self):
        return self._cfg

    @cfg.setter
    def cfg(self, v):
        self._cfg = v

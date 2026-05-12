"""자연어 시그널 스크리너 — 병렬 LLM + DB 인디케이터 캐시.

사용자가 자연어로 정의한 조건을 LLM 으로 평가하여 portfolio/universe 종목 중
매치되는 것을 찾는다. 매치된 결과는 기존 `signals` 테이블에 `rule='nl:{id}'`
또는 `rule='nl:adhoc'` 로 저장 → /signals 페이지에서도 확인 가능.

속도 최적화:
- LLM 호출은 ThreadPoolExecutor (기본 6 worker) 병렬화 — 18종목 ~7초
- 종목 지표 (RSI/MA/MACD/거래량/최근 5봉) 는 `ticker_snapshot` 테이블에 JSON 으로
  캐싱 — 캔들 update 후 첫 접근 시만 재계산, 이후 같은 일봉 날짜 동안은 cache hit
"""
from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import config


# ── 지표 payload 계산 / 포맷 ──────────────────────────────────────────

def _f(v):
    """ind dict 의 float 값 안전 추출 (None / NaN 처리)."""
    try:
        if v is None:
            return None
        f = float(v)
        # pandas NaN 체크 (NaN != NaN)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def _compute_payload(ticker: str, name: str) -> dict | None:
    """일봉 ~260봉 → LLM 컨텍스트에 쓸 모든 지표를 JSON-able dict 로.
    52주 고저까지 잡으려면 252봉 이상 필요. 캔들 없으면 None.
    """
    from database import load_candles
    from analyzer import compute_indicators

    df_d = load_candles(ticker, "D", limit=260)
    if df_d.empty:
        return None

    ma_periods = config.INDICATOR_CONFIG.get(
        "ma_periods", [5, 10, 20, 40, 60, 120]
    )
    ind = compute_indicators(df_d, ma_periods, config.INDICATOR_CONFIG)

    last5 = df_d.tail(5)
    last5_list = [
        {"date": idx.strftime("%Y-%m-%d"),
         "o": float(r.open), "h": float(r.high),
         "l": float(r.low),  "c": float(r.close),
         "v": int(r.volume)}
        for idx, r in last5.iterrows()
    ]

    return {
        "ticker":        ticker,
        "name":          name,
        # 가격
        "current":       _f(ind.get("current")),
        "prev":          _f(ind.get("prev")),
        "change_pct":    _f(ind.get("change_pct")),
        # 이동평균
        "ma":            {f"ma{p}": _f(ind[f"ma{p}"])
                          for p in ma_periods if f"ma{p}" in ind},
        "ma_align":      ind.get("ma_align"),
        "ma10_cross":    ind.get("ma10_cross"),
        "ma10_gap_pct":  _f(ind.get("ma10_gap_pct")),
        # RSI
        "rsi":           _f(ind.get("rsi")),
        "rsi_prev":      _f(ind.get("rsi_prev")),
        "rsi_signal":    ind.get("rsi_signal"),
        # MACD (compute_indicators 의 실제 키는 macd_hist / macd_cross)
        "macd_hist":     _f(ind.get("macd_hist")),
        "macd_cross":    ind.get("macd_cross"),
        # 볼린저밴드
        "bb_upper":      _f(ind.get("bb_upper")),
        "bb_middle":     _f(ind.get("bb_middle")),
        "bb_lower":      _f(ind.get("bb_lower")),
        "bb_pct":        _f(ind.get("bb_pct")),
        "bb_state":      ind.get("bb_state"),
        # 변동성
        "atr":           _f(ind.get("atr")),
        "atr_pct":       _f(ind.get("atr_pct")),
        # 거래량
        "vol":           _f(ind.get("vol")),
        "vol_ma":        _f(ind.get("vol_ma")),
        "vol_ratio":     _f(ind.get("vol_ratio")),
        # 52주
        "high_52w":      _f(ind.get("high_52w")),
        "low_52w":       _f(ind.get("low_52w")),
        "from_high":     _f(ind.get("from_high")),  # 52주 고점 대비 %
        # 최근 캔들
        "last5":         last5_list,
        "last_date":     last5_list[-1]["date"] if last5_list else "",
    }


def _format_payload(p: dict) -> str:
    """payload dict → LLM 프롬프트용 컨텍스트 문자열.
    None 값은 출력 생략 (LLM 이 헷갈리지 않게).
    """
    parts = [f"[{p['ticker']} {p.get('name', '')}]"]

    if p.get("current") is not None:
        chg = p.get("change_pct")
        chg_s = f" ({chg:+.2f}%)" if chg is not None else ""
        parts.append(f"현재가 {p['current']:,.2f}{chg_s}")

    if p.get("ma"):
        mas = ", ".join(f"{k.upper()}={v:,.2f}"
                        for k, v in p["ma"].items() if v is not None)
        if mas:
            parts.append(f"이동평균: {mas}")
    if p.get("ma_align"):
        parts.append(f"배열: {p['ma_align']}")
    if p.get("ma10_cross"):
        gap = p.get("ma10_gap_pct")
        gap_s = f" (괴리 {gap:+.2f}%)" if gap is not None else ""
        parts.append(f"{p['ma10_cross']}{gap_s}")

    if p.get("rsi") is not None:
        parts.append(f"RSI(14)={p['rsi']:.1f} — {p.get('rsi_signal', '')}")

    if p.get("macd_hist") is not None:
        parts.append(f"MACD hist={p['macd_hist']:.3f} — "
                     f"{p.get('macd_cross', '')}")

    if p.get("bb_upper") is not None and p.get("bb_lower") is not None:
        bb_pct = p.get("bb_pct")
        pct_s = f", 밴드내 위치 {bb_pct*100:.0f}%" if bb_pct is not None else ""
        parts.append(f"볼린저: 상={p['bb_upper']:,.2f} 중={p['bb_middle']:,.2f} "
                     f"하={p['bb_lower']:,.2f} ({p.get('bb_state', '')}{pct_s})")

    if p.get("atr") is not None and p.get("atr_pct") is not None:
        parts.append(f"ATR(14)={p['atr']:,.2f} ({p['atr_pct']:.2f}%)")

    if p.get("vol") is not None and p.get("vol_ratio") is not None:
        parts.append(f"거래량: {p['vol']:,.0f} ({p['vol_ratio']:.2f}× 20일평균)")

    if p.get("high_52w") is not None:
        from_high = p.get("from_high")
        fh_s = f" (현재 -{abs(from_high):.1f}%)" if from_high is not None else ""
        parts.append(f"52주 고={p['high_52w']:,.2f} / 저={p.get('low_52w', 0):,.2f}{fh_s}")

    parts.append("최근 5봉:")
    for c in p.get("last5", []):
        parts.append(
            f"  {c['date'][5:]}: O={c['o']:,.0f} H={c['h']:,.0f} "
            f"L={c['l']:,.0f} C={c['c']:,.0f} V={c['v']:,}"
        )
    return "\n".join(parts)


def _build_ticker_context(ticker: str, name: str) -> str:
    """캐시 우선 — DB snapshot 이 최신 일봉과 같으면 재사용, 아니면 갱신."""
    from database import (
        get_latest_candle_date, load_ticker_snapshot, save_ticker_snapshot
    )
    latest = get_latest_candle_date(ticker, "D")
    snap = load_ticker_snapshot(ticker) if latest else None
    if snap and snap.get("last_date") == latest:
        return _format_payload(snap["payload"])

    payload = _compute_payload(ticker, name)
    if payload is None:
        return f"[{ticker} {name}] 캔들 데이터 없음"
    try:
        save_ticker_snapshot(ticker, payload["last_date"], payload)
    except Exception as e:
        print(f"[nl_screener] {ticker} 스냅샷 저장 실패: {e}")
    return _format_payload(payload)


# ── LLM 평가 ──────────────────────────────────────────────────────────

_EVAL_PROMPT = """\
당신은 주식 스크리너입니다. 아래 자연어로 정의된 조건이 종목에 부합하는지 판단하세요.

## 시그널 조건
{signal_prompt}

## 종목 데이터 (일봉 기준)
{ticker_context}

## 답변
JSON 한 줄로 답하세요. JSON 외 다른 텍스트 금지.
형식: {{"match": true|false, "reason": "한 줄 근거 (한국어 80자 이내)"}}
"""


def _parse_json_response(text: str) -> dict | None:
    s = text.strip()
    if s.startswith("```"):
        s = s.strip("`").strip()
        if s.startswith("json"):
            s = s[4:].strip()
    if "{" in s and "}" in s:
        s = s[s.index("{"):s.rindex("}") + 1]
    try:
        return json.loads(s)
    except Exception:
        return None


@dataclass
class TickerMatch:
    ticker: str
    name:   str
    match:  bool
    reason: str


def eval_signal_for_ticker(signal_prompt: str, ticker: str, name: str,
                            analyzer=None) -> TickerMatch:
    from analyzer import StockAnalyzer
    az = analyzer or StockAnalyzer()
    ctx = _build_ticker_context(ticker, name)
    full = _EVAL_PROMPT.format(signal_prompt=signal_prompt, ticker_context=ctx)
    try:
        text, _provider = az._call_ai(full)
    except Exception as e:
        return TickerMatch(ticker, name, False, f"LLM 호출 실패: {e}")
    parsed = _parse_json_response(text)
    if not parsed:
        return TickerMatch(ticker, name, False,
                           f"응답 파싱 실패: {text[:80]}")
    return TickerMatch(
        ticker=ticker, name=name,
        match=bool(parsed.get("match")),
        reason=str(parsed.get("reason", ""))[:200],
    )


# ── 스캔 진입점 ────────────────────────────────────────────────────────

def _resolve_tickers(scope: str) -> list[tuple[str, str]]:
    if scope == "universe":
        ts = list(config.UNIVERSE)
    else:
        ts = list(config.PORTFOLIO)
    out = []
    for t in ts:
        info = (config.get_portfolio_detail().get(t)
             or config.get_universe_detail().get(t)
             or {})
        out.append((t, info.get("name", t)))
    return out


def run_nl_signal(signal_prompt: str, scope: str = "portfolio",
                  progress_cb=None) -> list[TickerMatch]:
    """전체 종목 스캔 — ThreadPoolExecutor 로 LLM 병렬 호출.
    기본 6 worker. NL_SCREENER_WORKERS 환경변수로 조정.
    """
    from analyzer import StockAnalyzer
    pairs = _resolve_tickers(scope)
    if not pairs:
        return []

    az = StockAnalyzer()  # Claude/Gemini 클라이언트는 스레드 공유 가능
    max_workers = max(1, int(os.getenv("NL_SCREENER_WORKERS", "6")))

    results: list[TickerMatch | None] = [None] * len(pairs)

    def _task(idx: int, t: str, name: str):
        return idx, eval_signal_for_ticker(signal_prompt, t, name, az)

    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_task, i, t, name)
                   for i, (t, name) in enumerate(pairs)]
        for fut in as_completed(futures):
            try:
                idx, r = fut.result()
                results[idx] = r
            except Exception as e:
                # 한 종목 실패가 전체 스캔을 막지 않도록 — 빈 TickerMatch 채움
                done_idx = next((i for i, v in enumerate(results) if v is None), -1)
                if done_idx >= 0:
                    t, name = pairs[done_idx]
                    results[done_idx] = TickerMatch(t, name, False, f"실행 실패: {e}")
            done += 1
            if progress_cb:
                r2 = results[idx] if 'idx' in locals() else None
                progress_cb(done, len(pairs), r2.ticker if r2 else "?")

    return [r for r in results if r is not None]


def save_matches_to_signals(signal_id: int | None, signal_name: str,
                             scope: str, matches: list[TickerMatch]) -> int:
    """매치 종목을 기존 `signals` 테이블에 저장 (priority='🟡')."""
    from database import get_conn, _now_kst
    rule = f"nl:{signal_id}" if signal_id is not None else "nl:adhoc"
    scan_date = _now_kst()[:10]
    rows = [
        (scan_date, m.ticker, m.name, rule, signal_name, m.reason, "🟡")
        for m in matches if m.match
    ]
    if not rows:
        return 0
    with get_conn(sync_after=True) as conn:
        conn.executemany("""
            INSERT INTO signals (scan_date, ticker, name, rule, title, detail, priority)
            VALUES (?,?,?,?,?,?,?)
        """, rows)
    return len(rows)


def run_morning_nl_signals() -> dict:
    """morning batch — enabled=1 NL 시그널 전체 실행. signals 테이블 기록."""
    from database import list_nl_signals, update_nl_signal_run
    runs = []
    total = 0
    for sig in list_nl_signals():
        if not sig.get("enabled"):
            continue
        print(f"[NL 시그널] '{sig['name']}' ({sig['scope']}) 실행 중...")
        try:
            matches = run_nl_signal(sig["prompt"], scope=sig["scope"])
            matched = [m for m in matches if m.match]
            saved = save_matches_to_signals(
                sig["id"], sig["name"], sig["scope"], matches
            )
            update_nl_signal_run(sig["id"], len(matched))
            runs.append({
                "id": sig["id"], "name": sig["name"], "scope": sig["scope"],
                "matched_count": len(matched),
                "matches": [{"ticker": m.ticker, "name": m.name,
                             "reason": m.reason} for m in matched],
            })
            total += len(matched)
            print(f"  → 매치 {len(matched)}건, signals 저장 {saved}건")
        except Exception as e:
            print(f"  [ERROR] {sig['name']}: {e}")
            runs.append({"id": sig["id"], "name": sig["name"], "error": str(e)})
    return {"runs": runs, "total_matches": total}

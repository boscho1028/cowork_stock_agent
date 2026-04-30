"""
backtest_ma5.py — MA5 추종/이격 가설 검증 (PoC)

가설:
  A. 종가가 5일선 위에 연속으로 머무를수록(추종) 다음 5/10일 수익률이 높다.
  B. 가격이 5일선에서 ATR 배수로 멀어질수록(이격) 다음 5/10일 수익률이 낮다(조정).

데이터: universe.csv 전체 종목 일봉 (DB 에 적재된 만큼).

방법:
  - 각 일자별로 streak(연속 close>MA5) 와 gap_atr=(close-MA5)/ATR14 를 계산.
  - 다음 5/10일 수익률을 기록.
  - 버킷별 median / mean / 승률 / 표본수 비교 (vs 전체 baseline).

사용:
  python backtest_ma5.py
"""
from __future__ import annotations

import io
from typing import List

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import config
from database import load_candles


# ═══════════════════════════════════════════════════════════════════════
# 1. 종목별 메트릭 계산
# ═══════════════════════════════════════════════════════════════════════

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """MA5, ATR14, streak(close>MA5 연속일), gap_atr 추가."""
    out = df.copy()
    out["ma5"] = out["close"].rolling(5).mean()

    h, l, c = out["high"], out["low"], out["close"]
    tr = pd.concat(
        [h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1
    ).max(axis=1)
    out["atr14"] = tr.rolling(14).mean()

    # close > MA5 가 끊기지 않고 이어지는 연속일수 (위반 시 0)
    above = (out["close"] > out["ma5"]).fillna(False)
    out["streak"] = above.groupby((~above).cumsum()).cumcount() + 1
    out.loc[~above, "streak"] = 0

    # ATR 단위 이격률 — 종목별 변동성 자동 정규화
    out["gap_atr"] = (out["close"] - out["ma5"]) / out["atr14"]
    return out


def add_forward_returns(df: pd.DataFrame, fwd_days=(5, 10)) -> pd.DataFrame:
    """다음 N일 수익률 컬럼 추가 (close 기준)."""
    out = df.copy()
    for n in fwd_days:
        out[f"ret_{n}d"] = out["close"].shift(-n) / out["close"] - 1
    return out


# ═══════════════════════════════════════════════════════════════════════
# 2. Universe 통합
# ═══════════════════════════════════════════════════════════════════════

def aggregate_universe(tickers: List[str], min_bars: int = 60) -> pd.DataFrame:
    """모든 종목 일봉 + 메트릭 + forward return 을 한 DataFrame 으로 결합.
    인덱스 무시하고 row 단위로 통합 — 종목·일자 무관 통계 가능."""
    frames = []
    for tk in tickers:
        try:
            df = load_candles(tk, "D", limit=2000)
        except Exception as e:
            print(f"  [SKIP] {tk}: {e}")
            continue
        if len(df) < min_bars:
            continue
        df = compute_metrics(df)
        df = add_forward_returns(df)
        df["ticker"] = tk
        # MA5/ATR14 미계산 구간 + forward 미관측 구간 제외
        df = df.dropna(subset=["ma5", "atr14", "ret_5d", "ret_10d"])
        if not df.empty:
            frames.append(df)
    return (pd.concat(frames, ignore_index=True)
            if frames else pd.DataFrame())


# ═══════════════════════════════════════════════════════════════════════
# 3. 버킷 통계
# ═══════════════════════════════════════════════════════════════════════

def _stats(sub: pd.DataFrame, fwd_cols=("ret_5d", "ret_10d")) -> dict:
    out = {"n": len(sub)}
    for col in fwd_cols:
        r = sub[col].dropna()
        out[f"{col}_med"]  = float(r.median() * 100) if len(r) else np.nan
        out[f"{col}_mean"] = float(r.mean() * 100)   if len(r) else np.nan
        out[f"{col}_win"]  = float((r > 0).mean() * 100) if len(r) else np.nan
    return out


def streak_table(combined: pd.DataFrame) -> pd.DataFrame:
    """가설 A: close > MA5 streak 길이별 forward 수익률."""
    buckets = [
        ("0  (close ≤ MA5)", combined["streak"] == 0),
        ("1-2 days",          combined["streak"].between(1, 2)),
        ("3-5 days",          combined["streak"].between(3, 5)),
        ("6-10 days",         combined["streak"].between(6, 10)),
        ("11-20 days",        combined["streak"].between(11, 20)),
        ("21+ days",          combined["streak"] >= 21),
    ]
    rows = [{"bucket": name, **_stats(combined[m])} for name, m in buckets]
    return pd.DataFrame(rows)


def gap_table(combined: pd.DataFrame) -> pd.DataFrame:
    """가설 B: gap_atr (ATR 단위 이격) 별 forward 수익률."""
    buckets = [
        ("< 0   (below MA5)", combined["gap_atr"] < 0),
        ("0 ~ 1 ATR",         combined["gap_atr"].between(0, 1, inclusive="left")),
        ("1 ~ 2 ATR",         combined["gap_atr"].between(1, 2, inclusive="left")),
        ("2 ~ 3 ATR",         combined["gap_atr"].between(2, 3, inclusive="left")),
        ("3 ~ 4 ATR",         combined["gap_atr"].between(3, 4, inclusive="left")),
        ("4+ ATR",            combined["gap_atr"] >= 4),
    ]
    rows = [{"bucket": name, **_stats(combined[m])} for name, m in buckets]
    return pd.DataFrame(rows)


def baseline(combined: pd.DataFrame) -> dict:
    return {"bucket": "ALL (baseline)", **_stats(combined)}


# ═══════════════════════════════════════════════════════════════════════
# 4. 출력
# ═══════════════════════════════════════════════════════════════════════

def _fmt(df: pd.DataFrame) -> str:
    show = df.copy()
    for c in show.columns:
        if c == "bucket" or c == "n":
            continue
        show[c] = show[c].map(lambda v: f"{v:+.2f}%" if pd.notna(v) else "—")
    return show.to_string(index=False)


def plot_buckets(streak_df: pd.DataFrame, gap_df: pd.DataFrame, out_path: str):
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))

    def _bar(ax, df, col, title, color):
        ax.bar(df["bucket"], df[col], color=color, alpha=0.8)
        for i, v in enumerate(df[col]):
            if pd.notna(v):
                ax.text(i, v, f"{v:+.2f}%", ha="center",
                        va="bottom" if v >= 0 else "top",
                        fontsize=8, color="black")
        ax.axhline(0, color="black", lw=0.5)
        ax.set_title(title, fontsize=11)
        ax.set_ylabel("median forward return (%)", fontsize=9)
        ax.tick_params(axis="x", labelsize=8, rotation=20)
        ax.grid(axis="y", linewidth=0.3, alpha=0.5)

    _bar(axes[0, 0], streak_df, "ret_5d_med",  "A. close>MA5 streak  →  next 5d (median)",  "#4ecdc4")
    _bar(axes[0, 1], streak_df, "ret_10d_med", "A. close>MA5 streak  →  next 10d (median)", "#4ecdc4")
    _bar(axes[1, 0], gap_df,    "ret_5d_med",  "B. gap (ATR units)  →  next 5d (median)",   "#ef9a9a")
    _bar(axes[1, 1], gap_df,    "ret_10d_med", "B. gap (ATR units)  →  next 10d (median)",  "#ef9a9a")

    plt.suptitle("MA5 momentum vs extension — universe daily backtest", fontsize=13)
    plt.tight_layout()
    plt.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)


# ═══════════════════════════════════════════════════════════════════════
# 5. 메인
# ═══════════════════════════════════════════════════════════════════════

def main():
    universe = list(config.get_universe_detail().keys())
    print(f"Universe: {len(universe)}종목 — 일봉 로딩·메트릭 계산 중...")

    combined = aggregate_universe(universe)
    if combined.empty:
        print("데이터 없음")
        return

    n_total = len(combined)
    n_tickers = combined["ticker"].nunique()
    date_min = combined["close"].index  # ignore — combined uses range index
    print(f"분석 row: {n_total:,}개  ({n_tickers}종목)")

    base = baseline(combined)
    streak_df = streak_table(combined)
    gap_df    = gap_table(combined)

    print("\n=== Baseline (모든 일자, 무조건) ===")
    print(_fmt(pd.DataFrame([base])))

    print("\n=== A. close > MA5 연속일수 → forward return ===")
    print(_fmt(streak_df))

    print("\n=== B. ATR 단위 이격률 → forward return ===")
    print(_fmt(gap_df))

    out_png = "backtest_ma5_result.png"
    plot_buckets(streak_df, gap_df, out_png)
    print(f"\n차트 저장: {out_png}")


if __name__ == "__main__":
    main()

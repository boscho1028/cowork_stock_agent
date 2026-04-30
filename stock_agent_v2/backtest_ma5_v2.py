"""
backtest_ma5_v2.py — MA5 추종/이격 가설 검증 Round 2

Round 1 (backtest_ma5.py) 결과:
  - 가설 A 부분 검증: 짧은 streak 무의미, 11일+ 부터 모멘텀 명확
  - 가설 B 반대 검증: 이격 클수록 추가 상승 (조정 아님)
  - 한계: median 만 측정해서 단기(1~3일) 조정·테일 리스크 미관측

Round 2 추가:
  A. 단기 조정 검증 — forward 1d / 2d / 3d 추가
     (사용자 직관 "이격 → 조정" 이 짧은 timeframe 에서만 통하는지)
  B. Drawdown / MFE — 다음 10일 동안의 종가 기준 최대 낙폭·최대 상승폭
     (median 만 본 한계 보완, 익절·손절 룰 설계용)

사용:
  python backtest_ma5_v2.py
"""
from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import config
from database import load_candles


FWD_HORIZONS = (1, 2, 3, 5, 10)
DRAWDOWN_WINDOW = 10


# ═══════════════════════════════════════════════════════════════════════
# 1. 메트릭 계산
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

    above = (out["close"] > out["ma5"]).fillna(False)
    out["streak"] = above.groupby((~above).cumsum()).cumcount() + 1
    out.loc[~above, "streak"] = 0

    out["gap_atr"] = (out["close"] - out["ma5"]) / out["atr14"]
    return out


def add_forward_returns(df: pd.DataFrame, fwd_days=FWD_HORIZONS) -> pd.DataFrame:
    """다음 N일 종가 수익률."""
    out = df.copy()
    for n in fwd_days:
        out[f"ret_{n}d"] = out["close"].shift(-n) / out["close"] - 1
    return out


def add_forward_extremes(df: pd.DataFrame, window: int = DRAWDOWN_WINDOW) -> pd.DataFrame:
    """다음 window 일 종가 기준 최대 낙폭(MDD)·최대 상승폭(MFE) 추가.

    MDD = min(close[t+1..t+W]) / close[t] - 1   (음수 또는 0)
    MFE = max(close[t+1..t+W]) / close[t] - 1   (양수 또는 0)

    pandas 트릭: rolling(W) 의 min/max 는 [k-W+1..k] 구간이므로,
    shift(-W) 로 한 칸씩 위로 옮기면 row t 에는 [t+1..t+W] 의 min/max 가 위치.
    """
    out = df.copy()
    fwd_min = out["close"].rolling(window).min().shift(-window)
    fwd_max = out["close"].rolling(window).max().shift(-window)
    out[f"mdd_{window}d"] = fwd_min / out["close"] - 1
    out[f"mfe_{window}d"] = fwd_max / out["close"] - 1
    return out


# ═══════════════════════════════════════════════════════════════════════
# 2. Universe 통합
# ═══════════════════════════════════════════════════════════════════════

def aggregate_universe(tickers: List[str], min_bars: int = 60) -> pd.DataFrame:
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
        df = add_forward_extremes(df)
        df["ticker"] = tk
        # 메트릭·forward 모두 관측되는 row 만
        keep = ["ma5", "atr14"] + [f"ret_{n}d" for n in FWD_HORIZONS] + \
               [f"mdd_{DRAWDOWN_WINDOW}d", f"mfe_{DRAWDOWN_WINDOW}d"]
        df = df.dropna(subset=keep)
        if not df.empty:
            frames.append(df)
    return (pd.concat(frames, ignore_index=True)
            if frames else pd.DataFrame())


# ═══════════════════════════════════════════════════════════════════════
# 3. 통계
# ═══════════════════════════════════════════════════════════════════════

def _stats(sub: pd.DataFrame) -> dict:
    out = {"n": len(sub)}
    # 단기·중기 forward median (가설 A 검증용 — 단기 조정 패턴)
    for n in FWD_HORIZONS:
        r = sub[f"ret_{n}d"].dropna()
        out[f"r{n}_med"] = float(r.median() * 100) if len(r) else np.nan
    # 테일 리스크 (가설 B 검증용 — 평균 외 위험 측정)
    mdd = sub[f"mdd_{DRAWDOWN_WINDOW}d"].dropna()
    mfe = sub[f"mfe_{DRAWDOWN_WINDOW}d"].dropna()
    out["mdd_med"] = float(mdd.median() * 100) if len(mdd) else np.nan
    out["mdd_p25"] = float(mdd.quantile(0.25) * 100) if len(mdd) else np.nan  # 하위 1/4 = 더 큰 낙폭
    out["mfe_med"] = float(mfe.median() * 100) if len(mfe) else np.nan
    out["mfe_p75"] = float(mfe.quantile(0.75) * 100) if len(mfe) else np.nan  # 상위 1/4 = 더 큰 상승
    return out


def streak_table(combined: pd.DataFrame) -> pd.DataFrame:
    buckets = [
        ("0  (close ≤ MA5)", combined["streak"] == 0),
        ("1-2 days",          combined["streak"].between(1, 2)),
        ("3-5 days",          combined["streak"].between(3, 5)),
        ("6-10 days",         combined["streak"].between(6, 10)),
        ("11-20 days",        combined["streak"].between(11, 20)),
        ("21+ days",          combined["streak"] >= 21),
    ]
    return pd.DataFrame([{"bucket": name, **_stats(combined[m])}
                         for name, m in buckets])


def gap_table(combined: pd.DataFrame) -> pd.DataFrame:
    buckets = [
        ("< 0   (below MA5)", combined["gap_atr"] < 0),
        ("0 ~ 1 ATR",         combined["gap_atr"].between(0, 1, inclusive="left")),
        ("1 ~ 2 ATR",         combined["gap_atr"].between(1, 2, inclusive="left")),
        ("2 ~ 3 ATR",         combined["gap_atr"].between(2, 3, inclusive="left")),
        ("3 ~ 4 ATR",         combined["gap_atr"].between(3, 4, inclusive="left")),
        ("4+ ATR",            combined["gap_atr"] >= 4),
    ]
    return pd.DataFrame([{"bucket": name, **_stats(combined[m])}
                         for name, m in buckets])


def baseline(combined: pd.DataFrame) -> dict:
    return {"bucket": "ALL (baseline)", **_stats(combined)}


# ═══════════════════════════════════════════════════════════════════════
# 4. 출력 포맷터
# ═══════════════════════════════════════════════════════════════════════

def _fmt(df: pd.DataFrame, cols: list[str]) -> str:
    show = df[["bucket", "n"] + cols].copy()
    for c in cols:
        show[c] = show[c].map(lambda v: f"{v:+.2f}%" if pd.notna(v) else "—")
    return show.to_string(index=False)


# ═══════════════════════════════════════════════════════════════════════
# 5. 시각화
# ═══════════════════════════════════════════════════════════════════════

def plot_round2(
    streak_df: pd.DataFrame,
    gap_df: pd.DataFrame,
    out_path: str,
):
    """4-pane 차트:
       1. A streak — horizon별 median return (line plot)
       2. B gap_atr — horizon별 median return (line plot)
       3. A streak — MDD vs MFE (양옆 bar)
       4. B gap_atr — MDD vs MFE (양옆 bar)
    """
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    horizons = list(FWD_HORIZONS)
    horizon_cols = [f"r{n}_med" for n in horizons]

    def _line(ax, df, title, palette):
        for i, row in df.iterrows():
            ys = [row[c] for c in horizon_cols]
            ax.plot(horizons, ys, marker="o", lw=1.6, color=palette[i],
                    label=f"{row['bucket']} (n={row['n']:,})")
        ax.axhline(0, color="black", lw=0.5)
        ax.set_xlabel("forward horizon (days)", fontsize=9)
        ax.set_ylabel("median return (%)", fontsize=9)
        ax.set_title(title, fontsize=11)
        ax.set_xticks(horizons)
        ax.legend(fontsize=7, loc="best")
        ax.grid(linewidth=0.3, alpha=0.5)

    def _mdd_mfe_bars(ax, df, title, color_mdd, color_mfe):
        x = np.arange(len(df))
        w = 0.35
        # mdd_p25 = 하위 1/4 임 (음수, 더 큰 낙폭)
        ax.bar(x - w/2, df["mdd_p25"], width=w, color=color_mdd, alpha=0.85,
               label="MDD p25 (worst quartile, 10d)")
        # mfe_p75 = 상위 1/4 임 (양수, 더 큰 상승)
        ax.bar(x + w/2, df["mfe_p75"], width=w, color=color_mfe, alpha=0.85,
               label="MFE p75 (best quartile, 10d)")
        for i, (m, M) in enumerate(zip(df["mdd_p25"], df["mfe_p75"])):
            if pd.notna(m):
                ax.text(i - w/2, m, f"{m:+.1f}%", ha="center", va="top", fontsize=7)
            if pd.notna(M):
                ax.text(i + w/2, M, f"{M:+.1f}%", ha="center", va="bottom", fontsize=7)
        ax.axhline(0, color="black", lw=0.5)
        ax.set_xticks(x)
        ax.set_xticklabels(df["bucket"], rotation=20, fontsize=8)
        ax.set_ylabel("return (%)", fontsize=9)
        ax.set_title(title, fontsize=11)
        ax.legend(fontsize=7, loc="best")
        ax.grid(axis="y", linewidth=0.3, alpha=0.5)

    streak_palette = ["#9e9e9e", "#bdbdbd", "#80cbc4", "#4db6ac",
                     "#26a69a", "#00897b"]
    gap_palette    = ["#9e9e9e", "#bdbdbd", "#ffab91", "#ff8a65",
                     "#ff7043", "#e64a19"]

    _line(axes[0, 0], streak_df,
          "A. close>MA5 streak — forward median return by horizon",
          streak_palette)
    _line(axes[0, 1], gap_df,
          "B. gap (ATR units) — forward median return by horizon",
          gap_palette)
    _mdd_mfe_bars(axes[1, 0], streak_df,
                  "A. close>MA5 streak — 10d tail risk (MDD vs MFE)",
                  "#ef5350", "#26a69a")
    _mdd_mfe_bars(axes[1, 1], gap_df,
                  "B. gap (ATR units) — 10d tail risk (MDD vs MFE)",
                  "#ef5350", "#26a69a")

    plt.suptitle(
        "MA5 backtest Round 2 — short-term reversion + tail risk", fontsize=13
    )
    plt.tight_layout()
    plt.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)


# ═══════════════════════════════════════════════════════════════════════
# 6. 메인
# ═══════════════════════════════════════════════════════════════════════

def main():
    universe = list(config.get_universe_detail().keys())
    print(f"Universe: {len(universe)}종목 — 메트릭 + forward + extremes 계산 중...")

    combined = aggregate_universe(universe)
    if combined.empty:
        print("데이터 없음")
        return

    print(f"분석 row: {len(combined):,}개  ({combined['ticker'].nunique()}종목)")

    base = baseline(combined)
    streak_df = streak_table(combined)
    gap_df    = gap_table(combined)

    fwd_cols = [f"r{n}_med" for n in FWD_HORIZONS]
    tail_cols = ["mdd_med", "mdd_p25", "mfe_med", "mfe_p75"]

    print("\n=== Baseline (모든 일자, 무조건) ===")
    print(_fmt(pd.DataFrame([base]), fwd_cols + tail_cols))

    print("\n=== A. close > MA5 streak — forward median return ===")
    print(_fmt(streak_df, fwd_cols))

    print("\n=== A. close > MA5 streak — 10d tail risk ===")
    print(_fmt(streak_df, tail_cols))

    print("\n=== B. ATR gap — forward median return ===")
    print(_fmt(gap_df, fwd_cols))

    print("\n=== B. ATR gap — 10d tail risk ===")
    print(_fmt(gap_df, tail_cols))

    out_png = "backtest_ma5_v2_result.png"
    plot_round2(streak_df, gap_df, out_png)
    print(f"\n차트 저장: {out_png}")


if __name__ == "__main__":
    main()

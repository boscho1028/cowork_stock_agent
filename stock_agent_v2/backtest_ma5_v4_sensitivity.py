"""
backtest_ma5_v4_sensitivity.py — Pullback 룰 robustness 검증

Round 3 결과: gap≥4 ATR + 2일 대기 → 통계적으로 유의 (p=0.014)
But n=27 표본 작음 → 임계값에 결과가 robust 한지 점검 필요.

Grid:
  gap_threshold ∈ {3.5, 4.0, 4.5}
  wait          ∈ {1, 2, 3}
  → 9 조합

청산 룰 (모두 동일, Round 3 와 일치):
  손절 -10%, 익절 +20%, streak 깨짐, max 보유 10일

각 조합별 측정: n / mean / median / win_rate / profit_factor / cum_return / max_dd / p
Robust 판정 기준: 9 조합 중 다수가 p < 0.05 + mean > baseline + max_dd 합리적.

사용:
  python backtest_ma5_v4_sensitivity.py
"""
from __future__ import annotations

from typing import List, Optional

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import config
from database import load_candles
from backtest_ma5_v3_simulation import (
    compute_metrics, trade_stats, t_test_one, t_test_welch,
    baseline_returns, _evaluate_exit, Trade,
)


# ═══════════════════════════════════════════════════════════════════════
# 1. Pullback 시뮬 (파라미터화 — gap_threshold, wait, sl, tp, max_hold)
# ═══════════════════════════════════════════════════════════════════════

def simulate_pullback(
    df: pd.DataFrame, ticker: str,
    gap_threshold: float, wait: int,
    sl: float = -0.10, tp: float = +0.20, max_hold: int = 10,
) -> List[Trade]:
    trades: List[Trade] = []
    in_pos = False
    entry_idx = -1
    entry_px = 0.0
    pending_entry: Optional[int] = None

    for i in range(len(df)):
        row = df.iloc[i]
        if any(pd.isna(row[c]) for c in ("ma5", "atr14", "gap_atr")):
            continue

        if not in_pos and pending_entry == i:
            in_pos = True
            entry_idx = i
            entry_px = float(row["close"])
            pending_entry = None

        if in_pos:
            held = i - entry_idx
            reason = _evaluate_exit(entry_px, row, held, sl, tp, max_hold)
            if reason:
                trades.append(Trade(
                    ticker, entry_idx, i, held, entry_px,
                    row["close"], row["close"] / entry_px - 1, reason))
                in_pos = False

        if not in_pos and pending_entry is None:
            if row["gap_atr"] >= gap_threshold:
                pending_entry = i + wait

    return trades


# ═══════════════════════════════════════════════════════════════════════
# 2. Grid 시뮬
# ═══════════════════════════════════════════════════════════════════════

GAP_GRID  = [3.5, 4.0, 4.5]
WAIT_GRID = [1, 2, 3]


def run_grid(tickers: List[str]) -> tuple[pd.DataFrame, np.ndarray]:
    print(f"Universe: {len(tickers)}종목 — 캔들 1회 로드 후 grid 평가 ...")

    # 1) candles 한 번만 로드 (grid 9개 모두 재사용)
    candles = {}
    for tk in tickers:
        try:
            df = load_candles(tk, "D", limit=2000)
        except Exception:
            continue
        if len(df) < 60:
            continue
        candles[tk] = compute_metrics(df)
    print(f"  로드 완료: {len(candles)}종목")

    # 2) baseline pool — 3일 보유 (룰 평균 보유 ~3일)
    base_pool: list[float] = []
    for df in candles.values():
        base_pool.extend(baseline_returns(df, hold_days=3).tolist())
    base_pool_arr = np.array(base_pool)
    print(f"  baseline (3d hold): n={len(base_pool_arr):,}, "
          f"mean={base_pool_arr.mean()*100:+.3f}%")

    # 3) grid 평가
    rows = []
    for g in GAP_GRID:
        for w in WAIT_GRID:
            all_trades: List[Trade] = []
            for tk, df in candles.items():
                all_trades.extend(simulate_pullback(df, tk, g, w))

            if not all_trades:
                rows.append(dict(gap=g, wait=w, n=0))
                continue

            stats = trade_stats(all_trades)
            t1 = t_test_one(stats["rets"])
            tw = t_test_welch(stats["rets"], base_pool_arr)
            rows.append(dict(
                gap=g, wait=w,
                n=stats["n"],
                mean=stats["mean"]*100,
                median=stats["median"]*100,
                win_rate=stats["win_rate"]*100,
                profit_factor=stats["profit_factor"],
                cum_return=stats["cum_return"]*100,
                max_dd=stats["max_dd"]*100,
                avg_hold=stats["avg_hold"],
                t1_p=t1["p"],
                welch_p=tw["p"],
                welch_d=tw.get("diff", np.nan)*100 if tw.get("diff") is not None else np.nan,
            ))

    return pd.DataFrame(rows), base_pool_arr


# ═══════════════════════════════════════════════════════════════════════
# 3. 출력
# ═══════════════════════════════════════════════════════════════════════

def _stars(p: float) -> str:
    if pd.isna(p):  return ""
    if p < 0.01:    return "***"
    if p < 0.05:    return "** "
    if p < 0.10:    return "*  "
    return "   "


def print_table(grid_df: pd.DataFrame):
    print("\n=== Pullback 룰 sensitivity grid ===")
    print(f"{'gap':>4} {'wait':>4} {'n':>5} {'mean':>8} {'med':>8} "
          f"{'win%':>6} {'PF':>6} {'cum%':>9} {'MDD%':>8} {'hold':>5} "
          f"{'p(0)':>7}     {'p(vs base)':>10}")
    for _, r in grid_df.iterrows():
        if r["n"] == 0:
            print(f"{r['gap']:>4.1f} {r['wait']:>4} {0:>5}  trades 0")
            continue
        print(f"{r['gap']:>4.1f} {r['wait']:>4} {int(r['n']):>5} "
              f"{r['mean']:>+7.2f}% {r['median']:>+7.2f}% "
              f"{r['win_rate']:>5.1f}% {r['profit_factor']:>5.2f} "
              f"{r['cum_return']:>+8.1f}% {r['max_dd']:>+7.2f}% "
              f"{r['avg_hold']:>4.1f}d "
              f"{r['t1_p']:>6.4f}{_stars(r['t1_p']):<3} "
              f"{r['welch_p']:>9.4f}{_stars(r['welch_p']):<3}")


# ═══════════════════════════════════════════════════════════════════════
# 4. Heatmap
# ═══════════════════════════════════════════════════════════════════════

def plot_heatmap(grid_df: pd.DataFrame, out_path: str):
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))

    for ax, metric, title, cmap in [
        (axes[0], "mean",     "mean return per trade (%)", "RdYlGn"),
        (axes[1], "welch_p",  "p-value vs baseline (Welch)", "RdYlGn_r"),
        (axes[2], "n",        "trade count (n)",             "Blues"),
    ]:
        mat = grid_df.pivot(index="gap", columns="wait", values=metric)
        im = ax.imshow(mat.values, aspect="auto", cmap=cmap)
        for i, gap in enumerate(mat.index):
            for j, wait in enumerate(mat.columns):
                v = mat.iloc[i, j]
                if pd.isna(v):
                    txt = "—"
                elif metric == "n":
                    txt = f"{int(v)}"
                elif metric == "welch_p":
                    txt = f"{v:.3f}{_stars(v).strip()}"
                else:
                    txt = f"{v:+.2f}"
                ax.text(j, i, txt, ha="center", va="center",
                        fontsize=10, color="black",
                        fontweight="bold" if metric == "welch_p" and v < 0.05 else "normal")
        ax.set_xticks(range(len(mat.columns)))
        ax.set_xticklabels([f"wait {w}d" for w in mat.columns])
        ax.set_yticks(range(len(mat.index)))
        ax.set_yticklabels([f"gap {g} ATR" for g in mat.index])
        ax.set_title(title, fontsize=11)
        plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)

    plt.suptitle("Pullback rule — sensitivity grid", fontsize=13)
    plt.tight_layout()
    plt.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)


# ═══════════════════════════════════════════════════════════════════════
# 5. 메인
# ═══════════════════════════════════════════════════════════════════════

def main():
    universe = list(config.get_universe_detail().keys())
    grid_df, base_pool = run_grid(universe)
    print_table(grid_df)

    # robust 판정
    sig = grid_df[(grid_df["n"] >= 20) & (grid_df["welch_p"] < 0.05)]
    print(f"\n=== Robustness 요약 ===")
    print(f"  9 조합 중 (n≥20, p<0.05) 충족: {len(sig)}개")
    if len(sig):
        print(f"  유의 조합 평균: mean={sig['mean'].mean():+.2f}%, "
              f"PF={sig['profit_factor'].mean():.2f}, "
              f"max_dd={sig['max_dd'].mean():.2f}%")
    print("  → 5개 이상이면 robust, 2~4개 약함, 1개 이하면 fragile (outlier 의존)")

    out_png = "backtest_ma5_v4_result.png"
    plot_heatmap(grid_df, out_png)
    print(f"\n차트 저장: {out_png}")


if __name__ == "__main__":
    main()

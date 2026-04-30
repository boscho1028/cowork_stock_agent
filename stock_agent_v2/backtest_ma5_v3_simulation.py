"""
backtest_ma5_v3_simulation.py — 룰 시뮬레이션 (페이퍼 트레이드)

Round 2 결과로 도출된 두 가지 매매 룰을 실거래 시뮬레이션 후,
trade 단위 통계와 통계적 유의성(t-test)을 측정.

Rule 1 (Momentum):
  진입: close > MA5 streak 가 11일째 도달한 시점 (그 캔들 종가)
  청산:  ① 손절 -7%  ② 익절 +15%  ③ streak 깨짐 (close ≤ MA5)
        ④ 보유 max 10일

Rule 2 (Pullback):
  트리거: gap_atr ≥ 4 ATR 관측
  진입: 트리거 2 캔들 후 종가 (Round 2 에서 1~2일 단기 조정 평균 -1.9% 활용)
  청산:  ① 손절 -10%  ② 익절 +20%  ③ streak 깨짐  ④ 보유 max 10일

기준선(Baseline):
  같은 종목·전체 기간을 일별 close 기준 buy&hold 했을 때의
  N일 보유 수익률 분포 (= 무작위 진입과 통계적으로 동등).

산출:
  - trade 수 / 평균·중앙 수익률 / 승률 / profit factor / max DD
  - one-sample t-test (mean ≠ 0) — p-value 정규근사
  - two-sample t-test (rule vs baseline) — Welch's t
  - 누적 equity curve PNG

사용:
  python backtest_ma5_v3_simulation.py
"""
from __future__ import annotations

import math
from dataclasses import dataclass, asdict
from typing import List, Optional

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import config
from database import load_candles


# ═══════════════════════════════════════════════════════════════════════
# 0. 설정
# ═══════════════════════════════════════════════════════════════════════

RULE1 = dict(streak_threshold=11, sl=-0.07, tp=+0.15, max_hold=10)
RULE2 = dict(gap_threshold=4.0,   wait=2,   sl=-0.10, tp=+0.20, max_hold=10)


# ═══════════════════════════════════════════════════════════════════════
# 1. 메트릭 (v2 와 동일 — close-based)
# ═══════════════════════════════════════════════════════════════════════

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().reset_index(drop=True)  # 정수 idx 보장
    out["ma5"] = out["close"].rolling(5).mean()

    h, l, c = out["high"], out["low"], out["close"]
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()],
                   axis=1).max(axis=1)
    out["atr14"] = tr.rolling(14).mean()

    above = (out["close"] > out["ma5"]).fillna(False)
    out["streak"] = above.groupby((~above).cumsum()).cumcount() + 1
    out.loc[~above, "streak"] = 0

    out["gap_atr"] = (out["close"] - out["ma5"]) / out["atr14"]
    return out


# ═══════════════════════════════════════════════════════════════════════
# 2. Trade 시뮬레이션 (close-only, 슬리피지·수수료 무시)
# ═══════════════════════════════════════════════════════════════════════

@dataclass
class Trade:
    ticker: str
    entry_idx: int
    exit_idx: int
    hold_days: int
    entry_price: float
    exit_price: float
    ret: float
    exit_reason: str  # 'SL', 'TP', 'streak_break', 'time'


def _evaluate_exit(entry_price: float, row: pd.Series, held: int,
                   sl: float, tp: float, max_hold: int) -> Optional[str]:
    ret = row["close"] / entry_price - 1
    if ret <= sl:
        return "SL"
    if ret >= tp:
        return "TP"
    if row["streak"] == 0:
        return "streak_break"
    if held >= max_hold:
        return "time"
    return None


def simulate_rule1(df: pd.DataFrame, ticker: str) -> List[Trade]:
    """streak == streak_threshold 첫 도달 시 진입 (재진입은 streak 0 으로
    리셋된 후 다시 threshold 도달해야 가능)."""
    cfg = RULE1
    trades: List[Trade] = []
    in_pos = False
    entry_idx = -1
    entry_px = 0.0

    for i in range(len(df)):
        row = df.iloc[i]
        if any(pd.isna(row[c]) for c in ("ma5", "atr14")):
            continue

        if in_pos:
            held = i - entry_idx
            reason = _evaluate_exit(entry_px, row, held,
                                    cfg["sl"], cfg["tp"], cfg["max_hold"])
            if reason:
                trades.append(Trade(
                    ticker, entry_idx, i, held, entry_px,
                    row["close"], row["close"] / entry_px - 1, reason))
                in_pos = False

        # 진입 (청산과 같은 캔들에서 동시 발생 방지: 청산 다음 캔들부터 재진입)
        if not in_pos and row["streak"] == cfg["streak_threshold"]:
            in_pos = True
            entry_idx = i
            entry_px = float(row["close"])

    return trades


def simulate_rule2(df: pd.DataFrame, ticker: str) -> List[Trade]:
    """gap_atr >= threshold 트리거 후 wait 캔들 뒤 종가 진입.
    트리거 발생 후 진입 전 다른 트리거 무시."""
    cfg = RULE2
    trades: List[Trade] = []
    in_pos = False
    entry_idx = -1
    entry_px = 0.0
    pending_entry: Optional[int] = None

    for i in range(len(df)):
        row = df.iloc[i]
        if any(pd.isna(row[c]) for c in ("ma5", "atr14", "gap_atr")):
            continue

        # 진입 (대기 만료 시점)
        if not in_pos and pending_entry == i:
            in_pos = True
            entry_idx = i
            entry_px = float(row["close"])
            pending_entry = None

        # 청산 평가
        if in_pos:
            held = i - entry_idx
            reason = _evaluate_exit(entry_px, row, held,
                                    cfg["sl"], cfg["tp"], cfg["max_hold"])
            if reason:
                trades.append(Trade(
                    ticker, entry_idx, i, held, entry_px,
                    row["close"], row["close"] / entry_px - 1, reason))
                in_pos = False

        # 트리거 감지 — pending 없고 포지션도 없을 때만
        if not in_pos and pending_entry is None:
            if row["gap_atr"] >= cfg["gap_threshold"]:
                pending_entry = i + cfg["wait"]

    return trades


# ═══════════════════════════════════════════════════════════════════════
# 3. Baseline — 동일 보유기간(N일) buy&hold 모집단
# ═══════════════════════════════════════════════════════════════════════

def baseline_returns(df: pd.DataFrame, hold_days: int) -> np.ndarray:
    """모든 캔들에서 hold_days 보유 시 수익률 (rule 의 평균 보유기간과 비교용)."""
    fwd = df["close"].shift(-hold_days) / df["close"] - 1
    return fwd.dropna().to_numpy()


# ═══════════════════════════════════════════════════════════════════════
# 4. 통계 + 유의성
# ═══════════════════════════════════════════════════════════════════════

def _norm_p_two_sided(z: float) -> float:
    """표준정규 양측 p-value (n > 30 일 때 t-분포 ≈ 정규)."""
    return 2 * (1 - 0.5 * (1 + math.erf(abs(z) / math.sqrt(2))))


def t_test_one(samples: np.ndarray, mu0: float = 0.0) -> dict:
    """One-sample t-test against mu0. n 작으면 정규근사 보수적."""
    n = len(samples)
    if n < 2:
        return dict(n=n, t=np.nan, p=np.nan)
    mean = float(np.mean(samples))
    sd = float(np.std(samples, ddof=1))
    se = sd / math.sqrt(n)
    if se == 0:
        return dict(n=n, t=np.nan, p=np.nan)
    t = (mean - mu0) / se
    return dict(n=n, mean=mean, t=t, p=_norm_p_two_sided(t))


def t_test_welch(a: np.ndarray, b: np.ndarray) -> dict:
    """Welch's two-sample t-test (등분산 가정 없이)."""
    na, nb = len(a), len(b)
    if na < 2 or nb < 2:
        return dict(t=np.nan, p=np.nan)
    ma, mb = float(np.mean(a)), float(np.mean(b))
    va, vb = float(np.var(a, ddof=1)), float(np.var(b, ddof=1))
    se = math.sqrt(va / na + vb / nb)
    if se == 0:
        return dict(t=np.nan, p=np.nan)
    t = (ma - mb) / se
    return dict(t=t, p=_norm_p_two_sided(t), diff=ma - mb)


def trade_stats(trades: List[Trade]) -> dict:
    if not trades:
        return dict(n=0)
    rets = np.array([t.ret for t in trades])
    holds = np.array([t.hold_days for t in trades])

    wins = rets[rets > 0]
    losses = rets[rets <= 0]
    profit_factor = (wins.sum() / abs(losses.sum())
                     if len(losses) and losses.sum() != 0 else np.inf)

    # 누적 equity (각 trade 1단위 자본 × (1+ret) 복리)
    equity = np.cumprod(1 + rets)
    peak = np.maximum.accumulate(equity)
    dd = (equity - peak) / peak
    max_dd = float(dd.min())

    # 청산 사유 분포
    reasons = {}
    for t in trades:
        reasons[t.exit_reason] = reasons.get(t.exit_reason, 0) + 1

    return dict(
        n=len(trades),
        mean=float(rets.mean()),
        median=float(np.median(rets)),
        std=float(rets.std(ddof=1)) if len(rets) > 1 else 0.0,
        win_rate=float((rets > 0).mean()),
        profit_factor=float(profit_factor),
        cum_return=float(equity[-1] - 1),  # 모든 trade 누적 (복리)
        max_dd=max_dd,
        avg_hold=float(holds.mean()),
        reasons=reasons,
        rets=rets,
    )


# ═══════════════════════════════════════════════════════════════════════
# 5. Universe 시뮬 + 통합
# ═══════════════════════════════════════════════════════════════════════

def run_simulation(tickers: List[str]):
    rule1_trades: List[Trade] = []
    rule2_trades: List[Trade] = []
    base_r1: List[float] = []  # baseline = rule1 평균 hold 일수 모집단
    base_r2: List[float] = []

    print(f"Universe: {len(tickers)}종목 — 시뮬레이션 중...")
    for tk in tickers:
        try:
            df = load_candles(tk, "D", limit=2000)
        except Exception as e:
            continue
        if len(df) < 60:
            continue
        df = compute_metrics(df)
        rule1_trades.extend(simulate_rule1(df, tk))
        rule2_trades.extend(simulate_rule2(df, tk))
        # baseline 은 룰별 평균 보유일수와 비교 위해 일단 전체 N일 보유 모집단 수집
        # (실제 룰의 평균 보유일수가 산출된 후 다시 매핑)
        for n in (5, 7, 10):
            base_returns = baseline_returns(df, n)
            if n == 5:  base_r1.extend(base_returns.tolist())
            if n == 10: base_r2.extend(base_returns.tolist())

    return rule1_trades, rule2_trades, np.array(base_r1), np.array(base_r2)


# ═══════════════════════════════════════════════════════════════════════
# 6. 시각화
# ═══════════════════════════════════════════════════════════════════════

def _equity_curve(rets: np.ndarray) -> np.ndarray:
    return np.cumprod(1 + rets) - 1


def plot_simulation(s1: dict, s2: dict, b_r1: np.ndarray, b_r2: np.ndarray,
                    out_path: str):
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    # 1. Rule 1 equity curve
    if s1.get("n"):
        eq1 = _equity_curve(s1["rets"]) * 100
        axes[0, 0].plot(range(len(eq1)), eq1, color="#26a69a", lw=1.4)
        axes[0, 0].set_title(f"Rule 1 (Momentum) — equity curve  n={s1['n']}", fontsize=11)
        axes[0, 0].set_xlabel("trade #", fontsize=9)
        axes[0, 0].set_ylabel("cumulative return (%)", fontsize=9)
        axes[0, 0].axhline(0, color="black", lw=0.5)
        axes[0, 0].grid(linewidth=0.3, alpha=0.5)

    # 2. Rule 2 equity curve
    if s2.get("n"):
        eq2 = _equity_curve(s2["rets"]) * 100
        axes[0, 1].plot(range(len(eq2)), eq2, color="#ef5350", lw=1.4)
        axes[0, 1].set_title(f"Rule 2 (Pullback) — equity curve  n={s2['n']}", fontsize=11)
        axes[0, 1].set_xlabel("trade #", fontsize=9)
        axes[0, 1].set_ylabel("cumulative return (%)", fontsize=9)
        axes[0, 1].axhline(0, color="black", lw=0.5)
        axes[0, 1].grid(linewidth=0.3, alpha=0.5)

    # 3. Rule 1 returns 분포 vs baseline
    def _hist(ax, rule_rets, base_rets, title, color):
        bins = np.linspace(-0.20, 0.30, 50)
        ax.hist(base_rets * 100, bins=bins * 100, alpha=0.4, color="gray",
                label=f"baseline (n={len(base_rets):,})", density=True)
        ax.hist(rule_rets * 100, bins=bins * 100, alpha=0.7, color=color,
                label=f"rule (n={len(rule_rets)})", density=True)
        ax.axvline(0, color="black", lw=0.5)
        ax.axvline(rule_rets.mean() * 100, color=color, lw=1.5, linestyle="--",
                   label=f"rule mean {rule_rets.mean()*100:+.2f}%")
        ax.axvline(base_rets.mean() * 100, color="gray", lw=1.5, linestyle="--",
                   label=f"base mean {base_rets.mean()*100:+.2f}%")
        ax.set_title(title, fontsize=11)
        ax.set_xlabel("return per trade (%)", fontsize=9)
        ax.set_ylabel("density", fontsize=9)
        ax.legend(fontsize=8)
        ax.grid(linewidth=0.3, alpha=0.5)

    if s1.get("n"):
        _hist(axes[1, 0], s1["rets"], b_r1,
              "Rule 1 vs baseline 5d-hold", "#26a69a")
    if s2.get("n"):
        _hist(axes[1, 1], s2["rets"], b_r2,
              "Rule 2 vs baseline 10d-hold", "#ef5350")

    plt.suptitle("MA5 backtest Round 3 — rule simulation", fontsize=13)
    plt.tight_layout()
    plt.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)


# ═══════════════════════════════════════════════════════════════════════
# 7. 메인
# ═══════════════════════════════════════════════════════════════════════

def _print_summary(name: str, stats: dict, t1: dict, tw: dict):
    if not stats.get("n"):
        print(f"\n=== {name} ===\n  trades 0개 — 스킵")
        return
    print(f"\n=== {name} ===")
    print(f"  trades         : {stats['n']:,}")
    print(f"  mean / median  : {stats['mean']*100:+.3f}% / {stats['median']*100:+.3f}%")
    print(f"  std            : {stats['std']*100:.3f}%")
    print(f"  win rate       : {stats['win_rate']*100:.2f}%")
    print(f"  profit factor  : {stats['profit_factor']:.3f}")
    print(f"  cumulative     : {stats['cum_return']*100:+.2f}%  (compound)")
    print(f"  max drawdown   : {stats['max_dd']*100:.2f}%")
    print(f"  avg hold days  : {stats['avg_hold']:.2f}")
    print(f"  exit reasons   : {stats['reasons']}")
    print(f"  one-sample t-test (mean ≠ 0):")
    print(f"    t = {t1['t']:+.3f},  p = {t1['p']:.4f}  "
          f"{'***유의' if t1['p'] < 0.01 else '**유의' if t1['p'] < 0.05 else '*경계' if t1['p'] < 0.10 else '무의미'}")
    print(f"  Welch t-test vs baseline:")
    print(f"    Δmean = {tw['diff']*100:+.3f}%,  t = {tw['t']:+.3f},  p = {tw['p']:.4f}  "
          f"{'***유의' if tw['p'] < 0.01 else '**유의' if tw['p'] < 0.05 else '*경계' if tw['p'] < 0.10 else '무의미'}")


def main():
    universe = list(config.get_universe_detail().keys())
    rule1_trades, rule2_trades, b_r1, b_r2 = run_simulation(universe)

    s1 = trade_stats(rule1_trades)
    s2 = trade_stats(rule2_trades)

    if s1.get("n"):
        t1_one = t_test_one(s1["rets"])
        t1_welch = t_test_welch(s1["rets"], b_r1)
        _print_summary("Rule 1 — Momentum (streak ≥ 11)", s1, t1_one, t1_welch)

    if s2.get("n"):
        t2_one = t_test_one(s2["rets"])
        t2_welch = t_test_welch(s2["rets"], b_r2)
        _print_summary("Rule 2 — Pullback (gap ≥ 4 ATR, wait 2)", s2, t2_one, t2_welch)

    print(f"\nbaseline pool — 5d-hold: n={len(b_r1):,}, mean={b_r1.mean()*100:+.3f}%")
    print(f"baseline pool — 10d-hold: n={len(b_r2):,}, mean={b_r2.mean()*100:+.3f}%")

    out_png = "backtest_ma5_v3_result.png"
    plot_simulation(s1, s2, b_r1, b_r2, out_png)
    print(f"\n차트 저장: {out_png}")


if __name__ == "__main__":
    main()

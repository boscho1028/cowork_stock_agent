"""
chart_generator.py - 일봉/주봉/월봉 기술적 분석 차트

패널 구성:
  1. 캔들 + 이동평균 + 일목균형표 (일/주/월봉 모두 구름·선행·후행 표시)
  2. 거래량 + MA20
  3. RSI(14) + Signal(6)
"""

import io
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from matplotlib.gridspec import GridSpec
from matplotlib.patches import Rectangle

import platform
if platform.system() == "Windows":
    matplotlib.rc("font", family="Malgun Gothic")
elif platform.system() == "Darwin":
    matplotlib.rc("font", family="AppleGothic")
else:
    matplotlib.rc("font", family="DejaVu Sans")
matplotlib.rcParams["axes.unicode_minus"] = False

# ── 색상 ──────────────────────────────────────────────────────────────
C = {
    "bg":         "#1a1a2e",
    "panel":      "#16213e",
    "grid":       "#2a2a4a",
    "text":       "#e0e0e0",
    "subtext":    "#9e9e9e",
    "bull":       "#e53935",   # 상승 양봉 - 빨간색
    "bear":       "#1565c0",   # 하락 음봉 - 파란색
    "tenkan":     "#ff6b6b",
    "kijun":      "#4ecdc4",
    "chikou":     "#b0bec5",
    "span_a":     "#ef9a9a",
    "span_b":     "#90caf9",
    "cloud_bull": "#e53935",
    "cloud_bear": "#1565c0",
    "vol_bull":   "#e53935",
    "vol_bear":   "#1565c0",
    "rsi_line":   "#ce93d8",
    "rsi_sig":    "#ff9800",
    "rsi_ob":     "#e53935",
    "rsi_os":     "#1565c0",
}

MA_COLORS = {
    5:   "#ffeb3b",   # 노랑
    10:  "#4fc3f7",   # 하늘
    20:  "#ff9800",   # 주황
    40:  "#aed581",   # 연두
    60:  "#e91e63",   # 분홍
    120: "#ce93d8",   # 연보라
}

# 인터벌별 기본값
_DEFAULTS = {
    "D": {"n": 80,  "cfg_key": "ma_periods",         "label": "일봉", "ichi": True, "future": True, "chikou": True},
    "W": {"n": 78,  "cfg_key": "ma_periods_weekly",   "label": "주봉", "ichi": True, "future": True, "chikou": True},
    "M": {"n": 36,  "cfg_key": "ma_periods_monthly",  "label": "월봉", "ichi": True, "future": True, "chikou": True},
}


def generate_chart(
    df_daily:  pd.DataFrame,
    ticker:    str,
    name:      str,
    cfg:       dict,
    interval:  str = "D",
    n_candles: int = None,
) -> bytes:
    """캔들차트 이미지 생성 → PNG bytes 반환"""
    if df_daily.empty or len(df_daily) < 5:
        return _error_image(ticker, "데이터 부족")

    d = _DEFAULTS.get(interval, _DEFAULTS["D"])
    if n_candles is None:
        n_candles = d["n"]

    ma_periods   = cfg.get(d["cfg_key"], [5, 20, 60, 120])
    show_ichi    = d["ichi"]
    show_future  = d["future"]
    show_chikou  = d["chikou"]
    interval_lbl = d["label"]

    os_line = cfg.get("rsi_oversold",   30)
    ob_line = cfg.get("rsi_overbought", 85)
    t_n     = cfg.get("ichimoku_tenkan",  9)
    k_n     = cfg.get("ichimoku_kijun",  26)
    sb_n    = cfg.get("ichimoku_span_b", 52)
    offset  = cfg.get("ichimoku_offset", 26)

    df = df_daily.tail(n_candles).copy()
    n  = len(df)
    xs = np.arange(n)
    c  = df["close"]

    # ── 이동평균 (전체 히스토리로 계산 후 슬라이싱) ──────────────────────
    ma_series = {}
    for p in ma_periods:
        ma = df_daily["close"].rolling(p).mean().tail(n_candles)
        valid = ma.dropna()
        if len(valid) > 0:
            ma_series[p] = (np.arange(n - len(valid), n), valid.values)

    # ── RSI + 시그널 ──────────────────────────────────────────────────────
    delta   = c.diff()
    gain    = delta.clip(lower=0).rolling(cfg.get("rsi_period", 14)).mean()
    loss    = (-delta.clip(upper=0)).rolling(cfg.get("rsi_period", 14)).mean()
    rsi     = 100 - 100 / (1 + gain / loss.replace(0, np.nan))
    rsi_sig = rsi.rolling(6).mean()

    # ── 일목균형표 ────────────────────────────────────────────────────────
    tenkan_s = kijun_s = span_a_s = span_b_s = np.full(n, np.nan)
    future_a = future_b = np.full(offset, np.nan)
    xs_future = np.arange(n, n + offset)

    if show_ichi and len(df_daily) >= t_n:
        extra = max(sb_n, offset) + n_candles
        src   = df_daily.tail(extra).copy()

        def _mid(s, p):
            return (s["high"].rolling(p).max() + s["low"].rolling(p).min()) / 2

        tenk = _mid(src, t_n)
        kij  = _mid(src, k_n)
        sa   = ((tenk + kij) / 2).shift(offset)
        sb   = _mid(src, sb_n).shift(offset)

        tenkan_s = tenk.tail(n_candles).values
        kijun_s  = kij.tail(n_candles).values
        span_a_s = sa.tail(n_candles).values
        span_b_s = sb.tail(n_candles).values

        if show_future and len(df_daily) >= sb_n:
            future_a = ((tenk + kij) / 2).tail(offset).values
            future_b = _mid(src, sb_n).tail(offset).values

    # ── 레이아웃 ─────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(14, 9), facecolor=C["bg"])
    gs  = GridSpec(3, 1, figure=fig, height_ratios=[4.5, 1.5, 1.5], hspace=0.04)
    ax_c = fig.add_subplot(gs[0])
    ax_v = fig.add_subplot(gs[1], sharex=ax_c)
    ax_r = fig.add_subplot(gs[2], sharex=ax_c)

    for ax in [ax_c, ax_v, ax_r]:
        ax.set_facecolor(C["panel"])
        ax.tick_params(colors=C["text"], labelsize=7.5)
        ax.yaxis.tick_right()
        ax.grid(color=C["grid"], linewidth=0.4, alpha=0.6)
        for spine in ax.spines.values():
            spine.set_edgecolor(C["grid"])

    # ── 패널1: 일목 구름 ─────────────────────────────────────────────────
    if show_ichi:
        # 과거 구름
        for i in range(n - 1):
            sa, sb = span_a_s[i], span_b_s[i]
            if np.isnan(sa) or np.isnan(sb):
                continue
            col = C["cloud_bull"] if sa >= sb else C["cloud_bear"]
            ax_c.fill_between([i, i+1], [min(sa,sb)]*2, [max(sa,sb)]*2,
                              color=col, alpha=0.15, zorder=1)

        # 미래 구름 (일봉만)
        if show_future:
            for i in range(len(xs_future) - 1):
                fa, fb = future_a[i], future_b[i]
                if np.isnan(fa) or np.isnan(fb):
                    continue
                col = C["cloud_bull"] if fa >= fb else C["cloud_bear"]
                ax_c.fill_between([xs_future[i], xs_future[i+1]],
                                  [min(fa,fb)]*2, [max(fa,fb)]*2,
                                  color=col, alpha=0.22, hatch="////",
                                  linewidth=0, zorder=1)

        # 선행스팬 선
        span_xs = np.concatenate([xs, xs_future]) if show_future else xs
        sa_vals = np.concatenate([span_a_s, future_a]) if show_future else span_a_s
        sb_vals = np.concatenate([span_b_s, future_b]) if show_future else span_b_s
        ax_c.plot(span_xs, sa_vals, color=C["span_a"], lw=0.7, alpha=0.7, zorder=2, label="선행A")
        ax_c.plot(span_xs, sb_vals, color=C["span_b"], lw=0.7, alpha=0.7, zorder=2, label="선행B")

        # 기준선/전환선
        ax_c.plot(xs, kijun_s,  color=C["kijun"],  lw=1.3, zorder=3, label=f"기준({k_n})")
        ax_c.plot(xs, tenkan_s, color=C["tenkan"], lw=1.1, zorder=3, label=f"전환({t_n})")

        # 후행스팬 (일봉만)
        if show_chikou:
            ck_xs   = xs - offset
            ck_mask = ck_xs >= 0
            ax_c.plot(ck_xs[ck_mask], c.values[ck_mask],
                      color=C["chikou"], lw=0.9, linestyle="--",
                      alpha=0.7, zorder=3, label="후행")

    # ── 패널1: 캔들 ───────────────────────────────────────────────────────
    price_range = df["high"].max() - df["low"].min()
    min_body_h  = price_range * 0.002
    w_body = 0.55
    for i, (_, row) in enumerate(df.iterrows()):
        is_bull = row["close"] >= row["open"]
        col     = C["bull"] if is_bull else C["bear"]
        lo      = min(row["open"], row["close"])
        hi      = max(row["open"], row["close"])
        ax_c.plot([i, i], [row["low"], row["high"]], color=col, lw=0.9, zorder=4)
        body_h = hi - lo
        if body_h > min_body_h:
            ax_c.add_patch(Rectangle((i - w_body/2, lo), w_body, body_h, color=col, zorder=5))
        else:
            mid = (lo + hi) / 2
            lo_d = max(row["low"],  mid - min_body_h / 2)
            hi_d = min(row["high"], mid + min_body_h / 2)
            ax_c.add_patch(Rectangle((i - w_body/2, lo_d), w_body, hi_d - lo_d, color=col, zorder=5))

    # ── 패널1: 이동평균 ───────────────────────────────────────────────────
    for p, (xs_ma, vals) in ma_series.items():
        color = MA_COLORS.get(p, C["subtext"])
        ax_c.plot(xs_ma, vals, color=color, lw=0.9, zorder=6, label=f"MA{p}")

    # 현재가 수평선
    ax_c.axhline(c.iloc[-1], color=C["subtext"], lw=0.5, linestyle=":", alpha=0.5)

    # 일목 현황 텍스트 (우상단, 일봉/주봉만)
    if show_ichi:
        t_val  = tenkan_s[-1]
        k_val  = kijun_s[-1]
        sa_val = span_a_s[-1]
        sb_val = span_b_s[-1]
        curr   = c.iloc[-1]

        def sf(v):
            return f"{v:,.0f}" if not np.isnan(v) else "N/A"

        if not (np.isnan(sa_val) or np.isnan(sb_val)):
            if curr > max(sa_val, sb_val):
                cloud_txt, cloud_col = "구름 위 (강세)", C["cloud_bull"]
            elif curr < min(sa_val, sb_val):
                cloud_txt, cloud_col = "구름 아래 (약세)", C["cloud_bear"]
            else:
                cloud_txt, cloud_col = "구름 안 (중립)", C["subtext"]
        else:
            cloud_txt, cloud_col = "구름 계산 중", C["subtext"]

        ax_c.text(0.995, 0.98,
                  f"전환 {sf(t_val)}  기준 {sf(k_val)}\n"
                  f"선행A {sf(sa_val)}  선행B {sf(sb_val)}\n{cloud_txt}",
                  transform=ax_c.transAxes, ha="right", va="top",
                  color=cloud_col, fontsize=7,
                  bbox=dict(boxstyle="round,pad=0.3", facecolor=C["panel"],
                            alpha=0.8, edgecolor=C["grid"]))

    # 범례
    ax_c.legend(loc="upper left", fontsize=6.5, ncol=5,
                facecolor=C["panel"], labelcolor=C["text"],
                edgecolor=C["grid"], framealpha=0.85)

    # 제목
    curr  = c.iloc[-1]
    prev  = c.iloc[-2] if n > 1 else curr
    chg   = (curr / prev - 1) * 100
    sign  = "+" if chg >= 0 else ""
    ax_c.set_title(
        f"[{interval_lbl}]  {name}({ticker})   {curr:,.0f}   {sign}{chg:.2f}%   "
        f"[{df.index[-1].strftime('%Y-%m-%d')} 기준  |  {n}봉]",
        color=C["text"], fontsize=10, pad=7, loc="left",
    )

    # y축 범위
    all_y = list(df["low"].values) + list(df["high"].values)
    if show_ichi:
        for arr in [span_a_s, span_b_s, future_a, future_b]:
            all_y += list(arr[~np.isnan(arr)])
    pmin, pmax = np.nanmin(all_y), np.nanmax(all_y)
    mg = (pmax - pmin) * 0.05
    ax_c.set_ylim(pmin - mg, pmax + mg)

    # ── 패널2: 거래량 ────────────────────────────────────────────────────
    for i, (_, row) in enumerate(df.iterrows()):
        col = C["vol_bull"] if row["close"] >= row["open"] else C["vol_bear"]
        ax_v.bar(i, row["volume"], color=col, width=0.7, alpha=0.85)
    ax_v.plot(xs, df["volume"].rolling(20).mean(), color=MA_COLORS[20], lw=0.9, alpha=0.8)
    ax_v.set_ylabel("VOL", color=C["subtext"], fontsize=7, labelpad=2)

    def vol_fmt(x, _):
        if x >= 1e8: return f"{x/1e8:.0f}억"
        if x >= 1e4: return f"{x/1e4:.0f}만"
        return f"{x:.0f}"
    ax_v.yaxis.set_major_formatter(mtick.FuncFormatter(vol_fmt))

    # ── 패널3: RSI + 시그널 ──────────────────────────────────────────────
    ax_r.plot(xs, rsi,     color=C["rsi_line"], lw=1.1, zorder=3, label=f"RSI(14)")
    ax_r.plot(xs, rsi_sig, color=C["rsi_sig"],  lw=0.9, zorder=3, label="Signal(6)")
    ax_r.axhline(ob_line, color=C["rsi_ob"], lw=0.8, linestyle="--", alpha=0.8)
    ax_r.axhline(os_line, color=C["rsi_os"], lw=0.8, linestyle="--", alpha=0.8)
    ax_r.axhline(50,       color=C["grid"],   lw=0.4, alpha=0.6)
    ax_r.fill_between(xs, rsi, ob_line, where=(rsi >= ob_line), color=C["rsi_ob"], alpha=0.18, zorder=1)
    ax_r.fill_between(xs, rsi, os_line, where=(rsi <= os_line), color=C["rsi_os"], alpha=0.18, zorder=1)

    curr_rsi = rsi.iloc[-1]
    if not np.isnan(curr_rsi):
        rsi_col = C["rsi_ob"] if curr_rsi >= ob_line else (C["rsi_os"] if curr_rsi <= os_line else C["rsi_line"])
        ax_r.annotate(f"RSI {curr_rsi:.1f}", xy=(n-1, curr_rsi),
                      xytext=(n-14, curr_rsi + (7 if curr_rsi < 75 else -10)),
                      color=rsi_col, fontsize=7.5, fontweight="bold",
                      arrowprops=dict(arrowstyle="-", color=rsi_col, lw=0.5))

    ax_r.set_ylim(0, 100)
    ax_r.set_yticks([os_line, 50, ob_line])
    ax_r.set_ylabel("RSI", color=C["subtext"], fontsize=7, labelpad=2)
    ax_r.legend(loc="upper left", fontsize=6.5, ncol=2,
                facecolor=C["panel"], labelcolor=C["text"],
                edgecolor=C["grid"], framealpha=0.85)

    # ── x축 날짜 ─────────────────────────────────────────────────────────
    total_w = n + (offset if show_future else 0)
    step    = max(total_w // 10, 1)
    ticks   = [i for i in range(0, n, step)]
    ax_r.set_xticks(ticks)
    ax_r.set_xticklabels([df.index[i].strftime("%m/%d") for i in ticks],
                         color=C["text"], fontsize=7.5)
    plt.setp(ax_c.get_xticklabels(), visible=False)
    plt.setp(ax_v.get_xticklabels(), visible=False)
    ax_c.set_xlim(-1, total_w)

    fig.text(0.995, 0.005, "Stock AI Agent", ha="right", va="bottom",
             color=C["grid"], fontsize=7, alpha=0.4)

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                facecolor=C["bg"], edgecolor="none")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def combine_charts_vertical(charts: dict, order=("D", "W", "M")) -> bytes:
    """일/주/월 차트 PNG bytes를 세로로 이어붙여 단일 PNG로 반환."""
    from PIL import Image

    images = []
    for key in order:
        b = charts.get(key)
        if not b:
            continue
        images.append(Image.open(io.BytesIO(b)).convert("RGB"))
    if not images:
        return b""

    target_w = max(im.width for im in images)
    resized  = []
    for im in images:
        if im.width != target_w:
            ratio = target_w / im.width
            im    = im.resize((target_w, int(im.height * ratio)), Image.LANCZOS)
        resized.append(im)

    total_h  = sum(im.height for im in resized)
    combined = Image.new("RGB", (target_w, total_h), color=C["bg"])
    y = 0
    for im in resized:
        combined.paste(im, (0, y))
        y += im.height

    buf = io.BytesIO()
    combined.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf.read()


def _error_image(ticker: str, msg: str) -> bytes:
    fig, ax = plt.subplots(figsize=(6, 2), facecolor=C["bg"])
    ax.set_facecolor(C["bg"])
    ax.text(0.5, 0.5, f"{ticker}: {msg}", ha="center", va="center",
            color=C["text"], fontsize=12)
    ax.axis("off")
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=100, bbox_inches="tight", facecolor=C["bg"])
    plt.close(fig)
    buf.seek(0)
    return buf.read()

"""
elliott_wave.py - 엘리엇 파동 자동 검출 (PoC)

검출 범위 (PoC):
  - 추진 5파 (1-2-3-4-5) 만 검출. A-B-C 조정파는 v2.
  - 일봉 기준만 적용. 다른 시간프레임은 v2.

파이프라인:
  1) ZigZag 로 의미있는 스윙 추출
  2) 6개 스윙 슬라이딩으로 5파 후보 생성
  3) 4축 검증
     - 피보나치 (HARD RULE 3개 + SOFT 점수)
     - 거래량 패턴 (3파 최대, 2/4파 감소, 5파 < 3파)
     - RSI 다이버전스 (5파 종료 임박 감지)
     - 2-4 추세선 + 1-3 평행선 채널
  4) 점수 합산 → 0~100 정규화 → A/B/C/D 등급

설계 노트:
  - 모든 후보를 노출. 게이트 없음. LLM 이 신뢰도까지 판단.
  - 마지막 봉을 포함한 "미확정 스윙" 도 후보에 넣어 진행 중인 5파를 잡는다.
  - HARD RULE 위반은 즉시 탈락 (점수 0). 정상 후보 중 최고점만 채택.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from typing import Optional


# ═══════════════════════════════════════════════════════════════════════
# 1. ZigZag 스윙 감지
# ═══════════════════════════════════════════════════════════════════════

def find_swing_points(
    df: pd.DataFrame,
    min_pct: float = 3.0,
    atr_mult: float = 2.0,
    atr_col: str = "atr_14",
) -> list[tuple[int, float, str]]:
    """
    의미있는 고점/저점 추출. 직전 스윙 대비 max(min_pct%, atr*atr_mult)
    이상 반대 방향으로 움직였을 때만 새 스윙으로 인정.

    반환: [(index, price, type), ...]  type ∈ {'H', 'L'}
          마지막 항목은 미확정 스윙 (진행 중인 파동의 끝 지점).
    """
    n = len(df)
    if n < 3:
        return []

    h = df["high"].to_numpy()
    l = df["low"].to_numpy()
    if atr_col in df.columns:
        atr = df[atr_col].to_numpy()
    else:
        atr = np.zeros(n)

    def thr(price: float, i: int) -> float:
        a = atr[i] if i < n and not np.isnan(atr[i]) else 0.0
        return max(price * min_pct / 100.0, a * atr_mult)

    swings: list[tuple[int, float, str]] = []

    # 마지막 확정 피봇
    pivot_idx: int = 0
    pivot_type: Optional[str] = None  # 'H' | 'L' | None

    # 피봇 이후 추적 중인 양쪽 극값
    ext_h_idx, ext_h = 0, h[0]
    ext_l_idx, ext_l = 0, l[0]

    for i in range(1, n):
        if h[i] > ext_h:
            ext_h, ext_h_idx = float(h[i]), i
        if l[i] < ext_l:
            ext_l, ext_l_idx = float(l[i]), i

        # H 피봇 확정 후보: 직전이 L 이거나 미정 — 위쪽 극값에서 충분히 반락했는가
        if pivot_type in (None, "L"):
            if ext_h - l[i] >= thr(ext_h, i) and ext_h_idx > pivot_idx:
                swings.append((ext_h_idx, ext_h, "H"))
                pivot_idx, pivot_type = ext_h_idx, "H"
                # 새 피봇 이후의 극값 트래킹 리셋
                ext_h_idx, ext_h = i, float(h[i])
                ext_l_idx, ext_l = i, float(l[i])
                continue

        # L 피봇 확정 후보
        if pivot_type in (None, "H"):
            if h[i] - ext_l >= thr(ext_l, i) and ext_l_idx > pivot_idx:
                swings.append((ext_l_idx, ext_l, "L"))
                pivot_idx, pivot_type = ext_l_idx, "L"
                ext_h_idx, ext_h = i, float(h[i])
                ext_l_idx, ext_l = i, float(l[i])
                continue

    # 마지막 확정 피봇 이후 미확정 극값을 잠정 스윙으로 추가.
    # (진행 중인 파동의 끝점을 5파 후보에 포함시키기 위함)
    if pivot_type == "H" and ext_l_idx > pivot_idx:
        swings.append((ext_l_idx, ext_l, "L"))
    elif pivot_type == "L" and ext_h_idx > pivot_idx:
        swings.append((ext_h_idx, ext_h, "H"))
    elif pivot_type is None:
        # 임계값을 넘은 움직임이 한 번도 없음 — 빈 결과
        return []

    return swings


# ═══════════════════════════════════════════════════════════════════════
# 2. 5파 추진파 후보 생성
# ═══════════════════════════════════════════════════════════════════════

def generate_impulse_candidates(
    swings: list[tuple[int, float, str]],
) -> list[dict]:
    """
    스윙 리스트에서 5파 추진 후보 생성.
    - 상승 추진: P0=L, P1=H, P2=L, P3=H, P4=L, P5=H  (6점, L-H 교번)
    - 하락 추진: P0=H, P1=L, P2=H, P3=L, P4=H, P5=L

    반환: [{"direction":"up"|"down", "points":[(idx,price,type), ...6]}, ...]
    """
    candidates: list[dict] = []
    n = len(swings)
    if n < 6:
        return candidates

    for i in range(n - 5):
        seq = swings[i : i + 6]
        types = [s[2] for s in seq]

        # 교번 검사
        if not all(types[j] != types[j + 1] for j in range(5)):
            continue

        direction = "up" if types[0] == "L" else "down"
        candidates.append({"direction": direction, "points": seq})

    return candidates


# ═══════════════════════════════════════════════════════════════════════
# 3. 4축 검증
# ═══════════════════════════════════════════════════════════════════════

def validate_fibonacci(candidate: dict) -> dict:
    """
    피보나치 비율 검증 + HARD RULE.

    HARD RULE (위반 시 즉시 탈락):
      1) 2파는 1파 시작점을 넘을 수 없다.
      2) 3파는 1, 5파 중 가장 짧을 수 없다.
      3) 4파는 1파 영역을 침범할 수 없다.

    SOFT RULE (점수, 만점 75):
      - 2파 되돌림 50~61.8% (+20) / 38.2~78.6% (+10)
      - 3파 161.8% 이상 (+20) / 100~161.8% (+10)
      - 4파 되돌림 23.6~38.2% (+20) / 38.2~50% (+10)
      - 5파 = 1파 등장 (±10%) 또는 50~70% (+15)
    """
    p = candidate["points"]
    is_up = candidate["direction"] == "up"

    p0, p1, p2, p3, p4, p5 = (s[1] for s in p)

    # HARD RULE 1: 2파가 1파 시작점 침범
    if is_up and p2 <= p0:
        return {"valid": False, "score": 0, "reason": "2파가 1파 시작점 침범"}
    if not is_up and p2 >= p0:
        return {"valid": False, "score": 0, "reason": "2파가 1파 시작점 침범"}

    # 파동 길이 (절댓값)
    w1 = abs(p1 - p0)
    w2 = abs(p2 - p1)
    w3 = abs(p3 - p2)
    w4 = abs(p4 - p3)
    w5 = abs(p5 - p4)

    if w1 == 0 or w3 == 0:
        return {"valid": False, "score": 0, "reason": "1파 또는 3파 길이 0"}

    # HARD RULE 2: 3파가 1·5파보다 짧으면 탈락
    if w3 < w1 and w3 < w5:
        return {"valid": False, "score": 0, "reason": "3파가 가장 짧음"}

    # HARD RULE 3: 4파가 1파 영역(=P1) 침범
    if is_up and p4 <= p1:
        return {"valid": False, "score": 0, "reason": "4파가 1파 영역 침범"}
    if not is_up and p4 >= p1:
        return {"valid": False, "score": 0, "reason": "4파가 1파 영역 침범"}

    # SOFT RULE
    score = 0
    r2 = w2 / w1
    r3 = w3 / w1
    r4 = w4 / w3
    r5 = w5 / w1

    if 0.5 <= r2 <= 0.618:
        score += 20
    elif 0.382 <= r2 <= 0.786:
        score += 10

    if r3 >= 1.618:
        score += 20
    elif r3 >= 1.0:
        score += 10

    if 0.236 <= r4 <= 0.382:
        score += 20
    elif 0.382 < r4 <= 0.5:
        score += 10

    if 0.9 <= r5 <= 1.1 or 0.5 <= r5 <= 0.7:
        score += 15

    return {
        "valid": True,
        "score": score,
        "ratios": {"w2": r2, "w3": r3, "w4": r4, "w5": r5},
        "lengths": {"w1": w1, "w2": w2, "w3": w3, "w4": w4, "w5": w5},
    }


def validate_volume(candidate: dict, df: pd.DataFrame) -> dict:
    """
    파동 구간별 평균 거래량 비교 (만점 60).
      - 3파 거래량이 1·5파보다 큼 (+25)  /  3파 > 5파만 충족 (+10)
      - 2파 거래량이 1·3파보다 작음 (+10)
      - 4파 거래량이 3·5파보다 작음 (+10)
      - 5파 거래량이 3파보다 작음 (다이버전스 약신호) (+15)
    """
    p = candidate["points"]
    idx = [s[0] for s in p]

    def avg_vol(i_start: int, i_end: int) -> float:
        if i_start >= i_end:
            return 0.0
        return float(df["volume"].iloc[i_start : i_end + 1].mean())

    v1 = avg_vol(idx[0], idx[1])
    v2 = avg_vol(idx[1], idx[2])
    v3 = avg_vol(idx[2], idx[3])
    v4 = avg_vol(idx[3], idx[4])
    v5 = avg_vol(idx[4], idx[5])

    score = 0
    if v3 > v1 and v3 > v5:
        score += 25
    elif v3 > v5:
        score += 10

    if v2 < v1 and v2 < v3:
        score += 10
    if v4 < v3 and v4 < v5:
        score += 10
    if v5 < v3:
        score += 15

    return {
        "score": score,
        "volumes": {"w1": v1, "w2": v2, "w3": v3, "w4": v4, "w5": v5},
    }


def validate_rsi_divergence(candidate: dict, df: pd.DataFrame) -> dict:
    """
    5파 종료 임박 신호로서의 RSI 다이버전스 (만점 30).
      - 상승: P5 가격 > P3 가격 인데 RSI(P5) < RSI(P3) → 베어리시 (+25)
      - 하락: P5 가격 < P3 가격 인데 RSI(P5) > RSI(P3) → 불리시 (+25)
      - 추가: 3파 끝점에서 RSI 과매수/과매도 (+5)
    """
    if "rsi_14" not in df.columns:
        return {"score": 0, "divergence": None, "reason": "RSI 미계산"}

    p = candidate["points"]
    is_up = candidate["direction"] == "up"
    idx_p3, price_p3 = p[3][0], p[3][1]
    idx_p5, price_p5 = p[5][0], p[5][1]

    rsi_p3 = float(df["rsi_14"].iloc[idx_p3])
    rsi_p5 = float(df["rsi_14"].iloc[idx_p5])

    score = 0
    divergence: Optional[str] = None

    if is_up:
        if price_p5 > price_p3 and rsi_p5 < rsi_p3:
            score += 25
            divergence = "bearish"
        if rsi_p3 >= 70:
            score += 5
    else:
        if price_p5 < price_p3 and rsi_p5 > rsi_p3:
            score += 25
            divergence = "bullish"
        if rsi_p3 <= 30:
            score += 5

    return {
        "score": score,
        "divergence": divergence,
        "rsi_p3": rsi_p3,
        "rsi_p5": rsi_p5,
    }


def validate_trendline(candidate: dict) -> dict:
    """
    2-4 추세선 + 1-3 평행 채널 검증 (만점 30).
      - 2-4 추세선이 추진 방향과 일치 (+15)
      - 5파가 1-3 평행 채널 상단/하단 부근에서 종료 (±5%) (+15)
      - 또는 throwover/failure (+5)
    """
    p = candidate["points"]
    is_up = candidate["direction"] == "up"

    x1, y1 = p[1][0], p[1][1]
    x2, y2 = p[2][0], p[2][1]
    x4, y4 = p[4][0], p[4][1]
    x5, y5 = p[5][0], p[5][1]

    if x4 == x2:
        return {"score": 0, "slope_24": 0.0, "reason": "x2=x4"}

    slope_24 = (y4 - y2) / (x4 - x2)

    score = 0
    if is_up and slope_24 > 0:
        score += 15
    elif (not is_up) and slope_24 < 0:
        score += 15

    # 1-3 평행선의 x5 지점 가격 (P1 통과, slope_24 와 동일 기울기)
    channel_top_at_x5 = y1 + slope_24 * (x5 - x1)

    if channel_top_at_x5 != 0:
        deviation = abs(y5 - channel_top_at_x5) / abs(channel_top_at_x5)
        if deviation < 0.05:
            score += 15
        elif is_up and y5 > channel_top_at_x5:
            score += 5  # throwover
        elif (not is_up) and y5 < channel_top_at_x5:
            score += 5
        elif is_up and y5 < channel_top_at_x5 * 0.97:
            score += 5  # 5파 미달 (failure)
        elif (not is_up) and y5 > channel_top_at_x5 * 1.03:
            score += 5

    return {
        "score": score,
        "slope_24": slope_24,
        "channel_top_at_p5": channel_top_at_x5,
    }


# ═══════════════════════════════════════════════════════════════════════
# 4. 보조지표 사전계산
# ═══════════════════════════════════════════════════════════════════════

def _ensure_aux_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """ZigZag·검증에 필요한 ATR(14)·RSI(14) 를 캔들 DF 에 추가.
    원본 DF 는 변경하지 않고 복사본에 컬럼을 붙여 반환."""
    out = df.copy()

    if "atr_14" not in out.columns:
        h, l, c = out["high"], out["low"], out["close"]
        tr = pd.concat(
            [h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1
        ).max(axis=1)
        out["atr_14"] = tr.rolling(14).mean()

    if "rsi_14" not in out.columns:
        delta = out["close"].diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        out["rsi_14"] = 100 - 100 / (1 + gain / loss.replace(0, np.nan))

    return out


# ═══════════════════════════════════════════════════════════════════════
# 5. 통합 진입점
# ═══════════════════════════════════════════════════════════════════════

def _grade(confidence: float) -> str:
    if confidence >= 80:
        return "A"
    if confidence >= 60:
        return "B"
    if confidence >= 40:
        return "C"
    return "D"


def _wave_status(candidate: dict, df: pd.DataFrame, score_rsi: dict) -> str:
    """현재 5파의 진행 상태 라벨링.
    - 마지막 캔들이 P5 직후이고 다이버전스 감지되면 '5파 종료 임박'
    - P5 가 마지막 봉이면 '5파 진행 중'
    - 그 외 '5파 종료 (조정 진입 가능)'
    """
    last_idx = len(df) - 1
    p5_idx = candidate["points"][5][0]

    if p5_idx == last_idx:
        if score_rsi.get("divergence"):
            return "5파 종료 임박"
        return "5파 진행 중"
    # 5파 끝난 뒤 추가 봉이 있음 = 조정 시작 추정
    return "5파 종료 (조정 진입 가능)"


def compute_elliott_wave(df: pd.DataFrame, cfg: dict) -> dict:
    """
    엘리엇 5파 추진파 검출 + 4축 검증 + 점수화.

    cfg 는 config.ELLIOTT_CONFIG 를 그대로 받음.

    반환 dict:
      available     : bool
      reason        : str (available=False 시)
      direction     : 'up' | 'down'
      current_wave  : 라벨 ('5파 진행 중' 등)
      confidence    : 0~100 정규화 점수
      grade         : 'A' | 'B' | 'C' | 'D'
      points        : [{"wave":"0".."5", "index", "date", "price"}]
      scores        : {"fib", "volume", "rsi", "trend", "raw", "max"}
      ratios        : 피보 비율 dict
      warnings      : list[str]
    """
    if df.empty or len(df) < cfg.get("min_bars", 120):
        return {
            "available": False,
            "reason": f"캔들 부족 ({len(df)}봉 / 최소 {cfg.get('min_bars', 120)}봉)",
        }

    work = _ensure_aux_indicators(df)

    # 1) 스윙 추출
    swings = find_swing_points(
        work,
        min_pct=cfg.get("swing_min_pct", 3.0),
        atr_mult=cfg.get("swing_atr_mult", 2.0),
    )
    if len(swings) < 6:
        return {
            "available": False,
            "reason": f"의미있는 스윙 부족 ({len(swings)}/6)",
        }

    # 2) 5파 후보 생성
    candidates = generate_impulse_candidates(swings)
    if not candidates:
        return {"available": False, "reason": "5파 교번 패턴 없음"}

    # 3) 4축 검증 — 정상 후보 중 최고점 채택
    max_fib = cfg.get("max_fib", 75)
    max_vol = cfg.get("max_volume", 60)
    max_rsi = cfg.get("max_rsi", 30)
    max_trd = cfg.get("max_trend", 30)
    raw_max = max_fib + max_vol + max_rsi + max_trd

    valid_results: list[dict] = []
    for cand in candidates:
        fib = validate_fibonacci(cand)
        if not fib["valid"]:
            continue
        vol = validate_volume(cand, work)
        rsi = validate_rsi_divergence(cand, work)
        trd = validate_trendline(cand)
        raw = fib["score"] + vol["score"] + rsi["score"] + trd["score"]
        valid_results.append({
            "candidate": cand,
            "fib": fib, "volume": vol, "rsi": rsi, "trend": trd,
            "raw": raw,
            "p5_idx": cand["points"][5][0],
        })

    if not valid_results:
        return {
            "available": False,
            "reason": "HARD RULE 통과 후보 없음 (불완전한 5파 구조)",
        }

    # 매매 의사결정에는 "현재 시점에 유효한 가장 최근 파동" 이 의미 있음.
    # 점수는 신뢰도 표시에만 쓰고, 채택은 P5 인덱스가 가장 큰(=최신) 후보 우선.
    # 동률(=최신 P5 가 여럿) 이면 raw 점수 높은 쪽.
    valid_results.sort(key=lambda r: (-r["p5_idx"], -r["raw"]))
    best = valid_results[0]

    # 신선도 게이트: P5 가 너무 오래됐으면 현재 추세와 무관 → 검출 안 됨 처리.
    last_idx = len(work) - 1
    max_age = cfg.get("max_p5_age", 60)
    age = last_idx - best["p5_idx"]
    if age > max_age:
        oldest_date = work.index[best["p5_idx"]]
        date_str = (
            oldest_date.strftime("%Y-%m-%d")
            if hasattr(oldest_date, "strftime") else str(oldest_date)
        )
        return {
            "available": False,
            "reason": (
                f"최근 {max_age}봉 이내 명확한 5파 없음 "
                f"(가장 최신 후보 P5={date_str}, {age}봉 전)"
            ),
        }

    cand = best["candidate"]
    confidence = round(best["raw"] / raw_max * 100, 1)
    status = _wave_status(cand, work, best["rsi"])

    points_out = []
    for label, (idx, price, _typ) in zip("012345", cand["points"]):
        date = work.index[idx]
        date_str = (
            date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date)
        )
        points_out.append({
            "wave": label,
            "index": int(idx),
            "date": date_str,
            "price": round(float(price), 4),
        })

    warnings: list[str] = []
    if best["rsi"].get("divergence") == "bearish":
        warnings.append("5파에서 베어리시 다이버전스 감지 → 추세 반전 주의")
    if best["rsi"].get("divergence") == "bullish":
        warnings.append("5파에서 불리시 다이버전스 감지 → 추세 반전 주의")
    if best["fib"]["ratios"].get("w3", 0) >= 2.618:
        warnings.append("3파가 1파의 261.8% 이상 — 연장 3파 가능성")

    return {
        "available": True,
        "direction": cand["direction"],
        "current_wave": status,
        "confidence": confidence,
        "grade": _grade(confidence),
        "points": points_out,
        "scores": {
            "fib":    best["fib"]["score"],
            "volume": best["volume"]["score"],
            "rsi":    best["rsi"]["score"],
            "trend":  best["trend"]["score"],
            "raw":    best["raw"],
            "max":    raw_max,
        },
        "ratios": best["fib"]["ratios"],
        "warnings": warnings,
    }

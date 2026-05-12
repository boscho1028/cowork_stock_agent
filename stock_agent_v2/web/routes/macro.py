"""미국 매크로 시그널 대시보드.

market_warning.py 의 인프라(fetch_fear_greed, fetch_market_quotes, nvda_status_label)
재사용. LLM 요약 없이 구조화된 카드로 표시.

yfinance 다운로드가 2~5초 걸려 5분 메모리 캐시 (RAW_TTL_SEC).
새로고침 버튼은 ?force=1 쿼리로 캐시 우회.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from market_warning import (
    fetch_fear_greed, fetch_market_quotes, nvda_status_label, DEFAULT_TICKERS,
)
from web.deps import require_user_or_redirect

router = APIRouter()

_KST = timezone(timedelta(hours=9))
RAW_TTL_SEC = int(os.getenv("MACRO_CACHE_TTL_SEC", "300"))

# 메모리 캐시 — 단일 프로세스 단순 dict
_cache: dict[str, Any] = {"ts": 0, "data": None}


# ── 시그널 라벨 ───────────────────────────────────────────────────────

def _vix_signal(v: float) -> tuple[str, str]:
    """VIX → (라벨, level)"""
    if v < 15:   return ("안정 (Low Volatility)", "info")
    if v < 25:   return ("정상 범위", "info")
    if v < 35:   return ("경계 (Elevated)", "warn")
    return ("패닉 (High Stress)", "error")


def _tnx_signal(y: float) -> tuple[str, str]:
    """미국 10년물 (단위: %, ^TNX 는 % * 10 으로 옴 — 38.0 = 3.80%)"""
    pct = y / 10 if y > 20 else y  # ^TNX 는 38 같은 raw 값
    if pct < 4.0: return (f"완화 ({pct:.2f}%)", "info")
    if pct < 4.5: return (f"중립 ({pct:.2f}%)", "info")
    if pct < 5.0: return (f"압박 ({pct:.2f}%)", "warn")
    return (f"위험 ({pct:.2f}%)", "error")


def _fg_level(rating: str | None) -> str:
    """F&G rating → 카드 색상 level"""
    if not rating:
        return "info"
    r = rating.upper()
    if "EXTREME FEAR" in r:   return "error"   # 빨강 (contrarian buy 기회)
    if "FEAR" in r:           return "warn"
    if "NEUTRAL" in r:        return "info"
    if "EXTREME GREED" in r:  return "error"   # 빨강 (과열 경고)
    if "GREED" in r:          return "warn"
    return "info"


# ── 묶음(섹션) 분류 ──────────────────────────────────────────────────

# (티커, 표시명, 카테고리)
_GROUPS = [
    ("indices", "주가 지수",   ["SPY", "QQQ", "^SOX", "^VIX"]),
    ("rates_fx", "금리·환율",  ["^TNX", "USDKRW=X"]),
    ("commodities", "원자재·BTC", ["GC=F", "CL=F", "BTC-USD"]),
    ("focus", "핵심 종목",     ["NVDA"]),
]


def _build_payload() -> dict:
    fg = fetch_fear_greed()
    quotes = fetch_market_quotes(DEFAULT_TICKERS)

    fg_card = {
        "available":  fg.get("available", False),
        "score":      fg.get("score"),
        "rating":     fg.get("rating"),
        "level":      _fg_level(fg.get("rating")),
        "prev_close": fg.get("prev_close"),
        "prev_1week": fg.get("prev_1week"),
        "prev_1month": fg.get("prev_1month"),
        "reason":     fg.get("reason"),
    }

    sections = []
    for key, label, tks in _GROUPS:
        items = []
        for tk in tks:
            q = quotes.get(tk)
            if not q:
                continue
            card: dict = {
                "ticker":     tk,
                "name":       q["name"],
                "price":      q["price"],
                "change_pct": q["change_pct"],
            }
            # 시그널 라벨 — 종목별 다름
            if tk == "^VIX":
                lbl, lvl = _vix_signal(q["price"])
                card["signal"] = lbl
                card["level"]  = lvl
            elif tk == "^TNX":
                lbl, lvl = _tnx_signal(q["price"])
                card["signal"] = lbl
                card["level"]  = lvl
            elif tk == "NVDA":
                card["signal"] = nvda_status_label(q["price"])
                # 라벨 텍스트로 level 추정
                if "과열" in card["signal"]:    card["level"] = "warn"
                elif "붕괴" in card["signal"]:  card["level"] = "error"
                else:                         card["level"] = "info"
            items.append(card)
        # key 'cards' — Jinja 가 dict.items 메서드와 헷갈리지 않게.
        sections.append({"key": key, "label": label, "cards": items})

    return {
        "fg":       fg_card,
        "sections": sections,
        "asof":     datetime.now(_KST).strftime("%Y-%m-%d %H:%M:%S"),
    }


def _get_cached(force: bool = False) -> dict:
    now = time.time()
    if (not force) and _cache["data"] and (now - _cache["ts"] < RAW_TTL_SEC):
        return _cache["data"]
    data = _build_payload()
    _cache["ts"] = now
    _cache["data"] = data
    return data


@router.get("/macro")
def macro_page(request: Request, force: int = 0):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    data = _get_cached(force=bool(force))
    return request.app.state.templates.TemplateResponse(
        request, "macro/index.html",
        {"user": u, "data": data, "ttl_sec": RAW_TTL_SEC},
    )

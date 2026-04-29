"""
market_warning.py — 매일 07:30 미국 시장 종합 경고 브리핑

원본: c:\\Users\\bosch\\Downloads\\market_warning.py 를 stock_agent_v2 인프라에 통합.

변경점:
- Fear & Greed: feargreedmeter.com (JS 렌더링, BS4 안 잡힘) → CNN 공식 JSON API
- 오라클 CDS 항목 제거 → "신용/유동성 위험" 일반 뉴스 카테고리로 대체
- AI 부정 뉴스 카테고리 추가 (거품/밸류/capex/칩규제)
- LLM: OpenAI 대신 기존 프로젝트의 Claude/Gemini 재사용 (StockAnalyzer._call_ai)
- 텔레그램: 기존 TelegramNotifier 재사용
"""
from __future__ import annotations

import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Iterable
from urllib.parse import quote_plus

import pytz
import requests
import yfinance as yf

import config
from analyzer import StockAnalyzer
from telegram_bot import TelegramNotifier


# ═══════════════════════════════════════════════════════════════════════
# 1. CNN Fear & Greed Index (공식 JSON API)
# ═══════════════════════════════════════════════════════════════════════

CNN_FG_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.cnn.com",
    "Referer": "https://www.cnn.com/",
}


def fetch_fear_greed() -> dict:
    """CNN Fear & Greed JSON 호출. 실패 시 reason 포함 dict 반환."""
    try:
        r = requests.get(CNN_FG_URL, headers=_BROWSER_HEADERS, timeout=10)
        r.raise_for_status()
        j = r.json().get("fear_and_greed", {})
        score = j.get("score")
        rating = j.get("rating")
        if score is None or rating is None:
            return {"available": False, "reason": "응답 형식 변경"}
        return {
            "available": True,
            "score":         round(float(score), 1),
            "rating":        rating.upper(),
            "prev_close":    round(float(j.get("previous_close", 0)), 1),
            "prev_1week":    round(float(j.get("previous_1_week", 0)), 1),
            "prev_1month":   round(float(j.get("previous_1_month", 0)), 1),
            "prev_1year":    round(float(j.get("previous_1_year", 0)), 1),
        }
    except Exception as e:
        return {"available": False, "reason": f"{type(e).__name__}: {e}"}


# ═══════════════════════════════════════════════════════════════════════
# 2. 시장 시세 (yfinance)
# ═══════════════════════════════════════════════════════════════════════

# 매일 추적할 티커. 라벨은 텔레그램 표시용.
DEFAULT_TICKERS = {
    "SPY":      "S&P 500",
    "QQQ":      "Nasdaq 100",
    "^SOX":     "Semiconductor",
    "^TNX":     "US 10Y Yield",
    "USDKRW=X": "USD/KRW 환율",
    "BTC-USD":  "Bitcoin",
    "GC=F":     "Gold",
    "CL=F":     "WTI Oil",
    "^VIX":     "VIX(공포지수)",
    "NVDA":     "NVIDIA",
}


def fetch_market_quotes(tickers: dict = None) -> dict:
    """전 영업일 종가 → 당일 종가 변동률을 yfinance 로 조회.
    반환: {ticker: {"name", "price", "change_pct"}, ...}
    """
    tickers = tickers or DEFAULT_TICKERS
    out: dict = {}
    try:
        # 10거래일 정도 받아서 미국 휴장·신규 봉 미개시 등으로 일부 NaN 있어도
        # "전 종목 데이터 있는 마지막 2행" 을 안전하게 찾을 수 있게 함.
        # ffill 은 쓰지 않음 — BTC-USD 같은 24/7 종목과 미장 종목 시간대가
        # 어긋날 때 ffill 하면 같은 종가가 두 번 잡혀 변동률이 0 으로 나옴.
        df = yf.download(
            list(tickers.keys()), period="10d",
            progress=False, auto_adjust=True,
        )
        # auto_adjust=True 일 때 'Close' 가 수정종가
        prices = df["Close"] if "Close" in df.columns.levels[0] else df.iloc[:, 0:0]
        prices = prices.dropna(how="any")  # 모든 ticker 데이터 있는 행만
        if prices.empty or len(prices) < 2:
            return out
        cur = prices.iloc[-1]
        prv = prices.iloc[-2]
        for tk, name in tickers.items():
            try:
                p = float(cur[tk])
                pp = float(prv[tk])
                out[tk] = {
                    "name":       name,
                    "price":      p,
                    "change_pct": (p - pp) / pp * 100.0,
                }
            except Exception:
                continue
    except Exception as e:
        print(f"  [WARN] yfinance 조회 실패: {e}")
    return out


def nvda_status_label(price: float) -> str:
    """NVIDIA 가격대별 단순 시그널."""
    if price >= 183:
        return "🚨 과열 (183불 돌파)"
    if price <= 170:
        return "⚠️ 붕괴 (170불 하향)"
    return "✅ 정상"


# ═══════════════════════════════════════════════════════════════════════
# 3. 뉴스 검색 (Google News RSS — 무료, 키 불필요)
# ═══════════════════════════════════════════════════════════════════════

GNEWS_URL = (
    "https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
)

# 카테고리별 검색 쿼리 — Google News RSS 는 복잡한 OR/큐트 조합을 잘 처리 못 함.
# 단순한 핵심 구문 1~2개로 정확 매칭(quoted) 위주로 구성.
NEWS_QUERIES = {
    "credit_liquidity": '"private credit" stress OR "liquidity squeeze"',
    "ai_concerns":      '"AI bubble" OR "AI valuation"',
}


def _humanize_age(pub_str: str) -> str:
    """RSS pubDate 를 'N시간 전'·'N일 전' 한국어 상대시간으로."""
    if not pub_str:
        return "최근"
    try:
        pub_dt = parsedate_to_datetime(pub_str)
        if pub_dt.tzinfo is None:
            pub_dt = pub_dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - pub_dt
        hours = delta.total_seconds() / 3600
        if hours < 1:
            return f"{max(int(delta.total_seconds() / 60), 1)}분 전"
        if hours < 24:
            return f"{int(hours)}시간 전"
        return f"{int(hours / 24)}일 전"
    except Exception:
        return "최근"


def fetch_news_snippets(query: str, limit: int = 7, days: int = 7) -> list[dict]:
    """Google News RSS 로 뉴스 검색.

    description 필드는 단순 링크 HTML 이라 의미 정보가 없음. **title + source**
    가 LLM 에 풍부한 컨텍스트를 제공하므로 출처 매체명을 함께 추출한다.

    days: when:Nd 검색 연산자로 최근 N일 제한.

    반환: [{"date","title","source","link"}, ...]  실패 시 빈 리스트.
    """
    full_q = f"{query} when:{days}d"
    url = GNEWS_URL.format(q=quote_plus(full_q))
    try:
        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        r.raise_for_status()
        root = ET.fromstring(r.content)
        out: list[dict] = []
        for it in root.findall(".//item")[:limit]:
            title = (it.findtext("title") or "").strip()
            if not title:
                continue
            src_el = it.find("source")
            source = (src_el.text or "").strip() if src_el is not None else ""
            # 헤드라인 끝의 " - Source Name" 부분 정리 (source 와 중복)
            if source and title.endswith(f" - {source}"):
                title = title[: -len(f" - {source}")].strip()
            out.append({
                "date":    _humanize_age(it.findtext("pubDate") or ""),
                "title":   title,
                "source":  source,
                "link":    (it.findtext("link") or "").strip(),
            })
        return out
    except Exception as e:
        print(f"  [WARN] Google News RSS 실패 ({query[:30]}...): {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════
# 4. 메시지 빌더 (LLM 호출 + 형식 정리)
# ═══════════════════════════════════════════════════════════════════════

def _format_news_for_prompt(label: str, items: list[dict]) -> str:
    if not items:
        return f"{label}: NO_NEWS"
    lines = [f"{label}:"]
    for it in items:
        src = it.get("source", "").strip()
        src_tag = f" ({src})" if src else ""
        lines.append(f"  [{it['date']}]{src_tag} {it['title']}")
    return "\n".join(lines)


def build_warning_message() -> str:
    """전체 데이터 수집 + LLM 요약 → 텔레그램 본문 반환."""
    kst = pytz.timezone("Asia/Seoul")
    now_str = datetime.now(kst).strftime("%Y-%m-%d %H:%M")

    # 1) 데이터 수집
    fg = fetch_fear_greed()
    quotes = fetch_market_quotes()
    credit_news = fetch_news_snippets(NEWS_QUERIES["credit_liquidity"])
    ai_news = fetch_news_snippets(NEWS_QUERIES["ai_concerns"])

    # 2) 시세 라인 — NVIDIA 만 별도 상태 라벨
    market_lines = []
    for tk, info in quotes.items():
        if tk == "NVDA":
            market_lines.append(
                f"- {info['name']}: {info['price']:,.2f} "
                f"({nvda_status_label(info['price'])})"
            )
        else:
            market_lines.append(
                f"- {info['name']}: {info['price']:,.2f} "
                f"({info['change_pct']:+.2f}%)"
            )

    # 3) Fear & Greed 라인
    if fg.get("available"):
        fg_line = (
            f"- Fear & Greed Index: {fg['score']} ({fg['rating']}) "
            f"[전일 {fg['prev_close']} / 1주 {fg['prev_1week']} / "
            f"1개월 {fg['prev_1month']}]"
        )
    else:
        fg_line = f"- Fear & Greed Index: 추출 실패 ({fg.get('reason','?')})"

    # 4) LLM 프롬프트
    credit_block = _format_news_for_prompt("[신용/유동성 뉴스]", credit_news)
    ai_block     = _format_news_for_prompt("[AI 우려 뉴스]",     ai_news)

    prompt = f"""당신은 금융 데이터 관리자입니다. 제공된 데이터로 리스트 형식의 한국어 리포트를 작성하세요.

[데이터]
{chr(10).join(market_lines)}
{fg_line}

{credit_block}

{ai_block}

[지침]
1. 모든 항목은 '-' 로 시작하는 리스트 형식 유지.
2. 시세·F&G 항목은 데이터 그대로 한 줄씩 출력.
3. 뉴스 카테고리는 두 개(신용/유동성, AI 우려). 각 카테고리는 헤더 한 줄을 먼저 출력:
      - 신용/유동성:
      - AI 우려:
   그 아래에 들여쓰기(공백 2칸 + ·)로 의미 있는 헤드라인을 1~3개 골라 나열.
4. 헤드라인 선별 기준 (의미 있는 뉴스):
   - 구체적 수치 (%, 억/조 단위 금액, 회사명, 인물명, 기관명) 가 포함된 것
   - 권위 있는 출처 (Bloomberg, Reuters, FT, WSJ, CNBC, Yahoo Finance, MarketWatch 등)
   - 단순 의견·전망보다 사건·발표·지표
5. 각 헤드라인 출력 형식:
      · [N일 전, 출처] 핵심 한 문장 요약 (원문의 수치·이름은 그대로 보존)
   원문에 수치(예: 30bp, 10%, $5B)·기업명(Oracle, BlackRock 등)·인물명(Warren, Burry 등) 이
   있으면 반드시 한국어 요약 안에 그대로 포함.
6. 의미 있는 헤드라인이 없으면 "  · 의미 있는 신호 없음" 한 줄.
7. 뉴스가 NO_NEWS 면 카테고리 헤더 다음 줄에 "  · 관련 뉴스 없음".
8. 인사말·결론·과잉 분석 절대 금지. 리스트만 출력."""

    # 5) LLM 호출 (Claude → Gemini 폴백)
    analyzer = StockAnalyzer()
    text, provider = analyzer._call_ai(prompt)

    header = f"📊 *시장 경고 브리핑* ({now_str})"
    footer = f"\n\n🤖 AI: {analyzer._PROVIDER_LABEL[provider]}"
    return f"{header}\n\n{text.strip()}{footer}"


# ═══════════════════════════════════════════════════════════════════════
# 5. 진입점
# ═══════════════════════════════════════════════════════════════════════

def run_market_warning() -> None:
    """매일 07:30 (KST) 트리거되는 메인 함수."""
    print(f"\n[{datetime.now():%Y-%m-%d %H:%M}] [WARNING] 시장 경고 브리핑 시작")
    try:
        msg = build_warning_message()
    except Exception as e:
        print(f"[WARNING] 빌드 실패: {type(e).__name__}: {e}")
        # 실패해도 사용자에게 알림 (조용히 묻히지 않게)
        msg = (
            f"⚠️ 시장 경고 브리핑 생성 실패\n"
            f"{type(e).__name__}: {str(e)[:200]}"
        )
    notifier = TelegramNotifier(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID)
    notifier.send(msg)
    print("[WARNING] 전송 완료")


if __name__ == "__main__":
    run_market_warning()

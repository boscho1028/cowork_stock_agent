"""
naver_news.py — 네이버 뉴스 검색 API 래퍼 (모닝 브리핑 공시 보강용)

Docs: https://developers.naver.com/docs/serviceapi/search/news/news.md
- 무료 25,000 req/day
- 응답의 title/description 은 `<b>...</b>` 하이라이트와 HTML 엔티티 포함 → 정리 필요
"""
import html
import re
import requests

import config

_API_URL = "https://openapi.naver.com/v1/search/news.json"
_SESS    = requests.Session()
_TAG_RE  = re.compile(r"<[^>]+>")


def _clean(text: str) -> str:
    """네이버가 돌려주는 HTML 엔티티·태그를 평문으로."""
    return _TAG_RE.sub("", html.unescape(text or "")).strip()


def search(query: str, display: int = 3, sort: str = "date") -> list[dict]:
    """네이버 뉴스 검색.
    sort: 'date' (최신순) | 'sim' (정확도순)
    자격증명 미설정이면 빈 리스트 반환 (graceful degrade).
    """
    if not (config.NAVER_CLIENT_ID and config.NAVER_CLIENT_SECRET):
        return []
    try:
        resp = _SESS.get(
            _API_URL,
            headers={
                "X-Naver-Client-Id":     config.NAVER_CLIENT_ID,
                "X-Naver-Client-Secret": config.NAVER_CLIENT_SECRET,
            },
            params={"query": query, "display": max(1, min(display, 10)),
                    "sort": sort},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"  [NAVER] 뉴스 검색 실패 ({query!r}): {e}")
        return []

    items = []
    for it in resp.json().get("items", []):
        items.append({
            "title":       _clean(it.get("title", "")),
            "description": _clean(it.get("description", "")),
            "link":        it.get("originallink") or it.get("link", ""),
            "pub_date":    it.get("pubDate", ""),
        })
    return items


# ── 공시 제목 → 뉴스 검색어 도출 ─────────────────────────────────────
# 중요도 filter + 키워드 매핑. 공시 제목은 DART 양식명 (예: "연결재무제표
# 기준영업(잠정)실적(공정공시)") 이라 그대로 검색하면 뉴스가 안 나옴.
# 보통 뉴스에서 쓰는 짧은 키워드로 치환해서 검색한다.
_NEWS_KEYWORDS = [
    # (공시 제목에 포함되면 매칭될 키워드, 뉴스 검색용 치환어)
    ("잠정실적", "실적"),
    ("실적공시", "실적"),
    ("영업실적", "실적"),
    ("실적",     "실적"),
    ("자기주식", "자사주"),
    ("자사주",   "자사주"),
    ("유상증자", "유상증자"),
    ("무상증자", "무상증자"),
    ("전환사채", "전환사채"),
    ("신주인수권", "신주인수권"),
    ("교환사채", "교환사채"),
    ("주식분할", "주식분할"),
    ("주식병합", "주식병합"),
    ("감자",     "감자"),
    ("합병",     "합병"),
    ("분할",     "분할"),
    ("영업양수", "영업양수"),
    ("영업양도", "영업양도"),
    ("최대주주 변경", "최대주주"),
    ("대표이사 변경", "대표이사"),
    ("공급계약", "공급계약"),
    ("수주",     "수주"),
    ("배당",     "배당"),
    ("대량보유", "대량보유"),
    ("상장폐지", "상장폐지"),
    ("관리종목", "관리종목"),
    ("소송",     "소송"),
]

# 🔴/🟠 외에 이 키워드가 제목에 있으면 🟡/🔵 여도 뉴스 보강 대상
_FORCE_NEWS_KEYWORDS = {
    "실적", "잠정실적", "자사주", "자기주식", "배당",
    "합병", "분할", "유상증자", "무상증자", "감자",
    "전환사채", "공급계약", "수주",
}


def should_fetch_news(line: str) -> bool:
    """한 공시 라인이 뉴스 보강 대상인지 판단.
    line 예: '🟡 2026-04-23  연결재무제표기준영업(잠정)실적(공정공시)'
    - 🔴/🟠 중요도 → 항상 포함
    - 키워드 매칭 → 포함
    """
    if not line:
        return False
    if "🔴" in line or "🟠" in line:
        return True
    return any(kw in line for kw in _FORCE_NEWS_KEYWORDS)


def query_for(company: str, disclosure_title: str) -> str:
    """'회사명 + 핵심 키워드' 검색어 생성. 매칭 키워드 없으면 회사명만."""
    for pat, term in _NEWS_KEYWORDS:
        if pat in disclosure_title:
            return f"{company} {term}"
    return company

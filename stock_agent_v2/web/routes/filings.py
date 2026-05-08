"""DART + SEC 공시 통합 피드 — 시장별 그룹 + 한 줄 요약 카드."""
from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

import config
from database import get_conn
from web.deps import require_user_or_redirect

router = APIRouter()


# SEC form 코드 → 한국어 라벨
SEC_FORM_LABELS = {
    "3":      "임원·주요주주 신규",
    "4":      "임원·주요주주 매매",
    "5":      "임원·주요주주 연간",
    "8-K":    "중요 이벤트",
    "8-K/A":  "중요 이벤트 (정정)",
    "10-Q":   "분기 보고서",
    "10-Q/A": "분기 보고서 (정정)",
    "10-K":   "연간 보고서",
    "10-K/A": "연간 보고서 (정정)",
    "13F":    "기관 보유 공시",
    "13F-HR": "기관 보유 공시",
    "13G":    "5%+ 지분 (수동)",
    "SC 13G": "5%+ 지분 (수동)",
    "SC 13G/A": "5%+ 지분 (수동·정정)",
    "13D":    "5%+ 지분 (능동)",
    "SC 13D": "5%+ 지분 (능동)",
    "S-1":    "증권신고서",
    "S-3":    "증권신고서 (단축)",
    "S-8":    "임직원 증권 등록",
    "144":    "Rule 144 매도 계획",
    "DEF 14A": "주주총회 위임장",
    "PRE 14A": "주주총회 위임장 (예비)",
    "424B2":  "투자설명서",
    "424B3":  "투자설명서",
    "424B5":  "투자설명서",
    "6-K":    "외국기업 보고",
    "20-F":   "외국기업 연간",
    "F-1":    "외국기업 증권신고",
    "FWP":    "자유 작성 투자설명서",
    "EFFECT": "효력 발생",
}

# 8-K Item 코드 → 한국어 라벨
SEC_8K_ITEMS = {
    "1.01": "중요 계약 체결",
    "1.02": "중요 계약 종료",
    "1.03": "회생 절차",
    "2.01": "자산 인수/매각",
    "2.02": "실적 발표",
    "2.03": "직접금융 의무 발생",
    "2.04": "직접금융 의무 가속",
    "2.05": "구조조정 비용",
    "2.06": "자산 손상",
    "3.01": "상장 폐지",
    "3.02": "비등록 증권 매각",
    "3.03": "주주 권리 변경",
    "4.01": "감사인 변경",
    "4.02": "재무제표 신뢰성 상실",
    "5.01": "지배구조 변경",
    "5.02": "경영진 변동",
    "5.03": "정관 변경",
    "5.07": "주주총회 결과",
    "5.08": "주주제안 마감 변경",
    "7.01": "Reg FD 공시",
    "8.01": "기타 중요 사항",
    "9.01": "재무제표·첨부 자료",
}


def _sec_summary(form_type: str, items: str | None) -> tuple[str, str]:
    """SEC 공시 → (메인 라벨, 부가 정보).

    8-K 의 경우 item 코드를 풀어 부가 정보로 붙임.
    """
    base = SEC_FORM_LABELS.get(form_type, form_type)
    extra = ""
    if items and form_type.startswith("8-K"):
        codes = [c.strip() for c in items.split(",") if c.strip()]
        labels = [SEC_8K_ITEMS.get(c, f"Item {c}") for c in codes]
        if labels:
            extra = ", ".join(labels)
    return base, extra


def _load_filings_grid(limit_days: int = 14, limit: int = 200) -> dict:
    """DART(KR) + SEC(US) 두 섹션, 각 섹션은 최신순 정렬된 카드 리스트."""
    universe = config.get_universe_detail()
    portfolio = config.get_portfolio_detail()
    today_iso = date.today().isoformat()  # YYYY-MM-DD

    kr: list[dict] = []
    us: list[dict] = []

    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT ticker, corp_name, report_nm, rcept_dt, flr_nm, rm
            FROM dart_disclosures
            WHERE rcept_dt >= strftime('%Y%m%d', date('now', ?))
            ORDER BY rcept_dt DESC
            LIMIT ?
            """,
            (f"-{int(limit_days)} days", int(limit)),
        )
        for ticker, corp, rnm, rdt, flr, rm in cur.fetchall():
            d = rdt or ""
            if len(d) == 8 and d.isdigit():
                d = f"{d[:4]}-{d[4:6]}-{d[6:]}"
            summary = (rnm or "").strip()
            extra = ""
            if flr and corp and flr.strip() != corp.strip():
                extra = f"제출인: {flr.strip()}"
            kr.append({
                "source":       "DART",
                "ticker":       ticker,
                "name":         (corp or "").strip(),
                "date":         d,
                "form":         "",        # DART 는 form code 노출 안 함
                "summary":      summary,
                "extra":        extra,
                "icon":         "",
                "url":          None,
                "is_portfolio": ticker in portfolio,
                "is_today":     d == today_iso,
            })

        cur = conn.execute(
            """
            SELECT ticker, form_type, filed_date, description, items, importance, url
            FROM sec_filings
            WHERE filed_date >= date('now', ?)
            ORDER BY filed_date DESC
            LIMIT ?
            """,
            (f"-{int(limit_days)} days", int(limit)),
        )
        for ticker, form, fdate, desc, items, imp, url in cur.fetchall():
            base, item_text = _sec_summary(form or "", items)
            # universe 에서 종목명 가져오기 (있으면)
            name = (universe.get(ticker) or {}).get("name", "")
            us.append({
                "source":       "SEC",
                "ticker":       ticker,
                "name":         name,
                "date":         fdate or "",
                "form":         form or "",
                "summary":      base,
                "extra":        item_text,
                "icon":         imp or "",
                "url":          url,
                "is_portfolio": ticker in portfolio,
                "is_today":     (fdate or "") == today_iso,
            })

    # 이미 SQL ORDER BY 로 desc 정렬됐지만 안전하게 재정렬
    kr.sort(key=lambda x: x["date"], reverse=True)
    us.sort(key=lambda x: x["date"], reverse=True)
    return {"kr": kr, "us": us}


@router.get("/filings")
def filings_list(request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    grid = _load_filings_grid(limit_days=14, limit=200)
    return request.app.state.templates.TemplateResponse(
        request, "filings/list.html",
        {"grid": grid, "user": u},
    )

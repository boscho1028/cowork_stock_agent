"""DART + SEC 공시 통합 피드."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from database import get_conn
from web.deps import require_user_or_redirect

router = APIRouter()


def _load_recent_filings(limit_days: int = 14, limit: int = 200) -> list[dict]:
    """DART + SEC 합쳐 최신순 정렬."""
    out: list[dict] = []
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
        for r in cur.fetchall():
            ticker, corp, rnm, rdt, flr, rm = r
            # YYYYMMDD → YYYY-MM-DD
            d = rdt
            if d and len(d) == 8 and d.isdigit():
                d = f"{d[:4]}-{d[4:6]}-{d[6:]}"
            out.append({
                "source": "DART",
                "ticker": ticker,
                "name":   corp or "",
                "title":  rnm or "",
                "date":   d or "",
                "extra":  flr or "",
                "url":    None,
            })
        cur = conn.execute(
            """
            SELECT ticker, form_type, filed_date, description, importance, url
            FROM sec_filings
            WHERE filed_date >= date('now', ?)
            ORDER BY filed_date DESC
            LIMIT ?
            """,
            (f"-{int(limit_days)} days", int(limit)),
        )
        for r in cur.fetchall():
            ticker, form, fdate, desc, imp, url = r
            out.append({
                "source": "SEC",
                "ticker": ticker,
                "name":   "",
                "title":  f"[{form}] {(desc or '')[:80]}",
                "date":   fdate or "",
                "extra":  imp or "",
                "url":    url,
            })
    out.sort(key=lambda x: x["date"], reverse=True)
    return out


@router.get("/filings")
def filings_list(request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u
    items = _load_recent_filings(limit_days=14, limit=200)
    return request.app.state.templates.TemplateResponse(
        request, "filings/list.html",
        {"items": items, "user": u},
    )

"""/intel/* → my_palantir (localhost:8001) — auth-gated reverse proxy.

Friend's flow: log into stock_agent_v2 once → click "인텔리전스" nav →
this route validates the session cookie, then forwards every request
under /intel/* to my_palantir. my_palantir's PrefixRewriteMiddleware
reads X-Forwarded-Prefix to re-add "/intel" into outgoing HTML links.

No session token or user info is forwarded — my_palantir trusts that
this proxy already authorized the request.
"""
from __future__ import annotations

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, Response

from web.deps import require_user_or_redirect

router = APIRouter()

_UPSTREAM = "http://localhost:8001"

# Hop-by-hop headers per RFC 7230 + content-length/encoding which Starlette
# recomputes on the outgoing response.
_HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
    "content-length", "content-encoding",
})


@router.api_route(
    "/intel/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"],
    include_in_schema=False,
)
async def intel_proxy(path: str, request: Request):
    u = require_user_or_redirect(request)
    if isinstance(u, RedirectResponse):
        return u

    qs = request.url.query
    url = f"{_UPSTREAM}/{path}" + (f"?{qs}" if qs else "")

    fwd_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in _HOP_BY_HOP and k.lower() != "host"
    }
    fwd_headers["X-Forwarded-Prefix"] = "/intel"
    fwd_headers["X-Forwarded-Host"] = request.headers.get("host", "")
    fwd_headers["X-Forwarded-Proto"] = request.url.scheme

    body = await request.body()

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=False) as client:
        upstream = await client.request(
            method=request.method,
            url=url,
            headers=fwd_headers,
            content=body,
        )

    resp_headers = {
        k: v for k, v in upstream.headers.items()
        if k.lower() not in _HOP_BY_HOP
    }
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=resp_headers,
        media_type=upstream.headers.get("content-type"),
    )

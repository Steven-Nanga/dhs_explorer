"""Build public URLs behind reverse proxies (Render, nginx, etc.)."""

import os

from flask import Request


def public_base_url(request: Request) -> str:
    """Base URL for links in emails and redirects (https, correct host).

    Set PUBLIC_BASE_URL=https://your-app.onrender.com in production.
    Otherwise uses the request (requires ProxyFix for correct scheme/host).
    """
    env = os.environ.get("PUBLIC_BASE_URL", "").strip().rstrip("/")
    if env:
        return env
    return request.url_root.rstrip("/")

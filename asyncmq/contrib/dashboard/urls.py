from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

from lilya.requests import Request


def local_url_path(url: Any) -> str:
    """Return the path/query/fragment portion of a Lilya-generated URL."""
    value = str(url)
    parsed = urlsplit(value)
    if not parsed.scheme and not parsed.netloc:
        return value

    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    if parsed.fragment:
        path = f"{path}#{parsed.fragment}"
    return path


def dashboard_path_for(request: Request, route_name: str, **path_params: Any) -> str:
    """Build a dashboard-local path for links and redirects."""
    return local_url_path(request.url_path_for(route_name, **path_params))

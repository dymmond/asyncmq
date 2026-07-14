from __future__ import annotations

from typing import Final

ContentSecurityPolicy = dict[str, str | list[str]] | str

DASHBOARD_CONTENT_SECURITY_POLICY: Final[dict[str, str | list[str]]] = {
    "default-src": "'self'",
    "script-src": "'self'",
    "script-src-elem": "'self'",
    "style-src": "'self'",
    "style-src-elem": "'self'",
    "img-src": ["'self'", "data:"],
    "font-src": "'self'",
    "connect-src": "'self'",
    "object-src": "'none'",
    "base-uri": "'self'",
    "form-action": "'self'",
    "frame-ancestors": "'none'",
    "worker-src": "'none'",
    "manifest-src": "'self'",
}

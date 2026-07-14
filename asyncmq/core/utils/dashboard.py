import secrets
from dataclasses import dataclass
from typing import Literal

try:
    from lilya.middleware import DefineMiddleware
    from lilya.middleware.sessions import SessionMiddleware
    from lilya.requests import Request
except ImportError:
    raise ModuleNotFoundError(
        "The dashboard functionality requires the 'lilya' package. Please install it with 'pip install lilya'."
    ) from None

from asyncmq import monkay


@dataclass
class DashboardConfig:
    title: str = "Dashboard"
    """The title displayed in the browser tab/window header."""

    header_title: str = "AsyncMQ"
    """The main title displayed within the dashboard application header."""

    description: str = "A simple dashboard for monitoring AsyncMQ jobs."
    """A brief description of the dashboard's purpose."""

    favicon: str = "/static/favicon.ico"
    """URL path or external URL for the favicon."""

    dashboard_url_prefix: str = "/admin"
    """The base URL prefix where the dashboard is mounted in the host application."""

    sidebar_bg_colour: str = "#CBDC38"
    """The CSS color value for the sidebar background."""

    secret_key: str | None = None
    """
    The cryptographic key used to sign the session cookie. Must be kept secret.
    If `None`, a secure key is generated automatically on first access.
    """

    session_cookie: str = "asyncz_admin"
    """The name of the session cookie to be set on the client."""

    max_age: int | None = 14 * 24 * 60 * 60  # 14 days, in seconds
    """
    The maximum age (lifetime) of the session cookie in seconds.
    `None` means the cookie expires when the browser closes.
    """

    path: str = "/"
    """The path scope for which the cookie is valid."""

    same_site: Literal["lax", "strict", "none"] = "lax"
    """Controls when cookies are sent in cross-site requests, balancing security and usability."""

    https_only: bool = False
    """If `True`, the cookie will only be transmitted over HTTPS connections (requires secure context)."""

    domain: str | None = None
    """The domain scope for which the cookie is valid."""

    cors_allow_origins: tuple[str, ...] = ()
    """
    Origins allowed to access the dashboard through browser CORS.
    Empty by default so the dashboard remains same-origin unless explicitly
    configured.
    """

    cors_allow_methods: tuple[str, ...] = ("GET", "POST", "OPTIONS")
    """HTTP methods allowed when dashboard CORS is explicitly configured."""

    cors_allow_headers: tuple[str, ...] = (
        "Accept",
        "Authorization",
        "Content-Type",
        "HX-Request",
        "HX-Target",
        "HX-Trigger",
    )
    """HTTP headers allowed when dashboard CORS is explicitly configured."""

    cors_allow_credentials: bool = False
    """
    Whether browsers may include credentials for configured CORS origins.
    Must not be combined with a wildcard origin.
    """

    trusted_proxies: tuple[str, ...] = ()
    """
    Client peer addresses whose forwarded host/proto headers may be trusted
    for dashboard same-origin checks. Empty by default so direct clients cannot
    forge reverse-proxy origins.
    """

    @property
    def session_middleware(self) -> DefineMiddleware:
        """
        Dynamically creates and returns a `DefineMiddleware` instance configured with the
        necessary `SessionMiddleware` parameters.

        A secure key is generated using `secrets.token_urlsafe` if `secret_key` is `None`.

        Returns:
            A `DefineMiddleware` instance ready to be included in an ASGI application.
        """
        return DefineMiddleware(
            SessionMiddleware,
            secret_key=self.secret_key or secrets.token_urlsafe(32),
            session_cookie=self.session_cookie,
            max_age=self.max_age,
            path=self.path,
            same_site=self.same_site,
            https_only=self.https_only,
            domain=self.domain,
        )


def get_effective_prefix(request: Request) -> str:
    """Compute the effective base URL prefix for the dashboard.

    Lilya stores the actual mounted path in ``scope["root_path"]`` as requests
    pass through includes and child applications. That runtime mount is the
    canonical prefix whenever present; the configured dashboard prefix is only a
    fallback for directly rendered dashboard apps with no mount scope.
    """
    configured_prefix = monkay.settings.dashboard_config.dashboard_url_prefix or ""
    mount_prefix = (getattr(request, "scope", None) or {}).get("root_path", "") or ""

    base = mount_prefix or configured_prefix or "/"

    return base if base == "/" else base.rstrip("/")

import secrets
from dataclasses import dataclass
from typing import Literal

try:
    from lilya.middleware import DefineMiddleware
    from lilya.middleware.sessions import SessionMiddleware
    from lilya.requests import Request
except ImportError:
    raise ModuleNotFoundError(
        "The dashboard functionality requires the 'lilya' package. " "Please install it with 'pip install lilya'."
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

    favicon: str = "https://raw.githubusercontent.com/dymmond/asyncmq/refs/heads/main/docs/statics/favicon.ico"
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

    Combines the ASGI mount root_path (if any) with the configured dashboard
    URL prefix. Ensures a clean result without double slashes and with no
    trailing slash, except when the result is exactly "/".
    """
    configured_prefix = monkay.settings.dashboard_config.dashboard_url_prefix or ""
    mount_prefix = (getattr(request, "scope", None) or {}).get("root_path", "") or ""

    # If the mount prefix already includes the configured prefix, don't double it.
    if configured_prefix and (
        mount_prefix.endswith(configured_prefix) or f"{mount_prefix}/".endswith(f"{configured_prefix}/")
    ):
        base = mount_prefix
    else:
        base = f"{mount_prefix}{configured_prefix or '/'}"

    # Avoid trailing slash unless it's the root
    return base if base == "/" else base.rstrip("/")

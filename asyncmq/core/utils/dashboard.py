import secrets
from dataclasses import dataclass

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
    header_title: str = "AsyncMQ"
    description: str = "A simple dashboard for monitoring AsyncMQ jobs."
    favicon: str = "https://raw.githubusercontent.com/dymmond/asyncmq/refs/heads/main/docs/statics/favicon.ico"
    dashboard_url_prefix: str = "/admin"
    sidebar_bg_colour: str = "#CBDC38"
    session_middleware: DefineMiddleware = DefineMiddleware(SessionMiddleware, secret_key=secrets.token_hex(32))


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

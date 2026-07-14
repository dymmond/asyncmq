from __future__ import annotations

from typing import Any

from lilya.requests import Request
from lilya.responses import HTMLResponse, Response
from lilya.testclient import TestClient
from lilya.types import ASGIApp, Receive, Scope, Send

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.contrib.dashboard.admin.protocols import AuthBackend


class PrefixBackend:
    """Minimal backend used to render dashboard pages under mounted prefixes."""

    async def list_queues(self) -> list[str]:
        """Return no queues for deterministic prefix-rendering tests."""
        return []

    async def list_workers(self) -> list[dict[str, Any]]:
        """Return no workers for deterministic prefix-rendering tests."""
        return []

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        """Return empty queue counts without falling back to unbounded scans."""
        return {"waiting": 0, "active": 0, "delayed": 0, "failed": 0, "completed": 0}


class HeaderBackend(AuthBackend):
    """Authenticate dashboard requests from a test header."""

    async def authenticate(self, request: Request) -> dict[str, object] | None:
        """Return an admin user when the test authentication header is present."""
        user = request.headers.get("X-Authenticated-User")
        return {"id": user, "name": user, "is_admin": True} if user else None

    async def login(self, request: Request) -> Response:
        """Render a simple auth-required response for test login routes."""
        return HTMLResponse("Header auth required")

    async def logout(self, request: Request) -> Response:
        """Render a simple logout response for the stateless test backend."""
        return HTMLResponse("Logged out")


class InjectClientPeerMiddleware:
    """Inject a deterministic ASGI client peer for trusted-proxy tests."""

    def __init__(self, app: ASGIApp, *, client: tuple[str, int]) -> None:
        """Store the wrapped app and client peer tuple."""
        self.app = app
        self.client = client

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Set the ASGI client peer before delegating to the dashboard app."""
        scope = dict(scope)
        scope["client"] = self.client
        await self.app(scope, receive, send)


def _dashboard_client(url_prefix: str = "/asyncmq", *, root_path: str = "") -> TestClient:
    """Build a dashboard test client mounted at the supplied URL prefix."""
    settings.backend = PrefixBackend()
    app = AsyncMQAdmin(
        enable_login=False,
        include_session=True,
        include_cors=False,
        url_prefix=url_prefix,
    ).get_asgi_app(with_url_prefix=True)
    return TestClient(app, root_path=root_path)


def test_dashboard_links_use_actual_mount_prefix_over_configured_prefix():
    """Use Lilya's runtime mount as the dashboard URL owner when prefixes differ."""
    client = _dashboard_client(url_prefix="/ops")
    response = client.get("/ops/queues")

    assert response.status_code == 200
    assert 'href="/ops/queues"' in response.text
    assert 'href="/asyncmq/queues"' not in response.text
    assert 'href="/ops/asyncmq/queues"' not in response.text
    assert 'href="/ops/static/favicon.ico"' in response.text
    assert "http://testserver/ops/static/css/asyncmq.css" in response.text


def test_dashboard_links_and_assets_include_nested_proxy_root_path():
    """Render static assets, nav links, and SSE URLs under a nested proxy root."""
    client = _dashboard_client(root_path="/operations")
    response = client.get("/operations/asyncmq/queues")

    assert response.status_code == 200
    assert 'href="/operations/asyncmq/queues"' in response.text
    assert 'action="/operations/asyncmq/logout"' in response.text
    assert 'data-asyncmq-queues data-events-url="/operations/asyncmq/events"' in response.text
    assert "http://testserver/operations/asyncmq/static/css/asyncmq.css" in response.text
    assert "http://testserver/operations/asyncmq/static/vendor/alpinejs/" in response.text


def test_dashboard_404_home_link_uses_nested_proxy_root_path():
    """Keep 404 recovery links inside the reverse-proxy mount path."""
    client = _dashboard_client(root_path="/operations")
    response = client.get("/operations/asyncmq/not-here")

    assert response.status_code == 404
    assert 'href="/operations/asyncmq"' in response.text
    assert 'href="/asyncmq"' not in response.text


def test_login_redirect_preserves_nested_proxy_root_path():
    """Redirect unauthenticated operators to the prefixed login route."""
    app = AsyncMQAdmin(
        enable_login=True,
        backend=HeaderBackend(),
        include_session=False,
        include_cors=False,
        url_prefix="/asyncmq",
    ).get_asgi_app(with_url_prefix=True)
    client = TestClient(app, root_path="/operations")

    response = client.get("/operations/asyncmq/", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/operations/asyncmq/login?next=/operations/asyncmq/"


def test_forwarded_origin_headers_are_ignored_without_trusted_proxy():
    """Reject forged forwarded origins from arbitrary direct clients."""
    app = AsyncMQAdmin(
        enable_login=True,
        backend=HeaderBackend(),
        include_session=False,
        include_cors=False,
        url_prefix="/asyncmq",
    ).get_asgi_app(with_url_prefix=True)
    client = TestClient(app)

    response = client.post(
        "/asyncmq/queues/critical",
        data={"action": "noop"},
        headers={
            "Host": "internal.local",
            "Origin": "https://ops.example",
            "X-Authenticated-User": "alice",
            "X-Forwarded-Host": "ops.example",
            "X-Forwarded-Proto": "https",
        },
        follow_redirects=False,
    )

    assert response.status_code == 403
    assert response.text == "Cross-origin dashboard request rejected"


def test_forwarded_origin_headers_work_for_configured_trusted_proxy():
    """Allow forwarded origins only when the request peer is explicitly trusted."""
    app = AsyncMQAdmin(
        enable_login=True,
        backend=HeaderBackend(),
        include_session=False,
        include_cors=False,
        url_prefix="/asyncmq",
        trusted_proxies=("10.0.0.10",),
    ).get_asgi_app(with_url_prefix=True)
    client = TestClient(InjectClientPeerMiddleware(app, client=("10.0.0.10", 443)))

    response = client.post(
        "/asyncmq/queues/critical",
        data={"action": "noop"},
        headers={
            "Host": "internal.local",
            "Origin": "https://ops.example:8443",
            "X-Authenticated-User": "alice",
            "X-Forwarded-Host": "ops.example",
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Port": "8443",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/asyncmq/queues/critical"

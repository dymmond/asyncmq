import typing as t

import pytest
from lilya.apps import Lilya
from lilya.requests import Request
from lilya.responses import HTMLResponse, RedirectResponse, Response
from lilya.testclient.base import TestClient

from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.contrib.dashboard.admin.protocols import AuthBackend


class HeaderTokenBackend(AuthBackend):
    """
    Auth via 'X-Authenticated-User' header. Useful to simulate SSO/proxy handoff.
    """

    def __init__(self, header: str = "X-Authenticated-User"):
        self.header = header

    async def authenticate(self, request: Request) -> dict | None:
        user = request.headers.get(self.header)
        return {"id": user, "name": user} if user else None

    async def login(self, request: Request) -> Response:
        # For header-only flows, just show a tiny page explaining how to auth.
        return HTMLResponse("This deployment expects an authenticated header.")

    async def logout(self, request: Request) -> Response:
        # Stateless – nothing to clear; send them back to "login".
        return RedirectResponse("/login", status_code=303)

    def routes(self) -> list[t.Any]:
        return []


@pytest.fixture
def lilya_app_header_backend(monkeypatch) -> Lilya:
    app = Lilya()

    # Mount a tiny health endpoint to make sure allowlist examples don't interfere with tests.
    # (Not strictly needed, but handy to debug middleware behavior locally.)
    async def health(_: Request) -> Response:
        return HTMLResponse("OK")

    app.add_route("/healthz", health)

    admin = AsyncMQAdmin(
        enable_login=True,
        backend=HeaderTokenBackend(),
        include_session=True,
        include_cors=False,
    )
    admin.include_in(app)
    return app


@pytest.fixture
def client(lilya_app_header_backend: Lilya) -> TestClient:
    return TestClient(lilya_app_header_backend)


def test_header_backend_allows_access_with_header(client: TestClient):
    # Without header -> blocked
    blocked = client.get("/asyncmq/", follow_redirects=False)
    assert blocked.status_code == 303

    # With header -> allowed
    ok = client.get("/asyncmq/", headers={"X-Authenticated-User": "alice"})
    assert ok.status_code == 200
    assert "alice" in ok.text or "Dashboard" in ok.text


def test_header_backend_logout_is_stateless(client: TestClient):
    # Even after "logout", access with header should still be allowed (stateless).
    client.get("/asyncmq/logout", follow_redirects=False)
    ok = client.get("/asyncmq/", headers={"X-Authenticated-User": "bob"})

    assert ok.status_code == 200

import typing as t

import pytest
from lilya.apps import Lilya
from lilya.requests import Request
from lilya.responses import HTMLResponse, RedirectResponse, Response
from lilya.testclient.base import TestClient

from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.contrib.dashboard.admin.protocols import AuthBackend


class SimpleUsernamePasswordBackend(AuthBackend):
    """
    Minimal session-backed username/password backend for tests.
    """

    SESSION_KEY = "asyncmq_admin_user"

    def __init__(self, verify: t.Callable[[str, str], dict | None] | None = None):
        # default "verify" for convenience
        self.verify = verify or (
            lambda u, p: ({"id": "admin", "name": "Admin"} if (u == "admin" and p == "secret") else None)
        )

    async def authenticate(self, request: Request) -> dict | None:
        return t.cast(dict | None, request.session.get(self.SESSION_KEY))

    async def login(self, request: Request) -> Response:
        if request.method == "GET":
            # Simple HTML form is enough for tests
            return HTMLResponse(
                "<form method='post'>"
                "<input name='username'/>"
                "<input name='password' type='password'/>"
                "<button type='submit'>Login</button>"
                "</form>"
            )
        form = await request.form()
        username = form.get("username")
        password = form.get("password")
        user = self.verify(username, password)
        if user:
            request.session[self.SESSION_KEY] = user
            return RedirectResponse(form.get("next") or "/", status_code=303)
        return RedirectResponse("/login", status_code=303)

    async def logout(self, request: Request) -> Response:
        request.session.pop(self.SESSION_KEY, None)
        return RedirectResponse("/login", status_code=303)

    def routes(self) -> list[t.Any]:
        return []


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
def lilya_app_username_password() -> Lilya:
    app = Lilya()

    # Use a dummy backend to avoid hitting Redis in tests
    admin = AsyncMQAdmin(
        enable_login=True,
        backend=SimpleUsernamePasswordBackend(),
        include_session=True,  # uses settings.dashboard_config.session_middleware
        include_cors=False,
    )
    admin.include_in(app)
    return app


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
def client_userpass(lilya_app_username_password: Lilya) -> TestClient:
    return TestClient(lilya_app_username_password)


@pytest.fixture
def client_header(lilya_app_header_backend: Lilya) -> TestClient:
    return TestClient(lilya_app_header_backend)


def test_auth_gate_blocks_unauthenticated(client_userpass: TestClient):
    # Not logged in -> should get a 303 to /login under the mount prefix (/admin by default)
    response = client_userpass.get("/asyncmq/", follow_redirects=False)

    assert response.status_code == 303
    assert "/asyncmq/login" in response.headers.get("location", "")


def test_login_failure_redirects_back_to_login(client_userpass: TestClient):
    response = client_userpass.post(
        "/asyncmq/login",
        data={"username": "admin", "password": "WRONG"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers.get("location", "").endswith("/login")


def test_login_success_sets_session_and_allows_access(client_userpass: TestClient):
    # Login
    response = client_userpass.post(
        "/asyncmq/login",
        data={"username": "admin", "password": "secret"},
        follow_redirects=False,
    )

    assert response.status_code == 303

    # Now authenticated -> dashboard root should be accessible
    response = client_userpass.get("/asyncmq/")
    assert response.status_code == 200

    # The exact content depends on templates; a loose assertion is safer.
    assert "Queues" in response.text or "Dashboard" in response.text


def test_logout_clears_session(client_userpass: TestClient):
    # Login first
    client_userpass.post(
        "/asyncmq/login",
        data={"username": "admin", "password": "secret"},
        follow_redirects=False,
    )

    # Logout
    response = client_userpass.get("/asyncmq/logout", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers.get("location", "").endswith("/login")

    # Access again -> blocked
    response = client_userpass.get("/asyncmq/", follow_redirects=False)
    assert response.status_code == 303
    assert "/asyncmq/login" in response.headers.get("location", "")


def test_header_backend_allows_access_with_header(client_header: TestClient):
    # Without header -> blocked
    blocked = client_header.get("/asyncmq/", follow_redirects=False)
    assert blocked.status_code == 303

    # With header -> allowed
    ok = client_header.get("/asyncmq/", headers={"X-Authenticated-User": "alice"})
    assert ok.status_code == 200
    assert "alice" in ok.text or "Dashboard" in ok.text


def test_header_backend_logout_is_stateless(client_header: TestClient):
    # Even after "logout", access with header should still be allowed (stateless).
    client_header.get("/asyncmq/logout", follow_redirects=False)
    ok = client_header.get("/asyncmq/", headers={"X-Authenticated-User": "bob"})

    assert ok.status_code == 200

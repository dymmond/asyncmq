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
def client(lilya_app_username_password: Lilya) -> TestClient:
    return TestClient(lilya_app_username_password)


def test_auth_gate_blocks_unauthenticated(client: TestClient):
    # Not logged in -> should get a 303 to /login under the mount prefix (/admin by default)
    response = client.get("/asyncmq/", follow_redirects=False)

    assert response.status_code == 303
    assert "/asyncmq/login" in response.headers.get("location", "")


def test_login_failure_redirects_back_to_login(client: TestClient):
    response = client.post(
        "/asyncmq/login",
        data={"username": "admin", "password": "WRONG"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers.get("location", "").endswith("/login")


def test_login_success_sets_session_and_allows_access(client: TestClient):
    # Login
    response = client.post(
        "/asyncmq/login",
        data={"username": "admin", "password": "secret"},
        follow_redirects=False,
    )

    assert response.status_code == 303

    # Now authenticated -> dashboard root should be accessible
    response = client.get("/asyncmq/")
    assert response.status_code == 200

    # The exact content depends on templates; a loose assertion is safer.
    assert "Queues" in response.text or "Dashboard" in response.text


def test_logout_clears_session(client: TestClient):
    # Login first
    client.post(
        "/asyncmq/login",
        data={"username": "admin", "password": "secret"},
        follow_redirects=False,
    )

    # Logout
    response = client.get("/asyncmq/logout", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers.get("location", "").endswith("/login")

    # Access again -> blocked
    response = client.get("/asyncmq/", follow_redirects=False)
    assert response.status_code == 303
    assert "/asyncmq/login" in response.headers.get("location", "")

import pytest
from lilya.requests import Request
from lilya.responses import HTMLResponse, RedirectResponse, Response
from lilya.testclient.base import TestClient

from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.contrib.dashboard.admin.protocols import AuthBackend


class HeaderBackend(AuthBackend):
    async def authenticate(self, request: Request) -> dict[str, object] | None:
        user = request.headers.get("X-Authenticated-User")
        if not user:
            return None
        return {
            "id": user,
            "name": user,
            "is_admin": request.headers.get("X-Authenticated-Admin", "true"),
            "roles": request.headers.get("X-Authenticated-Roles", "").split(","),
        }

    async def login(self, request: Request) -> Response:
        return HTMLResponse("Header auth required")

    async def logout(self, request: Request) -> Response:
        return RedirectResponse("/login", status_code=303)


def test_admin_does_not_enable_cross_origin_dashboard_access_by_default():
    client = TestClient(AsyncMQAdmin(enable_login=False).get_asgi_app(with_url_prefix=True))

    response = client.options(
        "/asyncmq/",
        headers={
            "Origin": "https://attacker.example",
            "Access-Control-Request-Method": "GET",
        },
    )

    assert "access-control-allow-origin" not in response.headers
    assert "access-control-allow-credentials" not in response.headers


def test_admin_allows_explicit_dashboard_cors_origin():
    client = TestClient(
        AsyncMQAdmin(
            enable_login=False,
            cors_allow_origins=("https://ops.example",),
            cors_allow_credentials=True,
        ).get_asgi_app(with_url_prefix=True)
    )

    response = client.options(
        "/asyncmq/",
        headers={
            "Origin": "https://ops.example",
            "Access-Control-Request-Method": "GET",
        },
    )

    assert response.headers["access-control-allow-origin"] == "https://ops.example"
    assert response.headers["access-control-allow-credentials"] == "true"


def test_admin_rejects_wildcard_credentialed_dashboard_cors():
    with pytest.raises(ValueError, match="wildcard origins with credentials"):
        AsyncMQAdmin(
            enable_login=False,
            cors_allow_origins=("*",),
            cors_allow_credentials=True,
        )


def test_auth_gate_rejects_cross_origin_mutating_dashboard_request():
    client = TestClient(
        AsyncMQAdmin(
            enable_login=True,
            backend=HeaderBackend(),
            include_session=False,
        ).get_asgi_app(with_url_prefix=True)
    )

    response = client.post(
        "/asyncmq/queues/critical",
        data={"action": "pause"},
        headers={
            "X-Authenticated-User": "alice",
            "Origin": "https://attacker.example",
        },
    )

    assert response.status_code == 403
    assert response.text == "Cross-origin dashboard request rejected"


def test_auth_gate_allows_same_origin_mutating_dashboard_request():
    client = TestClient(
        AsyncMQAdmin(
            enable_login=True,
            backend=HeaderBackend(),
            include_session=False,
        ).get_asgi_app(with_url_prefix=True)
    )

    response = client.post(
        "/asyncmq/queues/critical",
        data={"action": "pause"},
        headers={
            "X-Authenticated-User": "alice",
            "Origin": "http://testserver",
        },
        follow_redirects=False,
    )

    assert response.status_code != 403


def test_auth_gate_same_origin_enforcement_can_be_disabled():
    client = TestClient(
        AsyncMQAdmin(
            enable_login=True,
            backend=HeaderBackend(),
            include_session=False,
            enforce_same_origin=False,
        ).get_asgi_app(with_url_prefix=True)
    )

    response = client.post(
        "/asyncmq/queues/critical",
        data={"action": "pause"},
        headers={
            "X-Authenticated-User": "alice",
            "Origin": "https://attacker.example",
        },
        follow_redirects=False,
    )

    assert response.status_code != 403


def test_auth_gate_rejects_authenticated_non_admin_by_default():
    client = TestClient(
        AsyncMQAdmin(
            enable_login=True,
            backend=HeaderBackend(),
            include_session=False,
        ).get_asgi_app(with_url_prefix=True)
    )

    response = client.get(
        "/asyncmq/",
        headers={
            "X-Authenticated-User": "alice",
            "X-Authenticated-Admin": "false",
        },
        follow_redirects=False,
    )

    assert response.status_code == 403
    assert response.text == "Dashboard user is not authorized"


def test_auth_gate_allows_required_role_when_admin_check_disabled():
    client = TestClient(
        AsyncMQAdmin(
            enable_login=True,
            backend=HeaderBackend(),
            include_session=False,
            require_admin=False,
            required_roles=("ops",),
        ).get_asgi_app(with_url_prefix=True)
    )

    response = client.get(
        "/asyncmq/",
        headers={
            "X-Authenticated-User": "alice",
            "X-Authenticated-Admin": "false",
            "X-Authenticated-Roles": "viewer,ops",
        },
        follow_redirects=False,
    )

    assert response.status_code == 200


def test_auth_gate_rejects_missing_required_role():
    client = TestClient(
        AsyncMQAdmin(
            enable_login=True,
            backend=HeaderBackend(),
            include_session=False,
            require_admin=False,
            required_roles=("ops",),
        ).get_asgi_app(with_url_prefix=True)
    )

    response = client.get(
        "/asyncmq/",
        headers={
            "X-Authenticated-User": "alice",
            "X-Authenticated-Roles": "viewer",
        },
        follow_redirects=False,
    )

    assert response.status_code == 403
    assert response.text == "Dashboard user is not authorized"

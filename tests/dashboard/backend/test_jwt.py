import time
import typing as t

import jwt
import pytest
from lilya.apps import Lilya
from lilya.testclient.base import TestClient

from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.contrib.dashboard.admin.backends.jwt import JWTAuthBackend


@pytest.fixture
def lilya_app_jwt_backend() -> Lilya:
    app = Lilya()

    admin = AsyncMQAdmin(
        enable_login=True,
        backend=JWTAuthBackend(
            secret="test-secret",
            algorithms=["HS256"],
            audience=None,  # keep simple for tests
            issuer=None,
            user_claim="sub",
            user_name_claim="name",
            verify_options={"verify_exp": True},
        ),
        include_session=True,
        include_cors=False,
    )
    admin.include_in(app)
    return app


@pytest.fixture
def client(lilya_app_jwt_backend: Lilya) -> TestClient:
    return TestClient(lilya_app_jwt_backend)


def _make_token(
    *,
    sub: str = "alice",
    name: str = "Alice",
    secret: str = "test-secret",
    exp_in_seconds: int = 300,
    audience: str | None = None,
    issuer: str | None = None,
) -> str:
    now = int(time.time())
    payload: dict[str, t.Any] = {
        "sub": sub,
        "name": name,
        "iat": now,
        "exp": now + exp_in_seconds,
    }
    if audience is not None:
        payload["aud"] = audience
    if issuer is not None:
        payload["iss"] = issuer
    return jwt.encode(payload, secret, algorithm="HS256")


def test_jwt_backend_blocks_without_header(client: TestClient):
    response = client.get("/asyncmq/", follow_redirects=False)

    assert response.status_code == 303
    assert "/asyncmq/login" in response.headers.get("location", "")


def test_jwt_backend_accepts_valid_token(client: TestClient):
    token = _make_token()
    response = client.get("/asyncmq/", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 200

    # loose check — content depends on templates
    assert "Alice" in response.text or "Dashboard" in response.text


def test_jwt_backend_rejects_bad_token(client: TestClient):
    response = client.get(
        "/asyncmq/",
        headers={"Authorization": "Bearer not-a-real-jwt"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert "/asyncmq/login" in response.headers.get("location", "")


def test_jwt_backend_logout_is_stateless(client: TestClient):
    token = _make_token(sub="bob", name="Bob")

    # "logout" should not affect header-based auth
    client.get("/asyncmq/logout", follow_redirects=False)
    response = client.get("/asyncmq/", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 200

import time
import typing as t

import jwt
import pytest
from lilya.apps import Lilya
from lilya.testclient.base import TestClient

from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.contrib.dashboard.admin.backends.jwt import JWTAuthBackend

JWT_SECRET = "test-secret-for-asyncmq-jwt-32-bytes"


@pytest.fixture
def lilya_app_jwt_backend() -> Lilya:
    app = Lilya()

    admin = AsyncMQAdmin(
        enable_login=True,
        backend=JWTAuthBackend(
            secret=JWT_SECRET,
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
    is_admin: bool | None = True,
    roles: list[str] | None = None,
    secret: str = JWT_SECRET,
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
    if is_admin is not None:
        payload["is_admin"] = is_admin
    if roles is not None:
        payload["roles"] = roles
    if audience is not None:
        payload["aud"] = audience
    if issuer is not None:
        payload["iss"] = issuer
    return jwt.encode(payload, secret, algorithm="HS256")


def test_jwt_backend_blocks_without_header(client: TestClient):
    response = client.get("/asyncmq/", follow_redirects=False)

    assert response.status_code == 303
    assert "/asyncmq/login" in response.headers.get("location", "")


def test_jwt_backend_rejects_short_hmac_secret():
    with pytest.raises(ValueError, match="at least 32 bytes"):
        JWTAuthBackend(secret="test-secret", algorithms=["HS256"])


def test_jwt_backend_accepts_valid_token(client: TestClient):
    token = _make_token()
    response = client.get("/asyncmq/", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 200

    # loose check — content depends on templates
    assert "Alice" in response.text or "Dashboard" in response.text


def test_jwt_backend_rejects_token_without_admin_authorization(client: TestClient):
    token = _make_token(is_admin=None)
    response = client.get(
        "/asyncmq/",
        headers={"Authorization": f"Bearer {token}"},
        follow_redirects=False,
    )

    assert response.status_code == 403
    assert response.text == "Dashboard user is not authorized"


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

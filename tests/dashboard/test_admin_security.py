import pytest
from lilya.testclient.base import TestClient

from asyncmq.contrib.dashboard.admin import AsyncMQAdmin


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

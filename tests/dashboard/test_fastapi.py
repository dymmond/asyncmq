import pytest

from asyncmq.backends.redis import RedisBackend
from asyncmq.conf import monkay
from asyncmq.contrib.dashboard.application import dashboard
from asyncmq.core.utils.dashboard import DashboardConfig

# Skip all tests in this module if FastAPI is not installed
fastapi = pytest.importorskip("fastapi")

config = DashboardConfig()


@pytest.fixture(scope="module")
def app():
    from starlette.middleware.sessions import SessionMiddleware

    app = fastapi.FastAPI()
    app.add_middleware(SessionMiddleware, secret_key="your-secret")
    app.mount("/fastapi", dashboard, name="asyncmq-admin")
    return app


@pytest.fixture(scope="module")
def client(app):
    from starlette.testclient import TestClient

    with TestClient(app) as client:
        yield client


def test_home_page(client):
    monkay.settings.backend = RedisBackend()
    response = client.get(("fastapi/admin"))
    assert response.status_code == 200
    assert config.title.encode() in response.content

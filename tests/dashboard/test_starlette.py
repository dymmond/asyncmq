import pytest
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.testclient import TestClient

from asyncmq.backends.redis import RedisBackend
from asyncmq.conf import settings
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.core.utils.dashboard import DashboardConfig

fastapi = pytest.importorskip("starlette")

config = DashboardConfig()


@pytest.fixture(scope="module")
def app():
    asyncmq_admin = AsyncMQAdmin(enable_login=False)
    app = Starlette(
        routes=[
            Mount(
                "/starlette",
                asyncmq_admin.get_asgi_app(with_url_prefix=True),
            )
        ]
    )
    return app


@pytest.fixture(scope="module")
def client(app):
    with TestClient(app) as client:
        yield client


def test_home_page(client):
    settings.backend = RedisBackend()
    response = client.get("/starlette/asyncmq/")
    assert response.status_code == 200
    assert config.title.encode() in response.content


def test_sidebar_links_include_mount_prefix(client):
    settings.backend = RedisBackend()

    # Load the dashboard home page
    resp = client.get("/starlette/asyncmq")
    assert resp.status_code == 200

    html = resp.text

    # Expected links with mount prefix
    assert 'href="/starlette/asyncmq"' in html
    assert 'href="/starlette/asyncmq/queues"' in html
    assert 'href="/starlette/asyncmq/metrics"' in html
    assert 'href="/starlette/asyncmq/audit"' in html
    assert 'href="/starlette/asyncmq/workers"' in html

    # Ensure incorrect, unmounted links are NOT present
    assert 'href="/asyncmq"' not in html
    assert 'href="/asyncmq/queues"' not in html
    assert 'href="/asyncmq/metrics"' not in html
    assert 'href="/asyncmq/audit"' not in html
    assert 'href="/asyncmq/workers"' not in html

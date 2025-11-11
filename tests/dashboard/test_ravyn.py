import pytest
from ravyn import Ravyn

from asyncmq.backends.redis import RedisBackend
from asyncmq.conf import settings
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.core.utils.dashboard import DashboardConfig

config = DashboardConfig()


@pytest.fixture(scope="module")
def app():
    asyncmq_admin = AsyncMQAdmin(enable_login=False)
    app = Ravyn()
    app.include(
        "/ravyn",
        asyncmq_admin.get_asgi_app(with_url_prefix=True),
    )

    return app


@pytest.fixture(scope="module")
def client(app):
    from ravyn.testclient import TestClient

    with TestClient(app) as client:
        yield client


def test_home_page(client):
    settings.backend = RedisBackend()
    response = client.get("/ravyn/asyncmq/")
    assert response.status_code == 200
    assert config.title.encode() in response.content


def test_sidebar_links_include_mount_prefix(client):
    settings.backend = RedisBackend()

    # Load the dashboard home page
    resp = client.get("/ravyn/asyncmq")
    assert resp.status_code == 200

    html = resp.text

    # Expected links with mount prefix
    assert 'href="/ravyn/asyncmq"' in html
    assert 'href="/ravyn/asyncmq/queues"' in html
    assert 'href="/ravyn/asyncmq/metrics"' in html
    assert 'href="/ravyn/asyncmq/workers"' in html

    # Ensure incorrect, unmounted links are NOT present
    assert 'href="/admin"' not in html
    assert 'href="/admin/queues"' not in html
    assert 'href="/admin/metrics"' not in html
    assert 'href="/admin/workers"' not in html

import pytest

from asyncmq.backends.redis import RedisBackend
from asyncmq.conf import settings
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
    settings.backend = RedisBackend()
    response = client.get(("fastapi/admin"))
    assert response.status_code == 200
    assert config.title.encode() in response.content


def test_sidebar_links_include_mount_prefix(client):
    settings.backend = RedisBackend()

    # Load the dashboard home page
    resp = client.get("/fastapi/admin")
    assert resp.status_code == 200

    html = resp.text

    # Expected links with mount prefix
    assert 'href="/fastapi/admin"' in html
    assert 'href="/fastapi/admin/queues"' in html
    assert 'href="/fastapi/admin/metrics"' in html
    assert 'href="/fastapi/admin/workers"' in html

    # Ensure incorrect, unmounted links are NOT present
    assert 'href="/admin"' not in html
    assert 'href="/admin/queues"' not in html
    assert 'href="/admin/metrics"' not in html
    assert 'href="/admin/workers"' not in html


def test_extract_mount_path_prefers_root_path_when_available():
    scope = {"type": "http", "path": "/something/else", "root_path": "/fastapi"}
    # Expected behavior: if root_path is available, it should be returned as the mount path
    assert dashboard._extract_mount_path(scope) == "/fastapi"


def test_extract_mount_path_with_admin_in_path_still_uses_root_path():
    scope = {"type": "http", "path": "/fastapi/admin", "root_path": "/fastapi"}
    assert dashboard._extract_mount_path(scope) == "/fastapi"

import pytest
from lilya.apps import Lilya
from lilya.compat import reverse
from lilya.routing import Include
from lilya.testclient import TestClient

from asyncmq.backends.redis import RedisBackend
from asyncmq.conf import settings
from asyncmq.contrib.dashboard.application import create_dashboard_app
from asyncmq.core.utils.dashboard import DashboardConfig

config = DashboardConfig()


@pytest.fixture(scope="package")
def client():
    with TestClient(Lilya(routes=[Include(path="/", app=create_dashboard_app())])) as client:
        yield client


def test_home_page(client):
    settings.backend = RedisBackend()
    response = client.get(reverse("dashboard"))
    assert response.status_code == 200
    assert config.title.encode() in response.content


app = create_dashboard_app()


@pytest.mark.parametrize(
    "path, keyword",
    [
        (reverse("queues", app=app), b"Queues"),
        (reverse("workers", app=app), b"Workers"),
        (
            reverse("repeatables", path_params={"name": "default"}, app=app),
            b"Repeatable Jobs",
        ),
        (reverse("metrics"), b"Metrics"),
    ],
)
def test_pages(path, keyword, client):
    settings.backend = RedisBackend()
    response = client.get(path)
    assert response.status_code == 200
    assert keyword in response.content

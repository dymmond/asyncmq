import pytest
from lilya.apps import Lilya
from lilya.compat import reverse
from lilya.routing import Include
from lilya.testclient import TestClient

from asyncmq.backends.redis import RedisBackend
from asyncmq.conf import monkay
from asyncmq.contrib.dashboard.serve import dash_app
from asyncmq.core.utils.dashboard import DashboardConfig

config = DashboardConfig()


@pytest.fixture(scope="package")
def client():
    with TestClient(Lilya(routes=[Include(path="/", app=dash_app)])) as client:
        yield client


def xtest_home_page(client):
    monkay.settings.backend = RedisBackend()
    response = client.get(reverse("dashboard"))
    assert response.status_code == 200
    assert config.title.encode() in response.content


@pytest.mark.parametrize(
    "path, keyword",
    [
        (reverse("queues"), b"Queues"),
        (reverse("workers"), b"Workers"),
        (reverse("repeatables", path_params={"name": "default"}), b"Repeatable Jobs"),
        (reverse("metrics"), b"Metrics"),
    ],
)
def xtest_pages(path, keyword, client):
    monkay.settings.backend = RedisBackend()
    response = client.get(path)
    assert response.status_code == 200
    assert keyword in response.content

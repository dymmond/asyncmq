from __future__ import annotations

import hashlib
import json
import re
from importlib import resources
from typing import Any

import pytest
from lilya.apps import Lilya
from lilya.middleware import Middleware
from lilya.middleware.sessions import SessionMiddleware
from lilya.routing import Include
from lilya.testclient import TestClient

from asyncmq.conf import settings
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.contrib.dashboard.admin.backends.simple_user import SimpleUsernamePasswordBackend
from asyncmq.contrib.dashboard.admin.protocols import User
from asyncmq.contrib.dashboard.application import create_dashboard_app


class AssetBackend:
    """Minimal dashboard backend for rendering static-asset smoke pages."""

    async def list_queues(self) -> list[str]:
        """Return no queues so dashboard pages render without backend setup."""
        return []

    async def list_workers(self) -> list[dict[str, Any]]:
        """Return no workers so worker pages render deterministically."""
        return []


def verify_user(username: str, password: str) -> User | None:
    """Verify the fixed test credentials for the bundled login template."""
    if username == "admin" and password == "secret":
        return User(id="admin", name="Admin", is_admin=True)
    return None


@pytest.fixture
def client() -> TestClient:
    """Create a direct dashboard client for static and template checks."""
    settings.backend = AssetBackend()
    app = Lilya(
        middleware=[Middleware(SessionMiddleware, secret_key="dashboard-assets")],
        routes=[Include(path="/", app=create_dashboard_app())],
    )
    return TestClient(app)


@pytest.fixture
def admin_client() -> TestClient:
    """Create an authenticated contrib app client using the bundled login template."""
    settings.backend = AssetBackend()
    app = Lilya()
    admin = AsyncMQAdmin(
        enable_login=True,
        backend=SimpleUsernamePasswordBackend(verify_user),
        include_session=True,
        include_cors=False,
    )
    admin.include_in(app)
    return TestClient(app)


def package_static_root():
    """Return the packaged static resource root for the dashboard contrib app."""
    return resources.files("asyncmq.contrib.dashboard").joinpath("statics")


def sha256_bytes(data: bytes) -> str:
    """Return the lowercase SHA-256 digest for bytes."""
    return hashlib.sha256(data).hexdigest()


def test_static_assets_are_served_locally(client: TestClient):
    """Serve Alpine, Tailwind, Chart.js, and manifest assets from package statics."""
    expected_assets = {
        "/static/favicon.ico": b"\x00\x00\x01\x00",
        "/static/css/tailwind-4.3.2.min.css": "tailwindcss v4.3.2",
        "/static/vendor/alpinejs/alpine-csp-3.15.12.min.js": 'version:"3.15.12"',
        "/static/vendor/chartjs/chart.umd-4.5.1.min.js": "Chart.js v4.5.1",
        "/static/vendor/manifest.json": '"alpinejs"',
    }

    for path, marker in expected_assets.items():
        response = client.get(path)
        assert response.status_code == 200
        if isinstance(marker, bytes):
            assert response.content.startswith(marker)
        else:
            assert marker in response.text


def test_vendor_manifest_checksums_match_packaged_assets():
    """Ensure recorded integrity checksums match the files included in the package."""
    static_root = package_static_root()
    manifest = json.loads(static_root.joinpath("vendor/manifest.json").read_text())

    for metadata in manifest.values():
        artifact = static_root.joinpath(metadata["artifact"])
        assert artifact.is_file()
        assert sha256_bytes(artifact.read_bytes()) == metadata["artifact_sha256"]


def test_templates_reference_local_assets_without_public_cdns(client: TestClient):
    """Render core pages and verify they load local dashboard assets only."""
    disallowed = (
        "cdn.tailwindcss.com",
        "cdn.jsdelivr.net",
        "unpkg.com",
        "tailwind.config",
    )

    for path in ("/", "/queues", "/workers", "/metrics", "/missing-page"):
        response = client.get(path)
        assert response.status_code in {200, 404}
        html = response.text
        for marker in disallowed:
            assert marker not in html
        assert "/static/css/tailwind-4.3.2.min.css" in html
        if path != "/missing-page":
            assert "/static/vendor/alpinejs/alpine-csp-3.15.12.min.js" in html


def test_bundled_login_template_uses_local_assets(admin_client: TestClient):
    """Render the packaged login page and verify it does not depend on CDN assets."""
    prefix = settings.dashboard_config.dashboard_url_prefix
    response = admin_client.get(f"{prefix}/login")

    assert response.status_code == 200
    assert f'{prefix}/static/favicon.ico' in response.text
    assert f"{prefix}/static/css/tailwind-4.3.2.min.css" in response.text
    assert f"{prefix}/static/vendor/alpinejs/alpine-csp-3.15.12.min.js" in response.text
    assert "cdn.tailwindcss.com" not in response.text
    assert "unpkg.com" not in response.text
    assert "tailwind.config" not in response.text


def test_modern_dashboard_shell_renders_component_navigation(client: TestClient):
    """Render the shared operations shell with desktop and mobile navigation."""
    response = client.get("/queues")

    assert response.status_code == 200
    assert 'class="amq-shell"' in response.text
    assert 'class="amq-sidebar"' in response.text
    assert 'class="amq-mobile-nav"' in response.text
    assert 'aria-current="page"' in response.text
    assert '<h1 class="amq-page-title">Queues</h1>' in response.text
    assert "/static/css/asyncmq.css" in response.text
    assert "Operations Console" in response.text


def test_overview_live_rows_avoid_interpolated_html(client: TestClient):
    """Ensure live overview table updates do not interpolate runtime values as HTML."""
    response = client.get("/")
    js = package_static_root().joinpath("js/asyncmq.js").read_text()

    assert response.status_code == 200
    assert 'data-asyncmq-overview data-events-url="/asyncmq/events"' in response.text
    assert 'new EventSource("{{ url_prefix }}/events")' not in response.text
    assert 'new EventSource("/asyncmq/events")' not in response.text
    assert 'appendTextCell(row, job.id, "amq-mono")' in js
    assert "badgeForState(job.state)" in js
    assert "${j.id}" not in js
    assert "${q.name}" not in js


def test_primary_live_pages_do_not_render_inline_scripts(client: TestClient):
    """Keep primary live pages compatible with strict CSP."""
    inline_script = re.compile(r"<script(?![^>]*\bsrc=)[^>]*>", re.IGNORECASE)

    for path in ("/", "/queues", "/queues/critical-email", "/metrics"):
        response = client.get(path)

        assert response.status_code == 200
        assert inline_script.search(response.text) is None


def test_queue_pages_use_packaged_live_update_components(client: TestClient):
    """Render live-update hooks as data attributes consumed by packaged JS."""
    queue_list = client.get("/queues")
    queue_detail = client.get("/queues/critical-email")
    js = package_static_root().joinpath("js/asyncmq.js").read_text()

    assert queue_list.status_code == 200
    assert queue_detail.status_code == 200
    assert 'data-asyncmq-queues data-events-url="/asyncmq/events"' in queue_list.text
    assert 'data-asyncmq-queue-detail data-events-url="/asyncmq/events"' in queue_detail.text
    assert 'data-queue-name="critical-email"' in queue_detail.text
    assert "setupQueueListLive" in js
    assert "setupQueueDetailLive" in js


def test_metrics_page_uses_packaged_live_update_components(client: TestClient):
    """Render metrics live-update hooks as CSP-safe data consumed by packaged JS."""
    response = client.get("/metrics")
    js = package_static_root().joinpath("js/asyncmq.js").read_text()

    assert response.status_code == 200
    assert 'data-asyncmq-metrics' in response.text
    assert 'data-history-url="/asyncmq/metrics/history?limit=60"' in response.text
    assert '<template id="metrics-history-data">' in response.text
    assert "const initialHistory" not in response.text
    assert "innerHTML" not in response.text
    assert "setupMetricsLive" in js
    assert "appendTextCell(row, metric.throughput" in js

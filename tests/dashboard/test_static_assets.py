from __future__ import annotations

import hashlib
import json
import re
from html.parser import HTMLParser
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


class AccessibleControlParser(HTMLParser):
    """Collect rendered controls that do not expose an accessible name."""

    def __init__(self) -> None:
        super().__init__()
        self.label_depth = 0
        self.label_targets: set[str] = set()
        self.unnamed_controls: list[str] = []
        self._button_stack: list[dict[str, Any]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {key: value or "" for key, value in attrs}
        if tag == "label":
            self.label_depth += 1
            target = attributes.get("for")
            if target:
                self.label_targets.add(target)
            return

        if tag in {"input", "select", "textarea"}:
            input_type = attributes.get("type", "").lower()
            if input_type == "hidden":
                return
            control_id = attributes.get("id", "")
            has_name = bool(
                self.label_depth
                or attributes.get("aria-label")
                or attributes.get("aria-labelledby")
                or attributes.get("title")
                or (control_id and control_id in self.label_targets)
            )
            if not has_name:
                self.unnamed_controls.append(f"{tag}[name={attributes.get('name', '')}]")
            return

        if tag == "button":
            self._button_stack.append(
                {
                    "explicit": bool(
                        attributes.get("aria-label") or attributes.get("aria-labelledby") or attributes.get("title")
                    ),
                    "text": "",
                    "name": attributes.get("name", ""),
                }
            )

    def handle_data(self, data: str) -> None:
        if self._button_stack:
            self._button_stack[-1]["text"] += data

    def handle_endtag(self, tag: str) -> None:
        if tag == "label" and self.label_depth:
            self.label_depth -= 1
            return
        if tag == "button" and self._button_stack:
            button = self._button_stack.pop()
            if not button["explicit"] and not str(button["text"]).strip():
                self.unnamed_controls.append(f"button[name={button['name']}]")


class AssetBackend:
    """Minimal dashboard backend for rendering static-asset smoke pages."""

    async def list_queues(self) -> list[str]:
        """Return no queues so dashboard pages render without backend setup."""
        return []

    async def list_workers(self) -> list[dict[str, Any]]:
        """Return no workers so worker pages render deterministically."""
        return []

    async def list_jobs(self, queue: str, state: str) -> list[dict[str, Any]]:
        """Return no jobs so job-like pages render deterministically."""
        return []

    async def list_repeatables(self, queue_name: str) -> list[dict[str, Any]]:
        """Return no repeatables so repeatable pages render deterministically."""
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
        assert "toastify" not in html.lower()
        if path != "/missing-page":
            assert "/static/vendor/alpinejs/alpine-csp-3.15.12.min.js" in html


def test_bundled_login_template_uses_local_assets(admin_client: TestClient):
    """Render the packaged login page and verify it does not depend on CDN assets."""
    prefix = settings.dashboard_config.dashboard_url_prefix
    response = admin_client.get(f"{prefix}/login")

    assert response.status_code == 200
    assert f"{prefix}/static/favicon.ico" in response.text
    assert f"{prefix}/static/css/tailwind-4.3.2.min.css" in response.text
    assert f"{prefix}/static/css/asyncmq.css" in response.text
    assert f"{prefix}/static/vendor/alpinejs/alpine-csp-3.15.12.min.js" in response.text
    assert 'class="amq-auth-card"' in response.text
    assert 'class="amq-auth-form"' in response.text
    assert "bg-gradient-to-br" not in response.text
    assert "cdn.tailwindcss.com" not in response.text
    assert "unpkg.com" not in response.text
    assert "tailwind.config" not in response.text


def test_not_found_and_shared_feedback_use_operations_styles(client: TestClient):
    """Render error and shared feedback templates with AsyncMQ-owned classes."""
    response = client.get("/missing-page")
    template_root = resources.files("asyncmq.contrib.dashboard").joinpath("templates")
    static_root = package_static_root()
    loading = template_root.joinpath("shared/loading.html")
    messages = template_root.joinpath("shared/messages.html")

    assert response.status_code == 404
    assert "/static/css/asyncmq.css" in response.text
    assert 'class="amq-error-page"' in response.text
    assert "bg-gray-100" not in response.text
    assert "amq-loading-overlay" in loading.read_text()
    assert "amq-flash" in messages.read_text()
    assert "bg-{{ color }}-50" not in messages.read_text()
    assert ".amq-loading-overlay.hidden" in static_root.joinpath("css/asyncmq.css").read_text()


def test_dashboard_templates_do_not_keep_legacy_tailwind_utility_markup():
    """Keep operator templates on the AsyncMQ component classes after modernization."""
    template_root = resources.files("asyncmq.contrib.dashboard").joinpath("templates")
    legacy_markers = (
        "font-medium",
        "bg-gray-",
        "text-gray-",
        "rounded-lg",
        "shadow-soft",
        "space-y-",
    )

    for template in template_root.rglob("*.html"):
        text = template.read_text()
        for marker in legacy_markers:
            assert marker not in text, f"{template} still contains {marker!r}"


def test_rendered_dashboard_controls_have_accessible_names(client: TestClient, admin_client: TestClient):
    """Scan rendered dashboard pages for unlabeled controls and nameless buttons."""
    pages = [
        client.get("/queues/critical-email/jobs?state=failed"),
        client.get("/workers"),
        client.get("/audit"),
        client.get("/events/history"),
        client.get("/queues/critical-email/dlq"),
        client.get("/queues/critical-email/repeatables/new"),
        admin_client.get(f"{settings.dashboard_config.dashboard_url_prefix}/login"),
    ]

    for response in pages:
        assert response.status_code == 200
        parser = AccessibleControlParser()
        parser.feed(response.text)
        assert parser.unnamed_controls == []


def test_modern_dashboard_shell_renders_component_navigation(client: TestClient):
    """Render the shared operations shell with desktop and mobile navigation."""
    response = client.get("/queues")
    css = package_static_root().joinpath("css/asyncmq.css").read_text()

    assert response.status_code == 200
    assert 'class="amq-shell"' in response.text
    assert 'class="amq-sidebar"' in response.text
    assert 'class="amq-mobile-nav"' in response.text
    assert 'aria-current="page"' in response.text
    assert '<h1 class="amq-page-title">Queues</h1>' in response.text
    assert "/static/css/asyncmq.css" in response.text
    assert "Operations Console" in response.text
    assert "linear-gradient(90deg, #17200f 0 280px, transparent 280px)" in css
    assert ".amq-body {\n  height: 100vh;" in css
    assert "overflow: hidden;" in css
    assert ".amq-shell {\n  display: grid;\n  grid-template-columns: 280px minmax(0, 1fr);\n  height: 100dvh;" in css
    assert ".amq-sidebar {\n  position: fixed;" in css
    assert "width: 280px;" in css
    assert "height: 100dvh;" in css
    assert ".amq-workspace {\n  grid-column: 2;" in css
    assert "overflow-y: auto;" in css
    assert ".amq-workspace {\n    height: auto;\n    overflow: visible;" in css


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


def test_primary_operator_pages_do_not_render_inline_scripts_or_styles(client: TestClient):
    """Keep primary operator pages compatible with strict CSP."""
    inline_script = re.compile(r"<script(?![^>]*\bsrc=)[^>]*>", re.IGNORECASE)

    for path in (
        "/",
        "/queues",
        "/queues/critical-email",
        "/queues/critical-email/dlq",
        "/queues/critical-email/repeatables",
        "/queues/critical-email/repeatables/new",
        "/metrics",
        "/audit",
        "/events/history",
        "/workers",
    ):
        response = client.get(path)

        assert response.status_code == 200
        assert inline_script.search(response.text) is None
        assert " style=" not in response.text


def test_operator_controls_keep_visible_focus_and_table_scopes(client: TestClient):
    """Keep keyboard focus and audit-table semantics visible to operators."""
    css = package_static_root().joinpath("css/asyncmq.css").read_text()
    js = package_static_root().joinpath("js/asyncmq.js").read_text()
    response = client.get("/audit")

    assert response.status_code == 200
    assert ".amq-button:focus-visible" in css
    assert ".amq-nav-link:focus-visible" in css
    assert ".amq-logout-button:focus-visible" in css
    assert ".amq-diagnostic-section summary:focus-visible" in css
    assert ".amq-root-cause" in css
    assert ".amq-exception-chain" in css
    assert ".amq-scope-pill" in css
    assert ".amq-event-data-tools" in css
    assert "asyncmqClipboard" in js
    assert "navigator.clipboard" in js
    assert css.count("box-shadow: 0 0 0 3px rgba(203, 220, 56, .42)") >= 6
    assert '<th scope="col">Time</th>' in response.text
    assert '<th scope="col">Details</th>' in response.text


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
    assert 'class="amq-queue-detail-hero"' in queue_detail.text
    assert '<h1 class="text-3xl font-semibold mb-6">' not in queue_detail.text
    assert 'class="flex items-center p-4' not in queue_detail.text
    assert "setupQueueListLive" in js
    assert "setupQueueDetailLive" in js
    assert "root.querySelector(`.${state}`)" in js
    assert 'root.querySelector("[data-queue-status] .amq-badge")' in js


def test_metrics_page_uses_packaged_live_update_components(client: TestClient):
    """Render metrics live-update hooks as CSP-safe data consumed by packaged JS."""
    response = client.get("/metrics")
    js = package_static_root().joinpath("js/asyncmq.js").read_text()

    assert response.status_code == 200
    assert "data-asyncmq-metrics" in response.text
    assert 'data-history-url="/asyncmq/metrics/history?limit=60"' in response.text
    assert 'class="amq-table amq-metrics-table"' in response.text
    assert '<template id="metrics-history-data">' in response.text
    assert '<h1 class="text-3xl font-semibold mb-6">' not in response.text
    assert "const initialHistory" not in response.text
    assert "innerHTML" not in response.text
    assert "setupMetricsLive" in js
    assert 'appendTextCell(row, normalizeMetricTime(metric), "amq-mono")' in js
    assert 'appendTextCell(row, "No history yet.", "amq-table-empty")' in js

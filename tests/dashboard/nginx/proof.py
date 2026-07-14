from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import Page, Response, expect, sync_playwright

BASE_URL = os.environ.get("ASYNCMQ_NGINX_BASE_URL", "http://127.0.0.1:18080/operations/asyncmq")
OUT = Path(os.environ.get("ASYNCMQ_NGINX_SCREENSHOT_DIR", "/tmp/asyncmq-nginx-proof"))
DEFAULT_CHROME = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
BASE_PATH = urlparse(BASE_URL).path.rstrip("/")


def assert_security_headers(response: Response | None) -> None:
    """Verify security headers survive the Nginx proxy hop."""
    assert response is not None
    assert response.status < 400
    headers = response.headers
    assert "default-src 'self'" in headers.get("content-security-policy", "")
    assert headers.get("x-frame-options") == "DENY"


def assert_prefixed_assets(page: Page) -> None:
    """Verify static assets are generated under the proxied dashboard prefix."""
    links = page.locator("link[rel='stylesheet']").evaluate_all("els => els.map(el => el.href)")
    scripts = page.locator("script[src]").evaluate_all("els => els.map(el => el.src)")
    assert any(f"{BASE_PATH}/static/css/asyncmq.css" in href for href in links)
    assert any(f"{BASE_PATH}/static/vendor/alpinejs/" in src for src in scripts)


def assert_accessibility_basics(page: Page) -> None:
    """Run a browser-side accessibility smoke scan with no Node dependency."""
    violations = page.evaluate(
        """
        () => {
          const visible = (el) => {
            const style = window.getComputedStyle(el);
            return style.display !== "none" && style.visibility !== "hidden" && el.offsetParent !== null;
          };
          const text = (el) => (el.innerText || el.textContent || "").trim();
          const labelFor = (id) => id ? document.querySelector(`label[for="${CSS.escape(id)}"]`) : null;
          const nameFor = (el) => {
            const labelledBy = el.getAttribute("aria-labelledby");
            if (labelledBy) {
              const value = labelledBy
                .split(/\\s+/)
                .map((id) => text(document.getElementById(id) || document.createElement("span")))
                .join(" ")
                .trim();
              if (value) return value;
            }
            const aria = el.getAttribute("aria-label");
            if (aria && aria.trim()) return aria.trim();
            const label = labelFor(el.id);
            if (label && text(label)) return text(label);
            if (el.closest("label") && text(el.closest("label"))) return text(el.closest("label"));
            if (el.tagName === "BUTTON" || el.tagName === "A") return text(el);
            return "";
          };
          const failures = [];

          if (!document.title.trim()) failures.push("document title is empty");
          if (!document.querySelector("main")) failures.push("missing main landmark");
          if (document.querySelectorAll("h1").length < 1) failures.push("missing h1");

          document.querySelectorAll("img").forEach((img) => {
            if (visible(img) && !img.hasAttribute("alt")) failures.push(`image missing alt: ${img.src}`);
          });

          document.querySelectorAll("button, a[href], input:not([type=hidden]), select, textarea").forEach((el) => {
            if (visible(el) && !nameFor(el)) {
              failures.push(`${el.tagName.toLowerCase()} missing accessible name`);
            }
          });

          document.querySelectorAll("table").forEach((table) => {
            if (!table.querySelector("th[scope]")) failures.push("table missing scoped headers");
          });

          return failures;
        }
        """
    )
    assert violations == [], violations


def main() -> None:
    """Exercise the dashboard through Nginx with a real browser."""
    OUT.mkdir(parents=True, exist_ok=True)
    failed_requests: list[str] = []
    failed_responses: list[str] = []
    console_errors: list[str] = []

    with sync_playwright() as playwright:
        executable = os.environ.get("ASYNCMQ_BROWSER_EXECUTABLE")
        if executable is None and DEFAULT_CHROME.exists():
            executable = str(DEFAULT_CHROME)
        browser = playwright.chromium.launch(headless=True, executable_path=executable)
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.on("requestfailed", lambda req: failed_requests.append(f"{req.method} {req.url}"))
        page.on(
            "response",
            lambda resp: failed_responses.append(f"{resp.status} {resp.url}") if resp.status >= 400 else None,
        )
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)

        response = page.goto(f"{BASE_URL}/login", wait_until="domcontentloaded")
        assert_security_headers(response)
        assert_prefixed_assets(page)
        assert_accessibility_basics(page)
        page.get_by_label("Username").fill("admin")
        page.locator("#password").fill("secret")
        page.get_by_role("button", name="Sign In").click(no_wait_after=True)
        page.wait_for_load_state("domcontentloaded")
        expect(page.get_by_role("heading", name="Dashboard")).to_be_visible()
        assert_accessibility_basics(page)
        assert page.url.rstrip("/") == BASE_URL.rstrip("/")
        page.screenshot(path=str(OUT / "nginx-login-dashboard.png"), full_page=True)

        response = page.goto(f"{BASE_URL}/queues", wait_until="domcontentloaded")
        assert_security_headers(response)
        assert_prefixed_assets(page)
        assert_accessibility_basics(page)
        expect(page.get_by_text("critical-email")).to_be_visible()
        page.get_by_role("button", name="Pause Queue").click()
        page.wait_for_url(f"{BASE_URL}/queues/critical-email")
        expect(page.get_by_text("paused.")).to_be_visible()
        assert_accessibility_basics(page)
        page.screenshot(path=str(OUT / "nginx-queues-action.png"), full_page=True)

        response = page.goto(f"{BASE_URL}/workers", wait_until="domcontentloaded")
        assert_security_headers(response)
        assert_accessibility_basics(page)
        expect(page.get_by_text("proxy-worker-1")).to_be_visible()
        page.screenshot(path=str(OUT / "nginx-workers.png"), full_page=True)

        response = page.goto(f"{BASE_URL}/queues/critical-email/jobs?state=failed", wait_until="domcontentloaded")
        assert_security_headers(response)
        assert_accessibility_basics(page)
        page.get_by_role("link", name="Inspect").first.click()
        page.wait_for_url(f"{BASE_URL}/queues/critical-email/jobs/proxy-job-001")
        expect(page.get_by_text("Root cause")).to_be_visible()
        assert_accessibility_basics(page)
        assert "nginx-secret-token" not in page.content()
        page.screenshot(path=str(OUT / "nginx-failed-job.png"), full_page=True)

        browser.close()

    required_failures = [item for item in failed_requests if "/events" not in item]
    required_response_failures = [item for item in failed_responses if "/events" not in item]
    assert required_failures == [], required_failures
    assert required_response_failures == [], required_response_failures
    assert console_errors == [], console_errors


if __name__ == "__main__":
    main()

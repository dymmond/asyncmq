# AsyncMQ Admin Dashboard

A clean, ASGI-native web UI (plus tiny management endpoints) for observing and operating your AsyncMQ queues,
no frontend build, no hassle. Works with Lilya, Ravyn, FastAPI, Starlette, Litestar, and any ASGI framework.

- Responsive UI (Tailwind).
- Queues, jobs, repeatables, DLQ, workers, and metrics.
- Drop-in wrapper: AsyncMQAdmin.
- Optional, pluggable auth via a simple AuthGateMiddleware.
- Mount anywhere under a URL prefix (defaults to /admin).

!!! Danger "Important"
    This is the second release of the AsyncMQ Admin Dashboard docs focused on feedback and iteration. This version
    is no longer compatible with the one prior to 0.6.0 and that its because of the newest and cleanest wrappers around
    it. See these docs how to update it, its just a couple of lines and you can readapt your project.

# Installation

No need to worry about anything. AsyncMQ brings the dashboard by default for you.

This brings the bundled templates/assets and the minimal dependencies. No separate frontend build required.

You must also enable sessions (used for flash messages and, if enabled, auth).

- If you're on Lilya or Starlette/FastAPI, use their session middleware if you want (although the default works flawlessly).
- The dashboard is deliberately ASGI-agnostic: any compatible session middleware works.

# Configuration (settings)

AsyncMQ exposes `settings.dashboard_config`, which should return a DashboardConfig instance (provided by AsyncMQ).
You can derive your own settings object that returns a customized config.

```python
from asyncmq.core.utils.dashboard import DashboardConfig
from asyncmq.conf import Settings

class MySettings(Settings):
    @property
    def dashboard_config(self) -> DashboardConfig:
        return DashboardConfig(
            title="My Queue Monitor",
            header_title="MyApp Tasks",
            description="Background processing at a glance.",
            favicon="/static/myfavicon.ico",
            dashboard_url_prefix="/admin",   # default
            sidebar_bg_colour="#3498db",
        )
```

Point AsyncMQ to your [settings](../features/settings.md) (for example with an env var if your project uses that pattern), then import:

```python
from asyncmq import settings

# later:
config = settings.dashboard_config
```

Key fields:

- title, header_title, description, favicon – basic look & feel
- dashboard_url_prefix – where the dashboard mounts (default /admin)
- sidebar_bg_colour – quick theming
- session_middleware – a pre-configured DefineMiddleware (used by AsyncMQAdmin if include_session=True)

# Quick Start

Mount the ready made dashboard ASGI app:

```python
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin

admin = AsyncMQAdmin(enable_login=False)  # public dashboard
```

## Lilya

This is a special case since the dashboard is built on top of it, so the `include_in` works like a charm in Lilya.

```python
from lilya.apps import Lilya
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin

app = Lilya()
admin = AsyncMQAdmin(enable_login=False)   # optional auth (see below)
admin.include_in(app)                      # mounts at config.dashboard_url_prefix (e.g. /admin)
```

## Ravyn

```python
from ravyn import Ravyn, Include
from ravyn.core.config.session import SessionConfig
from ravyn.conf import settings
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin

session_config = SessionConfig(secret_key=settings.secret_key)

asyncmq_admin = AsyncMQAdmin(enable_login=False)
app = Ravyn(
    routes=[
        # Important: do NOT force a custom route name, let the static discovery work.
        Include("/", app=asyncmq_admin.get_asgi_app(with_url_prefix=True))
    ],
    session_config=session_config,
)
```

!!! warning
    Don't pass a custom name to your Include/mount. It can break static discovery.

## FastAPI / Starlette

```python
from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="your-secret")

admin = AsyncMQAdmin(enable_login=False)

# Important: do NOT pass a custom 'name' in mount. it can break statics.
app.mount("/", admin.get_asgi_app(with_url_prefix=True))
```

# AsyncMQAdmin: the wrapper you mount

```python
class AsyncMQAdmin:
    def __init__(
        self,
        enable_login: bool = False,
        backend: AuthBackend | None = None,
        url_prefix: str | None = None,
        include_session: bool = True,
        include_cors: bool = True,
        login_path: str = "/login",
        allowlist: tuple[str, ...] = ("/login", "/logout", "/static", "/assets"),
    ): ...
```

- **enable_login**: turn on auth. Requires backend.
- **backend**: your AuthBackend implementation (see below).
- **url_prefix**: override mount path; defaults to monkay.settings.dashboard_config.dashboard_url_prefix.
- **include_session**: include dashboard_config.session_middleware.
- **include_cors**: permissive CORS (handy for local dev or embedding).
- **login_path & allowlist**: used by the auth gate.

* **Mounting options**:
    - Lilya only: admin.include_in(app)
    - Any ASGI: app.mount(..., admin.get_asgi_app()) or admin.get_asgi_app(with_url_prefix=True) if you want it prefixed inside a parent app tree.

* **Under the hood, AsyncMQAdmin builds a private ChildLilya with**:
    - CORS (optional)
    - Session middleware (optional)
    - AuthGateMiddleware (optional)
    - `/login` and `/logout` routes (if auth enabled)
    - The dashboard app via create_dashboard_app() at `/`

## Authentication

Auth is opt-in. When `enable_login=True`, requests are gated by `AuthGateMiddleware`:

- Paths in allowlist (e.g. /login, /logout, static assets) pass through.
- All other requests call backend.authenticate(request).
- If not authenticated:
    - Others get 303 redirect to login.

### Implementing AuthBackend

`AuthBackend` is a simple protocol, just implement it for your auth style (sessions, headers, tokens, etc.).

```python
from typing import Any
from lilya.requests import Request
from lilya.responses import Response, RedirectResponse
from asyncmq.contrib.dashboard.admin.protocols import AuthBackend

class SimpleUsernamePassword(AuthBackend):
    SESSION_KEY = "asyncmq_admin_user"

    async def authenticate(self, request: Request) -> Any | None:
        return request.session.get(self.SESSION_KEY)

    async def login(self, request: Request) -> Response:
        if request.method == "GET":
            # Render your login template or return a simple page
            from lilya.responses import HTMLResponse
            return HTMLResponse("<form method='post'>...</form>")
        form = await request.form()
        username, password = form.get("username"), form.get("password")
        if username == "admin" and password == "secret":
            request.session[self.SESSION_KEY] = {"id": "admin", "name": "Admin"}
            return RedirectResponse(form.get("next") or "/", status_code=303)
        return RedirectResponse("/login", status_code=303)

    async def logout(self, request: Request) -> Response:
        request.session.pop(self.SESSION_KEY, None)
        return RedirectResponse("/login", status_code=303)

    def routes(self) -> list[Any]:
        # Optional: add extra auth routes if you need them
        return []
```

### Example: simple username/password with `verify()`

If you prefer a compact callback-based pattern (similar to AsyncZ), you can implement a tiny username/password backend that delegates the actual check to a `verify(username, password)` function.

```python
from typing import Any, Callable
from lilya.requests import Request
from lilya.responses import Response, RedirectResponse, HTMLResponse
from asyncmq.contrib.dashboard.admin.protocols import AuthBackend

class SimpleUsernamePasswordBackend(AuthBackend):
    """
    Minimal session-backed auth that relies on a verify() callback you provide.
    The callback should return a serializable user dict (or a small value) on success, or None on failure.
    """
    def __init__(self, verify: Callable[[str, str], Any], session_key: str = "asyncmq_admin_uid"):
        self.verify = verify
        self.session_key = session_key

    async def authenticate(self, request: Request) -> Any | None:
        return request.session.get(self.session_key)

    async def login(self, request: Request) -> Response:
        if request.method == "GET":
            # Render a minimal login page; you can replace with your Jinja2 template.
            return HTMLResponse(
                """
                <form method="post" class="p-6 max-w-sm mx-auto">
                  <input name="username" placeholder="Username" class="block w-full mb-2" />
                  <input name="password" type="password" placeholder="Password" class="block w-full mb-4" />
                  <input type="hidden" name="next" value="/" />
                  <button type="submit">Sign in</button>
                </form>
                """
            )

        form = await request.form()
        username, password = form.get("username"), form.get("password")
        user = self.verify(username or "", password or "")
        if user is None:
            return RedirectResponse("/login", status_code=303)

        # Store a minimal payload in session
        request.session[self.session_key] = {"id": getattr(user, "id", username), "name": getattr(user, "name", username)}
        return RedirectResponse(form.get("next") or "/", status_code=303)

    async def logout(self, request: Request) -> Response:
        request.session.pop(self.session_key, None)
        return RedirectResponse("/login", status_code=303)

    def routes(self) -> list[Any]:
        return []
```

Use it with a custom `verify()`:

```python
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin

# Replace this with your real validation (DB lookup, IdP, etc.)
def verify(username: str, password: str):
    if username == "admin" and password == "secret":
        class U: id = "admin"; name = "Admin"
        return U()
    return None

admin = AsyncMQAdmin(enable_login=True, backend=SimpleUsernamePasswordBackend(verify))
```

!!! tip
    Keep the session payload minimal (e.g., just `user_id`). If you need more data, fetch it server-side per request.

### Real‑world recipes

#### Use your existing user table (hashed passwords)

```python
from typing import Any
from passlib.hash import bcrypt
from myapp.db import get_user_by_username
from asyncmq.contrib.dashboard.admin.protocols import AuthBackend, User  # if you have a User VO, otherwise return a dict
from lilya.requests import Request
from lilya.responses import Response, RedirectResponse

class DBSessionBackend(AuthBackend):
    def __init__(self, session_key: str = "asyncmq_admin_uid"):
        self.session_key = session_key

    async def authenticate(self, request: Request) -> Any | None:
        uid = request.session.get(self.session_key)
        return uid

    async def login(self, request: Request) -> Response:
        if request.method == "GET":
            from lilya.responses import HTMLResponse
            return HTMLResponse("&lt;form method='post'&gt;...&lt;/form&gt;")
        form = await request.form()
        username, password = form.get("username"), form.get("password")
        u = get_user_by_username(username)
        if not u or not bcrypt.verify(password, u.password_hash):
            return RedirectResponse("/login", status_code=303)
        request.session[self.session_key] = {"id": u.id, "name": u.display_name}
        return RedirectResponse(form.get("next") or "/", status_code=303)

    async def logout(self, request: Request) -> Response:
        request.session.pop(self.session_key, None)
        return RedirectResponse("/login", status_code=303)

    def routes(self) -> list[Any]:
        return []
```

#### Only store a minimal identifier in the session

```python
class MinimalSessionBackend(AuthBackend):
    def __init__(self, session_key="asyncmq_admin_uid"):
        self.session_key = session_key

    async def authenticate(self, request: Request):
        return request.session.get(self.session_key)  # e.g., {"id": "..."} only

    async def login(self, request: Request) -> Response:
        # validate credentials, then:
        request.session[self.session_key] = {"id": "user-123"}
        return RedirectResponse("/", status_code=303)

    async def logout(self, request: Request) -> Response:
        request.session.pop(self.session_key, None)
        return RedirectResponse("/login", status_code=303)

    def routes(self) -> list[Any]:
        return []
```

#### API-only protection (reverse proxy/header token)

```python
class ProxyHeaderBackend(AuthBackend):
    async def authenticate(self, request: Request):
        # Trust a header injected by your reverse proxy / SSO gateway (e.g., Nginx, Traefik, Auth0 proxy)
        sub = request.headers.get("X-Authenticated-User")
        return {"id": sub, "name": sub} if sub else None

    async def login(self, request: Request) -> Response:
        # Explain how to obtain access, or redirect to your provider
        from lilya.responses import HTMLResponse
        return HTMLResponse("Please login via your SSO provider.")

    async def logout(self, request: Request) -> Response:
        from lilya.responses import RedirectResponse
        return RedirectResponse("/login", status_code=303)

    def routes(self) -> list[Any]:
        return []
```

#### JWT bearer tokens (no sessions)

```python
import jwt
from jwt import InvalidTokenError

class JWTBearerBackend(AuthBackend):
    def __init__(self, public_key: str, algorithms: list[str] = ["RS256"]):
        self.public_key = public_key
        self.algorithms = algorithms

    async def authenticate(self, request: Request):
        auth = request.headers.get("authorization") or ""
        if not auth.lower().startswith("bearer "):
            return None
        token = auth.split(" ", 1)[1]
        try:
            payload = jwt.decode(token, self.public_key, algorithms=self.algorithms)
            # return a tiny identity object or dict
            return {"id": payload.get("sub"), "name": payload.get("name") or payload.get("sub")}
        except InvalidTokenError:
            return None

    async def login(self, request: Request) -> Response:
        # Usually a 405 or a doc page if tokens come from elsewhere
        from lilya.responses import HTMLResponse
        return HTMLResponse("Use your bearer token to access this dashboard.")

    async def logout(self, request: Request) -> Response:
        from lilya.responses import RedirectResponse
        return RedirectResponse("/login", status_code=303)

    def routes(self) -> list[Any]:
        return []
```

#### OIDC/SSO hand-off (delegated login)

```python
class OIDCHandOffBackend(AuthBackend):
    """
    Expect the parent reverse proxy (or an upstream app) to perform the OIDC flow and pass
    the result through headers. This backend simply trusts those headers.
    """
    def __init__(self, user_header: str = "X-User-Sub", name_header: str = "X-User-Name"):
        self.user_header = user_header
        self.name_header = name_header

    async def authenticate(self, request: Request):
        sub = request.headers.get(self.user_header)
        name = request.headers.get(self.name_header)
        return {"id": sub, "name": name or sub} if sub else None

    async def login(self, request: Request) -> Response:
        from lilya.responses import HTMLResponse
        return HTMLResponse("This dashboard is protected by your organization's SSO.")

    async def logout(self, request: Request) -> Response:
        from lilya.responses import RedirectResponse
        return RedirectResponse("/login", status_code=303)

    def routes(self) -> list[Any]:
        return []
```

Use any of the above backends with:

```python
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin

backend = ProxyHeaderBackend()          # or DBSessionBackend(), JWTBearerBackend(...), etc.
admin = AsyncMQAdmin(enable_login=True, backend=backend)
```

!!! Warning
    These are examples and any attempt of using them without doing proper changes it is not AsyncMQ responsability. Take
    these as "good ideas".

# Endpoints

All endpoints live under your configured dashboard_url_prefix (default /admin).

| Path                                  | Controller              | What it does                                            |
|--------------------------------------|-------------------------|---------------------------------------------------------|
| /admin/                              | DashboardController     | Overview (totals, recent metrics).                      |
| /admin/queues                        | QueueController         | List queues.                                            |
| /admin/queues/{name}                 | QueueDetailController   | Queue details + pause/resume.                           |
| /admin/queues/{name}/jobs            | QueueJobController      | Filtered job lists (waiting, delayed, failed…). Bulk and single-job actions. |
| /admin/queues/{name}/jobs/{job_id}/{action} | JobActionController     | Single job action endpoint: retry, delete, cancel.     |
| /admin/queues/{name}/repeatables     | RepeatablesController   | View and create repeatable jobs (cron, args/kwargs).   |
| /admin/queues/{name}/dlq             | DLQController           | Dead-letter queue: review, retry, delete.               |
| /admin/workers                      | WorkerController        | Active workers and heartbeats.                          |
| /admin/metrics                      | MetricsController       | Throughput, durations, retries, failures.              |
| /admin/events                      | SSEController           | Server-Sent Events for live updates.                    |

## Templates & Static Assets

- Templates: asyncmq/contrib/dashboard/templates/
- Static: asyncmq/contrib/dashboard/statics/ served under /<prefix>/static/

To override: place a template/static asset with the same path in your app's template search path/static mount.

The engine is Jinja2 (via asyncmq.contrib.dashboard.engine.templates) and includes helpers from DashboardMixin.

### Examples

#### Mount under a custom prefix (FastAPI)

```python
from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="secret")

admin = AsyncMQAdmin(enable_login=False, url_prefix="/queues")
app.mount("/", admin.get_asgi_app(with_url_prefix=True))
```

#### Tightening CORS / using your own sessions

```python
admin = AsyncMQAdmin(
    include_session=True,   # uses dashboard_config.session_middleware
    include_cors=False,     # turn off permissive defaults (add your own in parent app)
)
```

#### Customizing login path & allowlist

```python
admin = AsyncMQAdmin(
    enable_login=True,
    backend=SimpleUsernamePassword(),
    login_path="/signin",
    allowlist=("/signin", "/logout", "/static", "/assets"),
)
```

### Notes & Best Practices

- Sessions are required (even without auth) for flash messages and smooth UI flows.
- Security: keep the session payload minimal (prefer IDs over user blobs).
- Prefix awareness: redirects and static paths are computed under your mount prefix.
- HTMX: unauthenticated partial requests receive HX-Redirect to the login page.
- Overriding UI: customize templates/partials to match your brand—no fork required.

## API Reference

```python
 # wrapper you mount
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin
from asyncmq.contrib.dashboard.admin.middleware import AuthGateMiddleware
from asyncmq.contrib.dashboard.admin.protocols import AuthBackend
```

# Roadmap

- Pluggable authentication & authorization helpers out of the box.
- Richer filtering and search across jobs and queues.
- Widget/plugin extension points for custom dashboards.
- Deeper retries analytics, alerting, and trend views.
- Theming and accessibility improvements.

Have ideas or needs? Open an issue, we're iterating fast.

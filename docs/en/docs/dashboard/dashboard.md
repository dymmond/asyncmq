# Dashboard

AsyncMQ includes a dashboard ASGI application under `asyncmq.contrib.dashboard`.

Main wrapper: `AsyncMQAdmin`.

## What You Get

Dashboard pages include:

- overview
- queues and queue details (pause/resume)
- queue jobs (retry/remove/cancel)
- DLQ view (retry/remove)
- repeatables view and form actions
- workers list
- metrics page
- SSE endpoint for live updates (`/events`)

## Quick Start

```python
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin

admin = AsyncMQAdmin(enable_login=False)
```

### Lilya

```python
from lilya.apps import Lilya
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin

app = Lilya()
admin = AsyncMQAdmin(enable_login=False)
admin.include_in(app)
```

### FastAPI / Starlette

```python
from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from asyncmq.contrib.dashboard.admin import AsyncMQAdmin

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="change-me")

admin = AsyncMQAdmin(enable_login=False)
app.mount("/", admin.get_asgi_app(with_url_prefix=True))
```

## Configuring Appearance and Session Defaults

`settings.dashboard_config` returns `DashboardConfig`.

```python
from asyncmq.conf.global_settings import Settings
from asyncmq.core.utils.dashboard import DashboardConfig


class AppSettings(Settings):
    secret_key = "replace-in-production"

    @property
    def dashboard_config(self) -> DashboardConfig:
        return DashboardConfig(
            title="My AsyncMQ",
            header_title="Background Jobs",
            dashboard_url_prefix="/admin",
            sidebar_bg_colour="#CBDC38",
            secret_key=self.secret_key,
        )
```

## Authentication

`enable_login=True` requires an auth backend implementing `AuthBackend`.

Built-in options:

- `SimpleUsernamePasswordBackend`
- `JWTAuthBackend`

See [JWT Auth Backend](jwt.md) for token-based setup.

## Operational Notes

- By default, `AsyncMQAdmin` can include CORS and session middleware.
- Route prefix defaults to `settings.dashboard_config.dashboard_url_prefix`.
- Auth gate supports HTMX-friendly redirects via `HX-Redirect`.

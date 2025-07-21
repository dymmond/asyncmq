---
hide:
  - navigation
---

# AsyncMQ Admin Dashboard

The AsyncMQ Admin Dashboard is a sleek, ASGI powered interface that lets you peek under the hood of your background job system.
Whether you're cruising with FastAPI, Esmerald, or Starlette, simply mount the dashboard, plug in a session middleware,
and voilà, you've got real-time insights into queues, workers, jobs, and metrics without writing a single JavaScript line.

!!! Note
    This is the first version of the AsyncMQ Admin Dashboard documentation, created to gather user feedback and understand user needs.

You eill need to install the dashboard.

The dashboard also requires the `SessionMiddleware` to be used due to messages being sent.

If you use FastAPI or Esmerald you can simply use the one from Starlette or Lilya.

The reason for all of this was to make the dashboard as ASGI friendly as possible and since Lilya and Starlette use Pure ASGI middlewares, that means you can also use them anywhere else but you are also free to use your own.

**Importing from Lilya**

When you install the dashboard, it will also install [Lilya](https://lilya.dev) as its used internally for this purpose.

```python
from lilya.middleware.session import SessionMiddleware
```

## The installation

Getting started is a breeze. Install the AsyncMQ package (which bundles the dashboard) via pip:

```shell
pip install asyncmq[dashboard]
```

Once installed, the dashboard is ready to roll, no extra dependencies required beyond your preferred ASGI framework
and a session middleware provider.

No additional dependencies are required to enable the dashboard beyond the ASGI framework of your choice.

---

## Configuration

Before mounting, you can tweak the dashboard’s appearance and behavior by adjusting settings.dashboard_config.
Import the settings from asyncmq.conf and apply your own `DashboardConfig` values:

```python
from asyncmq.core.utils.dashboard import DashboardConfig

# Default values:
dashboard_config = DashboardConfig(
    title="MyApp Queue Monitor",
    header_title="MyApp Tasks",
    description="Keep an eye on your background workers with a smile.",
    favicon="/static/myfavicon.ico",
    dashboard_url_prefix="/admin",
    sidebar_bg_colour="#3498db",
)
```

To customize:

```python
from asyncmq.core.utils.dashboard import DashboardConfig
from asyncmq.conf import Settings

class MyCustomSettings(Settings):

    @property
    def dashboard_config(self) -> DashboardConfig:
        return DashboardConfig(
            title="My Queue Monitor",
            header_title="MyApp Queue Dashboard",
            description="Overview of background tasks in MyApp",
            favicon="/static/myfavicon.ico",
            dashboard_url_prefix="/dashboard",
            sidebar_bg_colour="#3498db",
        )
```

---

## Quick Start

Once installed, simply import the `dashboard` ASGI app:

```python
from asyncmq.contrib.dashboard.application import dashboard
```

You can now mount it into any ASGI server or framework.

---

## Embedding in Your ASGI App

Because the dashboard relies on HTTP cookies to track flash messages and user state, you must add a SessionMiddleware.
If you’re using FastAPI or Starlette, you can use starlette.middleware.sessions.SessionMiddleware, or thanks to the
dashboard’s ASGI-agnostic design—any Lilya compatible middleware will do:

### Esmerald

```python
from esmerald import Esmerald, Include
from esmerald.core.config.session import SessionConfig
from esmerald.conf import settings
from asyncmq.contrib.dashboard.application import dashboard

session_config = SessionConfig(secret_key=settings.secret_key)

app = Esmerald(
    routes=[
        Include(path="/", app=dashboard, name="dashboard-admin")
    ],
    session_config=session_config,
)
```

### FastAPI

```python
from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from asyncmq.contrib.dashboard.application import dashboard

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="your-secret")
app.mount("/fastapi", dashboard, name="asyncmq-admin")
```

### Explanation

This snippet mounts the dashboard under `/admin` and ensures session support for flash messages and stateful interactions.

The `DashboadConfiguration` has a property to set the `dashboard_url_prefix` where it defaults to `/admin`

Simply adjust the path prefix as needed in your configuration.

---

## Standalone Server

A convenience script is provided for standalone use:

```bash
uvicorn asyncmq.contrib.dashboard.serve:dash_app --host 0.0.0.0 --port 8000 --reload
```

Out of the box, this serves your dashboard at http://localhost:8000/ with CORS enabled, so you can point any
frontend at it without hassle.

---

## Endpoints & Controllers

The dashboard exposes the following routes (prefix as configured by `dashboard_url_prefix`, default `/admin`):

| Path                                          | Controller              | Description                                                                                 |
| --------------------------------------------- | ----------------------- | ------------------------------------------------------------------------------------------- |
| `/admin/`                                     | `DashboardController`   | Overview: total queues, jobs, workers.                                                      |
| `/admin/queues`                               | `QueueController`       | Lists all queues.                                                                           |
| `/admin/queues/{name}`                        | `QueueDetailController` | Queue details; pause/resume operations.                                                     |
| `/admin/queues/{name}/jobs`                   | `QueueJobController`    | List/manage jobs in a specific queue. Retry, delete, or cancel via POST.                    |
| `/admin/queues/{name}/jobs/{job_id}/{action}` | `JobActionController`   | AJAX endpoint for single-job actions (`retry`, `delete`, `cancel`).                         |
| `/admin/queues/{name}/repeatables`            | `RepeatablesController` | View repeatable job definitions for a queue.                                                |
| `/admin/queues/{name}/dlq`                    | `DLQController`         | Dead-letter queue view; retry/delete dead jobs.                                             |
| `/admin/workers`                              | `WorkerController`      | Lists active workers and heartbeats.                                                        |
| `/admin/metrics`                              | `MetricsController`     | Dashboard metrics: throughput, avg. duration, retries, failures.                            |
| `/admin/events`                               | `SSEController`         | Server-Sent Events: real-time push of overview, job distribution, metrics, queues, workers. |

---

## Templates & Static Assets

All Jinja2 templates are bundled under `asyncmq/contrib/dashboard/templates/`, and static files
(JavaScript, CSS, icons) live in `asyncmq/contrib/dashboard/statics/`.

To override any asset, simply place a template or file with the same name in your application’s search path.
This lets you rebrand, restyle, or extend the dashboard without touching the core code.

* **Templates** are located in `asyncmq/contrib/dashboard/templates/`.
  Override any template by providing your own in your Jinja2 search path.
* **Static** files (CSS, JS) reside in `asyncmq/contrib/dashboard/statics/` and are served under `/admin/static/`.

---

### Template Engine

Built on Jinja2 via `asyncmq.contrib.dashboard.engine.templates`. Templates can use global `getattr` helper and receive
the context provided by `DashboardMixin`.

---

## Examples

### Basic FastAPI Integration

```python
from fastapi import FastAPI
from asyncmq.contrib.dashboard.application import dashboard

app = FastAPI()

# Mount under "/fastapi/admin"
app.mount("/fastapi", dashboard, name="asyncmq-admin")
```

### Customizing Appearance

```python
from asyncmq.core.utils.dashboard import DashboardConfig
from asyncmq.conf import Settings

class MyCustomSettings(Settings):

    @property
    def dashboard_config(self) -> DashboardConfig:
        DashboardConfig(
            title="Acme Queues",
            header_title="Acme Background Tasks",
            sidebar_bg_colour="#FF5722",
            dashboard_url_prefix="/tasks",
        )

```

Remember that you need to export the `ASYNCMQ_SETTINGS_MODULE` pointing to your [settings](../features/settings.md).

---

## Roadmap

* Add pluggable **authentication & authorization** system.
* Extend **filtering & search** on jobs and queues.
* Support **custom dashboards** and widget plugins.
* Enhanced **retries analytics** and **alerting**.
* **Theming** support and accessibility improvements.

More features and improvements to come in the next releases, including adding an authentication system.

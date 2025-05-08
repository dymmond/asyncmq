---
hide:
  - navigation
---

# AsyncMQ Admin Dashboard

The AsyncMQ Admin Dashboard provides a browser-based interface to monitor and manage your queues, jobs, workers, metrics, and dead-letter queues in real time. It is fully ASGI-friendly and can be mounted in any ASGI framework (e.g., FastAPI, Esmerald, Starlette).

!!! Note
    This is the first version of the AsyncMQ Admin Dashboard documentation, created to gather user feedback and understand user needs.

You eill need to install the dashboard.

```shell
pip install asyncmq[dashboard]
```

---

## Table of Contents

1. [Installation](#installation)
2. [Configuration](#configuration)
3. [Quick Start](#quick-start)
4. [Embedding in Your ASGI App](#embedding-in-your-asgi-app)
5. [Standalone Server](#standalone-server)
6. [Endpoints & Controllers](#endpoints--controllers)
7. [Templates & Static Assets](#templates--static-assets)
8. [API Reference](#api-reference)
9. [Examples](#examples)
10. [Roadmap](#roadmap)

---

## Installation

Install the core AsyncMQ package (which includes the Dashboard contrib) via pip:

```bash
pip install asyncmq
```

No additional dependencies are required to enable the dashboard beyond the ASGI framework of your choice.

---

## Configuration

AsyncMQ provides a `DashboardConfig` to adjust appearance and routing. By default:

```python
from asyncmq.core.utils.dashboard import DashboardConfig

# Default values:
DashboardConfig(
    title="Dashboard",
    header_title="AsyncMQ",
    description="A simple dashboard for monitoring AsyncMQ jobs.",
    favicon="https://raw.githubusercontent.com/dymmond/asyncmq/refs/heads/main/docs/statics/favicon.ico",
    dashboard_url_prefix="/admin",
    sidebar_bg_colour="#CBDC38",
)
```

To customize:

```python
from dataclasses import dataclass
from asyncmq.core.utils.dashboard import DashboardConfig
from asyncmq.conf import Settings

@dataclass
class MyCustomSettings(Settings):

    @property
    def dashboard_config(self) -> DashboardConfig:
        DashboardConfig(
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

### Esmerald

```python
from esmerald import Esmerald, Include
from asyncmq.contrib.dashboard.application import dashboard

app = Esmerald(
    routes=[
        Include(path="/esmerald", app=dashboard, name="dashboard-admin")
    ]
)
```

### FastAPI

```python
from fastapi import FastAPI
from asyncmq.contrib.dashboard.application import dashboard

app = FastAPI()
app.mount("/fastapi", dashboard, name="asyncmq-admin")
```

The `DashboadConfiguration` has a property to set the `dashboard_url_prefix` where it defaults to `/admin`

Simply adjust the path prefix as needed in your configuration.

---

## Standalone Server

A convenience script is provided for standalone use:

```bash
uvicorn asyncmq.contrib.dashboard.serve:dash_app --host 0.0.0.0 --port 8000 --reload
```

This runs the dashboard at `http://localhost:8000/` with CORS enabled for unrestricted access.

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

* **Templates** are located in `asyncmq/contrib/dashboard/templates/`.
  Override any template by providing your own in your Jinja2 search path.
* **Static** files (CSS, JS) reside in `asyncmq/contrib/dashboard/statics/` and are served under `/admin/static/`.

---

### Template Engine

Built on Jinja2 via `asyncmq.contrib.dashboard.engine.templates`. Templates can use global `getattr` helper and receive the context provided by `DashboardMixin`.

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
from dataclasses import dataclass
from asyncmq.core.utils.dashboard import DashboardConfig
from asyncmq.conf import Settings

@dataclass
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

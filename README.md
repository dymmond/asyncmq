# AsyncMQ

![PyPI](https://img.shields.io/pypi/v/asyncmq?label=PyPI) ![License](https://img.shields.io/pypi/l/asyncmq?label=License) ![Python Versions](https://img.shields.io/pypi/pyversions/asyncmq?label=Python%203.8%2B) ![Docs](https://img.shields.io/badge/docs-asyncmq.dymmond.com-blue)

> **AsyncMQ** is a native Python, **asyncio**-first task queue inspired by BullMQ.  
> It offers multi-backend support, robust scheduling, retries, and more—all with an idiomatic async/await API.

---

## 🔍 Overview

AsyncMQ provides:

- **Immediate & Delayed Jobs**: `add(..., delay=<seconds>)` schedules now or later  
- **Repeatable Tasks**: `add_repeatable(..., every=<sec> | cron="* * * * *")`  
- **Retries & Backoff**: `retries=<int>`, `backoff=<float>` (exponential base)  
- **TTL & DLQ**: `ttl=<seconds>`, expired or failed jobs can be moved to DLQ  
- **Priorities & Rate Limiting**: `priority=<int>`, `rate_limit=<jobs>`, `rate_interval=<sec>`  
- **Stalled-Job Recovery**: automatic scan & requeue of stuck jobs  
- **Sandboxed Execution**: handlers run in isolated subprocesses  
- **Flows & Dependencies**: `FlowProducer.add_flow(queue, jobs: list[Job])` for DAGs  
- **Multi-Backend**: `RedisBackend`, `PostgresBackend`, `MongoBackend`, `InMemoryBackend`  
- **Rich CLI**: `asyncmq queue|job|worker|info` commands  

---

## ⚙️ Installation

Requires Python **3.8+**.

```bash
pip install asyncmq
```

Or install latest from GitHub:

```shell
pip install git+https://github.com/dymmond/asyncmq.git
```

---

🚀 Quickstart

1️⃣ Register Tasks (tasks.py)

```python
from asyncmq.tasks import task

@task(queue="emails", retries=2, ttl=3600)
async def send_email(recipient: str, subject: str, body: str) -> None:
    """
    Send an email; exceptions trigger retries.
    """
    await smtp.send(recipient, subject, body)
```

2️⃣ Create a Queue (app.py)

```python
import anyio
from asyncmq.queues import Queue
from asyncmq.backends.redis import RedisBackend

async def main():
    queue = Queue(
        name="emails",
        backend=RedisBackend(redis_url="redis://localhost:6379"),
        concurrency=5,
        rate_limit=10,
        rate_interval=1.0,
    )
    # Enqueue or run worker...

anyio.run(main)
```

> Note: Omitting backend defaults to RedisBackend(redis_url="redis://localhost").



3️⃣ Enqueue Jobs (enqueue.py)

```python
import anyio
from asyncmq.queues import Queue

async def main():
    q = Queue("emails")  # default Redis

    # Immediate job
    job_id = await q.add(
        task_id="tasks.send_email",
        args=["user@example.com", "Hi", "Hello!"],
        retries=1,
        ttl=600,
        backoff=2.0,
        priority=3,
    )

    # Delayed job (delay in seconds)
    delay_id = await q.add(
        task_id="tasks.send_email",
        args=["user@example.com", "Reminder", "Don't forget!"],
        delay=30.0,
    )

    print(job_id, delay_id)

anyio.run(main)
```

4️⃣ Add Repeatable Tasks

```python
from asyncmq.queues import Queue

q = Queue("jobs")
# Every 5 minutes
q.add_repeatable(
    task_id="tasks.cleanup",
    every=300.0,
    args=["/tmp"],
    retries=0,
)

# Cron at midnight
q.add_repeatable(
    task_id="tasks.daily_backup",
    cron="0 0 * * *",
)
```

5️⃣ Enqueue Bulk Jobs

```python
jobs = [
    {"task_id": "tasks.send_email", "args": ["a@example.com", "A", "..."]},
    {"task_id": "tasks.send_email", "args": ["b@example.com", "B", "..."]},
]
available_ids = await q.add_bulk(jobs)
```

6️⃣ Start Workers

```shell
asyncmq worker start emails --concurrency 5
```

Or in code:

```python
# app.py
q.start()  # synchronous; blocks
```

---

📋 API Reference

```python
class Queue:
    def __init__(
        self,
        name: str,
        backend: BaseBackend | None = None,
        concurrency: int = 3,
        rate_limit: int | None = None,
        rate_interval: float = 1.0,
    )
    async def add(
        self,
        task_id: str,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        retries: int = 0,
        ttl: int | None = None,
        backoff: float | None = None,
        priority: int = 5,
        delay: float | None = None,
    ) -> str
    async def add_bulk(self, jobs: list[dict[str, Any]]) -> list[str]
    def add_repeatable(
        self,
        task_id: str,
        every: float | str | None = None,
        cron: str | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        retries: int = 0,
        ttl: int | None = None,
        priority: int = 5,
    ) -> None
    async def pause(self) -> None
    async def resume(self) -> None
    async def clean(self, state: str, older_than: float | None = None) -> None
    async def queue_stats(self) -> dict[str, int]
    async def list_jobs(self, state: str) -> list[dict[str, Any]]
    async def run(self) -> None
    def start(self) -> None
```

---

🖥 CLI Reference

### Queue Commands

```shell
asyncmq queue list
asyncmq queue info <queue>
asyncmq queue pause <queue>
asyncmq queue resume <queue>
asyncmq queue list-delayed <queue>
asyncmq queue remove-delayed <queue> <job_id>
asyncmq queue list-repeatables <queue>
asyncmq queue pause-repeatable <queue> '<job_def_json>'
asyncmq queue resume-repeatable <queue> '<job_def_json>'
```

### Job Commands

```shell
asyncmq job list --queue <queue> --state <state>
asyncmq job inspect <job_id> --queue <queue>
asyncmq job retry <job_id> --queue <queue>
asyncmq job remove <job_id> --queue <queue>
asyncmq job cancel-job <queue> <job_id>

Worker Commands

asyncmq worker start <queue> --concurrency <number>
```

Info Commands

```python
asyncmq info version
asyncmq info backend
```

---

🔧 Flows & Dependencies

```python
from asyncmq.flow import FlowProducer
from asyncmq.jobs import Job
import anyio

async def example():
    flow = FlowProducer()
    a = Job(task_id="tasks.A", args=[1])
    b = Job(task_id="tasks.B", args=[2], depends_on=[a.id])
    c = Job(task_id="tasks.C", args=[3], depends_on=[b.id])
    ids = await flow.add_flow("pipeline", [a, b, c])
    print(ids)

anyio.run(example)
```

---

🛠 Configuration

Override defaults in code:

```python
from asyncmq.conf import settings
from asyncmq.backends.postgres import PostgresBackend

settings.backend = PostgresBackend(dsn="postgres://...")
```

Or via environment:

```shell
export ASYNCMQ_SETTINGS_MODULE=path.to.your_settings
```
---

🧪 Testing

```shell
pytest
```

---

🤝 Contributing

Please see CONTRIBUTING.md. We welcome issues and PRs.


---

📜 License

Released under the BSD 3-Clause License. See LICENSE.


---

Made with 🐍 & 🚀 by the AsyncMQ community

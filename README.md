AsyncMQ

   

> AsyncMQ is a native Python, asyncio-first task queue inspired by BullMQ. It offers multi-backend support, robust scheduling, retries, and more—all with an idiomatic async/await API.




---

🔍 Overview

AsyncMQ provides:

Immediate & Delayed Jobs: add(..., delay=<seconds>) schedules jobs now or later.

Repeatable Tasks: add_repeatable(..., every=<sec> | cron="* * * * *").

Retries & Backoff: retries=<int>, backoff=<float> (exponential base).

TTL & DLQ: ttl=<seconds>, jobs expired or failed can be cleaned or moved to DLQ.

Priorities & Rate Limiting: priority=<int>, rate_limit=<jobs>, rate_interval=<sec>.

Stalled-Job Recovery: Automatic detection and requeue of stuck jobs.

Sandboxed Execution: Handlers run in isolated subprocesses.

Flows & Dependencies: FlowProducer.add_flow(queue, jobs: list[Job]) for DAGs.

Multi-Backend: RedisBackend, PostgresBackend, MongoBackend, or InMemoryBackend.

Rich CLI: asyncmq queue|job|worker|info commands.

30+ Tests: Ensures reliability across features and backends.



---

⚙️ Installation

Requires Python 3.8+.

pip install asyncmq

Or install latest from GitHub:

pip install git+https://github.com/yourorg/asyncmq.git


---

🚀 Quickstart

1. Register Tasks

# tasks.py
from asyncmq.tasks import task

@task(queue="emails", retries=2, ttl=3600)
async def send_email(recipient: str, subject: str, body: str) -> None:
    """
    Send an email; exceptions trigger retries.
    """
    await smtp.send(recipient, subject, body)

2. Create a Queue

# app.py
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

> Note: If backend is omitted, RedisBackend(redis_url="redis://localhost") is used by default.



3. Enqueue Jobs

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

4. Add Repeatable Tasks

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

These definitions are scheduled by the worker when run() or start() is invoked.

5. Enqueue Bulk Jobs

jobs = [
    {"task_id": "tasks.send_email", "args": ["a@example.com", "A", "..."]},
    {"task_id": "tasks.send_email", "args": ["b@example.com", "B", "..."]},
]
available_ids = await q.add_bulk(jobs)

6. Start Workers

# Blocking: runs the worker until interrupted
asyncmq worker start emails --concurrency 5

Or in code:

# app.py
q.start()  # synchronous; blocks


---

📋 Core API

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


---

🔧 Flows & Dependencies

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


---

🖥 CLI Reference

Queue Commands

asyncmq queue list
asyncmq queue info <queue>
asyncmq queue pause <queue>
asyncmq queue resume <queue>
asyncmq queue list-delayed <queue>
asyncmq queue remove-delayed <queue> <job_id>
asyncmq queue list-repeatables <queue>
asyncmq queue pause-repeatable <queue> '<job_def_json>'
asyncmq queue resume-repeatable <queue> '<job_def_json>'

Job Commands

asyncmq job list --queue <queue> --state <state>
asyncmq job inspect <job_id> --queue <queue>
asyncmq job retry <job_id> --queue <queue>
asyncmq job remove <job_id> --queue <queue>
asyncmq job cancel-job <queue> <job_id>

Worker Commands

asyncmq worker start <queue> --concurrency <number>

Info Commands

asyncmq info version
asyncmq info backend


---

🛠 Configuration

Default backend: settings.backend or environment via ASYNCMQ_SETTINGS_MODULE.

Redis: RedisBackend(redis_url="...")

Postgres: PostgresBackend(dsn="...")

MongoDB: MongoBackend(uri="...")

In-memory: InMemoryBackend()


Override globally in code:

from asyncmq.conf import settings
from asyncmq.backends.postgres import PostgresBackend
settings.backend = PostgresBackend(dsn="postgres://...")


---

🧪 Testing

pytest

Includes unit and integration tests across backends and features.


---

🤝 Contributing

Please see CONTRIBUTING.md. We welcome issues and PRs.


---

📜 License

Released under the BSD 3-Clause License. See LICENSE.


---

[Made with 🐍 & 🚀 by the AsyncMQ community]

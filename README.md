AsyncMQ

   

> Async task queue with BullMQ-like features, but 100% Pythonic and asyncio-powered 🚀



If you've ever wondered what BullMQ would feel like in Python—complete with async/await syntax, multiple backends, and a sprinkle of human-friendly CLI—well, wonder no more. AsyncMQ is your new best friend for orchestrating distributed jobs, cron tasks, retries, and more, all without leaving the comfy confines of Python.

🎯 Features

* Fully Async: Built on asyncio/anyio, speed and concurrency at your fingertips.

* Multi-Backend Support: Redis, PostgreSQL, MongoDB, or pure in-memory for local dev & testing.

* Delays & Repeatables: Schedule one-off delays or cron-style repeatable jobs with ease.

* Retries & Backoff: Configurable max retries and custom backoff strategies.

* TTL & DLQ: Job expiration and dead-letter handling, so nothing ever silently vanishes.

* Priorities & Rate Limiting: Push urgent jobs ahead and throttle workers when you need it.

* Stalled Recovery: Automatic detection & rescue of stuck jobs.

* Flows & Dependencies: Define DAGs of jobs that run atomically when dependencies are met.

* Sandboxed Processors: Run your task code in isolated subprocesses for extra safety.

* Rich CLI: asyncmq queue, asyncmq worker, asyncmq job, asyncmq info commands.

🚀 Quickstart

Installatio

```shell
pip install asyncmq
```

Define a Task

# tasks.py

```python
from asyncmq.tasks import task

@task(name="say-hello", max_retries=3, backoff=lambda n: 2**n)
async def say_hello(name: str) -> None:
    print(f"👋 Hello, {name}!")
```

Enqueue a Job

```python
from asyncmq.queues import Queue
import asyncio

async def main():
    q = Queue("greetings", backend_url="redis://localhost:6379")
    await q.add("say-hello", {"name": "World"}, priority=10)
    await q.close()

asyncio.run(main())
```

Run a Worker

```python
asyncmq worker greetings --concurrency 5
```

That’s it! Your worker will pick up say-hello jobs and run them safely in a subprocess sandbox.

🛠 CLI Usage

# List all queues

```python
asyncmq queue list
```

# Inspect jobs in a queue

```python
asyncmq job list --queue greetings
```

# Pause and resume

```python
asyncmq queue pause greetings
asyncmq queue resume greetings

# View queue stats
asyncmq info stats --queue greetings
```

📦 Backend Configuration

AsyncMQ supports multiple backends. Configure via URL or parameters:

# Redis (default)

```pythoj
q = Queue("q1", backend_url="redis://localhost:6379")

# PostgreSQL
q = Queue("q2", backend_url="postgres://user:pass@localhost:5432/db")

# MongoDB
tasks = Queue("q3", backend_url="mongodb://localhost:27017/db")

# In-memory (for tests)
q = Queue("test", backend_url="memory://")
```

📖 Documentation & Roadmap

Head over to the full docs for:

Detailed API reference

Backend-specific caveats & tuning

Guides on metrics, monitoring, and deployment


Roadmap highlights:

Cross-process event bus using Redis Pub/Sub

Built-in metrics & Prometheus exporter

Official dashboard (coming soon!)

Extra backoff presets (exponential, jitter, etc.)


🤝 Contributing

Contributions, issues, and feature requests are welcome! Feel free to check our Changelog and drop a PR.

1. Fork it


2. Create your feature branch (git checkout -b feature/fooBar)


3. Commit your changes (git commit -am 'Add some fooBar')


4. Push to the branch (git push origin feature/fooBar)


5. Create a new Pull Request



📜 License

AsyncMQ is released under the BSD 3-Clause License. See LICENSE for details.


---

Made with ❤️ in Python by Tiago Silva and the AsyncMQ community.

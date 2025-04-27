# AsyncMQ

[![Build Status](https://img.shields.io/github/actions/workflow/status/your-org/asyncmq/ci.yml?branch=main)](https://github.com/tarsil/asyncmq/actions)
[![PyPI Version](https://img.shields.io/pypi/v/asyncmq.svg)](https://pypi.org/project/asyncmq/)
[![License](https://img.shields.io/github/license/tarsil/asyncmq)](https://github.com/tarsil/asyncmq/blob/main/LICENSE)
[![Downloads](https://static.pepy.tech/badge/asyncmq)](https://pepy.tech/project/asyncmq)

**Supercharge your async applications with a modern, blazing-fast task queue for Python.**  
_Tasks so fast, you’ll think you’re bending time itself._

---

## What is AsyncMQ?

AsyncMQ is a **modern**, **async-native**, **highly extensible** task queue built for **Python 3.11+**.  
Inspired by giants like **BullMQ**, **Celery**, and **RQ** — but designed for the speed demons of the async world.

Whether you need **scheduled jobs**, **retries**, **dead letter queues**, **persistence**, or **Pub/Sub events**, AsyncMQ has your back — all without blocking your event loop.

> Build faster. Scale easier. Sleep better.

---

## Features

- ⚡ **Blazing Fast** — Built from the ground up with `asyncio`.
- ⛓ **Redis & Memory Backends** — Choose between lightweight or battle-tested.
- ♻️ **Automatic Retries & TTLs** — No more babysitting jobs.
- ☠️ **Dead Letter Queue** — Handle failures like a pro.
- ⏰ **Delayed Jobs** — Schedule work for the future (because who likes deadlines?).
- 📡 **Pub/Sub Job Events** — Real-time feedback on job progress.
- 🛠 **Pluggable Persistence** — Postgres-backed job storage included!
- 🧵 **Seamless Integration** — Works natively with **Esmerald**, **FastAPI**, and any async app.
- ✨ **CLI Goodness** — List, retry, inspect jobs directly from your terminal.
- ❤️ **Designed to be loved** — Beautifully documented. Dead simple to use.

---

## Installation

```bash
pip install asyncmq
```

For Postgres persistence support:

```bash
pip install asyncmq[postgres]

```
For Redis backend:

It's built-in.

---

Quick Start

from asyncmq import Queue, Worker, Job

# Create a queue
queue = Queue(name="emails")

# Define a job processor
async def send_email(job: Job):
    print(f"Sending email to {job.data['to']}")

# Register the worker
worker = Worker(queue)
worker.register_processor(send_email)

# Add a job
await queue.add({"to": "user@example.com"})

# Start the worker
await worker.start()

Boom. Emails flying faster than you can say "async def".


---

Real-World Example

Want to schedule a notification to be sent 1 hour later with retries and error handling?

await queue.add(
    data={"user_id": 42, "message": "Don't forget to hydrate!"},
    delay=3600,    # Delay by 1 hour
    attempts=5,    # Retry up to 5 times
    backoff=30,    # 30s backoff between retries
    ttl=7200       # TTL of 2 hours
)

> AsyncMQ makes it effortless to schedule, retry, and recover from failures.

---

Documentation

The full documentation lives at /docs/.
There you’ll find:

Beginner tutorials

Advanced guides

Real-world usage examples

Integration with Esmerald and FastAPI

How to extend AsyncMQ

And much more!

---

Integrations

AsyncMQ + Esmerald

Want to trigger background jobs from your Esmerald endpoints? It’s native.
See full Esmerald integration guide.

AsyncMQ + FastAPI

Need ultra-fast background tasks for your FastAPI apps? We got you.
See full FastAPI integration guide.

---

Contributing

Contributions are more than welcome!
AsyncMQ is community-driven — feel free to submit PRs, suggest features, fix typos, or just tell us you love us.

* Fork it
* Create your feature branch (git checkout -b feature/amazing-feature)
* Commit your changes (git commit -m 'Add amazing feature')
* Push to the branch (git push origin feature/amazing-feature)
* Open a Pull Request

---

License

Licensed under the BSD-3 License

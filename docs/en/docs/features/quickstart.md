# Quickstart

This quickstart covers the shortest end-to-end path:

- define a task
- enqueue a job
- run a worker
- inspect jobs from CLI

## 1. Configure AsyncMQ

```python
# myapp/settings.py
from asyncmq.backends.memory import InMemoryBackend
from asyncmq.conf.global_settings import Settings


class AppSettings(Settings):
    backend = InMemoryBackend()
    worker_concurrency = 1
```

```bash
export ASYNCMQ_SETTINGS_MODULE=myapp.settings.AppSettings
```

## 2. Define a Task

```python
# myapp/tasks.py
from asyncmq.tasks import task


@task(queue="default", retries=2, ttl=120)
async def say_hello(name: str) -> str:
    return f"hello {name}"
```

## 3. Enqueue Work

```python
# producer.py
import anyio
from asyncmq.queues import Queue
from myapp.tasks import say_hello


async def main() -> None:
    queue = Queue("default")

    # Immediate enqueue returns the job id.
    job_id = await say_hello.enqueue("world", backend=queue.backend)
    print("job_id:", job_id)

    # Delayed enqueue returns None in current API.
    await say_hello.enqueue("later", backend=queue.backend, delay=5)


anyio.run(main)
```

## 4. Start a Worker

```bash
asyncmq worker start default --concurrency 1
```

## 5. Inspect from CLI

```bash
asyncmq queue list
asyncmq queue info default
asyncmq job list --queue default --state waiting
asyncmq job list --queue default --state failed
```

Next: [Core Concepts](core-concepts.md).

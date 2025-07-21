# RabbitMQ

This page documents the new **RabbitMQ** backend for AsyncMQ, enabling AMQP-based job transport while delegating all metadata/state operations (delayed jobs, repeatables, dependencies, stats, workers, etc.) to a pluggable job store.

!!! Warning
    This backend is currently in **beta**. It is functional but may have some rough edges. Please report any issues you encounter.

## Overview

The `RabbitMQBackend`:

- Uses [aio-pika](https://github.com/mosquito/aio-pika) under the hood for AMQP.
- Publishes and consumes messages via the **default exchange**, routing by queue name.
- Persists all job metadata in a **job store** (e.g. Redis, Postgres, MongoDB) that implements `BaseJobStore`.
- Supports:
    - Immediate enqueue/dequeue
    - Dead-letter queues (DLQs)
    - Delayed jobs
    - Repeatable jobs
    - Dependency flows (`atomic_add_flow`)
    - Pause/resume
    - Job state/result tracking
    - Bulk enqueue
    - Worker registry and heartbeats
    - Stats and queue purging
    - Distributed locks

## Requirements

- `aio-pika>=8.0.0`
- A running RabbitMQ broker (AMQP 0-9-1)
- A stateful job store, e.g. Redis, Postgres, MongoDB

## Installation

```bash
pip install asyncmq[aio-pika]

# or include aio-pika manually:
pip install asyncmq aio-pika
```

## Configuration

1. **Choose a job store**. For Redis:

```python
from asyncmq.stores.redis_store import RedisJobStore

store = RedisJobStore(redis_url="redis://localhost:6379")
```

2. **Instantiate the backend**:

```python
from asyncmq.backends.rabbitmq import RabbitMQBackend

backend = RabbitMQBackend(
   rabbit_url="amqp://guest:guest@localhost:5672/",
   job_store=store,
   prefetch_count=5,  # number of unacked messages per worker
)
```

3. **Add the backend to your [custom settings](../settings.md)**:

```python
from dataclasses import dataclass
from asyncmq.conf.global_settings import Settings
from asyncmq.backends.base import BaseBackend
from asyncmq.backends.rabbitmq import RabbitMQBackend
from asyncmq.stores.redis_store import RedisJobStore

store = RedisJobStore(redis_url="redis://localhost:6379")

class MySettings(Settings):
   backend: BaseBackend = RabbitMQBackend(
        rabbit_url="amqp://guest:guest@localhost:5672/",
        job_store=store,
        prefetch_count=5,  # number of unacked messages per worker
   )
```

4. **(Optional) anyio compatibility**
   AsyncMQ uses anyio under the hood, no extra changes needed in your application.

## Basic Usage

### Enqueue & Dequeue

```python
# Enqueue a job
job = {"id": "job123", "task": "send_email", "to": "user@example.com"}
await backend.enqueue("email", job)
# => returns "job123"

# Dequeue a job (or None if empty)
item = await backend.dequeue("email")

if item:
    payload = item["payload"]
    job_id  = item["job_id"]
    # process...
    await backend.ack("email", job_id)
```

### Dead-Letter Queue

```python
# Move a failed job to DLQ
await backend.move_to_dlq("email", {"id": "job123", "task": "..."} )
# DLQ queue is named "email.dlq"

# Consume from DLQ
dlq_item = await backend.dequeue("email.dlq")
```

### Delayed Jobs

```python
import time

run_at = time.time() + 60  # schedule 1 minute from now
await backend.enqueue_delayed("email", job, run_at=run_at)

# Later, fetch due jobs
due = await backend.get_due_delayed("email")
for di in due:
    await backend.enqueue("email", di.payload)
```

### Repeatable Jobs

```python
# Schedule a job every 5 seconds:
rid = await backend.enqueue_repeatable("heartbeat", job, interval=5)

# Pause/resume
await backend.pause_repeatable("heartbeat", {"id": rid})
await backend.resume_repeatable("heartbeat", {"id": rid})
await backend.remove_repeatable("heartbeat", rid)
```

### Dependency Flows

```python
from asyncmq.flow import FlowProducer

fp = FlowProducer(backend=backend)
jobs = [
    {"id": "A", "task": "stepA"},
    {"id": "B", "task": "stepB", "depends_on": ["A"]},
]
# Atomically enqueue and register dependencies
ids = await fp.add_flow("pipeline", jobs)
# => ["A", "B"]

# B will only be enqueued after A completes and you call:
await backend.resolve_dependency("pipeline", "A")
```

## Advanced Features

* **Pause/Resume Queues**

  ```python
  await backend.pause_queue("email")
  assert await backend.is_queue_paused("email")
  await backend.resume_queue("email")
  ```
* **Job State & Result**

  ```python
  await backend.update_job_state("email", "job123", "completed")
  state = await backend.get_job_state("email", "job123")
  await backend.save_job_result("email", "job123", {"ok": True})
  result = await backend.get_job_result("email", "job123")
  ```
* **Bulk Enqueue**

  ```python
  jobs = [{"id": "j1", ...}, {"id": "j2", ...}]
  await backend.bulk_enqueue("email", jobs)
  ```
* **Worker Registry & Heartbeats**

  ```python
  await backend.register_worker("w1", "email", concurrency=3, timestamp=time.time())
  workers = await backend.list_workers()
  await backend.deregister_worker("w1")
  ```
* **Queue Stats & Purge**

  ```python
  stats = await backend.queue_stats("email")
  await backend.drain_queue("email")
  await backend.purge("email", state="failed", older_than=1620000000.0)
  ```
* **Distributed Locking**

  ```python
  lock = await backend.create_lock("my-lock", ttl=30)
  async with lock:
      # critical section
  ```

## Notes

This describes the RabbitMQ backend for AsyncMQ, which is designed to work seamlessly with any job store that implements the `BaseJobStore` interface.

This also means that you can also pass the `backend` inside your [settings](../settings.md) and make it global for your application.

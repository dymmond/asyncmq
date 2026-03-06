# Test AsyncMQ Code

## Unit Tests (Fast)

Use `InMemoryBackend` for deterministic tests without infrastructure dependencies.

```python
import pytest
from asyncmq.backends.memory import InMemoryBackend
from asyncmq.queues import Queue


@pytest.mark.anyio
async def test_enqueue_and_list_waiting():
    backend = InMemoryBackend()
    queue = Queue("test", backend=backend)

    job_id = await queue.add("myapp.tasks.echo", args=["x"])
    waiting = await queue.list_jobs("waiting")

    assert any(job["id"] == job_id for job in waiting)
```

## Integration Tests

- Redis: run with local/container Redis
- Postgres: run only when DB is available
- RabbitMQ: run with broker + metadata store

## Recommended Coverage Areas

- state transitions (`waiting -> active -> completed/failed/delayed`)
- retries/backoff and DLQ behavior
- cancellation and remove/retry APIs
- delayed scheduling semantics
- backend parity for common operations

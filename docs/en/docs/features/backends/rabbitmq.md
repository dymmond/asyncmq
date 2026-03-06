# RabbitMQ Backend

`RabbitMQBackend` uses RabbitMQ for message delivery and a job store for metadata/state.

## Install

```bash
pip install "asyncmq[aio-pika]"
```

If you use default `RabbitMQJobStore`, Redis is also required for job metadata persistence.

## Configure

```python
from asyncmq.backends.rabbitmq import RabbitMQBackend
from asyncmq.conf.global_settings import Settings


class AppSettings(Settings):
    backend = RabbitMQBackend(
        rabbit_url="amqp://guest:guest@localhost/",
        redis_url="redis://localhost:6379/0",
        prefetch_count=10,
    )
```

## Constructor

```python
RabbitMQBackend(
    rabbit_url: str,
    job_store: BaseJobStore | None = None,
    redis_url: str | None = None,
    prefetch_count: int = 1,
)
```

- Provide `job_store` if you want custom metadata persistence.
- If omitted, `RabbitMQJobStore(redis_url=...)` is used.

## Runtime Behavior

- `dequeue()` fetches one AMQP message and tracks it in-flight.
- `ack(queue, job_id)` performs the actual RabbitMQ message acknowledgement.
- DLQ publish uses `<queue>.dlq`.
- worker and queue metadata are maintained in the configured job store.

## Notes

- RabbitMQ delivery and metadata persistence are separate concerns in this backend design.
- Repeatable definitions are persisted via backend APIs, but periodic scheduling still depends on worker-side scheduler wiring.

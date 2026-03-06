# Switch Backends

This guide shows the minimum settings changes needed to switch runtime backends.

## InMemory -> Redis

```python
from asyncmq.backends.redis import RedisBackend
from asyncmq.conf.global_settings import Settings


class AppSettings(Settings):
    backend = RedisBackend("redis://localhost:6379/0")
```

## InMemory -> Postgres

```python
from asyncmq.backends.postgres import PostgresBackend
from asyncmq.conf.global_settings import Settings


class AppSettings(Settings):
    asyncmq_postgres_backend_url = "postgresql://postgres:postgres@localhost:5432/asyncmq"
    backend = PostgresBackend()
```

Then bootstrap Postgres tables:

```python
import anyio
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend

anyio.run(install_or_drop_postgres_backend)
```

## InMemory -> MongoDB

```python
from asyncmq.backends.mongodb import MongoDBBackend
from asyncmq.conf.global_settings import Settings


class AppSettings(Settings):
    asyncmq_mongodb_backend_url = "mongodb://localhost:27017"
    asyncmq_mongodb_database_name = "asyncmq"
    backend = MongoDBBackend()
```

## InMemory -> RabbitMQ

```python
from asyncmq.backends.rabbitmq import RabbitMQBackend
from asyncmq.conf.global_settings import Settings


class AppSettings(Settings):
    backend = RabbitMQBackend(
        rabbit_url="amqp://guest:guest@localhost/",
        redis_url="redis://localhost:6379/0",  # metadata store
    )
```

## Validation Checklist

After any backend switch:

1. `asyncmq info backend`
2. enqueue one task
3. run worker and verify completion
4. verify `asyncmq queue list` and `asyncmq job list ...`

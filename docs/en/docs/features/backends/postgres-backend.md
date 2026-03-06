# Postgres Backend

`PostgresBackend` stores queue/job state in PostgreSQL via `asyncpg`.

## Install

```bash
pip install "asyncmq[postgres]"
```

## Configure

```python
from asyncmq.backends.postgres import PostgresBackend
from asyncmq.conf.global_settings import Settings


class AppSettings(Settings):
    asyncmq_postgres_backend_url = "postgresql://postgres:postgres@localhost:5432/asyncmq"
    backend = PostgresBackend()
```

You can also pass DSN directly:

```python
backend = PostgresBackend(dsn="postgresql://...")
```

## Schema Bootstrap

Postgres tables are not auto-migrated by the backend constructor.
Use the helper utility:

```python
import anyio
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend


anyio.run(install_or_drop_postgres_backend)
```

This creates:

- jobs table
- repeatables table
- cancelled jobs table
- worker heartbeat table

Table names are configurable from settings.

## Operational Notes

- Dequeue uses SQL locking (`FOR UPDATE SKIP LOCKED`) semantics.
- Queue pause state is stored in backend runtime and persisted per queue state management.
- Worker listing is heartbeat-based and filtered by `heartbeat_ttl`.

## When to Use

Use Postgres backend when:

- SQL durability and operational tooling are a priority
- you already run PostgreSQL and want queue persistence there

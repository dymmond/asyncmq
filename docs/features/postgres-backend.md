# Postgres Backend

When using the Postgres backend, AsyncMQ requires certain tables and indexes to be present in the
database before any jobs can be scheduled or processed.

The `install_or_drop_postgres_backend` function in `asyncmq.core.utils.postgres` handles creating (or dropping)
these schema elements.

## Location

```python
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend
```

## Purpose

* **Install mode** (default): Creates the necessary tables and indexes:

  * `asyncmq_jobs`
  * `asyncmq_repeatables`
  * `asyncmq_cancelled_jobs`
  * Indexes: `idx_asyncmq_jobs_queue_name`, `idx_asyncmq_jobs_status`, `idx_asyncmq_jobs_delay_until`

* **Drop mode** (`drop=True`): Removes the above tables and indexes.

## Usage

```python
import asyncio
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend

# Install tables and indexes
asyncio.run(
    install_or_drop_postgres_backend(
        connection_string="postgresql://user:pass@host:port/dbname",
        # Optional: pass pool options
        min_size=1,
        max_size=10,
    )
)

# To drop existing schema first:
asyncio.run(
    install_or_drop_postgres_backend(
        connection_string="postgresql://user:pass@host/dbname",
        drop=True,
    )
)
```

## Function Signature

```python
async def install_or_drop_postgres_backend(
    connection_string: str | None = None,
    drop: bool = False,
    **pool_options: Any
) -> None:
```

### Parameters

| Name                | Type            | Default                                          | Description                                                                                                              |
| ------------------- | --------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| `connection_string` | `str` or `None` | `None`                                           | Postgres DSN (`postgresql://user:pass@host:port/dbname`). If not provided, uses `settings.asyncmq_postgres_backend_url`. |
| `drop`              | `bool`          | `False`                                          | If `True`, drops existing tables and indexes rather than installing them.                                                |
| `**pool_options`    | `Any`           | `settings.asyncmq_postgres_pool_options` or `{}` | Passed directly to `asyncpg.create_pool`, e.g., `min_size`, `max_size`, `timeout`, etc.                                  |

> **Note:** If neither `connection_string` nor `settings.asyncmq_postgres_backend_url` is set, the function will raise a `ValueError`.

## Settings Referenced

* `settings.asyncmq_postgres_backend_url` (`str | None`): Default DSN if `connection_string` is not provided.
* `settings.asyncmq_postgres_pool_options` (`dict[str, Any] | None`): Default pool options if none passed.
* `settings.postgres_jobs_table_name` (`str`): Default table name, e.g., `asyncmq_jobs`.
* `settings.postgres_repeatables_table_name` (`str`): Default repeatables table name, e.g., `asyncmq_repeatables`.
* `settings.postgres_cancelled_jobs_table_name` (`str`): Default cancelled jobs table name, e.g., `asyncmq_cancelled_jobs`.

## SQL Schema Definitions

### Install (Create)

```sql
CREATE TABLE IF NOT EXISTS {settings.postgres_jobs_table_name} (
  id SERIAL PRIMARY KEY,
  queue_name TEXT NOT NULL,
  job_id TEXT NOT NULL UNIQUE,
  data JSONB NOT NULL,
  status TEXT,
  delay_until DOUBLE PRECISION,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

-- For repeatable jobs
CREATE TABLE IF NOT EXISTS {settings.postgres_repeatables_table_name} (
  queue_name TEXT NOT NULL,
  job_def JSONB NOT NULL,
  next_run TIMESTAMPTZ NOT NULL,
  paused BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY(queue_name, job_def)
);

-- For cancellations
CREATE TABLE IF NOT EXISTS {settings.postgres_cancelled_jobs_table_name} (
  queue_name TEXT NOT NULL,
  job_id TEXT NOT NULL,
  PRIMARY KEY(queue_name, job_id)
);

-- Indexes for efficient lookups
CREATE INDEX IF NOT EXISTS idx_asyncmq_jobs_queue_name ON {settings.postgres_jobs_table_name}(queue_name);
CREATE INDEX IF NOT EXISTS idx_asyncmq_jobs_status ON {settings.postgres_jobs_table_name}(status);
CREATE INDEX IF NOT EXISTS idx_asyncmq_jobs_delay_until ON {settings.postgres_jobs_table_name}(delay_until);
```

### Drop

```sql
DROP TABLE IF EXISTS {settings.postgres_jobs_table_name};
DROP TABLE IF EXISTS {settings.postgres_repeatables_table_name};
DROP TABLE IF EXISTS {settings.postgres_cancelled_jobs_table_name};

DROP INDEX IF EXISTS idx_asyncmq_jobs_queue_name;
DROP INDEX IF EXISTS idx_asyncmq_jobs_status;
DROP INDEX IF EXISTS idx_asyncmq_jobs_delay_until;
```

## Example

```python
import asyncio
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend

# Run installation with pool tweaks
asyncio.run(
    install_or_drop_postgres_backend(
        connection_string="postgresql://postgres:secret@db.example.com/asyncmq",
        min_size=5,
        max_size=20,
    )
)
```

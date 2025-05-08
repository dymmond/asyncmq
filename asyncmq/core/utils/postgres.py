from typing import Any

try:
    import asyncpg
except ImportError:
    raise ImportError("Please install asyncpg: `pip install asyncpg`") from None

from asyncmq.conf import monkay


async def install_or_drop_postgres_backend(
    connection_string: str | None = None, drop: bool = False, **pool_options: Any
) -> None:
    """
    Utility function to install the required `{monkay.settings.postgres_jobs_table_name}` table and indexes
    in the connected Postgres database.

    Connects to the database specified by the DSN, creates a table named
    according to `monkay.settings.postgres_jobs_table_name` with columns for job ID, queue name,
    data (JSONB), status, delay timestamp, and creation/update timestamps.
    It also creates indexes on `queue_name`, `status`, and `delay_until` for
    efficient querying. Operations are wrapped in a transaction.

    Args:
        connection_string: The Postgres DSN (connection URL string) used to connect to the
             database where the schema should be installed.

    Example:
        >>> import asyncio
        >>> asyncio.run(install_postgres_backend("postgresql://user:pass@host/dbname"))
    """
    # Define the SQL schema for the jobs table and its indexes.
    # The table name is pulled from settings, but index names are hardcoded.
    if not connection_string and not monkay.settings.asyncmq_postgres_backend_url:
        raise ValueError(
            "Either 'connection_string' or 'monkay.settings.asyncmq_postgres_backend_url' must be " "provided."
        )

    pool_options: dict[str, Any] | None = pool_options or monkay.settings.asyncmq_postgres_pool_options or {}  # type: ignore
    dsn = connection_string or monkay.settings.asyncmq_postgres_backend_url
    # Build the proper SQL depending on drop vs install:
    if not drop:
        # DROP old version, then CREATE fresh
        schema = f"""
            -- drop any old schema
            DROP TABLE IF EXISTS {monkay.settings.postgres_jobs_table_name};
            DROP TABLE IF EXISTS {monkay.settings.postgres_repeatables_table_name};
            DROP TABLE IF EXISTS {monkay.settings.postgres_cancelled_jobs_table_name};
            DROP TABLE IF EXISTS {monkay.settings.postgres_workers_heartbeat_table_name};

            -- jobs table with delay_until column
            CREATE TABLE {monkay.settings.postgres_jobs_table_name} (
                id SERIAL PRIMARY KEY,
                queue_name TEXT NOT NULL,
                job_id TEXT NOT NULL UNIQUE,
                data JSONB NOT NULL,
                status TEXT,
                delay_until DOUBLE PRECISION,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            );

            -- repeatables table
            CREATE TABLE {monkay.settings.postgres_repeatables_table_name} (
                queue_name TEXT NOT NULL,
                job_def    JSONB NOT NULL,
                next_run   TIMESTAMPTZ NOT NULL,
                paused     BOOLEAN     NOT NULL DEFAULT FALSE,
                PRIMARY KEY(queue_name, job_def)
            );

            -- cancellations table
            CREATE TABLE {monkay.settings.postgres_cancelled_jobs_table_name} (
                queue_name TEXT NOT NULL,
                job_id     TEXT NOT NULL,
                PRIMARY KEY(queue_name, job_id)
            );

            -- worker heartbeats
            CREATE TABLE {monkay.settings.postgres_workers_heartbeat_table_name} (
                worker_id   TEXT PRIMARY KEY,
                queues      TEXT[],        -- array of queue names
                concurrency INT,
                heartbeat   DOUBLE PRECISION
            );

            -- indexes
            CREATE INDEX IF NOT EXISTS idx_{monkay.settings.postgres_jobs_table_name}_queue_name    ON {monkay.settings.postgres_jobs_table_name}(queue_name);
            CREATE INDEX IF NOT EXISTS idx_{monkay.settings.postgres_jobs_table_name}_status        ON {monkay.settings.postgres_jobs_table_name}(status);
            CREATE INDEX IF NOT EXISTS idx_{monkay.settings.postgres_jobs_table_name}_delay_until   ON {monkay.settings.postgres_jobs_table_name}(delay_until);
            """
    else:
        # only drop everything
        schema = f"""
            DROP TABLE IF EXISTS {monkay.settings.postgres_jobs_table_name};
            DROP TABLE IF EXISTS {monkay.settings.postgres_repeatables_table_name};
            DROP TABLE IF EXISTS {monkay.settings.postgres_cancelled_jobs_table_name};
            DROP TABLE IF EXISTS {monkay.settings.postgres_workers_heartbeat_table_name};
            DROP INDEX IF EXISTS idx_{monkay.settings.postgres_jobs_table_name}_queue_name;
            DROP INDEX IF EXISTS idx_{monkay.settings.postgres_jobs_table_name}_status;
            DROP INDEX IF EXISTS idx_{monkay.settings.postgres_jobs_table_name}_delay_until;
            """

    # Execute the chosen schema DDL
    pool = await asyncpg.create_pool(dsn=dsn, **pool_options)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(schema)
    await pool.close()

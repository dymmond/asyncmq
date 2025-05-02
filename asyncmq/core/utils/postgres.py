from typing import Any

try:
    import asyncpg
except ImportError:
    raise ImportError("Please install asyncpg: `pip install asyncpg`") from None

from asyncmq.conf import settings


async def install_or_drop_postgres_backend(
    connection_string: str | None = None, drop: bool = False, **pool_options: Any
) -> None:
    """
    Utility function to install the required `asyncmq_jobs` table and indexes
    in the connected Postgres database.

    Connects to the database specified by the DSN, creates a table named
    according to `settings.postgres_jobs_table_name` with columns for job ID, queue name,
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
    if not connection_string and not settings.asyncmq_postgres_backend_url:
        raise ValueError("Either 'connection_string' or 'settings.asyncmq_postgres_backend_url' must be " "provided.")

    pool_options: dict[str, Any] | None = pool_options or settings.asyncmq_postgres_pool_options or {}  # type: ignore
    dsn = connection_string or settings.asyncmq_postgres_backend_url
    if not drop:
        schema = f"""
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
          job_def    JSONB NOT NULL,
          next_run   TIMESTAMPTZ NOT NULL,
          paused     BOOLEAN     NOT NULL DEFAULT FALSE,
          PRIMARY KEY(queue_name, job_def)
        );

        -- For cancellations
        CREATE TABLE IF NOT EXISTS {settings.postgres_cancelled_jobs_table_name} (
          queue_name TEXT NOT NULL,
          job_id     TEXT NOT NULL,
          PRIMARY KEY(queue_name, job_id)
        );

        -- Indexes for efficient lookups
        CREATE INDEX IF NOT EXISTS idx_asyncmq_jobs_queue_name ON asyncmq_jobs(queue_name);
        CREATE INDEX IF NOT EXISTS idx_asyncmq_jobs_status ON asyncmq_jobs(status);
        CREATE INDEX IF NOT EXISTS idx_asyncmq_jobs_delay_until ON asyncmq_jobs(delay_until);
        """
    else:
        schema = f"""
        DROP TABLE IF EXISTS {settings.postgres_jobs_table_name};
        DROP TABLE IF EXISTS {settings.postgres_repeatables_table_name};
        DROP TABLE IF EXISTS {settings.postgres_cancelled_jobs_table_name};
        DROP INDEX IF EXISTS idx_asyncmq_jobs_queue_name;
        DROP INDEX IF EXISTS idx_asyncmq_jobs_status;
        DROP INDEX IF EXISTS idx_asyncmq_jobs_delay_until;
        """

    # Create an asyncpg connection pool.
    pool = await asyncpg.create_pool(dsn=dsn, **pool_options)
    # Acquire a connection from the pool.
    async with pool.acquire() as conn:
        # Start a transaction.
        async with conn.transaction():
            # Execute the schema creation SQL.
            await conn.execute(schema)
    # Close the connection pool.
    await pool.close()

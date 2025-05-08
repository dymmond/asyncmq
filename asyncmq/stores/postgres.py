try:
    import asyncpg
except ImportError:
    raise ImportError("Please install asyncpg: `pip install asyncpg`") from None

import json
from typing import Any, cast

from asyncmq.conf import monkay
from asyncmq.stores.base import BaseJobStore


class PostgresJobStore(BaseJobStore):
    """
    A concrete implementation of `BaseJobStore` using PostgreSQL as the backend
    for storing and retrieving job data.

    This store connects to a PostgreSQL database using `asyncpg` and uses a
    designated table (`asyncmq_jobs` by default, configurable via settings)
    to persist job data as JSONB, along with metadata like status, queue name,
    and timestamps. It includes methods for saving, loading, deleting, and
    querying jobs by queue or status. It manages an internal connection pool.
    """

    def __init__(self, dsn: str | None = None, pool_options: Any | None = None) -> None:
        """
        Initializes the PostgresJobStore instance.

        Checks if either a DSN is provided directly or if the database URL is
        available in application monkay.settings. Stores the resolved DSN and initializes
        the connection pool attribute to None.

        Args:
            dsn: An optional database connection string (DSN). If None, the DSN
                 is read from `monkay.settings.asyncmq_postgres_backend_url`. Defaults to None.

        Raises:
            ValueError: If neither `dsn` nor `monkay.settings.asyncmq_postgres_backend_url`
                        is provided.
        """
        # Check if a DSN is provided or available in monkay.settings.
        if not dsn and not monkay.settings.asyncmq_postgres_backend_url:
            # Raise an error if no DSN source is available.
            raise ValueError("Either 'dsn' or 'monkay.settings.asyncmq_postgres_backend_url' must be " "provided.")
        # Store the resolved DSN, prioritizing the explicit 'dsn' argument.
        self.dsn = dsn or monkay.settings.asyncmq_postgres_backend_url
        # Initialize the connection pool to None; it will be created on first connection.
        self.pool: asyncpg.Pool | None = None
        self.pool_options = pool_options or monkay.settings.asyncmq_postgres_pool_options or {}

    async def connect(self) -> None:
        """
        Asynchronously establishes a connection pool to the PostgreSQL database
        if one does not already exist.
        """
        # Check if the connection pool has already been created.
        if self.pool is None:
            # If not, create a new connection pool using the stored DSN.
            self.pool = await asyncpg.create_pool(dsn=self.dsn, **self.pool_options)

    async def disconnect(self) -> None:
        """
        Asynchronously closes the PostgreSQL database connection pool if it exists.
        """
        # Check if the connection pool exists.
        if self.pool:
            # If it exists, close the pool.
            await self.pool.close()
            # Set the pool attribute back to None.
            self.pool = None

    async def save(self, queue_name: str, job_id: str, data: dict[str, Any]) -> None:
        """
        Asynchronously saves or updates the data for a specific job in the
        PostgreSQL database.

        Performs an INSERT operation. If a job with the same `job_id` already
        exists (due to the UNIQUE constraint), it updates the `data`, `status`,
        and `updated_at` fields using `ON CONFLICT (job_id) DO UPDATE`.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            data: A dictionary containing the job's data and metadata to be saved.
        """
        # Ensure the connection pool is established.
        await self.connect()
        # Acquire a connection from the pool for the duration of the operation.
        async with self.pool.acquire() as conn:
            # Execute the INSERT or UPDATE SQL query.
            await conn.execute(
                f"""
                INSERT INTO {monkay.settings.postgres_jobs_table_name} (queue_name, job_id, data, status)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (job_id)
                DO UPDATE SET data = EXCLUDED.data, status = EXCLUDED.status, updated_at = now()
                """,
                # Pass parameters to the query to prevent SQL injection.
                queue_name,
                job_id,
                json.dumps(data),  # Serialize data dictionary to JSON string.
                data.get("status"),
            )

    async def load(self, queue_name: str, job_id: str) -> dict[str, Any] | None:
        """
        Asynchronously loads the data for a specific job from the PostgreSQL
        database by its queue name and job ID.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            A dictionary containing the job's data and metadata if found,
            otherwise None. The JSONB data is automatically converted back
            to a Python dictionary by asyncpg.
        """
        # Ensure the connection pool is established.
        await self.connect()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # Fetch a single row matching the queue name and job ID.
            row = await conn.fetchrow(
                f"""
                SELECT data FROM {monkay.settings.postgres_jobs_table_name} WHERE queue_name = $1 AND job_id = $2
                """,
                # Pass parameters to the query.
                queue_name,
                job_id,
            )
            # If a row was found, return the data column (which is JSONB and decoded by asyncpg).
            # Otherwise, return None.
            if row:
                return cast(dict[str, Any], json.loads(row["data"]))
            return None

    async def delete(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously deletes the data for a specific job from the PostgreSQL
        database by its queue name and job ID.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
        """
        # Ensure the connection pool is established.
        await self.connect()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # Execute the DELETE SQL query.
            await conn.execute(
                f"""
                DELETE FROM {monkay.settings.postgres_jobs_table_name} WHERE queue_name = $1 AND job_id = $2
                """,
                # Pass parameters to the query.
                queue_name,
                job_id,
            )

    async def all_jobs(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves data for all jobs associated with a specific
        queue from the PostgreSQL database.

        Args:
            queue_name: The name of the queue.

        Returns:
            A list of dictionaries, where each dictionary contains the data
            for a job in the specified queue.
        """
        # Ensure the connection pool is established.
        await self.connect()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # Fetch all rows for the given queue name.
            rows = await conn.fetch(
                f"""
                SELECT data FROM {monkay.settings.postgres_jobs_table_name} WHERE queue_name = $1
                """,
                # Pass the queue name as a parameter.
                queue_name,
            )
            # Extract and return the 'data' column (JSONB) from each row as a list of dictionaries.
            return [json.loads(row["data"]) for row in rows]

    async def jobs_by_status(self, queue_name: str, status: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves data for jobs in a specific queue that are
        currently in a given status from the PostgreSQL database.

        Args:
            queue_name: The name of the queue.
            status: The status of the jobs to retrieve (e.g., "waiting", "active").

        Returns:
            A list of dictionaries, where each dictionary contains the data
            for a job matching the criteria.
        """
        # Ensure the connection pool is established.
        await self.connect()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # Fetch rows matching the queue name and status.
            rows = await conn.fetch(
                f"""
                SELECT data FROM {monkay.settings.postgres_jobs_table_name} WHERE queue_name = $1 AND status = $2
                """,
                # Pass parameters to the query.
                queue_name,
                status,
            )
            # Extract and return the 'data' column (JSONB) from each row as a list of dictionaries.
            return [json.loads(row["data"]) for row in rows]

    async def filter(self, queue: str, state: str) -> list[dict[str, Any]]:
        await self.connect()
        query = f"""
            SELECT data FROM {monkay.settings.postgres_jobs_table_name}
            WHERE queue_name = $1 AND status = $2
        """
        rows = await self.pool.fetch(query, queue, state)
        return [json.loads(row["data"]) for row in rows]

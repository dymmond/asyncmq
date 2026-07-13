try:
    import asyncpg
except ImportError:
    raise ImportError("Please install asyncpg: `pip install asyncpg`") from None

import time
from datetime import datetime, timezone
from typing import Any, cast

import anyio
from asyncpg import Pool, Record

from asyncmq import monkay
from asyncmq.backends.base import (
    BaseBackend,
    DelayedInfo,
    RepeatableInfo,
    WorkerInfo,
)
from asyncmq.core.enums import State
from asyncmq.core.inspection import matches_job_type, normalize_job_type
from asyncmq.core.repeatables import normalize_repeatable_job_def
from asyncmq.schedulers import compute_next_run
from asyncmq.stores.postgres import PostgresJobStore


class PostgresBackend(BaseBackend):
    """
    A PostgreSQL-based implementation of the asyncmq backend interface.

    This backend uses a PostgreSQL database to store and manage job queues,
    job states, delayed jobs, DLQs, repeatable tasks, and dependencies. It
    relies on an `asyncpg` connection pool for asynchronous database access
    and delegates job data persistence to a `PostgresJobStore`.
    """

    def __init__(self, dsn: str | None = None, pool_options: Any | None = None) -> None:
        """
        Initializes the PostgresBackend by configuring the database connection
        details and initializing the job store.

        Verifies that a Database Source Name (DSN) is provided either directly
        or via settings, and initializes the `PostgresJobStore` with the DSN.
        The `asyncpg` connection pool is created asynchronously later during
        the `connect` call.

        Args:
            dsn: The connection DSN for the PostgreSQL database. If None,
                 `monkay.settings.asyncmq_postgres_backend_url` is used.
        """

        # Ensure a DSN is provided either directly or via monkay.settings.
        if not dsn and not self._settings.asyncmq_postgres_backend_url:
            raise ValueError("Either 'dsn' or 'self._settings.asyncmq_postgres_backend_url' must be provided.")
        # Store the resolved DSN.
        resolved_dsn = dsn or self._settings.asyncmq_postgres_backend_url
        if resolved_dsn is None:
            raise ValueError("A PostgreSQL DSN must be provided.")
        self.dsn: str = resolved_dsn
        # Initialize the asyncpg connection pool to None; it will be created on connect.
        self.pool: Pool | None = None
        self._paused_queues: set[str] = set()
        # Initialize the PostgresJobStore with the DSN.
        self.pool_options = pool_options or self._settings.asyncmq_postgres_pool_options or {}
        self.store: PostgresJobStore = PostgresJobStore(dsn=self.dsn, pool_options=self.pool_options)

    async def pop_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Atomically retrieve and remove all delayed jobs whose delay_until ≤ now
        in a single SQL statement, returning their payloads.
        """
        await self.connect()
        now = time.time()
        # Pull & delete in one CTE-backed statement
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                WITH due AS (
                  DELETE FROM {monkay.settings.postgres_jobs_table_name}
                  WHERE queue_name = $1
                    AND status = $2
                    AND (data ->>'delay_until') IS NOT NULL
                    AND (data ->>'delay_until')::float <= $3
                  RETURNING data
                )
                SELECT data FROM due
                """,
                queue_name,
                State.DELAYED,
                now,
            )
        # Each row.data is a JSON string
        return [self._json_serializer.to_dict(r["data"]) for r in rows]

    async def promote_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Atomically move due delayed rows to waiting rows and return the promoted payloads.
        """
        await self.connect()
        now = time.time()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                WITH due AS (
                    SELECT job_id
                    FROM {monkay.settings.postgres_jobs_table_name}
                    WHERE queue_name = $1
                      AND status = $2
                      AND (data ->> 'delay_until') IS NOT NULL
                      AND (data ->> 'delay_until')::float <= $3
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE {monkay.settings.postgres_jobs_table_name} AS jobs
                SET
                    status = $4::text,
                    delay_until = NULL,
                    data = jobs.data || jsonb_build_object('status', $4::text, 'delay_until', NULL),
                    updated_at = now()
                FROM due
                WHERE jobs.queue_name = $1
                  AND jobs.job_id = due.job_id
                RETURNING jobs.data
                """,
                queue_name,
                State.DELAYED,
                now,
                State.WAITING,
            )
        return [self._json_serializer.to_dict(row["data"]) for row in rows]

    async def connect(self) -> None:
        """
        Asynchronously establishes a connection to the PostgreSQL database by
        creating an `asyncpg` connection pool and connecting the job store.

        This method is idempotent; subsequent calls will not recreate the pool.
        """
        # Create the connection pool if it doesn't already exist.
        if self.pool is None:
            self.pool = await asyncpg.create_pool(dsn=self.dsn, **self.pool_options)
            # Also ensure the associated job store is connected.
            await self.store.connect()
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS asyncmq_paused_queues (
                        queue_name TEXT PRIMARY KEY,
                        paused_at  TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
                    )
                    """
                )

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> str:
        """
        Asynchronously enqueues a job payload onto the specified queue for
        immediate processing.

        The job's status is updated to `State.WAITING`, and the full job
        payload is saved in the job store.

        Args:
            queue_name: The name of the queue to enqueue the job onto.
            payload: The job data as a dictionary. Must contain an "id" key.
        """
        # Ensure connection is established before interacting with the DB.
        await self.connect()
        # Update job status to WAITING.
        payload["status"] = State.WAITING
        # Save the job payload in the job store.
        await self.store.save(queue_name, payload["id"], payload)
        return cast(str, payload["id"])

    async def bulk_enqueue(self, queue_name: str, jobs: list[dict[str, Any]]) -> None:
        """
        Asynchronously enqueues multiple job payloads onto the specified queue.

        Iterates through the list of job payloads and calls `enqueue` for each.
        Note that this is not a single atomic database transaction.

        Args:
            queue_name: The name of the queue to enqueue jobs onto.
            jobs: A list of job payloads (dictionaries) to be enqueued. Each
                  dictionary is expected to contain at least an "id" key.
        """
        # Ensure connection is established.
        await self.connect()
        # Iterate and enqueue each job individually.
        for payload in jobs:
            await self.enqueue(queue_name, payload)

    async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
        """
        Asynchronously attempts to dequeue the next job from the specified queue.

        Uses a `SELECT FOR UPDATE SKIP LOCKED` query within a transaction to
        atomically select the next waiting job, update its status to `State.ACTIVE`,
        and return its payload. This prevents multiple workers from picking the
        same job. Jobs are selected by priority first and creation time second,
        preserving FIFO order among jobs with the same priority.

        Args:
            queue_name: The name of the queue to dequeue a job from.

        Returns:
            The job data as a dictionary if a job was successfully dequeued,
            otherwise None.
        """
        # Ensure connection is established.
        await self.connect()
        active_since = time.time()
        # Acquire a connection from the pool and start a transaction for atomicity.
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""
                    DELETE FROM {self._settings.postgres_jobs_table_name} AS jobs
                    WHERE jobs.queue_name = $1
                      AND jobs.status = $2
                      AND EXISTS (
                          SELECT 1
                          FROM {self._settings.postgres_cancelled_jobs_table_name} AS cancelled
                          WHERE cancelled.queue_name = jobs.queue_name
                            AND cancelled.job_id = jobs.job_id
                      )
                    """,
                    queue_name,
                    State.WAITING,
                )
                # Select the oldest WAITING job for the given queue, lock it,
                # update its status to ACTIVE, and return its data.
                row: Record | None = await conn.fetchrow(
                    f"""
                    UPDATE {self._settings.postgres_jobs_table_name}
                    SET
                        status = $3,
                        data = data || jsonb_build_object('status', $3::text, 'active_since', $4::double precision),
                        updated_at = now()
                    WHERE id = (
                        SELECT jobs.id FROM {self._settings.postgres_jobs_table_name} AS jobs
                        WHERE jobs.queue_name = $1
                          AND jobs.status = $2
                          AND COALESCE(
                              CASE
                                  WHEN jsonb_typeof(jobs.data -> 'depends_on') = 'array'
                                  THEN jsonb_array_length(jobs.data -> 'depends_on')
                                  ELSE 0
                              END,
                              0
                          ) = 0
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {self._settings.postgres_cancelled_jobs_table_name} AS cancelled
                              WHERE cancelled.queue_name = jobs.queue_name
                                AND cancelled.job_id = jobs.job_id
                          )
                        ORDER BY COALESCE((jobs.data ->> 'priority')::int, 5) ASC, jobs.created_at ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING data
                    """,
                    queue_name,
                    State.WAITING,
                    State.ACTIVE,
                    active_since,
                )
                # If a row was returned (a job was dequeued).
                if row:
                    # Deserialize and return the job payload.
                    return cast(dict[str, Any], self._json_serializer.to_dict(row["data"]))
                # Return None if no waiting job was found.
                return None

    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously acknowledges the successful processing of a job.

        This typically involves removing the job from the backend storage.
        In this implementation, it delegates the deletion to the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job being acknowledged.
        """
        # Ensure connection is established.
        await self.connect()

        # Only delete if there is no saved result to retrieve.
        job = await self.store.load(queue_name, job_id)
        if not job or job.get("result") is None:
            await self.store.delete(queue_name, job_id)

    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously moves a job payload to the Dead Letter Queue (DLQ)
        associated with the specified queue.

        The job's status is updated to `State.FAILED`, and the full job
        payload is saved/updated in the job store.

        Args:
            queue_name: The name of the queue the job originated from.
            payload: The job data as a dictionary. Must contain an "id" key.
        """
        # Ensure connection is established.
        await self.connect()
        # Update job status to FAILED.
        payload["status"] = State.FAILED
        # Save the job payload in the job store.
        await self.store.save(queue_name, payload["id"], payload)

    async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        """
        Asynchronously schedules a job to be available for processing at a
        specific future time by storing it with a delay timestamp.

        The job's status is updated to `State.DELAYED`, the `delay_until` field
        is set to the target timestamp, and the full payload is saved in the
        job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            payload: The job data as a dictionary. Must contain an "id" key.
            run_at: The absolute Unix timestamp (float) when the job should
                    become available for processing.
        """
        # Ensure connection is established.
        await self.connect()
        # Update job status and set the delay_until timestamp.
        payload["status"] = State.DELAYED
        payload["delay_until"] = run_at
        # Save the job payload in the job store.
        await self.store.save(queue_name, payload["id"], payload)

    async def _save_lifecycle_payload(self, queue_name: str, payload: dict[str, Any]) -> None:
        await self.connect()
        job_id = str(payload["id"])
        payload_json = self._json_serializer.to_json(payload)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                cancelled = await conn.fetchval(
                    f"""
                    SELECT 1
                    FROM {self._settings.postgres_cancelled_jobs_table_name}
                    WHERE queue_name = $1
                      AND job_id = $2
                    """,
                    queue_name,
                    job_id,
                )
                if cancelled:
                    await conn.execute(
                        f"""
                        DELETE FROM {self._settings.postgres_jobs_table_name}
                        WHERE queue_name = $1
                          AND job_id = $2
                        """,
                        queue_name,
                        job_id,
                    )
                    return
                await conn.execute(
                    f"""
                    INSERT INTO {self._settings.postgres_jobs_table_name}
                        (queue_name, job_id, data, status, delay_until)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (queue_name, job_id)
                    DO UPDATE SET
                        data = EXCLUDED.data,
                        status = EXCLUDED.status,
                        delay_until = EXCLUDED.delay_until,
                        updated_at = now()
                    """,
                    queue_name,
                    job_id,
                    payload_json,
                    payload.get("status"),
                    payload.get("delay_until"),
                )

    async def complete_active_job(self, queue_name: str, payload: dict[str, Any], result: Any) -> None:
        stored = {**payload, "status": State.COMPLETED, "result": result, "delay_until": None}
        stored.pop("heartbeat", None)
        await self._save_lifecycle_payload(queue_name, stored)

    async def retry_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        await self._delay_active_job(queue_name, payload, run_at)

    async def defer_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        await self._delay_active_job(queue_name, payload, run_at)

    async def _delay_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        stored = {**payload, "status": State.DELAYED, "delay_until": run_at}
        stored.pop("heartbeat", None)
        await self._save_lifecycle_payload(queue_name, stored)

    async def fail_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        target_status = payload.get("status")
        if target_status not in {State.FAILED, State.EXPIRED}:
            target_status = State.FAILED
        stored = {**payload, "status": target_status, "delay_until": None}
        stored.pop("heartbeat", None)
        await self._save_lifecycle_payload(queue_name, stored)

    async def expire_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        stored = {**payload, "status": State.EXPIRED, "delay_until": None}
        stored.pop("heartbeat", None)
        await self._save_lifecycle_payload(queue_name, stored)

    async def cancel_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        await self.connect()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""
                    DELETE FROM {self._settings.postgres_jobs_table_name}
                    WHERE queue_name = $1
                      AND job_id = $2
                      AND (NOT (data ? 'result') OR data -> 'result' = 'null'::jsonb)
                    """,
                    queue_name,
                    str(payload["id"]),
                )

    async def get_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves a list of delayed job payloads from the
        specified queue that are now due for processing.

        Queries the database for jobs in the specified queue where the
        `delay_until` timestamp (stored in the `data` JSONB field) is less
        than or equal to the current time.

        Args:
            queue_name: The name of the queue to check for due delayed jobs.

        Returns:
            A list of dictionaries, where each dictionary is a job payload
            that is ready to be moved to the main queue.
        """
        return await self.pop_due_delayed(queue_name)

    async def remove_delayed(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously removes a specific job from the backend's delayed storage
        by its ID.

        This delegates the deletion to the job store. Note that this method does
        not check if the job is actually in the delayed state before attempting
        removal.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove from delayed
                    storage.
        """
        # Ensure connection is established.
        await self.connect()
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                f"""
                DELETE FROM {self._settings.postgres_jobs_table_name}
                WHERE queue_name = $1
                  AND job_id = $2
                  AND status = $3
                """,
                queue_name,
                job_id,
                State.DELAYED,
            )
        return cast(bool, result.endswith("DELETE 1"))

    async def list_delayed(self, queue_name: str) -> list[DelayedInfo]:
        """
        list all jobs currently scheduled for the future.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT data,
                       (data ->> 'delay_until')::float AS run_at
                FROM {self._settings.postgres_jobs_table_name}
                WHERE queue_name = $1
                  AND data ->> 'delay_until' IS NOT NULL
                ORDER BY run_at
                """,
                queue_name,
            )
        result: list[DelayedInfo] = []
        for r in rows:
            payload = self._json_serializer.to_dict(r["data"])
            result.append(DelayedInfo(job_id=payload["id"], run_at=r["run_at"], payload=payload))
        return result

    async def list_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        """
        Return all repeatable definitions for this queue.
        Requires a `repeatables` table with columns (queue_name, job_def JSONB, next_run TIMESTAMPTZ, paused BOOL).
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT job_def, next_run, paused
                FROM {self._settings.postgres_repeatables_table_name}
                WHERE queue_name = $1
                ORDER BY next_run
                """,
                queue_name,
            )
        return [
            RepeatableInfo(
                job_def=r["job_def"],
                next_run=r["next_run"].timestamp(),
                paused=r["paused"],
            )
            for r in rows
        ]

    async def upsert_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Create or update a durable repeatable definition in PostgreSQL.

        Args:
            queue_name: The queue that owns the repeatable definition.
            job_def: The logical repeatable definition.

        Returns:
            The next scheduled UNIX timestamp for the definition.
        """
        clean_def = normalize_repeatable_job_def(job_def)
        next_ts = compute_next_run(clean_def)
        next_dt = datetime.fromtimestamp(next_ts, tz=timezone.utc)
        await self.connect()
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self._settings.postgres_repeatables_table_name}
                  (queue_name, job_def, next_run, paused)
                VALUES ($1, $2::jsonb, $3, FALSE)
                ON CONFLICT (queue_name, job_def)
                DO UPDATE SET next_run = EXCLUDED.next_run,
                              paused = FALSE
                """,
                queue_name,
                self._json_serializer.to_json(clean_def),
                next_dt,
            )
        return next_ts

    async def get_due_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        """
        Return durable repeatables whose next execution time is due.

        Args:
            queue_name: The queue to inspect.

        Returns:
            Due repeatable definitions ordered by next-run time.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT job_def, next_run, paused
                FROM {self._settings.postgres_repeatables_table_name}
                WHERE queue_name = $1
                  AND paused = FALSE
                  AND next_run <= now()
                ORDER BY next_run
                """,
                queue_name,
            )
        return [
            RepeatableInfo(
                job_def=r["job_def"],
                next_run=r["next_run"].timestamp(),
                paused=r["paused"],
            )
            for r in rows
        ]

    async def advance_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Advance a durable repeatable definition after one occurrence is enqueued.

        Args:
            queue_name: The queue that owns the repeatable definition.
            job_def: The logical repeatable definition that just fired.

        Returns:
            The newly persisted next-run UNIX timestamp.
        """
        clean_def = normalize_repeatable_job_def(job_def)
        next_ts = compute_next_run(clean_def)
        next_dt = datetime.fromtimestamp(next_ts, tz=timezone.utc)
        await self.connect()
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._settings.postgres_repeatables_table_name}
                SET next_run = $1,
                    paused = FALSE
                WHERE queue_name = $2
                  AND job_def = $3::jsonb
                """,
                next_dt,
                queue_name,
                self._json_serializer.to_json(clean_def),
            )
        return next_ts

    async def remove_repeatable(self, queue_name: str, job_def: dict[str, Any] | str) -> None:
        """
        Remove a repeatable definition.
        """
        if isinstance(job_def, str):
            raise TypeError("Postgres repeatable removal expects the repeatable job definition, not an identifier.")
        clean_def = normalize_repeatable_job_def(job_def)
        await self.connect()
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                DELETE
                FROM {self._settings.postgres_repeatables_table_name}
                WHERE queue_name = $1
                  AND job_def = $2::jsonb
                """,
                queue_name,
                self._json_serializer.to_json(clean_def),
            )

    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Mark a repeatable as paused so the scheduler will skip it.
        """
        clean_def = normalize_repeatable_job_def(job_def)
        await self.connect()
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._settings.postgres_repeatables_table_name}
                SET paused = TRUE
                WHERE queue_name = $1
                  AND job_def = $2::jsonb
                """,
                queue_name,
                self._json_serializer.to_json(clean_def),
            )

    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> Any:
        """
        Un-pause a repeatable, recompute next_run, and return the new timestamp.
        """
        await self.connect()
        clean = normalize_repeatable_job_def(job_def)
        next_ts = compute_next_run(clean)
        next_dt = datetime.fromtimestamp(next_ts, tz=timezone.utc)
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {self._settings.postgres_repeatables_table_name}
                SET paused   = FALSE,
                    next_run = $1
                WHERE queue_name = $2
                  AND job_def = $3::jsonb
                """,
                next_dt,
                queue_name,
                self._json_serializer.to_json(clean),
            )
        return next_ts

    async def is_job_cancelled(self, queue_name: str, job_id: str) -> bool:
        """
        Check whether a job has been cancelled.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            found = await conn.fetchval(
                f"""
                SELECT 1
                FROM {self._settings.postgres_cancelled_jobs_table_name}
                WHERE queue_name = $1
                  AND job_id = $2
                """,
                queue_name,
                job_id,
            )
        return bool(found)

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        """
        Asynchronously updates the status string of a specific job in the
        job store.

        The job data is loaded, its 'status' field is updated with the new
        state string, and the modified data is saved back to the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            state: The new state string for the job (e.g., "active", "completed").
        """
        # Ensure connection is established.
        await self.connect()
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.store.load(queue_name, job_id)
        # If the job data was successfully loaded.
        if job:
            # Update the status field.
            job["status"] = state
            if state == State.ACTIVE:
                job.setdefault("active_since", time.time())
            # Save the updated job data back to the job store.
            await self.store.save(queue_name, job_id, job)

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        """
        Asynchronously saves the result of a job's execution in the job store.

        The job data is loaded, the 'result' field is added or updated with
        the execution result, and the modified data is saved back to the
        job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            result: The result returned by the job's task function. This should
                    be a value that the configured `PostgresJobStore` can
                    successfully serialize (e.g., JSON serializable).
        """
        # Ensure connection is established.
        await self.connect()
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.store.load(queue_name, job_id)
        # If the job data was successfully loaded.
        if job:
            # Add or update the result field.
            job["result"] = result
            # Save the updated job data back to the job store.
            await self.store.save(queue_name, job_id, job)

    async def save_job_payload(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Persist an updated canonical payload in PostgreSQL.

        Args:
            queue_name: The queue that owns the job.
            payload: The full canonical payload to persist.
        """
        await self.connect()
        await self.store.save(queue_name, str(payload["id"]), dict(payload))

    async def get_job_state(self, queue_name: str, job_id: str) -> str | None:
        """
        Asynchronously retrieves the current status string of a specific job
        from the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            The job's status string if the job is found and has a status,
            otherwise None.
        """
        # Ensure connection is established.
        await self.connect()
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.store.load(queue_name, job_id)
        # Return the status field if the job is found, otherwise None.
        return cast(str, job.get("status")) if job else None

    async def get_job_result(self, queue_name: str, job_id: str) -> Any | None:
        """
        Asynchronously retrieves the execution result of a specific job from
        the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            The result of the job's task function if the job is found and has
            a 'result' field, otherwise None. The type of the result depends
            on the job's return value and how it was saved by `save_job_result`.
        """
        # Ensure connection is established.
        await self.connect()
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.store.load(queue_name, job_id)
        # Return the result field if the job is found, otherwise None.
        return job.get("result") if job else None

    async def add_dependencies(self, queue_name: str, job_dict: dict[str, Any]) -> None:
        """
        Asynchronously adds a list of parent job IDs to a job's dependencies.

        Loads the job data from the store, adds the parent IDs from the
        `depends_on` key of the input dictionary to the job's stored dependencies,
        and saves the updated job data.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: The job data dictionary containing at least an "id" key
                      and an optional "depends_on" key (list of parent job IDs).
        """
        # Ensure connection is established.
        await self.connect()
        job_id: str = str(job_dict["id"])
        # Get the list of parent IDs from the input dictionary.
        dependencies: list[str] = list(dict.fromkeys(job_dict.get("depends_on", [])))
        if not dependencies:
            return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    f"""
                    SELECT data
                    FROM {self._settings.postgres_jobs_table_name}
                    WHERE queue_name = $1 AND job_id = $2
                    FOR UPDATE
                    """,
                    queue_name,
                    job_id,
                )
                job = self._json_serializer.to_dict(row["data"]) if row else {**job_dict, "id": job_id}
                merged = sorted(set(job.get("depends_on", [])) | set(dependencies))
                job["depends_on"] = merged
                job["status"] = job.get("status") or State.WAITING
                await conn.execute(
                    f"""
                    INSERT INTO {self._settings.postgres_jobs_table_name} (queue_name, job_id, data, status)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (queue_name, job_id)
                    DO UPDATE SET data = EXCLUDED.data, status = EXCLUDED.status, updated_at = now()
                    """,
                    queue_name,
                    job_id,
                    self._json_serializer.to_json(job),
                    job.get("status"),
                )

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Asynchronously signals that a parent job has completed and checks if
        any dependent child jobs are now ready to be enqueued.

        It queries for all jobs in the queue that have dependencies. It then
        iterates through these jobs, checks if they depend on the `parent_id`,
        removes `parent_id` from their dependencies, updates the job state to
        `State.WAITING` if all dependencies are met, and saves the updated job.

        Args:
            queue_name: The name of the queue the parent job belonged to.
            parent_id: The unique identifier of the job that just completed
                       and potentially resolves dependencies for other jobs.
        """
        # Ensure connection is established.
        await self.connect()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # Fetch all jobs in the queue that have a non-null 'depends_on' field
            # in their JSONB data.
            rows: list[Record] = await conn.fetch(
                f"""
                SELECT id, data FROM {self._settings.postgres_jobs_table_name}
                WHERE queue_name = $1 AND data->'depends_on' IS NOT NULL
                """,
                queue_name,
            )
            # Iterate through each job returned.
            for row in rows:
                # Deserialize the job data from JSONB.
                data: dict[str, Any] = self._json_serializer.to_dict(row["data"])
                # Get the list of dependencies, defaulting to empty if not found.
                depends_on: list[str] = data.get("depends_on", [])
                # Check if the current parent_id is in this job's dependencies.
                if parent_id in depends_on:
                    # Remove the completed parent_id from the dependencies list.
                    depends_on.remove(parent_id)
                    # If the dependencies list is now empty.
                    if not depends_on:
                        # Remove the 'depends_on' key and set the status to WAITING.
                        data.pop("depends_on")
                        data["status"] = State.WAITING
                    else:
                        # Otherwise, update the dependencies list in the job data.
                        data["depends_on"] = depends_on
                    # Save the updated job data back to the job store.
                    await self.store.save(queue_name, data["id"], data)

    # Atomic flow addition - Note: This implementation does not use a Lua script.
    async def atomic_add_flow(
        self,
        queue_name: str,
        job_dicts: list[dict[str, Any]],
        dependency_links: list[tuple[str, str]],
    ) -> list[str]:
        """
        Atomically enqueues multiple jobs to the waiting queue and registers
        their parent-child dependencies within a single database transaction.

        All jobs provided in `job_dicts` are saved with `State.WAITING`. Then,
        for each dependency link, the child job's data is loaded, the parent
        ID is added to its `depends_on` list if not already present, and the
        child job data is saved. All these operations are wrapped in an
        `asyncpg` transaction.

        Args:
            queue_name: The name of the queue.
            job_dicts: A list of job payloads (dictionaries) to be enqueued. Must
                       contain an "id" key for each job.
            dependency_links: A list of (parent_id, child_id) tuples representing
                              the dependencies.
        Returns:
            A list of the IDs (strings) of the jobs that were processed (enqueued).
        """
        # Ensure connection is established.
        await self.connect()
        # Acquire a connection from the pool and start a transaction for atomicity.
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                deps_by_child: dict[str, set[str]] = {}
                for parent, child in dependency_links:
                    deps_by_child.setdefault(child, set()).add(parent)

                # Save all jobs with their merged dependency metadata.
                for payload in job_dicts:
                    job_id = str(payload["id"])
                    deps = set(payload.get("depends_on", [])) | deps_by_child.get(job_id, set())
                    stored = {**payload, "status": State.WAITING}
                    if deps:
                        stored["depends_on"] = sorted(deps)
                    else:
                        stored.pop("depends_on", None)
                    await conn.execute(
                        f"""
                        INSERT INTO {self._settings.postgres_jobs_table_name} (queue_name, job_id, data, status)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (queue_name, job_id)
                        DO UPDATE SET data = EXCLUDED.data, status = EXCLUDED.status, updated_at = now()
                        """,
                        queue_name,
                        job_id,
                        self._json_serializer.to_json(stored),
                        State.WAITING,
                    )
        # Return the list of IDs of the jobs that were initially provided.
        return [payload["id"] for payload in job_dicts]

    async def pause_queue(self, queue_name: str) -> None:
        """
        Marks the specified queue as paused in PostgreSQL.

        Args:
            queue_name: The name of the queue to pause.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO asyncmq_paused_queues (queue_name)
                VALUES ($1)
                ON CONFLICT (queue_name) DO UPDATE SET paused_at = now()
                """,
                queue_name,
            )
        self._paused_queues.add(queue_name)

    async def resume_queue(self, queue_name: str) -> None:
        """
        Resumes the specified queue by clearing persisted pause state.

        Args:
            queue_name: The name of the queue to resume.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM asyncmq_paused_queues WHERE queue_name = $1
                """,
                queue_name,
            )
        self._paused_queues.discard(queue_name)

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Checks if the specified queue is currently marked as paused.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the queue is paused, otherwise False.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval(
                """
                SELECT EXISTS(SELECT 1 FROM asyncmq_paused_queues WHERE queue_name = $1)
                """,
                queue_name,
            )
        if exists:
            self._paused_queues.add(queue_name)
        else:
            self._paused_queues.discard(queue_name)
        return bool(exists)

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Asynchronously saves the progress percentage for a specific job in the
        job store.

        The job data is loaded from the job store, the 'progress' field is
        added or updated with the new progress value, and the modified data
        is saved back. This allows external monitoring of job progress.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            progress: The progress value, typically a float between 0.0 and 1.0
                      or an integer representing a percentage (0-100).
        """
        # Ensure connection is established.
        await self.connect()
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.store.load(queue_name, job_id)
        # If the job data was successfully loaded.
        if job:
            # Add or update the progress field.
            job["progress"] = progress
            # Save the updated job data back to the job store.
            await self.store.save(queue_name, job_id, job)

    async def purge(self, queue_name: str, state: str, older_than: float | None = None) -> None:
        """
        Removes jobs from a queue based on their state and optional age criteria.

        Deletes jobs from the database where the queue name and status match
        the criteria. If `older_than` is provided, it also filters for jobs
        whose `created_at` timestamp is older than the specified duration
        (current time minus `older_than`).

        Args:
            queue_name: The name of the queue from which to purge jobs.
            state: The state string of the jobs to be removed (e.g., "completed",
                   "failed", "waiting").
            older_than: An optional duration in seconds. Only jobs in the
                        specified `state` created *older* than this duration
                        will be removed. If None, all jobs in the specified state
                        will be removed.
        """
        await self._purge_jobs_by_state(queue_name, state, older_than)

    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        """
        Emits an event.

        Note: This operation is not currently implemented for the Postgres
        backend using database features like LISTEN/NOTIFY and does nothing.
        Event handling must be managed externally.

        Args:
            event: The name of the event to emit.
            data: The data associated with the event.
        """
        pass  # Could use LISTEN/NOTIFY in the future

    async def create_lock(self, key: str, ttl: int) -> Any:
        """
        Creates and returns a database-based distributed lock instance.

        Uses PostgreSQL advisory locks (`pg_try_advisory_lock`,
        `pg_advisory_unlock`) for distributed locking. The lock is identified
        by a hashed key and has an implicit session-based scope (it's released
        when the session ends, not strictly by TTL in the same way as Redis
        locks).

        Args:
            key: A unique string identifier for the lock. This key will be
                 hashed for use with PostgreSQL advisory locks.
            ttl: The time-to-live for the lock in seconds. Note that PostgreSQL
                 advisory locks are session-scoped, not time-scoped via TTL.
                 The `ttl` parameter is accepted for compatibility but does not
                 strictly enforce an expiration time within PostgreSQL itself;
                 it's released when the connection holding it ends.

        Returns:
            A `PostgresLock` instance representing the distributed lock.
            This instance must be acquired (`await lock.acquire()`) and released
            (`await lock.release()`) by the caller. The return type hint is
            `Any` as per the original code.
        """
        # Ensure connection is established.
        await self.connect()
        # Return a new PostgresLock instance.
        return PostgresLock(self.pool, key, ttl)

    async def close(self) -> None:
        """
        Asynchronously closes the database connection pool and disconnects
        the associated job store.

        This should be called when the backend is no longer needed.
        """
        # Close the connection pool if it exists.
        if self.pool:
            await self.pool.close()
            self.pool = None
        # Disconnect the job store.
        await self.store.disconnect()

    async def list_queues(self) -> list[str]:
        """
        Asynchronously lists all known queue names managed by this backend.

        This is done by querying the distinct `queue_name` values from the
        jobs table.

        Returns:
            A list of strings, where each string is the name of a queue.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            # Fetch all distinct queue names from the jobs table.
            rows: list[Record] = await conn.fetch(
                f"SELECT DISTINCT queue_name FROM {self._settings.postgres_jobs_table_name}"
            )
            # Extract and return the queue names.
            return [row["queue_name"] for row in rows]

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        """
        Asynchronously returns statistics about the number of jobs in different
        states for a specific queue.

        Provides counts for jobs currently waiting, delayed, and in the dead
        letter queue (DLQ) by querying the database.

        Args:
            queue_name: The name of the queue to get statistics for.

        Returns:
            A dictionary containing the counts with keys "waiting", "delayed",
            and "failed". Counts are defaulted to 0 if no jobs are found in a state.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            # Fetch the count of waiting jobs.
            waiting: int | None = await conn.fetchval(
                f"SELECT COUNT(*) FROM {self._settings.postgres_jobs_table_name} WHERE queue_name=$1 AND status='waiting'",
                queue_name,
            )
            # Fetch the count of delayed jobs.
            delayed: int | None = await conn.fetchval(
                f"SELECT COUNT(*) FROM {self._settings.postgres_jobs_table_name} WHERE queue_name=$1 AND status='delayed'",
                queue_name,
            )
            # Fetch the count of failed jobs.
            failed: int | None = await conn.fetchval(
                f"SELECT COUNT(*) FROM {self._settings.postgres_jobs_table_name} WHERE queue_name=$1 AND status='failed'",
                queue_name,
            )
            # Return the counts, defaulting None to 0.
            return {
                "waiting": waiting or 0,
                "delayed": delayed or 0,
                "failed": failed or 0,
            }

    async def list_jobs(self, queue_name: str, state: str) -> list[dict[str, Any]]:
        """
        lists jobs in a specific queue filtered by job state.

        Args:
            queue_name: The name of the queue.
            state: The job status (e.g. waiting, active, completed, failed, delayed).

        Returns:
            A list of job payload dictionaries.
        """
        normalized = normalize_job_type(state)
        if normalized == "waiting-children":
            jobs = await self.store.all_jobs(queue_name)
            return [job for job in jobs if matches_job_type(job, normalized)]

        if normalized in {"paused", "prioritized"}:
            return []

        await self.connect()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT data
                FROM {self._settings.postgres_jobs_table_name}
                WHERE queue_name = $1 AND status = $2
                """,
                queue_name,
                normalized,
            )
            jobs = [self._json_serializer.to_dict(row["data"]) for row in rows]
            return [job for job in jobs if matches_job_type(job, normalized)]

    async def cancel_job(self, queue_name: str, job_id: str) -> bool:
        """
        Cancel a job—prevents waiting, delayed, and in-flight execution.
        Requires a `cancelled_jobs(queue_name TEXT, job_id TEXT PRIMARY KEY)` table.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""
                        INSERT INTO {self._settings.postgres_cancelled_jobs_table_name}
                          (queue_name, job_id)
                        VALUES ($1, $2)
                        ON CONFLICT DO NOTHING
                        """,
                    queue_name,
                    job_id,
                )
                await conn.execute(
                    f"""
                    DELETE FROM {self._settings.postgres_jobs_table_name}
                    WHERE queue_name = $1
                      AND job_id = $2
                      AND status = ANY($3::text[])
                    """,
                    queue_name,
                    job_id,
                    [State.WAITING, State.DELAYED],
                )
                await conn.execute(
                    f"""
                    UPDATE {self._settings.postgres_jobs_table_name}
                    SET status = $3,
                        data = data || jsonb_build_object(
                            'status', $3::text,
                            'cancelled_at', EXTRACT(EPOCH FROM now())::double precision
                        ),
                        delay_until = NULL,
                        updated_at = now()
                    WHERE queue_name = $1
                      AND job_id = $2
                      AND status = $4
                    """,
                    queue_name,
                    job_id,
                    "cancelled",
                    State.ACTIVE,
                )
        return True

    async def retry_job(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously retries a failed job by updating its status from 'failed'
        to 'waiting' in the database.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the failed job to retry.

        Returns:
            True if a job with the given ID and queue name was found in the
            'failed' state and successfully updated to 'waiting', False otherwise.
        """
        await self.connect()
        job = await self.store.load(queue_name, job_id)
        if job is None:
            return False

        retry_payload = self._prepare_retry_payload(job, job_id)
        retry_payload["updated_at"] = time.time()
        await self.store.save(queue_name, job_id, retry_payload)
        return True

    async def remove_job(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously removes a specific job from the database by its ID and
        queue name, regardless of its current state.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove.

        Returns:
            True if a job with the given ID and queue name was found and
            successfully deleted, False otherwise.
        """
        # Acquire a connection from the job store's pool.
        # Note: This accesses the pool via the job store, which differs from
        # other methods using `self.pool.acquire()`.
        await self.connect()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                job_cmd = await conn.execute(
                    f"""
                            DELETE FROM {self._settings.postgres_jobs_table_name}
                            WHERE job_id = $1
                              AND queue_name = $2
                            """,
                    job_id,
                    queue_name,
                )
                cancel_cmd = await conn.execute(
                    f"""
                            DELETE FROM {self._settings.postgres_cancelled_jobs_table_name}
                            WHERE job_id = $1
                              AND queue_name = $2
                            """,
                    job_id,
                    queue_name,
                )
        return cast(bool, job_cmd.endswith("DELETE 1") or cancel_cmd.endswith("DELETE 1"))

    async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
        """
        Records the last heartbeat timestamp for a specific job.

        This method updates the `data` JSONB column of the job's row in the database
        by setting or updating the 'heartbeat' key with the provided timestamp.
        It also updates the `updated_at` column to the current time.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            timestamp: The Unix timestamp representing the time of the heartbeat.
        """
        await self.connect()  # Ensure connection is established

        # Acquire a connection from the pool
        async with self.pool.acquire() as conn:
            # Execute the SQL UPDATE statement
            await conn.execute(
                f"""
                UPDATE {self._settings.postgres_jobs_table_name}
                   SET data = jsonb_set(
                               data,
                               '{{heartbeat}}',
                               to_jsonb($3::double precision),
                               true
                             ),
                       updated_at = now()
                 WHERE queue_name = $1
                   AND job_id     = $2
                """,
                queue_name,
                job_id,
                timestamp,
            )

    async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
        """
        Retrieves jobs currently marked as ACTIVE that have not had a heartbeat
        recorded since a specified timestamp.

        This method queries the database for jobs where the 'heartbeat' key
        within the `data` JSONB column is less than the `older_than` timestamp
        and the job's `status` is 'active'.

        Args:
            older_than: A Unix timestamp. Jobs with a heartbeat timestamp
                        strictly less than this value will be considered stalled.

        Returns:
            A list of dictionaries. Each dictionary contains 'queue_name' and
            'job_data'. 'job_data' is the parsed JSON content of the `data` column.
        """
        await self.connect()  # Ensure connection is established

        # Fetch rows from the database pool
        rows = await self.pool.fetch(
            f"""
            SELECT queue_name, data
              FROM {self._settings.postgres_jobs_table_name}
             WHERE status = $2
               AND (
                   (data ? 'heartbeat' AND (data->>'heartbeat')::float < $1)
                   OR (
                       NOT (data ? 'heartbeat')
                       AND COALESCE((data->>'active_since')::float, EXTRACT(EPOCH FROM updated_at)) < $1
                   )
               )
            """,
            older_than,
            State.ACTIVE,
        )

        # Process the retrieved rows. The 'data' column is returned as a JSON string.
        # It needs to be parsed into a Python dictionary.
        return [
            {"queue_name": row["queue_name"], "job_data": self._json_serializer.to_dict(row["data"])} for row in rows
        ]

    async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
        """
        Re-enqueues a job that was previously identified as stalled.

        This method prepares the job data for re-enqueueing by resetting its status
        to WAITING and then calls the internal `enqueue` method to place it back
        into the processing queue.

        Args:
            queue_name: The name of the queue the job should be re-enqueued into.
            job_data: The dictionary containing the job's data, including its current
                      state and payload. This dictionary is modified in-place.
        """
        await self.connect()
        retry_payload = dict(job_data)
        job_id = str(retry_payload["id"])
        expected_active_since = retry_payload.get("active_since")
        retry_payload["status"] = State.WAITING
        retry_payload.pop("heartbeat", None)
        retry_payload.pop("active_since", None)
        retry_payload.pop("updated_at", None)
        payload_json = self._json_serializer.to_json(retry_payload)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""
                    UPDATE {self._settings.postgres_jobs_table_name} AS jobs
                    SET status = $3,
                        data = $4,
                        delay_until = NULL,
                        updated_at = now()
                    WHERE jobs.queue_name = $1
                      AND jobs.job_id = $2
                      AND jobs.status = $5
                      AND NOT EXISTS (
                          SELECT 1
                          FROM {self._settings.postgres_cancelled_jobs_table_name} AS cancelled
                          WHERE cancelled.queue_name = jobs.queue_name
                            AND cancelled.job_id = jobs.job_id
                      )
                      AND (
                          $6::double precision IS NULL
                          OR abs(
                              COALESCE(
                                  (jobs.data ->> 'active_since')::double precision,
                                  EXTRACT(EPOCH FROM jobs.updated_at)
                              ) - $6::double precision
                          ) <= 0.000001
                      )
                    """,
                    queue_name,
                    job_id,
                    State.WAITING,
                    payload_json,
                    State.ACTIVE,
                    expected_active_since,
                )

    async def register_worker(
        self,
        worker_id: str,
        queue: str,
        concurrency: int,
        timestamp: float,
    ) -> None:
        """
        Registers or updates a worker's heartbeat in the PostgreSQL backend.

        This function inserts a new worker record or updates an existing one
        based on the worker_id. It stores the worker's assigned queue(s),
        concurrency level, and the current heartbeat timestamp.

        Args:
            worker_id: The unique identifier for the worker.
            queue: The name of the queue the worker is associated with.
                   (Note: Currently stored as a single item list in the DB).
            concurrency: The maximum number of tasks the worker can process concurrently.
            timestamp: The timestamp representing the worker's last heartbeat.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                        INSERT INTO {self._settings.postgres_workers_heartbeat_table_name}
                          (worker_id, queues, concurrency, heartbeat)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (worker_id)
                        DO UPDATE SET
                          queues      = EXCLUDED.queues,
                          concurrency = EXCLUDED.concurrency,
                          heartbeat   = EXCLUDED.heartbeat;
                        """,
                worker_id,
                [queue],  # wrap in list to match text[] type
                concurrency,
                timestamp,
            )

    async def deregister_worker(self, worker_id: str | int) -> None:
        """
        Removes a worker's record from the PostgreSQL backend.

        Deletes the entry for the specified worker_id from the heartbeat table.

        Args:
            worker_id: The unique identifier of the worker to deregister.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"DELETE FROM {self._settings.postgres_workers_heartbeat_table_name} WHERE worker_id = $1", worker_id
            )

    async def list_workers(self) -> list[WorkerInfo]:
        """
        Lists active workers from the PostgreSQL backend.

        Retrieves workers whose last heartbeat is within the configured
        time-to-live (TTL) from the heartbeat table.

        Returns:
            A list of WorkerInfo objects representing the active workers.
        """
        cutoff = time.time() - self._settings.heartbeat_ttl
        await self.connect()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                                    SELECT worker_id, queues, concurrency, heartbeat
                                    FROM {self._settings.postgres_workers_heartbeat_table_name}
                                    WHERE heartbeat >= $1
                                    """,
                cutoff,
            )
        out: list[WorkerInfo] = []
        for row in rows:
            queues = row["queues"] or []
            queue_name = queues[0] if isinstance(queues, list) and queues else ""
            out.append(
                WorkerInfo(
                    id=row["worker_id"],
                    queue=queue_name,
                    concurrency=row["concurrency"],
                    heartbeat=row["heartbeat"],
                )
            )
        return out


class PostgresLock:
    """
    A distributed lock implementation using PostgreSQL advisory locks.

    Uses `pg_try_advisory_lock` for non-blocking acquisition and
    `pg_advisory_unlock` for releasing the lock. Locks are identified by
    a hashed key and are session-scoped.
    """

    def __init__(self, pool: Pool, key: str, ttl: int) -> None:
        """
        Initializes the PostgresLock.

        Args:
            pool: The `asyncpg` connection pool to use for acquiring and
                  releasing the lock.
            key: A unique string identifier for the lock. This key will be
                 hashed for use with PostgreSQL advisory locks.
            ttl: The time-to-live for the lock in seconds. Note that PostgreSQL
                 advisory locks are session-scoped, not time-scoped via TTL.
                 The `ttl` parameter is accepted for compatibility but does not
                 strictly enforce an expiration time within PostgreSQL itself;
                 it's released when the connection holding it ends.
        """
        self.pool: Pool = pool
        self.key: str = key
        self.ttl: int = ttl  # Note: TTL is not directly enforced by pg_advisory_lock
        self._conn: asyncpg.Connection | None = None

    async def try_acquire(self) -> bool:
        """
        Attempt to acquire the advisory lock without blocking.

        PostgreSQL advisory locks are session-scoped, so the connection that
        acquires the lock must stay pinned to this lock object until release.

        Returns:
            ``True`` if the lock was acquired, otherwise ``False``.
        """
        if self._conn is not None:
            return True

        conn = await self.pool.acquire()
        try:
            result: bool | None = await conn.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", self.key)
            if result:
                self._conn = conn
                return True
        except Exception:
            await self.pool.release(conn)
            raise

        await self.pool.release(conn)
        return False

    async def acquire(self) -> bool:
        """
        Asynchronously acquires the PostgreSQL advisory lock.

        Returns:
            True if the lock was successfully acquired, False otherwise.
        """
        if self._conn is not None:
            return True

        while True:
            if await self.try_acquire():
                return True
            await anyio.sleep(0.05)

    async def release(self) -> None:
        """
        Asynchronously releases the PostgreSQL advisory lock.

        Uses `pg_advisory_unlock` to release the lock held by the current
        session for the given key.

        Args:
            None.
        """
        if self._conn is None:
            return

        conn = self._conn
        self._conn = None
        try:
            await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", self.key)
        finally:
            await self.pool.release(conn)

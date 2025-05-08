try:
    import asyncpg
except ImportError:
    raise ImportError("Please install asyncpg: `pip install asyncpg`") from None

import json
import time
from datetime import datetime
from typing import Any, cast

from asyncpg import Pool, Record

from asyncmq.backends.base import (
    BaseBackend,
    DelayedInfo,
    RepeatableInfo,
    WorkerInfo,
)
from asyncmq.conf import monkay
from asyncmq.core.enums import State
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
        if not dsn and not monkay.settings.asyncmq_postgres_backend_url:
            raise ValueError("Either 'dsn' or 'monkay.settings.asyncmq_postgres_backend_url' must be " "provided.")
        # Store the resolved DSN.
        self.dsn: str = dsn or monkay.settings.asyncmq_postgres_backend_url
        # Initialize the asyncpg connection pool to None; it will be created on connect.
        self.pool: Pool | None = None
        # Initialize the PostgresJobStore with the DSN.
        self.pool_options = pool_options or monkay.settings.asyncmq_postgres_pool_options or {}
        self.store: PostgresJobStore = PostgresJobStore(dsn=dsn, pool_options=self.pool_options)

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

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> None:
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
        same job. Jobs are selected based on creation time (`created_at ASC`).

        Args:
            queue_name: The name of the queue to dequeue a job from.

        Returns:
            The job data as a dictionary if a job was successfully dequeued,
            otherwise None.
        """
        # Ensure connection is established.
        await self.connect()
        # Acquire a connection from the pool and start a transaction for atomicity.
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Select the oldest WAITING job for the given queue, lock it,
                # update its status to ACTIVE, and return its data.
                row: Record | None = await conn.fetchrow(
                    f"""
                    UPDATE {monkay.settings.postgres_jobs_table_name}
                    SET status = $3, updated_at = now()
                    WHERE id = (
                        SELECT id FROM {monkay.settings.postgres_jobs_table_name}
                        WHERE queue_name = $1 AND status = $2
                        ORDER BY created_at ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING data
                    """,
                    queue_name,
                    State.WAITING,
                    State.ACTIVE,
                )
                # If a row was returned (a job was dequeued).
                if row:
                    # Deserialize and return the job payload.
                    return cast(dict[str, Any], json.loads(row["data"]))
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
        # Get the current timestamp.
        now: float = time.time()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # Select job data for delayed jobs that are due.
            rows: list[Record] = await conn.fetch(
                f"""
                SELECT data
                FROM {monkay.settings.postgres_jobs_table_name}
                WHERE queue_name = $1
                  AND (data ->>'delay_until') IS NOT NULL
                  AND (data ->>'delay_until')::float <= $2
                """,
                queue_name,
                now,
            )
            # Deserialize and return the job payloads.
            return [json.loads(row["data"]) for row in rows]

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
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
        # Delete the job from the job store.
        await self.store.delete(queue_name, job_id)

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
                FROM {monkay.settings.postgres_jobs_table_name}
                WHERE queue_name = $1
                  AND data ->> 'delay_until' IS NOT NULL
                ORDER BY run_at
                """,
                queue_name,
            )
        result: list[DelayedInfo] = []
        for r in rows:
            payload = json.loads(r["data"])
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
                FROM {monkay.settings.postgres_repeatables_table_name}
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

    async def remove_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Remove a repeatable definition.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                DELETE
                FROM {monkay.settings.postgres_repeatables_table_name}
                WHERE queue_name = $1
                  AND job_def = $2
                """,
                queue_name,
                json.dumps(job_def),
            )

    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Mark a repeatable as paused so the scheduler will skip it.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {monkay.settings.postgres_repeatables_table_name}
                SET paused = TRUE
                WHERE queue_name = $1
                  AND job_def = $2
                """,
                queue_name,
                json.dumps(job_def),
            )

    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> Any:
        """
        Un-pause a repeatable, recompute next_run, and return the new timestamp.
        """
        await self.connect()
        clean = {k: v for k, v in job_def.items() if k != "paused"}
        next_ts = compute_next_run(clean)
        next_dt = datetime.fromtimestamp(next_ts)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE {monkay.settings.postgres_repeatables_table_name}
                SET paused   = FALSE,
                    next_run = $1
                WHERE queue_name = $2
                  AND job_def = $3
                """,
                next_dt,
                queue_name,
                json.dumps(job_def),
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
                FROM {monkay.settings.postgres_cancelled_jobs_table_name}
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
        job_id: str = job_dict["id"]
        # Get the list of parent IDs from the input dictionary.
        dependencies: list[str] = job_dict.get("depends_on", [])
        # Load the existing job data from the job store.
        job: dict[str, Any] | None = await self.store.load(queue_name, job_id)
        # If the job was found.
        if job:
            # Add the new dependencies to the job's data. Overwrites existing list.
            job["depends_on"] = dependencies
            # Save the updated job data back to the job store.
            await self.store.save(queue_name, job_id, job)

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
                SELECT id, data FROM {monkay.settings.postgres_jobs_table_name}
                WHERE queue_name = $1 AND data->'depends_on' IS NOT NULL
                """,
                queue_name,
            )
            # Iterate through each job returned.
            for row in rows:
                # Deserialize the job data from JSONB.
                data: dict[str, Any] = json.loads(row["data"])
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
                # Enqueue all jobs by saving them with WAITING status.
                for payload in job_dicts:
                    payload["status"] = State.WAITING
                    # Use the job store's save method within the transaction.
                    await self.store.save(queue_name, payload["id"], payload)
                # Register dependencies by updating child job data.
                for parent, child in dependency_links:
                    # Load the child job data from the store.
                    job: dict[str, Any] | None = await self.store.load(queue_name, child)
                    # If the child job exists.
                    if job is not None:
                        # Get the existing dependencies or initialize an empty list.
                        deps: list[str] = job.get("depends_on", [])
                        # Add the parent ID to the dependencies list if not already present.
                        if parent not in deps:
                            deps.append(parent)
                            job["depends_on"] = deps
                            # Save the updated child job data back to the store.
                            await self.store.save(queue_name, child, job)
        # Return the list of IDs of the jobs that were initially provided.
        return [payload["id"] for payload in job_dicts]

    async def pause_queue(self, queue_name: str) -> None:
        """
        Marks the specified queue as paused.

        Note: This operation is not currently implemented for the Postgres
        backend and does nothing. Queue pausing logic must be handled externally
        or implemented here using a database flag.

        Args:
            queue_name: The name of the queue to pause.
        """
        pass  # Not implemented

    async def resume_queue(self, queue_name: str) -> None:
        """
        Resumes the specified queue.

        Note: This operation is not currently implemented for the Postgres
        backend and does nothing. Queue resuming logic must be handled externally
        or implemented here using a database flag.

        Args:
            queue_name: The name of the queue to resume.
        """
        pass  # Not implemented

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Checks if the specified queue is currently marked as paused.

        Note: This operation is not currently implemented for the Postgres
        backend and always returns False. Queue pausing logic must be handled
        externally or implemented here using a database flag.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            Always False as pausing is not implemented.
        """
        return False  # Not implemented

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
        # Ensure connection is established.
        await self.connect()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # Check if an age threshold is provided.
            if older_than:
                # Calculate the timestamp threshold.
                threshold_time: float = time.time() - older_than
                # Execute the DELETE query with queue name, status, and age filter.
                await conn.execute(
                    f"""
                    DELETE FROM {monkay.settings.postgres_jobs_table_name}
                    WHERE queue_name = $1 AND status = $2
                      AND EXTRACT(EPOCH FROM created_at) < $3
                    """,
                    queue_name,
                    state,
                    threshold_time,
                )
            else:
                # Execute the DELETE query with only queue name and status filter.
                await conn.execute(
                    f"""
                    DELETE FROM {monkay.settings.postgres_jobs_table_name}
                    WHERE queue_name = $1 AND status = $2
                    """,
                    queue_name,
                    state,
                )

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
        # Acquire a connection from the job store's pool.
        # Note: This accesses the pool via the job store, which differs from
        # other methods using `self.pool.acquire()`.
        async with self.store.pool.acquire() as conn:
            # Fetch all distinct queue names from the jobs table.
            rows: list[Record] = await conn.fetch(
                f"SELECT DISTINCT queue_name FROM {monkay.settings.postgres_jobs_table_name}"
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
        # Acquire a connection from the job store's pool.
        # Note: This accesses the pool via the job store, which differs from
        # other methods using `self.pool.acquire()`.
        async with self.store.pool.acquire() as conn:
            # Fetch the count of waiting jobs.
            waiting: int | None = await conn.fetchval(
                f"SELECT COUNT(*) FROM {monkay.settings.postgres_jobs_table_name} WHERE queue_name=$1 AND status='waiting'",
                queue_name,
            )
            # Fetch the count of delayed jobs.
            delayed: int | None = await conn.fetchval(
                f"SELECT COUNT(*) FROM {monkay.settings.postgres_jobs_table_name} WHERE queue_name=$1 AND status='delayed'",
                queue_name,
            )
            # Fetch the count of failed jobs.
            failed: int | None = await conn.fetchval(
                f"SELECT COUNT(*) FROM {monkay.settings.postgres_jobs_table_name} WHERE queue_name=$1 AND status='failed'",
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
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT data
                FROM {monkay.settings.postgres_jobs_table_name}
                WHERE queue_name = $1 AND status = $2
                """,
                queue_name,
                state,
            )
            return [json.loads(row["data"]) for row in rows]

    async def cancel_job(self, queue_name: str, job_id: str) -> bool:
        """
        Cancel a job—prevents waiting, delayed, and in-flight execution.
        Requires a `cancelled_jobs(queue_name TEXT, job_id TEXT PRIMARY KEY)` table.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                        INSERT INTO {monkay.settings.postgres_cancelled_jobs_table_name}
                          (queue_name, job_id)
                        VALUES ($1, $2)
                        ON CONFLICT DO NOTHING
                        """,
                queue_name,
                job_id,
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
        # Acquire a connection from the job store's pool.
        # Note: This accesses the pool via the job store, which differs from
        # other methods using `self.pool.acquire()`.
        await self.connect()
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                        UPDATE {monkay.settings.postgres_jobs_table_name}
                        SET status = 'waiting'
                        WHERE job_id = $1
                          AND queue_name = $2
                          AND status = 'failed'
                        """,
                job_id,
                queue_name,
            )
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
            cmd_tag = await conn.execute(
                f"""
                        DELETE FROM {monkay.settings.postgres_jobs_table_name}
                        WHERE job_id = $1
                          AND queue_name = $2
                        """,
                job_id,
                queue_name,
            )
        return cast(bool, cmd_tag.endswith("DELETE 1"))

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
                UPDATE {monkay.settings.postgres_jobs_table_name}
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
              FROM {monkay.settings.postgres_jobs_table_name}
             WHERE (data->>'heartbeat')::float < $1 -- Filter by heartbeat timestamp
               AND status = $2                      -- Filter by job status being ACTIVE
            """,
            older_than,
            State.ACTIVE,
        )

        # Process the retrieved rows. The 'data' column is returned as a JSON string.
        # It needs to be parsed into a Python dictionary.
        return [{"queue_name": row["queue_name"], "job_data": json.loads(row["data"])} for row in rows]

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
        # Reset the job status to WAITING so the enqueue method correctly processes it
        job_data["status"] = State.WAITING

        # Call the internal enqueue method to place the job back in the queue
        # Note: The `enqueue` method is assumed to exist elsewhere in this class.
        await self.enqueue(queue_name, job_data)

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
        conn = await asyncpg.connect(self.dsn)
        await conn.execute(
            f"""
                    INSERT INTO {monkay.settings.postgres_workers_heartbeat_table_name}
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
        await conn.close()

    async def deregister_worker(self, worker_id: str | int) -> None:
        """
        Removes a worker's record from the PostgreSQL backend.

        Deletes the entry for the specified worker_id from the heartbeat table.

        Args:
            worker_id: The unique identifier of the worker to deregister.
        """
        conn = await asyncpg.connect(self.dsn)
        await conn.execute(
            f"DELETE FROM {monkay.settings.postgres_workers_heartbeat_table_name} WHERE worker_id = $1", worker_id
        )
        await conn.close()

    async def list_workers(self) -> list[WorkerInfo]:
        """
        Lists active workers from the PostgreSQL backend.

        Retrieves workers whose last heartbeat is within the configured
        time-to-live (TTL) from the heartbeat table.

        Returns:
            A list of WorkerInfo objects representing the active workers.
        """
        cutoff = time.time() - monkay.settings.heartbeat_ttl
        conn = await asyncpg.connect(self.dsn)
        rows = await conn.fetch(
            f"""
                                SELECT worker_id, queues, concurrency, heartbeat
                                FROM {monkay.settings.postgres_workers_heartbeat_table_name}
                                WHERE heartbeat >= $1
                                """,
            cutoff,
        )
        await conn.close()
        return [
            WorkerInfo(
                id=r["worker_id"],
                queue=r["queues"],
                concurrency=r["concurrency"],
                heartbeat=r["heartbeat"],
            )
            for r in rows
        ]


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

    async def acquire(self) -> bool:
        """
        Asynchronously attempts to acquire the PostgreSQL advisory lock.

        Uses `pg_try_advisory_lock` which attempts to acquire the lock without
        blocking.

        Returns:
            True if the lock was successfully acquired, False otherwise.
        """
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # Attempt to acquire the advisory lock using the hash of the key.
            result: bool | None = await conn.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", self.key)
            # pg_try_advisory_lock returns boolean True/False.
            # fetchval might return None if query fails, though unlikely here.
            return result if result is not None else False

    async def release(self) -> None:
        """
        Asynchronously releases the PostgreSQL advisory lock.

        Uses `pg_advisory_unlock` to release the lock held by the current
        session for the given key.

        Args:
            None.
        """
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # Release the advisory lock using the hash of the key.
            # The return value of pg_advisory_unlock indicates success/failure,
            # but the original code doesn't check it, so we just execute.
            await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", self.key)

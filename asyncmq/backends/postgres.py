try:
    import asyncpg
except ImportError:
    raise ImportError("Please install asyncpg: `pip install asyncpg`") from None

import json
import time
from datetime import datetime
from typing import Any

# Import specific types from asyncpg for type hinting.
from asyncpg import Pool, Record

from asyncmq.backends.base import BaseBackend, DelayedInfo, RepeatableInfo
from asyncmq.conf import settings
from asyncmq.core.enums import State
from asyncmq.logging import logger
from asyncmq.schedulers import compute_next_run
from asyncmq.stores.postgres import PostgresJobStore


class PostgresBackend(BaseBackend):
    """
    Implements the AsyncMQ backend interface using a PostgreSQL database.

    This backend leverages a PostgreSQL database to persistently store and
    manage job queues, their states, delayed jobs, the Dead Letter Queue (DLQ),
    repeatable task definitions, and job dependencies. It utilizes the
    `asyncpg` library for asynchronous database interactions and relies on
    an instance of `PostgresJobStore` to handle the low-level details of
    saving and retrieving job data within the database tables.
    """

    def __init__(self, dsn: str | None = None, pool_options: Any | None = None) -> None:
        """
        Initializes the PostgresBackend instance.

        Configures the database connection details and sets up the internal
        job storage mechanism. A Database Source Name (DSN) must be provided
        either directly or through the application's settings. The `asyncpg`
        connection pool itself is created asynchronously during the `connect` call.

        Args:
            dsn: The connection string (DSN) for the PostgreSQL database. If None,
                 the DSN is retrieved from `settings.asyncmq_postgres_backend_url`.
            pool_options: A dictionary of options to pass to `asyncpg.create_pool`.
                          If None, options are taken from `settings.asyncmq_postgres_pool_options`.
        """
        # Validate that a DSN is available.
        if not dsn and not settings.asyncmq_postgres_backend_url:
            raise ValueError(
                "Either 'dsn' or 'settings.asyncmq_postgres_backend_url' must be "
                "provided."
            )
        # Determine the DSN to use.
        self.dsn: str = dsn or settings.asyncmq_postgres_backend_url
        # Initialize the connection pool to None; it will be created on demand.
        self.pool: Pool | None = None
        # Determine the connection pool options.
        self.pool_options = pool_options or settings.asyncmq_postgres_pool_options or {}
        # Initialize the associated job store.
        self.store: PostgresJobStore = PostgresJobStore(
            dsn=self.dsn, pool_options=self.pool_options
        )

    async def connect(self) -> None:
        """
        Asynchronously establishes the connection to the PostgreSQL database.

        Creates the `asyncpg` connection pool if it hasn't been created yet
        and ensures the associated job store is also connected. This method is
        designed to be idempotent.
        """
        # Create the connection pool only if it does not exist.
        if self.pool is None:
            logger.info(f"Connecting to Postgres...: {self.dsn}")
            self.pool = await asyncpg.create_pool(dsn=self.dsn, **self.pool_options)
            # Ensure the job store also establishes its connection.
            await self.store.connect()

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously enqueues a single job onto the specified queue for
        immediate processing.

        Updates the job's internal status to `State.WAITING` and persists
        the full job payload in the job store.

        Args:
            queue_name: The name of the queue to add the job to.
            payload: The job data as a dictionary. Must include an "id" key.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Update the job's status to indicate it is waiting.
        payload["status"] = State.WAITING
        # Save the complete job payload to the persistent store.
        await self.store.save(queue_name, payload["id"], payload)

    async def bulk_enqueue(self, queue_name: str, jobs: list[dict[str, Any]]) -> None:
        """
        Asynchronously enqueues multiple job payloads onto the specified queue.

        Iterates through the provided list of jobs and calls the `enqueue`
        method for each job individually. Note that this does not perform
        a single atomic database transaction for all jobs.

        Args:
            queue_name: The name of the queue to add the jobs to.
            jobs: A list of job data dictionaries to be enqueued. Each
                  dictionary must contain an "id" key.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Enqueue each job in the list individually.
        for payload in jobs:
            await self.enqueue(queue_name, payload)

    async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
        """
        Asynchronously attempts to dequeue the next available job from the
        specified queue using a transactional approach.

        Selects the oldest waiting job (`ORDER BY created_at ASC`) for the
        given queue, atomically updates its status to `State.ACTIVE`, and
        returns its data using `SELECT FOR UPDATE SKIP LOCKED` to prevent
        concurrent workers from selecting the same job.

        Args:
            queue_name: The name of the queue to dequeue a job from.

        Returns:
            The dequeued job's data as a dictionary if a job was found,
            otherwise None.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Acquire a connection from the pool and start an atomic transaction.
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # SQL query to select, lock, and update the status of the next waiting job.
                row: Record | None = await conn.fetchrow(
                    f"""
                    UPDATE {settings.postgres_jobs_table_name}
                    SET status = $3, updated_at = now()
                    WHERE id = (
                        SELECT id FROM {settings.postgres_jobs_table_name}
                        WHERE queue_name = $1 AND status = $2
                        ORDER BY created_at ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED -- Skip jobs locked by other transactions
                    )
                    RETURNING data -- Return the updated job data
                    """,
                    queue_name,  # $1: queue_name
                    State.WAITING,  # $2: current status to select
                    State.ACTIVE,  # $3: new status to set
                )
                # Check if a job was successfully updated and returned.
                if row:
                    # Deserialize the job data from the database record.
                    return json.loads(row["data"])
                # Return None if no waiting job was found.
                return None

    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously acknowledges the successful processing of a job.

        In this implementation, if the job does not have a saved result, it
        delegates the deletion of the job record to the job store. If a result
        exists, the job is not deleted by this method.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job to acknowledge.
        """
        # Ensure the database connection is active.
        await self.connect()

        # Load the job data to check if a result was saved.
        job = await self.store.load(queue_name, job_id)
        # If the job doesn't exist or has no saved result, delete it.
        if not job or job.get("result") is None:
            await self.store.delete(queue_name, job_id)

    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously moves a job to the Dead Letter Queue (DLQ).

        Updates the job's internal status to `State.FAILED` and persists
        the full job payload (with the updated status) in the job store.

        Args:
            queue_name: The name of the queue the job originated from.
            payload: The job data as a dictionary. Must include an "id" key.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Update the job's status to indicate failure.
        payload["status"] = State.FAILED
        # Save the updated job payload to the persistent store.
        await self.store.save(queue_name, payload["id"], payload)

    async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        """
        Asynchronously schedules a job for future processing by marking it as
        delayed with a target execution time.

        Updates the job's status to `State.DELAYED`, sets the `delay_until` field
        within the job's data payload to the specified timestamp, and saves the
        modified payload in the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            payload: The job data as a dictionary. Must include an "id" key.
            run_at: The absolute Unix timestamp (float) when the job should
                    become available for processing.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Update the job's status to indicate it is delayed.
        payload["status"] = State.DELAYED
        # Store the target run timestamp in the job data.
        payload["delay_until"] = run_at
        # Save the updated job payload to the persistent store.
        await self.store.save(queue_name, payload["id"], payload)

    async def get_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves a list of delayed jobs from the specified queue
        that have passed their scheduled execution time.

        Queries the database for jobs with `State.DELAYED` in the given queue
        where the `delay_until` timestamp stored in the JSONB data is less than
        or equal to the current time.

        Args:
            queue_name: The name of the queue to check for due delayed jobs.

        Returns:
            A list of dictionaries, where each dictionary is a job payload
            that is ready to be moved to the main queue.
        """
        # Get the current timestamp for comparison.
        now: float = time.time()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # SQL query to select the data of delayed jobs that are due.
            rows: list[Record] = await conn.fetch(
                f"""
                SELECT data
                FROM {settings.postgres_jobs_table_name}
                WHERE queue_name = $1
                  AND data ->> 'delay_until' IS NOT NULL -- Ensure delay_until exists
                  AND (data ->> 'delay_until')::float <= $2 -- Cast and compare timestamp
                """,
                queue_name,  # $1: queue_name
                now,  # $2: current timestamp
            )
            # Deserialize the job data from each record and return as a list.
            return [json.loads(row["data"]) for row in rows]

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously removes a specific job from the backend's storage
        by its ID, regardless of its current state.

        This method delegates the deletion operation to the job store.
        It does not specifically check if the job is in the delayed state.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Delete the job from the persistent store.
        await self.store.delete(queue_name, job_id)

    async def list_delayed(self, queue_name: str) -> list[DelayedInfo]:
        """
        Asynchronously lists all jobs currently scheduled for future execution
        in the specified queue.

        Queries the database for jobs where the `delay_until` timestamp exists
        in the JSONB data, retrieves their payload and scheduled run time, and
        returns them as a list of `DelayedInfo` objects.

        Args:
            queue_name: The name of the queue to list delayed jobs for.

        Returns:
            A list of `DelayedInfo` instances, sorted by their `run_at` timestamp.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # SQL query to select delayed job data and their scheduled run time.
            rows = await conn.fetch(
                f"""
                SELECT data,
                       (data ->> 'delay_until')::float AS run_at
                FROM {settings.postgres_jobs_table_name}
                WHERE queue_name = $1
                  AND data ->> 'delay_until' IS NOT NULL -- Filter for jobs with delay_until
                ORDER BY run_at -- Order by the scheduled run time
                """,
                queue_name,  # $1: queue_name
            )
        result: list[DelayedInfo] = []
        # Process each row returned by the query.
        for r in rows:
            # Deserialize the job payload from JSON.
            payload = json.loads(r["data"])
            # Create a DelayedInfo object and add it to the result list.
            result.append(DelayedInfo(job_id=payload["id"], run_at=r["run_at"], payload=payload))
        return result

    async def list_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        """
        Asynchronously retrieves all repeatable job definitions for a specific queue.

        Queries the `repeatables` table for entries matching the queue name,
        retrieves the job definition, next scheduled run time, and paused status,
        and returns them as a list of `RepeatableInfo` objects.

        Args:
            queue_name: The name of the queue to list repeatable jobs for.

        Returns:
            A list of `RepeatableInfo` instances, sorted by their `next_run` timestamp.
            Requires a database table defined by `settings.postgres_repeatables_table_name`
            with columns (queue_name TEXT, job_def JSONB, next_run TIMESTAMPTZ, paused BOOL).
        """
        # Ensure the database connection is active.
        await self.connect()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # SQL query to select repeatable job information.
            rows = await conn.fetch(
                f"""
                SELECT job_def, next_run, paused
                FROM {settings.postgres_repeatables_table_name}
                WHERE queue_name = $1 -- Filter by queue name
                ORDER BY next_run -- Order by the next scheduled run time
                """,
                queue_name,  # $1: queue_name
            )
        # Convert fetched rows into a list of RepeatableInfo objects.
        return [
            RepeatableInfo(
                job_def=r["job_def"],  # job_def is already parsed from JSONB by asyncpg
                next_run=r["next_run"].timestamp(),  # Convert datetime to Unix timestamp
                paused=r["paused"],  # paused status (boolean)
            )
            for r in rows
        ]

    async def remove_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Asynchronously removes a specific repeatable job definition from the database.

        Deletes the entry from the `repeatables` table that matches the queue
        name and the exact JSON structure of the job definition.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to remove.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # SQL query to delete a repeatable job definition.
            await conn.execute(
                f"""
                DELETE
                FROM {settings.postgres_repeatables_table_name}
                WHERE queue_name = $1 -- Filter by queue name
                  AND job_def = $2 -- Match the job definition JSONB
                """,
                queue_name,  # $1: queue_name
                json.dumps(job_def),  # $2: job_def as JSON string for comparison
            )

    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Asynchronously marks a specific repeatable job definition as paused in the database.

        Updates the `paused` column to TRUE for the matching entry in the
        `repeatables` table. The scheduler should check this flag and skip
        scheduling new instances of a paused repeatable job.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to pause.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # SQL query to update the paused status of a repeatable job.
            await conn.execute(
                f"""
                UPDATE {settings.postgres_repeatables_table_name}
                SET paused = TRUE -- Set the paused flag to true
                WHERE queue_name = $1 -- Filter by queue name
                  AND job_def = $2 -- Match the job definition JSONB
                """,
                queue_name,  # $1: queue_name
                json.dumps(job_def),  # $2: job_def as JSON string for comparison
            )

    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Asynchronously un-pauses a repeatable job definition, recomputes its
        next scheduled run time, and updates it in the database.

        Updates the `paused` column to FALSE for the matching entry, computes
        the next run time using `compute_next_run` based on the definition,
        and updates the `next_run` timestamp in the `repeatables` table.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to resume.
                     This dictionary should contain the necessary information
                     for `compute_next_run`.

        Returns:
            The newly computed timestamp (float) for the next run.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Create a 'clean' definition by removing the 'paused' key for computation.
        clean = {k: v for k, v in job_def.items() if k != "paused"}
        # Compute the next run timestamp using the scheduler utility.
        next_ts = compute_next_run(clean)
        # Convert the Unix timestamp to a datetime object for database storage.
        next_dt = datetime.fromtimestamp(next_ts)
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # SQL query to update the paused status and next run time.
            await conn.execute(
                f"""
                UPDATE {settings.postgres_repeatables_table_name}
                SET paused   = FALSE, -- Set the paused flag to false
                    next_run = $1     -- Update the next run timestamp
                WHERE queue_name = $2 -- Filter by queue name
                  AND job_def = $3    -- Match the original job definition JSONB
                """,
                next_dt,  # $1: new next_run timestamp as datetime
                queue_name,  # $2: queue_name
                json.dumps(job_def),  # $3: original job_def as JSON string for matching
            )
        # Return the newly computed next run timestamp.
        return next_ts

    async def cancel_job(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously marks a job as cancelled, preventing it from being
        processed if it's waiting, delayed, or currently in-flight.

        Inserts a record into the `cancelled_jobs` table with the queue name
        and job ID. If a record already exists for this job, the operation
        does nothing (`ON CONFLICT DO NOTHING`). Workers are expected to check
        this table.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to cancel.

        Requires a database table defined by `settings.postgres_cancelled_jobs_table_name`
        with columns (queue_name TEXT, job_id TEXT PRIMARY KEY).
        """
        # Ensure the database connection is active.
        await self.connect()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # SQL query to insert a record indicating the job is cancelled.
            await conn.execute(
                f"""
                INSERT INTO {settings.postgres_cancelled_jobs_table_name}(queue_name, job_id)
                VALUES ($1, $2) ON CONFLICT DO NOTHING -- Avoid errors if already cancelled
                """,
                queue_name,  # $1: queue_name
                job_id,  # $2: job_id
            )

    async def is_job_cancelled(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously checks if a specific job has been marked as cancelled.

        Queries the `cancelled_jobs` table for a record matching the queue name
        and job ID.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to check.

        Returns:
            True if a record for the job exists in the `cancelled_jobs` table,
            indicating it is cancelled; False otherwise.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # SQL query to check for the existence of a cancellation record.
            found = await conn.fetchval(
                f"""
                SELECT 1 -- Select a constant if a row is found
                FROM {settings.postgres_cancelled_jobs_table_name}
                WHERE queue_name = $1 -- Filter by queue name
                  AND job_id = $2     -- Filter by job ID
                """,
                queue_name,  # $1: queue_name
                job_id,  # $2: job_id
            )
        # Return True if a row was found (fetchval returned 1), False otherwise (fetchval returned None).
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
        # Ensure the database connection is active.
        await self.connect()
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.store.load(queue_name, job_id)
        # If the job data was successfully loaded.
        if job:
            # Update the status field in the job data dictionary.
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
        # Ensure the database connection is active.
        await self.connect()
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.store.load(queue_name, job_id)
        # If the job data was successfully loaded.
        if job:
            # Add or update the result field in the job data dictionary.
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
        # Ensure the database connection is active.
        await self.connect()
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.store.load(queue_name, job_id)
        # Return the value of the 'status' field if the job exists and has it, otherwise None.
        return job.get("status") if job else None

    async def get_job_result(self, queue_name: str, job_id: str) -> Any | None:
        """
        Asynchronously retrieves the execution result of a specific job from
        the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            The value of the 'result' field from the job's data if the job is
            found and has a result, otherwise None.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.store.load(queue_name, job_id)
        # Return the value of the 'result' field if the job exists and has it, otherwise None.
        return job.get("result") if job else None

    async def add_dependencies(self, queue_name: str, job_dict: dict[str, Any]) -> None:
        """
        Asynchronously adds a list of parent job IDs to a job's dependencies.

        Loads the job data from the store, adds the parent IDs specified in the
        input `job_dict`'s 'depends_on' key to the job's stored dependencies
        list, and saves the updated job data back to the store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: The job data dictionary containing at least an "id" key
                      and an optional "depends_on" key which is a list of parent
                      job IDs this job depends on.
        """
        # Ensure the database connection is active.
        await self.connect()
        job_id: str = job_dict["id"]
        # Get the list of dependencies from the input dictionary, default to empty list.
        dependencies: list[str] = job_dict.get("depends_on", [])
        # Load the existing job data for the specified job ID.
        job: dict[str, Any] | None = await self.store.load(queue_name, job_id)
        # If the job data was successfully loaded.
        if job:
            # Update or add the 'depends_on' field in the job data. This replaces
            # any existing dependency list.
            job["depends_on"] = dependencies
            # Save the modified job data back to the job store.
            await self.store.save(queue_name, job_id, job)

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Asynchronously signals that a parent job has completed and checks if
        any dependent child jobs are now ready to be enqueued.

        Queries the database for jobs in the queue that have dependencies.
        For each such job, it checks if the `parent_id` is in its dependencies list.
        If so, it removes the `parent_id`. If the dependencies list becomes empty
        as a result, the child job's 'depends_on' field is removed, its status is
        set to `State.WAITING`, and the updated job data is saved.

        Args:
            queue_name: The name of the queue the parent job belonged to.
            parent_id: The unique identifier of the job that just completed.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # Fetch the id and data of all jobs in the queue that have dependencies.
            rows: list[Record] = await conn.fetch(
                f"""
                SELECT id, data FROM {settings.postgres_jobs_table_name}
                WHERE queue_name = $1 AND data->'depends_on' IS NOT NULL -- Filter for jobs with dependencies
                """,
                queue_name,  # $1: queue_name
            )
            # Process each job that has dependencies.
            for row in rows:
                # Deserialize the job data from the fetched row.
                data: dict[str, Any] = json.loads(row["data"])
                # Get the current dependencies list, default to empty if not found or is null.
                depends_on: list[str] = data.get("depends_on", [])
                # Check if the completed parent is in the dependencies list.
                if parent_id in depends_on:
                    # Remove the parent ID from the dependencies list.
                    depends_on.remove(parent_id)
                    # If the dependencies list is now empty.
                    if not depends_on:
                        # Remove the 'depends_on' field and set status to WAITING.
                        data.pop("depends_on")
                        data["status"] = State.WAITING
                    else:
                        # Otherwise, update the dependencies list in the job data.
                        data["depends_on"] = depends_on
                    # Save the modified job data back to the job store.
                    await self.store.save(queue_name, data["id"], data)

    async def atomic_add_flow(
        self,
        queue_name: str,
        job_dicts: list[dict[str, Any]],
        dependency_links: list[tuple[str, str]],
    ) -> list[str]:
        """
        Atomically enqueues multiple jobs and registers their parent-child
        dependencies within a single database transaction.

        All jobs in `job_dicts` are saved with `State.WAITING`. Then, for each
        dependency link (parent, child), the child job's data is loaded, the
        parent ID is added to its `depends_on` list if not already present,
        and the child job is saved. All these operations occur within an
        `asyncpg` transaction to ensure atomicity.

        Args:
            queue_name: The name of the queue.
            job_dicts: A list of job payloads (dictionaries) to be enqueued.
                       Each dictionary must include an "id" key.
            dependency_links: A list of (parent_id, child_id) tuples specifying
                              which child job depends on which parent job.

        Returns:
            A list of the IDs (strings) of the jobs that were successfully
            processed (enqueued) as part of this flow.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Acquire a connection from the pool and start an atomic transaction.
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Enqueue all jobs provided in the list.
                for payload in job_dicts:
                    payload["status"] = State.WAITING
                    # Save each job using the job store within the transaction.
                    await self.store.save(queue_name, payload["id"], payload)
                # Register dependencies by updating the child jobs' data.
                for parent, child in dependency_links:
                    # Load the child job data from the store within the transaction.
                    job: dict[str, Any] | None = await self.store.load(queue_name, child)
                    # If the child job exists.
                    if job is not None:
                        # Get the existing dependencies, defaulting to an empty list.
                        deps: list[str] = job.get("depends_on", [])
                        # Add the parent ID to the dependencies list if it's not already there.
                        if parent not in deps:
                            deps.append(parent)
                            job["depends_on"] = deps
                            # Save the updated child job data back to the store.
                            await self.store.save(queue_name, child, job)
        # Return the IDs of the jobs that were initially provided for enqueuing.
        return [payload["id"] for payload in job_dicts]

    async def pause_queue(self, queue_name: str) -> None:
        """
        Marks the specified queue as paused.

        Note: This operation is not implemented in the current Postgres backend
        and performs no action. Queue pausing functionality would require
        adding a specific flag or mechanism in the database schema or
        configuration accessible to workers.

        Args:
            queue_name: The name of the queue to pause.
        """
        pass  # Not implemented

    async def resume_queue(self, queue_name: str) -> None:
        """
        Resumes the specified queue if it was paused.

        Note: This operation is not implemented in the current Postgres backend
        and performs no action. Queue resuming functionality would require
        complementary logic to the pausing mechanism.

        Args:
            queue_name: The name of the queue to resume.
        """
        pass  # Not implemented

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Checks if the specified queue is currently marked as paused.

        Note: This operation is not implemented in the current Postgres backend
        and always returns False. The implementation would require checking the
        state or presence of a pausing indicator in the database or configuration.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            Always False as pausing is not implemented in this backend.
        """
        return False  # Not implemented

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Asynchronously saves the progress percentage for a specific job in the
        job store.

        Loads the job data from the job store, adds or updates the 'progress'
        field within the job's JSONB data with the provided value, and saves
        the modified data back to the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            progress: The progress value, typically a float between 0.0 and 1.0
                      or an integer representing a percentage (0-100).
        """
        # Ensure the database connection is active.
        await self.connect()
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.store.load(queue_name, job_id)
        # If the job data was successfully loaded.
        if job:
            # Add or update the 'progress' field in the job data dictionary.
            job["progress"] = progress
            # Save the modified job data back to the job store.
            await self.store.save(queue_name, job_id, job)

    async def purge(self, queue_name: str, state: str, older_than: float | None = None) -> None:
        """
        Asynchronously removes jobs from a queue based on their state and
        optionally, their age.

        Deletes records from the jobs table where the queue name and status
        match the criteria. If `older_than` (a duration in seconds) is provided,
        it additionally filters for jobs whose `created_at` timestamp is older
        than the current time minus this duration.

        Args:
            queue_name: The name of the queue from which to purge jobs.
            state: The state string of the jobs to be removed (e.g., "completed",
                   "failed", "waiting").
            older_than: An optional duration in seconds. If provided, only jobs
                        in the specified `state` that were created more than
                        `older_than` seconds ago will be removed. If None, all
                        jobs in the specified state will be removed.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # Check if an age threshold is specified.
            if older_than:
                # Calculate the timestamp threshold for purging.
                threshold_time: float = time.time() - older_than
                # SQL query to delete jobs filtered by queue, status, and creation time.
                await conn.execute(
                    f"""
                    DELETE FROM {settings.postgres_jobs_table_name}
                    WHERE queue_name = $1 AND status = $2
                      AND EXTRACT(EPOCH FROM created_at) < $3 -- Filter by creation time (Unix epoch)
                    """,
                    queue_name,  # $1: queue_name
                    state,  # $2: job status
                    threshold_time,  # $3: timestamp threshold
                )
            else:
                # SQL query to delete jobs filtered by queue and status only.
                await conn.execute(
                    f"""
                    DELETE FROM {settings.postgres_jobs_table_name}
                    WHERE queue_name = $1 AND status = $2
                    """,
                    queue_name,  # $1: queue_name
                    state,  # $2: job status
                )

    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        """
        Emits an event.

        Note: This operation is not implemented in the current Postgres backend
        using database features (like `LISTEN`/`NOTIFY`) for event distribution
        and performs no action. Event handling would need to be managed externally.

        Args:
            event: The name of the event to emit.
            data: The data associated with the event.
        """
        pass  # Could potentially implement using PostgreSQL LISTEN/NOTIFY

    async def create_lock(self, key: str, ttl: int) -> Any:
        """
        Creates and returns a database-based distributed lock instance using
        PostgreSQL advisory locks.

        Utilizes `pg_try_advisory_lock` for non-blocking acquisition and
        `pg_advisory_unlock` for release. Locks are identified by a hashed
        integer derived from the input string `key`. Note that PostgreSQL
        advisory locks are session-scoped and are automatically released when
        the connection holding them is closed, regardless of the `ttl` parameter.

        Args:
            key: A unique string identifier for the lock. This string is hashed
                 to produce the integer identifier for the PostgreSQL advisory lock.
            ttl: The intended time-to-live for the lock in seconds. This parameter
                 is accepted for compatibility with other backend lock interfaces
                 but is not directly enforced by the PostgreSQL advisory lock
                 mechanism itself; the lock's lifetime is tied to the database
                 session holding it.

        Returns:
            A `PostgresLock` instance. The caller is responsible for acquiring
            (`await lock.acquire()`) and releasing (`await lock.release()`) this lock.
            The return type is `Any` as specified in the base backend.
        """
        # Ensure the database connection is active.
        await self.connect()
        # Return a new instance of the PostgresLock class.
        return PostgresLock(self.pool, key, ttl)

    async def close(self) -> None:
        """
        Asynchronously closes the database connection pool and disconnects
        the associated job store.

        This method should be called to release database resources when the
        backend instance is no longer needed.
        """
        # Close the connection pool if it is active.
        if self.pool:
            await self.pool.close()
            self.pool = None
        # Disconnect the associated job store.
        await self.store.disconnect()

    async def list_queues(self) -> list[str]:
        """
        Asynchronously lists all known queue names managed by this backend.

        Queries the jobs table for distinct `queue_name` values to identify
        all queues that have had jobs stored in them.

        Returns:
            A list of strings, where each string is the name of a queue.
        """
        # Acquire a connection from the job store's connection pool.
        async with self.store.pool.acquire() as conn:
            # SQL query to select all distinct queue names from the jobs table.
            rows: list[Record] = await conn.fetch(
                f"SELECT DISTINCT queue_name FROM {settings.postgres_jobs_table_name}"
            )
            # Extract the queue names from the fetched records and return as a list.
            return [row["queue_name"] for row in rows]

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        """
        Asynchronously returns statistics about the number of jobs in different
        states (waiting, delayed, failed/DLQ) for a specific queue.

        Performs COUNT queries on the jobs table, filtering by queue name and
        status to get the counts for waiting, delayed, and failed jobs.

        Args:
            queue_name: The name of the queue to retrieve statistics for.

        Returns:
            A dictionary containing the counts for "waiting", "delayed", and
            "failed" jobs. If no jobs are found for a state, the count will be 0.
        """
        # Acquire a connection from the job store's connection pool.
        async with self.store.pool.acquire() as conn:
            # Fetch the count of jobs with status 'waiting'.
            waiting: int | None = await conn.fetchval(
                f"SELECT COUNT(*) FROM {settings.postgres_jobs_table_name} WHERE queue_name=$1 AND status='waiting'",
                queue_name,
            )
            # Fetch the count of jobs with status 'delayed'.
            delayed: int | None = await conn.fetchval(
                f"SELECT COUNT(*) FROM {settings.postgres_jobs_table_name} WHERE queue_name=$1 AND status='delayed'",
                queue_name,
            )
            # Fetch the count of jobs with status 'failed'.
            failed: int | None = await conn.fetchval(
                f"SELECT COUNT(*) FROM {settings.postgres_jobs_table_name} WHERE queue_name=$1 AND status='failed'",
                queue_name,
            )
            # Return a dictionary with the counts, using 0 if fetchval returned None.
            return {
                "waiting": waiting or 0,
                "delayed": delayed or 0,
                "failed": failed or 0,
            }

    async def list_jobs(self, queue_name: str, state: str) -> list[dict[str, Any]]:
        """
        Asynchronously lists jobs in a specific queue, filtered by their current state.

        Queries the jobs table for records matching the queue name and specified
        status, and returns their JSONB data as a list of Python dictionaries.

        Args:
            queue_name: The name of the queue to list jobs from.
            state: The job status to filter by (e.g., "waiting", "active",
                   "completed", "failed", "delayed").

        Returns:
            A list of job payload dictionaries for jobs in the specified state
            within the given queue.
        """
        # Acquire a connection from the job store's connection pool.
        async with self.store.pool.acquire() as conn:
            # SQL query to select the job data for jobs matching the queue and status.
            rows = await conn.fetch(
                f"""
                SELECT data
                FROM {settings.postgres_jobs_table_name}
                WHERE queue_name = $1 AND status = $2 -- Filter by queue and status
                """,
                queue_name,  # $1: queue_name
                state,  # $2: job status
            )
            # Deserialize the JSONB data from each row and return as a list.
            return [json.loads(row["data"]) for row in rows]

    async def retry_job(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously retries a failed job by updating its status in the database.

        Sets the `status` of a job matching the given ID and queue name from
        `State.FAILED` to `State.WAITING`.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the failed job to retry.

        Returns:
            True if a job with the given ID and queue name was found in the
            'failed' state and successfully updated to 'waiting'; False otherwise.
        """
        # Acquire a connection from the job store's connection pool.
        async with self.store.pool.acquire() as conn:
            # SQL UPDATE query to change the status of a failed job to waiting.
            updated_command_tag: str = await conn.execute(
                f"UPDATE {settings.postgres_jobs_table_name} SET status='waiting' WHERE id=$1 AND queue_name=$2 AND status='failed'",
                job_id,  # $1: job_id
                queue_name,  # $2: queue_name
            )
            # Check the command tag returned by execute to see if exactly one row was updated.
            return updated_command_tag.endswith("UPDATE 1")

    async def remove_job(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously removes a specific job record from the database.

        Deletes the row from the jobs table that matches the given job ID and
        queue name, regardless of the job's current status.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove.

        Returns:
            True if a job with the given ID and queue name was found and
            successfully deleted; False otherwise.
        """
        # Acquire a connection from the job store's connection pool.
        async with self.store.pool.acquire() as conn:
            # SQL DELETE query to remove a specific job by ID and queue name.
            deleted_command_tag: str = await conn.execute(
                f"DELETE FROM {settings.postgres_jobs_table_name} WHERE id=$1 AND queue_name=$2",
                job_id,  # $1: job_id
                queue_name,  # $2: queue_name
            )
            # Check the command tag returned by execute to see if exactly one row was deleted.
            return deleted_command_tag.endswith("DELETE 1")

    async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
        """
        Asynchronously records the last heartbeat timestamp for a specific job.

        Updates the `data` JSONB column of the job's row in the database by
        setting or updating the 'heartbeat' key with the provided timestamp
        and also updates the `updated_at` column to the current time.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            timestamp: The Unix timestamp (float) of the heartbeat.
        """
        # Ensure the database connection is active.
        await self.connect()

        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # SQL UPDATE statement to update the 'heartbeat' field in the JSONB data and updated_at timestamp.
            await conn.execute(
                f"""
                UPDATE {settings.postgres_jobs_table_name}
                   SET data = jsonb_set( -- Update the data JSONB column
                               data, -- The original JSONB data
                               '{{heartbeat}}', -- The path to the key ('heartbeat')
                               to_jsonb($3::double precision), -- The new value (timestamp as JSONB double)
                               true -- Create the path if it doesn't exist
                             ),
                       updated_at = now() -- Update the updated_at timestamp
                 WHERE queue_name = $1 -- Filter by queue name
                   AND id     = $2      -- Filter by job ID
                """,
                queue_name,  # $1: queue_name
                job_id,  # $2: job_id
                timestamp,  # $3: timestamp for the heartbeat
            )

    async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves jobs currently marked as ACTIVE that have not
        had a heartbeat recorded since a specified timestamp.

        Queries the database for jobs where the 'heartbeat' key within the
        `data` JSONB column is less than the `older_than` timestamp and the
        job's `status` is `State.ACTIVE`.

        Args:
            older_than: A Unix timestamp. Jobs with a heartbeat timestamp
                        strictly less than this value are considered stalled.

        Returns:
            A list of dictionaries. Each dictionary contains 'queue_name' and
            'job_data' (the full parsed JSON payload from the database).
        """
        # Ensure the database connection is active.
        await self.connect()

        # Acquire a connection from the pool.
        async with self.pool.acquire() as conn:
            # SQL query to select stalled jobs based on heartbeat timestamp and status.
            rows = await conn.fetch(
                f"""
            SELECT queue_name, data
              FROM {settings.postgres_jobs_table_name}
             WHERE (data->>'heartbeat')::float < $1 -- Filter by heartbeat timestamp (casted to float)
               AND status = $2                      -- Filter by job status (must be ACTIVE)
            """,
                older_than,  # $1: the timestamp threshold for stalled jobs
                State.ACTIVE,  # $2: the required status for a job to be considered stalled
            )

        # Process the retrieved rows: extract queue name and deserialize the JSONB data.
        return [{"queue_name": row["queue_name"], "job_data": json.loads(row["data"])} for row in rows]

    async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
        """
        Asynchronously re-enqueues a job that was previously identified as stalled.

        This method prepares the job data for re-enqueueing by resetting its
        status to `State.WAITING` and then calls the internal `enqueue` method
        to place it back into the processing queue.

        Args:
            queue_name: The name of the queue the job should be re-enqueued into.
            job_data: The dictionary containing the job's data, including its current
                      state and payload. This dictionary's 'status' field is
                      modified in-place before being passed to `enqueue`.
        """
        # Update the job's status to WAITING before re-enqueueing.
        job_data["status"] = State.WAITING

        # Use the existing enqueue method to add the job back to the queue.
        await self.enqueue(queue_name, job_data)


class PostgresLock:
    """
    A distributed lock implementation utilizing PostgreSQL advisory locks.

    This class provides a context manager for acquiring and releasing locks
    identified by a string key, which is hashed to an integer for use with
    PostgreSQL's advisory lock functions (`pg_try_advisory_lock` and
    `pg_advisory_unlock`). These locks are session-scoped, meaning they are
    automatically released when the database connection holding them is closed.
    The `ttl` parameter is accepted but not strictly enforced by the PostgreSQL
    lock mechanism itself.
    """

    def __init__(self, pool: Pool, key: str, ttl: int) -> None:
        """
        Initializes the PostgresLock instance.

        Args:
            pool: The `asyncpg` connection pool to use for database interactions
                  needed to acquire and release the lock.
            key: A unique string identifier for this lock. This string is used
                 to generate the integer identifier for the PostgreSQL advisory lock.
            ttl: The intended time-to-live (in seconds) for the lock. Note that
                 PostgreSQL advisory locks are session-scoped, not time-bound,
                 so this parameter does not enforce an automatic expiration.
                 It is included for compatibility with other lock implementations.
        """
        self.pool: Pool = pool
        self.key: str = key
        self.ttl: int = ttl  # Note: TTL is not directly enforced by pg_advisory_lock

    async def acquire(self) -> bool:
        """
        Asynchronously attempts to acquire the PostgreSQL advisory lock.

        Uses `pg_try_advisory_lock` which attempts to obtain the lock without
        blocking. If the lock is already held by another session, this function
        returns False immediately.

        Returns:
            True if the lock was successfully acquired by the current session;
            False otherwise.
        """
        # Acquire a connection from the pool to perform the lock operation.
        async with self.pool.acquire() as conn:
            # Execute the pg_try_advisory_lock function using the hash of the key.
            result: bool | None = await conn.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", self.key)
            # pg_try_advisory_lock returns a boolean. fetchval might return None on error.
            # Return the boolean result, defaulting to False if None.
            return result if result is not None else False

    async def release(self) -> None:
        """
        Asynchronously releases the PostgreSQL advisory lock held by the current session.

        Uses `pg_advisory_unlock` to release the lock identified by the hashed
        key. Note that advisory locks are also automatically released when the
        session ends.

        Args:
            None.
        """
        # Acquire a connection from the pool to perform the unlock operation.
        async with self.pool.acquire() as conn:
            # Execute the pg_advisory_unlock function using the hash of the key.
            # The return value of pg_advisory_unlock indicates success/failure,
            # but is not checked by the original code.
            await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", self.key)

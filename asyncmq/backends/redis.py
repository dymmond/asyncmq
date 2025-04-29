import json
import time
from typing import Any, Set

import redis.asyncio as redis
from redis.commands.core import AsyncScript

from asyncmq.backends.base import BaseBackend
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.stores.redis_store import RedisJobStore

# Lua script used to atomically retrieve and remove the highest priority job
# (first element in the sorted set, i.e., score 0) from a Redis Sorted Set.
# This prevents race conditions when multiple workers try to dequeue jobs simultaneously.
# KEYS[1] is the key of the Redis Sorted Set (e.g., 'queue:{queue_name}:waiting').
# The script returns the job payload (as a JSON string) or nil if the set is empty.
POP_SCRIPT: str = """
local items = redis.call('ZRANGE', KEYS[1], 0, 0)
if #items == 0 then
  return nil
end
redis.call('ZREM', KEYS[1], items[1])
return items[1]
"""

FLOW_SCRIPT: str = r"""
-- ARGV: num_jobs, <job1_json>,...,<jobN_json>, num_deps, <parent1>,<child1>,...,<parentM>,<childM>
local num_jobs = tonumber(ARGV[1])
local idx = 2
local result = {}
for i=1,num_jobs do
  local job_json = ARGV[idx]
  idx = idx + 1
  redis.call('ZADD', KEYS[1], 0, job_json)
  local job = cjson.decode(job_json)
  table.insert(result, job.id)
end
local num_deps = tonumber(ARGV[idx]); idx = idx + 1
for i=1,num_deps do
  local parent = ARGV[idx]; local child = ARGV[idx+1]
  idx = idx + 2
  redis.call('HSET', KEYS[2] .. ':' .. parent, child, 1)
end
return result
"""


class RedisBackend(BaseBackend):
    """
    A Redis-based implementation of the asyncmq backend interface.

    This backend uses various Redis data structures (Sorted Sets, Hashes, Sets,
    Keys) to manage queues, job states, delayed jobs, DLQs, repeatable tasks,
    dependencies, queue pause/resume status, progress, bulk operations,
    purging, events (via Pub/Sub), and distributed locking. It relies on a
    separate `RedisJobStore` for persistent storage of full job data.
    """

    def __init__(self, redis_url: str = "redis://localhost") -> None:
        """
        Initializes the RedisBackend by connecting to Redis and preparing the
        necessary components.

        Establishes an asynchronous connection to Redis and initializes a
        `RedisJobStore` for persistent job data storage. It also registers
        the `POP_SCRIPT` Lua script with Redis for atomic dequeue operations.

        Args:
            redis_url: The connection URL for the Redis instance. Defaults to
                       "redis://localhost".
        """
        # Connect to the Redis instance.
        self.redis: redis.Redis = redis.from_url(redis_url, decode_responses=True)
        # Initialize the RedisJobStore for job data persistence.
        self.job_store: RedisJobStore = RedisJobStore(redis_url)
        # Register the Lua POP_SCRIPT for atomic dequeue operations.
        self.pop_script: AsyncScript = self.redis.register_script(POP_SCRIPT)
        self.flow_script: AsyncScript = self.redis.register_script(FLOW_SCRIPT)

    def _waiting_key(self, name: str) -> str:
        """Generates the Redis key for the Sorted Set holding jobs waiting in a queue."""
        # Key format: 'queue:{queue_name}:waiting'
        return f"queue:{name}:waiting"

    def _active_key(self, name: str) -> str:
        """Generates the Redis key for the Hash holding jobs currently being processed."""
        # Key format: 'queue:{queue_name}:active'
        return f"queue:{name}:active"

    def _delayed_key(self, name: str) -> str:
        """Generates the Redis key for the Sorted Set holding delayed jobs."""
        # Key format: 'queue:{queue_name}:delayed'
        return f"queue:{name}:delayed"

    def _dlq_key(self, name: str) -> str:
        """Generates the Redis key for the Sorted Set holding jobs in the Dead Letter Queue."""
        # Key format: 'queue:{queue_name}:dlq'
        return f"queue:{name}:dlq"

    def _repeat_key(self, name: str) -> str:
        """Generates the Redis key for the Sorted Set holding repeatable job definitions."""
        # Key format: 'queue:{queue_name}:repeatables'
        return f"queue:{name}:repeatables"

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously enqueues a job payload onto the specified queue for
        immediate processing.

        Jobs are stored in a Redis Sorted Set (`queue:{queue_name}:waiting`)
        where the score is calculated based on priority and enqueue timestamp
        to ensure priority-based ordering (lower score is higher priority).
        The full job payload is also saved in the job store, and its state
        is updated to State.WAITING.

        Args:
            queue_name: The name of the queue to enqueue the job onto.
            payload: The job data as a dictionary, expected to contain at least
                     an "id" and optionally a "priority" key.
        """
        # Get job priority, defaulting to 5 if not provided.
        priority: int = payload.get("priority", 5)
        # Calculate the score for the Sorted Set: lower priority + earlier time = higher priority.
        # Using 1e16 ensures priority dominates over timestamp for sorting.
        score: float = priority * 1e16 + time.time()
        # Get the Redis key for the waiting queue.
        key: str = self._waiting_key(queue_name)
        # Add the JSON-serialized payload to the Sorted Set with the calculated score.
        await self.redis.zadd(key, {json.dumps(payload): score})
        # Update the job's status to WAITING and save the full payload in the job store.
        payload["status"] = State.WAITING
        await self.job_store.save(queue_name, payload["id"], payload)

    async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
        """
        Asynchronously attempts to dequeue the next job from the specified queue.

        Uses a Lua script (`POP_SCRIPT`) to atomically retrieve and remove the
        highest priority job (the one with the lowest score) from the waiting
        queue's Sorted Set. If a job is dequeued, its state is updated to
        State.ACTIVE in the job store and it's added to an State.ACTIVE Redis
        Hash keyed by job ID, along with the timestamp of when it became active.

        Args:
            queue_name: The name of the queue to dequeue a job from.

        Returns:
            The job data as a dictionary if a job was successfully dequeued,
            otherwise None.
        """
        # Get the Redis key for the waiting queue.
        key: str = self._waiting_key(queue_name)
        # Execute the Lua script to atomically pop the next job.
        raw: str | None = await self.pop_script(keys=[key])
        # If a job was returned by the script.
        if raw:
            # Deserialize the job payload from JSON.
            payload: dict[str, Any] = json.loads(raw)
            # Update the job's status to ACTIVE and save it in the job store.
            payload["status"] = State.ACTIVE
            await self.job_store.save(queue_name, payload["id"], payload)
            # Add the job ID and the current time to the active jobs Hash.
            await self.redis.hset(self._active_key(queue_name), payload["id"], time.time())
            # Return the dequeued job payload.
            return payload
        # If the script returned nil (no jobs in the waiting queue).
        return None

    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously moves a job payload to the Dead Letter Queue (DLQ)
        associated with the specified queue.

        Jobs are stored in a Redis Sorted Set (`queue:{queue_name}:dlq`)
        scored by the current timestamp. The job's state is updated to State.FAILED
        in the job store.

        Args:
            queue_name: The name of the queue the job originated from.
            payload: The job data as a dictionary, expected to contain at least
                     an "id" key.
        """
        # Get the Redis key for the DLQ.
        key: str = self._dlq_key(queue_name)
        # Create a new payload dict with status set to FAILED.
        dlq_payload: dict[str, Any] = {**payload, "status": State.FAILED}
        # Add the JSON-serialized DLQ payload to the DLQ Sorted Set, scored by current time.
        await self.redis.zadd(key, {json.dumps(dlq_payload): time.time()})
        # Update the job's status to FAILED and save it in the job store.
        await self.job_store.save(queue_name, payload["id"], dlq_payload)

    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously acknowledges the successful processing of a job.

        Removes the job from the State.ACTIVE Redis Hash, indicating it is
        no longer being actively processed by this worker. The job's state
        should be updated in the job store separately (e.g., to COMPLETED).

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job being acknowledged.
        """
        # Remove the job ID from the active jobs Hash.
        await self.redis.hdel(self._active_key(queue_name), job_id)

    async def enqueue_delayed(
        self, queue_name: str, payload: dict[str, Any], run_at: float
    ) -> None:
        """
        Asynchronously schedules a job to be available for processing at a
        specific future time by adding it to a Redis Sorted Set for delayed jobs.

        Jobs are stored in a Redis Sorted Set (`queue:{queue_name}:delayed`)
        scored by the `run_at` timestamp. The job's state is updated to State.EXPIRED
        in the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            payload: The job data as a dictionary, expected to contain at least
                     an "id" key.
            run_at: The absolute timestamp (e.g., from time.time()) when the
                    job should become available for processing.
        """
        # Get the Redis key for the delayed queue.
        key: str = self._delayed_key(queue_name)
        # Add the JSON-serialized payload to the delayed Sorted Set, scored by run_at time.
        await self.redis.zadd(key, {json.dumps(payload): run_at})
        # Update the job's status to EXPIRED (or DELAYED) and save it in the job store.
        # NOTE: Original code sets status to EXPIRED here. Consider changing to DELAYED.
        payload["status"] = State.EXPIRED
        await self.job_store.save(queue_name, payload["id"], payload)

    async def get_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves a list of delayed job payloads from the
        specified queue that are now due for processing.

        Queries the Redis Sorted Set for delayed jobs (`queue:{queue_name}:delayed`)
        to find items with a score (run_at timestamp) less than or equal to the
        current time.

        Args:
            queue_name: The name of the queue to check for due delayed jobs.

        Returns:
            A list of dictionaries, where each dictionary is a job payload
            that is ready to be moved to the main queue.
        """
        # Get the Redis key for the delayed queue.
        key: str = self._delayed_key(queue_name)
        # Get the current timestamp.
        now: float = time.time()
        # Retrieve all jobs from the delayed set with a score <= now.
        raw_jobs: list[str] = await self.redis.zrangebyscore(key, 0, now)
        # Deserialize the JSON strings back into dictionaries.
        return [json.loads(raw) for raw in raw_jobs]

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously removes a specific job from the backend's delayed storage
        by its ID.

        This involves iterating through the delayed jobs' Sorted Set to find
        the job by its ID within the payload and then removing it. This is
        potentially inefficient for large delayed sets and should ideally be
        handled with more efficient data structures or a different storage approach
        if performance is critical for frequent removal.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove from delayed
                    storage.
        """
        # Get the Redis key for the delayed queue.
        key: str = self._delayed_key(queue_name)
        # Retrieve all members of the delayed set.
        all_jobs_raw: list[str] = await self.redis.zrange(key, 0, -1)
        # Iterate through the raw job payloads to find the one matching the job_id.
        for raw in all_jobs_raw:
            job: dict[str, Any] = json.loads(raw)
            # If the job ID matches.
            if job.get("id") == job_id:
                # Remove the specific member (the JSON string) from the set.
                await self.redis.zrem(key, raw)
                # Stop iterating once found and removed.
                break

    async def update_job_state(
        self, queue_name: str, job_id: str, state: str
    ) -> None:
        """
        Asynchronously updates the status of a specific job in the job store.

        Loads the job data from the job store using the job_id, updates the
        'status' field with the new state string, and saves it back to the
        job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            state: The new state string for the job (e.g., "active", "completed").
        """
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
        # If the job data was successfully loaded.
        if job:
            # Update the status field.
            job["status"] = state
            # Save the updated job data back to the job store.
            await self.job_store.save(queue_name, job_id, job)

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        """
        Asynchronously saves the result of a job's execution in the job store.

        Loads the job data from the job store using the job_id, adds/updates
        the 'result' field with the execution result, and saves the modified
        job data back to the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            result: The result returned by the job's task function. Can be of
                    any type that is JSON serializable by the job store.
        """
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
        # If the job data was successfully loaded.
        if job:
            # Add or update the result field.
            job["result"] = result
            # Save the updated job data back to the job store.
            await self.job_store.save(queue_name, job_id, job)

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
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
        # Return the status field if the job is found, otherwise None.
        return job.get("status") if job else None

    async def get_job_result(self, queue_name: str, job_id: str) -> Any | None:
        """
        Asynchronously retrieves the execution result of a specific job from
        the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            The result of the job's task function if the job is found and has
            a 'result' field, otherwise None.
        """
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
        # Return the result field if the job is found, otherwise None.
        return job.get("result") if job else None

    async def add_repeatable(
        self, queue_name: str, job_def: dict[str, Any], next_run: float
    ) -> None:
        """
        Asynchronously adds or updates a repeatable job definition in Redis.

        Repeatable job definitions are stored in a Redis Sorted Set
        (`queue:{queue_name}:repeatables`) scored by their `next_run` timestamp.
        This allows the scheduler to efficiently query for jobs that are due.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job. This dictionary
                     should contain all necessary information to recreate the job
                     instance for future runs.
            next_run: The absolute timestamp (e.g., from time.time()) when the
                      next instance of this repeatable job should be enqueued.
        """
        # Get the Redis key for the repeatable jobs set.
        key: str = self._repeat_key(queue_name)
        # Serialize the job definition dictionary to a JSON string.
        payload: str = json.dumps(job_def)
        # Add the JSON-serialized definition to the repeatable jobs Sorted Set,
        # scored by the next_run timestamp. ZADD updates the score if the member exists.
        await self.redis.zadd(key, {payload: next_run})

    async def remove_repeatable(
        self, queue_name: str, job_def: dict[str, Any]
    ) -> None:
        """
        Asynchronously removes a repeatable job definition from Redis.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to remove.
        """
        # Get the Redis key for the repeatable jobs set.
        key: str = self._repeat_key(queue_name)
        # Serialize the job definition dictionary to a JSON string.
        payload: str = json.dumps(job_def)
        # Remove the specific member (the JSON string) from the set.
        await self.redis.zrem(key, payload)

    async def get_due_repeatables(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves repeatable job definitions from Redis that
        are now due for enqueuing.

        Queries the Redis Sorted Set for repeatable jobs (`queue:{queue_name}:repeatables`)
        to find items with a score (next_run timestamp) less than or equal to the
        current time.

        Args:
            queue_name: The name of the queue to check for due repeatable jobs.

        Returns:
            A list of dictionaries, where each dictionary is a repeatable job
            definition that is ready to be enqueued.
        """
        # Get the Redis key for the repeatable jobs set.
        key: str = self._repeat_key(queue_name)
        # Get the current timestamp.
        now: float = time.time()
        # Retrieve all members from the repeatable set with a score <= now.
        raw: list[str] = await self.redis.zrangebyscore(key, 0, now)
        # Deserialize the JSON strings back into dictionaries.
        return [json.loads(item) for item in raw]

    async def add_dependencies(
        self, queue_name: str, job_dict: dict[str, Any]
    ) -> None:
        """
        Asynchronously registers a job's dependencies and the relationship
        between parent and child jobs in Redis using Sets.

        Uses a Set (`deps:{queue_name}:{job_id}:pending`) to store the IDs
        of parent jobs a child job is waiting on. Uses a Set
        (`deps:{queue_name}:parent:{parent_id}`) to store the IDs of child jobs
        waiting on a parent.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: The job data dictionary, expected to contain at least
                      an "id" key and an optional "depends_on" list of parent job IDs.
        """
        # Get the ID of the child job.
        job_id: str = job_dict["id"]
        # Get the list of parent job IDs this child depends on.
        pending: list[str] = job_dict.get("depends_on", [])

        # If there are dependencies.
        if pending:
            # Key for the set of parent IDs this job is waiting on.
            pend_key: str = f"deps:{queue_name}:{job_id}:pending"
            # Add all parent IDs to this set.
            await self.redis.sadd(pend_key, *pending)

            # For each parent job ID in the dependencies.
            for parent in pending:
                # Key for the set of child IDs waiting on this parent.
                parent_children_key: str = f"deps:{queue_name}:parent:{parent}"
                # Add the child job ID to this parent's children set.
                await self.redis.sadd(parent_children_key, job_id)

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Asynchronously signals that a parent job has completed and checks if
        any dependent child jobs are now ready to be enqueued.

        Removes the `parent_id` from the pending dependencies Set of all its
        children. If a child job's pending dependencies Set becomes empty,
        it means all its dependencies are met, and the child job is loaded
        from the job store and enqueued. Cleans up the pending dependencies Set
        for the child and the children Set for the parent.

        Args:
            queue_name: The name of the queue the parent job belonged to.
            parent_id: The unique identifier of the job that just completed.
        """
        # Key for the set of child IDs waiting on this parent.
        child_set_key: str = f"deps:{queue_name}:parent:{parent_id}"
        # Get all child IDs from this set.
        children: Set[str] = await self.redis.smembers(child_set_key)

        # Iterate through each child job ID that was waiting on the parent.
        for child_id in children:
            # Key for the set of parent IDs this child is waiting on.
            pend_key: str = f"deps:{queue_name}:{child_id}:pending"
            # Remove the completed parent's ID from the child's pending set.
            await self.redis.srem(pend_key, parent_id)
            # Check the number of remaining pending dependencies for the child.
            rem: int = await self.redis.scard(pend_key)

            # If there are no remaining pending dependencies.
            if rem == 0:
                # Load the child job data from the job store.
                data: dict[str, Any] | None = await self.job_store.load(
                    queue_name, child_id
                )
                # If the job data was successfully loaded.
                if data:
                    # Enqueue the child job into the main queue.
                    await self.enqueue(queue_name, data)
                    # Emit a local event indicating the job is now ready.
                    await event_emitter.emit("job:ready", {"id": child_id})
                # Delete the child's empty pending dependencies set.
                await self.redis.delete(pend_key)

        # Delete the parent's children set as all children have been processed or checked.
        await self.redis.delete(child_set_key)

    async def pause_queue(self, queue_name: str) -> None:
        """
        Marks the specified queue as paused in Redis by setting a key.

        Workers should check for the existence of this key to determine if
        they should stop dequeuing jobs from this queue.

        Args:
            queue_name: The name of the queue to pause.
        """
        # Set a Redis key to indicate the queue is paused.
        await self.redis.set(f"queue:{queue_name}:paused", 1)

    async def resume_queue(self, queue_name: str) -> None:
        """
        Removes the key indicating that the specified queue is paused in Redis.

        After this, workers should be able to resume dequeuing jobs from this queue.

        Args:
            queue_name: The name of the queue to resume.
        """
        # Delete the Redis key that indicates the queue is paused.
        await self.redis.delete(f"queue:{queue_name}:paused")

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Checks if the specified queue is currently marked as paused in Redis.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the pause key exists for the queue, False otherwise.
        """
        # Check if the pause key exists in Redis.
        return bool(await self.redis.exists(f"queue:{queue_name}:paused"))

    async def save_job_progress(
        self, queue_name: str, job_id: str, progress: float
    ) -> None:
        """
        Asynchronously saves the progress percentage for a specific job in the
        job store.

        Loads the job data from the job store, adds/updates the 'progress'
        field, and saves it back. This allows external monitoring of job progress.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            progress: The progress value, typically a float between 0.0 and 1.0.
        """
        # Load the job data from the job store.
        data: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
        # If the job data was successfully loaded.
        if data:
            # Add or update the progress field.
            data["progress"] = progress
            # Save the updated job data back to the job store.
            await self.job_store.save(queue_name, job_id, data)

    async def bulk_enqueue(
        self, queue_name: str, jobs: list[dict[str, Any]]
    ) -> None:
        """
        Asynchronously enqueues multiple job payloads onto the specified queue
        in a single batch operation using a Redis Pipeline.

        Each job is added to the waiting queue's Sorted Set and saved in the
        job store.

        Args:
            queue_name: The name of the queue to enqueue jobs onto.
            jobs: A list of job payloads (dictionaries) to be enqueued. Each
                  dictionary is expected to contain at least an "id" and
                  optionally a "priority" key.
        """
        # Create a Redis pipeline for efficient batch operations.
        pipe: redis.client.Pipeline = self.redis.pipeline()
        # Iterate through each job dictionary in the list.
        for job in jobs:
            # Serialize the job dictionary to a JSON string.
            raw: str = json.dumps(job)
            # Calculate the score for the waiting queue Sorted Set based on priority and time.
            score: float = job.get("priority", 5) * 1e16 + time.time()
            # Add the job to the waiting queue Sorted Set using the pipeline.
            await pipe.zadd(f"queue:{queue_name}:waiting", {raw: score})
            # Add the job ID to the set of all job IDs for this queue using the pipeline.
            await pipe.sadd(f"jobs:{queue_name}:ids", job["id"])
            # Save the raw job data in the job store (using a direct Redis SET
            # via the pipeline, assuming job store uses simple keys).
            await pipe.set(f"jobs:{queue_name}:{job['id']}", raw) # Note: This bypasses job_store.save logic (e.g., status update) if job_store is more complex than simple SET. Based on original code, keeping it.

        # Execute all commands in the pipeline atomically.
        await pipe.execute()

    async def purge(
        self, queue_name: str, state: str, older_than: float | None = None
    ) -> None:
        """
        Removes jobs from a queue based on their state and optional age criteria.

        This implementation fetches jobs by status from the job store and
        then removes them from the job store and the corresponding Redis
        Sorted Set (e.g., waiting, failed, completed, etc.) if they match
        the age criteria (`older_than`).

        Args:
            queue_name: The name of the queue from which to purge jobs.
            state: The state of the jobs to be removed (e.g., "completed", "failed").
            older_than: An optional timestamp. Only jobs in the specified state
                        whose relevant timestamp (completion, failure, expiration
                        time) is older than this value will be removed. If None,
                        all jobs in the specified state might be purged.
                        Defaults to None.
        """
        # Retrieve jobs matching the specified status from the job store.
        jobs: list[dict[str, Any]] = await self.job_store.jobs_by_status(
            queue_name, state
        )
        # Iterate through the jobs retrieved.
        for job in jobs:
            # Determine the relevant timestamp for comparison (using completed_at
            # or current time as a fallback if no relevant timestamp exists).
            ts: float = job.get("completed_at", time.time()) # Might need to be smarter based on state
            # Check if the job is older than the 'older_than' timestamp (if provided).
            if older_than is None or ts < older_than:
                # Delete the job from the job store.
                await self.job_store.delete(queue_name, job["id"])
                # Remove the job from the corresponding Redis Sorted Set by its payload.
                await self.redis.zrem(f"queue:{queue_name}:{state}", json.dumps(job))

    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        """
        Emits an event both locally using the global event emitter and
        distributes it via Redis Pub/Sub.

        Args:
            event: The name of the event to emit.
            data: The data associated with the event.
        """
        # Define the Redis Pub/Sub channel for events.
        channel: str = "asyncmq:events"
        # Create a payload including event name, data, and timestamp.
        payload: str = json.dumps({"event": event, "data": data, "ts": time.time()})
        # Emit the event to local listeners using the global event emitter.
        await event_emitter.emit(event, data)
        # Publish the event payload to the Redis channel for distributed listeners.
        await self.redis.publish(channel, payload)

    async def create_lock(self, key: str, ttl: int) -> redis.lock.Lock:
        """
        Creates and returns a Redis-based distributed lock instance.

        Uses the `redis.asyncio.lock.Lock` implementation, which provides
        distributed locking capabilities with a specified time-to-live (TTL).

        Args:
            key: A unique string identifier for the lock.
            ttl: The time-to-live for the lock in seconds. The lock will
                 automatically expire after this duration if not released.

        Returns:
            A `redis.asyncio.lock.Lock` instance representing the distributed lock.
        """
        # Create and return a Redis Lock instance with the specified key and timeout.
        lock: redis.lock.Lock = self.redis.lock(key, timeout=ttl)
        return lock

    async def list_queues(self) -> list[str]:
        """
        Asynchronously lists all known queue names managed by this backend.

        This is done by fetching all keys matching the waiting queue pattern
        (`queue:*:waiting`) and extracting the queue name from the key.

        Returns:
            A list of strings, where each string is the name of a queue.
        """
        # Get all keys matching the pattern for waiting queues.
        # Note: Using KEYS can be blocking in Redis for large key spaces.
        keys: list[str] = await self.redis.keys("queue:*:waiting")
        # Extract the queue name from each key by splitting on ':' and taking
        # the second part (index 1). Example: 'queue:my_queue:waiting' -> 'my_queue'.
        queue_names: list[str] = [key.split(":")[1] for key in keys]
        return queue_names

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        """
        Asynchronously returns statistics about the number of jobs in different
        states for a specific queue.

        Provides counts for jobs currently waiting, delayed, and in the dead
        letter queue (DLQ).

        Args:
            queue_name: The name of the queue to get statistics for.

        Returns:
            A dictionary containing the counts with keys "waiting", "delayed",
            and "failed".
        """
        # Get the count of members in the waiting queue Sorted Set.
        waiting: int = await self.redis.zcard(self._waiting_key(queue_name))
        # Get the count of members in the delayed queue Sorted Set.
        delayed: int = await self.redis.zcard(self._delayed_key(queue_name))
        # Get the count of members in the DLQ Sorted Set.
        failed: int = await self.redis.zcard(self._dlq_key(queue_name))
        return {
            "waiting": waiting,
            "delayed": delayed,
            "failed": failed,
        }

    async def list_jobs(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously lists all jobs (payloads) across the waiting, delayed,
        and Dead Letter Queue (DLQ) for a specific queue.

        Retrieves all members from the respective Redis Sorted Sets, decodes
        the JSON payloads, and returns them as a list of dictionaries.

        Args:
            queue_name: The name of the queue to list jobs from.

        Returns:
            A list of dictionaries, where each dictionary is a job payload.
        """
        jobs: list[dict[str, Any]] = []
        # Get all members from the waiting queue Sorted Set.
        raw_waiting: list[str] = await self.redis.zrange(
            self._waiting_key(queue_name), 0, -1
        )
        # Get all members from the delayed queue Sorted Set.
        raw_delayed: list[str] = await self.redis.zrange(
            self._delayed_key(queue_name), 0, -1
        )
        # Get all members from the DLQ Sorted Set.
        raw_dlq: list[str] = await self.redis.zrange(
            self._dlq_key(queue_name), 0, -1
        )
        # Combine the results from all three sets and deserialize the JSON.
        for raw in raw_waiting + raw_delayed + raw_dlq:
            jobs.append(json.loads(raw))
        return jobs

    async def retry_job(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously retries a failed job by moving it from the Dead Letter
        Queue (DLQ) back to the waiting queue for processing.

        It iterates through the DLQ to find the job by its ID, removes it from
        the DLQ, and then enqueues it into the main waiting queue using the
        standard enqueue logic (which handles priority and timestamps).

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the failed job to retry.

        Returns:
            True if the job was found in the DLQ and successfully moved back
            to the waiting queue, False otherwise.
        """
        key: str = self._dlq_key(queue_name)
        # Retrieve all members from the DLQ to find the specific job.
        raw_jobs: list[str] = await self.redis.zrange(key, 0, -1)
        # Iterate through the raw job payloads.
        for raw in raw_jobs:
            job: dict[str, Any] = json.loads(raw)
            # Check if the job ID matches.
            if job.get("id") == job_id:
                # Atomically remove the job from the DLQ.
                await self.redis.zrem(key, raw)
                # Enqueue the job back into the waiting queue.
                await self.enqueue(queue_name, job)
                return True
        # Return False if the job with the given ID was not found in the DLQ.
        return False

    async def remove_job(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously removes a specific job from any of the main job queues
        (waiting, delayed, or DLQ) by its ID.

        It checks each of the relevant Redis Sorted Sets (waiting, delayed, DLQ),
        iterates through their members, finds the job by its ID within the
        payload, and removes it from the set where it is found.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove.

        Returns:
            True if the job was found and removed from any queue, False otherwise.
        """
        removed: bool = False
        # Define the list of Redis keys to check for the job.
        keys_to_check: list[str] = [
            self._waiting_key(queue_name),
            self._delayed_key(queue_name),
            self._dlq_key(queue_name),
        ]
        # Iterate through each key (queue type).
        for redis_key in keys_to_check:
            # Retrieve all members from the current set.
            raw_jobs: list[str] = await self.redis.zrange(redis_key, 0, -1)
            # Iterate through the raw job payloads in the set.
            for raw in raw_jobs:
                job: dict[str, Any] = json.loads(raw)
                # Check if the job ID matches.
                if job.get("id") == job_id:
                    # Atomically remove the job from the set.
                    await self.redis.zrem(redis_key, raw)
                    removed = True
                    # No need to continue checking other queues or this queue.
                    break
            # If the job was removed from this queue, we can stop checking others.
            if removed:
                break
        # Return whether the job was found and removed.
        return removed

    async def atomic_add_flow(
        self,
        queue_name: str,
        job_dicts: list[dict[str, Any]],
        dependency_links: list[tuple[str, str]],
    ) -> list[str]:
        """
        Atomically enqueues multiple jobs to the waiting queue and registers
        their parent-child dependencies using the pre-registered `FLOW_SCRIPT`
        Lua script.

        This script performs the following actions atomically:
        1. Adds each job payload from `job_dicts` to the waiting queue Sorted
           Set (`queue:{queue_name}:waiting`) with a score of 0 (highest priority).
           Note this differs from the `enqueue` and `bulk_enqueue` methods which
           use a priority-based score.
        2. Decodes each job payload JSON to extract the job ID.
        3. For each dependency pair (parent_id, child_id) from `dependency_links`,
           it sets a field in a Redis Hash keyed by the parent ID
           (`queue:{queue_name}:deps:parent_id`) with the child_id as the field
           and '1' as the value. This tracks which children are waiting on a
           parent using HSET. Note this differs from the `add_dependencies` method
           which uses SADD to track children waiting on a parent.

        Important Considerations:
        - This script *does not* save the full job payload in the job store
          (e.g., `jobs:{queue_name}:{job_id}`).
        - This script *does not* set up the child's pending dependency set
          (`deps:{queue_name}:{child_id}:pending`) which is used by `resolve_dependency`.
        - Due to the atomic nature of the script, jobs are enqueued *before*
          their pending dependencies are registered (if `add_dependencies` is called
          separately). The system relies on `resolve_dependency` to handle the
          actual state transitions.

        Args:
            queue_name: The name of the queue.
            job_dicts: A list of job payloads (dictionaries) to be enqueued. Must
                       contain an "id" key for each job.
            dependency_links: A list of (parent_id, child_id) tuples representing
                              the dependencies.

        Returns:
            A list of the IDs (strings) of the jobs that were successfully
            enqueued by the script.
        """
        waiting_key = f"queue:{queue_name}:waiting"
        deps_prefix = f"queue:{queue_name}:deps"

        # Build ARGV
        args = [str(len(job_dicts))]
        for jd in job_dicts:
            args.append(json.dumps(jd))
        args.append(str(len(dependency_links)))

        for parent, child in dependency_links:
            args.extend([parent, child])

        raw = await self.flow_script(
            keys=[waiting_key, deps_prefix],
            args=args
        )
        # raw is a Lua table of job IDs
        return raw

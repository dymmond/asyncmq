import json
import time
from typing import Any

import redis.asyncio as redis
from redis.commands.core import AsyncScript

from asyncmq.backends.base import BaseBackend, RepeatableInfo
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.schedulers import compute_next_run
from asyncmq.stores.redis_store import RedisJobStore

# Lua script to atomically pop the highest priority job from a Redis Sorted Set.
# KEYS[1]: The key of the Redis Sorted Set (e.g., 'queue:{queue_name}:waiting').
# Returns the job payload (JSON string) or nil if the set is empty.
POP_SCRIPT: str = """
local items = redis.call('ZRANGE', KEYS[1], 0, 0)
if #items == 0 then
  return nil
end
redis.call('ZREM', KEYS[1], items[1])
return items[1]
"""

# Lua script to atomically add multiple jobs and register dependencies for flows.
# KEYS[1]: Waiting queue Sorted Set key (e.g., 'queue:{queue_name}:waiting').
# KEYS[2]: Dependency Hash key prefix (e.g., 'queue:{queue_name}:deps').
# ARGV: num_jobs, job1_json, ..., jobN_json, num_deps, parent1, child1, ...
# Returns a Lua table of the IDs of the jobs that were added.
FLOW_SCRIPT: str = r"""
local num_jobs = tonumber(ARGV[1])
local idx = 2
local result = {}
for i=1,num_jobs do
  local job_json = ARGV[idx]
  idx = idx + 1
  -- Add job to the waiting queue Sorted Set with score 0 (highest priority).
  redis.call('ZADD', KEYS[1], 0, job_json)
  -- Decode the job JSON to extract the ID for the result list.
  local job = cjson.decode(job_json)
  table.insert(result, job.id)
end
local num_deps = tonumber(ARGV[idx]); idx = idx + 1
for i=1,num_deps do
  local parent = ARGV[idx]; local child = ARGV[idx+1]
  idx = idx + 2
  -- Register dependency: set a field in a Hash keyed by the parent ID.
  redis.call('HSET', KEYS[2] .. ':' .. parent, child, 1)
end
return result
"""


class RedisBackend(BaseBackend):
    """
    Implements the AsyncMQ backend interface using Redis as the storage and
    messaging layer.

    This backend utilizes various Redis data structures (Sorted Sets for queues
    like waiting, delayed, DLQ; Hashes for active jobs and heartbeats; Sets for
    dependencies and cancelled jobs; Pub/Sub for events) to manage job lifecycle,
    state transitions, scheduling, and communication. It interacts with a
    `RedisJobStore` for persisting full job payloads.
    """

    def __init__(self, redis_url: str = "redis://localhost") -> None:
        """
        Initializes the RedisBackend, connecting to Redis and setting up
        job storage and Lua scripts.

        Establishes an asynchronous connection to Redis and initializes the
        `RedisJobStore` for managing job data persistence. Registers necessary
        Lua scripts for atomic operations, improving efficiency and correctness.

        Args:
            redis_url: The connection URL string for the Redis instance.
                       Defaults to "redis://localhost".
        """
        # Establish an asynchronous connection to the Redis server.
        # decode_responses=True ensures all responses are strings.
        self.redis: redis.Redis = redis.from_url(redis_url, decode_responses=True)
        # Initialize the job store responsible for storing full job payloads.
        self.job_store: RedisJobStore = RedisJobStore(redis_url)
        # Register the POP_SCRIPT for atomic job dequeueing.
        self.pop_script: AsyncScript = self.redis.register_script(POP_SCRIPT)
        # Register the FLOW_SCRIPT for atomic flow creation (bulk enqueue/deps).
        self.flow_script: AsyncScript = self.redis.register_script(FLOW_SCRIPT)

    def _waiting_key(self, name: str) -> str:
        """
        Generates the Redis key for the Sorted Set holding jobs waiting in a queue.

        Args:
            name: The name of the queue.

        Returns:
            The Redis key string in the format 'queue:{queue_name}:waiting'.
        """
        return f"queue:{name}:waiting"

    def _active_key(self, name: str) -> str:
        """
        Generates the Redis key for the Hash holding jobs currently being processed.

        Args:
            name: The name of the queue.

        Returns:
            The Redis key string in the format 'queue:{queue_name}:active'.
        """
        return f"queue:{name}:active"

    def _delayed_key(self, name: str) -> str:
        """
        Generates the Redis key for the Sorted Set holding delayed jobs.

        Args:
            name: The name of the queue.

        Returns:
            The Redis key string in the format 'queue:{queue_name}:delayed'.
        """
        return f"queue:{name}:delayed"

    def _dlq_key(self, name: str) -> str:
        """
        Generates the Redis key for the Sorted Set holding jobs in the Dead
        Letter Queue (DLQ).

        Args:
            name: The name of the queue.

        Returns:
            The Redis key string in the format 'queue:{queue_name}:dlq'.
        """
        return f"queue:{name}:dlq"

    def _repeat_key(self, name: str) -> str:
        """
        Generates the Redis key for the Sorted Set holding repeatable job definitions.

        Args:
            name: The name of the queue.

        Returns:
            The Redis key string in the format 'queue:{queue_name}:repeatables'.
        """
        return f"queue:{name}:repeatables"

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously enqueues a job onto the specified queue for immediate
        processing.

        Adds the job to the waiting queue Sorted Set with a score calculated
        from its priority and the current timestamp. Saves the job's full
        payload with `State.WAITING` in the job store.

        Args:
            queue_name: The name of the queue.
            payload: The job data dictionary, must include 'id'.
                     'priority' key is optional (defaults to 5).
        """
        priority: int = payload.get("priority", 5)
        # Calculate a score for the Sorted Set based on priority and current time.
        # Lower score means higher priority.
        score: float = priority * 1e16 + time.time()
        key: str = self._waiting_key(queue_name)
        # Add the job payload (as JSON) to the waiting queue Sorted Set.
        await self.redis.zadd(key, {json.dumps(payload): score})
        # Save the full job payload with its status set to WAITING in the job store.
        await self.job_store.save(queue_name, payload["id"], {**payload, "status": State.WAITING})

    async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
        """
        Asynchronously dequeues the next available job from the specified queue.

        Uses a Lua script (`POP_SCRIPT`) to atomically retrieve and remove the
        highest priority job from the waiting queue. If a job is found, its
        state is updated to `State.ACTIVE` in the job store, and it is added
        to the set of active jobs.

        Args:
            queue_name: The name of the queue.

        Returns:
            The dequeued job data as a dictionary, or None if the queue is empty.
        """
        key: str = self._waiting_key(queue_name)
        # Execute the Lua script to atomically pop a job from the waiting queue.
        raw: str | None = await self.pop_script(keys=[key])
        # Check if a job was successfully popped.
        if raw:
            payload: dict[str, Any] = json.loads(raw)
            # Save the job with its status updated to ACTIVE.
            await self.job_store.save(queue_name, payload["id"], {**payload, "status": State.ACTIVE})
            # Add the job ID and the current timestamp to the set of active jobs.
            await self.redis.hset(self._active_key(queue_name), payload["id"], time.time())
            return payload
        return None

    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously moves a job to the Dead Letter Queue (DLQ).

        Adds the job to the DLQ Sorted Set, scored by the current timestamp,
        and updates the job's state to `State.FAILED` in the job store.

        Args:
            queue_name: The name of the queue the job originated from.
            payload: The job data dictionary, must include 'id'.
        """
        key: str = self._dlq_key(queue_name)
        # Create a new payload with the status set to FAILED.
        dlq_payload: dict[str, Any] = {**payload, "status": State.FAILED}
        # Add the job payload to the DLQ Sorted Set, scored by the current time.
        await self.redis.zadd(key, {json.dumps(dlq_payload): time.time()})
        # Save the updated job payload with FAILED status in the job store.
        await self.job_store.save(queue_name, payload["id"], dlq_payload)

    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously acknowledges the successful processing of a job.

        Removes the job ID from the set of active jobs for the specified queue.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
        """
        # Remove the job ID from the hash tracking active jobs.
        await self.redis.hdel(self._active_key(queue_name), job_id)

    async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        """
        Asynchronously schedules a job for future processing.

        Adds the job to the delayed queue Sorted Set, scored by the `run_at`
        timestamp. Updates the job's state to `State.EXPIRED` in the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            payload: The job data dictionary, must include 'id'.
            run_at: The absolute timestamp (float) when the job should become
                    available for processing.
        """
        key: str = self._delayed_key(queue_name)
        # Add the job payload to the delayed queue Sorted Set, scored by the run_at time.
        await self.redis.zadd(key, {json.dumps(payload): run_at})
        # Save the job with its status set to EXPIRED in the job store.
        await self.job_store.save(queue_name, payload["id"], {**payload, "status": State.EXPIRED})

    async def get_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves delayed jobs that are due for processing.

        Queries the delayed queue Sorted Set for jobs with a score less than
        or equal to the current time. Does not remove jobs from the set.

        Args:
            queue_name: The name of the queue.

        Returns:
            A list of job payloads (dictionaries) for jobs that are due.
        """
        key: str = self._delayed_key(queue_name)
        now: float = time.time()
        # Retrieve jobs from the delayed set with score <= current time.
        raw_jobs: list[str] = await self.redis.zrangebyscore(key, 0, now)
        # Deserialize the job payloads from JSON strings.
        return [json.loads(raw) for raw in raw_jobs]

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously removes a specific job from the delayed queue by its ID.

        Iterates through the delayed jobs to find and remove the job matching
        the specified ID. Note: This can be inefficient for large delayed queues.

        Args:
            queue_name: The name of the queue.
            job_id: The unique identifier of the job to remove.
        """
        key: str = self._delayed_key(queue_name)
        # Get all members of the delayed set.
        all_jobs_raw: list[str] = await self.redis.zrange(key, 0, -1)
        # Iterate through the jobs to find the one matching the job_id.
        for raw in all_jobs_raw:
            job: dict[str, Any] = json.loads(raw)
            # If the job ID matches.
            if job.get("id") == job_id:
                # Remove the job's JSON payload from the set.
                await self.redis.zrem(key, raw)
                # Stop searching after removing.
                break

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        """
        Asynchronously updates the status of a job in the job store.

        Loads the job, modifies its 'status' field, and saves it back.

        Args:
            queue_name: The name of the queue.
            job_id: The unique identifier of the job.
            state: The new state string for the job.
        """
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
        # If the job exists, update its status and save.
        if job:
            job["status"] = state
            await self.job_store.save(queue_name, job_id, job)

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        """
        Asynchronously saves the result of a job's execution in the job store.

        Loads the job, adds/updates its 'result' field, and saves it back.

        Args:
            queue_name: The name of the queue.
            job_id: The unique identifier of the job.
            result: The result of the job's task function (must be JSON serializable).
        """
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
        # If the job exists, add the result and save.
        if job:
            job["result"] = result
            await self.job_store.save(queue_name, job_id, job)

    async def get_job_state(self, queue_name: str, job_id: str) -> str | None:
        """
        Asynchronously retrieves the current status of a job from the job store.

        Args:
            queue_name: The name of the queue.
            job_id: The unique identifier of the job.

        Returns:
            The job's status string, or None if the job is not found.
        """
        # Load the job and return its status if available.
        job: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
        return job.get("status") if job else None

    async def get_job_result(self, queue_name: str, job_id: str) -> Any | None:
        """
        Asynchronously retrieves the execution result of a job from the job store.

        Args:
            queue_name: The name of the queue.
            job_id: The unique identifier of the job.

        Returns:
            The job's result, or None if the job is not found or has no result.
        """
        # Load the job and return its result if available.
        job: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
        return job.get("result") if job else None

    async def add_repeatable(self, queue_name: str, job_def: dict[str, Any], next_run: float) -> None:
        """
        Asynchronously adds or updates a repeatable job definition in Redis.

        Stores the job definition in the repeatable jobs Sorted Set, scored
        by its `next_run` timestamp.

        Args:
            queue_name: The name of the queue.
            job_def: The dictionary defining the repeatable job.
            next_run: The absolute timestamp (float) for the next run.
        """
        key: str = self._repeat_key(queue_name)
        payload: str = json.dumps(job_def)
        # Add the job definition (as JSON) to the repeatable jobs set.
        await self.redis.zadd(key, {payload: next_run})

    async def remove_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Asynchronously removes a repeatable job definition from Redis.

        Removes the specified job definition from the repeatable jobs Sorted Set.

        Args:
            queue_name: The name of the queue.
            job_def: The dictionary defining the repeatable job to remove.
        """
        key: str = self._repeat_key(queue_name)
        payload: str = json.dumps(job_def)
        # Remove the job definition (as JSON) from the repeatable jobs set.
        await self.redis.zrem(key, payload)

    async def get_due_repeatables(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves repeatable job definitions that are due.

        Queries the repeatable jobs Sorted Set for definitions with a score
        less than or equal to the current time. Does not remove items.

        Args:
            queue_name: The name of the queue.

        Returns:
            A list of repeatable job definition dictionaries that are due.
        """
        key: str = self._repeat_key(queue_name)
        now: float = time.time()
        # Retrieve definitions from the repeatable set with score <= current time.
        raw: list[str] = await self.redis.zrangebyscore(key, 0, now)
        # Deserialize the definitions from JSON strings.
        return [json.loads(item) for item in raw]

    async def add_dependencies(self, queue_name: str, job_dict: dict[str, Any]) -> None:
        """
        Asynchronously registers a job's dependencies using Redis Sets.

        Uses a Set to track parent jobs a child is waiting on (`deps:{queue_name}:{child_id}:pending`)
        and another Set to track children waiting on a parent (`deps:{queue_name}:parent:{parent_id}`).

        Args:
            queue_name: The name of the queue.
            job_dict: The job data dictionary, must include 'id' and can
                      optionally include 'depends_on' (list of parent IDs).
        """
        job_id: str = job_dict["id"]
        # Get the list of parent job IDs this job depends on.
        pending: list[str] = job_dict.get("depends_on", [])

        # If there are dependencies specified.
        if pending:
            # Add parent IDs to the child's pending dependencies Set.
            pend_key: str = f"deps:{queue_name}:{job_id}:pending"
            await self.redis.sadd(pend_key, *pending)

            # For each parent, add the child ID to the parent's children Set.
            for parent in pending:
                parent_children_key: str = f"deps:{queue_name}:parent:{parent}"
                await self.redis.sadd(parent_children_key, job_id)

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Asynchronously resolves dependencies after a parent job completes.

        Checks child jobs that were waiting on the parent. If a child's
        pending dependency count reaches zero, the child job is loaded
        and enqueued. Cleans up dependency tracking keys.

        Args:
            queue_name: The name of the queue the parent job belonged to.
            parent_id: The unique identifier of the completed parent job.
        """
        child_set_key: str = f"deps:{queue_name}:parent:{parent_id}"
        # Get all child IDs waiting on this parent.
        children: set[str] = await self.redis.smembers(child_set_key)

        # Iterate through each child job.
        for child_id in children:
            pend_key: str = f"deps:{queue_name}:{child_id}:pending"
            # Remove the parent's ID from the child's pending dependencies.
            await self.redis.srem(pend_key, parent_id)
            # Get the number of remaining pending dependencies for the child.
            rem: int = await self.redis.scard(pend_key)

            # If the child has no remaining pending dependencies.
            if rem == 0:
                # Load the child job data.
                data: dict[str, Any] | None = await self.job_store.load(queue_name, child_id)
                # If the job data exists.
                if data:
                    # Enqueue the child job.
                    await self.enqueue(queue_name, data)
                    # Emit an event indicating the job is ready.
                    await event_emitter.emit("job:ready", {"id": child_id})
                # Delete the child's empty pending dependencies set.
                await self.redis.delete(pend_key)

        # Delete the parent's children set.
        await self.redis.delete(child_set_key)

    async def pause_queue(self, queue_name: str) -> None:
        """
        Asynchronously pauses the specified queue.

        Sets a simple key in Redis (`queue:{queue_name}:paused`) to indicate
        that the queue is paused. Workers should check for this key.

        Args:
            queue_name: The name of the queue to pause.
        """
        pause_key: str = f"queue:{queue_name}:paused"
        # Set the pause key. The value is not significant.
        await self.redis.set(pause_key, 1)

    async def resume_queue(self, queue_name: str) -> None:
        """
        Asynchronously resumes the specified queue.

        Deletes the key that indicates the queue is paused (`queue:{queue_name}:paused`).

        Args:
            queue_name: The name of the queue to resume.
        """
        pause_key: str = f"queue:{queue_name}:paused"
        # Delete the pause key.
        await self.redis.delete(pause_key)

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Asynchronously checks if the specified queue is currently paused.

        Checks for the existence of the pause key in Redis.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the queue is paused, False otherwise.
        """
        pause_key: str = f"queue:{queue_name}:paused"
        # Check if the pause key exists.
        return bool(await self.redis.exists(pause_key))

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Asynchronously saves the progress of a job in the job store.

        Loads the job, updates its 'progress' field, and saves it back.

        Args:
            queue_name: The name of the queue.
            job_id: The unique identifier of the job.
            progress: The progress value (float, typically 0.0 to 1.0).
        """
        # Load the job data.
        data: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
        # If job exists, update progress and save.
        if data:
            data["progress"] = progress
            await self.job_store.save(queue_name, job_id, data)

    async def bulk_enqueue(self, queue_name: str, jobs: list[dict[str, Any]]) -> None:
        """
        Asynchronously enqueues multiple jobs in a single batch using a Redis
        Pipeline.

        Adds each job to the waiting queue Sorted Set and saves its payload
        in the job store.

        Args:
            queue_name: The name of the queue.
            jobs: A list of job payloads (dictionaries), each with 'id' and
                  optional 'priority'.
        """
        # Create a Redis pipeline for batching commands.
        pipe: redis.client.Pipeline = self.redis.pipeline()
        # Iterate through each job to add commands to the pipeline.
        for job in jobs:
            raw: str = json.dumps(job)
            # Calculate score based on priority and time.
            score: float = job.get("priority", 5) * 1e16 + time.time()
            # Add job to the waiting queue Sorted Set via the pipeline.
            await pipe.zadd(f"queue:{queue_name}:waiting", {raw: score})
            # Save the raw job data in the job store via the pipeline.
            await pipe.set(f"jobs:{queue_name}:{job['id']}", raw)

        # Execute all commands in the pipeline.
        await pipe.execute()

    async def purge(self, queue_name: str, state: str, older_than: float | None = None) -> None:
        """
        Asynchronously purges jobs in a specific state, optionally older than
        a given timestamp.

        Retrieves jobs by status from the job store, then removes them from
        the job store and the corresponding Redis set if they meet the age criteria.

        Args:
            queue_name: The name of the queue.
            state: The state of jobs to purge ('completed', 'failed', etc.).
            older_than: Optional timestamp (float). Jobs with a relevant timestamp
                        older than this will be purged.
        """
        # Get jobs matching the status from the job store.
        jobs: list[dict[str, Any]] = await self.job_store.jobs_by_status(queue_name, state)
        # Iterate through the jobs.
        for job in jobs:
            # Determine the relevant timestamp for age check (using 'completed_at').
            ts: float = job.get("completed_at", time.time())
            # Check if the job is older than the specified timestamp (if provided).
            if older_than is None or ts < older_than:
                # Delete the job from the job store.
                await self.job_store.delete(queue_name, job["id"])
                # Remove the job's JSON payload from the corresponding Redis set.
                await self.redis.zrem(f"queue:{queue_name}:{state}", json.dumps(job))

    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        """
        Asynchronously emits an event locally and via Redis Pub/Sub.

        Args:
            event: The name of the event.
            data: The data associated with the event.
        """
        channel: str = "asyncmq:events"
        # Create the event payload including event name, data, and timestamp.
        event_payload: dict[str, Any] = {"event": event, "data": data, "ts": time.time()}
        payload_json: str = json.dumps(event_payload)
        # Emit the event to local listeners.
        await event_emitter.emit(event, data)
        # Publish the event payload to the Redis channel.
        await self.redis.publish(channel, payload_json)

    async def create_lock(self, key: str, ttl: int) -> redis.lock.Lock:
        """
        Asynchronously creates a Redis-based distributed lock.

        Uses `redis-py`'s `Lock` implementation with a specified key and TTL.

        Args:
            key: A unique string identifier for the lock.
            ttl: The time-to-live for the lock in seconds.

        Returns:
            A `redis.asyncio.lock.Lock` instance.
        """
        # Create and return a Redis Lock instance.
        lock: redis.lock.Lock = self.redis.lock(key, timeout=ttl)
        return lock

    async def list_queues(self) -> list[str]:
        """
        Asynchronously lists all known queue names managed by this backend.

        Scans Redis keys matching the waiting queue pattern (`queue:*:waiting`)
        and extracts the queue name from each key. Note: Uses `SCAN_ITER` for
        efficiency on potentially large key spaces.

        Returns:
            A list of queue name strings.
        """
        queue_names: list[str] = []
        # Scan for keys matching the waiting queue pattern.
        async for full_key in self.redis.scan_iter(match="queue:*:waiting"):
            # Extract the queue name from the key (e.g., 'queue:my_queue:waiting').
            parts: list[str] = full_key.split(":")
            # Add the queue name to the list if the key format is as expected.
            if len(parts) == 3:
                queue_names.append(parts[1])
        return queue_names

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        """
        Asynchronously returns statistics about the number of jobs in different
        states for a queue.

        Counts jobs in the waiting, delayed, and DLQ sets using `ZCARD`.
        Calculates effective waiting count by subtracting failed count.

        Args:
            queue_name: The name of the queue.

        Returns:
            A dictionary with counts for "waiting", "delayed", and "failed" jobs.
        """
        # Get raw counts from Redis Sorted Sets.
        raw_waiting: int = await self.redis.zcard(self._waiting_key(queue_name))
        delayed: int = await self.redis.zcard(self._delayed_key(queue_name))
        failed: int = await self.redis.zcard(self._dlq_key(queue_name))

        # Calculate effective waiting count, ensuring it's not negative.
        waiting: int = max(0, raw_waiting - failed)

        # Return the statistics dictionary.
        return {
            "waiting": waiting,
            "delayed": delayed,
            "failed": failed,
        }

    async def list_jobs(self, queue_name: str, state: str) -> list[dict[str, Any]]:
        """
        Asynchronously lists jobs in a queue filtered by state.

        Retrieves jobs from the relevant Redis Sorted Set based on the state
        ('waiting', 'delayed', 'failed').

        Args:
            queue_name: The name of the queue.
            state: The job state to filter by. Supported: 'waiting', 'delayed', 'failed'.

        Returns:
            A list of job dictionaries. Returns an empty list for unsupported states.
        """
        # Map state strings to their corresponding Redis keys.
        key_map = {
            "waiting": self._waiting_key(queue_name),
            "delayed": self._delayed_key(queue_name),
            "failed": self._dlq_key(queue_name),
        }

        # Get the Redis key for the requested state.
        key = key_map.get(state)
        # If the state is not supported or has no corresponding key, return empty list.
        if not key:
            return []

        # Retrieve all members from the relevant Sorted Set.
        raw_jobs: list[str] = await self.redis.zrange(key, 0, -1)
        jobs: list[dict[str, Any]] = []

        # Deserialize each JSON job payload.
        for raw in raw_jobs:
            try:
                jobs.append(json.loads(raw))
            except json.JSONDecodeError:
                # Skip members that are not valid JSON.
                continue

        return jobs

    async def retry_job(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously retries a failed job by moving it from the DLQ back
        to the waiting queue.

        Searches the DLQ for the job by ID, removes it, and enqueues it back
        into the main queue.

        Args:
            queue_name: The name of the queue.
            job_id: The unique identifier of the failed job.

        Returns:
            True if the job was found in the DLQ and retried, False otherwise.
        """
        key: str = self._dlq_key(queue_name)
        # Retrieve all jobs from the DLQ to find the one by ID.
        raw_jobs: list[str] = await self.redis.zrange(key, 0, -1)
        # Iterate through the jobs in the DLQ.
        for raw in raw_jobs:
            job: dict[str, Any] = json.loads(raw)
            # Check if the job ID matches.
            if job.get("id") == job_id:
                # Atomically remove the job from the DLQ.
                removed_count: int = await self.redis.zrem(key, raw)
                # If removed successfully, enqueue it back.
                if removed_count > 0:
                    await self.enqueue(queue_name, job)
                    return True
        return False

    async def remove_job(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously removes a job from any of the main queues (waiting,
        delayed, or DLQ) by its ID.

        Checks each relevant Redis Sorted Set, finds the job by ID within
        its payload, and removes it from the set where it's found.

        Args:
            queue_name: The name of the queue.
            job_id: The unique identifier of the job to remove.

        Returns:
            True if the job was found and removed from any queue, False otherwise.
        """
        removed: bool = False
        # List of Redis keys for the main job queues.
        keys_to_check: list[str] = [
            self._waiting_key(queue_name),
            self._delayed_key(queue_name),
            self._dlq_key(queue_name),
        ]
        # Iterate through each queue key.
        for redis_key in keys_to_check:
            # Get all jobs in the current queue set.
            raw_jobs: list[str] = await self.redis.zrange(redis_key, 0, -1)
            # Iterate through the jobs in the set.
            for raw in raw_jobs:
                job: dict[str, Any] = json.loads(raw)
                # If the job ID matches.
                if job.get("id") == job_id:
                    # Atomically remove the job from the set.
                    removed_count: int = await self.redis.zrem(redis_key, raw)
                    # If removal was successful.
                    if removed_count > 0:
                        removed = True
                        # Stop searching this queue and other queues.
                        break
            # If job was removed, exit the outer loop.
            if removed:
                break
        return removed

    async def atomic_add_flow(
        self,
        queue_name: str,
        job_dicts: list[dict[str, Any]],
        dependency_links: list[tuple[str, str]],
    ) -> list[str]:
        """
        Atomically enqueues multiple jobs and registers dependencies for a flow
        using a Lua script.

        Enqueues jobs to the waiting queue and sets up parent-child dependency
        relationships in Redis Hashes.

        Args:
            queue_name: The name of the queue.
            job_dicts: A list of job payloads (dictionaries), each with 'id'.
            dependency_links: A list of (parent_id, child_id) tuples.

        Returns:
            A list of the IDs of the jobs that were successfully enqueued by the script.
        """
        # Generate Redis keys for the script.
        waiting_key = f"queue:{queue_name}:waiting"
        deps_prefix = f"queue:{queue_name}:deps"

        # Build the arguments list for the Lua script.
        args = [str(len(job_dicts))]
        for jd in job_dicts:
            args.append(json.dumps(jd))
        args.append(str(len(dependency_links)))

        for parent, child in dependency_links:
            args.extend([parent, child])

        # Execute the pre-registered FLOW_SCRIPT.
        raw = await self.flow_script(keys=[waiting_key, deps_prefix], args=args)
        # The script returns a list of job IDs.
        return raw

    async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
        """
        Asynchronously records a job's heartbeat timestamp.

        Stores the timestamp in a Redis Hash specific to the queue
        (`queue:{queue_name}:heartbeats`).

        Args:
            queue_name: The name of the queue.
            job_id: The unique identifier of the running job.
            timestamp: The Unix timestamp (float) of the heartbeat.
        """
        key: str = f"queue:{queue_name}:heartbeats"
        # Store the timestamp associated with the job ID in the heartbeats Hash.
        await self.redis.hset(key, job_id, timestamp)

    async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves jobs considered stalled based on heartbeat timestamps.

        Scans all heartbeat Hashes across queues, checks timestamps against
        `older_than`, loads stalled job data from the job store, and returns
        a list of stalled job information.

        Args:
            older_than: An absolute Unix timestamp (float). Heartbeats older
                        than this mark a job as stalled.

        Returns:
            A list of dictionaries, each containing 'queue_name' and 'job_data'
            for a stalled job.
        """
        stalled: list[dict[str, Any]] = []
        # Scan for all heartbeat Hash keys.
        async for full_key in self.redis.scan_iter(match="queue:*:heartbeats"):
            # Extract the queue name from the key.
            parts: list[str] = full_key.split(":")
            # Ensure the key format is correct.
            if len(parts) != 3:
                continue
            _, queue_name, _ = parts
            # Retrieve all heartbeats (job ID -> timestamp) for this queue.
            heartbeats: dict[str, str] = await self.redis.hgetall(full_key)
            # Iterate through each job and its heartbeat timestamp.
            for job_id, ts_str in heartbeats.items():
                ts: float = float(ts_str)
                # If the heartbeat is older than the threshold.
                if ts < older_than:
                    # Load the full job data from the job store.
                    job_data: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
                    # If job data is found, add it to the stalled list.
                    if job_data is not None:
                        stalled.append({"queue_name": queue_name, "job_data": job_data})
        return stalled

    async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
        """
        Asynchronously re-enqueues a stalled job and removes its heartbeat record.

        Adds the job back to the waiting queue and deletes its entry from
        the heartbeat Hash using a Redis Pipeline.

        Args:
            queue_name: The name of the queue.
            job_data: The full job payload dictionary of the stalled job.
        """
        waiting_key: str = self._waiting_key(queue_name)
        payload: str = json.dumps(job_data)
        # Get job priority, default to 0 for re-enqueued stalled jobs.
        score: float = job_data.get("priority", 0)
        # Create a Redis pipeline.
        pipe: redis.client.Pipeline = self.redis.pipeline()
        # Add the job back to the waiting queue.
        await pipe.zadd(waiting_key, {payload: score})
        # Remove the heartbeat record for the job.
        await pipe.hdel(f"queue:{queue_name}:heartbeats", job_data["id"])
        # Execute the pipeline.
        await pipe.execute()

    async def list_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves a list of all delayed jobs for a queue.

        Gets all members and scores from the delayed queue Sorted Set,
        deserializes payloads, and returns a list of job info dictionaries.

        Args:
            queue_name: The name of the queue.

        Returns:
            A list of dictionaries, each containing delayed job info
            ('id', 'payload', 'run_at').
        """
        key = self._delayed_key(queue_name)
        # Retrieve all members and their scores from the delayed set.
        items: list[tuple[str, float]] = await self.redis.zrange(key, 0, -1, withscores=True)
        out: list[dict[str, Any]] = []
        # Iterate through each item and format the output.
        for raw, score in items:
            job = json.loads(raw)
            out.append({"id": job["id"], "payload": job, "run_at": score})
        return out

    async def list_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        """
        Asynchronously retrieves a list of all repeatable job definitions
        for a queue.

        Gets all members and scores from the repeatable jobs Sorted Set,
        deserializes definitions, and returns a list of `RepeatableInfo` instances.

        Args:
            queue_name: The name of the queue.

        Returns:
            A list of `RepeatableInfo` dataclass instances.
        """
        key = self._repeat_key(queue_name)
        # Retrieve all members and their scores from the repeatable jobs set.
        items: list[tuple[str, float]] = await self.redis.zrange(key, 0, -1, withscores=True)
        out: list[RepeatableInfo] = []
        # Iterate through each item and create RepeatableInfo instances.
        for raw, score in items:
            jd = json.loads(raw)
            # Extract 'paused' status, default to False.
            paused = jd.get("paused", False)
            out.append(RepeatableInfo(job_def=jd, next_run=score, paused=paused))
        return out

    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Asynchronously marks a repeatable job definition as paused in Redis.

        Removes the original definition and adds a modified version with
        'paused': True, maintaining the original score.

        Args:
            queue_name: The name of the queue.
            job_def: The dictionary defining the repeatable job to pause.
        """
        key = self._repeat_key(queue_name)
        raw = json.dumps(job_def)
        # Remove the original job definition.
        await self.redis.zrem(key, raw)
        # Create a paused version of the definition.
        job_def_paused = {**job_def, "paused": True}
        # Get the next_run score, default to current time.
        next_run = job_def.get("next_run", time.time())
        # Add the paused definition back with the same score.
        await self.redis.zadd(key, {json.dumps(job_def_paused): next_run})

    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Asynchronously un-pauses a repeatable job definition, computes its next
        run time, and updates it in Redis.

        Removes the paused definition, computes the next run time for the
        un-paused definition, and adds it back with the new score.

        Args:
            queue_name: The name of the queue.
            job_def: The dictionary defining the repeatable job to resume.
        """
        key = self._repeat_key(queue_name)
        # Create JSON for the paused version to remove.
        raw_paused = json.dumps({**job_def, "paused": True})
        # Remove the paused definition.
        await self.redis.zrem(key, raw_paused)
        # Create a clean definition without the paused flag.
        clean_def = {k: v for k, v in job_def.items() if k != "paused"}
        # Compute the next run time.
        next_run = compute_next_run(clean_def)
        # Add the cleaned definition back with the new next_run score.
        await self.redis.zadd(key, {json.dumps(clean_def): next_run})
        return next_run

    async def cancel_job(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously cancels a job by removing it from waiting/delayed queues
        and marking it as cancelled.

        Removes the job from the waiting and delayed Sorted Sets (note: original
        code uses LREM for the waiting queue, which is likely incorrect for a
        Sorted Set). Adds the job ID to a 'cancelled' Set.

        Args:
            queue_name: The name of the queue.
            job_id: The unique identifier of the job to cancel.
        """
        # Remove from waiting list (Potential issue: LREM on a Sorted Set).
        await self.redis.lrem(self._waiting_key(queue_name), 0, json.dumps({"id": job_id}))
        # Remove from delayed zset (Potential issue: ZREM with partial payload).
        await self.redis.zrem(self._delayed_key(queue_name), json.dumps({"id": job_id}))
        # Mark the job ID as cancelled in a Redis Set.
        await self.redis.sadd(f"queue:{queue_name}:cancelled", job_id)

    async def is_job_cancelled(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously checks if a job has been marked as cancelled.

        Checks for the presence of the job ID in the 'cancelled' Set.

        Args:
            queue_name: The name of the queue.
            job_id: The unique identifier of the job.

        Returns:
            True if the job is cancelled, False otherwise.
        """
        # Check if the job ID is a member of the cancelled Set.
        return await self.redis.sismember(f"queue:{queue_name}:cancelled", job_id)

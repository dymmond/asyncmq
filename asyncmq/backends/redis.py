import json
import time
from typing import Any, cast

import redis.asyncio as redis
from redis.commands.core import AsyncScript

from asyncmq.backends.base import BaseBackend, RepeatableInfo, WorkerInfo
from asyncmq.conf import monkay
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.schedulers import compute_next_run
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

# Lua script used to atomically add multiple jobs to the waiting queue and
# register their parent-child dependencies. This is used for flow creation.
# KEYS[1]: The Redis key for the waiting queue Sorted Set (e.g., 'queue:{queue_name}:waiting').
# KEYS[2]: The Redis key prefix for dependency Hashes (e.g., 'queue:{queue_name}:deps').
# ARGV[1]: The number of jobs to enqueue.
# ARGV[2]...ARGV[1+num_jobs]: JSON strings of job payloads.
# ARGV[2+num_jobs]: The number of dependency links.
# ARGV[3+num_jobs]...: Pairs of parent_id, child_id strings for dependency links.
# The script returns a Lua table containing the IDs of the jobs that were added.
FLOW_SCRIPT: str = r"""
-- ARGV: num_jobs, <job1_json>,...,<jobN_json>, num_deps, <parent1>,<child1>,...,<parentM>,<childM>
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
  -- This Hash tracks which children are waiting on this parent.
  redis.call('HSET', KEYS[2] .. ':' .. parent, child, 1)
end
return result
"""


class RedisBackend(BaseBackend):
    """
    A Redis-based implementation of the asyncmq backend interface.

    This backend leverages various Redis data structures (Sorted Sets, Hashes,
    Sets, Keys, Pub/Sub) to manage job queues, job states, delayed jobs,
    Dead Letter Queues (DLQs), repeatable tasks, dependencies, queue pause/resume
    status, job progress, bulk operations, purging, event emission, and
    distributed locking. It utilizes a separate `RedisJobStore` for persistent
    storage of the full job data payloads.
    """

    def __init__(self, redis_url: str = "redis://localhost") -> None:
        """
        Initializes the RedisBackend by establishing a connection to Redis and
        preparing the necessary components.

        Establishes an asynchronous connection to the specified Redis instance.
        Initializes a `RedisJobStore` instance, which is responsible for
        handling the persistent storage and retrieval of full job data payloads
        in Redis (typically using simple key-value pairs or Hashes). It also
        registers the `POP_SCRIPT` and `FLOW_SCRIPT` Lua scripts with Redis
        for atomic operations like dequeueing and flow creation.

        Args:
            redis_url: The connection URL string for the Redis instance.
                       Defaults to "redis://localhost".
        """
        # Connect to the Redis instance using the provided URL.
        # decode_responses=True ensures Redis returns strings instead of bytes.
        self.redis: redis.Redis = redis.from_url(redis_url, decode_responses=True)  # type: ignore
        # Initialize the RedisJobStore for persistent job data storage.
        self.job_store: RedisJobStore = RedisJobStore(redis_url)
        # Register the Lua POP_SCRIPT for atomic dequeue operations.
        self.pop_script: AsyncScript = self.redis.register_script(POP_SCRIPT)
        # Register the Lua FLOW_SCRIPT for atomic flow creation (bulk enqueue + dependencies).
        self.flow_script: AsyncScript = self.redis.register_script(FLOW_SCRIPT)

    def _waiting_key(self, name: str) -> str:
        """
        Generates the Redis key for the Sorted Set holding jobs waiting in a queue.

        Args:
            name: The name of the queue.

        Returns:
            The Redis key string in the format 'queue:{queue_name}:waiting'.
        """
        # Key format: 'queue:{queue_name}:waiting'
        return f"queue:{name}:waiting"

    def _active_key(self, name: str) -> str:
        """
        Generates the Redis key for the Hash holding jobs currently being processed.

        Args:
            name: The name of the queue.

        Returns:
            The Redis key string in the format 'queue:{queue_name}:active'.
        """
        # Key format: 'queue:{queue_name}:active'
        return f"queue:{name}:active"

    def _delayed_key(self, name: str) -> str:
        """
        Generates the Redis key for the Sorted Set holding delayed jobs.

        Args:
            name: The name of the queue.

        Returns:
            The Redis key string in the format 'queue:{queue_name}:delayed'.
        """
        # Key format: 'queue:{queue_name}:delayed'
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
        # Key format: 'queue:{queue_name}:dlq'
        return f"queue:{name}:dlq"

    def _repeat_key(self, name: str) -> str:
        """
        Generates the Redis key for the Sorted Set holding repeatable job definitions.

        Args:
            name: The name of the queue.

        Returns:
            The Redis key string in the format 'queue:{queue_name}:repeatables'.
        """
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
        is updated to `State.WAITING` in the job store.

        Args:
            queue_name: The name of the queue to enqueue the job onto.
            payload: The job data as a dictionary, expected to contain at least
                     an "id" and optionally a "priority" key.
        """
        # Get job priority from the payload, defaulting to 5 if not provided.
        priority: int = payload.get("priority", 5)
        # Calculate the score for the Sorted Set: lower priority + earlier time = higher priority.
        # Using 1e16 ensures priority dominates over timestamp for sorting.
        score: float = priority * 1e16 + time.time()
        # Get the Redis key for the waiting queue's Sorted Set.
        key: str = self._waiting_key(queue_name)
        # Add the JSON-serialized payload to the Sorted Set with the calculated score.
        # ZADD updates the score if the member (the JSON string) already exists.
        await self.redis.zadd(key, {json.dumps(payload): score})
        # Update the job's status to WAITING and save the full payload in the job store.
        # Create a new dictionary to avoid modifying the original payload in place.
        await self.job_store.save(queue_name, payload["id"], {**payload, "status": State.WAITING})

    async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
        """
        Asynchronously attempts to dequeue the next job from the specified queue.

        Uses a pre-registered Lua script (`POP_SCRIPT`) to atomically retrieve
        and remove the highest priority job (the one with the lowest score)
        from the waiting queue's Sorted Set. If a job is dequeued, its state
        is updated to `State.ACTIVE` in the job store, and it's added to a
        Redis Hash (`queue:{queue_name}:active`) keyed by job ID, along with
        the timestamp of when it became active.

        Args:
            queue_name: The name of the queue to dequeue a job from.

        Returns:
            The job data as a dictionary if a job was successfully dequeued,
            otherwise None.
        """
        # Get the Redis key for the waiting queue's Sorted Set.
        key: str = self._waiting_key(queue_name)
        # Execute the Lua script to atomically pop the next job from the Sorted Set.
        # The script returns the JSON string of the job payload or nil.
        raw: str | None = await self.pop_script(keys=[key])
        # If a job payload (as a JSON string) was returned by the script.
        if raw:
            # Deserialize the job payload from the JSON string into a dictionary.
            payload: dict[str, Any] = json.loads(raw)
            # Update the job's status to ACTIVE and save the full payload in the job store.
            # Create a new dictionary to avoid modifying the original payload in place.
            await self.job_store.save(queue_name, payload["id"], {**payload, "status": State.ACTIVE})
            # Add the job ID and the current time to the active jobs Hash.
            # This hash tracks jobs currently being processed.
            await self.redis.hset(self._active_key(queue_name), payload["id"], time.time())  # type: ignore
            # Return the dequeued job payload as a dictionary.
            return payload
        # If the script returned nil (no jobs in the waiting queue).
        return None

    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Asynchronously moves a job payload to the Dead Letter Queue (DLQ)
        associated with the specified queue.

        Jobs moved to the DLQ are stored in a Redis Sorted Set (`queue:{queue_name}:dlq`)
        scored by the current timestamp. The job's state is also updated to
        `State.FAILED` in the job store.

        Args:
            queue_name: The name of the queue the job originated from.
            payload: The job data as a dictionary, expected to contain at least
                     an "id" key.
        """
        # 1) Remove from the waiting set if present
        waiting_key = self._waiting_key(queue_name)
        for member in await self.redis.zrange(waiting_key, 0, -1):
            try:
                job = json.loads(member)
            except Exception:
                continue
            if job.get("id") == payload["id"]:
                await self.redis.zrem(waiting_key, member)
                break

        # 2) Remove from the delayed set if present
        delayed_key = self._delayed_key(queue_name)
        for member in await self.redis.zrange(delayed_key, 0, -1):
            try:
                job = json.loads(member)
            except Exception:
                continue
            if job.get("id") == payload["id"]:
                await self.redis.zrem(delayed_key, member)
                break

        # 3) Add to the DLQ Sorted Set
        dlq_key: str = self._dlq_key(queue_name)
        # Prepare a new payload dict with FAILED status
        raw_json = json.dumps({**payload, "status": State.FAILED})
        await self.redis.zadd(dlq_key, {raw_json: time.time()})

        # 4) Persist FAILED status in the job store
        await self.job_store.save(queue_name, payload["id"], {**payload, "status": State.FAILED})

    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously acknowledges the successful processing of a job.

        Removes the job ID from the Redis Hash tracking active jobs
        (`queue:{queue_name}:active`), indicating that the job is no longer
        being actively processed by a worker. The job's state should be updated
        in the job store separately (e.g., to `State.COMPLETED`) after this.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job being acknowledged.
        """
        # Remove the job ID from the active jobs Hash.
        await self.redis.hdel(self._active_key(queue_name), job_id)

    async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        """
        Asynchronously schedules a job to be available for processing at a
        specific future time by adding it to a Redis Sorted Set for delayed jobs.

        Jobs are stored in a Redis Sorted Set (`queue:{queue_name}:delayed`)
        scored by the `run_at` timestamp. The job's state is also updated to
        `State.EXPIRED` in the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            payload: The job data as a dictionary, expected to contain at least
                     an "id" key.
            run_at: The absolute timestamp (float, e.g., from time.time()) when the
                    job should become available for processing.
        """
        # Get the Redis key for the delayed queue's Sorted Set.
        key: str = self._delayed_key(queue_name)
        # Add the JSON-serialized payload to the delayed Sorted Set, scored by the run_at time.
        await self.redis.zadd(key, {json.dumps(payload): run_at})
        # Update the job's status to EXPIRED (or DELAYED) and save the full payload in the job store.
        # NOTE: Original code sets status to EXPIRED here. Consider changing to DELAYED
        # if a distinct DELAYED state is desired in the store.
        await self.job_store.save(queue_name, payload["id"], {**payload, "status": State.EXPIRED})

    async def get_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves a list of delayed job payloads from the
        specified queue that are now due for processing.

        Queries the Redis Sorted Set for delayed jobs (`queue:{queue_name}:delayed`)
        to find items with a score (run_at timestamp) less than or equal to the
        current time. It retrieves these jobs but *does not* remove them from
        the set. A separate process or the caller is responsible for removing
        the jobs after they are processed or moved.

        Args:
            queue_name: The name of the queue to check for due delayed jobs.

        Returns:
            A list of dictionaries, where each dictionary is a job payload
            that is ready to be moved to the main queue.
        """
        # Get the Redis key for the delayed queue's Sorted Set.
        key: str = self._delayed_key(queue_name)
        # Get the current timestamp.
        now: float = time.time()
        # Retrieve all jobs from the delayed set with a score <= now.
        # zrangebyscore retrieves members within a score range.
        raw_jobs: list[str] = await self.redis.zrangebyscore(key, 0, now)
        # Deserialize the JSON strings back into dictionaries.
        return [json.loads(raw) for raw in raw_jobs]

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously removes a specific job from the backend's delayed storage
        by its ID.

        This involves iterating through the delayed jobs' Sorted Set to find
        the job by its ID within the payload and then removing it. This approach
        can be inefficient for large delayed sets and should ideally be
        handled with more efficient data structures or a different storage approach
        if performance is critical for frequent removal.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove from delayed
                    storage.
        """
        # Get the Redis key for the delayed queue's Sorted Set.
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

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
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
            job["status"] = state  # Update the status field.
            await self.job_store.save(queue_name, job_id, job)  # Save the updated job data.

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
            job["result"] = result  # Add or update the result field.
            await self.job_store.save(queue_name, job_id, job)  # Save the updated job data.

    async def get_job_state(self, queue_name: str, job_id: str) -> dict[str, Any] | None:
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
        return cast(dict[str, Any], job.get("status")) if job else None

    async def get_job_result(self, queue_name: str, job_id: str) -> Any | None:
        """
        Asynchronously retrieves the execution result of a specific job from
        the job store.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job.

        Returns:
            The result of the job's task function if the job is found and has
            a 'result' field, otherwise None.
        """
        # Load the job data from the job store.
        job: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
        # Return the result field if the job is found, otherwise None.
        return job.get("result") if job else None

    async def add_repeatable(self, queue_name: str, job_def: dict[str, Any], next_run: float) -> None:
        """
        Asynchronously adds or updates a repeatable job definition in Redis.

        Repeatable job definitions are stored in a Redis Sorted Set
        (`queue:{queue_name}:repeatables`) scored by their `next_run` timestamp.
        This allows the scheduler to efficiently query for jobs that are due.
        The full job definition dictionary is serialized to a JSON string and
        used as the member in the Sorted Set.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job. This dictionary
                     should contain all necessary information to recreate the job
                     instance for future runs.
            next_run: The absolute timestamp (float, e.g., from time.time()) when the
                      next instance of this repeatable job should be enqueued.
        """
        # Get the Redis key for the repeatable jobs Sorted Set.
        key: str = self._repeat_key(queue_name)
        # Serialize the job definition dictionary to a JSON string.
        payload: str = json.dumps(job_def)
        # Add the JSON-serialized definition to the repeatable jobs Sorted Set,
        # scored by the next_run timestamp. ZADD updates the score if the member exists.
        await self.redis.zadd(key, {payload: next_run})

    async def remove_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Asynchronously removes a repeatable job definition from Redis.

        Removes the repeatable job definition from the Redis Sorted Set
        (`queue:{queue_name}:repeatables`). The job definition dictionary is
        serialized to a JSON string to match the member in the Sorted Set.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to remove.
        """
        # Get the Redis key for the repeatable jobs Sorted Set.
        key: str = self._repeat_key(queue_name)
        # Serialize the job definition dictionary to a JSON string to match the member.
        payload: str = json.dumps(job_def)
        # Remove the specific member (the JSON string) from the Sorted Set.
        await self.redis.zrem(key, payload)

    async def get_due_repeatables(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves repeatable job definitions from Redis that
        are now due for enqueuing.

        Queries the Redis Sorted Set for repeatable jobs (`queue:{queue_name}:repeatables`)
        to find items with a score (next_run timestamp) less than or equal to the
        current time. It retrieves these definitions but *does not* remove them
        from the set. A separate process or the caller is responsible for removing
        or updating the definitions after they are processed.

        Args:
            queue_name: The name of the queue to check for due repeatable jobs.

        Returns:
            A list of dictionaries, where each dictionary is a repeatable job
            definition that is ready to be enqueued.
        """
        # Get the Redis key for the repeatable jobs Sorted Set.
        key: str = self._repeat_key(queue_name)
        # Get the current timestamp.
        now: float = time.time()
        # Retrieve all members from the repeatable set with a score <= now.
        raw: list[str] = await self.redis.zrangebyscore(key, 0, now)
        # Deserialize the JSON strings back into dictionaries.
        return [json.loads(item) for item in raw]

    async def add_dependencies(self, queue_name: str, job_dict: dict[str, Any]) -> None:
        """
        Asynchronously registers a job's dependencies and the relationship
        between parent and child jobs in Redis using Sets.

        Uses a Redis Set (`deps:{queue_name}:{job_id}:pending`) to store the IDs
        of parent jobs a child job is waiting on. Uses a Redis Set
        (`deps:{queue_name}:parent:{parent_id}`) to store the IDs of child jobs
        waiting on a parent.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: The job data dictionary, expected to contain at least
                      an "id" key and an optional "depends_on" list of parent job IDs.
        """
        # Get the ID of the child job from the input dictionary.
        job_id: str = job_dict["id"]
        # Get the list of parent job IDs this child depends on from the input dictionary,
        # defaulting to an empty list if 'depends_on' is not present.
        pending: list[str] = job_dict.get("depends_on", [])

        # If there are dependencies specified in the input dictionary.
        if pending:
            # Generate the Redis key for the Set of parent IDs this job is waiting on.
            pend_key: str = f"deps:{queue_name}:{job_id}:pending"
            # Add all parent IDs from the 'pending' list to this Set.
            # *pending unpacks the list elements as separate arguments to sadd.
            await self.redis.sadd(pend_key, *pending)

            # For each parent job ID in the dependencies list.
            for parent in pending:
                # Generate the Redis key for the Set of child IDs waiting on this parent.
                parent_children_key: str = f"deps:{queue_name}:parent:{parent}"
                # Add the child job ID to this parent's children Set.
                await self.redis.sadd(parent_children_key, job_id)

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Asynchronously signals that a parent job has completed and checks if
        any dependent child jobs are now ready to be enqueued.

        Retrieves all child job IDs waiting on the `parent_id` from the
        `deps:{queue_name}:parent:{parent_id}` Set. For each child, it removes
        the `parent_id` from the child's pending dependencies Set
        (`deps:{queue_name}:{child_id}:pending`). If a child job's pending
        dependencies Set becomes empty, it means all its dependencies are met,
        and the child job is loaded from the job store and enqueued. Finally,
        it cleans up the pending dependencies Set for the child and the children
        Set for the parent.

        Args:
            queue_name: The name of the queue the parent job belonged to.
            parent_id: The unique identifier of the job that just completed.
        """
        # Generate the Redis key for the Set of child IDs waiting on this parent.
        child_set_key: str = f"deps:{queue_name}:parent:{parent_id}"
        # Get all child IDs from this Set. smembers returns a set of members.
        children: set[str] = await self.redis.smembers(child_set_key)

        # Iterate through each child job ID that was waiting on the parent.
        for child_id in children:
            # Generate the Redis key for the Set of parent IDs this child is waiting on.
            pend_key: str = f"deps:{queue_name}:{child_id}:pending"
            # Remove the completed parent's ID from the child's pending dependencies Set.
            await self.redis.srem(pend_key, parent_id)
            # Check the number of remaining pending dependencies for the child.
            rem: int = await self.redis.scard(pend_key)

            # If there are no remaining pending dependencies (the count is 0).
            if rem == 0:
                # Load the child job data from the job store.
                data: dict[str, Any] | None = await self.job_store.load(queue_name, child_id)
                # If the job data was successfully loaded.
                if data:
                    # Enqueue the child job into the main waiting queue.
                    await self.enqueue(queue_name, data)
                    # Emit a local event indicating the job is now ready for processing.
                    await event_emitter.emit("job:ready", {"id": child_id})
                # Delete the child's empty pending dependencies set to clean up.
                await self.redis.delete(pend_key)

        # Delete the parent's children set as all children have been processed or checked.
        await self.redis.delete(child_set_key)

    async def pause_queue(self, queue_name: str) -> None:
        """
        Asynchronously marks the specified queue as paused in Redis by setting a key.

        A simple key (`queue:{queue_name}:paused`) is set with a value (e.g., 1).
        Workers should check for the existence of this key to determine if
        they should stop dequeueing jobs from this queue.

        Args:
            queue_name: The name of the queue to pause.
        """
        # Generate the Redis key to indicate the queue is paused.
        pause_key: str = f"queue:{queue_name}:paused"
        # Set the key. The value (1) is arbitrary, existence is what matters.
        await self.redis.set(pause_key, 1)

    async def resume_queue(self, queue_name: str) -> None:
        """
        Asynchronously removes the key indicating that the specified queue is
        paused in Redis.

        Deleting the key (`queue:{queue_name}:paused`) allows workers to resume
        dequeueing jobs from this queue.

        Args:
            queue_name: The name of the queue to resume.
        """
        # Generate the Redis key that indicates the queue is paused.
        pause_key: str = f"queue:{queue_name}:paused"
        # Delete the key to unpause the queue.
        await self.redis.delete(pause_key)

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Asynchronously checks if the specified queue is currently marked as
        paused in Redis.

        Checks for the existence of the pause key (`queue:{queue_name}:paused`).

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the pause key exists for the queue, False otherwise.
        """
        # Generate the Redis key that indicates the queue is paused.
        pause_key: str = f"queue:{queue_name}:paused"
        # Check if the key exists in Redis. exists returns 1 if key exists, 0 otherwise.
        return bool(await self.redis.exists(pause_key))

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Asynchronously saves the progress percentage for a specific job in the
        job store.

        Loads the job data from the job store using the job ID, adds or updates
        the 'progress' field with the provided progress value, and saves the
        modified job data back to the job store. This allows external monitoring
        of job progress.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            progress: The progress value, typically a float between 0.0 and 1.0.
        """
        # Load the job data from the job store.
        data: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
        # If the job data was successfully loaded.
        if data:
            data["progress"] = progress  # Add or update the progress field.
            await self.job_store.save(queue_name, job_id, data)  # Save the updated job data.

    async def bulk_enqueue(self, queue_name: str, jobs: list[dict[str, Any]]) -> None:
        """
        Asynchronously enqueues multiple job payloads onto the specified queue
        in a single batch operation using a Redis Pipeline.

        Each job is added to the waiting queue's Sorted Set (`queue:{queue_name}:waiting`)
        with a priority-based score and saved in the job store. The operations
        are batched using a Redis Pipeline for efficiency.

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
            # ZADD updates the score if the member already exists.
            await pipe.zadd(f"queue:{queue_name}:waiting", {raw: score})
            # Save the raw job data in the job store (using a direct Redis SET
            # via the pipeline, assuming job store uses simple keys).
            # Note: This bypasses job_store.save logic (e.g., status update) if
            # job_store is more complex than simple SET. Based on original code,
            # keeping this direct pipeline SET.
            await pipe.set(f"jobs:{queue_name}:{job['id']}", raw)

        # Execute all commands in the pipeline atomically.
        await pipe.execute()

    async def purge(self, queue_name: str, state: str, older_than: float | None = None) -> None:
        """
        Asynchronously removes jobs from Redis and the job store based on their
        state and optional age criteria.

        This implementation fetches jobs by status from the job store and
        then removes them from the job store and the corresponding Redis
        Sorted Set (e.g., waiting, failed, completed, etc.) if they match
        the age criteria (`older_than`).

        Args:
            queue_name: The name of the queue from which to purge jobs.
            state: The state of the jobs to be removed (e.g., "completed", "failed").
            older_than: An optional timestamp (float). Only jobs in the specified state
                        whose relevant timestamp (completion, failure, expiration
                        time) is older than this value will be removed. If None,
                        all jobs in the specified state might be purged.
                        Defaults to None.
        """
        # Retrieve jobs matching the specified status from the job store.
        jobs: list[dict[str, Any]] = await self.job_store.jobs_by_status(queue_name, state)
        # Iterate through the jobs retrieved.
        for job in jobs:
            # Determine the relevant timestamp for comparison (using 'completed_at'
            # or current time as a fallback if no relevant timestamp exists).
            # NOTE: This logic assumes 'completed_at' is the relevant timestamp for purging.
            # More robust logic might consider other timestamps based on the 'state' argument.
            ts: float = job.get("completed_at", time.time())
            # Check if the job is older than the 'older_than' timestamp (if provided).
            if older_than is None or ts < older_than:
                # Delete the job from the job store.
                await self.job_store.delete(queue_name, job["id"])
                # Remove the job from the corresponding Redis Sorted Set by its payload.
                # This assumes the state name directly maps to the Redis key suffix.
                await self.redis.zrem(f"queue:{queue_name}:{state}", json.dumps(job))

    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        """
        Asynchronously emits an event both locally using the global event emitter
        and distributes it via Redis Pub/Sub.

        Args:
            event: The name of the event to emit.
            data: The data associated with the event.
        """
        # Define the Redis Pub/Sub channel for events.
        channel: str = "asyncmq:events"
        # Create a payload dictionary including the event name, data, and current timestamp.
        event_payload: dict[str, Any] = {"event": event, "data": data, "ts": time.time()}
        # Serialize the event payload to a JSON string.
        payload_json: str = json.dumps(event_payload)
        # Emit the event to local listeners using the global event emitter.
        await event_emitter.emit(event, data)
        # Publish the JSON event payload to the Redis channel for distributed listeners.
        await self.redis.publish(channel, payload_json)

    async def create_lock(self, key: str, ttl: int) -> redis.lock.Lock:
        """
        Asynchronously creates and returns a Redis-based distributed lock instance.

        Uses the `redis.asyncio.lock.Lock` implementation provided by `redis-py`,
        which offers distributed locking capabilities with a specified time-to-live (TTL).

        Args:
            key: A unique string identifier for the lock. This key is used in Redis.
            ttl: The time-to-live for the lock in seconds. The lock will
                 automatically expire after this duration if not explicitly released.

        Returns:
            A `redis.asyncio.lock.Lock` instance representing the distributed lock.
        """
        # Create and return a Redis Lock instance with the specified key and timeout (TTL).
        lock: redis.lock.Lock = self.redis.lock(key, timeout=ttl)
        return lock

    async def list_queues(self) -> list[str]:
        """
        Asynchronously lists all known queue names managed by this backend.

        This is done by fetching all Redis keys matching the waiting queue pattern
        (`queue:*:waiting`) and extracting the queue name from the key.
        Note: Using `KEYS` can be blocking in Redis for large key spaces and should
        be used with caution or replaced with `SCAN` in production environments
        with many keys.

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
        states (waiting, delayed, failed/DLQ) for a specific queue.

        Counts jobs in the waiting, delayed, and dead letter queue (DLQ) Redis
        Sorted Sets using `ZCARD`. Calculates the effective waiting count by
        subtracting the failed count from the raw waiting count (as jobs might
        exist in both the waiting set and the DLQ set temporarily).

        Args:
            queue_name: The name of the queue to get statistics for.

        Returns:
            A dictionary containing the counts for "waiting", "delayed", and
            "failed" jobs.
        """
        now = time.time()

        # Use your existing key helpers so they stay in sync
        waiting_key = self._waiting_key(queue_name)
        delayed_key = self._delayed_key(queue_name)
        dlq_key = self._dlq_key(queue_name)

        # waiting: count all entries in the sorted set for ready jobs
        waiting = await self.redis.zcard(waiting_key)

        # delayed: count entries whose score (run_at) is in the future
        delayed = await self.redis.zcount(delayed_key, now, float("+inf"))

        # failed: count all entries in the DLQ sorted set
        failed = await self.redis.zcard(dlq_key)

        return {
            "waiting": waiting,
            "delayed": delayed,
            "failed": failed,
        }

    async def list_jobs(self, queue_name: str, state: str) -> list[dict[str, Any]]:
        """
        lists jobs in a given queue filtered by a specific state.

        Supported states: waiting, delayed, failed.

        Args:
            queue_name: The name of the queue.
            state: The job state to filter by.

        Returns:
            A list of job dictionaries.
        """
        key_map = {
            "waiting": self._waiting_key(queue_name),
            "delayed": self._delayed_key(queue_name),
            "failed": self._dlq_key(queue_name),  # DLQ is for failed jobs
        }

        key = key_map.get(state)
        if not key:
            return []  # Unknown state or unsupported in Redis

        raw_jobs: list[str] = await self.redis.zrange(key, 0, -1)
        jobs: list[dict[str, Any]] = []

        for raw in raw_jobs:
            try:
                jobs.append(json.loads(raw))
            except Exception:
                continue

        return jobs

    async def retry_job(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously retries a failed job by moving it from the Dead Letter
        Queue (DLQ) back to the waiting queue for processing.

        It iterates through the DLQ to find the job by its ID within the payload,
        removes it from the DLQ using `ZREM`, and then enqueues it into the
        main waiting queue using the standard `enqueue` logic (which handles
        priority and timestamps).

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
            # Check if the job ID matches the one to retry.
            if job.get("id") == job_id:
                # Atomically remove the job from the DLQ. ZREM returns the number of elements removed.
                removed_count: int = await self.redis.zrem(key, raw)
                # If the job was successfully removed from the DLQ.
                if removed_count > 0:
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
        payload, and removes it from the set where it is found using `ZREM`.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove.

        Returns:
            True if the job was found and removed from any queue, False otherwise.
        """
        removed: bool = False
        # Define the list of Redis keys for the Sorted Sets to check for the job.
        keys_to_check: list[str] = [
            self._waiting_key(queue_name),
            self._delayed_key(queue_name),
            self._dlq_key(queue_name),
        ]
        # Iterate through each key (representing a queue type).
        for redis_key in keys_to_check:
            # Retrieve all members from the current set.
            raw_jobs: list[str] = await self.redis.zrange(redis_key, 0, -1)
            # Iterate through the raw job payloads in the set.
            for raw in raw_jobs:
                job: dict[str, Any] = json.loads(raw)
                # Check if the job ID matches the one to remove.
                if job.get("id") == job_id:
                    # Atomically remove the job from the set. ZREM returns the number of elements removed.
                    removed_count: int = await self.redis.zrem(redis_key, raw)
                    # If the job was successfully removed.
                    if removed_count > 0:
                        removed = True
                        # No need to continue checking other queues or this queue's remaining members.
                        break
            # If the job was removed from this queue, we can stop checking others.
            if removed:
                break
        # Return whether the job was found and removed from any queue.
        return removed

    async def atomic_add_flow(
        self,
        queue_name: str,
        job_dicts: list[dict[str, Any]],
        dependency_links: list[tuple[str, str]],
    ) -> Any:
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
          (e.g., `jobs:{queue_name}:{job_id}`). This needs to be handled separately
          if persistent storage of all job details is required immediately upon
          flow creation.
        - This script *does not* set up the child's pending dependency set
          (`deps:{queue_name}:{child_id}:pending`) which is used by `resolve_dependency`.
          The dependency resolution logic in `resolve_dependency` relies on this
          Set being populated elsewhere.
        - Due to the atomic nature of the script, jobs are enqueued *before*
          their pending dependencies are fully registered (if `add_dependencies`
          is called separately). The system relies on `resolve_dependency` to
          handle the actual state transitions based on the pending Sets.

        Args:
            queue_name: The name of the queue.
            job_dicts: A list of job payloads (dictionaries) to be enqueued. Each
                       dictionary must contain an "id" key.
            dependency_links: A list of (parent_id, child_id) tuples representing
                              the dependencies.

        Returns:
            A list of the IDs (strings) of the jobs that were successfully
            enqueued by the script.
        """
        # Generate the Redis key for the waiting queue Sorted Set.
        waiting_key = f"queue:{queue_name}:waiting"
        # Generate the Redis key prefix for dependency Hashes.
        deps_prefix = f"queue:{queue_name}:deps"

        # Build the ARGV list for the Lua script.
        args = [str(len(job_dicts))]  # First argument is the number of jobs.
        # Add JSON-serialized job payloads to ARGV.
        for jd in job_dicts:
            args.append(json.dumps(jd))
        # Add the number of dependency links to ARGV.
        args.append(str(len(dependency_links)))

        # Add parent and child ID pairs to ARGV.
        for parent, child in dependency_links:
            args.extend([parent, child])

        # Execute the pre-registered FLOW_SCRIPT.
        # KEYS are the waiting queue key and the dependency prefix.
        # ARGV contains the job payloads and dependency links.
        raw = await self.flow_script(keys=[waiting_key, deps_prefix], args=args)
        # The script returns a Lua table of job IDs that were added.
        return raw

    async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
        """
        Asynchronously records the timestamp of the last heartbeat for a
        running job in a Redis Hash.

        This allows tracking the liveness of worker processes. The timestamp
        (converted to a string) is stored as the value associated with the job ID
        key within a Hash specific to the queue (`queue:{queue_name}:heartbeats`).

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the running job.
            timestamp: The Unix timestamp (float) of the heartbeat.
        """
        # Generate the Redis key for the heartbeats Hash for this queue.
        key: str = f"queue:{queue_name}:heartbeats"
        # Store the timestamp (converted to string by Redis) associated with
        # the job_id in the heartbeats Hash using HSET.
        await self.redis.hset(key, job_id, timestamp)  # type: ignore

    async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves information about jobs whose last recorded
        heartbeat in Redis is older than a specified timestamp.

        It scans all Redis keys matching the heartbeat pattern (`queue:*:heartbeats`),
        iterates through the heartbeats (job ID -> timestamp) in each queue's Hash,
        and if a heartbeat's timestamp is older than `older_than`, it loads the
        full job data from the job store and includes it in the result list.
        Note: `SCAN_ITER` is used instead of `KEYS` for efficiency on large key spaces.

        Args:
            older_than: An absolute Unix timestamp (float). Jobs with heartbeats
                        older than this timestamp are considered stalled.

        Returns:
            A list of dictionaries, where each dictionary contains the
            'queue_name' (string) and the full 'job_data' (dictionary) for
            each stalled job found.
        """
        stalled: list[dict[str, Any]] = []
        # Scan for all Redis keys that are heartbeat Hashes across all queues.
        # scan_iter is a generator that yields keys matching the pattern.
        async for full_key in self.redis.scan_iter(match="queue:*:heartbeats"):
            # Split the key to extract the queue name. Expected format: 'queue:queue_name:heartbeats'.
            parts: list[str] = full_key.split(":")
            # Ensure the key format is as expected (3 parts).
            if len(parts) != 3:
                continue
            # Extract the queue name from the key parts.
            _, queue_name, _ = parts
            # Retrieve all job IDs and their last heartbeat timestamps from the Hash.
            # HGETALL returns a dictionary of field-value pairs.
            heartbeats: dict[str, str] = await self.redis.hgetall(full_key)
            # Iterate through each job ID and its timestamp string in the heartbeats Hash.
            for job_id, ts_str in heartbeats.items():
                # Convert the timestamp string to a float.
                ts: float = float(ts_str)
                # Check if the heartbeat timestamp is older than the threshold.
                if ts < older_than:
                    # If stalled, load the full job payload from the job store.
                    job_data: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
                    # If the job data was successfully loaded (it exists in the job store).
                    if job_data is not None:
                        # Add the queue name and job data to the list of stalled jobs.
                        stalled.append({"queue_name": queue_name, "job_data": job_data})
        # Return the list of stalled jobs found.
        return stalled

    async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
        """
        Asynchronously re-enqueues a stalled job back onto its original queue
        for re-processing and removes its heartbeat record.

        It adds the job payload back to the waiting queue's Sorted Set
        (`queue:{queue_name}:waiting`) and deletes the job's entry from the
        heartbeat Hash (`queue:{queue_name}:heartbeats`) for that queue. These
        operations are performed within a Redis Pipeline for atomicity.

        Args:
            queue_name: The name of the queue the stalled job belongs to.
            job_data: The full job payload dictionary of the stalled job.
                      Must contain an "id" key.
        """
        # Get the Redis key for the waiting queue's Sorted Set.
        waiting_key: str = self._waiting_key(queue_name)
        # Serialize the job data payload to a JSON string.
        payload: str = json.dumps(job_data)
        # Get the job's priority for the Sorted Set score, defaulting to 0.
        # Note: The original code uses priority 0 here for re-enqueued stalled jobs.
        score: float = job_data.get("priority", 0)
        # Create a Redis pipeline for batching commands.
        pipe: redis.client.Pipeline = self.redis.pipeline()
        # Add the job payload to the waiting queue Sorted Set with the determined score.
        await pipe.zadd(waiting_key, {payload: score})
        # Clean up the heartbeat record for this job ID from the heartbeats Hash.
        await pipe.hdel(f"queue:{queue_name}:heartbeats", job_data["id"])
        # Execute all commands in the pipeline atomically.
        await pipe.execute()

    async def list_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Asynchronously retrieves a list of all currently delayed jobs for a
        specific queue from the Redis Sorted Set.

        Retrieves all members and their scores from the delayed queue Sorted Set
        (`queue:{queue_name}:delayed`), deserializes the JSON job payloads, and
        returns a list of dictionaries, where each dictionary includes the job ID,
        the full payload, and the scheduled run time (`run_at`).

        Args:
            queue_name: The name of the queue to list delayed jobs for.

        Returns:
            A list of dictionaries, where each dictionary represents a delayed
            job and includes information like 'id', 'payload', and 'run_at'.
        """
        # Get the Redis key for the delayed queue's Sorted Set.
        key = self._delayed_key(queue_name)
        # Retrieve all members and their scores from the delayed set.
        # withscores=True returns a list of (member, score) tuples.
        items: list[tuple[str, float]] = await self.redis.zrange(key, 0, -1, withscores=True)
        out: list[dict[str, Any]] = []
        # Iterate through each (raw_payload, score) tuple.
        for raw, score in items:
            # raw is the serialized job dict. Deserialize it.
            job = json.loads(raw)
            # Append a dictionary containing the job ID, full payload, and run_at time.
            out.append({"id": job["id"], "payload": job, "run_at": score})
        return out

    async def list_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        """
        Asynchronously retrieves a list of all repeatable job definitions for a
        specific queue from the Redis Sorted Set.

        Retrieves all members and their scores from the repeatable jobs Sorted Set
        (`queue:{queue_name}:repeatables`), deserializes the JSON job definitions,
        and converts them into a list of `RepeatableInfo` dataclass instances.
        The 'paused' status is extracted from the job definition dictionary.

        Args:
            queue_name: The name of the queue to list repeatable jobs for.

        Returns:
            A list of `RepeatableInfo` dataclass instances.
        """
        # Get the Redis key for the repeatable jobs Sorted Set.
        key = self._repeat_key(queue_name)
        # Retrieve all members and their scores from the repeatable set.
        items: list[tuple[str, float]] = await self.redis.zrange(key, 0, -1, withscores=True)
        out: list[RepeatableInfo] = []
        # Iterate through each (raw_job_def, score) tuple.
        for raw, score in items:
            # raw is the serialized job definition dict. Deserialize it.
            jd = json.loads(raw)
            # Extract the 'paused' status from the job definition, defaulting to False.
            paused = jd.get("paused", False)
            # Append a new RepeatableInfo instance to the output list.
            out.append(RepeatableInfo(job_def=jd, next_run=score, paused=paused))
        return out

    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Asynchronously marks a specific repeatable job definition as paused in Redis.

        Removes the original job definition (serialized to JSON) from the
        repeatable jobs Sorted Set (`queue:{queue_name}:repeatables`) and
        then adds a new version of the job definition with the 'paused' flag
        set to True back into the set, using the original 'next_run' score.
        The scheduler should check this flag and skip scheduling new instances
        of a paused repeatable job.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to pause.
                     This dictionary should ideally include the 'next_run' key.
        """
        # Get the Redis key for the repeatable jobs Sorted Set.
        key = self._repeat_key(queue_name)
        # Serialize the original job definition to JSON.
        raw = json.dumps(job_def)
        # Remove the original job definition from the Sorted Set.
        await self.redis.zrem(key, raw)
        # Create a new job definition dictionary with the 'paused' flag set to True.
        job_def_paused = {**job_def, "paused": True}
        # Get the next_run timestamp from the original job definition, defaulting to current time.
        next_run = job_def.get("next_run", time.time())
        # Add the modified (paused) job definition back to the Sorted Set with the same score.
        await self.redis.zadd(key, {json.dumps(job_def_paused): next_run})

    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> Any:
        """
        Asynchronously un-pauses a repeatable job definition in Redis, computes
        its next scheduled run time, and updates the definition in the Sorted Set.

        Removes the paused version of the job definition (serialized to JSON with
        'paused': True) from the repeatable jobs Sorted Set (`queue:{queue_name}:repeatables`).
        It then creates a 'clean' definition without the 'paused' flag, computes
        its next run time using `compute_next_run`, and adds this updated
        definition back into the Sorted Set with the new next run time as the score.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to resume.
                     This dictionary should ideally include the original definition
                     details needed by `compute_next_run`.

        Returns:
            The newly computed timestamp (float) for the next run of the repeatable job.
        """
        # Get the Redis key for the repeatable jobs Sorted Set.
        key = self._repeat_key(queue_name)
        # Serialize the paused version of the job definition to JSON to match the member.
        raw_paused = json.dumps({**job_def, "paused": True})
        # Remove the paused job definition from the Sorted Set.
        await self.redis.zrem(key, raw_paused)
        # Create a 'clean' job definition dictionary by excluding the 'paused' key.
        clean_def = {k: v for k, v in job_def.items() if k != "paused"}
        # Compute the next scheduled run timestamp using the scheduler utility function.
        next_run = compute_next_run(clean_def)
        # Add the cleaned job definition back to the Sorted Set with the new next_run score.
        await self.redis.zadd(key, {json.dumps(clean_def): next_run})
        return next_run  # Return the newly computed next run timestamp.

    async def cancel_job(self, queue_name: str, job_id: str) -> bool:
        """
        Asynchronously cancels a job, removing it from the waiting and delayed
        queues in Redis and marking it as cancelled in a Redis Set.

        Removes the job from the waiting queue (using `LREM` - Note: original code
        uses LREM on a key that is a Sorted Set, which is incorrect; should likely
        be ZREM). Removes the job from the delayed queue (using `ZREM`). Adds the
        job ID to a Redis Set (`queue:{queue_name}:cancelled`) to mark it as
        cancelled. Workers should check this set and stop processing or skip the
        job if it's found here.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to cancel.
        """
        removed = False

        # Remove matching jobs from the waiting sorted set
        wait_key = self._waiting_key(queue_name)
        raw_jobs = await self.redis.zrange(wait_key, 0, -1)
        for raw in raw_jobs:
            job = json.loads(raw)
            if job.get("id") == job_id:
                await self.redis.zrem(wait_key, raw)
                removed = True

        delayed_key = self._delayed_key(queue_name)
        raw_jobs = await self.redis.zrange(delayed_key, 0, -1)
        for raw in raw_jobs:
            job = json.loads(raw)
            if job.get("id") == job_id:
                await self.redis.zrem(delayed_key, raw)
                removed = True

        await self.redis.sadd(f"queue:{queue_name}:cancelled", job_id)
        return removed

    async def is_job_cancelled(self, queue_name: str, job_id: str) -> Any:
        """
        Asynchronously checks if a specific job has been marked as cancelled
        in the Redis cancelled Set.

        Checks for the presence of the job ID in the Redis Set
        (`queue:{queue_name}:cancelled`). Workers can use this to determine if
        they should skip or stop processing a job.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to check.

        Returns:
            True if the job ID is found in the cancelled Set for the specified
            queue, False otherwise.
        """
        # Check if the job ID is a member of the cancelled Set. sismember returns 1 or 0.
        return await self.redis.sismember(f"queue:{queue_name}:cancelled", job_id)

    async def register_worker(
        self,
        worker_id: str,
        queue: str,
        concurrency: int,
        timestamp: float,
    ) -> None:
        """
        Record or bump this worker's heartbeat in a Redis hash.

        Records or updates the heartbeat and concurrency for a specific worker
        within the Redis hash dedicated to heartbeats for the given queue.
        Sets a TTL on the hash to ensure expiration if not updated.

        Args:
            worker_id: The unique identifier for the worker.
            queue: The name of the queue the worker is associated with.
            concurrency: The concurrency level of the worker.
            timestamp: The timestamp representing the worker's last heartbeat.
        """
        payload = json.dumps({"heartbeat": timestamp, "concurrency": concurrency})
        key = f"queue:{queue}:heartbeats"
        # HSET the timestamp
        await self.redis.hset(key, worker_id, payload)
        # Reset TTL so the whole hash expires if no updates
        await self.redis.expire(key, monkay.settings.heartbeat_ttl)

    async def deregister_worker(self, worker_id: str) -> None:
        """
        Remove this worker_id from all queue heartbeats hashes in Redis.

        Scans all Redis keys matching "queue:*:heartbeats" and removes
        the field corresponding to the specified worker_id from each hash.

        Args:
            worker_id: The unique identifier of the worker to deregister.
        """
        # Scan for any queue heartbeats key and HDEL the field
        async for key in self.redis.scan_iter(match="queue:*:heartbeats"):
            await self.redis.hdel(key, worker_id)

    async def list_workers(self) -> list[WorkerInfo]:
        """
        Lists active workers from Redis heartbeat hashes.

        Scans all Redis keys matching "queue:*:heartbeats", retrieves worker
        information from each hash, and filters entries based on whether
        their heartbeat is within the configured time-to-live (TTL).

        Returns:
            A list of WorkerInfo objects representing the active workers.
        """
        infos: list[WorkerInfo] = []
        now = time.time()
        async for full_key in self.redis.scan_iter(match="queue:*:heartbeats"):
            # full_key is bytes; decode to string
            if isinstance(full_key, bytes):
                key_str = full_key.decode()
            else:
                key_str = full_key

            _, queue_name, _ = key_str.split(":", 2)

            # fetch all worker_id→timestamp mappings
            heartbeats = await self.redis.hgetall(full_key)
            for wid, data in heartbeats.items():
                worker_id = wid.decode() if isinstance(wid, bytes) else wid
                payload = json.loads(data)
                timestamp = float(payload.get("heartbeat", 0))
                if now - timestamp <= monkay.settings.heartbeat_ttl:
                    infos.append(
                        WorkerInfo(
                            id=worker_id,
                            queue=queue_name,
                            concurrency=payload.get("concurrency"),  # each heartbeat represents one slot
                            heartbeat=timestamp,
                        )
                    )
        return infos

import json
import time
from typing import Any, Dict, List, Optional, Set

from redis import asyncio as redis

from asyncmq.backends.base import BaseBackend
from asyncmq.event import event_emitter
from asyncmq.stores.redis_store import RedisJobStore

# Lua script to atomically pop the next job from a sorted set
# KEYS[1] = waiting_key
# Returns the job payload or nil
POP_SCRIPT: str = """
local items = redis.call('ZRANGE', KEYS[1], 0, 0)
if #items == 0 then
  return nil
end
redis.call('ZREM', KEYS[1], items[1])
return items[1]
"""
"""
Lua script used to atomically retrieve and remove the highest priority job
(first element in the sorted set) from a Redis Sorted Set. This prevents
race conditions when multiple workers try to dequeue jobs simultaneously.
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
        Initializes the RedisBackend by connecting to Redis and preparing
        necessary components.

        Args:
            redis_url: The connection URL for the Redis instance. Defaults to
                       "redis://localhost".
        """
        # Establish an asynchronous connection to the Redis server.
        # decode_responses=True ensures Redis returns strings instead of bytes.
        self.redis: redis.Redis = redis.from_url(redis_url, decode_responses=True)
        # Initialize a RedisJobStore for persistent job data storage.
        self.job_store: RedisJobStore = RedisJobStore(redis_url)

        # Prepare the Lua script for atomic dequeueing.
        # The script is registered with Redis and can be called by its SHA1 hash.
        self.pop_script: redis.Script = self.redis.register_script(POP_SCRIPT)

    def _waiting_key(self, name: str) -> str:
        """Generates the Redis key for the Sorted Set holding jobs waiting in a queue."""
        return f"queue:{name}:waiting"

    def _active_key(self, name: str) -> str:
        """Generates the Redis key for the Hash holding jobs currently being processed."""
        return f"queue:{name}:active"

    def _delayed_key(self, name: str) -> str:
        """Generates the Redis key for the Sorted Set holding delayed jobs."""
        return f"queue:{name}:delayed"

    def _dlq_key(self, name: str) -> str:
        """Generates the Redis key for the Sorted Set holding jobs in the Dead Letter Queue."""
        return f"queue:{name}:dlq"

    def _repeat_key(self, name: str) -> str:
        """Generates the Redis key for the Sorted Set holding repeatable job definitions."""
        return f"queue:{name}:repeatables"

    async def enqueue(self, queue_name: str, payload: Dict[str, Any]) -> None:
        """
        Asynchronously enqueues a job payload onto the specified queue for
        immediate processing.

        Jobs are stored in a Redis Sorted Set (`queue:{queue_name}:waiting`)
        where the score is calculated based on priority and enqueue timestamp
        to ensure priority-based ordering. The full job payload is also saved
        in the job store, and its state is updated to "waiting".

        Args:
            queue_name: The name of the queue to enqueue the job onto.
            payload: The job data as a dictionary, expected to contain at least
                     an "id" and optionally a "priority" key.
        """
        # Calculate the score for the Sorted Set based on priority and current time.
        # Lower priority number means higher priority (smaller score).
        priority: int = payload.get('priority', 5)
        score: float = priority * 1e16 + time.time() # Using a large multiplier to ensure priority dominates timestamp.
        # Get the Redis key for the waiting queue's Sorted Set.
        key: str = self._waiting_key(queue_name)
        # Add the job payload (as a JSON string) to the Sorted Set with the calculated score.
        await self.redis.zadd(key, {json.dumps(payload): score})
        # Update the job's status to "waiting" and save the full payload in the job store.
        payload['status'] = 'waiting'
        await self.job_store.save(queue_name, payload['id'], payload)

    async def dequeue(self, queue_name: str) -> Optional[Dict[str, Any]]:
        """
        Asynchronously attempts to dequeue the next job from the specified queue.

        Uses a Lua script (`POP_SCRIPT`) to atomically retrieve and remove the
        highest priority job from the waiting queue's Sorted Set. If a job is
        dequeued, it's marked as "active" in the job store and optionally
        tracked in an "active" Redis Hash.

        Args:
            queue_name: The name of the queue to dequeue a job from.

        Returns:
            The job data as a dictionary if a job was successfully dequeued,
            otherwise None.
        """
        # Get the Redis key for the waiting queue's Sorted Set.
        key: str = self._waiting_key(queue_name)
        # Execute the Lua script to atomically pop the next job.
        raw: Optional[str] = await self.pop_script(keys=[key])
        # If a job payload was returned by the script:
        if raw:
            # Parse the JSON string back into a dictionary.
            payload: Dict[str, Any] = json.loads(raw)
            # Mark the job as active in the job store.
            payload['status'] = 'active'
            await self.job_store.save(queue_name, payload['id'], payload)
            # Optionally track the job in an active hash (e.g., for monitoring).
            await self.redis.hset(self._active_key(queue_name), payload['id'], time.time())
            # Return the dequeued job payload.
            return payload
        # If no job was available, return None.
        return None

    async def move_to_dlq(self, queue_name: str, payload: Dict[str, Any]) -> None:
        """
        Asynchronously moves a job payload to the Dead Letter Queue (DLQ)
        associated with the specified queue.

        Jobs are stored in a Redis Sorted Set (`queue:{queue_name}:dlq`)
        scored by the current timestamp. The job's state is updated to "failed"
        in the job store.

        Args:
            queue_name: The name of the queue the job originated from.
            payload: The job data as a dictionary, expected to contain at least an "id" key.
        """
        # Get the Redis key for the DLQ's Sorted Set.
        key: str = self._dlq_key(queue_name)
        # Add the job payload (with status updated to 'failed') to the DLQ Sorted Set, scored by current time.
        await self.redis.zadd(key, {json.dumps({**payload, 'status': 'failed'}): time.time()})
        # Update the job's state to "failed" in the job store.
        await self.job_store.save(queue_name, payload['id'], {**payload, 'status': 'failed'})

    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously acknowledges the successful processing of a job.

        Removes the job from the "active" Redis Hash. Optional cleanup
        in the job store might be needed depending on the store's lifecycle.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job being acknowledged.
        """
        # Remove the job from the "active" hash.
        await self.redis.hdel(self._active_key(queue_name), job_id)
        # Note: Additional cleanup in job_store (e.g., marking as completed or deleting)
        # might be handled by the worker logic calling this ack method.

    async def enqueue_delayed(self, queue_name: str, payload: Dict[str, Any], run_at: float) -> None:
        """
        Asynchronously schedules a job to be available for processing at a
        specific future time by adding it to a Redis Sorted Set for delayed jobs.

        Jobs are stored in a Redis Sorted Set (`queue:{queue_name}:delayed`)
        scored by the `run_at` timestamp. The job's state is updated to "delayed"
        in the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            payload: The job data as a dictionary, expected to contain at least an "id" key.
            run_at: The absolute timestamp (e.g., from time.time()) when the
                    job should become available for processing.
        """
        # Get the Redis key for the delayed jobs' Sorted Set.
        key: str = self._delayed_key(queue_name)
        # Add the job payload to the delayed Sorted Set, scored by the run_at timestamp.
        await self.redis.zadd(key, {json.dumps(payload): run_at})
        # Update the job's state to "delayed" in the job store.
        payload['status'] = 'delayed'
        await self.job_store.save(queue_name, payload['id'], payload)

    async def get_due_delayed(self, queue_name: str) -> List[Dict[str, Any]]:
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
        # Get the Redis key for the delayed jobs' Sorted Set.
        key: str = self._delayed_key(queue_name)
        # Get the current time.
        now: float = time.time()
        # Retrieve all members from the Sorted Set with a score between 0 and the current time.
        raw_jobs: List[str] = await self.redis.zrangebyscore(key, 0, now)
        # Parse the JSON strings back into dictionaries and return the list.
        return [json.loads(raw) for raw in raw_jobs]

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously removes a specific job from the backend's delayed storage
        by its ID.

        This involves iterating through the delayed jobs' Sorted Set to find
        the job by its ID within the payload and then removing it. This is
        potentially inefficient for large delayed sets.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove from delayed storage.
        """
        # Get the Redis key for the delayed jobs' Sorted Set.
        key: str = self._delayed_key(queue_name)
        # Retrieve all members from the delayed Sorted Set.
        all_jobs_raw: List[str] = await self.redis.zrange(key, 0, -1)
        # Iterate through the raw job payloads.
        for raw in all_jobs_raw:
            # Parse each job payload.
            job: Dict[str, Any] = json.loads(raw)
            # Check if the job ID matches the target ID.
            if job.get('id') == job_id:
                # If found, remove the job from the Sorted Set by its member value (the raw JSON string).
                await self.redis.zrem(key, raw)
                break # Exit the loop once the job is found and removed.

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        """
        Asynchronously updates the status of a specific job in the job store.

        Loads the job data from the job store, updates the 'status' field,
        and saves it back.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            state: The new state string for the job (e.g., "active", "completed").
        """
        # Load the job data from the job store.
        job: Optional[Dict[str, Any]] = await self.job_store.load(queue_name, job_id)
        # If the job data was found:
        if job:
            # Update the 'status' field.
            job['status'] = state
            # Save the updated job data back to the job store.
            await self.job_store.save(queue_name, job_id, job)

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        """
        Asynchronously saves the result of a job's execution in the job store.

        Loads the job data, adds/updates the 'result' field, and saves it back.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            result: The result returned by the job's task function.
        """
        # Load the job data from the job store.
        job: Optional[Dict[str, Any]] = await self.job_store.load(queue_name, job_id)
        # If the job data was found:
        if job:
            # Add or update the 'result' field.
            job['result'] = result
            # Save the updated job data back to the job store.
            await self.job_store.save(queue_name, job_id, job)

    async def get_job_state(self, queue_name: str, job_id: str) -> Optional[str]:
        """
        Asynchronously retrieves the current status string of a specific job
        from the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            The job's status string if the job is found, otherwise None.
        """
        # Load the job data from the job store.
        job: Optional[Dict[str, Any]] = await self.job_store.load(queue_name, job_id)
        # Return the 'status' field if the job was found, otherwise None.
        return job.get('status') if job else None

    async def get_job_result(self, queue_name: str, job_id: str) -> Optional[Any]:
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
        job: Optional[Dict[str, Any]] = await self.job_store.load(queue_name, job_id)
        # Return the 'result' field if the job was found, otherwise None.
        return job.get('result') if job else None

    # Repeatable jobs persistence methods
    async def add_repeatable(self, queue_name: str, job_def: Dict[str, Any], next_run: float) -> None:
        """
        Asynchronously adds or updates a repeatable job definition in Redis.

        Repeatable job definitions are stored in a Redis Sorted Set
        (`queue:{queue_name}:repeatables`) scored by their `next_run` timestamp.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job.
            next_run: The absolute timestamp when the next instance of this
                      repeatable job should be enqueued.
        """
        # Get the Redis key for the repeatable jobs' Sorted Set.
        key: str = self._repeat_key(queue_name)
        # Serialize the job definition dictionary to a JSON string.
        payload: str = json.dumps(job_def)
        # Add the job definition to the Sorted Set, scored by the next_run timestamp.
        await self.redis.zadd(key, {payload: next_run})

    async def remove_repeatable(self, queue_name: str, job_def: Dict[str, Any]) -> None:
        """
        Asynchronously removes a repeatable job definition from Redis.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to remove.
        """
        # Get the Redis key for the repeatable jobs' Sorted Set.
        key: str = self._repeat_key(queue_name)
        # Serialize the job definition dictionary to a JSON string.
        payload: str = json.dumps(job_def)
        # Remove the job definition from the Sorted Set by its member value.
        await self.redis.zrem(key, payload)

    async def get_due_repeatables(self, queue_name: str) -> List[Dict[str, Any]]:
        """
        Asynchronously retrieves repeatable job definitions from Redis that
        are now due for enqueuing.

        Queries the Redis Sorted Set for repeatable jobs to find items with a
        score (next_run timestamp) less than or equal to the current time.

        Args:
            queue_name: The name of the queue to check for due repeatable jobs.

        Returns:
            A list of dictionaries, where each dictionary is a repeatable job
            definition that is ready to be enqueued.
        """
        # Get the Redis key for the repeatable jobs' Sorted Set.
        key: str = self._repeat_key(queue_name)
        # Get the current time.
        now: float = time.time()
        # Retrieve all members from the Sorted Set with a score between 0 and the current time.
        raw: List[str] = await self.redis.zrangebyscore(key, 0, now)
        # Parse the JSON strings back into dictionaries and return the list.
        return [json.loads(item) for item in raw]

    async def add_dependencies(self, queue_name: str, job_dict: Dict[str, Any]) -> None:
        """
        Asynchronously registers a job's dependencies and the relationship
        between parent and child jobs in Redis using Sets.

        Uses a Set (`deps:{queue_name}:{job_id}:pending`) to store the IDs
        of parent jobs a child job is waiting on. Uses a Set (`deps:{queue_name}:parent:{parent_id}`)
        to store the IDs of child jobs waiting on a parent.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: The job data dictionary, expected to contain at least
                      an "id" key and an optional "depends_on" list.
        """
        job_id: str = job_dict['id']
        # Get the list of parent job IDs this job depends on.
        pending: List[str] = job_dict.get('depends_on', [])

        # If there are pending dependencies:
        if pending:
            # Get the Redis key for the Set storing pending dependencies for this child job.
            pend_key: str = f"deps:{queue_name}:{job_id}:pending"
            # Add all parent job IDs to the child's pending dependencies Set.
            await self.redis.sadd(pend_key, *pending)

            # For each parent job ID, register this job as a child dependency.
            for parent in pending:
                # Get the Redis key for the Set storing children dependent on this parent.
                parent_children_key: str = f"deps:{queue_name}:parent:{parent}"
                # Add the child job ID to the parent's children Set.
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
        # Get the Redis key for the Set storing children dependent on this parent.
        child_set_key: str = f"deps:{queue_name}:parent:{parent_id}"
        # Get all child job IDs that depend on this parent.
        children: Set[str] = await self.redis.smembers(child_set_key)

        # Iterate through each child job ID.
        for child_id in children:
            # Get the Redis key for the Set storing pending dependencies for this child job.
            pend_key: str = f"deps:{queue_name}:{child_id}:pending"
            # Remove the parent_id from the child's pending dependencies Set.
            await self.redis.srem(pend_key, parent_id)
            # Get the number of remaining pending dependencies for the child.
            rem: int = await self.redis.scard(pend_key)

            # If the number of remaining pending dependencies is 0, the child job is ready.
            if rem == 0:
                # Load the full job data for the child from the job store.
                data: Optional[Dict[str, Any]] = await self.job_store.load(queue_name, child_id)
                # If the job data was found:
                if data:
                    # Enqueue the child job for processing.
                    await self.enqueue(queue_name, data)
                    # Emit an event indicating the job is ready.
                    await event_emitter.emit('job:ready', {'id': child_id})
                # Delete the child's pending dependencies Set as it's no longer needed.
                await self.redis.delete(pend_key)

        # Delete the parent's children Set as all dependencies have been processed.
        await self.redis.delete(child_set_key)

    # Queue Pause/Resume methods
    async def pause_queue(self, queue_name: str) -> None:
        """
        Marks the specified queue as paused in Redis by setting a key.

        Args:
            queue_name: The name of the queue to pause.
        """
        # Set a key to indicate the queue is paused. The value (1) is arbitrary.
        await self.redis.set(f"queue:{queue_name}:paused", 1)

    async def resume_queue(self, queue_name: str) -> None:
        """
        Removes the key indicating that the specified queue is paused in Redis.

        Args:
            queue_name: The name of the queue to resume.
        """
        # Delete the key that indicates the queue is paused.
        await self.redis.delete(f"queue:{queue_name}:paused")

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Checks if the specified queue is currently marked as paused in Redis.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the pause key exists for the queue, False otherwise.
        """
        # Check if the pause key exists for the queue.
        return bool(await self.redis.exists(f"queue:{queue_name}:paused"))

    # Progress methods
    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Asynchronously saves the progress percentage for a specific job in the
        job store.

        Loads the job data, adds/updates the 'progress' field, and saves it back.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            progress: The progress value, typically a float between 0.0 and 1.0.
        """
        # Load the job data from the job store.
        data: Optional[Dict[str, Any]] = await self.job_store.load(queue_name, job_id)
        # If the job data was found:
        if data:
            # Add or update the 'progress' field.
            data['progress'] = progress
            # Save the updated job data back to the job store.
            await self.job_store.save(queue_name, job_id, data)

    # Bulk operations
    async def bulk_enqueue(self, queue_name: str, jobs: List[Dict[str, Any]]) -> None:
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
        # Create a Redis pipeline for batch execution.
        pipe: redis.client.Pipeline = self.redis.pipeline()
        # Iterate through each job payload in the list.
        for job in jobs:
            # Serialize the job payload to a JSON string.
            raw: str = json.dumps(job)
            # Calculate the score for the Sorted Set.
            score: float = job.get('priority', 5) * 1e16 + time.time()
            # Add the job to the waiting queue's Sorted Set in the pipeline.
            pipe.zadd(f"queue:{queue_name}:waiting", {raw: score})
            # Add the job ID to the queue's set of all job IDs in the pipeline (via job store).
            # Note: The original code used raw `SADD` and `SET` here, which duplicates job store logic.
            # Relying on job_store.save for the SET and job_store._set_key for SADD is better.
            # This implementation keeps the original Redis commands for consistency with the source.
            pipe.sadd(f"jobs:{queue_name}:ids", job['id']) # Corresponds to RedisJobStore._set_key
            pipe.set(f"jobs:{queue_name}:{job['id']}", raw) # Corresponds to RedisJobStore._key

        # Execute the pipeline, sending all commands to Redis in a single round trip.
        await pipe.execute()
        # Note: This bulk enqueue does not update the job state to 'waiting' in the job store
        # for each job individually after the bulk operation. A separate loop or modification
        # of the pipeline might be needed if this state update is critical immediately.

    async def purge(self, queue_name: str, state: str, older_than: Optional[float] = None) -> None:
        """
        Removes jobs from a queue based on their state and optional age criteria.

        This implementation fetches jobs by status from the job store and
        then removes them from the job store and the corresponding Redis
        Sorted Set (e.g., waiting, failed, completed, etc.). It considers
        the `older_than` timestamp if provided.

        Args:
            queue_name: The name of the queue from which to purge jobs.
            state: The state of the jobs to be removed (e.g., "completed", "failed").
            older_than: An optional timestamp. Only jobs in the specified state
                        whose relevant timestamp (completion, failure, expiration time)
                        is older than this value will be removed. If None, all jobs
                        in the specified state might be purged.
        """
        # Get jobs by status from the job store.
        jobs: List[Dict[str, Any]] = await self.job_store.jobs_by_status(queue_name, state)
        # Iterate through the retrieved jobs.
        for job in jobs:
            # Determine the timestamp to check against older_than.
            # Using 'completed_at' as a proxy; a real backend might track state transition times.
            ts: float = job.get('completed_at', time.time())
            # Check if the job meets the age criteria (if older_than is provided).
            if older_than is None or ts < older_than:
                # Remove the job from the job store.
                await self.job_store.delete(queue_name, job['id'])
                # Remove the job from the corresponding Redis Sorted Set based on its state.
                # Note: This assumes a Sorted Set exists for each state, which is not
                # explicitly defined for all states (like 'completed') in other methods.
                # This part might need adjustment based on the actual backend structure.
                await self.redis.zrem(f"queue:{queue_name}:{state}", json.dumps(job))

    # Event methods
    async def emit_event(self, event: str, data: Dict[str, Any]) -> None:
        """
        Emits an event both locally using the global event emitter and
        distributes it via Redis Pub/Sub.

        Args:
            event: The name of the event to emit.
            data: The data associated with the event.
        """
        # Define the Redis Pub/Sub channel for events.
        channel: str = 'asyncmq:events'
        # Prepare the payload for the Pub/Sub message.
        payload: str = json.dumps({'event': event, 'data': data, 'ts': time.time()})
        # Emit the event to local listeners.
        await event_emitter.emit(event, data)
        # Publish the event payload to the Redis Pub/Sub channel.
        await self.redis.publish(channel, payload)

    # Locking methods
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
        # Create and return a Redis Lock instance.
        lock: redis.lock.Lock = self.redis.lock(key, timeout=ttl)
        return lock

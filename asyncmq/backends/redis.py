import json
import time
from typing import Any, Union, cast

import redis.asyncio as redis
from redis.commands.core import AsyncScript

from asyncmq import monkay
from asyncmq.backends.base import BaseBackend, DelayedInfo, RepeatableInfo, WorkerInfo
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.core.inspection import has_pending_dependencies, matches_job_type, normalize_job_type
from asyncmq.core.repeatables import normalize_repeatable_job_def, repeatable_identity
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

POP_DELAYED = """
local key, now = KEYS[1], tonumber(ARGV[1])
local items = redis.call('ZRANGEBYSCORE', key, '-inf', now)
if #items > 0 then
    redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
end
return items
"""

PROMOTE_DELAYED: str = """
-- KEYS[1]: delayed zset
-- KEYS[2]: waiting zset
-- KEYS[3]: queue job-id set
-- ARGV[1]: number of candidate jobs
-- Per candidate:
--   delayed member JSON
--   waiting member/canonical JSON
--   job id
--   waiting zset score
--   canonical job key
local total = tonumber(ARGV[1])
local idx = 2
local promoted = {}

for _ = 1, total do
  local delayed_json = ARGV[idx]
  local waiting_json = ARGV[idx + 1]
  local job_id = ARGV[idx + 2]
  local waiting_score = tonumber(ARGV[idx + 3])
  local job_key = ARGV[idx + 4]
  idx = idx + 5

  if redis.call('ZREM', KEYS[1], delayed_json) > 0 then
    redis.call('ZADD', KEYS[2], waiting_score, waiting_json)
    redis.call('SET', job_key, waiting_json)
    redis.call('SADD', KEYS[3], job_id)
    table.insert(promoted, waiting_json)
  end
end

return promoted
"""

LIFECYCLE_SCRIPT: str = """
-- KEYS[1]: active hash
-- KEYS[2]: job heartbeat hash
-- KEYS[3]: waiting zset
-- KEYS[4]: delayed zset
-- KEYS[5]: dlq zset
-- KEYS[6]: canonical job key
-- KEYS[7]: queue job-id set
-- KEYS[8]: cancelled job-id set
-- ARGV[1]: action: complete, retry, defer, fail, expire, cancel
-- ARGV[2]: job id
-- ARGV[3]: canonical job JSON
-- ARGV[4]: delayed run_at score, when applicable
-- ARGV[5]: now score, when applicable
local action = ARGV[1]
local job_id = ARGV[2]
local job_json = ARGV[3]
local run_at = tonumber(ARGV[4])
local now = tonumber(ARGV[5])

local function remove_job_member(zset_key, target_id)
  local members = redis.call('ZRANGE', zset_key, 0, -1)
  for _, member in ipairs(members) do
    local ok, job = pcall(cjson.decode, member)
    if ok and job and tostring(job.id) == target_id then
      redis.call('ZREM', zset_key, member)
    end
  end
end

redis.call('HDEL', KEYS[1], job_id)
redis.call('HDEL', KEYS[2], job_id)

if redis.call('SISMEMBER', KEYS[8], job_id) == 1 then
  remove_job_member(KEYS[3], job_id)
  remove_job_member(KEYS[4], job_id)
  remove_job_member(KEYS[5], job_id)
  local cancelled_job = cjson.decode(job_json)
  cancelled_job.status = 'cancelled'
  cancelled_job.result = nil
  redis.call('SET', KEYS[6], cjson.encode(cancelled_job))
  redis.call('SADD', KEYS[7], job_id)
  return 1
end

if action == 'cancel' then
  redis.call('SADD', KEYS[8], job_id)
  local cancelled_job = cjson.decode(job_json)
  cancelled_job.status = 'cancelled'
  cancelled_job.result = nil
  redis.call('SET', KEYS[6], cjson.encode(cancelled_job))
  redis.call('SADD', KEYS[7], job_id)
  return 1
end

remove_job_member(KEYS[3], job_id)
remove_job_member(KEYS[4], job_id)

redis.call('SET', KEYS[6], job_json)
redis.call('SADD', KEYS[7], job_id)

if action == 'retry' or action == 'defer' then
  redis.call('ZADD', KEYS[4], run_at, job_json)
elseif action == 'fail' or action == 'expire' then
  redis.call('ZADD', KEYS[5], now, job_json)
end

return 1
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

    def __init__(self, redis_url_or_client: Union[str, redis.Redis] = "redis://localhost") -> None:
        """
        Initializes the RedisBackend by establishing a connection to Redis and
        preparing the necessary components.

        Establishes an asynchronous connection to the specified Redis instance or
        uses the provided Redis client instance. Initializes a `RedisJobStore`
        instance, which is responsible for handling the persistent storage and
        retrieval of full job data payloads in Redis (typically using simple
        key-value pairs or Hashes). It also registers the `POP_SCRIPT` and
        `FLOW_SCRIPT` Lua scripts with Redis for atomic operations like
        dequeueing and flow creation.

        Args:
            redis_url_or_client: Either a connection URL string for the Redis
                                instance or an async Redis client instance.
                                Defaults to "redis://localhost".
        """
        if isinstance(redis_url_or_client, str):
            # Connect to the Redis instance using the provided URL.
            # decode_responses=True ensures Redis returns strings instead of bytes.
            self.redis = redis.from_url(redis_url_or_client, decode_responses=True)  # type: ignore
            # Initialize the RedisJobStore for persistent job data storage.
            self.job_store = RedisJobStore(redis_url_or_client)
        else:
            # Use the provided Redis client instance.
            self.redis = redis_url_or_client
            # Initialize the RedisJobStore with the same Redis client instance.
            self.job_store = RedisJobStore(redis_client=redis_url_or_client)

        # Register the Lua POP_SCRIPT for atomic dequeue operations.
        self.pop_script: AsyncScript = self.redis.register_script(POP_SCRIPT)
        # Register the Lua FLOW_SCRIPT for atomic flow creation (bulk enqueue + dependencies).
        self.flow_script: AsyncScript = self.redis.register_script(FLOW_SCRIPT)
        self._pop_delayed_script: AsyncScript = self.redis.register_script(POP_DELAYED)
        self._promote_delayed_script: AsyncScript = self.redis.register_script(PROMOTE_DELAYED)
        self._lifecycle_script: AsyncScript = self.redis.register_script(LIFECYCLE_SCRIPT)

    async def pop_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        key = self._delayed_key(queue_name)
        now = time.time()
        # This calls the Lua in one round-trip: fetch + remove
        raw_items: list[str | bytes] = await self._pop_delayed_script(keys=[key], args=[now])
        # Decode & parse JSON
        out: list[dict[str, Any]] = []
        for item in raw_items:
            text = item.decode("utf-8") if isinstance(item, bytes) else item
            out.append(self._json_serializer.to_dict(text))
        return out

    async def promote_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        delayed_key = self._delayed_key(queue_name)
        now = time.time()
        raw_items: list[str | bytes] = await self.redis.zrangebyscore(delayed_key, "-inf", now)
        if not raw_items:
            return []

        args: list[Any] = [len(raw_items)]
        for item in raw_items:
            delayed_json = item.decode("utf-8") if isinstance(item, bytes) else item
            payload = self._json_serializer.to_dict(delayed_json)
            job_id = str(payload["id"])
            waiting_payload = {**payload, "status": State.WAITING, "delay_until": None}
            waiting_json = self._json_serializer.to_json(waiting_payload)
            priority_value = payload.get("priority", 5)
            score = await self._next_waiting_score(queue_name, int(priority_value if priority_value is not None else 5))
            args.extend([delayed_json, waiting_json, job_id, score, self._job_store_key(queue_name, job_id)])

        promoted_raw: list[str | bytes] = await self._promote_delayed_script(
            keys=[delayed_key, self._waiting_key(queue_name), self._job_store_ids_key(queue_name)],
            args=args,
        )
        promoted: list[dict[str, Any]] = []
        for item in promoted_raw:
            text = item.decode("utf-8") if isinstance(item, bytes) else item
            promoted.append(self._json_serializer.to_dict(text))
        return promoted

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

    def _waiting_sequence_key(self, name: str) -> str:
        """
        Return the Redis counter key used to preserve FIFO order within a priority.

        Redis sorted-set scores are IEEE 754 doubles. The previous implementation
        combined a very large priority multiplier with ``time.time()``, which can
        collapse multiple same-priority enqueues onto the same score once the
        magnitude grows large enough. A per-queue monotonic counter keeps waiting
        jobs stable and deterministic for dashboard inspection and worker dequeue
        order.

        Args:
            name: The queue name.

        Returns:
            The Redis key for the waiting sequence counter.
        """
        return f"queue:{name}:waiting:seq"

    async def _next_waiting_score(self, queue_name: str, priority: int) -> int:
        """
        Allocate a deterministic waiting score for a queued job.

        The score packs the logical priority and a monotonic enqueue sequence
        into an integer range that remains exactly representable by Redis' double
        scores for practical queue sizes. Lower priority numbers still sort
        first, and jobs with the same priority retain FIFO ordering.

        Args:
            queue_name: The target queue.
            priority: The logical priority for the job.

        Returns:
            An integer score suitable for ``ZADD``.
        """
        sequence = await self.redis.incr(self._waiting_sequence_key(queue_name))
        return priority * 1_000_000_000_000 + sequence

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

    def _job_heartbeat_key(self, name: str) -> str:
        return f"queue:{name}:heartbeats"

    def _cancelled_key(self, name: str) -> str:
        return f"queue:{name}:cancelled"

    def _worker_heartbeat_key(self, name: str) -> str:
        return f"queue:{name}:worker_heartbeats"

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

    async def _remove_waiting_member(self, queue_name: str, job_id: str) -> None:
        waiting_key = self._waiting_key(queue_name)
        for member in await self.redis.zrange(waiting_key, 0, -1):
            raw = member.decode("utf-8") if isinstance(member, bytes) else member
            try:
                payload = self._json_serializer.to_dict(raw)
            except Exception:
                continue
            if str(payload.get("id")) == job_id:
                await self.redis.zrem(waiting_key, member)

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

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> str:
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
        job_id = str(payload["id"])
        waiting_payload = {**payload, "status": State.WAITING}
        if has_pending_dependencies(waiting_payload):
            await self._remove_waiting_member(queue_name, job_id)
            await self.job_store.save(queue_name, job_id, waiting_payload)
            await self.add_dependencies(queue_name, waiting_payload)
            return job_id

        # Get job priority from the payload, defaulting to 5 if not provided.
        priority: int = int(waiting_payload.get("priority", 5))
        score = await self._next_waiting_score(queue_name, priority)
        # Get the Redis key for the waiting queue's Sorted Set.
        key: str = self._waiting_key(queue_name)
        # Add the JSON-serialized payload to the Sorted Set with the calculated score.
        # ZADD updates the score if the member (the JSON string) already exists.
        await self.redis.zadd(key, {self._json_serializer.to_json(waiting_payload): score})
        # Update the job's status to WAITING and save the full payload in the job store.
        await self.job_store.save(queue_name, job_id, waiting_payload)
        return job_id

    async def get_job(self, queue_name: str, job_id: str) -> dict[str, Any] | None:
        """
        Retrieve a single stored job payload directly from Redis job storage.

        Redis maintains a canonical per-job record alongside its queue indexes,
        so point lookups should consult that store directly instead of walking
        the inspection buckets. This avoids race-prone scans and mirrors the
        O(1) lookup expectations BullMQ users have for admin APIs.

        Args:
            queue_name: The queue that owns the job.
            job_id: The identifier of the job to retrieve.

        Returns:
            The stored payload if present, otherwise ``None``.
        """
        job = await self.job_store.load(queue_name, job_id)
        return dict(job) if job is not None else None

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
        while raw := await self.pop_script(keys=[key]):
            # Deserialize the job payload from the JSON string into a dictionary.
            payload: dict[str, Any] = self._json_serializer.to_dict(raw)
            job_id = str(payload["id"])
            stored = await self.job_store.load(queue_name, job_id)
            candidate = stored or payload
            if await self.is_job_cancelled(queue_name, job_id):
                cancelled_payload = {**candidate, "status": "cancelled", "delay_until": None}
                cancelled_payload.pop("result", None)
                await self.job_store.save(queue_name, job_id, cancelled_payload)
                await self.redis.hdel(self._active_key(queue_name), job_id)
                await self.redis.hdel(self._job_heartbeat_key(queue_name), job_id)
                continue
            if has_pending_dependencies(candidate):
                await self.job_store.save(queue_name, job_id, {**candidate, "status": State.WAITING})
                continue
            # Update the job's status to ACTIVE and save the full payload in the job store.
            # Create a new dictionary to avoid modifying the original payload in place.
            active_since = time.time()
            await self.job_store.save(
                queue_name,
                job_id,
                {**candidate, "status": State.ACTIVE, "active_since": active_since},
            )
            # Add the job ID and the current time to the active jobs Hash.
            # This hash tracks jobs currently being processed.
            await self.redis.hset(self._active_key(queue_name), job_id, str(active_since))
            # Return the dequeued job payload as a dictionary.
            return {**candidate, "status": State.ACTIVE, "active_since": active_since}
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

        # Remove any in-flight bookkeeping for this job.
        await self.redis.hdel(self._active_key(queue_name), payload["id"])
        await self.redis.hdel(self._job_heartbeat_key(queue_name), payload["id"])

        # 3) Add to the DLQ Sorted Set
        dlq_key: str = self._dlq_key(queue_name)
        # Preserve explicit terminal status (for example "expired") when provided.
        target_status = payload.get("status")
        if target_status not in {State.FAILED, State.EXPIRED}:
            target_status = State.FAILED
        raw_json = self._json_serializer.to_json({**payload, "status": target_status})
        await self.redis.zadd(dlq_key, {raw_json: time.time()})

        # 4) Persist terminal status in the job store
        await self.job_store.save(queue_name, payload["id"], {**payload, "status": target_status})

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

    def _job_store_key(self, queue_name: str, job_id: str) -> str:
        return self.job_store._key(queue_name, job_id)

    def _job_store_ids_key(self, queue_name: str) -> str:
        return self.job_store._set_key(queue_name)

    def _lifecycle_keys(self, queue_name: str, job_id: str) -> list[str]:
        return [
            self._active_key(queue_name),
            self._job_heartbeat_key(queue_name),
            self._waiting_key(queue_name),
            self._delayed_key(queue_name),
            self._dlq_key(queue_name),
            self._job_store_key(queue_name, job_id),
            self._job_store_ids_key(queue_name),
            self._cancelled_key(queue_name),
        ]

    async def _run_lifecycle_transition(
        self,
        queue_name: str,
        job_id: str,
        action: str,
        payload: dict[str, Any],
        *,
        run_at: float | None = None,
        now: float | None = None,
    ) -> None:
        # Serialize before running the script so serialization failures do not
        # partially acknowledge or move a job.
        payload_json = self._json_serializer.to_json(payload)
        await self._lifecycle_script(
            keys=self._lifecycle_keys(queue_name, job_id),
            args=[
                action,
                job_id,
                payload_json,
                run_at if run_at is not None else 0,
                now if now is not None else time.time(),
            ],
        )

    async def complete_active_job(self, queue_name: str, payload: dict[str, Any], result: Any) -> None:
        job_id = str(payload["id"])
        await self._run_lifecycle_transition(
            queue_name,
            job_id,
            "complete",
            {**payload, "status": State.COMPLETED, "result": result},
        )

    async def retry_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        await self._delay_active_job(queue_name, payload, run_at, action="retry")

    async def defer_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        await self._delay_active_job(queue_name, payload, run_at, action="defer")

    async def _delay_active_job(self, queue_name: str, payload: dict[str, Any], run_at: float, *, action: str) -> None:
        job_id = str(payload["id"])
        await self._run_lifecycle_transition(
            queue_name,
            job_id,
            action,
            {**payload, "status": State.DELAYED, "delay_until": run_at},
            run_at=run_at,
        )

    async def fail_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        target_status = payload.get("status")
        if target_status not in {State.FAILED, State.EXPIRED}:
            target_status = State.FAILED
        job_id = str(payload["id"])
        await self._run_lifecycle_transition(
            queue_name,
            job_id,
            "fail",
            {**payload, "status": target_status},
        )

    async def expire_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        job_id = str(payload["id"])
        await self._run_lifecycle_transition(
            queue_name,
            job_id,
            "expire",
            {**payload, "status": State.EXPIRED},
        )

    async def cancel_active_job(self, queue_name: str, payload: dict[str, Any]) -> None:
        job_id = str(payload["id"])
        await self._run_lifecycle_transition(queue_name, job_id, "cancel", payload)

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
        await self.redis.zadd(key, {self._json_serializer.to_json(payload): run_at})
        # Update the job's status to DELAYED and save the full payload in the job store.
        await self.job_store.save(queue_name, payload["id"], {**payload, "status": State.DELAYED})

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
        return [self._json_serializer.to_dict(raw) for raw in raw_jobs]

    async def remove_delayed(self, queue_name: str, job_id: str) -> bool:
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
        removed = False
        # Iterate through the raw job payloads to find the one matching the job_id.
        for raw in all_jobs_raw:
            job: dict[str, Any] = self._json_serializer.to_dict(raw)
            # If the job ID matches.
            if job.get("id") == job_id:
                # Remove the specific member (the JSON string) from the set.
                removed = (await self.redis.zrem(key, raw)) > 0
                # Stop iterating once found and removed.
                break
        return removed

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
            if state == State.ACTIVE:
                job.setdefault("active_since", time.time())
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

    async def save_job_payload(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Persist an updated canonical payload in Redis without requeueing it.

        Args:
            queue_name: The queue that owns the job.
            payload: The full canonical payload to persist.
        """
        await self.job_store.save(queue_name, str(payload["id"]), dict(payload))

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
        return cast(str, job.get("status")) if job else None

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
        await self._store_repeatable(queue_name, job_def, next_run=next_run, paused=bool(job_def.get("paused", False)))

    async def upsert_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Create or update a durable repeatable definition in Redis.

        Args:
            queue_name: The queue that owns the repeatable definition.
            job_def: The logical repeatable definition.

        Returns:
            The next scheduled UNIX timestamp for the definition.
        """
        clean_def = normalize_repeatable_job_def(job_def)
        next_run = compute_next_run(clean_def)
        await self._store_repeatable(queue_name, clean_def, next_run=next_run, paused=False)
        return next_run

    async def remove_repeatable(self, queue_name: str, job_def: dict[str, Any] | str) -> None:
        """
        Asynchronously removes a repeatable job definition from Redis.

        Removes the repeatable job definition from the Redis Sorted Set
        (`queue:{queue_name}:repeatables`). The job definition dictionary is
        serialized to a JSON string to match the member in the Sorted Set.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: The dictionary defining the repeatable job to remove.
        """
        await self._remove_repeatable_members(queue_name, job_def)

    async def get_due_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
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
        key: str = self._repeat_key(queue_name)
        now: float = time.time()
        items: list[tuple[str, float]] = await self.redis.zrangebyscore(key, 0, now, withscores=True)
        due: list[RepeatableInfo] = []
        for raw, score in items:
            job_def = self._json_serializer.to_dict(raw)
            due.append(
                RepeatableInfo(
                    job_def=normalize_repeatable_job_def(job_def),
                    next_run=score,
                    paused=bool(job_def.get("paused", False)),
                )
            )
        return [item for item in due if not item.paused]

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
        pending: list[str] = list(dict.fromkeys(job_dict.get("depends_on", [])))

        # If there are dependencies specified in the input dictionary.
        if pending:
            job_id = str(job_id)
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
            data = await self.job_store.load(queue_name, job_id)
            if data is not None:
                merged = sorted(set(data.get("depends_on", [])) | set(pending))
                data["depends_on"] = merged
                data["status"] = data.get("status") or State.WAITING
                await self.job_store.save(queue_name, job_id, data)
                await self._remove_waiting_member(queue_name, job_id)

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
                    data.pop("depends_on", None)
                    data["status"] = State.WAITING
                    # Enqueue the child job into the main waiting queue.
                    await self.enqueue(queue_name, data)
                    # Emit a local event indicating the job is now ready for processing.
                    await event_emitter.emit("job:ready", {"id": child_id})
                # Delete the child's empty pending dependencies set to clean up.
                await self.redis.delete(pend_key)
            else:
                data = await self.job_store.load(queue_name, child_id)
                if data:
                    remaining = await self.redis.smembers(pend_key)
                    data["depends_on"] = sorted(
                        item.decode("utf-8") if isinstance(item, bytes) else item for item in remaining
                    )
                    await self.job_store.save(queue_name, child_id, data)

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
        for job in jobs:
            await self.enqueue(queue_name, job)

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
        await self._purge_jobs_by_state(queue_name, state, older_than)

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
        payload_json: str = self._json_serializer.to_json(event_payload)
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

        Queues can be discovered in two ways:
        - By worker heartbeats stored under keys matching `queue:*:heartbeats`.
        - By waiting job sets created when tasks are enqueued under `queue:*:waiting`.

        This method scans for both patterns and merges the queue names.

        Returns:
            A list of strings, where each string is the name of a queue.
        """

        def _name_from_key(key: str | bytes | bytearray) -> str | None:
            queue_name = None
            key_str = key.decode() if isinstance(key, (bytes, bytearray)) else key
            parts = key_str.split(":", 2)
            if len(parts) == 3:
                # Expected format: 'queue:{name}:heartbeats'
                _, queue_name, _ = parts
            return queue_name

        queue_names: set[str] = set()

        # 1) Queues discovered from worker heartbeats (new key format)
        async for full_key in self.redis.scan_iter(match="queue:*:worker_heartbeats"):
            queue_name = _name_from_key(full_key)
            if queue_name is not None:
                queue_names.add(queue_name)

        # 2) Queues discovered from worker heartbeats (legacy key format)
        async for full_key in self.redis.scan_iter(match="queue:*:heartbeats"):
            queue_name = _name_from_key(full_key)
            if queue_name is not None:
                queue_names.add(queue_name)

        # 3) Queues discovered from waiting job sorted sets
        async for full_key in self.redis.scan_iter(match="queue:*:waiting"):
            queue_name = _name_from_key(full_key)
            if queue_name is not None:
                queue_names.add(queue_name)

        # 4) Queues discovered from delayed job sorted sets
        async for full_key in self.redis.scan_iter(match="queue:*:delayed"):
            queue_name = _name_from_key(full_key)
            if queue_name is not None:
                queue_names.add(queue_name)

        # 5) Queues discovered from DLQ sorted sets
        async for full_key in self.redis.scan_iter(match="queue:*:dlq"):
            queue_name = _name_from_key(full_key)
            if queue_name is not None:
                queue_names.add(queue_name)

        return sorted(queue_names)

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

        Args:
            queue_name: The name of the queue.
            state: The job state to filter by.

        Returns:
            A list of job dictionaries.
        """
        normalized = normalize_job_type(state)

        if normalized == "waiting-children":
            jobs = await self.job_store.all_jobs(queue_name)
            return [job for job in jobs if matches_job_type(job, normalized)]

        if normalized in {"paused", "prioritized"}:
            return []

        key_map = {
            "waiting": self._waiting_key(queue_name),
            "delayed": self._delayed_key(queue_name),
            "failed": self._dlq_key(queue_name),  # DLQ is for failed jobs
        }

        key = key_map.get(normalized)
        if key is not None:
            raw_jobs: list[str] = await self.redis.zrange(key, 0, -1)
            jobs: list[dict[str, Any]] = []

            for raw in raw_jobs:
                try:
                    payload = self._json_serializer.to_dict(raw)
                except Exception:
                    continue

                job_id = payload.get("id")
                stored = await self.job_store.load(queue_name, str(job_id)) if job_id is not None else None
                candidate = stored or payload
                if matches_job_type(candidate, normalized):
                    jobs.append(candidate)
            return jobs

        jobs = await self.job_store.jobs_by_status(queue_name, normalized)
        return [job for job in jobs if matches_job_type(job, normalized)]

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
            job: dict[str, Any] = self._json_serializer.to_dict(raw)
            # Check if the job ID matches the one to retry.
            if job.get("id") == job_id:
                # Atomically remove the job from the DLQ. ZREM returns the number of elements removed.
                removed_count: int = await self.redis.zrem(key, raw)
                # If the job was successfully removed from the DLQ.
                if removed_count > 0:
                    # Enqueue the job back into the waiting queue.
                    await self.enqueue(queue_name, self._prepare_retry_payload(job, job_id))
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
        stored = await self.job_store.load(queue_name, job_id)
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
        active_removed = await self.redis.hdel(self._active_key(queue_name), job_id)
        heartbeat_removed = await self.redis.hdel(self._job_heartbeat_key(queue_name), job_id)
        if active_removed > 0 or heartbeat_removed > 0:
            removed = True
        if await self.redis.srem(self._cancelled_key(queue_name), job_id) > 0:
            removed = True

        if stored is not None:
            removed = True
            for parent in stored.get("depends_on", []):
                await self.redis.srem(f"deps:{queue_name}:parent:{parent}", job_id)
        if removed:
            await self.redis.delete(f"deps:{queue_name}:{job_id}:pending")
            await self.redis.delete(f"deps:{queue_name}:parent:{job_id}")
            await self.job_store.delete(queue_name, job_id)
        # Return whether the job was found and removed from any queue.
        return removed

    async def atomic_add_flow(
        self,
        queue_name: str,
        job_dicts: list[dict[str, Any]],
        dependency_links: list[tuple[str, str]],
    ) -> Any:
        """
        Adds multiple jobs and registers their parent-child dependencies.

        Jobs with unresolved parents are saved in the canonical job store and
        registered in the same pending/parent Redis sets used by
        ``resolve_dependency``. Only root jobs, or children whose dependencies
        have already been cleared, are added to the runnable waiting sorted set.

        Args:
            queue_name: The name of the queue.
            job_dicts: A list of job payloads (dictionaries) to be enqueued. Each
                       dictionary must contain an "id" key.
            dependency_links: A list of (parent_id, child_id) tuples representing
                              the dependencies.

        Returns:
            A list of the IDs (strings) of the jobs that were successfully
            created.
        """
        deps_by_child: dict[str, set[str]] = {}
        for parent_id, child_id in dependency_links:
            deps_by_child.setdefault(child_id, set()).add(parent_id)

        created_ids: list[str] = []
        for payload in job_dicts:
            job_id = str(payload["id"])
            deps = set(payload.get("depends_on", [])) | deps_by_child.get(job_id, set())
            stored = {**payload, "status": State.WAITING}
            if deps:
                stored["depends_on"] = sorted(deps)
            else:
                stored.pop("depends_on", None)
            created_ids.append(await self.enqueue(queue_name, stored))

        for child_id, parents in deps_by_child.items():
            await self.add_dependencies(queue_name, {"id": child_id, "depends_on": sorted(parents)})

        return created_ids

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
        key: str = self._job_heartbeat_key(queue_name)
        # Store the timestamp (converted to string by Redis) associated with
        # the job_id in the heartbeats Hash using HSET.
        await self.redis.hset(key, job_id, str(timestamp))

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
        seen: set[tuple[str, str]] = set()
        # Scan for all Redis keys that are heartbeat Hashes across all queues.
        # scan_iter is a generator that yields keys matching the pattern.
        async for full_key in self.redis.scan_iter(match="queue:*:heartbeats"):
            key_str = full_key.decode() if isinstance(full_key, bytes) else full_key
            # Split the key to extract the queue name. Expected format: 'queue:queue_name:heartbeats'.
            parts: list[str] = key_str.split(":")
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
                try:
                    ts = float(ts_str)
                except (TypeError, ValueError):
                    # Ignore non-job heartbeat values (for example legacy worker payloads).
                    continue
                # Check if the heartbeat timestamp is older than the threshold.
                if ts < older_than:
                    # If stalled, load the full job payload from the job store.
                    job_data: dict[str, Any] | None = await self.job_store.load(queue_name, job_id)
                    # If the job data was successfully loaded (it exists in the job store).
                    if job_data is not None:
                        # Add the queue name and job data to the list of stalled jobs.
                        stalled.append({"queue_name": queue_name, "job_data": job_data})
                        seen.add((queue_name, job_id))
        async for full_key in self.redis.scan_iter(match="queue:*:active"):
            key_str = full_key.decode() if isinstance(full_key, bytes) else full_key
            parts = key_str.split(":")
            if len(parts) != 3:
                continue
            _, queue_name, _ = parts
            active_jobs: dict[str, str] = await self.redis.hgetall(full_key)
            for job_id, ts_str in active_jobs.items():
                if (queue_name, job_id) in seen:
                    continue
                try:
                    active_since = float(ts_str)
                except (TypeError, ValueError):
                    continue
                if active_since >= older_than:
                    continue
                heartbeat = await self.redis.hget(self._job_heartbeat_key(queue_name), job_id)
                if heartbeat is not None:
                    try:
                        if float(heartbeat) >= older_than:
                            continue
                    except (TypeError, ValueError):
                        pass
                job_data = await self.job_store.load(queue_name, job_id)
                if job_data is not None and job_data.get("status") == State.ACTIVE:
                    stalled.append({"queue_name": queue_name, "job_data": job_data})
                    seen.add((queue_name, job_id))
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
        retry_payload = dict(job_data)
        retry_payload["status"] = State.WAITING
        # Serialize the job data payload to a JSON string.
        payload: str = self._json_serializer.to_json(retry_payload)
        # Get the job's priority for the Sorted Set score, defaulting to 0.
        # Note: The original code uses priority 0 here for re-enqueued stalled jobs.
        score: float = retry_payload.get("priority", 5) * 1e16 + time.time()
        # Create a Redis pipeline for batching commands.
        pipe: redis.client.Pipeline = self.redis.pipeline()
        # Add the job payload to the waiting queue Sorted Set with the determined score.
        await pipe.zadd(waiting_key, {payload: score})
        # Clean up the heartbeat record for this job ID from the heartbeats Hash.
        await pipe.hdel(self._job_heartbeat_key(queue_name), retry_payload["id"])
        await pipe.hdel(self._active_key(queue_name), retry_payload["id"])
        # Execute all commands in the pipeline atomically.
        await pipe.execute()
        await self.job_store.save(queue_name, retry_payload["id"], retry_payload)

    async def list_delayed(self, queue_name: str) -> list[DelayedInfo]:
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
        out: list[DelayedInfo] = []
        # Iterate through each (raw_payload, score) tuple.
        for raw, score in items:
            # raw is the serialized job dict. Deserialize it.
            job = self._json_serializer.to_dict(raw)
            # Append a dictionary containing the job ID, full payload, and run_at time.
            out.append(DelayedInfo(job_id=job["id"], run_at=score, payload=job))
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
            jd = self._json_serializer.to_dict(raw)
            # Extract the 'paused' status from the job definition, defaulting to False.
            paused = jd.get("paused", False)
            # Append a new RepeatableInfo instance to the output list.
            out.append(RepeatableInfo(job_def=normalize_repeatable_job_def(jd), next_run=score, paused=paused))
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
        key = self._repeat_key(queue_name)
        items = await self.redis.zrange(key, 0, -1, withscores=True)
        target_id = repeatable_identity(job_def)
        for raw, score in items:
            jd = self._json_serializer.to_dict(raw)
            if repeatable_identity(jd) != target_id:
                continue
            await self.redis.zrem(key, raw)
            await self.redis.zadd(
                key, {self._json_serializer.to_json({**normalize_repeatable_job_def(jd), "paused": True}): score}
            )
            return

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
        clean_def = normalize_repeatable_job_def(job_def)
        next_run = compute_next_run(clean_def)
        await self._store_repeatable(queue_name, clean_def, next_run=next_run, paused=False)
        return next_run  # Return the newly computed next run timestamp.

    async def advance_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        """
        Persist the next scheduled run for a repeatable definition after it fires.

        Args:
            queue_name: The queue that owns the repeatable definition.
            job_def: The logical repeatable definition that just fired.

        Returns:
            The newly persisted next-run UNIX timestamp.
        """
        clean_def = normalize_repeatable_job_def(job_def)
        next_run = compute_next_run(clean_def)
        await self._store_repeatable(queue_name, clean_def, next_run=next_run, paused=False)
        return next_run

    async def _store_repeatable(
        self, queue_name: str, job_def: dict[str, Any], *, next_run: float, paused: bool
    ) -> None:
        """
        Store a repeatable definition in Redis after removing previous variants.

        Args:
            queue_name: The queue that owns the repeatable definition.
            job_def: The logical repeatable definition.
            next_run: The timestamp for the next scheduled run.
            paused: Whether the stored definition should be marked paused.
        """
        clean_def = normalize_repeatable_job_def(job_def)
        await self._remove_repeatable_members(queue_name, clean_def)
        stored_def = {**clean_def, "paused": paused} if paused else clean_def
        await self.redis.zadd(self._repeat_key(queue_name), {self._json_serializer.to_json(stored_def): next_run})

    async def _remove_repeatable_members(self, queue_name: str, job_def: dict[str, Any] | str) -> None:
        """
        Remove all Redis members that represent the same logical repeatable definition.

        Args:
            queue_name: The queue that owns the repeatable definition.
            job_def: Either the logical definition or its stable identifier.
        """
        key = self._repeat_key(queue_name)
        target_id = job_def if isinstance(job_def, str) else repeatable_identity(job_def)
        items = await self.redis.zrange(key, 0, -1)
        for raw in items:
            jd = self._json_serializer.to_dict(raw)
            if repeatable_identity(jd) == target_id:
                await self.redis.zrem(key, raw)

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
            job = self._json_serializer.to_dict(raw)
            if job.get("id") == job_id:
                await self.redis.zrem(wait_key, raw)
                removed = True

        delayed_key = self._delayed_key(queue_name)
        raw_jobs = await self.redis.zrange(delayed_key, 0, -1)
        for raw in raw_jobs:
            job = self._json_serializer.to_dict(raw)
            if job.get("id") == job_id:
                await self.redis.zrem(delayed_key, raw)
                removed = True

        active_removed = await self.redis.hdel(self._active_key(queue_name), job_id)
        heartbeat_removed = await self.redis.hdel(self._job_heartbeat_key(queue_name), job_id)
        if active_removed > 0 or heartbeat_removed > 0:
            removed = True

        await self.redis.sadd(self._cancelled_key(queue_name), job_id)
        stored = await self.job_store.load(queue_name, job_id)
        if stored is not None:
            cancelled_payload = {**stored, "status": "cancelled", "delay_until": None}
            cancelled_payload.pop("result", None)
            await self.job_store.save(queue_name, job_id, cancelled_payload)
        return removed

    async def is_job_cancelled(self, queue_name: str, job_id: str) -> bool:
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
        return bool(await self.redis.sismember(self._cancelled_key(queue_name), job_id))

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
        payload = self._json_serializer.to_json({"heartbeat": timestamp, "concurrency": concurrency})
        key = self._worker_heartbeat_key(queue)
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
        # Scan worker-heartbeat keys and remove the worker field.
        async for key in self.redis.scan_iter(match="queue:*:worker_heartbeats"):
            await self.redis.hdel(key, worker_id)

        # Compatibility cleanup for legacy key format.
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
        async for full_key in self.redis.scan_iter(match="queue:*:worker_heartbeats"):
            key_str = full_key.decode() if isinstance(full_key, bytes) else full_key
            _, queue_name, _ = key_str.split(":", 2)
            heartbeats = await self.redis.hgetall(full_key)
            for wid, data in heartbeats.items():
                worker_id = wid.decode() if isinstance(wid, bytes) else wid
                payload = self._json_serializer.to_dict(data)
                if not isinstance(payload, dict):
                    continue
                timestamp = float(payload.get("heartbeat", 0))
                if now - timestamp <= monkay.settings.heartbeat_ttl:
                    infos.append(
                        WorkerInfo(
                            id=worker_id,
                            queue=queue_name,
                            concurrency=int(payload.get("concurrency", 0)),
                            heartbeat=timestamp,
                        )
                    )

        # Legacy fallback: parse worker entries that may still exist in queue:*:heartbeats.
        async for full_key in self.redis.scan_iter(match="queue:*:heartbeats"):
            key_str = full_key.decode() if isinstance(full_key, bytes) else full_key
            _, queue_name, _ = key_str.split(":", 2)
            heartbeats = await self.redis.hgetall(full_key)
            for wid, data in heartbeats.items():
                worker_id = wid.decode() if isinstance(wid, bytes) else wid
                payload = self._json_serializer.to_dict(data)
                if not isinstance(payload, dict):
                    continue
                timestamp = float(payload.get("heartbeat", 0))
                if now - timestamp <= monkay.settings.heartbeat_ttl:
                    infos.append(
                        WorkerInfo(
                            id=worker_id,
                            queue=queue_name,
                            concurrency=int(payload.get("concurrency", 0)),
                            heartbeat=timestamp,
                        )
                    )
        return infos

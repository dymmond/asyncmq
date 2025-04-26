import json
import time
from typing import Any, List, Optional

from redis import asyncio as redis

from asyncmq.backends.base import BaseBackend
from asyncmq.event import event_emitter
from asyncmq.stores.redis_store import RedisJobStore

# Lua script to atomically pop the next job from a sorted set
# KEYS[1] = waiting_key
# Returns the job payload or nil
POP_SCRIPT = """
local items = redis.call('ZRANGE', KEYS[1], 0, 0)
if #items == 0 then
  return nil
end
redis.call('ZREM', KEYS[1], items[1])
return items[1]
"""

class RedisBackend(BaseBackend):
    def __init__(self, redis_url: str = "redis://localhost"):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.job_store = RedisJobStore(redis_url)

        # Prepare the Lua script
        self.pop_script = self.redis.register_script(POP_SCRIPT)

    def _waiting_key(self, name: str) -> str:
        return f"queue:{name}:waiting"

    def _active_key(self, name: str) -> str:
        return f"queue:{name}:active"

    def _delayed_key(self, name: str) -> str:
        return f"queue:{name}:delayed"

    def _dlq_key(self, name: str) -> str:
        return f"queue:{name}:dlq"

    def _repeat_key(self, name: str) -> str:
        return f"queue:{name}:repeatables"

    async def enqueue(self, queue_name: str, payload: dict):
        # score: (priority * 1e16) + enqueue timestamp
        priority = payload.get('priority', 5)
        score = priority * 1e16 + time.time()
        key = self._waiting_key(queue_name)
        await self.redis.zadd(key, {json.dumps(payload): score})
        payload['status'] = 'waiting'
        await self.job_store.save(queue_name, payload['id'], payload)

    async def dequeue(self, queue_name: str) -> Optional[dict]:
        key = self._waiting_key(queue_name)
        raw = await self.pop_script(keys=[key])
        if raw:
            payload = json.loads(raw)
            # mark as active and track in a hash if desired
            await self.redis.hset(self._active_key(queue_name), payload['id'], time.time())
            await self.job_store.save(queue_name, payload['id'], {**payload, 'status': 'active'})
            return payload
        return None

    async def move_to_dlq(self, queue_name: str, payload: dict):
        key = self._dlq_key(queue_name)
        await self.redis.zadd(key, {json.dumps({**payload, 'status': 'failed'}): time.time()})
        await self.job_store.save(queue_name, payload['id'], {**payload, 'status': 'failed'})

    async def ack(self, queue_name: str, job_id: str):
        # remove from active hash
        await self.redis.hdel(self._active_key(queue_name), job_id)
        # optional cleanup of job_store

    async def enqueue_delayed(self, queue_name: str, payload: dict, run_at: float):
        key = self._delayed_key(queue_name)
        await self.redis.zadd(key, {json.dumps(payload): run_at})
        payload['status'] = 'delayed'
        await self.job_store.save(queue_name, payload['id'], payload)

    async def get_due_delayed(self, queue_name: str) -> List[dict]:
        key = self._delayed_key(queue_name)
        now = time.time()
        raw_jobs = await self.redis.zrangebyscore(key, 0, now)
        return [json.loads(raw) for raw in raw_jobs]

    async def remove_delayed(self, queue_name: str, job_id: str):
        key = self._delayed_key(queue_name)
        all_jobs = await self.redis.zrange(key, 0, -1)
        for raw in all_jobs:
            job = json.loads(raw)
            if job['id'] == job_id:
                await self.redis.zrem(key, raw)
                break

    async def update_job_state(self, queue_name: str, job_id: str, state: str):
        job = await self.job_store.load(queue_name, job_id)
        if job:
            job['status'] = state
            await self.job_store.save(queue_name, job_id, job)

    async def save_job_result(self, queue_name: str, job_id: str, result: Any):
        job = await self.job_store.load(queue_name, job_id)
        if job:
            job['result'] = result
            await self.job_store.save(queue_name, job_id, job)

    async def get_job_state(self, queue_name: str, job_id: str) -> Optional[str]:
        job = await self.job_store.load(queue_name, job_id)
        return job.get('status') if job else None

    async def get_job_result(self, queue_name: str, job_id: str) -> Optional[Any]:
        job = await self.job_store.load(queue_name, job_id)
        return job.get('result') if job else None

    # Repeatable jobs persistence
    async def add_repeatable(self, queue_name: str, job_def: dict, next_run: float):
        key = self._repeat_key(queue_name)
        payload = json.dumps(job_def)
        await self.redis.zadd(key, {payload: next_run})

    async def remove_repeatable(self, queue_name: str, job_def: dict):
        key = self._repeat_key(queue_name)
        payload = json.dumps(job_def)
        await self.redis.zrem(key, payload)

    async def get_due_repeatables(self, queue_name: str) -> List[dict]:
        key = self._repeat_key(queue_name)
        now = time.time()
        raw = await self.redis.zrangebyscore(key, 0, now)
        return [json.loads(item) for item in raw]

    async def add_dependencies(self, queue_name: str, job_dict: dict):
        pend_key = f"deps:{queue_name}:{job_dict['id']}:pending"
        children_key = f"deps:{queue_name}:children:"
        pending = job_dict.get('depends_on', [])
        if pending:
            await self.redis.sadd(pend_key, *pending)
            for parent in pending:
                await self.redis.sadd(f"deps:{queue_name}:parent:{parent}", job_dict['id'])

    async def resolve_dependency(self, queue_name: str, parent_id: str):
        child_set = f"deps:{queue_name}:parent:{parent_id}"
        children = await self.redis.smembers(child_set)
        for child in children:
            pend_key = f"deps:{queue_name}:{child}:pending"
            await self.redis.srem(pend_key, parent_id)
            rem = await self.redis.scard(pend_key)
            if rem == 0:
                data = await self.job_store.load(queue_name, child)
                if data:
                    await self.enqueue(queue_name, data)
                    await event_emitter.emit('job:ready', {'id': child})
                await self.redis.delete(pend_key)
        await self.redis.delete(child_set)

    # Pause/Resume
    async def pause_queue(self, queue_name: str):
        await self.redis.set(f"queue:{queue_name}:paused", 1)

    async def resume_queue(self, queue_name: str):
        await self.redis.delete(f"queue:{queue_name}:paused")

    async def is_queue_paused(self, queue_name: str) -> bool:
        return bool(await self.redis.exists(f"queue:{queue_name}:paused"))

    # Progress
    async def save_job_progress(self, queue_name: str, job_id: str, progress: float):
        data = await self.job_store.load(queue_name, job_id)
        if data:
            data['progress'] = progress
            await self.job_store.save(queue_name, job_id, data)

    # Bulk
    async def bulk_enqueue(self, queue_name: str, jobs: List[dict]):
        pipe = self.redis.pipeline()
        for job in jobs:
            raw = json.dumps(job)
            score = job.get('priority', 5) * 1e16 + time.time()
            pipe.zadd(f"queue:{queue_name}:waiting", {raw: score})
            pipe.call('SADD', f"jobs:{queue_name}:ids", job['id'])
            pipe.call('SET', f"job:data:{queue_name}:{job['id']}", raw)
        await pipe.execute()

    async def purge(self, queue_name: str, state: str, older_than: Optional[float] = None):
        jobs = await self.job_store.jobs_by_status(queue_name, state)
        for job in jobs:
            ts = job.get('completed_at', time.time())
            if older_than is None or ts < older_than:
                # remove from store
                await self.job_store.delete(queue_name, job['id'])
                # remove from queue set
                await self.redis.zrem(f"queue:{queue_name}:{state}", json.dumps(job))

    # Events
    async def emit_event(self, event: str, data: dict):
        # Use Redis pub/sub
        channel = 'asyncmq:events'
        payload = json.dumps({'event': event, 'data': data, 'ts': time.time()})
        # local emit
        await event_emitter.emit(event, data)
        await self.redis.publish(channel, payload)

    # Locking
    async def create_lock(self, key: str, ttl: int):
        # Return a Redis-based lock
        lock = self.redis.lock(key, timeout=ttl)
        return lock

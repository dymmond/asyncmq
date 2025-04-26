import random
from typing import Any, Dict, List, Optional

from asyncmq.event import event_emitter
from asyncmq.job import Job


# -------------------------------------------------
# 1. DEPENDENCY GRAPHS
# -------------------------------------------------
async def add_dependencies(backend, queue: str, job: Job):
    """
    Persist a job's dependencies and register children for resolution.
    Requires backend.add_dependencies(queue, job_dict).
    """
    if not job.depends_on:
        return
    await backend.add_dependencies(queue, job.to_dict())

async def resolve_dependency(backend, queue: str, parent_id: str):
    """
    Check for any jobs whose dependencies are now all satisfied,
    enqueue them via backend.enqueue().
    Requires backend.resolve_dependency(queue, parent_id).
    """
    await backend.resolve_dependency(queue, parent_id)

# -------------------------------------------------
# 2. QUEUE PAUSE/RESUME
# -------------------------------------------------
async def pause_queue(backend, queue: str):
    """
    Pause consumption from the given queue.
    Requires backend.pause_queue(queue).
    """
    await backend.pause_queue(queue)

async def resume_queue(backend, queue: str):
    """
    Resume consumption from the given queue.
    Requires backend.resume_queue(queue).
    """
    await backend.resume_queue(queue)

async def is_queue_paused(backend, queue: str) -> bool:
    """
    Check if a queue is currently paused.
    Requires backend.is_queue_paused(queue).
    """
    return await backend.is_queue_paused(queue)

# -------------------------------------------------
# 3. BACKOFF WITH JITTER
# -------------------------------------------------
def jittered_backoff(base_delay: float, attempt: int, jitter: float = 0.1) -> float:
    """
    Compute exponential backoff with a jitter fraction.
    """
    delay = base_delay * (2 ** (attempt - 1))
    jitter_amount = delay * jitter
    return delay + random.uniform(-jitter_amount, jitter_amount)

# -------------------------------------------------
# 4. JOB PROGRESS EVENTS
# -------------------------------------------------
async def report_progress(
    backend, queue: str, job: Job, pct: float, info: Any = None
):
    """
    Persist and emit progress for a job.
    Requires backend.save_job_progress(queue, job_id, pct).
    """
    await backend.save_job_progress(queue, job.id, pct)
    # also emit via pub/sub
    await event_emitter.emit(
        "job:progress",
        {**job.to_dict(), "progress": pct, "info": info},
    )

# Attach to Job if desired
Job.report_progress = report_progress

# -------------------------------------------------
# 5. BULK OPERATIONS & AUTO-CLEANUP
# -------------------------------------------------
async def bulk_enqueue(backend, queue: str, jobs: List[Dict[str, Any]]):
    """
    Enqueue many jobs in one operation.
    Requires backend.bulk_enqueue(queue, jobs).
    """
    await backend.bulk_enqueue(queue, jobs)

async def purge_jobs(backend, queue: str, state: str, older_than: Optional[float] = None):
    """
    Remove jobs by state (and optionally older than a timestamp).
    Requires backend.purge(queue, state, older_than).
    """
    await backend.purge(queue, state, older_than)

# -------------------------------------------------
# 6. DISTRIBUTED EVENTS & MONITORING
# -------------------------------------------------
async def emit_event(backend, event: str, data: dict):
    """
    Emit an event locally and (optionally) via backend pub/sub.
    Requires backend.emit_event(event, data).
    """
    # Local handlers
    await event_emitter.emit(event, data)
    # Distributed
    if hasattr(backend, 'emit_event'):
        await backend.emit_event(event, data)

# -------------------------------------------------
# 7. DISTRIBUTED LOCKING / EXACTLY-ONCE
# -------------------------------------------------
class Lock:
    """
    Abstracted lock object. Backends should provide create_lock(key, ttl).
    """
    def __init__(self, lock_obj: Any):
        self._lock = lock_obj

    async def acquire(self) -> bool:
        return await self._lock.acquire()

    async def release(self) -> bool:
        return await self._lock.release()

async def create_lock(backend, key: str, ttl: int = 30) -> Lock:
    """
    Create a lock for this key via backend.
    Requires backend.create_lock(key, ttl).
    """
    lock_obj = await backend.create_lock(key, ttl)
    return Lock(lock_obj)



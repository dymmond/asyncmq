import time
import traceback
from typing import TYPE_CHECKING

import anyio

from asyncmq.event import event_emitter
from asyncmq.job import Job
from asyncmq.task import TASK_REGISTRY

if TYPE_CHECKING:
    from asyncmq.queue import Queue


async def process_job(
    queue_name: str,
    backend,
    limiter: anyio.CapacityLimiter,
    rate_limiter=None,
):
    """
    Pull jobs off `queue_name`, obeying pause, concurrency-limiter and (optionally) a rate limiter.
    Spawns handle_job tasks under its own TaskGroup.
    """
    async with anyio.create_task_group() as tg:
        while True:
            if await backend.is_queue_paused(queue_name):
                await anyio.sleep(0.5)
                continue

            async with limiter:
                raw_job = await backend.dequeue(queue_name)
                if not raw_job:
                    await anyio.sleep(0.1)
                    continue

                # run main work
                tg.start_soon(handle_job, queue_name, backend, raw_job, rate_limiter)

                # register dependencies and re-spawn if needed
                if raw_job.get("depends_on"):
                    await backend.add_dependencies(queue_name, raw_job)
                    tg.start_soon(handle_job, queue_name, backend, raw_job, rate_limiter)


async def handle_job(
    queue_name: str,
    backend,
    raw_job: dict,
    rate_limiter=None,
):
    job = Job.from_dict(raw_job)

    # TTL check
    if job.is_expired():
        job.status = "expired"
        await event_emitter.emit("job:expired", job.to_dict())
        await backend.move_to_dlq(queue_name, job.to_dict())
        return

    # Delay‐until
    if job.delay_until and time.time() < job.delay_until:
        await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)
        return

    try:
        # optional rate‐limit
        if rate_limiter:
            await rate_limiter.acquire()

        # mark active
        job.status = "active"
        await backend.update_job_state(queue_name, job.id, "active")
        await event_emitter.emit("job:started", job.to_dict())

        # execute user task
        task_meta = TASK_REGISTRY[job.task_id]
        result = await task_meta["func"](*job.args, **job.kwargs)

        # success path
        job.status = "completed"
        job.result = result
        await backend.ack(queue_name, job.id)
        await backend.save_job_result(queue_name, job.id, result)
        await backend.update_job_state(queue_name, job.id, "completed")
        await event_emitter.emit("job:completed", job.to_dict())

        # unlock dependents
        await backend.resolve_dependency(queue_name, job.id)

    except Exception:
        traceback.print_exc()
        # retry logic
        job.retries += 1
        # monotonic from AnyIO
        job.last_attempt = anyio.current_time()

        if job.retries > job.max_retries:
            job.status = "failed"
            await event_emitter.emit("job:failed", job.to_dict())
            await backend.move_to_dlq(queue_name, job.to_dict())
        else:
            delay = job.next_retry_delay()
            job.delay_until = time.time() + delay
            job.status = "delayed"
            await backend.update_job_state(queue_name, job.id, "delayed")
            await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)


class Worker:
    """
    Convenience wrapper to start and stop a worker programmatically under AnyIO.
    """
    def __init__(self, queue: "Queue"):
        self.queue = queue
        self._cancel_scope: anyio.CancelScope | None = None

    async def _run_with_scope(self):
        # Create a CancelScope and save it so stop() can cancel it
        with anyio.CancelScope() as scope:
            self._cancel_scope = scope
            await self.queue.run()

    def start(self) -> None:
        """
        Blocks until the queue runner finishes or stop() is called.
        Call this from a background thread if you don't want to block your main thread.
        """
        anyio.run(self._run_with_scope)

    def stop(self) -> None:
        """
        Cancel the running worker, causing start() to return.
        Safe to call from any thread.
        """
        if self._cancel_scope is not None:
            self._cancel_scope.cancel()

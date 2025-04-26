import asyncio
import time
import traceback

from typing import Optional, TYPE_CHECKING
from asyncmq.event import event_emitter
from asyncmq.task import TASK_REGISTRY
from asyncmq.job import Job

if TYPE_CHECKING:
    from asyncmq.queue import Queue


async def process_job(queue_name: str, backend, semaphore: asyncio.Semaphore, rate_limiter=None):
    while True:
        # don’t even try to pull if this queue is paused
        if await backend.is_queue_paused(queue_name):
            await asyncio.sleep(0.5)
            continue

        async with semaphore:
            raw_job = await backend.dequeue(queue_name)
            if not raw_job:
                await asyncio.sleep(0.1)
                continue

            asyncio.create_task(handle_job(queue_name, backend, raw_job, rate_limiter=rate_limiter))

            # register any declared dependencies before actual execution
            if raw_job.get("depends_on"):
                await backend.add_dependencies(queue_name, raw_job)
                asyncio.create_task(
                    handle_job(queue_name, backend, raw_job, rate_limiter=rate_limiter)
                )


async def handle_job(queue_name: str, backend, raw_job: dict, rate_limiter=None):
    job = Job.from_dict(raw_job)

    # Check TTL
    if job.is_expired():
        # move to DLQ persistently
        job.status = 'expired'
        await event_emitter.emit('job:expired', job.to_dict())
        await backend.move_to_dlq(queue_name, job.to_dict())
        return

    # Respect delay_until
    if job.delay_until and time.time() < job.delay_until:
        # re-schedule persistently
        await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)
        return

    # ... existing dependency logic ...
    try:
        # Rate limiter
        if rate_limiter:
            await rate_limiter.acquire()

        # Start
        job.status = 'active'
        await backend.update_job_state(queue_name, job.id, 'active')
        await event_emitter.emit('job:started', job.to_dict())

        # Execute
        task_meta = TASK_REGISTRY[job.task_id]
        result = await task_meta['func'](*job.args, **job.kwargs)

        # Success
        job.status = 'completed'
        job.result = result
        await backend.ack(queue_name, job.id)
        await backend.save_job_result(queue_name, job.id, result)
        await backend.update_job_state(queue_name, job.id, 'completed')
        await event_emitter.emit('job:completed', job.to_dict())

        # once this job is done, unlock any dependents
        await backend.resolve_dependency(queue_name, job.id)

    except Exception:
        traceback.print_exc()
        job.retries += 1
        job.last_attempt = asyncio.get_event_loop().time()

        if job.retries > job.max_retries:
            job.status = 'failed'
            await event_emitter.emit('job:failed', job.to_dict())
            await backend.move_to_dlq(queue_name, job.to_dict())
        else:
            # schedule retry persistently
            delay = job.next_retry_delay()
            job.delay_until = time.time() + delay
            job.status = 'delayed'
            await backend.update_job_state(queue_name, job.id, 'delayed')
            await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)


class Worker:
    """
    Convenience wrapper to start and stop a worker programmatically.
    """
    def __init__(
        self,
        queue: "Queue",
    ):
        self.queue = queue
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._worker_task: Optional[asyncio.Task] = None

    def start(self) -> None:
        """Run until cancelled or stopped."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._worker_task = self._loop.create_task(self.queue.run())
        try:
            self._loop.run_until_complete(self._worker_task)
        except KeyboardInterrupt:
            pass
        finally:
            if self._loop and not self._loop.is_closed():
                self._loop.close()

    def stop(self) -> None:
        """Stop the running worker."""
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
        if self._loop and self._loop.is_running():
            self._loop.stop()

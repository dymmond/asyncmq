import inspect
import time
import traceback
from typing import TYPE_CHECKING, Any

import anyio
from anyio import CapacityLimiter

from asyncmq import sandbox
from asyncmq.backends.base import BaseBackend
from asyncmq.conf import settings
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.jobs import Job
from asyncmq.rate_limiter import RateLimiter
from asyncmq.tasks import TASK_REGISTRY

if TYPE_CHECKING:
    from asyncmq.queues import Queue

async def process_job(
    queue_name: str,
    limiter: CapacityLimiter,
    rate_limiter: RateLimiter | None = None,
    backend: BaseBackend | None = None,
) -> None:
    """
    Asynchronously pulls jobs from a specified queue,
    enforces concurrency and rate limits, and handles job processing.
    """
    backend = backend or settings.backend

    async with anyio.create_task_group() as tg:
        while True:
            # Respect queue pause
            if await backend.is_queue_paused(queue_name):
                await anyio.sleep(settings.stalled_check_interval)
                continue

            # Dequeue next job
            raw_job = await backend.dequeue(queue_name)
            if raw_job is None:
                # No job; back off briefly
                await anyio.sleep(0.1)
                continue

            # Schedule job handling with limits
            tg.start_soon(
                _run_with_limits,
                raw_job,
                queue_name,
                backend,
                limiter,
                rate_limiter,
            )

async def _run_with_limits(
    raw_job: dict[str, Any],
    queue_name: str,
    backend: BaseBackend,
    limiter: CapacityLimiter,
    rate_limiter: RateLimiter | None,
) -> None:
    """
    Wrapper to apply concurrency and rate limits before processing the job.
    """
    async with limiter:
        if rate_limiter:
            await rate_limiter.acquire(queue_name, raw_job["id"])
        await handle_job(queue_name, raw_job, backend)

async def handle_job(
    queue_name: str,
    raw_job: dict[str, Any],
    backend: BaseBackend | None = None,
) -> None:
    """
    Asynchronously processes a single job instance, managing TTL, delays,
    sandboxed execution, success and failure paths, and retries.
    """
    backend = backend or settings.backend
    job = Job.from_dict(raw_job)

    # 1. TTL expiration
    if job.is_expired():
        job.status = State.EXPIRED
        await backend.update_job_state(queue_name, job.id, job.status)
        await event_emitter.emit("job:expired", job.to_dict())
        await backend.move_to_dlq(queue_name, job.to_dict())
        return

    # 2. Delayed jobs not yet due
    if job.delay_until and time.time() < job.delay_until:
        await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)
        return

    try:
        # 3. Mark active and emit start event
        job.status = State.ACTIVE
        await backend.update_job_state(queue_name, job.id, job.status)
        await event_emitter.emit("job:started", job.to_dict())

        task_meta: dict[str, Any] = TASK_REGISTRY[job.task_id]
        handler = task_meta["func"]

        # 4. Execute handler: sandbox or direct
        if settings.sandbox_enabled:
            result = await anyio.to_thread.run_sync(
                sandbox.run_handler,
                job.task_id,
                tuple(job.args),
                job.kwargs,
                settings.sandbox_default_timeout,
            )
        else:
            if inspect.iscoroutinefunction(handler):
                result = await handler(*job.args, **job.kwargs)
            else:
                result = handler(*job.args, **job.kwargs)

        # 5. Success path
        job.status = State.COMPLETED
        job.result = result
        # Persist state and result
        await backend.update_job_state(queue_name, job.id, job.status)
        await backend.save_job_result(queue_name, job.id, result)
        await backend.ack(queue_name, job.id)
        await event_emitter.emit("job:completed", job.to_dict())

    except Exception as exc:
        # 6. Failure & retry logic
        tb = traceback.format_exc()
        job.last_error = str(exc)
        job.error_traceback = tb
        job.retries += 1

        if job.retries > job.max_retries:
            job.status = State.FAILED
            await backend.update_job_state(queue_name, job.id, job.status)
            await event_emitter.emit("job:failed", job.to_dict())
            await backend.move_to_dlq(queue_name, job.to_dict())
        else:
            delay: float = job.next_retry_delay()
            job.delay_until = time.time() + delay
            job.status = State.EXPIRED
            await backend.update_job_state(queue_name, job.id, job.status)
            await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)

class Worker:
    """
    A convenience wrapper class designed to start and stop an AsyncMQ queue worker.
    """
    def __init__(self, queue: "Queue") -> None:
        self.queue = queue
        self._cancel_scope: anyio.CancelScope | None = None

    async def _run_with_scope(self) -> None:
        """
        Internal asynchronous method that executes the queue's main processing
        loop within an AnyIO CancelScope.
        """
        with anyio.CancelScope() as scope:
            self._cancel_scope = scope
            # Instantiate limiter and rate limiter from settings
            limiter = CapacityLimiter(settings.worker_concurrency)
            rate_limiter = (
                RateLimiter(settings.rate_limit_config)
                if settings.rate_limit_config is not None
                else None
            )
            await process_job(self.queue.name, limiter, rate_limiter)

    def start(self) -> None:
        """
        Starts the worker synchronously; blocks until cancelled.
        """
        anyio.run(self._run_with_scope)

    def stop(self) -> None:
        """
        Cancels the running worker if active.
        """
        if self._cancel_scope is not None:
            self._cancel_scope.cancel()

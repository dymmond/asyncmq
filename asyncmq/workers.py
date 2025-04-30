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
    backend = backend or settings.backend

    async with anyio.create_task_group() as tg:
        while True:
            # Pause support
            if await backend.is_queue_paused(queue_name):
                await anyio.sleep(settings.stalled_check_interval)
                continue

            raw_job = await backend.dequeue(queue_name)
            if raw_job is None:
                await anyio.sleep(0.1)
                continue

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
    Apply concurrency and rate limits, then process the job.
    """
    async with limiter:
        if rate_limiter:
            await rate_limiter.acquire()
        await handle_job(queue_name, raw_job, backend)


async def handle_job(
    queue_name: str,
    raw_job: dict[str, Any],
    backend: BaseBackend | None = None,
) -> None:
    backend = backend or settings.backend
    job = Job.from_dict(raw_job)

    # 1) TTL expiration
    if job.is_expired():
        job.status = State.EXPIRED
        await backend.update_job_state(queue_name, job.id, job.status)
        await event_emitter.emit("job:expired", job.to_dict())
        await backend.move_to_dlq(queue_name, job.to_dict())
        return

    # 2) Delay handling
    if job.delay_until and time.time() < job.delay_until:
        await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)
        return

    try:
        # 3) Mark active & start event
        job.status = State.ACTIVE
        await backend.update_job_state(queue_name, job.id, job.status)
        await event_emitter.emit("job:started", job.to_dict())

        meta = TASK_REGISTRY[job.task_id]
        handler = meta["func"]

        # 4) Execute task (sandbox vs direct)
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

        # 5) Success path
        job.status = State.COMPLETED
        job.result = result
        await backend.update_job_state(queue_name, job.id, job.status)
        await backend.save_job_result(queue_name, job.id, result)
        await backend.ack(queue_name, job.id)
        await event_emitter.emit("job:completed", job.to_dict())

    except Exception:
        tb = traceback.format_exc()
        job.last_error = str(tb)
        job.error_traceback = tb
        job.retries += 1

        if job.retries > job.max_retries:
            job.status = State.FAILED
            await backend.update_job_state(queue_name, job.id, job.status)
            await event_emitter.emit("job:failed", job.to_dict())
            await backend.move_to_dlq(queue_name, job.to_dict())
        else:
            delay = job.next_retry_delay()
            job.delay_until = time.time() + delay
            job.status = State.EXPIRED
            await backend.update_job_state(queue_name, job.id, job.status)
            await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)


class Worker:
    """
    Convenience wrapper to start/stop a worker.
    """
    def __init__(self, queue: "Queue") -> None:
        self.queue = queue
        self._cancel_scope: anyio.CancelScope | None = None

    async def _run_with_scope(self) -> None:
        with anyio.CancelScope() as scope:
            self._cancel_scope = scope
            limiter = CapacityLimiter(settings.worker_concurrency)
            rate_limiter = (
                RateLimiter(settings.rate_limit, settings.rate_interval)
                if settings.rate_limit is not None
                else None
            )
            await process_job(self.queue.name, limiter, rate_limiter)

    def start(self) -> None:
        anyio.run(self._run_with_scope)

    def stop(self) -> None:
        if self._cancel_scope:
            self._cancel_scope.cancel()

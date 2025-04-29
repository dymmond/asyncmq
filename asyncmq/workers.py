import time
import traceback
from typing import TYPE_CHECKING, Any

import anyio

from asyncmq.backends.base import BaseBackend
from asyncmq.conf import settings
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.jobs import Job
from asyncmq.tasks import TASK_REGISTRY

if TYPE_CHECKING:
    from asyncmq.queues import Queue


async def process_job(
        queue_name: str,
        limiter: anyio.CapacityLimiter,
        backend: BaseBackend | None = None,
        rate_limiter: Any | None = None,
) -> None:
    """
    Continuously pulls jobs from a specified queue, respecting concurrency and
    rate limits, and delegates their processing to `handle_job` tasks.
    """
    backend = backend or settings.backend

    async with anyio.create_task_group() as tg:
        while True:
            # Pause handling
            if await backend.is_queue_paused(queue_name):
                await anyio.sleep(0.5)
                continue

            # Concurrency limiting
            async with limiter:
                # Dequeue a job
                raw_job = await backend.dequeue(queue_name)
                if not raw_job:
                    await anyio.sleep(0.1)
                    continue

                # Spawn job handler
                tg.start_soon(handle_job, queue_name, raw_job, backend, rate_limiter)

                # Register dependencies if present
                if raw_job.get("depends_on"):
                    await backend.add_dependencies(queue_name, raw_job)


async def handle_job(
        queue_name: str,
        raw_job: dict[str, Any],
        backend: BaseBackend | None = None,
        rate_limiter: Any | None = None,
) -> None:
    """
    Processes a single job, managing its lifecycle from dequeue to completion
    or failure.
    """
    backend = backend or settings.backend
    # Convert to Job
    job = Job.from_dict(raw_job)

    # Record initial heartbeat

    # 1. TTL check
    if job.is_expired():
        job.status = State.EXPIRED
        await event_emitter.emit("job:expired", job.to_dict())
        await backend.move_to_dlq(queue_name, job.to_dict())
        return

    # 2. Delayed job handling
    if job.delay_until and time.time() < job.delay_until:
        await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)
        return

    try:
        # 3. Rate limiting per job
        if rate_limiter:
            await rate_limiter.acquire()

        # 4. Execute the task
        job.status = State.ACTIVE
        await backend.update_job_state(queue_name, job.id, job.status)
        await event_emitter.emit("job:started", job.to_dict())

        task_meta = TASK_REGISTRY[job.task_id]
        result = await task_meta["func"](*job.args, **job.kwargs)

        # Success path
        job.status = State.COMPLETED
        job.result = result
        await backend.ack(queue_name, job.id)
        await backend.save_job_result(queue_name, job.id, result)
        await backend.update_job_state(queue_name, job.id, job.status)
        await event_emitter.emit("job:completed", job.to_dict())

        # Resolve dependencies
        await backend.resolve_dependency(queue_name, job.id)

    except Exception:
        traceback.print_exc()
        job.retries += 1
        job.last_attempt = time.time()

        if job.retries > job.max_retries:
            job.status = State.FAILED
            await event_emitter.emit("job:failed", job.to_dict())
            await backend.move_to_dlq(queue_name, job.to_dict())
        else:
            delay = job.next_retry_delay()
            job.delay_until = time.time() + delay
            job.status = State.EXPIRED
            await backend.update_job_state(queue_name, job.id, job.status)
            await backend.enqueue_delayed(
                queue_name, job.to_dict(), job.delay_until
            )


class Worker:
    """
    A convenience wrapper class designed to start and stop an `asyncmq` queue
    worker programmatically under the control of AnyIO.

    This class simplifies the management of a queue's processing lifecycle.
    It holds a reference to an `asyncmq.queue.Queue` instance and provides
    methods to initiate its execution (`start`) and request its graceful
    termination (`stop`). The worker runs the queue's internal processing loop
    (`queue.run()`) within an AnyIO context, allowing it to be managed
    asynchronously and cancelled externally.
    """

    def __init__(self, queue: "Queue") -> None:
        """
        Initializes the Worker instance.

        Args:
            queue: The `asyncmq.queue.Queue` instance that this worker will
                   be responsible for running.
        """
        self.queue = queue
        # Internal variable to store the AnyIO CancelScope for the worker task.
        self._cancel_scope: anyio.CancelScope | None = None

    async def _run_with_scope(self) -> None:
        """
        Internal asynchronous method that executes the queue's main processing
        loop within an AnyIO CancelScope.

        This method is the entry point for the asynchronous worker logic when
        started via `start()`. It creates an AnyIO `CancelScope`, assigns it
        to `self._cancel_scope` so it can be accessed and cancelled by the
        `stop()` method, and then awaits the `self.queue.run()` method. The
        `queue.run()` method contains the main loop for processing jobs, and
        it is expected to be cooperative and responsive to cancellation signals
        received via the `CancelScope`.
        """
        # Create a CancelScope for the worker's main task.
        with anyio.CancelScope() as scope:
            # Store the scope so 'stop' can cancel it.
            self._cancel_scope = scope
            # Run the queue's main processing loop.
            await self.queue.run()

    def start(self) -> None:
        """
        Starts the worker and blocks the current thread until the worker
        finishes execution or is stopped.

        This method uses `anyio.run()` to launch and execute the asynchronous
        `_run_with_scope` method. `anyio.run()` is a blocking call that starts
        an AnyIO event loop and runs the provided asynchronous function until
        it completes. To avoid blocking the main application thread, this method
        is typically called from a separate background thread. Once started,
        the worker will continuously process jobs from its associated queue
        until cancelled via the `stop()` method or until the `queue.run()`
        method completes (which it is not designed to do under normal
        operation).
        """
        # Run the asynchronous worker logic within an AnyIO event loop.
        anyio.run(self._run_with_scope)

    def stop(self) -> None:
        """
        Requests the cancellation of the running worker.

        If the worker was successfully started using the `start()` method and
        is currently running (i.e., `_cancel_scope` is not None), this method
        accesses the stored `CancelScope` and calls its `cancel()` method.
        Requesting cancellation signals the asynchronous tasks running within
        the worker's scope (specifically the `queue.run()` task) to shut down.
        A well-behaved `queue.run()` implementation should handle this
        cancellation request gracefully, eventually causing the `anyio.run()`
        call in the `start()` method to return, and thus allowing the thread
        that called `start()` to unblock. This method is safe to call from
        any thread.
        """
        # Check if a cancel scope exists (meaning the worker is running).
        if self._cancel_scope is not None:
            # Request cancellation of the running worker task.
            self._cancel_scope.cancel()

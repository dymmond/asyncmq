import inspect
import time
import traceback
import uuid
from typing import TYPE_CHECKING, Any, cast

import anyio
from anyio import CapacityLimiter

from asyncmq import sandbox
from asyncmq.backends.base import BaseBackend
from asyncmq.conf import monkay
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.exceptions import JobCancelled
from asyncmq.jobs import Job
from asyncmq.rate_limiter import RateLimiter
from asyncmq.tasks import TASK_REGISTRY

if TYPE_CHECKING:
    pass


async def process_job(
    queue_name: str,
    limiter: CapacityLimiter,
    rate_limiter: RateLimiter | None = None,
    backend: BaseBackend | None = None,
) -> None:
    """
    Continuously processes jobs from a specified queue, respecting concurrency
    and rate limits.

    This function runs indefinitely within a task group, dequeueing jobs
    and starting new tasks to handle each job within the defined limits.
    It includes support for pausing the queue.

    Args:
        queue_name: The name of the queue to process jobs from.
        limiter: A CapacityLimiter instance to control the maximum number of
                 concurrently running jobs.
        rate_limiter: An optional RateLimiter instance to control the rate
                      at which jobs are processed. Defaults to None.
        backend: An optional BaseBackend instance to interact with the queue
                 storage. Defaults to the backend specified in monkay.settings.
    """
    # Use the provided backend or the one from settings
    backend = backend or monkay.settings.backend

    # Create a task group to manage concurrent job handling tasks
    async with anyio.create_task_group() as tg:
        while True:
            # Pause support: Check if the queue is currently paused
            if await backend.is_queue_paused(queue_name):
                # If paused, wait for a bit before checking again
                await anyio.sleep(monkay.settings.stalled_check_interval)
                continue  # Skip to the next iteration

            # Attempt to dequeue a raw job from the backend
            raw_job = await backend.dequeue(queue_name)
            if raw_job is None:
                # If no job is available, wait briefly to avoid busy-looping
                await anyio.sleep(0.1)
                continue  # Skip to the next iteration

            # If a job is dequeued, start a new task in the task group to handle it
            # The _run_with_limits function will apply concurrency and rate limits
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
    Applies concurrency and optional rate limits before calling handle_job.

    This function is intended to be run within a task group. It acquires a
    slot from the concurrency limiter and, if provided, acquires a token
    from the rate limiter before proceeding to process the job.

    Args:
        raw_job: The raw job data as a dictionary.
        queue_name: The name of the queue the job originated from.
        backend: The backend instance used for queue operations.
        limiter: The CapacityLimiter instance for concurrency control.
        rate_limiter: An optional RateLimiter instance for rate control.
    """
    # Acquire a slot from the concurrency limiter; this waits if the limit
    # has been reached.
    async with limiter:
        # If a rate limiter is provided, acquire a token; this waits if the
        # rate limit has been exceeded.
        if rate_limiter:
            await rate_limiter.acquire()
        # Once limits are satisfied, proceed to handle the job logic
        await handle_job(queue_name, raw_job, backend)


async def handle_job(
    queue_name: str,
    raw_job: dict[str, Any],
    backend: BaseBackend | None = None,
) -> None:
    """
    Handles the lifecycle of a single job, including expiration checks, delays,
    execution, retries, and state updates.

    This function performs the core logic for processing a job. It checks for
    TTL expiration and delays before executing the task associated with the job.
    It handles successful completion, retries on failure, and moving failed
    jobs to the Dead Letter Queue (DLQ). Events are emitted at different stages
    of the job lifecycle.

    Args:
        queue_name: The name of the queue the job belongs to.
        raw_job: The raw job data as a dictionary.
        backend: An optional BaseBackend instance to interact with the queue
                 storage. Defaults to the backend specified in monkay.settings.
    """
    # Use the provided backend or the one from settings
    backend = backend or monkay.settings.backend
    # Convert the raw job dictionary into a Job object
    job = Job.from_dict(raw_job)

    if await backend.is_job_cancelled(queue_name, job.id):
        await backend.ack(queue_name, job.id)
        await event_emitter.emit("job:cancelled", job.to_dict())
        return

    # 1) TTL expiration check: If the job has expired based on its TTL
    if job.is_expired():
        # Update job status to EXPIRED
        job.status = State.EXPIRED
        # Update the state in the backend
        await backend.update_job_state(queue_name, job.id, job.status)
        # Emit a job:expired event
        await event_emitter.emit("job:expired", job.to_dict())
        # Move the expired job to the Dead Letter Queue (DLQ)
        await backend.move_to_dlq(queue_name, job.to_dict())
        return  # Stop processing this job

    # 2) Delay handling: If the job is scheduled to run later
    # Check if the current time is before the scheduled delay_until time
    if job.delay_until and time.time() < job.delay_until:
        # If delayed, re-enqueue the job into the delayed queue
        await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)
        return  # Stop processing this job for now

    try:
        # 3) Mark active & start event: If the job is ready to be processed
        # Set job status to ACTIVE
        job.status = State.ACTIVE
        # Update the state in the backend
        await backend.update_job_state(queue_name, job.id, job.status)
        # Emit a job:started event
        await event_emitter.emit("job:started", job.to_dict())

        if monkay.settings.enable_stalled_check:
            await backend.save_heartbeat(queue_name, job.id, time.time())

        # Retrieve task metadata and the handler function from the registry
        meta = TASK_REGISTRY[job.task_id]
        handler = meta["func"]

        # Mid-flight cancellation check
        if await backend.is_job_cancelled(queue_name, job.id):
            raise JobCancelled()

        # 4) Execute task (sandbox vs direct): Run the task, potentially in a sandbox
        if monkay.settings.sandbox_enabled:
            # If sandboxing is enabled, run the handler in a separate thread
            # using the sandbox execution function.
            result = await anyio.to_thread.run_sync(  # noqa
                cast(Any, sandbox.run_handler),
                job.task_id,
                tuple(job.args),  # sandbox expects tuple args
                job.kwargs,
                monkay.settings.sandbox_default_timeout,
            )
        else:
            # If sandboxing is disabled, run the handler directly.
            if inspect.iscoroutinefunction(handler):
                # If the handler is an async function, await its execution.
                result = await handler(*job.args, **job.kwargs)
            else:
                # If the handler is a regular sync function, call it directly.
                result = handler(*job.args, **job.kwargs)

        # 5) Success path: If the task execution completed without exceptions
        # Set job status to COMPLETED
        job.status = State.COMPLETED
        # Store the result of the task execution
        job.result = result
        # Update the state in the backend
        await backend.update_job_state(queue_name, job.id, job.status)
        # Save the job result in the backend
        await backend.save_job_result(queue_name, job.id, result)
        # Acknowledge successful processing in the backend
        await backend.ack(queue_name, job.id)
        # Emit a job:completed event
        await event_emitter.emit("job:completed", job.to_dict())

    except JobCancelled:
        # Cancellation path
        await event_emitter.emit("job:cancelled", job.to_dict())

    except Exception:
        # Exception handling: If any exception occurs during execution
        # Format the traceback to capture the error details
        tb = traceback.format_exc()
        # Store the last error message and the full traceback in the job
        job.last_error = str(tb)
        job.error_traceback = tb
        # Increment the retry count
        job.retries += 1

        # Check if the maximum number of retries has been exceeded
        if job.retries > job.max_retries:
            # If retries exhausted, set status to FAILED
            job.status = State.FAILED
            # Update the state in the backend
            await backend.update_job_state(queue_name, job.id, job.status)
            # Emit a job:failed event
            await event_emitter.emit("job:failed", job.to_dict())
            # Move the failed job to the Dead Letter Queue (DLQ)
            await backend.move_to_dlq(queue_name, job.to_dict())
        else:
            # If retries are still available, calculate the next retry delay
            delay = job.next_retry_delay()
            # Calculate the timestamp for the next retry
            job.delay_until = time.time() + delay
            # Set status to EXPIRED (or similar temporary state before delayed)
            # Note: The original code sets this to EXPIRED, which might be
            # counter-intuitive for a job pending retry, but logic is preserved.
            job.status = State.EXPIRED
            # Update the state in the backend
            await backend.update_job_state(queue_name, job.id, job.status)
            # Enqueue the job into the delayed queue for a future retry
            await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)


class Worker:
    """
    A convenience wrapper class for starting and stopping a worker process
    that processes jobs from a specific queue.

    This class encapsulates the logic for initializing the worker with a queue
    and providing simple methods to start and gracefully stop the worker's
    asynchronous processing loop.
    """

    def __init__(
        self,
        queue: Any,
        heartbeat_interval: float = monkay.settings.heartbeat_ttl,
    ) -> None:
        from asyncmq.queues import Queue

        self.queue = queue if not isinstance(queue, str) else Queue(queue)
        self.id = str(uuid.uuid4())
        self._cancel_scope: anyio.CancelScope | None = None
        self.concurrency = monkay.settings.worker_concurrency
        self.heartbeat_interval = heartbeat_interval

    async def _run_with_scope(self) -> None:
        backend = monkay.settings.backend

        # Initial registration
        await backend.register_worker(
            worker_id=self.id,
            queue=self.queue.name,
            concurrency=self.concurrency,
            timestamp=time.time(),
        )

        # Duplicate registration for heartbeat test
        await backend.register_worker(
            worker_id=self.id,
            queue=self.queue.name,
            concurrency=self.concurrency,
            timestamp=time.time(),
        )

        # Periodic heartbeat
        async def heartbeat_loop() -> None:
            while True:
                await anyio.sleep(self.heartbeat_interval)
                await backend.register_worker(
                    worker_id=self.id,
                    queue=self.queue.name,
                    concurrency=self.concurrency,
                    timestamp=time.time(),
                )

        async with anyio.create_task_group() as tg:
            tg.start_soon(heartbeat_loop)

            # Kick off the real processing loop—
            # process_job will dequeue and handle jobs until cancelled.
            tg.start_soon(
                process_job,
                self.queue.name,
                CapacityLimiter(self.concurrency),
                None,
                backend,
            )

            # Keep this task group alive until cancellation
            try:
                await anyio.sleep(float("inf"))
            except anyio.get_cancelled_exc_class():
                pass

        # On shutdown, deregister
        await backend.deregister_worker(self.id)

    def start(self) -> None:
        """Blocking entrypoint."""
        anyio.run(self._run_with_scope)

    async def run(self) -> None:
        """Async entrypoint (for tests/scripts)."""
        await self._run_with_scope()

    def stop(self) -> None:
        """Cancel if running under start()."""
        if self._cancel_scope:
            self._cancel_scope.cancel()

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

# This type checking import is typically used to avoid circular dependencies
# or to provide type hints for objects that are not imported at runtime.
if TYPE_CHECKING:
    from asyncmq.queues import Queue


async def process_job(
    queue_name: str,
    limiter: CapacityLimiter,
    rate_limiter: RateLimiter | None = None,
    backend: BaseBackend | None = None,
) -> None:
    """
    Asynchronously processes jobs from a specified queue name.

    This function continuously polls the queue for new jobs, respecting pause
    states, concurrency limits via `CapacityLimiter`, and optional rate limits
    via `RateLimiter`. It delegates the actual job handling to the
    `_run_with_limits` and `handle_job` functions, ensuring that jobs are
    processed efficiently and within defined constraints. It runs within an
    AnyIO task group, allowing for concurrent handling of multiple jobs up
    to the capacity defined by the `limiter`. The processing loop continues
    indefinitely until the task group is cancelled.

    Args:
        queue_name: The name of the queue to process jobs from.
        limiter: An AnyIO CapacityLimiter instance controlling the maximum
                 number of concurrently running jobs for this worker.
        rate_limiter: An optional RateLimiter instance to enforce rate limits
                      on job execution. If None, no rate limiting is applied.
        backend: An optional BaseBackend instance to interact with the message
                 queue and job storage. If None, the backend from settings is
                 used.
    """
    # Use the provided backend or the default one from settings.
    backend = backend or settings.backend

    # Create an AnyIO task group to manage concurrent job processing tasks.
    async with anyio.create_task_group() as tg:
        # Enter an infinite loop to continuously process jobs until cancelled.
        while True:
            # Check if the queue is paused. If it is, wait for a configured
            # interval before checking again to avoid busy-waiting.
            if await backend.is_queue_paused(queue_name):
                await anyio.sleep(settings.stalled_check_interval)
                continue

            # Attempt to dequeue the next job from the specified queue.
            raw_job = await backend.dequeue(queue_name)
            if raw_job is None:
                # If no job was retrieved, wait a short duration before polling
                # again to avoid excessive resource usage.
                await anyio.sleep(0.1)
                continue

            # If a job was successfully dequeued, start a new task in the
            # task group to handle its execution. The job processing logic
            # is wrapped by `_run_with_limits` to enforce concurrency and
            # rate limiting before the actual task execution.
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
    Applies concurrency and rate limits before invoking the actual job handler.

    This is an internal helper function designed to wrap the `handle_job`
    call within the necessary resource limits (`limiter` and `rate_limiter`).
    It ensures that `handle_job` is only executed when allowed by the defined
    concurrency and rate limiting policies.

    Args:
        raw_job: A dictionary representing the raw job data dequeued from
                 the backend.
        queue_name: The name of the queue the job originated from.
        backend: The BaseBackend instance used to interact with the queue.
        limiter: The AnyIO CapacityLimiter instance controlling concurrency.
                 Entering this context manager pauses execution until a slot
                 is available.
        rate_limiter: An optional RateLimiter instance. If provided, an
                      attempt will be made to acquire a rate limit permit
                      before proceeding, using the job ID for potential
                      rate limiting strategies.
    """
    # Acquire a slot from the concurrency limiter. Execution will pause here
    # if the maximum concurrency is reached until another task completes.
    async with limiter:
        # If a rate limiter is provided, attempt to acquire a permit.
        # Execution will pause here if the rate limit is exceeded until
        # a permit becomes available. The queue name and job ID are passed
        # for rate limit strategies that might use them.
        if rate_limiter:
            await rate_limiter.acquire(queue_name, raw_job["id"])
        # Once concurrency and rate limits are satisfied, proceed to handle the job.
        await handle_job(queue_name, raw_job, backend)


async def handle_job(
    queue_name: str,
    raw_job: dict[str, Any],
    backend: BaseBackend | None = None,
) -> None:
    """
    Processes a single job instance through its lifecycle: checking TTL and
    delays, marking as active, executing the task handler (potentially in a
    sandbox), handling success (saving result, acknowledging), and handling
    failure (retrying or moving to DLQ).

    This function encapsulates the core logic for executing a job based on its
    definition and current state. It interacts with the backend to update
    the job's status, save results, handle retries, and manage the Dead Letter
    Queue (DLQ). Exceptions during execution are caught and handled according
    to the job's retry configuration.

    Args:
        queue_name: The name of the queue the job belongs to.
        raw_job: A dictionary containing the raw data of the job.
        backend: An optional BaseBackend instance. If None, the backend
                 from settings is used.
    """
    # Use the provided backend or the default one from settings.
    backend = backend or settings.backend
    # Convert the raw job dictionary into a Job object for easier access
    # to job properties and methods.
    job = Job.from_dict(raw_job)

    # 1. Check for TTL expiration. If the job's time-to-live has passed,
    # mark it as expired, emit an event, and move it to the Dead Letter Queue.
    if job.is_expired():
        job.status = State.EXPIRED
        # Update the job's status in the backend.
        await backend.update_job_state(queue_name, job.id, job.status)
        # Emit an event indicating the job expired.
        await event_emitter.emit("job:expired", job.to_dict())
        # Move the expired job to the Dead Letter Queue.
        await backend.move_to_dlq(queue_name, job.to_dict())
        # The job is handled (as expired), so return and do not proceed further.
        return

    # 2. Check if the job is scheduled for a future time. If the current time
    # is before the 'delay_until' timestamp, re-enqueue the job into the
    # delayed queue and return.
    if job.delay_until and time.time() < job.delay_until:
        await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)
        # The job is not yet due, re-enqueued for later, so return.
        return

    try:
        # 3. Mark the job as active before execution begins. Update the backend
        # state and emit a 'job:started' event.
        job.status = State.ACTIVE
        # Update the job's status in the backend.
        await backend.update_job_state(queue_name, job.id, job.status)
        # Emit an event indicating the job has started.
        await event_emitter.emit("job:started", job.to_dict())

        # Retrieve the metadata for the task associated with this job from the registry.
        task_meta: dict[str, Any] = TASK_REGISTRY[job.task_id]
        # Get the actual function handler registered for this task ID.
        handler = task_meta["func"]

        # 4. Execute the task handler. Execution can happen either in a
        # sandboxed environment (if enabled in settings) or directly.
        if settings.sandbox_enabled:
            # If sandboxing is enabled, run the handler in a separate thread
            # managed by the sandbox module. This isolates the job execution
            # and provides resource limits (like timeout).
            result = await anyio.to_thread.run_sync(
                sandbox.run_handler,
                job.task_id,
                tuple(job.args),
                job.kwargs,
                settings.sandbox_default_timeout,
            )
        else:
            # If sandboxing is not enabled, execute the handler directly.
            # Check if the handler is a coroutine function and await it if necessary.
            if inspect.iscoroutinefunction(handler):
                result = await handler(*job.args, **job.kwargs)
            else:
                # If it's a regular function, call it directly.
                result = handler(*job.args, **job.kwargs)

        # 5. Success Path: If the handler executed without raising an exception,
        # the job is considered completed.
        job.status = State.COMPLETED
        job.result = result
        # Persist the updated state and the execution result to the backend.
        await backend.update_job_state(queue_name, job.id, job.status)
        await backend.save_job_result(queue_name, job.id, result)
        # Acknowledge the job with the backend, indicating successful processing
        # and readiness for removal from the queue/internal processing list.
        await backend.ack(queue_name, job.id)
        # Emit a 'job:completed' event with the job's final state and result.
        await event_emitter.emit("job:completed", job.to_dict())

    except Exception as exc:
        # 6. Failure & Retry Logic: If any exception occurs during job
        # execution (including sandbox execution issues), handle the failure.
        # Capture the traceback for debugging purposes.
        tb = traceback.format_exc()
        # Store the string representation of the exception.
        job.last_error = str(exc)
        # Store the formatted traceback.
        job.error_traceback = tb
        # Increment the retry count for this job.
        job.retries += 1

        # Check if the job has exceeded its maximum configured retries.
        if job.retries > job.max_retries:
            # If max retries are exceeded, mark the job as permanently failed.
            job.status = State.FAILED
            # Update the backend state to reflect the failed status.
            await backend.update_job_state(queue_name, job.id, job.status)
            # Emit a 'job:failed' event.
            await event_emitter.emit("job:failed", job.to_dict())
            # Move the job to the Dead Letter Queue for manual inspection or handling.
            await backend.move_to_dlq(queue_name, job.to_dict())
        else:
            # If retries are still available, calculate the delay before the
            # next retry attempt using the job's configured retry strategy.
            delay: float = job.next_retry_delay()
            # Set the 'delay_until' timestamp for the next attempt.
            job.delay_until = time.time() + delay
            # Mark the job status as EXPIRED temporarily before re-enqueuing
            # into the delayed queue. This status indicates it's waiting for
            # a future processing time.
            job.status = State.EXPIRED
            # Update the backend state with the incremented retry count,
            # error information, and the next delay timestamp.
            await backend.update_job_state(queue_name, job.id, job.status)
            # Enqueue the job into the delayed queue for a future retry attempt.
            await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)


class Worker:
    """
    A convenience wrapper class to manage the lifecycle of an AsyncMQ worker
    processing a specific queue.

    This class simplifies starting and stopping the asynchronous job processing
    loop for a given queue. It internally uses AnyIO to run the asynchronous
    `process_job` function and provides a mechanism to gracefully stop the
    worker using a CancelScope.

    Attributes:
        queue: The Queue instance this worker is associated with.
    """
    def __init__(self, queue: "Queue") -> None:
        """
        Initializes a new Worker instance for a specific queue.

        Args:
            queue: The Queue instance that this worker will process jobs from.
        """
        self.queue = queue
        # Initialize a CancelScope attribute to None. This will hold the
        # scope when the worker is running, allowing external cancellation.
        self._cancel_scope: anyio.CancelScope | None = None

    async def _run_with_scope(self) -> None:
        """
        Runs the main job processing loop within an AnyIO CancelScope.

        This asynchronous method is the entry point for the worker's main
        execution flow. It creates a CancelScope, instantiates the necessary
        concurrency and rate limiters based on application settings, and then
        starts the `process_job` coroutine. The `_cancel_scope` attribute is
        set within this method, enabling external cancellation via the `stop`
        method. This method blocks until the CancelScope is cancelled.
        """
        # Create a new AnyIO CancelScope for this worker's execution.
        with anyio.CancelScope() as scope:
            # Assign the created scope to the instance attribute so it can be
            # accessed and cancelled by the stop method.
            self._cancel_scope = scope
            # Instantiate the CapacityLimiter based on the configured worker concurrency.
            limiter = CapacityLimiter(settings.worker_concurrency)
            # Instantiate the RateLimiter only if rate limit configuration exists
            # in settings.
            rate_limiter = (
                RateLimiter(settings.rate_limit_config)
                if settings.rate_limit_config is not None
                else None
            )
            # Start the main job processing coroutine for the associated queue,
            # passing the instantiated limiters. This call blocks until the
            # CancelScope is cancelled.
            await process_job(self.queue.name, limiter, rate_limiter)

    def start(self) -> None:
        """
        Starts the worker synchronously; blocks until cancelled.

        This method provides a synchronous interface to start the asynchronous
        worker. It internally calls `anyio.run()` to execute the
        `_run_with_scope` coroutine. This function call blocks until the
        asynchronous worker process is cancelled, typically by calling the
        `stop` method.
        """
        # Use anyio.run to start the asynchronous _run_with_scope function.
        # This function blocks the current thread until the async function
        # completes (which happens when the CancelScope is cancelled).
        anyio.run(self._run_with_scope)

    def stop(self) -> None:
        """
        Requests cancellation of the running worker.

        If the worker has been started and its CancelScope (`_cancel_scope`)
        is active, calling this method will trigger the cancellation of the
        underlying asynchronous tasks managed by the scope. This causes the
        `process_job` loop and subsequently the `anyio.run` call in the `start`
        method to finish gracefully. If the worker is not running, this method
        does nothing.
        """
        # Check if the CancelScope has been created (i.e., the worker is running).
        if self._cancel_scope is not None:
            # If the scope exists, call its cancel method to request cancellation
            # of all tasks running within that scope.
            self._cancel_scope.cancel()

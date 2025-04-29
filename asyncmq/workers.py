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
    # This import is only for type checking purposes to avoid circular imports
    from asyncmq.queues import Queue


async def process_job(
    queue_name: str,
    limiter: anyio.CapacityLimiter,
    backend: BaseBackend | None = None,
    rate_limiter: Any | None = None,
) -> None:
    """
    Asynchronously pulls jobs from a specified queue, manages concurrency and
    rate limits, and delegates job processing to `handle_job` tasks.

    This function runs in a loop, continuously attempting to dequeue jobs.
    It respects queue pausing, concurrency limits imposed by the `limiter`,
    and rate limits (if a `rate_limiter` is provided). Each dequeued job
    is passed to `handle_job` which is run concurrently within a task group.

    Args:
        queue_name: The name of the queue to process jobs from.
        limiter: An `anyio.CapacityLimiter` instance to control the maximum
                 number of concurrent jobs being processed from this queue.
        backend: The backend instance to use for queue operations. If None,
                 `settings.backend` is used.
        rate_limiter: An optional rate limiter instance (e.g., from `limits`)
                      to control the rate at which jobs are processed.
    """
    # Use the provided backend or the default one from settings.
    backend = backend or settings.backend

    # Create a task group to manage concurrent job handling tasks.
    async with anyio.create_task_group() as tg:
        # Start an infinite loop to continuously process jobs.
        while True:
            # Check if the queue is paused via the backend.
            if await backend.is_queue_paused(queue_name):
                # If paused, sleep briefly before checking again.
                await anyio.sleep(0.5)
                continue

            # Acquire a slot from the concurrency limiter. This pauses if the
            # maximum number of concurrent jobs is reached.
            async with limiter:
                # Attempt to dequeue a job from the waiting queue.
                raw_job: dict[str, Any] | None = await backend.dequeue(queue_name)
                # If no job was dequeued.
                if not raw_job:
                    # Sleep briefly to avoid busy-waiting.
                    await anyio.sleep(0.1)
                    continue

                # If a job was dequeued, start a new task in the task group
                # to handle its processing.
                tg.start_soon(handle_job, queue_name, raw_job, backend, rate_limiter)

                # Register dependencies if the dequeued job has them.
                # This seems slightly misplaced here as dependencies are typically
                # added when a job is enqueued, but preserving original logic.
                if raw_job.get("depends_on"):
                    await backend.add_dependencies(queue_name, raw_job)


async def handle_job(
    queue_name: str,
    raw_job: dict[str, Any],
    backend: BaseBackend | None = None,
    rate_limiter: Any | None = None,
) -> None:
    """
    Asynchronously processes a single job instance.

    This function manages the full lifecycle of a job: checking for expiration,
    handling delayed jobs, applying rate limits, executing the task function,
    handling success (acknowledging, saving result, resolving dependencies),
    and handling failure (retries, moving to DLQ).

    Args:
        queue_name: The name of the queue the job belongs to.
        raw_job: The raw job data as a dictionary, typically retrieved from
                 the backend.
        backend: The backend instance to use for job operations. If None,
                 `settings.backend` is used.
        rate_limiter: An optional rate limiter instance to apply before
                      executing the task function.
    """
    # Use the provided backend or the default one from settings.
    backend = backend or settings.backend
    # Convert the raw job dictionary into a Job object for easier access to methods.
    job: Job = Job.from_dict(raw_job)

    # Record initial heartbeat (placeholder, actual implementation might be elsewhere)

    # 1. TTL check: Check if the job has expired based on its TTL.
    if job.is_expired():
        job.status = State.EXPIRED
        # Emit an event indicating the job has expired.
        await event_emitter.emit("job:expired", job.to_dict())
        # Move the expired job to the Dead Letter Queue (DLQ).
        await backend.move_to_dlq(queue_name, job.to_dict())
        return # Stop processing this job

    # 2. Delayed job handling: If the job is delayed and not yet due.
    if job.delay_until and time.time() < job.delay_until:
        # Re-enqueue the job as delayed with its original delay_until timestamp.
        await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)
        return # Stop processing this job

    try:
        # 3. Rate limiting per job: Acquire a token from the rate limiter if provided.
        if rate_limiter:
            await rate_limiter.acquire()

        # 4. Execute the task: Update job status and find the task function.
        job.status = State.ACTIVE
        # Update the job's state in the backend.
        await backend.update_job_state(queue_name, job.id, job.status)
        # Emit an event indicating the job has started.
        await event_emitter.emit("job:started", job.to_dict())

        # Retrieve the task metadata (including the wrapped function) from the registry.
        task_meta: dict[str, Any] = TASK_REGISTRY[job.task_id]
        # Execute the wrapped task function with job arguments and keyword arguments.
        # The wrapper handles awaiting sync/async functions and progress reporting.
        result: Any = await task_meta["func"](*job.args, **job.kwargs)

        # Success path: If the task execution completes without exception.
        job.status = State.COMPLETED
        job.result = result
        # Acknowledge the job with the backend (removes from active state).
        await backend.ack(queue_name, job.id)
        # Save the result of the task execution.
        await backend.save_job_result(queue_name, job.id, result)
        # Update the final job state in the backend.
        await backend.update_job_state(queue_name, job.id, job.status)
        # Emit an event indicating the job has completed successfully.
        await event_emitter.emit("job:completed", job.to_dict())

        # Resolve dependencies: Notify the backend that this job has completed,
        # potentially unblocking dependent jobs.
        await backend.resolve_dependency(queue_name, job.id)

    except Exception:
        # Exception handling path: If the task execution raises an exception.
        # Print the traceback for debugging.
        traceback.print_exc()
        # Increment the retry count.
        job.retries += 1
        # Record the time of this attempt.
        job.last_attempt = time.time()

        # Check if the maximum number of retries has been exceeded.
        if job.retries > job.max_retries:
            # If retries exhausted, mark the job as FAILED.
            job.status = State.FAILED
            # Emit an event indicating the job has failed.
            await event_emitter.emit("job:failed", job.to_dict())
            # Move the failed job to the Dead Letter Queue (DLQ).
            await backend.move_to_dlq(queue_name, job.to_dict())
        else:
            # If retries remain, calculate the delay until the next retry.
            delay: float = job.next_retry_delay()
            # Set the delay_until timestamp for the next attempt.
            job.delay_until = time.time() + delay
            # Mark the job status as EXPIRED (or DELAYED) to indicate it's
            # temporarily out of the waiting queue. Preserving original logic.
            job.status = State.EXPIRED
            # Update the job state in the backend.
            await backend.update_job_state(queue_name, job.id, job.status)
            # Enqueue the job as delayed for the next retry attempt.
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
        self.queue: "Queue" = queue
        # Internal variable to store the AnyIO CancelScope for the worker task.
        # Initialized to None, set when the worker starts.
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
            # Run the queue's main processing loop. This call blocks until
            # the queue's run method completes or is cancelled.
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
        # This call is blocking.
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

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

    This function runs indefinitely within its own AnyIO TaskGroup, acting as
    the main loop for a queue worker. It orchestrates fetching and initiating
    the handling of jobs from a single queue.

    The loop performs the following steps:
    1.  Checks if the target queue (`queue_name`) is currently paused using
        the provided `backend`. If paused, it waits briefly (0.5 seconds) and
        checks again to avoid tight looping.
    2.  If the queue is not paused, it attempts to acquire a slot from the
        `limiter`. This `anyio.CapacityLimiter` controls the maximum number of
        concurrent `handle_job` tasks allowed to run at any given time,
        preventing system overload.
    3.  Once a slot is acquired (the `async with limiter:` context is entered),
        it attempts to dequeue a job from the `queue_name` using the
        `backend.dequeue()` method. This method is expected to return a raw
        dictionary representing the job or `None` if the queue is empty.
    4.  If `backend.dequeue()` returns a job dictionary (`raw_job` is not None),
        it immediately spawns a new asynchronous task using `tg.start_soon()`
        to execute the `handle_job` function. This task is responsible for the
        actual execution of the job's task function and managing its lifecycle.
        The `handle_job` task is passed the queue name, the raw job data,
        the backend instance, and the optional rate limiter.
    5.  After spawning the `handle_job` task, it checks if the dequeued job
        dictionary contains a 'depends_on' key. This indicates that other jobs
        might be waiting on this job's completion. If dependencies are present,
        it calls `backend.add_dependencies()` to register these dependencies
        within the backend system, allowing dependent jobs to be processed later.
    6.  If `backend.dequeue()` returns `None` (meaning no jobs were available
        in the queue at that moment), the function waits briefly (0.1 seconds)
        before the next dequeue attempt. This pause prevents the loop from
        consuming excessive CPU resources when the queue is idle.

    Args:
        queue_name: The name of the queue from which jobs are to be processed.
        limiter: An `anyio.CapacityLimiter` instance used to restrict the
                 number of simultaneously running `handle_job` tasks,
                 controlling concurrency.
        backend: An object implementing the `BaseBackend` interface, used for
                 interactions with the queue system (e.g., checking pause
                 status, dequeuing jobs, adding dependencies). If `None`,
                 the default backend from `settings` is used.
        rate_limiter: An optional object (expected to have an `acquire()`
                      method) used to control the rate at which jobs are
                      processed *after* being dequeued but *before* their
                      task function is executed. If `None`, no rate limiting
                      is applied at this stage within `handle_job`.
    """
    # Use the provided backend or fall back to the default configured backend.
    backend = backend or settings.backend

    # Create a task group to manage concurrently running handle_job tasks.
    async with anyio.create_task_group() as tg:
        # Infinite loop to continuously process jobs.
        while True:
            # Check if the queue is paused; if so, wait and continue the loop.
            if await backend.is_queue_paused(queue_name):
                await anyio.sleep(0.5)
                continue

            # Acquire a slot from the limiter before dequeuing a job.
            # This limits the number of concurrent handle_job tasks.
            async with limiter:
                # Attempt to dequeue a job from the backend.
                raw_job = await backend.dequeue(queue_name)
                # If no job is returned, wait briefly and continue the loop.
                if not raw_job:
                    await anyio.sleep(0.1)
                    continue

                # If a job is dequeued, start a new task to handle it.
                tg.start_soon(handle_job, queue_name, raw_job, backend, rate_limiter)

                # Check if the dequeued job has dependencies.
                if raw_job.get("depends_on"):
                    # Add job dependencies to the backend.
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

    This function is typically spawned as a separate task for each dequeued job
    by `process_job`. It encapsulates the core logic for executing a job's
    associated task function and handling the outcomes, including expiry,
    delaying, rate limiting, execution, completion, failure, retries, and
    DLQ placement.

    The processing flow includes:
    1.  Converting the raw job dictionary (`raw_job`) into a `Job` object for
        easier attribute access and manipulation.
    2.  Performing a Time-To-Live (TTL) check using `job.is_expired()`. If the
        job's creation time plus its TTL has passed relative to the current time,
        the job is considered expired.
        a.  If expired, the job's status is set to `State.EXPIRED`.
        b.  A `job:expired` event is emitted with the job's data.
        c.  The job is moved to the Dead Letter Queue (DLQ) via
            `backend.move_to_dlq()` to remove it from regular processing.
        d.  The function returns, ending processing for this job.
    3.  Checking if the job has a `delay_until` timestamp set in the future.
        This indicates a job that was previously processed but marked for future
        re-processing (e.g., a failed job scheduled for retry with a delay).
        a.  If `delay_until` is set and is in the future (checked against
            `time.time()`), the job is re-enqueued back into the delayed queue
            using `backend.enqueue_delayed()` with the existing `delay_until`
            timestamp. This ensures it's picked up again only after the delay
            expires.
        b.  The function returns, ending processing for this job in the current
            pass.
    4.  If the job is not expired and not delayed for the future, the process
        proceeds to execution:
        a.  Optionally applies a rate limit by calling `rate_limiter.acquire()`
            if a `rate_limiter` object was provided. This call might block
            until a rate limit slot is available.
        b.  Updates the job's status to `State.ACTIVE` locally and in the backend
            using `backend.update_job_state()`.
        c.  Emits a `job:started` event with the job's data.
        d.  Looks up the task function metadata in the `TASK_REGISTRY` using the
            job's `task_id`. The `TASK_REGISTRY` is assumed to be a dictionary
            mapping task IDs to metadata, including the actual callable function
            (`task_meta["func"]`).
        e.  Executes the task function (`task_meta["func"]`) using the job's
            arguments (`job.args`) and keyword arguments (`job.kwargs`).
            This is the core work of the job.
    5.  **Success Path:** If the task execution completes without raising an
        exception:
        a.  The job status is set to `State.COMPLETED` locally.
        b.  The result returned by the task function is stored in `job.result`.
        c.  The job is acknowledged with the backend using `backend.ack()`.
            This typically removes the job from the queue's active set.
        d.  The result of the job's execution is saved using
            `backend.save_job_result()`.
        e.  The job state is updated to `State.COMPLETED` in the backend
            using `backend.update_job_state()`.
        f.  A `job:completed` event is emitted with the job's data.
        g.  Any dependencies waiting on this job's completion are resolved via
            `backend.resolve_dependency()`. This might trigger dependent jobs.
    6.  **Failure Path:** If any exception occurs during task execution or
        any other part of the `try` block:
        a.  The exception traceback is printed to the console using
            `traceback.print_exc()` for debugging purposes.
        b.  The job's retry count (`job.retries`) is incremented.
        c.  The time of the failed attempt is recorded using `anyio.current_time()`
            and stored in `job.last_attempt`.
        d.  If the updated retry count (`job.retries`) exceeds the maximum
            allowed retries (`job.max_retries`) defined for the job:
            i.  The job status is set to `State.FAILED` locally.
            ii. A `job:failed` event is emitted with the job's data.
            iii. The job is moved to the Dead Letter Queue (DLQ) using
                `backend.move_to_dlq()`. Processing for this job ends.
        e.  If retries are remaining (`job.retries <= job.max_retries`):
            i.  The delay before the next retry is calculated using
                `job.next_retry_delay()`. This method is expected to implement
                retry delay logic (e.g., exponential backoff).
            ii. The absolute time for the next attempt is calculated by adding
                the calculated `delay` to the current time (`time.time()`)
                and stored in `job.delay_until`.
            iii. The job status is temporarily set to `State.EXPIRED` locally
                 (as per original code logic, possibly to mark it as temporarily
                 out of the active set while awaiting re-enqueue/delay).
            iv. The job state is updated in the backend using
                `backend.update_job_state()` with the temporary `State.EXPIRED`
                status.
            v.  The job is re-enqueued with the calculated delay using
                `backend.enqueue_delayed()`, placing it back into the processing
                cycle to be picked up after the `delay_until` timestamp.

    Args:
        queue_name: The name of the queue the job was dequeued from.
        raw_job: A dictionary containing the raw data representation of the job
                 as received from the backend.
        backend: An object implementing the `BaseBackend` interface, used for
                 interacting with the queue system (e.g., moving to DLQ,
                 enqueueing delayed jobs, updating state, acknowledging,
                 saving results, resolving dependencies). If `None`, the
                 default backend from `settings` is used.
        rate_limiter: An optional object (expected to have an `acquire()`
                      method) used to apply a rate limit just before the job's
                      task function is executed. If `None`, no rate limit is
                      applied at this point.
    """
    # Use the provided backend or fall back to the default configured backend.
    backend = backend or settings.backend
    # Convert the raw dictionary job data into a Job object.
    job = Job.from_dict(raw_job)

    # Check if the job has expired based on its TTL.
    if job.is_expired():
        job.status = State.EXPIRED
        await event_emitter.emit("job:expired", job.to_dict())
        await backend.move_to_dlq(queue_name, job.to_dict())
        return  # Stop processing if expired.

    # Check if the job is scheduled for a future time (delayed job).
    if job.delay_until and time.time() < job.delay_until:
        # If delayed for the future, re-enqueue it with the delay.
        await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)
        return  # Stop processing now; it will be picked up later.

    try:
        # Apply rate limiting if a rate limiter is provided.
        if rate_limiter:
            await rate_limiter.acquire()

        # Update job status to ACTIVE before execution.
        job.status = State.ACTIVE
        await backend.update_job_state(queue_name, job.id, State.ACTIVE)
        # Emit event indicating job started.
        await event_emitter.emit("job:started", job.to_dict())

        # Look up the task function in the registry.
        task_meta = TASK_REGISTRY[job.task_id]
        # Execute the task function with job arguments.
        result = await task_meta["func"](*job.args, **job.kwargs)

        # --- Success Path ---
        # Update job status to COMPLETED.
        job.status = State.COMPLETED
        job.result = result
        # Acknowledge the job with the backend (remove from queue).
        await backend.ack(queue_name, job.id)
        # Save the job result.
        await backend.save_job_result(queue_name, job.id, result)
        # Update job state in the backend.
        await backend.update_job_state(queue_name, job.id, State.COMPLETED)
        # Emit event indicating job completed.
        await event_emitter.emit("job:completed", job.to_dict())

        # Resolve dependencies waiting on this job.
        await backend.resolve_dependency(queue_name, job.id)

    except Exception:
        # --- Failure Path ---
        # Print traceback for debugging.
        traceback.print_exc()
        # Increment retry count.
        job.retries += 1
        # Record the time of this failed attempt.
        job.last_attempt = anyio.current_time()

        # Check if max retries exceeded.
        if job.retries > job.max_retries:
            job.status = State.FAILED
            # Emit event indicating job failed.
            await event_emitter.emit("job:failed", job.to_dict())
            # Move job to Dead Letter Queue (DLQ).
            await backend.move_to_dlq(queue_name, job.to_dict())
        else:
            # Calculate delay for the next retry.
            delay = job.next_retry_delay()
            # Calculate the absolute time for the next attempt.
            job.delay_until = time.time() + delay
            # Update status before re-enqueue for delay.
            job.status = State.EXPIRED
            # Update state in the backend.
            await backend.update_job_state(queue_name, job.id, State.EXPIRED)
            # Re-enqueue the job with the calculated delay.
            await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)


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

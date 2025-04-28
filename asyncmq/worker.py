import time
import traceback
from typing import TYPE_CHECKING, Any, Dict, Optional

import anyio

from asyncmq.backends.base import BaseBackend
from asyncmq.conf import settings
from asyncmq.enums import State
from asyncmq.event import event_emitter
from asyncmq.job import Job
from asyncmq.tasks import TASK_REGISTRY

if TYPE_CHECKING:
    from asyncmq.queue import Queue


async def process_job(
    queue_name: str,
    limiter: anyio.CapacityLimiter,
    backend: BaseBackend | None = None,
    rate_limiter: Optional[Any] = None,
) -> None:
    """
    Continuously pulls jobs from a specified queue, respecting concurrency
    and rate limits, and delegates their processing to `handle_job` tasks.

    This function runs indefinitely within its own AnyIO TaskGroup. It first
    checks if the queue associated with `queue_name` is paused using the
    `backend` object. If paused, it sleeps and retries. If not paused, it
    attempts to acquire a slot from the provided `limiter` (concurrency
    control).

    Once a slot is acquired from the limiter, it attempts to dequeue a job
    from the `queue_name` using the `backend`. If a job (represented as a
    raw dictionary) is successfully dequeued, a `handle_job` task is spawned
    within the TaskGroup to process it concurrently. If the dequeued job has
    dependencies specified, those dependencies are registered with the
    `backend`, and `handle_job` is spawned again with the same raw job data.

    If no job is available after dequeueing, it sleeps briefly before the next
    attempt to avoid busy-waiting.

    Args:
        queue_name: The name of the queue from which to process jobs.
        backend: An object providing the necessary interface for interacting
                 with the queue, including methods like `is_queue_paused`,
                 `dequeue`, and `add_dependencies`. Its specific type is not
                 defined in the original code, hence typed as `Any`.
        limiter: An `anyio.CapacityLimiter` instance used to limit the number
                 of concurrently running `handle_job` tasks.
        rate_limiter: An optional object expected to have an `acquire()`
                      method, used to control the rate at which jobs are
                      processed after dequeueing but before execution. Its
                      specific type is not defined, hence typed as `Optional[Any]`.
                      Defaults to None, meaning no rate limiting is applied at this level.
    """
    backend = backend or settings.backend

    async with anyio.create_task_group() as tg:
        while True:
            # Check if the queue is currently paused.
            if await backend.is_queue_paused(queue_name):
                await anyio.sleep(0.5)  # Sleep briefly if paused before checking again.
                continue

            # Acquire a slot in the concurrency limiter before attempting to dequeue.
            async with limiter:
                # Attempt to dequeue a job from the backend.
                raw_job = await backend.dequeue(queue_name)
                if not raw_job:
                    # If no job is available, sleep briefly before the next poll.
                    await anyio.sleep(0.1)
                    continue

                # Spawn a task to handle the main work of processing the job.
                tg.start_soon(handle_job, queue_name, raw_job, backend, rate_limiter)

                # Check for and register job dependencies if they exist.
                # The original code also spawns handle_job again here if dependencies exist,
                # which might lead to processing the same job twice.
                if raw_job.get("depends_on"):
                    await backend.add_dependencies(queue_name, raw_job)
                    # tg.start_soon(handle_job, queue_name, backend, raw_job, rate_limiter) # Re-spawning handle_job
                    ...


async def handle_job(
    queue_name: str,
    raw_job: Dict[str, Any],
    backend: BaseBackend | None = None,
    rate_limiter: Optional[Any] = None,  # Type is Optional[Any] as per original code structure.
) -> None:
    """
    Handles the processing of a single job dequeued from the queue.

    This function takes a raw job dictionary, converts it into a Job object,
    and manages its lifecycle including TTL checks, delayed execution, rate
    limiting (optional), execution of the associated task function, handling
    success (acknowledging, saving result, resolving dependencies), and
    managing retries or moving the job to the Dead Letter Queue (DLQ) in
    case of failure.

    Args:
        queue_name: The name of the queue the job originated from.
        backend: An object providing the necessary interface for interacting
                 with the queue, including methods like `move_to_dlq`,
                 `enqueue_delayed`, `update_job_state`, `ack`, `save_job_result`,
                 and `resolve_dependency`. Its specific type is not defined in
                 the original code, hence typed as `Any`.
        raw_job: A dictionary containing the raw job data as received from the
                 backend.
        rate_limiter: An optional object expected to have an `acquire()`
                      method, used to apply a rate limit before executing the
                      job's task. Its specific type is not defined, hence typed
                      as `Optional[Any]`. Defaults to None.
    """
    backend = backend or settings.backend
    # Convert the raw dictionary data into a Job object.
    job = Job.from_dict(raw_job)

    # Perform Time-To-Live (TTL) check.
    if job.is_expired():
        job.status = State.EXPIRED
        # Emit an event indicating the job has expired.
        await event_emitter.emit("job:expired", job.to_dict())
        # Move the expired job to the Dead Letter Queue (DLQ) using the backend.
        await backend.move_to_dlq(queue_name, job.to_dict())
        return  # Stop processing this job as it's expired.

    # Check if the job should be delayed until a future time (`delay_until`).
    if job.delay_until and time.time() < job.delay_until:
        # If delayed, re-enqueue it with the delay information using the backend.
        await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)
        return  # Stop processing this job for now, it will be picked up later.

    try:
        # Apply the optional rate limit if a rate_limiter is provided.
        if rate_limiter:
            # Acquire a permit from the rate limiter; this might block.
            await rate_limiter.acquire()

        # Mark the job as active in the backend before execution.
        job.status = State.ACTIVE
        await backend.update_job_state(queue_name, job.id, State.ACTIVE)
        # Emit an event indicating that the job has started processing.
        await event_emitter.emit("job:started", job.to_dict())

        # Retrieve the metadata for the task function based on job.task_id
        # and execute the task function with the job's arguments and keyword arguments.
        task_meta = TASK_REGISTRY[job.task_id]
        result = await task_meta["func"](*job.args, **job.kwargs)

        # --- Success Path ---
        # If the task execution completes without raising an exception:
        job.status = State.COMPLETED
        job.result = result  # Store the result of the task execution.
        # Acknowledge the job completion with the backend.
        await backend.ack(queue_name, job.id)
        # Save the result of the job execution using the backend.
        await backend.save_job_result(queue_name, job.id, result)
        # Update the job state to completed in the backend.
        await backend.update_job_state(queue_name, job.id, State.COMPLETED)
        # Emit an event indicating the job has completed successfully.
        await event_emitter.emit("job:completed", job.to_dict())

        # Resolve any dependencies that were waiting on this job's completion using the backend.
        await backend.resolve_dependency(queue_name, job.id)

    except Exception:
        # --- Failure Path ---
        # If any exception occurs during task execution or processing:
        # Print the traceback of the exception for debugging.
        traceback.print_exc()
        # Increment the retry count for the job.
        job.retries += 1
        # Record the time of this failed attempt using AnyIO's monotonic time.
        job.last_attempt = anyio.current_time()

        # Check if the maximum number of retries allowed for this job has been exceeded.
        if job.retries > job.max_retries:
            # If retries are exhausted, mark the job as failed.
            job.status = State.FAILED
            # Emit an event indicating the job has permanently failed.
            await event_emitter.emit("job:failed", job.to_dict())
            # Move the failed job to the Dead Letter Queue (DLQ) using the backend.
            await backend.move_to_dlq(queue_name, job.to_dict())
        else:
            # If retries are remaining, calculate the delay before the next retry attempt.
            delay = job.next_retry_delay()
            # Calculate the absolute time until the job should be retried.
            job.delay_until = time.time() + delay
            # Mark the job status as delayed for the next retry.
            job.status = State.EXPIRED
            # Update the job state in the backend to reflect it's delayed.
            await backend.update_job_state(queue_name, job.id, State.EXPIRED)
            # Re-enqueue the job with the calculated delay using the backend.
            await backend.enqueue_delayed(queue_name, job.to_dict(), job.delay_until)


class Worker:
    """
    A convenience wrapper class designed to start and stop an `asyncmq`
    queue worker programmatically under the control of AnyIO.

    This class simplifies the management of a queue's processing lifecycle.
    It holds a reference to an `asyncmq.queue.Queue` instance and provides
    methods to initiate its execution (`start`) and request its termination
    (`stop`). The worker runs the queue's internal processing loop within
    an AnyIO context.
    """
    def __init__(self, queue: "Queue") -> None:
        """
        Initializes the Worker instance.

        Args:
            queue: The `asyncmq.queue.Queue` instance that this worker will
                   be responsible for running.
        """
        self.queue = queue
        # _cancel_scope is used to store the AnyIO CancelScope that controls
        # the worker's main loop, allowing it to be cancelled externally.
        self._cancel_scope: anyio.CancelScope | None = None

    async def _run_with_scope(self) -> None:
        """
        Internal asynchronous method that executes the queue's main processing
        loop within an AnyIO CancelScope.

        This method is the entry point for the asynchronous worker logic. It
        creates a CancelScope, assigns it to `self._cancel_scope`, and then
        awaits the `self.queue.run()` method. The presence of the CancelScope
        enables the `stop()` method to interrupt the `queue.run()` execution
        and terminate the worker.
        """
        # Create a CancelScope that can be used to cancel the tasks started
        # within this scope. Store a reference for external cancellation.
        with anyio.CancelScope() as scope:
            self._cancel_scope = scope
            # Await the queue's run method, which contains the job processing loops.
            await self.queue.run()

    def start(self) -> None:
        """
        Starts the worker and blocks the current thread until the worker
        finishes execution or is stopped.

        This method uses `anyio.run()` to execute the asynchronous
        `_run_with_scope` method. `anyio.run()` is a blocking call that
        starts an AnyIO event loop and runs the provided asynchronous function
        until it completes. To avoid blocking the main application thread,
        this method should typically be called from a separate background thread.
        """
        # Use anyio.run to start the event loop and execute the async worker task.
        anyio.run(self._run_with_scope)

    def stop(self) -> None:
        """
        Requests the cancellation of the running worker.

        If the worker was started using the `start()` method and is currently
        running, this method accesses the stored `CancelScope` and calls its
        `cancel()` method. Requesting cancellation signals the asynchronous
        tasks within the worker's scope to shut down, which will eventually
        cause the `anyio.run()` call in the `start()` method to return.

        This method is safe to call from any thread.
        """
        # If the _cancel_scope has been initialized (meaning start was called), cancel it.
        if self._cancel_scope is not None:
            self._cancel_scope.cancel()

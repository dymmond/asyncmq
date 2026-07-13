import importlib
import pkgutil
import time
import traceback
import uuid
from typing import Any, cast

import anyio
from anyio import CapacityLimiter

import asyncmq
from asyncmq import monkay, sandbox
from asyncmq.backends.base import BaseBackend
from asyncmq.core.enums import State
from asyncmq.core.event import event_emitter
from asyncmq.core.lifecycle import run_hooks, run_hooks_safely
from asyncmq.core.stalled import stalled_recovery_scheduler
from asyncmq.core.tracing import (
    job_execution_span,
    mark_job_span_completed,
    mark_job_span_exception,
)
from asyncmq.exceptions import JobCancelled
from asyncmq.jobs import Job
from asyncmq.logging import logger
from asyncmq.rate_limiter import RateLimiter
from asyncmq.tasks import TASK_REGISTRY


def autodiscover_tasks() -> None:
    """
    Import every module in the configured task package so that
    @task(...) decorators run and populate TASK_REGISTRY.
    """
    tasks = monkay.settings.tasks  # e.g. ['myproject.tasks', 'myproject.something.tasks']

    for pkg_name in tasks:
        try:
            pkg = importlib.import_module(pkg_name)
        except ImportError as e:
            logger.warning(f"Could not import task package {pkg_name!r}: {e}")
            continue

        for _, module_name, _ in pkgutil.iter_modules(pkg.__path__):
            full_name = f"{pkg_name}.{module_name}"
            try:
                importlib.import_module(full_name)
            except Exception as e:
                logger.warning(f"autodiscover failed importing {full_name!r}: {e}")


async def _call_lifecycle_transition(backend: Any, name: str, *args: Any) -> bool:
    transition = getattr(backend, name, None)
    if callable(transition):
        await transition(*args)
        return True
    return False


async def _complete_active_job(backend: Any, queue_name: str, payload: dict[str, Any], result: Any) -> None:
    if await _call_lifecycle_transition(backend, "complete_active_job", queue_name, payload, result):
        return

    job_id = str(payload["id"])
    await backend.update_job_state(queue_name, job_id, State.COMPLETED)
    await backend.save_job_result(queue_name, job_id, result)
    await backend.ack(queue_name, job_id)


async def _retry_active_job(backend: Any, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
    if await _call_lifecycle_transition(backend, "retry_active_job", queue_name, payload, run_at):
        return

    job_id = str(payload["id"])
    await backend.update_job_state(queue_name, job_id, State.DELAYED)
    await backend.enqueue_delayed(queue_name, payload, run_at)
    await backend.ack(queue_name, job_id)


async def _defer_active_job(backend: Any, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
    if await _call_lifecycle_transition(backend, "defer_active_job", queue_name, payload, run_at):
        return

    await _retry_active_job(backend, queue_name, payload, run_at)


async def _fail_active_job(backend: Any, queue_name: str, payload: dict[str, Any]) -> None:
    if await _call_lifecycle_transition(backend, "fail_active_job", queue_name, payload):
        return

    job_id = str(payload["id"])
    await backend.update_job_state(queue_name, job_id, State.FAILED)
    await backend.ack(queue_name, job_id)
    await backend.move_to_dlq(queue_name, payload)


async def _expire_active_job(backend: Any, queue_name: str, payload: dict[str, Any]) -> None:
    if await _call_lifecycle_transition(backend, "expire_active_job", queue_name, payload):
        return

    job_id = str(payload["id"])
    await backend.update_job_state(queue_name, job_id, State.EXPIRED)
    await backend.ack(queue_name, job_id)
    await backend.move_to_dlq(queue_name, payload)


async def _cancel_active_job(backend: Any, queue_name: str, payload: dict[str, Any]) -> None:
    if await _call_lifecycle_transition(backend, "cancel_active_job", queue_name, payload):
        return

    await backend.ack(queue_name, str(payload["id"]))


def _heartbeat_renewal_interval(settings: Any) -> float:
    threshold = max(float(settings.stalled_threshold), 0.1)
    check_interval = max(float(settings.stalled_check_interval), 0.1)
    return max(0.1, min(check_interval, threshold / 3))


async def _save_job_heartbeat_safely(
    backend: BaseBackend,
    queue_name: str,
    job_id: str,
    timestamp: float,
    failure_count: int,
) -> bool:
    try:
        await backend.save_heartbeat(queue_name, job_id, timestamp)
    except Exception as exc:
        if failure_count == 1 or failure_count % 10 == 0:
            logger.warning(
                f"Failed to renew heartbeat for job {job_id!r} in queue {queue_name!r} "
                f"(failure #{failure_count}); continuing execution: {exc}",
                exc_info=True,
            )
        return False
    return True


async def process_job(
    queue_name: str,
    limiter: CapacityLimiter,
    rate_limiter: RateLimiter | None = None,
    backend: BaseBackend | None = None,
    *,
    drain_event: anyio.Event | None = None,
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
        drain_event: Optional event used for cooperative worker draining. Once
                     set, the loop stops claiming new jobs and returns after
                     already-started job tasks finish.
    """
    # Use the provided backend or the one from settings
    settings = asyncmq.monkay.settings
    backend = backend or settings.backend

    # Create a task group to manage concurrent job handling tasks
    async with anyio.create_task_group() as tg:
        while True:
            if drain_event is not None and drain_event.is_set():
                return

            # Pause support: Check if the queue is currently paused
            if await backend.is_queue_paused(queue_name):
                # If paused, wait for a bit before checking again
                await anyio.sleep(settings.stalled_check_interval)
                continue  # Skip to the next iteration

            borrower = object()
            acquired = False
            rate_acquired = False
            handed_off = False
            await limiter.acquire_on_behalf_of(borrower)
            acquired = True
            try:
                if drain_event is not None and drain_event.is_set():
                    return

                if rate_limiter:
                    await rate_limiter.acquire()
                    rate_acquired = True

                if drain_event is not None and drain_event.is_set():
                    return

                # Attempt to dequeue only after capacity and any rate-limit
                # token are available. This prevents workers from hiding more
                # jobs than they can execute.
                raw_job = await backend.dequeue(queue_name)
                if raw_job is None:
                    if rate_acquired and rate_limiter:
                        rate_limiter.refund_latest()
                        rate_acquired = False
                    # If no job is available, wait briefly to avoid busy-looping.
                    await anyio.sleep(0.1)
                    continue  # Skip to the next iteration

                # If a job is dequeued, start a new task in the task group to handle it.
                # The _run_with_limits function owns the capacity token after handoff.
                tg.start_soon(
                    _run_with_limits,
                    raw_job,
                    queue_name,
                    backend,
                    limiter,
                    borrower,
                )
                handed_off = True
            finally:
                if rate_acquired and not handed_off and rate_limiter:
                    rate_limiter.refund_latest()
                if acquired and not handed_off:
                    limiter.release_on_behalf_of(borrower)


async def _run_with_limits(
    raw_job: dict[str, Any],
    queue_name: str,
    backend: BaseBackend,
    limiter: CapacityLimiter,
    limiter_borrower: object,
) -> None:
    """
    Calls ``handle_job`` and releases the capacity token when it finishes.

    This function is intended to be run within a task group. Capacity is
    acquired before dequeue by ``process_job`` and released here after the job
    attempt finishes.

    Args:
        raw_job: The raw job data as a dictionary.
        queue_name: The name of the queue the job originated from.
        backend: The backend instance used for queue operations.
        limiter: The CapacityLimiter instance whose borrowed token must be released.
        limiter_borrower: The borrower object used to acquire the capacity token.
    """
    try:
        await handle_job(queue_name, raw_job, backend)
    finally:
        limiter.release_on_behalf_of(limiter_borrower)


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
    settings = asyncmq.monkay.settings
    backend = backend or settings.backend

    # Some backends (e.g. RabbitMQ) may wrap dequeued payloads as
    # {"job_id": "...", "payload": {...}} for compatibility.
    normalized_job = raw_job
    if isinstance(raw_job.get("payload"), dict) and "task_id" not in raw_job:
        normalized_job = dict(cast(dict[str, Any], raw_job["payload"]))
        if "id" not in normalized_job and raw_job.get("job_id") is not None:
            normalized_job["id"] = raw_job["job_id"]

    # Convert the raw job dictionary into a Job object
    job = Job.from_dict(normalized_job)

    # Dependency gating (backend-agnostic)
    # If this job depends on other jobs, ensure all parents are COMPLETED before executing.
    if job.depends_on:
        for parent_id in job.depends_on:
            parent_state = await backend.get_job_state(queue_name, parent_id)
            if parent_state != State.COMPLETED:
                # Parent not done yet, requeue this job slightly in the future to avoid hot loops.
                job.status = State.DELAYED
                await _defer_active_job(backend, queue_name, job.to_dict(), time.time() + 0.05)
                return

    if await backend.is_job_cancelled(queue_name, job.id):
        await _cancel_active_job(backend, queue_name, job.to_dict())
        await event_emitter.emit("job:cancelled", job.to_dict())
        return

    # 1) TTL expiration check: If the job has expired based on its TTL
    if job.is_expired():
        # Update job status to EXPIRED
        job.status = State.EXPIRED
        await _expire_active_job(backend, queue_name, job.to_dict())
        # Emit a job:expired event
        await event_emitter.emit("job:expired", job.to_dict())
        return  # Stop processing this job

    # 2) Delay handling: If the job is scheduled to run later
    # Check if the current time is before the scheduled delay_until time
    if job.delay_until and time.time() < job.delay_until:
        # If delayed, re-enqueue the job into the delayed queue
        job.status = State.DELAYED
        await _defer_active_job(backend, queue_name, job.to_dict(), job.delay_until)
        return  # Stop processing this job for now

    try:
        # 3) Mark active & start event: If the job is ready to be processed
        # Set job status to ACTIVE
        job.status = State.ACTIVE
        job.last_attempt = time.time()
        # Update the state in the backend
        await backend.update_job_state(queue_name, job.id, job.status)
        # Emit a job:started event
        await event_emitter.emit("job:started", job.to_dict())

        if settings.enable_stalled_check:
            await backend.save_heartbeat(queue_name, job.id, time.time())

        async def heartbeat_renewal_loop() -> None:
            heartbeat_failures = 0
            while True:
                await anyio.sleep(_heartbeat_renewal_interval(settings))
                saved = await _save_job_heartbeat_safely(
                    backend,
                    queue_name,
                    job.id,
                    time.time(),
                    heartbeat_failures + 1,
                )
                heartbeat_failures = 0 if saved else heartbeat_failures + 1

        with job_execution_span(settings, queue_name, job.to_dict()) as span:
            try:
                # Retrieve task metadata and the handler function from the registry
                try:
                    meta = TASK_REGISTRY[job.task_id]
                except KeyError:
                    # Try importing as a module named exactly job.task_id
                    try:
                        importlib.import_module(job.task_id)
                    except Exception:
                        pass

                    # If that populated the registry, great.
                    if job.task_id in TASK_REGISTRY:
                        meta = TASK_REGISTRY[job.task_id]
                    else:
                        # Otherwise, import & reload its parent module
                        module_name, _, _ = job.task_id.rpartition(".")
                        if module_name:
                            try:
                                m = importlib.import_module(module_name)
                                importlib.reload(m)
                            except Exception:
                                pass

                        # One more lookup
                        if job.task_id in TASK_REGISTRY:
                            meta = TASK_REGISTRY[job.task_id]
                        else:
                            raise RuntimeError(f"Task {job.task_id!r} not found in TASK_REGISTRY") from None

                handler = meta["func"]

                # Mid-flight cancellation check
                if await backend.is_job_cancelled(queue_name, job.id):
                    raise JobCancelled()

                async def execute_handler() -> Any:
                    # Execute task (sandbox vs direct): run the task, potentially in a sandbox.
                    if settings.sandbox_enabled:
                        return await anyio.to_thread.run_sync(  # noqa
                            cast(Any, sandbox.run_handler),
                            job.task_id,
                            tuple(job.args),  # sandbox expects tuple args
                            job.kwargs,
                            settings.sandbox_default_timeout,
                        )
                    return await handler(*job.args, **job.kwargs)

                if settings.enable_stalled_check:
                    async with anyio.create_task_group() as heartbeat_tg:
                        heartbeat_tg.start_soon(heartbeat_renewal_loop)
                        try:
                            result = await execute_handler()
                        finally:
                            heartbeat_tg.cancel_scope.cancel()
                else:
                    result = await execute_handler()
            except JobCancelled as exc:
                mark_job_span_exception(span, exc, final_status="cancelled")
                raise
            except Exception as exc:
                mark_job_span_exception(span, exc, final_status="error")
                raise
            else:
                mark_job_span_completed(span)

        # 5) Success path: If the task execution completed without exceptions
        # Set job status to COMPLETED
        job.status = State.COMPLETED
        # Store the result of the task execution
        job.result = result
        job.last_error = None
        job.error_traceback = None
        await _complete_active_job(backend, queue_name, job.to_dict(), result)
        # Unblock dependent jobs waiting on this parent.
        resolve_dependency = getattr(backend, "resolve_dependency", None)
        if callable(resolve_dependency):
            await cast(Any, resolve_dependency)(queue_name, job.id)
        # Emit a job:completed event
        await event_emitter.emit("job:completed", job.to_dict())

    except JobCancelled:
        # Cancellation path
        await _cancel_active_job(backend, queue_name, job.to_dict())
        await event_emitter.emit("job:cancelled", job.to_dict())

    except Exception:
        # Exception handling: If any exception occurs during execution
        # Format the traceback to capture the error details
        tb = traceback.format_exc()
        # Log the exception to ensure it's not swallowed
        logger.error(f"Job {job.id} in queue '{queue_name}' failed with exception: \n{tb}")
        # Store the last error message and the full traceback in the job
        job.last_error = str(tb)
        job.error_traceback = tb
        # Increment the retry count
        job.retries += 1

        # Check if the maximum number of retries has been exceeded
        if job.retries > job.max_retries:
            # If retries exhausted, set status to FAILED
            job.status = State.FAILED
            await _fail_active_job(backend, queue_name, job.to_dict())
            # Emit a job:failed event
            await event_emitter.emit("job:failed", job.to_dict())
        else:
            # If retries are still available, calculate the next retry delay
            delay = job.next_retry_delay()
            # Calculate the timestamp for the next retry
            job.delay_until = time.time() + delay
            # Mark retriable failure as delayed before requeueing.
            job.status = State.DELAYED
            await _retry_active_job(backend, queue_name, job.to_dict(), job.delay_until)


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
        heartbeat_interval: float | None = None,
    ) -> None:
        from asyncmq.queues import Queue

        self._settings = asyncmq.monkay.settings
        self.queue = queue if not isinstance(queue, str) else Queue(queue)
        self.id = str(uuid.uuid4())
        self._cancel_scope: anyio.CancelScope | None = None
        self._drain_event: anyio.Event | None = None
        self._stopped_event: anyio.Event | None = None
        self.concurrency = self._settings.worker_concurrency
        self.heartbeat_interval = heartbeat_interval or self._settings.heartbeat_ttl

    async def _run_with_scope(self) -> None:
        backend = asyncmq.monkay.settings.backend
        drain_event = anyio.Event()
        stopped_event = anyio.Event()
        self._drain_event = drain_event
        self._stopped_event = stopped_event

        if monkay.settings.tasks:
            # Trigger the auto discover tasks
            autodiscover_tasks()

        # Start the lifecycle hooks if required
        await run_hooks(
            monkay.settings.worker_on_startup,
            backend=backend,
            worker_id=self.id,
            queue=self.queue.name,
        )

        # Initial registration
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
                try:
                    await backend.register_worker(
                        worker_id=self.id,
                        queue=self.queue.name,
                        concurrency=self.concurrency,
                        timestamp=time.time(),
                    )
                except Exception:
                    logger.error(
                        "Failed to renew worker heartbeat for worker %r on queue %r",
                        self.id,
                        self.queue.name,
                        exc_info=True,
                    )

        try:
            async with anyio.create_task_group() as tg:
                self._cancel_scope = tg.cancel_scope

                async def processor_loop() -> None:
                    await process_job(
                        self.queue.name,
                        CapacityLimiter(self.concurrency),
                        None,
                        backend,
                        drain_event=drain_event,
                    )
                    if drain_event.is_set():
                        tg.cancel_scope.cancel()

                tg.start_soon(heartbeat_loop)
                if monkay.settings.enable_stalled_check:
                    tg.start_soon(stalled_recovery_scheduler, backend)

                # Kick off the real processing loop—
                # process_job will dequeue and handle jobs until cancelled.
                tg.start_soon(processor_loop)

                # Keep this task group alive until cancellation.
                await anyio.sleep(float("inf"))
        finally:
            self._cancel_scope = None
            self._drain_event = None
            try:
                await backend.deregister_worker(self.id)
                # Run the hooks on shutdown
                await run_hooks_safely(
                    monkay.settings.worker_on_shutdown,
                    backend=backend,
                    worker_id=self.id,
                    queue=self.queue.name,
                )
            finally:
                stopped_event.set()
                self._stopped_event = None

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

    async def drain(self) -> None:
        """
        Stop claiming new jobs and wait for this worker run to finish.

        Draining is cooperative: already-running jobs are allowed to finish,
        then the worker deregisters and runs shutdown hooks. If the worker is
        not currently running, this method is a no-op.
        """
        drain_event = self._drain_event
        stopped_event = self._stopped_event
        if drain_event is None or stopped_event is None:
            return
        drain_event.set()
        await stopped_event.wait()

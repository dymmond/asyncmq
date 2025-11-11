import inspect
import time
from typing import Any, Callable, Generic, ParamSpec, Protocol, TypeVar, cast

import anyio

import asyncmq
from asyncmq.backends.base import BaseBackend
from asyncmq.core.event import event_emitter
from asyncmq.jobs import Job

# Global registry holding metadata for all functions registered as asyncmq tasks.
# The dictionary is keyed by a unique task ID (usually 'module.function_name')
# and stores a dictionary containing the callable 'func' (the wrapped function),
# the 'queue' name, and other task-specific metadata like 'retries', 'ttl',
# and 'progress_enabled'.
TASK_REGISTRY: dict[str, dict[str, Any]] = {}

# Type variables to preserve function signature
P = ParamSpec("P")  # For capturing parameter types
R = TypeVar("R")  # For capturing return type


# Protocol for progress reporter function
class ProgressReporter(Protocol):
    def __call__(self, pct: float, data: Any | None = None) -> None: ...


# Type for a function with an optional progress reporter
# For functions that accept progress reporting
class TaskWithProgress(Protocol[P, R]):
    def __call__(self, *args: Any, report_progress: ProgressReporter, **kwargs: Any) -> R: ...


# Wrapper class for task functions that properly exposes attributes to type checker
class TaskWrapper(Generic[P, R]):
    """
    A wrapper class for task functions that properly exposes task-specific
    attributes and methods to the type checker.
    """

    def __init__(
        self,
        func: Callable[P, R],
        task_id: str,
        queue: str,
        retries: int = 0,
        ttl: int | None = None,
        progress_enabled: bool = False,
    ):
        self.func = func
        self.task_id = task_id
        self.queue = queue
        self.retries = retries
        self.ttl = ttl
        self.progress_enabled = progress_enabled

        # Preserve original function metadata
        self.__name__ = func.__name__
        self.__doc__ = func.__doc__
        self.__module__ = func.__module__
        self._is_asyncmq_task = True

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """
        The wrapped task function that gets executed by the worker.

        This function is responsible for setting up the execution context
        for the original task function. If progress reporting is enabled,
        it injects a `report_progress` callback. It then calls the original
        task function and handles whether the result is an awaitable
        (awaiting it) or a regular value (running the function in a thread
        to avoid blocking the event loop).

        Args:
            *args: Positional arguments received by the task function.
            **kwargs: Keyword arguments received by the task function.

        Returns:
            The result returned by the original task function.
        """
        # If progress reporting is enabled, create a progress reporter
        if self.progress_enabled:

            async def execute_with_progress() -> R:
                # Use a local task group for potential progress-emitting tasks
                async with anyio.create_task_group() as tg:
                    # Define the report_progress callback function
                    def report(pct: float, data: Any | None = None) -> None:
                        # Spawn a task to emit the progress event non-blockingly
                        tg.start_soon(
                            event_emitter.emit,
                            "job:progress",
                            # Note: The job ID is set to None here; it's typically
                            # filled in by the worker/handling logic.
                            {"id": None, "progress": pct, "data": data},
                        )

                    # Cast to the appropriate type
                    task_func = cast(TaskWithProgress[P, R], self.func)

                    # Execute the function with progress reporting based on its type
                    if inspect.iscoroutinefunction(self.func):
                        # For async functions
                        result = await task_func(*args, report_progress=report, **kwargs)
                    else:
                        # For sync functions - create a wrapper to handle keyword arguments
                        def sync_wrapper() -> R:
                            return task_func(*args, report_progress=report, **kwargs)

                        result = await anyio.to_thread.run_sync(sync_wrapper)

                    return cast(R, result)

            return await execute_with_progress()
        else:
            # If progress reporting is not enabled, execute the function directly
            if inspect.iscoroutinefunction(self.func):
                # For async functions
                result = await self.func(*args, **kwargs)
                return cast(R, result)
            else:
                # For sync functions - create a wrapper to handle keyword arguments
                def sync_wrapper() -> R:
                    return self.func(*args, **kwargs)

                result = await anyio.to_thread.run_sync(sync_wrapper)
                return cast(R, result)

    async def enqueue(
        self,
        *args: Any,
        backend: BaseBackend | None = None,
        delay: float = 0,
        priority: int = 5,
        depends_on: list[str] | None = None,
        repeat_every: float | None = None,
        **kwargs: Any,
    ) -> str | None:
        """
        Helper method to enqueue a new job for this task.

        This method constructs a `Job` object with the specified arguments,
        keyword arguments, and task-specific configuration (retries, ttl,
        etc.). It handles dependency registration and enqueues the job
        either immediately or with a specified delay using the provided
        backend object.

        Args:
            *args: Positional arguments to be passed to the task function.
            backend: An object providing the necessary interface for
                     interacting with the queue, including
                     `add_dependencies`, `enqueue_delayed`, and `enqueue`.
            delay: The time in seconds to wait before the job becomes
                   available for processing. Defaults to 0 (immediate).
            priority: The priority level of the job (lower numbers
                      typically mean higher priority). Defaults to 5.
            depends_on: A list of job IDs that this job depends on. This job
                        will not be executed until all dependent jobs are
                        completed. Defaults to None.
            repeat_every: If set, the job will be automatically re-enqueued
                          after successful completion, with this value as
                          the delay between completions. Defaults to None.
            **kwargs: Keyword arguments to be passed to the task function.

        Returns:
            The job ID for the enqueued job or None if a delay is provided
        """
        # Create a Job object from the provided arguments and task metadata.
        job = Job(
            task_id=self.task_id,
            args=list(args) if args else [],
            kwargs=kwargs or {},
            max_retries=self.retries,
            ttl=self.ttl,
            priority=priority,
            depends_on=depends_on,
            repeat_every=repeat_every,
        )

        # If the job has dependencies, add them to the backend.
        backend = backend or asyncmq.monkay.settings.backend

        if job.depends_on:
            await backend.add_dependencies(self.queue, job.to_dict())

        # Enqueue the job, either with a delay or immediately.
        if delay and delay > 0:
            run_at = time.time() + delay
            job.delay_until = run_at
            await backend.enqueue_delayed(self.queue, job.to_dict(), run_at)
            return None
        else:
            return await backend.enqueue(self.queue, job.to_dict())

    # Alias for enqueue
    delay = enqueue
    send = enqueue


def task(
    queue: str,
    retries: int = 0,
    ttl: int | None = None,
    progress: bool = False,
) -> Callable[[Callable[P, R]], TaskWrapper[P, R]]:
    """
    Decorator factory used to register an asynchronous or synchronous function
    as an asyncmq task.

    When applied to a function, this decorator enhances it by adding an
    `enqueue` method and storing task-specific metadata in the global
    `TASK_REGISTRY`. The decorated function itself becomes a wrapper that
    executes the original task logic, handling potential `report_progress`
    calls and managing synchronous function execution within a thread.

    Args:
        queue: The name of the message queue where jobs for this task should
               be enqueued. Workers process jobs from specific queues.
        retries: The maximum number of times a failed job for this task
                 should be retried. A value of 0 means no retries. Defaults to 0.
        ttl: The time-to-live (TTL) for jobs of this task, in seconds. If a job
             is not processed before its TTL expires, it is considered expired
             and moved to the Dead Letter Queue (DLQ). Defaults to None (no TTL).
        progress: If True, the task function will be injected with a
                  `report_progress` keyword argument, which is a callable
                  function allowing the task to report its progress back to
                  the worker or monitoring system. Defaults to False.

    Returns:
        A decorator function that takes the original task function as input
        and returns a wrapped version of that function.
    """

    def decorator(func: Callable[P, R]) -> TaskWrapper[P, R]:
        """
        The actual decorator applied to the user's task function.

        This function generates a unique task ID, creates a TaskWrapper instance,
        registers the task metadata, and returns the TaskWrapper.

        Args:
            func: The original asynchronous or synchronous function being
                  decorated as an asyncmq task.

        Returns:
            A TaskWrapper instance that wraps the original function.
        """
        # Generate a unique task ID based on the module and function name.
        module = func.__module__
        name = func.__name__
        task_id = f"{module}.{name}"

        # Create the task wrapper
        task_wrapper = TaskWrapper(
            func=func,
            task_id=task_id,
            queue=queue,
            retries=retries,
            ttl=ttl,
            progress_enabled=progress,
        )

        # Register the task metadata in the global registry.
        TASK_REGISTRY[task_id] = {
            "func": task_wrapper,
            "queue": queue,
            "retries": retries,
            "ttl": ttl,
            "progress_enabled": progress,
        }

        # Return the wrapped function.
        return task_wrapper

    # Return the decorator function itself.
    return decorator


def list_tasks() -> dict[str, dict[str, Any]]:
    """
    Retrieves the metadata for all tasks that have been registered using the
    `@task` decorator.

    Returns:
        A dictionary where keys are task IDs (string) and values are
        dictionaries containing the task's metadata (function, queue,
        retries, ttl, progress_enabled).
    """
    # Return the global task registry.
    return TASK_REGISTRY

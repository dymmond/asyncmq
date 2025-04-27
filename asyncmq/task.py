import inspect
import time
from typing import Any, Callable, Dict, Optional

import anyio

from asyncmq.event import event_emitter
from asyncmq.job import Job

# Registry for all tasks and their metadata
TASK_REGISTRY: Dict[str, Dict[str, Any]] = {}


def task(
    queue: str,
    retries: int = 0,
    ttl: Optional[int] = None,
    progress: bool = False
) -> Callable[..., Any]:
    """
    Decorator to register a function as an asyncmq task.

    :param queue: Name of the queue tasks should be enqueued on.
    :param retries: Number of retries on failure.
    :param ttl: Time-to-live for the job in seconds.
    :param progress: If True, exposes a `report_progress` callback to the task.
    """
    def decorator(func: Callable) -> Callable:
        module = func.__module__
        name = func.__name__
        task_id = f"{module}.{name}"

        async def enqueue_task(
            backend,
            *args,
            delay: float = 0,
            priority: int = 5,
            depends_on: Optional[list[str]] = None,
            repeat_every: Optional[float] = None,
            **kwargs
        ):
            # Build job
            job = Job(
                task_id=task_id,
                args=list(args) if args else [],
                kwargs=kwargs or {},
                max_retries=retries,
                ttl=ttl,
                priority=priority,
                depends_on=depends_on,
                repeat_every=repeat_every
            )
            # Register dependencies
            if job.depends_on:
                await backend.add_dependencies(queue, job.to_dict())
            # Enqueue immediately or delayed
            if delay and delay > 0:
                run_at = time.time() + delay
                job.delay_until = run_at
                await backend.enqueue_delayed(queue, job.to_dict(), run_at)
            else:
                await backend.enqueue(queue, job.to_dict())

        async def wrapper(*args, **kwargs):
            # Use a local task group for any progress‐emit tasks
            async with anyio.create_task_group() as tg:
                if progress:
                    def report(pct: float, data: Any = None):
                        tg.start_soon(
                            event_emitter.emit,
                            "job:progress",
                            {"id": None, "progress": pct, "data": data},
                        )
                    result = func(*args, report_progress=report, **kwargs)
                else:
                    result = func(*args, **kwargs)

                # If the function returned an awaitable, await it
                if inspect.isawaitable(result):
                    return await result
                # Otherwise run synchronously in a thread
                return await anyio.to_thread.run_sync(lambda: result)

        # Preserve metadata
        wrapper.__name__ = name
        wrapper.__doc__ = func.__doc__
        wrapper.__module__ = module

        # Attach helper methods/attributes
        wrapper.enqueue = enqueue_task  # type: ignore
        wrapper.task_id = task_id      # type: ignore
        wrapper._is_asyncmq_task = True # type: ignore

        # Register in the global registry
        TASK_REGISTRY[task_id] = {
            "func": wrapper,
            "queue": queue,
            "retries": retries,
            "ttl": ttl,
            "progress_enabled": progress
        }

        return wrapper

    return decorator


def list_tasks() -> Dict[str, Dict[str, Any]]:
    """
    Returns metadata for all registered tasks.
    """
    return TASK_REGISTRY

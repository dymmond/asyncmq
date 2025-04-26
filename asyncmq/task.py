import asyncio
import inspect
import time
from typing import Any, Callable, Dict, Optional

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

        # Register metadata
        TASK_REGISTRY[task_id] = {
            "func": func,
            "queue": queue,
            "retries": retries,
            "ttl": ttl,
            "progress_enabled": progress
        }

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

            if job.depends_on:
                await backend.add_dependencies(queue, job.to_dict())

            if delay and delay > 0:
                run_at = time.time() + delay
                job.delay_until = run_at
                await backend.enqueue_delayed(queue, job.to_dict(), run_at)
            else:
                await backend.enqueue(queue, job.to_dict())

        # Attach enqueue function
        func.enqueue = enqueue_task  # type: ignore

        # Optionally provide progress reporter inside the task
        async def wrapper(*args, **kwargs):
            # If progress reporting is enabled, inject reporter
            if progress:
                # Create a simple callback
                def report(pct: float, data: Any = None):
                    event_emitter.emit(
                        "job:progress",
                        {"id": None, "progress": pct, "data": data}
                    )
                result = func(*args, report_progress=report, **kwargs)
            else:
                result = func(*args, **kwargs)

            # If function is sync, run in thread
            if inspect.iscoroutine(result):
                return await result
            return await asyncio.to_thread(lambda: result)

        # Copy metadata to wrapper so scheduler/worker sees it
        wrapper.task_id = task_id  # type: ignore
        wrapper._is_asyncmq_task = True  # type: ignore
        return wrapper

    return decorator


def list_tasks() -> Dict[str, Dict[str, Any]]:
    """
    Returns metadata for all registered tasks.
    """
    return TASK_REGISTRY

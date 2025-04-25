import time
from typing import Any, Callable

from asyncmq.job import Job

TASK_REGISTRY = {}

def task(queue: str, retries: int = 0, ttl: int = 60) -> Callable[..., Any]:
    def wrapper(func):
        task_id = f"{func.__module__}.{func.__name__}"
        TASK_REGISTRY[task_id] = {
            "func": func,
            "queue": queue,
            "retries": retries,
            "ttl": ttl
        }

        async def enqueue_task(backend, *args, delay: float = 0, priority: int = 5, **kwargs):
            job = Job(
                task_id=task_id,
                args=args,
                kwargs=kwargs,
                max_retries=retries,
                ttl=ttl
            )
            job.priority = priority
            if delay:
                job.delay_until = time.time() + delay
                await backend.enqueue_delayed(queue, job.to_dict(), job.delay_until)
            else:
                await backend.enqueue(queue, job.to_dict())

        func.enqueue = enqueue_task
        return func
    return wrapper

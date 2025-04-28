from typing import TYPE_CHECKING

from monkay import Monkay

__version__ = "0.1.0"

if TYPE_CHECKING:
    from .backends.memory import InMemoryBackend
    from .backends.redis import RedisBackend
    from .conf import settings
    from .job import Job
    from .queue import Queue
    from .stores.base import BaseJobStore
    from .tasks import task
    from .worker import Worker

_monkay: Monkay = Monkay(
    globals(),
    lazy_imports={
        "BaseJobStore": ".stores.base.BaseJobStore",
        "InMemoryBackend": ".backends.memory.InMemoryBackend",
        "Job": ".job.Job",
        "Queue": ".queue.Queue",
        "RedisBackend": ".backends.redis.RedisBackend",
        "Worker": ".worker.Worker",
        "settings": ".conf.settings",
    },
    skip_all_update=True,
    package="asyncmq",
)

__all__ = [
    "BaseJobStore",
    "InMemoryBackend",
    "Job",
    "Queue",
    "RedisBackend",
    "task",
    "Worker"
]

_monkay.add_lazy_import("task", ".tasks.task")

from typing import TYPE_CHECKING

from monkay import Monkay

__version__ = "0.1.0"

if TYPE_CHECKING:
    from .backends.memory import InMemoryBackend
    from .backends.redis import RedisBackend
    from .job import Job
    from .queue import Queue
    from .stores.base import BaseJobStore
    from .tasks import task
    from .worker import Worker

_monkay: Monkay = Monkay(
    globals(),
    lazy_imports={
        "BaseJobStore": ".stores.base",
        "InMemoryBackend": ".backends.memory",
        "Job": ".job",
        "Queue": ".queue",
        "RedisBackend": ".backends.redis",
        "Worker": ".worker",
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

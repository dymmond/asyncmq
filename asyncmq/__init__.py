from typing import TYPE_CHECKING

from monkay import Monkay

__version__ = "0.2.2"

if TYPE_CHECKING:
    from .backends.memory import InMemoryBackend
    from .backends.redis import RedisBackend
    from .conf import settings
    from .conf.global_settings import Settings
    from .jobs import Job
    from .queues import Queue
    from .stores.base import BaseJobStore
    from .tasks import task
    from .workers import Worker

monkay: Monkay = Monkay(
    globals(),
    lazy_imports={
        "BaseJobStore": ".stores.base.BaseJobStore",
        "InMemoryBackend": ".backends.memory.InMemoryBackend",
        "Job": ".jobs.Job",
        "Queue": ".queues.Queue",
        "RedisBackend": ".backends.redis.RedisBackend",
        "Worker": ".workers.Worker",
        "settings": ".conf.settings",
        "Settings": ".conf.global_monkay.settings.Settings",
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
    "Settings",
    "settings",
    "task",
    "Worker",
]

monkay.add_lazy_import("task", ".tasks.task")

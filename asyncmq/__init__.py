from typing import TYPE_CHECKING

__version__ = "0.6.0"

from .monkay import create_monkay

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

monkay = create_monkay(globals())
del create_monkay

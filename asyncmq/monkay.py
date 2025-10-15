from __future__ import annotations

import os
from typing import TYPE_CHECKING

from monkay import Monkay

if TYPE_CHECKING:
    from asyncmq.conf.global_settings import Settings


def create_monkay(global_dict: dict) -> Monkay[None, Settings]:
    monkay: Monkay[None, Settings] = Monkay(
        global_dict,
        # enable if we want to have extensions. The second line is only relevant if they should be loaded from settings
        # with_extensions=True,
        # settings_extensions_name="extensions",
        settings_path=lambda: os.environ.get("ASYNCMQ_SETTINGS_MODULE", "asyncmq.conf.global_settings.Settings"),
        lazy_imports={
            # this way we have always fresh settings because of the forward
            "settings": "asyncmq.conf.settings",  # Lazy import for application settings
            "BaseJobStore": "asyncmq.stores.base.BaseJobStore",
            "InMemoryBackend": "asyncmq.backends.memory.InMemoryBackend",
            "Job": "asyncmq.jobs.Job",
            "Queue": "asyncmq.queues.Queue",
            "RedisBackend": "asyncmq.backends.redis.RedisBackend",
            "Worker": "asyncmq.workers.Worker",
            "Settings": "asyncmq.conf.global_settings.Settings",
            "task": "asyncmq.tasks.task",
        },
        skip_all_update=True,
        package="asyncmq",
    )
    return monkay

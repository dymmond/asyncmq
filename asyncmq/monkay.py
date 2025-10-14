from __future__ import annotations

import os
from typing import Any

from monkay import Monkay


def create_monkay(global_dict: dict) -> Any:
    monkay: Monkay = Monkay[None, Any](
        global_dict,
        with_extensions=True,
        settings_path=lambda: os.environ.get("ASYNCMQ_SETTINGS_MODULE", "asyncmq.conf.global_settings.Settings") or "",
        # uncached_imports={"settings"},
        lazy_imports={
            "settings": lambda: monkay.settings,  # Lazy import for application settings
            # "settings": "asyncmq.conf.settings",  # Lazy import for application settings
            "BaseJobStore": "asyncmq.stores.base.BaseJobStore",
            "InMemoryBackend": "asyncmq.backends.memory.InMemoryBackend",
            "Job": "asyncmq.jobs.Job",
            "Queue": "asyncmq.queues.Queue",
            "RedisBackend": "asyncmq.backends.redis.RedisBackend",
            "Worker": "asyncmq.workers.Worker",
            "Settings": "asyncmq.conf.global_settings.Settings",
        },
        skip_all_update=True,
        package="asyncmq",
    )
    monkay.add_lazy_import("task", "asyncmq.tasks.task")
    return monkay

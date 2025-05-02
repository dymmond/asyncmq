from dataclasses import dataclass

from asyncmq import Settings as BaseSettings
from asyncmq.backends.base import BaseBackend
from asyncmq.backends.redis import RedisBackend


@dataclass
class Settings(BaseSettings):
    """
    Extend global defaults with your own preferences.
    """
    # URL for your Redis server (change to suit your environment)
    backend: BaseBackend = RedisBackend(redis_url="redis://localhost:6379/0")

    # How many tasks a worker will run concurrently (default: 3)
    worker_concurrency: int = 4

    # How often (in seconds) to scan for delayed jobs
    scan_interval: float = 1.0

    # Logging verbosity: DEBUG, INFO, WARNING, ERROR
    logging_level: str = "INFO"

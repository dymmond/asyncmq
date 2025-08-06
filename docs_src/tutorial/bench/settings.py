from asyncmq.backends.base import BaseBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.conf.global_settings import Settings as BaseSettings


class Settings(BaseSettings):
    backend: BaseBackend = RedisBackend(redis_url_or_client="redis://localhost:6379/0")
    # Lower scan interval to 0.2 seconds for sub-second delayed job latency
    scan_interval: float = 0.2

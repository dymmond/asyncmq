from asyncmq.backends.redis import RedisBackend
from asyncmq.conf.global_settings import Settings


class AsyncMQSettings(Settings):
    # Use Redis at a custom URL
    backend = RedisBackend(redis_url="redis://redis:6379/0")
    # Increase default worker concurrency
    worker_concurrency = 5
    # Enable debug mode for verbose logging
    debug = True

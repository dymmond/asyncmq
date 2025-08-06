from asyncmq.backends.redis import RedisBackend
from asyncmq.queues import Queue

queue = Queue(
    name="default",
    backend=RedisBackend(redis_url_or_client="redis://localhost:6379/0"),
    concurrency=5,
    rate_limit=10,        # max 10 jobs per rate_interval
    rate_interval=1.0     # per second
)

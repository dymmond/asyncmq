from asyncmq.backends.redis import RedisBackend
from asyncmq.queues import Queue

backend = RedisBackend(redis_url="redis://localhost:6379/0")
# Override to poll every 0.2s just for the 'perf' queue
queue = Queue(
   name="perf",
   backend=backend,
   concurrency=20,
   scan_interval=0.2
)

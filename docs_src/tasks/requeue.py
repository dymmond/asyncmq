from asyncmq.backends.redis import RedisBackend
from asyncmq.tasks import task

backend = RedisBackend(redis_url_or_client="redis://localhost:6379/0")

@task(queue="maintenance")
def cleanup_temp():
    import os; os.remove_temp_files()

# Schedule every 3600s (1h)
await cleanup_temp.enqueue(backend, repeat_every=3600)

from asyncmq.backends.redis import RedisBackend
from asyncmq.tasks import task

backend = RedisBackend(redis_url_or_client=...)

@task(queue="default", retries=2, ttl=60)
async def greet(name: str):
    print(f"Hello {name}")

# Enqueue a job explicitly decorating alone does NOT enqueue on call.
# You explicitly want to pass a backend
await greet.enqueue(backend, "Alice", delay=5, priority=3)

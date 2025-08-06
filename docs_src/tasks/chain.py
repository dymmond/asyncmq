from asyncmq.backends.redis import RedisBackend
from asyncmq.tasks import task

backend = RedisBackend(redis_url_or_client="redis://localhost:6379/0")

# Job A
@task(queue="pipeline")
async def step_one(): ...
# Job B depends on A
@task(queue="pipeline")
async def step_two(): ...

# Enqueue A then B waits for A
id_a = await step_one.enqueue(backend)
id_b = await step_two.enqueue(backend, depends_on=[id_a])

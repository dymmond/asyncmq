from asyncmq.backends.redis import RedisBackend
from asyncmq.logging import logger
from asyncmq.tasks import task

backend = RedisBackend()

@task(queue="default")
async def say_hello(name: str):
    logger.info(f"👋 Hello, {name}!")

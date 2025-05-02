from asyncmq.tasks import task


@task(queue="default", retries=2, ttl=60)
async def greet(name: str):
    print(f"Hello {name}")

# Enqueue a job explicitly decorating alone does NOT enqueue on call.
await greet.enqueue("Alice", delay=5, priority=3)

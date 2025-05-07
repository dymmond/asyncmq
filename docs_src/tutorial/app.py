import asyncio
from contextlib import asynccontextmanager

from esmerald import Esmerald, Gateway, get, post
from pydantic import BaseModel

from asyncmq.backends.redis import RedisBackend
from asyncmq.logging import logger
from asyncmq.queues import Queue

from .tasks import send_welcome

# Instantiate backend and queue (must mirror settings.py)
backend = RedisBackend(redis_url="redis://localhost:6379/0")
email_queue = Queue(name="email", backend=backend)

class SignupPayload(BaseModel):
    email: str

@post(path="/signup")
async def signup(payload: SignupPayload) -> dict:
    """
    Enqueue a send_welcome job and return immediately.
    """
    job_id = await send_welcome.enqueue(
        payload.email,
        delay=0,           # Optional: schedule in the future
        priority=5         # 1=high priority, 10=low priority
    )
    return {"status": "queued", "job_id": job_id}

# Health-check endpoint
@get(path="/health")
async def health() -> dict:
    stats = await email_queue.queue_stats()
    return {s.name: count for s, count in stats.items()}

# Lifecycle events for worker management
async def on_startup():
    logger.info("🚀 Starting background worker...")
    # Run in background; .run() is async and never returns on its own
    app.state.worker_task = asyncio.create_task(
        email_queue.run()
    )

async def on_shutdown():
    logger.info("🛑 Shutting down worker...")
    # Cancel and await graceful exit
    app.state.worker_task.cancel()
    try:
        await app.state.worker_task
    except asyncio.CancelledError:
        ...

@asynccontextmanager
async def lifespan(app: Esmerald):
    # What happens on startup
    await on_startup()
    yield
    await on_shutdown()

# Assemble the app
app = Esmerald(
    routes=[
        Gateway(handler=signup),
        Gateway(handler=health)
    ],
    lifespan=lifespan
)

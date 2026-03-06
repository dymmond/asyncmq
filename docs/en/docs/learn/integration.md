# Integration

## Integrating AsyncMQ in an ASGI App

Typical integration pattern:

- define tasks in importable modules
- enqueue from request handlers/services
- run workers as separate processes (recommended)

## FastAPI Producer Example

```python
from fastapi import FastAPI
from asyncmq.queues import Queue
from myapp.tasks import send_email

app = FastAPI()
queue = Queue("emails")


@app.post("/users/{email}")
async def create_user(email: str):
    await send_email.enqueue(email, backend=queue.backend)
    return {"queued": True}
```

Run worker separately:

```bash
asyncmq worker start emails --concurrency 4
```

## Running Worker in-Process (Development)

For local development/tests, you can start a worker task in app lifespan.

```python
import anyio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from asyncmq.queues import Queue

queue = Queue("emails")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with anyio.create_task_group() as tg:
        tg.start_soon(queue.run)
        yield
        tg.cancel_scope.cancel()


app = FastAPI(lifespan=lifespan)
```

For production, prefer dedicated worker processes for isolation and scaling.

import asyncio

from event import event_emitter
from runner import run_worker

from asyncqueue.backends.memory import InMemoryBackend

# 👇 Repeatable jobs config
REPEATABLE_JOBS = [
    {
        "task_id": "tasks.cleanup",
        "args": [],
        "kwargs": {},
        "repeat_every": 30,  # every 30 seconds
        "queue": "default"
    }
]

# 👇 Optional event logging for visibility
async def log_event(event):
    print(f"[EVENT] {event['status'].upper()} - Job {event['id']} ({event['task']})")

event_emitter.on("job:started", log_event)
event_emitter.on("job:completed", log_event)
event_emitter.on("job:failed", log_event)
event_emitter.on("job:expired", log_event)
event_emitter.on("job:requeued", log_event)

# 👇 Main entrypoint
if __name__ == "__main__":
    backend = InMemoryBackend()
    asyncio.run(run_worker(
        queue_name="default",
        backend=backend,
        concurrency=3,
        rate_limit=10,
        rate_interval=1.0,
        repeatables=REPEATABLE_JOBS
    ))

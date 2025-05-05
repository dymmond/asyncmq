import json

import anyio
from lilya.controllers import Controller
from lilya.requests import Request
from lilya.responses import StreamingResponse

from asyncmq.conf import settings


class SSEController(Controller):
    """
    Streams Server-Sent Events for real-time dashboard updates.
    Emits an 'overview' event with JSON payload:
      { total_queues, total_jobs, total_workers }
    every 5 seconds.
    """

    async def get(self, request: Request) -> StreamingResponse:
        backend = settings.backend

        async def event_generator() -> None:
            while True:
                # 1) compute total queues
                queues = await backend.list_queues()  # noqa
                total_queues = len(queues)

                # 2) compute total jobs across states
                total_jobs = 0
                for q in queues:
                    for state in ("waiting", "active", "completed", "failed", "delayed"):
                        jobs = await backend.list_jobs(q, state)
                        total_jobs += len(jobs)

                # 3) compute total workers
                total_workers = len(await backend.list_workers())

                payload = {
                    "total_queues": total_queues,
                    "total_jobs": total_jobs,
                    "total_workers": total_workers,
                }

                # send a named event 'overview'
                yield f"event: overview\ndata: {json.dumps(payload)}\n\n"  # noqa

                # pause before the next update
                await anyio.sleep(5)

        return StreamingResponse(event_generator(), media_type="text/event-stream")  # type: ignore

import json
from datetime import datetime

import anyio
from lilya.controllers import Controller
from lilya.requests import Request
from lilya.responses import StreamingResponse

from asyncmq.conf import monkay


class SSEController(Controller):
    """
    Streams Server-Sent Events for real-time dashboard updates.
    Emits these event types every 5 seconds:
      - 'overview': { total_queues, total_jobs, total_workers }
      - 'job dist':  { waiting, active, delayed, completed, failed }
      - 'metrics':  { throughput, avg_duration, retries, failures }
      - 'queues':   [ { name, paused, waiting, active, delayed, failed, completed }, ... ]
      - 'workers':  [ { id, queue, concurrency, heartbeat }, ... ]
      - latest_jobs    : [ { id, queue, state, time }, … ]
      - latest_queues  : [ { name, time }, … ]
    """

    async def get(self, request: Request) -> StreamingResponse:
        backend = monkay.settings.backend

        async def event_generator() -> None:
            while True:
                queues = await backend.list_queues()

                # OVERVIEW
                total_queues = len(queues)
                total_jobs = 0
                for q in queues:
                    for s in ("waiting", "active", "completed", "failed", "delayed"):
                        total_jobs += len(await backend.list_jobs(q, s))
                total_workers = len(await backend.list_workers())

                overview = {
                    "total_queues": total_queues,
                    "total_jobs": total_jobs,
                    "total_workers": total_workers,
                }
                yield f"event: overview\ndata: {json.dumps(overview)}\n\n"  # noqa

                # JOB DISTRIBUTION
                dist = dict.fromkeys(("waiting", "active", "delayed", "completed", "failed"), 0)
                for q in queues:
                    for s in dist:
                        dist[s] += len(await backend.list_jobs(q, s))
                yield f"event: jobdist\ndata: {json.dumps(dist)}\n\n"

                # METRICS
                metrics = {
                    "throughput": dist["completed"],
                    "avg_duration": None,
                    "retries": dist["failed"],
                    "failures": dist["failed"],
                }
                yield f"event: metrics\ndata: {json.dumps(metrics)}\n\n"

                # QUEUE STATS
                qrows = []
                for q in queues:
                    paused = hasattr(backend, "is_queue_paused") and await backend.is_queue_paused(q)
                    counts = {
                        s: len(await backend.list_jobs(q, s))
                        for s in ("waiting", "active", "delayed", "failed", "completed")
                    }
                    qrows.append({"name": q, "paused": paused, **counts})
                yield f"event: queues\ndata: {json.dumps(qrows)}\n\n"

                # WORKERS
                wk = await backend.list_workers()
                wk_rows = [
                    {"id": w.id, "queue": w.queue, "concurrency": w.concurrency, "heartbeat": w.heartbeat} for w in wk
                ]
                yield f"event: workers\ndata: {json.dumps(wk_rows)}\n\n"

                # LATEST 10 JOBS
                all_jobs = []
                for q in queues:
                    for s in ("waiting", "active", "completed", "failed", "delayed"):
                        for job in await backend.list_jobs(q, s):
                            ts = job.get("timestamp") or job.get("created_at") or 0
                            all_jobs.append(
                                {
                                    "id": job.get("id"),
                                    "queue": q,
                                    "state": s,
                                    "ts": ts,
                                }
                            )
                all_jobs.sort(key=lambda j: j["ts"], reverse=True)
                latest_jobs = [
                    {
                        "id": j["id"],
                        "queue": j["queue"],
                        "state": j["state"],
                        "time": datetime.fromtimestamp(j["ts"]).strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    for j in all_jobs[:10]
                ]
                yield f"event: latest_jobs\ndata: {json.dumps(latest_jobs)}\n\n"

                # --- RECENT 5 QUEUES BY ACTIVITY ---
                last_act: dict[str, float] = {}
                for j in all_jobs:
                    last_act[j["queue"]] = max(last_act.get(j["queue"], 0), j["ts"])
                qacts = [
                    {"name": q, "time": datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")}
                    for q, ts in last_act.items()
                ]
                qacts.sort(key=lambda x: x["time"], reverse=True)
                yield f"event: latest_queues\ndata: {json.dumps(qacts[:5])}\n\n"

                await anyio.sleep(5)

        return StreamingResponse(event_generator(), media_type="text/event-stream")  # type: ignore

from __future__ import annotations

import json
from datetime import datetime
from typing import (
    Any,
    AsyncGenerator,
)

import anyio
from lilya.controllers import Controller
from lilya.requests import Request
from lilya.responses import StreamingResponse

from asyncmq import monkay

BackendJobRecord = dict[str, Any]
BackendWorkerRecord = Any


class SSEController(Controller):
    """
    Streams Server-Sent Events (SSE) for real-time dashboard updates.

    This controller periodically polls the AsyncMQ backend for comprehensive system status
    and broadcasts the aggregated data to the client in structured events.

    Emits the following event types every 5 seconds:
      - 'overview': { total_queues, total_jobs, total_workers }
      - 'jobdist':  { waiting, active, delayed, completed, failed }
      - 'metrics':  { throughput, avg_duration, retries, failures }
      - 'queues':   [ { name, paused, waiting, active, delayed, failed, completed }, ... ]
      - 'workers':  [ { id, queue, concurrency, heartbeat }, ... ]
      - 'latest_jobs': [ { id, queue, state, time }, ... ]
      - 'latest_queues': [ { name, time }, ... ]
    """

    async def get(self, request: Request) -> StreamingResponse:
        """
        Handles the GET request and starts the continuous SSE stream.

        Args:
            request: The incoming Lilya Request object.

        Returns:
            StreamingResponse: An HTTP response configured for SSE (`text/event-stream`).
        """
        backend: Any = monkay.settings.backend

        async def event_generator() -> AsyncGenerator[str, None]:
            """
            The core async generator that polls the backend, aggregates data, and yields
            SSE-formatted strings.
            """
            while True:
                # 1. INITIAL SETUP: Get all queues (required for most subsequent steps)
                queues: list[str] = await backend.list_queues()

                # --- OVERVIEW: Total Counts ---
                total_jobs: int = 0
                total_queues: int = len(queues)

                # Fetch job counts across all states/queues
                for q in queues:
                    for s in ("waiting", "active", "completed", "failed", "delayed"):
                        jobs: list[BackendJobRecord] = await backend.list_jobs(q, s)
                        total_jobs += len(jobs)

                total_workers: int = len(await backend.list_workers())

                overview: dict[str, int] = {
                    "total_queues": total_queues,
                    "total_jobs": total_jobs,
                    "total_workers": total_workers,
                }
                yield f"event: overview\ndata: {json.dumps(overview)}\n\n"  # noqa

                # --- JOB DISTRIBUTION: Sum by State ---
                # dict.fromkeys initializes all values to 0
                dist: dict[str, int] = dict.fromkeys(("waiting", "active", "delayed", "completed", "failed"), 0)

                for q in queues:
                    for s in dist:
                        dist[s] += len(await backend.list_jobs(q, s))
                yield f"event: jobdist\ndata: {json.dumps(dist)}\n\n"

                # --- METRICS: Throughput/Failure Summary ---
                metrics: dict[str, int | None] = {
                    "throughput": dist["completed"],
                    "avg_duration": None,
                    "retries": dist["failed"],
                    "failures": dist["failed"],
                }
                yield f"event: metrics\ndata: {json.dumps(metrics)}\n\n"

                # --- QUEUE STATS: Per-Queue Detail ---
                qrows: list[dict[str, Any]] = []
                for q in queues:
                    paused: bool = hasattr(backend, "is_queue_paused") and await backend.is_queue_paused(q)
                    counts: dict[str, int] = {
                        s: len(await backend.list_jobs(q, s))
                        for s in ("waiting", "active", "delayed", "failed", "completed")
                    }
                    qrows.append({"name": q, "paused": paused, **counts})
                yield f"event: queues\ndata: {json.dumps(qrows)}\n\n"

                # --- WORKERS ---
                wk: list[BackendWorkerRecord] = await backend.list_workers()
                wk_rows: list[dict[str, Any]] = [
                    {
                        "id": w.id,
                        "queue": w.queue,
                        "concurrency": w.concurrency,
                        "heartbeat": w.heartbeat,
                    }
                    for w in wk
                ]
                yield f"event: workers\ndata: {json.dumps(wk_rows)}\n\n"

                # --- LATEST 10 JOBS (Time-Intensive Aggregation) ---
                all_jobs_raw: list[dict[str, Any]] = []
                for q in queues:
                    for s in ("waiting", "active", "completed", "failed", "delayed"):
                        for job in await backend.list_jobs(q, s):
                            # Extract timestamp (ts) using fallbacks
                            ts: float = job.get("timestamp") or job.get("created_at") or 0
                            all_jobs_raw.append(
                                {
                                    "id": job.get("id"),
                                    "queue": q,
                                    "state": s,
                                    "ts": ts,
                                }
                            )

                # Sort by timestamp (most recent first)
                all_jobs_raw.sort(key=lambda j: j["ts"], reverse=True)

                # Format the top 10 results
                latest_jobs: list[dict[str, Any]] = [
                    {
                        "id": j["id"],
                        "queue": j["queue"],
                        "state": j["state"],
                        "time": datetime.fromtimestamp(j["ts"]).strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    for j in all_jobs_raw[:10]
                ]
                yield f"event: latest_jobs\ndata: {json.dumps(latest_jobs)}\n\n"

                # --- RECENT 5 QUEUES BY ACTIVITY ---
                last_act: dict[str, float] = {}
                for j in all_jobs_raw:
                    # Find the maximum (latest) timestamp for each queue
                    last_act[j["queue"]] = max(last_act.get(j["queue"], 0.0), j["ts"])

                # Format and sort by activity time
                qacts: list[dict[str, str | float]] = [
                    {
                        "name": q,
                        "time": datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    for q, ts in last_act.items()
                ]
                qacts.sort(key=lambda x: x["time"], reverse=True)  # Sorts by formatted time string

                yield f"event: latest_queues\ndata: {json.dumps(qacts[:5])}\n\n"

                await anyio.sleep(5)

        return StreamingResponse(event_generator(), media_type="text/event-stream")

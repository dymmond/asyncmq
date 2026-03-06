from __future__ import annotations

import json
from datetime import datetime
from typing import (
    Any,
    AsyncGenerator,
    cast,
)

import anyio
from lilya.controllers import Controller
from lilya.requests import Request
from lilya.responses import StreamingResponse

from asyncmq import monkay
from asyncmq.contrib.dashboard.controllers._counts import JOB_STATES, get_queue_state_counts
from asyncmq.contrib.dashboard.metrics_history import record_metrics_snapshot

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
                try:
                    queues: list[str] = await backend.list_queues()
                except Exception:
                    queues = []
                queue_counts: dict[str, dict[str, int]] = {}
                for queue_name in queues:
                    queue_counts[queue_name] = await get_queue_state_counts(backend, queue_name)

                # --- OVERVIEW: Total Counts ---
                total_queues: int = len(queues)
                total_jobs: int = sum(sum(counts.values()) for counts in queue_counts.values())

                try:
                    total_workers: int = len(await backend.list_workers())
                except Exception:
                    total_workers = 0

                overview: dict[str, int] = {
                    "total_queues": total_queues,
                    "total_jobs": total_jobs,
                    "total_workers": total_workers,
                }
                yield f"event: overview\ndata: {json.dumps(overview)}\n\n"  # noqa

                # --- JOB DISTRIBUTION: Sum by State ---
                # dict.fromkeys initializes all values to 0
                dist: dict[str, int] = dict.fromkeys(("waiting", "active", "delayed", "completed", "failed"), 0)

                for counts in queue_counts.values():
                    for state in dist:
                        dist[state] += counts.get(state, 0)
                yield f"event: jobdist\ndata: {json.dumps(dist)}\n\n"

                # --- METRICS: Throughput/Failure Summary ---
                metrics: dict[str, int | None] = {
                    "throughput": dist["completed"],
                    "avg_duration": None,
                    "retries": dist["failed"],
                    "failures": dist["failed"],
                }
                yield f"event: metrics\ndata: {json.dumps(metrics)}\n\n"
                record_metrics_snapshot(
                    metrics=metrics,
                    counts=dist,
                    total_queues=total_queues,
                    total_workers=total_workers,
                )

                # --- QUEUE STATS: Per-Queue Detail ---
                qrows: list[dict[str, Any]] = []
                for q in queues:
                    paused: bool = False
                    if hasattr(backend, "is_queue_paused"):
                        try:
                            paused = await backend.is_queue_paused(q)
                        except Exception:
                            paused = False
                    counts = queue_counts[q]
                    qrows.append({"name": q, "paused": paused, **counts})
                yield f"event: queues\ndata: {json.dumps(qrows)}\n\n"

                # --- WORKERS ---
                try:
                    wk: list[BackendWorkerRecord] = await backend.list_workers()
                except Exception:
                    wk = []
                wk_rows: list[dict[str, Any]] = []
                for worker in wk:
                    if isinstance(worker, dict):
                        worker_dict = cast(dict[str, Any], worker)
                        wk_rows.append(
                            {
                                "id": worker_dict.get("id"),
                                "queue": worker_dict.get("queue"),
                                "concurrency": worker_dict.get("concurrency"),
                                "heartbeat": worker_dict.get("heartbeat"),
                            }
                        )
                    else:
                        wk_rows.append(
                            {
                                "id": getattr(worker, "id", None),
                                "queue": getattr(worker, "queue", None),
                                "concurrency": getattr(worker, "concurrency", None),
                                "heartbeat": getattr(worker, "heartbeat", None),
                            }
                        )
                yield f"event: workers\ndata: {json.dumps(wk_rows)}\n\n"

                # --- LATEST 10 JOBS (Time-Intensive Aggregation) ---
                all_jobs_raw: list[dict[str, Any]] = []
                for q in queues:
                    for s in JOB_STATES:
                        try:
                            jobs = await backend.list_jobs(q, s)
                        except Exception:
                            jobs = []
                        for job in jobs:
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
                        "ts": ts,
                        "time": datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    for q, ts in last_act.items()
                ]
                qacts.sort(key=lambda x: x["ts"], reverse=True)

                latest_queues = [{"name": rec["name"], "time": rec["time"]} for rec in qacts[:5]]
                yield f"event: latest_queues\ndata: {json.dumps(latest_queues)}\n\n"

                await anyio.sleep(5)

        return StreamingResponse(event_generator(), media_type="text/event-stream")

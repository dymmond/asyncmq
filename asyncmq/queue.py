import time
from typing import Any, Dict, List, Optional

import anyio

from asyncmq.backends.base import BaseBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.job import Job
from asyncmq.runner import run_worker


class Queue:
    """
    High-level Queue API mirroring BullMQ:
      - .add(): enqueue a job (with optional delay)
      - .add_bulk(): enqueue multiple jobs
      - .add_repeatable(): schedule a repeatable job
      - .pause() / .resume()
      - .clean(): purge jobs by state
      - .run(): start worker(s)
      - .start(): synchronous entry point
    """
    def __init__(
        self,
        name: str,
        backend: Optional[BaseBackend] = None,
        concurrency: int = 3,
        rate_limit: Optional[int] = None,
        rate_interval: float = 1.0,
    ):
        self.name = name
        self.backend = backend or RedisBackend()
        self._repeatables: List[Dict[str, Any]] = []
        self.concurrency = concurrency
        self.rate_limit = rate_limit
        self.rate_interval = rate_interval

    async def add(
        self,
        task_id: str,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        retries: int = 0,
        ttl: Optional[int] = None,
        backoff: Optional[float] = None,
        priority: int = 5,
        delay: Optional[float] = None,
    ) -> str:
        """Enqueue a single job and return its ID. If `delay` is set, the job is scheduled."""
        job = Job(
            task_id=task_id,
            args=args or [],
            kwargs=kwargs or {},
            retries=0,
            max_retries=retries,
            backoff=backoff,
            ttl=ttl,
            priority=priority,
        )
        if delay is not None:
            job.delay_until = time.time() + delay
            await self.backend.enqueue_delayed(self.name, job.to_dict(), job.delay_until)
        else:
            await self.backend.enqueue(self.name, job.to_dict())
        return job.id

    async def add_bulk(self, jobs: List[Dict[str, Any]]) -> List[str]:
        """Enqueue multiple jobs at once. Each dict must include job constructor params."""
        created_ids: List[str] = []
        payloads: List[Dict[str, Any]] = []
        for cfg in jobs:
            job = Job(
                task_id=cfg.get("task_id"),
                args=cfg.get("args", []),
                kwargs=cfg.get("kwargs", {}),
                retries=0,
                max_retries=cfg.get("retries", 0),
                backoff=cfg.get("backoff"),
                ttl=cfg.get("ttl"),
                priority=cfg.get("priority", 5),
            )
            created_ids.append(job.id)
            payloads.append(job.to_dict())
        await self.backend.bulk_enqueue(self.name, payloads)
        return created_ids

    def add_repeatable(
        self,
        task_id: str,
        every: float,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        retries: int = 0,
        ttl: Optional[int] = None,
        priority: int = 5,
    ):
        """Schedule a repeatable job definition."""
        self._repeatables.append({
            "task_id": task_id,
            "args": args or [],
            "kwargs": kwargs or {},
            "repeat_every": every,
            "max_retries": retries,
            "ttl": ttl,
            "priority": priority,
        })

    async def pause(self) -> None:
        """Pause processing of this queue."""
        await self.backend.pause_queue(self.name)

    async def resume(self) -> None:
        """Resume a paused queue."""
        await self.backend.resume_queue(self.name)

    async def clean(
        self,
        state: str,
        older_than: Optional[float] = None,
    ) -> None:
        """Purge jobs in a given state older than a timestamp."""
        await self.backend.purge(self.name, state, older_than)

    async def run(self) -> None:
        """Start worker to process this queue with configured settings."""
        await run_worker(
            self.name,
            self.backend,
            concurrency=self.concurrency,
            rate_limit=self.rate_limit,
            rate_interval=self.rate_interval,
            repeatables=self._repeatables,
        )

    def start(self) -> None:
        """
        Synchronous entry point: runs the async `run()` under AnyIO’s runner.
        """
        anyio.run(self.run)

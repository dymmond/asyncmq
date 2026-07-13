"""Shared task definitions used by competitive benchmark worker processes."""

from __future__ import annotations

import argparse
import asyncio
import os
from typing import Any
from urllib.parse import urlparse

_REDIS_CLIENT: Any | None = None
_REDIS_CLIENT_URL: str | None = None


def _redis_url() -> str:
    return os.environ.get("ASYNCMQ_BENCH_REDIS_URL", "redis://localhost:6379/15")


def _queue_name() -> str:
    return os.environ.get("ASYNCMQ_BENCH_QUEUE", "asyncmq-benchmark")


def _completion_key(run_id: str) -> str:
    return f"asyncmq:benchmark:{run_id}:completed"


def _bytes_key(run_id: str) -> str:
    return f"asyncmq:benchmark:{run_id}:bytes"


def _redis_client():
    global _REDIS_CLIENT, _REDIS_CLIENT_URL

    url = _redis_url()
    if _REDIS_CLIENT is None or _REDIS_CLIENT_URL != url:
        if _REDIS_CLIENT is not None:
            _REDIS_CLIENT.close()
        from redis import Redis

        _REDIS_CLIENT = Redis.from_url(url)
        _REDIS_CLIENT_URL = url
    return _REDIS_CLIENT


def _mark_completed(payload: str, run_id: str) -> int:
    pipe = _redis_client().pipeline()
    pipe.incr(_completion_key(run_id))
    pipe.incrby(_bytes_key(run_id), len(payload))
    pipe.execute()
    return len(payload)


try:
    from celery import Celery

    celery_app = Celery("asyncmq_benchmark", broker=_redis_url(), backend=_redis_url())
    celery_app.conf.update(
        accept_content=["json"],
        broker_connection_retry_on_startup=True,
        result_expires=3600,
        result_serializer="json",
        task_acks_late=False,
        task_default_queue=_queue_name(),
        task_serializer="json",
        worker_prefetch_multiplier=1,
    )

    @celery_app.task(name="benchmarks.competitor_apps.celery_payload_task")
    def celery_payload_task(payload: str, run_id: str) -> int:
        return _mark_completed(payload, run_id)

except ImportError:  # pragma: no cover - optional competitor dependency.
    celery_app = None  # type: ignore[assignment]


try:
    import dramatiq
    from dramatiq.brokers.redis import RedisBroker

    dramatiq_broker = RedisBroker(url=_redis_url(), namespace="asyncmq-benchmark-dramatiq")
    dramatiq.set_broker(dramatiq_broker)

    @dramatiq.actor(queue_name=_queue_name(), max_retries=0)
    def dramatiq_payload_task(payload: str, run_id: str) -> int:
        return _mark_completed(payload, run_id)

except ImportError:  # pragma: no cover - optional competitor dependency.
    dramatiq_broker = None  # type: ignore[assignment]


def rq_payload_task(payload: str, run_id: str) -> int:
    return _mark_completed(payload, run_id)


try:
    from huey import RedisHuey

    huey = RedisHuey(
        "asyncmq-benchmark",
        immediate=False,
        results=False,
        url=_redis_url(),
    )

    @huey.task()
    def huey_payload_task(payload: str, run_id: str) -> int:
        return _mark_completed(payload, run_id)

except ImportError:  # pragma: no cover - optional competitor dependency.
    huey = None  # type: ignore[assignment]


try:
    from arq.connections import RedisSettings

    def _arq_redis_settings() -> RedisSettings:
        parsed = urlparse(_redis_url())
        database = int(parsed.path.lstrip("/") or "0")
        return RedisSettings(
            host=parsed.hostname or "localhost",
            port=parsed.port or 6379,
            database=database,
            username=parsed.username,
            password=parsed.password,
        )

    async def arq_payload_task(ctx, payload: str, run_id: str) -> int:  # noqa: ANN001
        return _mark_completed(payload, run_id)

    class ArqWorkerSettings:
        functions = [arq_payload_task]
        redis_settings = _arq_redis_settings()
        queue_name = _queue_name()
        max_jobs = int(os.environ.get("ASYNCMQ_BENCH_CONCURRENCY", "1"))
        keep_result = 0
        poll_delay = 0.01
        retry_jobs = False

except ImportError:  # pragma: no cover - optional competitor dependency.
    ArqWorkerSettings = None  # type: ignore[assignment]


def _set_runtime_env(redis_url: str, queue: str) -> None:
    os.environ["ASYNCMQ_BENCH_QUEUE"] = queue
    os.environ["ASYNCMQ_BENCH_REDIS_URL"] = redis_url


async def _produce_arq(*, jobs: int, payload: str, run_id: str, queue: str, redis_url: str) -> None:
    from arq.connections import create_pool

    _set_runtime_env(redis_url, queue)
    redis = await create_pool(_arq_redis_settings(), default_queue_name=queue)
    try:
        for index in range(jobs):
            await redis.enqueue_job("arq_payload_task", payload, run_id, _job_id=f"{run_id}-{index}", _queue_name=queue)
    finally:
        await redis.aclose()


def produce_jobs(*, target: str, jobs: int, payload_bytes: int, run_id: str, queue: str, redis_url: str) -> None:
    _set_runtime_env(redis_url, queue)
    payload = "x" * payload_bytes
    if target == "celery":
        for index in range(jobs):
            celery_payload_task.apply_async(args=[payload, run_id], queue=queue, task_id=f"{run_id}-{index}")
    elif target == "dramatiq":
        for _ in range(jobs):
            dramatiq_payload_task.send(payload, run_id)
    elif target == "arq":
        asyncio.run(_produce_arq(jobs=jobs, payload=payload, run_id=run_id, queue=queue, redis_url=redis_url))
    elif target == "rq":
        from redis import Redis
        from rq import Queue

        queue_obj = Queue(queue, connection=Redis.from_url(redis_url))
        for index in range(jobs):
            queue_obj.enqueue(
                "benchmarks.competitor_apps.rq_payload_task",
                payload,
                run_id,
                failure_ttl=0,
                job_id=f"{run_id}-{index}",
                result_ttl=0,
            )
    elif target == "huey":
        for _ in range(jobs):
            huey_payload_task(payload, run_id)
    else:
        raise ValueError(f"unsupported target: {target}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Produce competitor benchmark jobs.")
    parser.add_argument("--target", required=True)
    parser.add_argument("--jobs", type=int, required=True)
    parser.add_argument("--payload-bytes", type=int, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--queue", required=True)
    parser.add_argument("--redis-url", required=True)
    args = parser.parse_args()

    produce_jobs(
        target=args.target,
        jobs=args.jobs,
        payload_bytes=args.payload_bytes,
        run_id=args.run_id,
        queue=args.queue,
        redis_url=args.redis_url,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

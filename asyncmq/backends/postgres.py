try:
    import asyncpg
except ImportError:
    raise ImportError("Please install asyncpg: `pip install asyncpg`") from None

import json
import time
from typing import Any, Dict, List, Optional

from asyncmq.backends.base import BaseBackend
from asyncmq.conf import settings
from asyncmq.core.enums import State
from asyncmq.logging import logger
from asyncmq.stores.postgres import PostgresJobStore


class PostgresBackend(BaseBackend):
    def __init__(self, dsn: str | None = None) -> None:
        if not dsn and not settings.asyncmq_postgres_backend_url:
            raise ValueError(
                "Either 'dsn' or 'settings.asyncmq_postgres_backend_url' must be "
                "provided."
            )
        self.dsn = dsn or settings.asyncmq_postgres_backend_url
        self.pool: Optional[asyncpg.Pool] = None
        self.store = PostgresJobStore(dsn=dsn)

    async def connect(self) -> None:
        if self.pool is None:
            logger.info(f"Connecting to Postgres...: {self.dsn}")
            self.pool = await asyncpg.create_pool(dsn=self.dsn)
            await self.store.connect()

    async def enqueue(self, queue_name: str, payload: Dict[str, Any]) -> None:
        await self.connect()
        payload["status"] = State.WAITING
        await self.store.save(queue_name, payload["id"], payload)

    async def bulk_enqueue(self, queue_name: str, jobs: List[Dict[str, Any]]) -> None:
        await self.connect()
        for payload in jobs:
            await self.enqueue(queue_name, payload)

    async def dequeue(self, queue_name: str) -> Optional[Dict[str, Any]]:
        await self.connect()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    f"""
                    UPDATE {settings.jobs_table_name}
                    SET status = $3, updated_at = now()
                    WHERE id = (
                        SELECT id FROM {settings.jobs_table_name}
                        WHERE queue_name = $1 AND status = $2
                        ORDER BY created_at ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING data
                    """,
                    queue_name, State.WAITING, State.ACTIVE
                )
                if row:
                    return json.loads(row["data"])
                return None

    async def ack(self, queue_name: str, job_id: str) -> None:
        await self.connect()
        await self.store.delete(queue_name, job_id)

    async def move_to_dlq(self, queue_name: str, payload: Dict[str, Any]) -> None:
        await self.connect()
        payload["status"] = State.FAILED
        await self.store.save(queue_name, payload["id"], payload)

    async def enqueue_delayed(self, queue_name: str, payload: Dict[str, Any], run_at: float) -> None:
        await self.connect()
        payload["status"] = State.DELAYED
        payload["delay_until"] = run_at
        await self.store.save(queue_name, payload["id"], payload)

    async def get_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        now = time.time()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT data
                FROM {settings.jobs_table_name}
                WHERE queue_name = $1
                  AND (data ->>'delay_until') IS NOT NULL
                  AND (data ->>'delay_until')::float <= $2
                """,
                queue_name,
                now,
            )
            return [json.loads(row["data"]) for row in rows]

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        await self.connect()
        await self.store.delete(queue_name, job_id)

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        await self.connect()
        job = await self.store.load(queue_name, job_id)
        if job:
            job["status"] = state
            await self.store.save(queue_name, job_id, job)

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        await self.connect()
        job = await self.store.load(queue_name, job_id)
        if job:
            job["result"] = result
            await self.store.save(queue_name, job_id, job)

    async def get_job_state(self, queue_name: str, job_id: str) -> Optional[str]:
        await self.connect()
        job = await self.store.load(queue_name, job_id)
        if job:
            return job.get("status")
        return None

    async def get_job_result(self, queue_name: str, job_id: str) -> Optional[Any]:
        await self.connect()
        job = await self.store.load(queue_name, job_id)
        if job:
            return job.get("result")
        return None

    async def add_dependencies(self, queue_name: str, job_dict: Dict[str, Any]) -> None:
        await self.connect()
        job_id = job_dict["id"]
        dependencies = job_dict.get("depends_on", [])
        job = await self.store.load(queue_name, job_id)
        if job:
            job["depends_on"] = dependencies
            await self.store.save(queue_name, job_id, job)

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        await self.connect()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT id, data FROM {settings.jobs_table_name} WHERE queue_name = $1 AND data->'depends_on' IS NOT NULL
                """,
                queue_name
            )
            for row in rows:
                data = json.loads(row["data"])
                depends_on = data.get("depends_on", [])
                if parent_id in depends_on:
                    depends_on.remove(parent_id)
                    if not depends_on:
                        data.pop("depends_on")
                        data["status"] = State.WAITING
                    else:
                        data["depends_on"] = depends_on
                    await self.store.save(queue_name, data["id"], data)

    # Atomic flow addition
    async def atomic_add_flow(
            self,
            queue_name: str,
            job_dicts: list[dict[str, Any]],
            dependency_links: list[tuple[str, str]],
    ) -> List[str]:
        """
        Atomically enqueue multiple jobs and their dependencies within a transaction.
        """
        await self.connect()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Enqueue all jobs
                for payload in job_dicts:
                    payload["status"] = State.WAITING
                    await self.store.save(queue_name, payload["id"], payload)
                # Register dependencies
                for parent, child in dependency_links:
                    job = await self.store.load(queue_name, child)
                    if job is not None:
                        deps = job.get("depends_on", [])
                        if parent not in deps:
                            deps.append(parent)
                            job["depends_on"] = deps
                            await self.store.save(queue_name, child, job)
        # Return the ordered list of IDs
        return [payload["id"] for payload in job_dicts]

    async def pause_queue(self, queue_name: str) -> None:
        pass

    async def resume_queue(self, queue_name: str) -> None:
        pass

    async def is_queue_paused(self, queue_name: str) -> bool:
        return False

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        await self.connect()
        job = await self.store.load(queue_name, job_id)
        if job:
            job["progress"] = progress
            await self.store.save(queue_name, job_id, job)

    async def purge(self, queue_name: str, state: str, older_than: Optional[float] = None) -> None:
        await self.connect()
        async with self.pool.acquire() as conn:
            if older_than:
                threshold_time = time.time() - older_than
                await conn.execute(
                    f"""
                    DELETE FROM {settings.jobs_table_name}
                    WHERE queue_name = $1 AND status = $2 AND EXTRACT(EPOCH FROM created_at) < $3
                    """,
                    queue_name, state, threshold_time
                )
            else:
                await conn.execute(
                    f"""
                    DELETE FROM {settings.jobs_table_name}
                    WHERE queue_name = $1 AND status = $2
                    """,
                    queue_name, state
                )

    async def emit_event(self, event: str, data: Dict[str, Any]) -> None:
        pass  # Could use LISTEN/NOTIFY in the future

    async def create_lock(self, key: str, ttl: int) -> Any:
        await self.connect()
        return PostgresLock(self.pool, key, ttl)

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None
        await self.store.disconnect()

    async def list_queues(self) -> list[str]:
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch("SELECT DISTINCT queue_name FROM jobs")
            return [row["queue_name"] for row in rows]

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        async with self.store.pool.acquire() as conn:
            waiting = await conn.fetchval(
                "SELECT COUNT(*) FROM jobs WHERE queue_name=$1 AND status='waiting'",
                queue_name,
            )
            delayed = await conn.fetchval(
                "SELECT COUNT(*) FROM jobs WHERE queue_name=$1 AND status='delayed'",
                queue_name,
            )
            failed = await conn.fetchval(
                "SELECT COUNT(*) FROM jobs WHERE queue_name=$1 AND status='failed'",
                queue_name,
            )
            return {
                "waiting": waiting or 0,
                "delayed": delayed or 0,
                "failed": failed or 0,
            }

    async def list_jobs(self, queue_name: str) -> list[dict[str, Any]]:
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT data FROM {settings.jobs_table_name} WHERE queue_name=$1",
                queue_name,
            )
            return [json.loads(row["data"]) for row in rows]

    async def retry_job(self, queue_name: str, job_id: str) -> bool:
        async with self.store.pool.acquire() as conn:
            updated = await conn.execute(
                f"UPDATE {settings.jobs_table_name} SET status='waiting' WHERE id=$1 AND queue_name=$2 AND status='failed'",
                job_id,
                queue_name,
            )
            return updated.endswith("UPDATE 1")

    async def remove_job(self, queue_name: str, job_id: str) -> bool:
        async with self.store.pool.acquire() as conn:
            deleted = await conn.execute(
                f"DELETE FROM {settings.jobs_table_name} WHERE id=$1 AND queue_name=$2",
                job_id,
                queue_name,
            )
            return deleted.endswith("DELETE 1")

class PostgresLock:
    def __init__(self, pool: asyncpg.Pool, key: str, ttl: int) -> None:
        self.pool = pool
        self.key = key
        self.ttl = ttl

    async def acquire(self) -> bool:
        async with self.pool.acquire() as conn:
            result = await conn.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", self.key)
            return result

    async def release(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", self.key)

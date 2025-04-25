import asyncpg
from typing import Optional, List
from store.base import BaseJobStore
import json

class PostgresJobStore(BaseJobStore):
    def __init__(self, dsn: str = "postgresql://user:pass@localhost:5432/queue"):
        self.dsn = dsn
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn)
        await self._init_table()

    async def _init_table(self):
        async with self.pool.acquire() as conn:
            await conn.execute(\"\"\"
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                queue TEXT,
                data JSONB
            )
            \"\"\")

    async def save(self, queue_name: str, job_id: str, data: dict):
        async with self.pool.acquire() as conn:
            await conn.execute(
                \"\"\"INSERT INTO jobs (id, queue, data)
                VALUES ($1, $2, $3)
                ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data\"\"\",
                job_id, queue_name, json.dumps(data)
            )

    async def load(self, queue_name: str, job_id: str) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data FROM jobs WHERE id = $1", job_id)
            return dict(row["data"]) if row else None

    async def delete(self, queue_name: str, job_id: str):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM jobs WHERE id = $1", job_id)

    async def all_jobs(self, queue_name: str) -> List[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT data FROM jobs WHERE queue = $1", queue_name)
            return [dict(row["data"]) for row in rows]

    async def jobs_by_status(self, queue_name: str, status: str) -> List[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT data FROM jobs WHERE queue = $1 AND data->>'status' = $2",
                queue_name, status
            )
            return [dict(row["data"]) for row in rows]

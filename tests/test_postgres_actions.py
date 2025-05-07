# tests/test_postgres_actions.py

import pytest

from asyncmq.backends.postgres import PostgresBackend
from asyncmq.conf import settings

pytestmark = pytest.mark.anyio


async def test_cancel_job_postgres():
    backend = PostgresBackend()
    await backend.connect()

    # Ensure cancelled_jobs table exists
    cancel_table = settings.postgres_cancelled_jobs_table_name
    async with backend.pool.acquire() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {cancel_table} (
                queue_name TEXT,
                job_id TEXT PRIMARY KEY
            );
        """)

    # Act
    queue, job_id = "q1", "p1"
    result = await backend.cancel_job(queue, job_id)

    assert isinstance(result, bool) and result is True

    # Verify insertion
    async with backend.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT job_id FROM {cancel_table}
            WHERE queue_name=$1 AND job_id=$2
        """,
            queue,
            job_id,
        )
    assert row["job_id"] == job_id


async def test_retry_job_postgres():
    backend = PostgresBackend()
    await backend.connect()

    # Ensure jobs table exists
    jobs_table = settings.postgres_jobs_table_name
    async with backend.pool.acquire() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {jobs_table} (
                id TEXT PRIMARY KEY,
                queue_name TEXT,
                status TEXT
            );
        """)
        # Insert a failed job
        queue, job_id = "q1", "p2"
        await conn.execute(
            f"""
            INSERT INTO {jobs_table}(id, queue_name, status)
            VALUES($1, $2, 'failed')
            ON CONFLICT DO NOTHING
        """,
            job_id,
            queue,
        )

    # Act
    result = await backend.retry_job(queue, job_id)
    assert isinstance(result, bool) and result is True

    # Verify status updated
    async with backend.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT status FROM {jobs_table}
            WHERE id=$1 AND queue_name=$2
        """,
            job_id,
            queue,
        )
    assert row["status"] == "waiting"


async def test_remove_job_postgres():
    backend = PostgresBackend()
    await backend.connect()

    # Ensure jobs table exists
    jobs_table = settings.postgres_jobs_table_name
    async with backend.pool.acquire() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {jobs_table} (
                id TEXT PRIMARY KEY,
                queue_name TEXT,
                status TEXT
            );
        """)
        # Insert a ready job
        queue, job_id = "q1", "p3"
        await conn.execute(
            f"""
            INSERT INTO {jobs_table}(id, queue_name, status)
            VALUES($1, $2, 'waiting')
            ON CONFLICT DO NOTHING
        """,
            job_id,
            queue,
        )

    # Act
    result = await backend.remove_job(queue, job_id)
    assert isinstance(result, bool) and result is True

    # Verify deletion
    async with backend.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT id FROM {jobs_table}
            WHERE id=$1 AND queue_name=$2
        """,
            job_id,
            queue,
        )
    assert row is None

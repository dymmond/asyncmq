import json

import pytest

from asyncmq.backends.postgres import PostgresBackend
from asyncmq.conf import settings
from asyncmq.core.utils.postgres import install_or_drop_postgres_backend

pytestmark = pytest.mark.anyio


@pytest.fixture(scope="module")
async def backend():
    # (Re)create fresh schema including delay_until
    await install_or_drop_postgres_backend()
    backend = PostgresBackend()
    await backend.connect()
    yield backend
    # Clean up
    await backend.close()
    await install_or_drop_postgres_backend(drop=True)


async def test_cancel_job_postgres(backend):
    queue, job_id = "q1", "p1"
    delayed_id = "p1-delayed"
    await backend.enqueue(queue, {"id": job_id, "task": "waiting"})
    await backend.enqueue_delayed(queue, {"id": delayed_id, "task": "delayed"}, 9999999999)

    # Act
    result = await backend.cancel_job(queue, job_id)
    assert result is True
    delayed_result = await backend.cancel_job(queue, delayed_id)
    assert delayed_result is True

    # Verify row in cancelled_jobs
    cancel_table = settings.postgres_cancelled_jobs_table_name
    async with backend.pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT job_id
              FROM {cancel_table}
             WHERE queue_name = $1 AND job_id = ANY($2::text[])
            """,
            queue,
            [job_id, delayed_id],
        )
    assert {row["job_id"] for row in rows} == {job_id, delayed_id}
    assert await backend.get_job(queue, job_id) is None
    assert await backend.get_job(queue, delayed_id) is None


async def test_remove_job_clears_postgres_cancellation_marker(backend):
    queue, job_id = "q1", "p1-remove-cancelled"
    await backend.enqueue(queue, {"id": job_id, "task": "waiting"})
    assert await backend.cancel_job(queue, job_id) is True
    assert await backend.is_job_cancelled(queue, job_id) is True

    assert await backend.remove_job(queue, job_id) is True

    assert await backend.is_job_cancelled(queue, job_id) is False


async def test_retry_job_postgres(backend):
    jobs_table = settings.postgres_jobs_table_name
    queue, job_id = "q1", "p2"

    # Insert a failed job into the real jobs table
    async with backend.pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {jobs_table}
               (queue_name, job_id, data, status)
            VALUES ($1, $2, $3::jsonb, $4)
            ON CONFLICT (queue_name, job_id) DO UPDATE
              SET status = EXCLUDED.status
            """,
            queue,
            job_id,
            json.dumps({"id": job_id, "status": "failed", "result": "old", "last_error": "old failure"}),
            "failed",
        )

    # Act
    result = await backend.retry_job(queue, job_id)
    assert result is True

    # Verify status was set back to 'waiting'
    async with backend.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT status, data
              FROM {jobs_table}
             WHERE job_id = $1 AND queue_name = $2
            """,
            job_id,
            queue,
        )
    assert row["status"] == "waiting"
    data = row["data"] if isinstance(row["data"], dict) else json.loads(row["data"])
    assert data["status"] == "waiting"
    assert "result" not in data
    assert "last_error" not in data


async def test_remove_job_postgres(backend):
    jobs_table = settings.postgres_jobs_table_name
    queue, job_id = "q1", "p3"

    # Insert a waiting job into the real jobs table
    async with backend.pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {jobs_table}
               (queue_name, job_id, data, status)
            VALUES ($1, $2, $3::jsonb, $4)
            ON CONFLICT (queue_name, job_id) DO NOTHING
            """,
            queue,
            job_id,
            "{}",
            "waiting",
        )

    # Act
    result = await backend.remove_job(queue, job_id)
    assert result is True

    # Verify deletion
    async with backend.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT job_id
              FROM {jobs_table}
             WHERE job_id = $1 AND queue_name = $2
            """,
            job_id,
            queue,
        )
    assert row is None


async def test_postgres_job_ids_are_scoped_per_queue(backend):
    await backend.store.save("q1", "shared-job", {"id": "shared-job", "status": "waiting", "task": "a"})
    await backend.store.save("q2", "shared-job", {"id": "shared-job", "status": "waiting", "task": "b"})

    async with backend.pool.acquire() as conn:
        count = await conn.fetchval(
            f"""
            SELECT COUNT(*)
              FROM {settings.postgres_jobs_table_name}
             WHERE job_id = $1
            """,
            "shared-job",
        )

    assert count == 2
    assert (await backend.store.load("q1", "shared-job"))["task"] == "a"
    assert (await backend.store.load("q2", "shared-job"))["task"] == "b"

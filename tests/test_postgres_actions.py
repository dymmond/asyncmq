import pytest

from asyncmq.backends.postgres import PostgresBackend
from asyncmq.conf import monkay
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
    # Act
    result = await backend.cancel_job(queue, job_id)
    assert result is True

    # Verify row in cancelled_jobs
    cancel_table = monkay.settings.postgres_cancelled_jobs_table_name
    async with backend.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT job_id
              FROM {cancel_table}
             WHERE queue_name = $1 AND job_id = $2
            """,
            queue,
            job_id,
        )
    assert row["job_id"] == job_id


async def test_retry_job_postgres(backend):
    jobs_table = monkay.settings.postgres_jobs_table_name
    queue, job_id = "q1", "p2"

    # Insert a failed job into the real jobs table
    async with backend.pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {jobs_table}
               (queue_name, job_id, data, status)
            VALUES ($1, $2, $3::jsonb, $4)
            ON CONFLICT (job_id) DO UPDATE
              SET status = EXCLUDED.status
            """,
            queue,
            job_id,
            "{}",
            "failed",
        )

    # Act
    result = await backend.retry_job(queue, job_id)
    assert result is True

    # Verify status was set back to 'waiting'
    async with backend.pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT status
              FROM {jobs_table}
             WHERE job_id = $1 AND queue_name = $2
            """,
            job_id,
            queue,
        )
    assert row["status"] == "waiting"


async def test_remove_job_postgres(backend):
    jobs_table = monkay.settings.postgres_jobs_table_name
    queue, job_id = "q1", "p3"

    # Insert a waiting job into the real jobs table
    async with backend.pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {jobs_table}
               (queue_name, job_id, data, status)
            VALUES ($1, $2, $3::jsonb, $4)
            ON CONFLICT (job_id) DO NOTHING
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

import asyncio

import typer

from asyncmq.backends.redis import RedisBackend

app = typer.Typer()

@app.command()
def list_jobs(queue: str, status: str = typer.Option(None)):
    """List all jobs in a queue (optionally filter by status)."""
    async def inner():
        backend = RedisBackend()
        jobs = await backend.job_store.jobs_by_status(queue, status) if status else await backend.job_store.all_jobs(queue)
        for job in jobs:
            typer.echo(f"[{job['id']}] {job['task']} - {job['status']}")
    asyncio.run(inner())

@app.command()
def get_job(job_id: str, queue: str):
    """Show detailed info for a job."""
    async def inner():
        backend = RedisBackend()
        job = await backend.job_store.load(queue, job_id)
        if job:
            typer.echo(job)
        else:
            typer.echo("Job not found.")
    asyncio.run(inner())

@app.command()
def retry_job(job_id: str, queue: str):
    """Retry a failed job (re-enqueue)."""
    async def inner():
        backend = RedisBackend()
        job = await backend.job_store.load(queue, job_id)
        if job and job.get("status") == "failed":
            job["status"] = "waiting"
            job["retries"] = 0
            await backend.enqueue(queue, job)
            typer.echo(f"Requeued job {job_id}.")
        else:
            typer.echo("Job not found or not failed.")
    asyncio.run(inner())

@app.command()
def purge_dlq(queue: str):
    """Clear the DLQ for a queue."""
    async def inner():
        key = f"queue:{queue}:dlq"
        backend = RedisBackend()
        count = await backend.redis.llen(key)
        await backend.redis.delete(key)
        typer.echo(f"Purged {count} items from DLQ.")
    asyncio.run(inner())


def main():
    app()

if __name__ == "__main__":  # pragma: no cover
    app()

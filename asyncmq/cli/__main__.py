import anyio

from asyncmq.cli.info import info_app
from asyncmq.cli.job import job_app
from asyncmq.cli.queue import queue_app
from asyncmq.cli.sync import AsyncTyper
from asyncmq.cli.worker import worker_app

app = AsyncTyper(name="AsyncMQ CLI", help="AsyncMQ Command Line Interface")

# Subcommands
app.add_typer(queue_app, name="queue", help="Manage queues")
app.add_typer(job_app, name="job", help="Manage jobs")
app.add_typer(worker_app, name="worker", help="Manage workers")
app.add_typer(info_app, name="info", help="Information")


async def async_main():
    await app()

def main():
    anyio.run(async_main)


if __name__ == "__main__":
    main()

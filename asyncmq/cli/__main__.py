import click

from asyncmq.cli.info import info_app
from asyncmq.cli.job import job_app
from asyncmq.cli.queue import queue_app
from asyncmq.cli.worker import worker_app


@click.group()
def app():
    """AsyncMQ command-line interface."""
    pass

# Register subcommands
app.add_command(queue_app, name="queue")
app.add_command(job_app, name="job")
app.add_command(worker_app, name="worker")
app.add_command(info_app, name="info")

def main():
    app()

if __name__ == "__main__":
    main()

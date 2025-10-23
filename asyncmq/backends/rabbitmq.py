import time
from typing import Any, cast

try:
    import aio_pika
    from aio_pika import DeliveryMode, Message
except ImportError:
    raise ImportError("Please install aio_pika to use this backend.") from None

from asyncmq.backends.base import BaseBackend, DelayedInfo, RepeatableInfo, WorkerInfo
from asyncmq.core.event import event_emitter
from asyncmq.stores.base import BaseJobStore
from asyncmq.stores.rabbitmq import RabbitMQJobStore


class RabbitMQBackend(BaseBackend):
    """
    RabbitMQ-backed implementation of the asynchronous message queue backend.

    This class provides methods for interacting with RabbitMQ as a message broker
    and a BaseJobStore for managing job states and metadata. It handles connection
    management, message publishing, consumption, and various job lifecycle operations.
    """

    def __init__(
        self,
        rabbit_url: str,
        job_store: BaseJobStore | None = None,
        redis_url: str | None = None,
        prefetch_count: int = 1,
    ):
        """
        Initializes the RabbitMQBackend instance.

        Args:
            rabbit_url: The URL for connecting to the RabbitMQ server.
            job_store: An optional BaseJobStore instance to use for job state
                management. If not provided, a RabbitMQJobStore will be
                initialized using the `redis_url`.
            redis_url: An optional URL for connecting to Redis, used if a
                `job_store` is not explicitly provided (for RabbitMQJobStore).
            prefetch_count: The maximum number of messages that the channel will
                proactively dispatch to consumers. This helps with flow control.
        """

        self.rabbit_url = rabbit_url
        self.prefetch_count = prefetch_count
        # Initialize the job store, using RabbitMQJobStore if none is provided.
        self._state: BaseJobStore = job_store or RabbitMQJobStore(redis_url=redis_url)
        self._conn: aio_pika.RobustConnection | None = None
        self._chan: aio_pika.abc.AbstractChannel | None = None
        # Dictionary to store declared queues to avoid re-declaring them.
        self._queues: dict[str, aio_pika.abc.AbstractQueue] = {}

    async def _connect(self) -> None:
        """
        Establishes a robust connection and channel to RabbitMQ.

        This method ensures that a connection and channel are established before
        performing any RabbitMQ operations. It reuses existing connections if they
        are not closed.
        """
        if self._conn and not self._conn.is_closed:
            # If connection exists and is not closed, no need to reconnect.
            return
        # Establish a robust connection which handles reconnections automatically.
        self._conn = await aio_pika.connect_robust(self.rabbit_url)  # type: ignore
        # Create a channel with publisher confirms enabled for reliable message delivery.
        self._chan = await self._conn.channel(publisher_confirms=True)
        # Set the quality of service for the channel, controlling prefetch count.
        await self._chan.set_qos(prefetch_count=self.prefetch_count)

    async def pop_due_delayed(self, queue_name: str) -> list[dict[str, Any]]:
        """
        Atomically fetch and remove all scheduled ("delayed") jobs whose
        run_at ≤ now.  We leverage the existing get_due_delayed(), which
        already deletes each job as it yields it, and then extract the
        raw payload dict from each DelayedInfo.
        """
        infos = await self.get_due_delayed(queue_name)
        return [info.payload for info in infos]

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> str:
        """
        Enqueues a job into the specified RabbitMQ queue.

        The job's state is first saved to the job store as 'waiting', then the
        payload is published to the RabbitMQ queue.

        Args:
            queue_name: The name of the queue to which the job will be enqueued.
            payload: A dictionary containing the job's data, including an 'id' key.

        Returns:
            The unique identifier of the enqueued job.
        """
        job_id = str(payload["id"])

        # 1) persist job metadata as 'waiting'
        await self._state.save(queue_name, job_id, {"id": job_id, "payload": payload, "status": "waiting"})

        # 2) ensure AMQP connection & channel
        await self._connect()

        # 3) declare (and track) the queue itself
        if queue_name not in self._queues:
            queue = await self._chan.declare_queue(name=queue_name, durable=True)
            self._queues[queue_name] = queue

        # 4) build and publish the message
        msg = Message(
            self._json_serializer.to_json(payload).encode(), message_id=job_id, delivery_mode=DeliveryMode.PERSISTENT
        )
        await self._chan.default_exchange.publish(msg, routing_key=queue_name)

        return job_id

    async def dequeue(self, queue_name: str) -> dict[str, Any] | None:
        """
        Dequeues a job from the specified RabbitMQ queue.

        This method attempts to retrieve a message from the queue. If a message
        is found, its payload is returned, and the message is acknowledged
        automatically upon successful processing (async with msg.process()).

        Args:
            queue_name: The name of the queue from which to dequeue a job.

        Returns:
            A dictionary containing 'job_id' and 'payload' if a job is
            successfully dequeued, otherwise None.
        """
        await self._connect()

        # 2) declare (and track) the queue if this is the first time
        if queue_name not in self._queues:
            queue = await self._chan.declare_queue(name=queue_name, durable=True)
            self._queues[queue_name] = queue

        # 3) attempt to get one message, returning None if empty
        msg = await self._queues[queue_name].get(no_ack=False, fail=False)
        if msg is None:
            return None

        # 4) process (ack) and return its payload
        async with msg.process():
            payload = self._json_serializer.to_dict(msg.body.decode())
            return {"job_id": msg.message_id, "payload": payload}

    async def ack(self, queue_name: str, job_id: str) -> None:
        """
        Acknowledges the successful processing of a job.

        This method updates the job's status in the job store to 'completed'.

        Args:
            queue_name: The name of the queue the job belonged to.
            job_id: The unique identifier of the job to acknowledge.
        """
        # Update the job's status in the job store to 'completed'.
        await self._state.save(queue_name, job_id, {"id": job_id, "status": "completed"})

    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Moves a failed job to the Dead Letter Queue (DLQ) for the given queue.

        The job's status is updated to 'failed' in the job store, and its
        payload is published to a dedicated DLQ.

        Args:
            queue_name: The name of the original queue.
            payload: The payload of the job that failed, including its 'id'.
        """
        job_id = str(payload["id"])

        # 1) persist failure in your state store
        if hasattr(self._state, "move_to_dlq"):
            await self._state.move_to_dlq(queue_name, payload)
        else:
            await self._state.save(
                queue_name,
                job_id,
                {
                    "id": job_id,
                    "payload": payload,
                    "status": "failed",
                },
            )

        # 2) ensure we have an open connection & channel
        await self._connect()

        # 3) declare (and track) the DLQ queue itself
        dlq_name = f"{queue_name}.dlq"
        if dlq_name not in self._queues:
            q = await self._chan.declare_queue(name=dlq_name, durable=True)
            self._queues[dlq_name] = q

        # 4) publish via the default exchange (routes to the queue named dlq_name)
        msg = Message(
            self._json_serializer.to_json(payload).encode(), message_id=job_id, delivery_mode=DeliveryMode.PERSISTENT
        )
        await self._chan.default_exchange.publish(msg, routing_key=dlq_name)

    async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        """
        Enqueues a job to be executed at a future time.

        The job's metadata, including its scheduled execution time, is saved
        to the job store with a 'scheduled' status.

        Args:
            queue_name: The name of the queue where the job will eventually be run.
            payload: A dictionary containing the job's data, including an 'id' key.
            run_at: The Unix timestamp (float) at which the job should be executed.
        """
        job_id = str(payload["id"])
        # Save the job metadata to the job store with 'scheduled' status
        # and the specified 'run_at' timestamp.
        await self._state.save(
            queue_name, job_id, {"id": job_id, "payload": payload, "status": "scheduled", "run_at": run_at}
        )

    async def get_due_delayed(self, queue_name: str) -> list[DelayedInfo]:
        """
        Retrieves delayed jobs that are now due for execution.

        Jobs are considered due if their 'run_at' timestamp is less than or
        equal to the current time. Due jobs are then removed from the job store.

        Args:
            queue_name: The name of the queue to check for due delayed jobs.

        Returns:
            A list of DelayedInfo objects representing the jobs that are due.
        """
        now = time.time()  # Get current time.
        due: list[DelayedInfo] = []
        # Iterate through all scheduled jobs for the given queue.
        for j in await self._state.jobs_by_status(queue_name, "scheduled"):
            if j["run_at"] <= now:
                # If the job's run_at time is in the past or present, it's due.
                due.append(DelayedInfo(job_id=j["id"], run_at=j["run_at"], payload=j["payload"]))
                # Delete the job from the job store as it's now being processed.
                await self._state.delete(queue_name, j["id"])
        return due

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        """
        Removes a specific delayed job from the job store.

        Args:
            queue_name: The name of the queue the delayed job belongs to.
            job_id: The unique identifier of the delayed job to remove.
        """
        # Delete the specified job from the job store.
        await self._state.delete(queue_name, job_id)

    async def list_delayed(self, queue_name: str) -> list[DelayedInfo]:
        """
        Lists all currently delayed jobs for a given queue.

        Args:
            queue_name: The name of the queue to list delayed jobs for.

        Returns:
            A list of DelayedInfo objects, each representing a delayed job.
        """
        # Retrieve all jobs with 'scheduled' status for the given queue
        # and convert them into DelayedInfo objects.
        return [
            DelayedInfo(job_id=j["id"], run_at=j["run_at"], payload=j["payload"])
            for j in await self._state.jobs_by_status(queue_name, "scheduled")
        ]

    async def enqueue_repeatable(
        self, queue_name: str, payload: dict[str, Any], interval: float, repeat_id: str | None = None
    ) -> str:
        """
        Enqueues a job that should be repeated at a fixed interval.

        The job's metadata, including its interval and next scheduled run time,
        is saved to the job store with a 'repeatable' status. An optional
        `repeat_id` can be provided to identify the repeatable job.

        Args:
            queue_name: The name of the queue where the job will be repeated.
            payload: A dictionary containing the job's data, including an 'id' key.
            interval: The time interval (in seconds) between repetitions.
            repeat_id: An optional unique identifier for this repeatable job.
                If not provided, the job's 'id' from the payload will be used.

        Returns:
            The unique identifier of the repeatable job.
        """
        rid = repeat_id or str(payload["id"])
        next_run = time.time() + interval  # Calculate the next run time.
        # Save metadata for the repeatable job.
        await self._state.save(
            queue_name,
            rid,
            {
                "id": rid,
                "payload": payload,
                "repeatable": True,
                "interval": interval,
                "next_run": next_run,
                "status": "repeatable",
            },
        )
        return rid

    async def list_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        """
        Lists all repeatable jobs for a given queue.

        Args:
            queue_name: The name of the queue to list repeatable jobs for.

        Returns:
            A list of RepeatableInfo objects, each representing a repeatable job.
        """
        # Retrieve all jobs with 'repeatable' status for the given queue
        # and convert them into RepeatableInfo objects.
        return [
            RepeatableInfo(job_def=j["payload"], next_run=j["next_run"], paused=j.get("paused", False))
            for j in await self._state.jobs_by_status(queue_name, "repeatable")
        ]

    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        """
        Pauses a specific repeatable job.

        This method updates the job's metadata in the job store to mark it as paused.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: A dictionary representing the repeatable job definition,
                including its 'id'.
        """
        # Load the existing entry for the repeatable job.
        entry = await self._state.load(queue_name, job_def["id"])
        # Set the 'paused' flag to True.
        entry["paused"] = True
        # Save the updated entry back to the job store.
        await self._state.save(queue_name, job_def["id"], entry)

    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> Any:
        """
        Resumes a paused repeatable job.

        This method removes the 'paused' flag, recalculates the 'next_run' time,
        and updates the job's metadata in the job store.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            job_def: A dictionary representing the repeatable job definition,
                including its 'id'.

        Returns:
            The newly calculated 'next_run' timestamp for the resumed job.
        """
        # Load the existing entry for the repeatable job.
        entry = await self._state.load(queue_name, job_def["id"])
        # Remove the 'paused' flag if it exists.
        entry.pop("paused", None)
        # Recalculate the next run time based on the job's interval.
        next_run = time.time() + entry["interval"]
        entry["next_run"] = next_run
        # Save the updated entry back to the job store.
        await self._state.save(queue_name, job_def["id"], entry)
        return next_run

    async def remove_repeatable(self, queue_name: str, repeat_id: str) -> None:
        """
        Removes a specific repeatable job from the job store.

        Args:
            queue_name: The name of the queue the repeatable job belongs to.
            repeat_id: The unique identifier of the repeatable job to remove.
        """
        # Delete the specified repeatable job from the job store.
        await self._state.delete(queue_name, repeat_id)

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        """
        Updates the status of a specific job in the job store.

        If the job does not exist, an empty dictionary is used as a base
        before updating the status.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to update.
            state: The new status to set for the job (e.g., 'processing', 'completed').
        """
        # Load the existing job entry or create an empty dictionary if not found.
        entry = await self._state.load(queue_name, job_id) or {}
        entry["status"] = state  # Update the 'status' field.
        # Save the updated entry back to the job store.
        await self._state.save(queue_name, job_id, entry)

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        """
        Saves the result of a job in the job store.

        If the job does not exist, an empty dictionary is used as a base
        before saving the result.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            result: The result data to associate with the job.
        """
        # Load the existing job entry or create an empty dictionary if not found.
        entry = await self._state.load(queue_name, job_id) or {}
        entry["result"] = result  # Store the job result.
        # Save the updated entry back to the job store.
        await self._state.save(queue_name, job_id, entry)

    async def get_job_state(self, queue_name: str, job_id: str) -> Any:
        """
        Retrieves the current status of a specific job.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            The status string of the job (e.g., 'waiting', 'processing',
            'completed', 'failed', 'scheduled'), or None if the job is not found.
        """
        # Load the job entry.
        entry = await self._state.load(queue_name, job_id)
        # Return the 'status' if the entry exists, otherwise None.
        return entry.get("status") if entry else None

    async def get_job_result(self, queue_name: str, job_id: str) -> Any:
        """
        Retrieves the result of a specific job.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            The result data of the job, or None if the job or its result is not found.
        """
        # Load the job entry.
        entry = await self._state.load(queue_name, job_id)
        # Return the 'result' if the entry exists, otherwise None.
        return entry.get("result") if entry else None

    async def add_dependencies(self, queue_name: str, job_dict: dict[str, Any]) -> None:
        """
        Adds dependencies to a job.

        The `job_dict` should contain an 'id' for the job and an optional
        'depends_on' key which is a list of job IDs that this job depends on.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_dict: A dictionary containing the job's 'id' and its
                'depends_on' list (if any).
        """
        # Load the existing job entry or create an empty dictionary if not found.
        entry = await self._state.load(queue_name, job_dict["id"]) or {}
        # Update the 'depends_on' field with the provided dependencies.
        entry["depends_on"] = job_dict.get("depends_on", [])
        # Save the updated entry back to the job store.
        await self._state.save(queue_name, job_dict["id"], entry)

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        """
        Resolves a dependency for all jobs that depend on a given parent job.

        When a parent job (identified by `parent_id`) is completed, this method
        removes it from the 'depends_on' list of all dependent jobs. If a
        dependent job no longer has any unresolved dependencies, it is then
        enqueued.

        Args:
            queue_name: The name of the queue where the jobs reside.
            parent_id: The unique identifier of the parent job that has been
                completed.
        """
        # Iterate through all jobs in the specified queue.
        for j in await self._state.all_jobs(queue_name):
            # Get the list of dependencies for the current job.
            deps = j.get("depends_on", [])
            if parent_id in deps:
                # If the parent_id is in the dependencies, remove it.
                deps.remove(parent_id)
                j["depends_on"] = deps
                # Save the updated job entry.
                await self._state.save(queue_name, j["id"], j)
                if not deps:
                    # If all dependencies are resolved, enqueue the job.
                    await self.enqueue(queue_name, j["payload"])

    async def pause_queue(self, queue_name: str) -> None:
        """
        Pauses a specific queue, preventing new jobs from being dequeued.

        This is implemented by saving a special '_pause' entry in the job store
        for the given queue.

        Args:
            queue_name: The name of the queue to pause.
        """
        # Save a special entry to indicate that the queue is paused.
        await self._state.save(queue_name, "_pause", {"paused": True})

    async def resume_queue(self, queue_name: str) -> None:
        """
        Resumes a paused queue, allowing jobs to be dequeued again.

        This is implemented by removing the special '_pause' entry from the job
        store for the given queue.

        Args:
            queue_name: The name of the queue to resume.
        """
        # Delete the special entry that indicates the queue is paused.
        await self._state.delete(queue_name, "_pause")

    async def is_queue_paused(self, queue_name: str) -> bool:
        """
        Checks if a specific queue is currently paused.

        Args:
            queue_name: The name of the queue to check.

        Returns:
            True if the queue is paused, False otherwise.
        """
        # Check if the special '_pause' entry exists for the queue.
        return bool(await self._state.load(queue_name, "_pause"))

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """
        Saves the progress of a specific job.

        If the job does not exist, an empty dictionary is used as a base
        before saving the progress.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            progress: The current progress of the job, typically a float between 0.0 and 1.0.
        """
        # Load the existing job entry or create an empty dictionary if not found.
        entry = await self._state.load(queue_name, job_id) or {}
        entry["progress"] = progress  # Store the job progress.
        # Save the updated entry back to the job store.
        await self._state.save(queue_name, job_id, entry)

    async def bulk_enqueue(self, queue_name: str, jobs: list[dict[str, Any]]) -> None:
        """
        Enqueues multiple jobs into the specified RabbitMQ queue in bulk.

        Each job in the provided list is enqueued individually using the `enqueue` method.

        Args:
            queue_name: The name of the queue to which the jobs will be enqueued.
            jobs: A list of dictionaries, where each dictionary represents a job
                payload and must include an 'id' key.
        """
        for j in jobs:
            # Enqueue each job individually.
            await self.enqueue(queue_name, j)

    async def purge(self, queue_name: str, state: str, older_than: float | None = None) -> None:
        """
        Purges jobs from a queue based on their status and optional age.

        Jobs with the specified `state` are removed. If `older_than` is provided,
        only jobs older than the given timestamp are removed.

        Args:
            queue_name: The name of the queue to purge jobs from.
            state: The status of the jobs to be purged (e.g., 'completed', 'failed').
            older_than: An optional Unix timestamp (float). If provided, only
                jobs with a 'timestamp' (heartbeat or creation) older than this
                value will be purged.
        """
        # Iterate through jobs with the specified status.
        for j in await self._state.jobs_by_status(queue_name, state):
            # Check if older_than is not specified or if the job's timestamp
            # is older than the specified value.
            if older_than is None or j.get("timestamp", 0) < older_than:
                # Delete the job from the job store.
                await self._state.delete(queue_name, j["id"])

    async def atomic_add_flow(
        self, queue_name: str, job_dicts: list[dict[str, Any]], dependency_links: list[tuple[str, str]]
    ) -> list[str]:
        """
        Atomically adds a flow of interconnected jobs with dependencies.

        This method enqueues jobs and sets up dependencies such that child jobs
        are only enqueued after their parent dependencies are resolved. Jobs
        that are children in a dependency link are initially only saved as
        metadata in the job store, not immediately enqueued.

        Args:
            queue_name: The name of the queue for the job flow.
            job_dicts: A list of job dictionaries, each with an 'id' and 'payload'.
            dependency_links: A list of tuples, where each tuple is
                (parent_job_id, child_job_id), indicating a dependency.

        Returns:
            A list of job IDs that were created as part of this flow.
        """
        # Identify all job IDs that are children in any dependency link.
        created_ids: list[str] = []

        for jd in job_dicts:
            job_id = jd["id"]
            created_ids.append(job_id)
            await self.enqueue(queue_name, jd)

        for parent_id, child_id in dependency_links:
            # this will push a Redis HSET (e.g. queue:<q>:deps:<parent> => child)
            await self.add_dependencies(
                queue_name,
                {
                    "id": child_id,
                    "depends_on": [parent_id],
                },
            )

        return created_ids

    async def cancel_job(self, queue_name: str, job_id: str) -> bool:
        """
        Cancels a specific job by updating its status to 'cancelled'.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to cancel.

        Returns:
            True if the job's status was updated to cancelled, False otherwise.
        """
        # Update the job's status to 'cancelled' in the job store.
        await self._state.save(queue_name, job_id, {"id": job_id, "status": "cancelled"})
        return True

    async def remove_job(self, queue_name: str, job_id: str) -> bool:
        """
        Removes a specific job from the job store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to remove.

        Returns:
            True if the job was successfully removed, False otherwise.
        """
        # Delete the specified job from the job store.
        await self._state.delete(queue_name, job_id)
        return True

    async def retry_job(self, queue_name: str, job_id: str) -> bool:
        """
        Retries a specific job by re-enqueuing its payload.

        The job's status is updated to 'waiting' after re-enqueueing.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to retry.

        Returns:
            True if the job was successfully re-enqueued, False otherwise
            (e.g., if the job was not found).
        """
        # Load the existing job entry.
        entry = await self._state.load(queue_name, job_id)
        if not entry:
            # If the job entry is not found, cannot retry.
            return False
        # Re-enqueue the job using its original payload.
        await self.enqueue(queue_name, entry["payload"])
        # Update the job's status back to 'waiting' in the job store.
        await self._state.save(queue_name, job_id, {"id": job_id, "status": "waiting"})
        return True

    async def is_job_cancelled(self, queue_name: str, job_id: str) -> bool:
        """
        Checks if a specific job has been cancelled.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job to check.

        Returns:
            True if the job's status is 'cancelled', False otherwise.
        """
        # Load the job entry.
        entry = await self._state.load(queue_name, job_id)
        # Check if the entry exists and its status is 'cancelled'.
        return cast(bool, entry.get("status") == "cancelled") if entry else False

    async def register_worker(self, worker_id: str, queue: str, concurrency: int, timestamp: float) -> None:
        """
        Registers a worker with its associated queue, concurrency, and last heartbeat.

        Worker information is saved in a special 'workers' collection in the job store.

        Args:
            worker_id: The unique identifier of the worker.
            queue: The name of the queue the worker is processing.
            concurrency: The maximum number of jobs the worker can process concurrently.
            timestamp: The Unix timestamp of the worker's last heartbeat.
        """
        # Save worker information in the 'workers' collection.
        await self._state.save(
            "workers", worker_id, {"id": worker_id, "queue": queue, "concurrency": concurrency, "heartbeat": timestamp}
        )

    async def deregister_worker(self, worker_id: str) -> None:
        """
        Deregisters a worker, removing its entry from the job store.

        Args:
            worker_id: The unique identifier of the worker to deregister.
        """
        # Delete the worker's entry from the 'workers' collection.
        await self._state.delete("workers", worker_id)

    async def list_workers(self) -> list[WorkerInfo]:
        """
        Lists all currently registered workers.

        Returns:
            A list of WorkerInfo objects, each representing a registered worker.
        """
        # Retrieve all jobs from the 'workers' collection and convert them
        # into WorkerInfo objects.
        return [WorkerInfo(**w) for w in await self._state.all_jobs("workers")]

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        """
        Retrieves statistics for a specific RabbitMQ queue.

        Currently, this method provides the message count for the queue.

        Args:
            queue_name: The name of the queue for which to retrieve statistics.

        Returns:
            A dictionary containing queue statistics, e.g., {'message_count': int}.
        """
        await self._connect()  # Ensure connection is established.
        # Declare the queue passively to get its properties without creating it
        # if it doesn't exist.
        q = await self._chan.declare_queue(queue_name, durable=True, passive=True)
        # Return the message count from the declaration result.
        return {"message_count": q.declaration_result.message_count}

    async def drain_queue(self, queue_name: str) -> None:
        """
        Drains (purges) all messages from a specific RabbitMQ queue.

        This operation permanently removes all unacknowledged messages from the queue.

        Args:
            queue_name: The name of the queue to drain.
        """
        await self._connect()  # Ensure connection is established.
        # Declare the queue, ensuring its durability.
        q = await self._chan.declare_queue(queue_name, durable=True)
        await q.purge()  # Purge all messages from the queue.

    async def create_lock(self, key: str, ttl: int) -> Any:
        """
        Acquires a distributed lock.

        This method uses the underlying job store's locking mechanism.

        Args:
            key: The unique identifier for the lock.
            ttl: The time-to-live (in seconds) for the lock.

        Returns:
            An object representing the acquired lock. The specific type depends
            on the underlying job store's implementation.
        """
        # Delegate the lock creation to the job store.
        return await self._state.create_lock(key, ttl)

    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        """
        Emits a generic event with associated data.

        This method uses the global `event_emitter` to broadcast events.

        Args:
            event: The name of the event to emit.
            data: A dictionary containing data associated with the event.
        """
        await event_emitter.emit(event, data)

    async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
        """
        Saves a heartbeat timestamp for a specific job.

        This updates the 'heartbeat' field in the job's metadata, indicating
        that the job is still actively being processed.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            timestamp: The Unix timestamp (float) of the heartbeat.
        """
        # Load the existing job entry or create an empty dictionary if not found.
        entry = await self._state.load(queue_name, job_id) or {}
        entry["heartbeat"] = timestamp  # Update the heartbeat timestamp.
        # Save the updated entry back to the job store.
        await self._state.save(queue_name, job_id, entry)

    async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
        """
        Fetches jobs that appear to be stalled (i.e., no recent heartbeat).

        A job is considered stalled if its last 'heartbeat' timestamp is older
        than the specified `older_than` timestamp.

        Args:
            older_than: The Unix timestamp (float). Jobs with a 'heartbeat'
                older than this value will be considered stalled.

        Returns:
            A list of dictionaries, where each dictionary contains 'queue'
            and 'job_data' for a stalled job.
        """
        stalled: list[dict[str, Any]] = []
        # Iterate through all queues.
        for q_name in await self.list_queues():
            # Iterate through all jobs in each queue.
            for j in await self._state.all_jobs(q_name):
                # Check if the job has a heartbeat and it's older than 'older_than'.
                if j.get("heartbeat", 0) < older_than:
                    stalled.append({"queue": q_name, "job_data": j})
        return stalled

    async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
        """
        Re-enqueues a stalled job.

        This method takes the payload of a stalled job and enqueues it back
        into its original queue, effectively attempting a retry.

        Args:
            queue_name: The name of the queue the stalled job originally belonged to.
            job_data: A dictionary containing the stalled job's data, including
                its 'payload' key.
        """
        # Enqueue the job using its original payload.
        await self.enqueue(queue_name, job_data["payload"])

    async def list_jobs(self, queue_name: str, state: str) -> list[dict[str, Any]]:
        """
        Lists jobs in a specific queue filtered by their status.

        Args:
            queue_name: The name of the queue to list jobs from.
            state: The status of the jobs to retrieve (e.g., 'waiting', 'completed').

        Returns:
            A list of dictionaries, where each dictionary represents a job
            matching the specified status.
        """
        # Retrieve jobs filtered by the given status from the job store.
        return await self._state.jobs_by_status(queue_name, state)

    async def list_queues(self) -> list[str]:
        """
        Lists all known queues managed by the job store.

        Returns:
            A list of strings, where each string is the name of a queue.
        """
        # Delegate the listing of queues to the job store.
        return list(self._queues.keys())

    async def close(self) -> None:
        """
        Closes the RabbitMQ channel and connection.

        This method should be called to properly release resources when the
        backend is no longer needed.
        """
        if self._chan and not self._chan.is_closed:
            # Close the channel if it's open.
            await self._chan.close()
        if self._conn and not self._conn.is_closed:
            # Close the connection if it's open.
            await self._conn.close()

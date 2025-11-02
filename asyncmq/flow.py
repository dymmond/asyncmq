from typing import Awaitable, Callable

import asyncmq
from asyncmq.backends.base import BaseBackend
from asyncmq.core.dependencies import add_dependencies
from asyncmq.jobs import Job

_AddDependenciesCallable = Callable[[str, Job, BaseBackend], Awaitable[None]]


class FlowProducer:
    """
    Facilitates the atomic or near-atomic addition of a set of related jobs
    with defined dependencies (referred to as a "flow").

    This class provides a high-level interface to enqueue multiple jobs and
    ensure their dependencies are registered with the backend. It attempts
    to use the backend's native `atomic_add_flow` method if available and
    falls back to a sequential enqueue and dependency registration process
    otherwise.
    """

    def __init__(self, backend: BaseBackend | None = None) -> None:
        """
        Initializes the FlowProducer with a specific backend or the default.

        Args:
            backend: An optional instance of a class inheriting from
                     `BaseBackend`. If None, the backend specified in
                     `asyncmq.conf.settings` is used.
        """
        # Use the provided backend instance or fall back to the configured default.
        self.backend: BaseBackend = backend or asyncmq.monkay.settings.backend
        # Assign the dependency adder function.
        self._add_dependencies: _AddDependenciesCallable = add_dependencies

    async def add_flow(self, queue: str, jobs: list[Job]) -> list[str]:
        """
        Asynchronously enqueues a graph of jobs and their dependencies onto
        the specified queue.

        This method first prepares the job payloads and dependency links. It
        then attempts to use the backend's `atomic_add_flow` method for a
        single, atomic operation. If the backend does not support this method
        (AttributeError or NotImplementedError), it falls back to sequentially
        enqueuing each job and then registering its dependencies individually.

        Args:
            queue: The name of the queue where all jobs in the flow should be
                   enqueued.
            jobs: A list of `Job` instances that constitute the flow. Dependencies
                  should be defined within the `depends_on` attribute of each `Job`
                  instance.

        Returns:
            A list of strings, representing the unique IDs of the jobs that were
            successfully enqueued, in the order they were provided in the `jobs` list.
        """
        # Prepare the job payloads (dictionaries) from the Job instances.
        payloads = [job.to_dict() for job in jobs]
        # Prepare the list of dependency links as (parent_id, child_id) tuples.
        deps: list[tuple[str, str]] = []
        # Iterate through each job to extract its dependencies.
        for job in jobs:
            # For each parent ID in the job's dependencies.
            for parent in job.depends_on:
                # Add a tuple representing the dependency link (parent -> child).
                deps.append((parent, job.id))

        # Attempt to use the backend's atomic_add_flow method if it exists and is implemented.
        try:
            # Call the backend's atomic method.
            return await self.backend.atomic_add_flow(queue, payloads, deps)
        except (AttributeError, NotImplementedError):
            # If atomic_add_flow is not supported, execute the fallback logic.
            created: list[str] = []
            for job in jobs:
                if not job.depends_on:
                    await self.backend.enqueue(queue, job.to_dict())
                    created.append(job.id)

            # Iterate through each job in the flow.
            # Fallback: sequential enqueue + dependency registration.
            for job in jobs:
                if job.depends_on:
                    await self._add_dependencies(queue, job, self.backend)
                    # Add the job ID to the list of created IDs.
                    # Enqueue the job individually.
                    await self.backend.enqueue(queue, job.to_dict())
                    # Register the job's dependencies individually using the helper callable.
                    created.append(job.id)

            # Return the list of IDs for the jobs that were enqueued via the fallback.
            return created

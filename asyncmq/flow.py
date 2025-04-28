from typing import Awaitable, Callable

from asyncmq.backends.base import BaseBackend
from asyncmq.conf import settings
from asyncmq.core.dependencies import add_dependencies
from asyncmq.job import Job

# Define the expected type signature for the add_dependencies function
_AddDependenciesCallable = Callable[[BaseBackend, str, Job], Awaitable[None]]


class FlowProducer:
    """
    Facilitates the atomic addition of a set of jobs that have defined dependencies
    on each other, forming a job "flow" or graph.

    This class simplifies the process of enqueuing a collection of jobs where
    the execution order or readiness of some jobs depends on the successful
    completion of others. It ensures that each job is enqueued and its
    dependencies are registered with the backend.
    """

    def __init__(self, backend: BaseBackend | None = None) -> None:
        """
        Initializes the FlowProducer with an optional backend instance.

        Args:
            backend: An optional backend instance to use for interacting with
                     the queue and managing dependencies. If None, a `RedisBackend`
                     instance is created and used by default.
        """
        # Use the provided backend or fall back to the default configured backend.
        self.backend: BaseBackend = backend or settings.backend
        # Assign the dependency management function.
        self._add_dependencies: _AddDependenciesCallable = add_dependencies

    async def add_flow(self, queue: str, jobs: list[Job]) -> list[str]:
        """
        Adds a set of jobs, including their defined dependencies, to the queue.

        This method iterates through a list of `Job` instances. For each job,
        it first enqueues the job itself using the backend and then calls
        the internal dependency management function (`_add_dependencies`)
        to register any jobs listed in the `job.depends_on` attribute with
        the backend for this specific job.

        Args:
            queue: The name of the queue onto which the jobs should be enqueued.
            jobs: A list of `Job` instances to be added as a flow. Each `Job`
                  instance in this list may have its `depends_on` attribute populated
                  with the IDs of other jobs in this same list or previously added jobs.

        Returns:
            A list of the unique ID strings for all the jobs that were created
            and processed in this flow, in the same order as the input `jobs` list.
        """
        created_ids: list[str] = []
        # Iterate through each job in the provided list.
        for job in jobs:
            # Append the job's ID to the list of created IDs.
            created_ids.append(job.id)
            # Enqueue the job's dictionary representation using the backend.
            await self.backend.enqueue(queue, job.to_dict())
            # Add any dependencies defined for this job using the internal function.
            await self._add_dependencies(self.backend, queue, job)

        # Return the list of IDs for all jobs added in this flow.
        return created_ids

from typing import Awaitable, Callable, List, Optional

from asyncmq.backends.base import BaseBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.dependencies import add_dependencies
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
    def __init__(self, backend: Optional[BaseBackend] = None) -> None:
        """
        Initializes the FlowProducer with an optional backend instance.

        Args:
            backend: An optional backend instance to use for interacting with
                     the queue and managing dependencies. If None, a `RedisBackend`
                     instance is created and used by default.
        """
        # Store the backend instance, defaulting to RedisBackend if none is provided.
        self.backend: BaseBackend = backend or RedisBackend()
        # Store a reference to the dependency management function.
        # The type hint describes a callable that takes a BaseBackend, a string,
        # and a Job, and returns an Awaitable resolving to None.
        self._add_dependencies: _AddDependenciesCallable = add_dependencies

    async def add_flow(self, queue: str, jobs: List[Job]) -> List[str]:
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
        created_ids: List[str] = []
        # Iterate through each Job instance in the provided list.
        for job in jobs:
            # Add the ID of the job to the list of created IDs.
            created_ids.append(job.id)
            # Enqueue the job itself onto the specified queue using the backend.
            # The job's state and parameters are stored via its dictionary representation.
            await self.backend.enqueue(queue, job.to_dict())
            # Register any dependencies specified in the job's `depends_on` list
            # with the backend. This step ensures that jobs waiting on this one
            # can be unlocked later.
            await self._add_dependencies(self.backend, queue, job)

        # Return the list of IDs for the jobs that were added as part of this flow.
        return created_ids

from typing import Awaitable, Callable, List

from asyncmq.backends.base import BaseBackend
from asyncmq.conf import settings
from asyncmq.core.dependencies import add_dependencies
from asyncmq.jobs import Job

# Define expected signature for dependency adder
_AddDependenciesCallable = Callable[[BaseBackend, str, Job], Awaitable[None]]


class FlowProducer:
    """
    Facilitates atomic addition of a set of jobs with dependencies (flows).
    """
    def __init__(self, backend: BaseBackend | None = None) -> None:
        self.backend: BaseBackend = backend or settings.backend
        self._add_dependencies: _AddDependenciesCallable = add_dependencies

    async def add_flow(self, queue: str, jobs: List[Job]) -> List[str]:
        """
        Enqueue a graph of jobs and their dependencies.

        Tries atomic_add_flow if supported; otherwise falls back to per-job logic.
        """
        # Prepare payloads and dependency links
        payloads = [job.to_dict() for job in jobs]
        deps: List[tuple[str, str]] = []
        for job in jobs:
            for parent in job.depends_on:
                deps.append((parent, job.id))

        # Attempt atomic operation
        try:
            return await self.backend.atomic_add_flow(queue, payloads, deps)
        except (AttributeError, NotImplementedError):
            # Fallback: sequential enqueue + dependency registration
            created: List[str] = []
            for job in jobs:
                created.append(job.id)
                await self.backend.enqueue(queue, job.to_dict())
                await self._add_dependencies(queue, job, self.backend)
            return created

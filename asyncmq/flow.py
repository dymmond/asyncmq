from typing import List, Optional

from asyncmq.backends.base import BaseBackend
from asyncmq.backends.redis import RedisBackend
from asyncmq.dependencies import add_dependencies
from asyncmq.job import Job


class FlowProducer:
    """
    Simple flow producer for jobs with dependencies:
    Define graphs and add them atomically.
    """
    def __init__(self, backend: Optional[BaseBackend] = None):
        self.backend = backend or RedisBackend()
        self._add_dependencies = add_dependencies

    async def add_flow(self, queue: str, jobs: List[Job]) -> List[str]:
        """Add a set of jobs with defined depends_on lists."""
        created_ids = []
        for job in jobs:
            created_ids.append(job.id)
            # Save job state and dependencies
            await self.backend.enqueue(queue, job.to_dict())
            await self._add_dependencies(self.backend, queue, job)
        return created_ids

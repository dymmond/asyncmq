from abc import ABC, abstractmethod
from typing import List, Optional


class BaseJobStore(ABC):
    @abstractmethod
    async def save(self, queue_name: str, job_id: str, data: dict): ...

    @abstractmethod
    async def load(self, queue_name: str, job_id: str) -> Optional[dict]: ...

    @abstractmethod
    async def delete(self, queue_name: str, job_id: str): ...

    @abstractmethod
    async def all_jobs(self, queue_name: str) -> List[dict]: ...

    @abstractmethod
    async def jobs_by_status(self, queue_name: str, status: str) -> List[dict]: ...

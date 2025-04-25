from abc import ABC, abstractmethod
from typing import Any, Optional


class BaseBackend(ABC):
    @abstractmethod
    async def enqueue(self, queue_name: str, payload: dict): ...

    @abstractmethod
    async def dequeue(self, queue_name: str) -> Optional[dict]: ...

    @abstractmethod
    async def move_to_dlq(self, queue_name: str, payload: dict): ...

    @abstractmethod
    async def ack(self, queue_name: str, job_id: str): ...

    @abstractmethod
    async def enqueue_delayed(self, queue_name: str, payload: dict, run_at: float): ...

    @abstractmethod
    async def get_due_delayed(self, queue_name: str) -> list[dict]: ...

    @abstractmethod
    async def remove_delayed(self, queue_name: str, job_id: str): ...

    @abstractmethod
    async def update_job_state(self, queue_name: str, job_id: str, state: str): ...

    @abstractmethod
    async def save_job_result(self, queue_name: str, job_id: str, result: Any): ...

    @abstractmethod
    async def get_job_state(self, queue_name: str, job_id: str) -> Optional[str]: ...

    @abstractmethod
    async def get_job_result(self, queue_name: str, job_id: str) -> Optional[Any]: ...

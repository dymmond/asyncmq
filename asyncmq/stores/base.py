from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseJobStore(ABC):
    """
    Abstract base class defining the interface for backend job data storage.

    Concrete backend implementations must inherit from this class and provide
    implementations for all the abstract methods. This store is responsible
    for persisting, retrieving, and deleting job data dictionaries.
    """
    @abstractmethod
    async def save(self, queue_name: str, job_id: str, data: Dict[str, Any]) -> None:
        """
        Asynchronously saves or updates the data for a specific job in the store.

        This method is used to persist the current state and data of a job,
        identified by its queue name and job ID.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
            data: A dictionary containing the job's data and metadata to be saved.
        """
        ...

    @abstractmethod
    async def load(self, queue_name: str, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Asynchronously loads the data for a specific job from the store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.

        Returns:
            A dictionary containing the job's data and metadata if found,
            otherwise None.
        """
        ...

    @abstractmethod
    async def delete(self, queue_name: str, job_id: str) -> None:
        """
        Asynchronously deletes the data for a specific job from the store.

        Args:
            queue_name: The name of the queue the job belongs to.
            job_id: The unique identifier of the job.
        """
        ...

    @abstractmethod
    async def all_jobs(self, queue_name: str) -> List[Dict[str, Any]]:
        """
        Asynchronously retrieves data for all jobs associated with a specific queue.

        Args:
            queue_name: The name of the queue.

        Returns:
            A list of dictionaries, where each dictionary contains the data
            for a job in the specified queue.
        """
        ...

    @abstractmethod
    async def jobs_by_status(self, queue_name: str, status: str) -> List[Dict[str, Any]]:
        """
        Asynchronously retrieves data for jobs in a specific queue that are
        currently in a given status.

        Args:
            queue_name: The name of the queue.
            status: The status of the jobs to retrieve (e.g., State.WAITING, State.ACTIVE).

        Returns:
            A list of dictionaries, where each dictionary contains the data
            for a job matching the criteria.
        """
        ...

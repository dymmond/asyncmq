import time
import uuid
from typing import Any, Dict

JOB_STATES = ("waiting", "active", "completed", "failed", "delayed", "expired")

class Job:
    def __init__(
        self,
        task_id: str,
        args: list,
        kwargs: dict,
        retries: int = 0,
        max_retries: int = 3,
        backoff: float | None = 1.5,
        ttl: int | None = None,
        job_id: str | None = None,
        created_at: float | None = None,
        priority: int = 5,
        repeat_every: int | float | None = None,
        depends_on: list[Any] | None = None,
    ):
        self.id = job_id or str(uuid.uuid4())
        self.task_id = task_id
        self.args = args
        self.kwargs = kwargs
        self.retries = retries
        self.max_retries = max_retries
        self.backoff = backoff
        self.ttl = ttl
        self.created_at = created_at or time.time()
        self.last_attempt = None
        self.status = "waiting"
        self.result = None
        self.delay_until = None
        self.priority = priority
        self.depends_on = depends_on if depends_on else[]
        self.repeat_every = repeat_every  # seconds

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "task": self.task_id,
            "args": self.args,
            "kwargs": self.kwargs,
            "retries": self.retries,
            "max_retries": self.max_retries,
            "backoff": self.backoff,
            "ttl": self.ttl,
            "created_at": self.created_at,
            "last_attempt": self.last_attempt,
            "status": self.status,
            "result": self.result,
            "delay_until": self.delay_until,
            "priority": self.priority,
            "depends_on": self.depends_on,
            "repeat_every": self.repeat_every,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Job":
        job = Job(
            task_id=data["task"],
            args=data["args"],
            kwargs=data["kwargs"],
            retries=data["retries"],
            max_retries=data.get("max_retries", 3),
            backoff=data.get("backoff"),
            ttl=data.get("ttl"),
            job_id=data["id"],
            created_at=data.get("created_at"),
            priority=data.get("priority", 5)
        )
        job.status = data.get("status", "waiting")
        job.result = data.get("result")
        job.delay_until = data.get("delay_until")
        job.last_attempt = data.get("last_attempt")
        job.depends_on = data.get("depends_on", [])
        job.repeat_every = data.get("repeat_every")
        return job

    def is_expired(self) -> bool:
        if self.ttl is None:
            return False
        return (time.time() - self.created_at) > self.ttl

    def next_retry_delay(self) -> float:
        if self.backoff:
            return self.backoff ** self.retries
        return 0

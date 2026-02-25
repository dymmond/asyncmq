"""Benchmarks for Job creation and serialization."""

import pytest

from asyncmq.jobs import Job


@pytest.mark.benchmark
def test_job_creation():
    """Benchmark creating a Job instance with default parameters."""
    job = Job(
        task_id="mymodule.my_task",
        args=[1, 2, 3],
        kwargs={"key": "value"},
    )
    assert job.task_id == "mymodule.my_task"


@pytest.mark.benchmark
def test_job_creation_with_all_options():
    """Benchmark creating a Job with all options specified."""
    job = Job(
        task_id="mymodule.complex_task",
        args=[1, "hello", {"nested": True}],
        kwargs={"timeout": 30, "retries": 5},
        retries=0,
        max_retries=10,
        backoff=2.0,
        ttl=3600,
        priority=1,
        repeat_every=60.0,
        depends_on=["job-1", "job-2", "job-3"],
    )
    assert job.priority == 1


@pytest.mark.benchmark
def test_job_to_dict():
    """Benchmark serializing a Job to a dictionary."""
    job = Job(
        task_id="mymodule.my_task",
        args=[1, 2, 3],
        kwargs={"key": "value"},
        max_retries=5,
        backoff=2.0,
        ttl=120,
        priority=3,
        depends_on=["dep-1", "dep-2"],
    )
    data = job.to_dict()
    assert data["task"] == "mymodule.my_task"


@pytest.mark.benchmark
def test_job_from_dict():
    """Benchmark deserializing a Job from a dictionary."""
    data = {
        "id": "test-job-id",
        "task": "mymodule.my_task",
        "args": [1, 2, 3],
        "kwargs": {"key": "value"},
        "retries": 0,
        "max_retries": 5,
        "backoff": 2.0,
        "ttl": 120,
        "created_at": 1700000000.0,
        "status": "waiting",
        "result": None,
        "delay_until": None,
        "last_attempt": None,
        "priority": 3,
        "depends_on": ["dep-1", "dep-2"],
        "repeat_every": None,
    }
    job = Job.from_dict(data)
    assert job.id == "test-job-id"


@pytest.mark.benchmark
def test_job_roundtrip():
    """Benchmark a full serialize/deserialize roundtrip."""
    job = Job(
        task_id="mymodule.roundtrip_task",
        args=list(range(10)),
        kwargs={f"key_{i}": f"value_{i}" for i in range(5)},
        max_retries=3,
        backoff=1.5,
        ttl=300,
        priority=2,
    )
    data = job.to_dict()
    restored = Job.from_dict(data)
    assert restored.task_id == job.task_id


@pytest.mark.benchmark
def test_job_is_expired():
    """Benchmark the is_expired check."""
    job = Job(
        task_id="mymodule.expiring_task",
        args=[],
        kwargs={},
        ttl=3600,
    )
    result = job.is_expired()
    assert result is False


@pytest.mark.benchmark
def test_job_next_retry_delay():
    """Benchmark computing the next retry delay with exponential backoff."""
    job = Job(
        task_id="mymodule.retry_task",
        args=[],
        kwargs={},
        retries=3,
        backoff=2.0,
    )
    delay = job.next_retry_delay()
    assert delay == 8.0

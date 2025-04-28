# tests/test_job_model.py
import time

from asyncmq.job import Job


def test_job_creation_defaults():
    job = Job(task_id="test.task", args=[1], kwargs={})
    assert job.task_id == "test.task"
    assert job.status == "waiting"
    assert job.priority == 5
    assert job.retries == 0
    assert job.max_retries == 3
    assert job.result is None
    assert job.delay_until is None
    assert isinstance(job.id, str)
    assert isinstance(job.created_at, float)


def test_job_to_from_dict():
    job = Job(task_id="test.serialize", args=[1, 2], kwargs={"key": "value"})
    job.status = "active"
    job.result = 42
    job.priority = 2
    job.depends_on = ["abc"]
    d = job.to_dict()
    clone = Job.from_dict(d)
    assert clone.task_id == job.task_id
    assert clone.args == job.args
    assert clone.kwargs == job.kwargs
    assert clone.status == job.status
    assert clone.result == job.result
    assert clone.priority == job.priority
    assert clone.depends_on == job.depends_on


def test_job_expiration():
    job = Job(task_id="expire.test", args=[], kwargs={}, ttl=1)
    assert not job.is_expired()
    time.sleep(1.1)
    assert job.is_expired()


def test_next_retry_delay():
    job = Job(task_id="retry.test", args=[], kwargs={}, backoff=2.0)
    job.retries = 1
    assert job.next_retry_delay() == 2.0
    job.retries = 2
    assert job.next_retry_delay() == 4.0


def test_priority_handling():
    job1 = Job(task_id="low", args=[], kwargs={}, priority=10)
    job2 = Job(task_id="high", args=[], kwargs={}, priority=1)
    assert job1.priority > job2.priority


def test_job_dependencies():
    job = Job(task_id="depends.test", args=[], kwargs={})
    job.depends_on = ["123", "456"]
    d = job.to_dict()
    assert d["depends_on"] == ["123", "456"]
    clone = Job.from_dict(d)
    assert clone.depends_on == ["123", "456"]

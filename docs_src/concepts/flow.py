from asyncmq.backends.redis import RedisBackend
from asyncmq.flow import FlowProducer
from asyncmq.jobs import Job

# 1. Initialize your backend and flow producer
backend = RedisBackend(redis_url_or_client="redis://localhost:6379/0")

# Not mandatory as it reads the default backend from the settings.
flow = FlowProducer(backend=backend)

# 2. Create Job objects for each step in your pipeline
#    - job1 runs first
#    - job2 depends_on job1 and runs only after job1 completes
job1 = Job(
    task_id="app.say_hello",  # must match your registered task ID
    args=["Alice"],
    kwargs={},
    # Other params (retries, ttl, priority) use defaults
)
job2 = Job(
    task_id="app.process_data",
    args=[42],
    kwargs={"verbose": True},
    depends_on=[job1.id],
)

# 3. Atomically enqueue the flow to the "default" queue
#    If your backend supports `atomic_add_flow`, this will happen in one operation;
#    otherwise, AsyncMQ falls back to sequential enqueue + dependency registration.
job_ids = await flow.add_flow("default", [job1, job2])
print(f"Enqueued jobs: {job_ids}")

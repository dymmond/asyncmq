from asyncmq.backends.redis import RedisBackend
from asyncmq.flow import FlowProducer
from asyncmq.jobs import Job
from asyncmq.logging import logger

backend = RedisBackend(redis_url="redis://localhost:6379/0")
flow = FlowProducer(backend=backend)

# Define jobs
job_ingest = Job(task_id="app.ingest_data", args=["s3://bucket/file.csv"], kwargs={})
job_transform = Job(
    task_id="app.transform",
    args=[],
    kwargs={"threshold": 0.75},
    depends_on=[job_ingest.id],
)
job_load = Job(
    task_id="app.load_to_db", args=["db_table"], kwargs={},
    depends_on=[job_transform.id]
)

# Enqueue the entire flow atomically if supported
job_ids = await flow.add_flow("data_pipeline", [job_ingest, job_transform, job_load])
logger.info(f"Enqueued pipeline jobs: {job_ids}")

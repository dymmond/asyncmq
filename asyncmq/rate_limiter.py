import asyncio
import time
from collections import deque

class RateLimiter:
    """
    A simple token-bucket rate limiter.
    """

    def __init__(self, rate: int, interval: float):
        """
        :param rate: Maximum number of jobs per interval
        :param interval: Time window in seconds
        """
        self.rate = rate
        self.interval = interval
        self.timestamps = deque()  # timestamps of recent acquires

    async def acquire(self):
        """
        Acquire permission to run one job. Will sleep if rate is exceeded.
        """
        now = time.time()
        # Remove timestamps older than the window
        while self.timestamps and (now - self.timestamps[0]) >= self.interval:
            self.timestamps.popleft()

        if len(self.timestamps) < self.rate:
            self.timestamps.append(now)
            return

        # Otherwise wait until the oldest timestamp falls out
        earliest = self.timestamps[0]
        wait = self.interval - (now - earliest)
        if wait > 0:
            await asyncio.sleep(wait)

        # Record this run
        self.timestamps.append(time.time())


    async def schedule_job(self, job):
        """
        Schedules a job for execution, applying rate limiting.

        :param job: The job to be processed
        """
        # Wait until a token is available
        await self.acquire()

        # Process the job now that it has been rate-limited
        self.active_jobs.add(job)
        await job()  # Execute the task asynchronously
        self.active_jobs.remove(job)

    def add_waiting_job(self, job):
        """
        Adds a job to the waiting queue when rate-limited.

        :param job: The job that needs to wait before it can be processed
        """
        self.waiting_jobs.append(job)

    def job_completed(self):
        """
        Removes the job from the waiting queue once it has completed and a token becomes available.
        """
        if self.waiting_jobs:
            next_job = self.waiting_jobs.popleft()
            asyncio.create_task(self.schedule_job(next_job))


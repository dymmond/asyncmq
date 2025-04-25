import asyncio
import time
from collections import deque


class RateLimiter:
    def __init__(self, rate: int, interval: float):
        """
        Initializes the rate limiter.

        :param rate: The number of tasks that can be processed in each interval (e.g., 3 tasks per second).
        :param interval: The time interval in seconds to allow the rate-limiting to reset.
        """
        self.rate = rate  # Maximum number of jobs allowed in each interval
        self.interval = interval  # Time window in seconds for rate limiting
        self.tokens = rate  # Initial number of tokens available
        self.last_checked = time.monotonic()  # Last time the tokens were refilled
        self.lock = asyncio.Lock()
        self.waiting_jobs = deque()  # Queue for jobs waiting for rate-limiting
        self.active_jobs = set()  # Set of jobs that are currently being processed

    async def acquire(self):
        """
        Acquires a token to process a job. If no tokens are available, it waits for the next available token.
        """
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_checked
            refill = (elapsed / self.interval) * self.rate
            self.tokens = min(self.rate, self.tokens + refill)  # Refill tokens
            self.last_checked = now

            if self.tokens >= 1:
                self.tokens -= 1
                return True
            # Wait for the next token to be available
            wait_time = self.interval / self.rate
            await asyncio.sleep(wait_time)
            return True

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


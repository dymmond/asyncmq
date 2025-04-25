import asyncio
import time


class RateLimiter:
    def __init__(self, rate: int, interval: float):
        self.rate = rate  # e.g., 10 jobs
        self.interval = interval  # per N seconds
        self.tokens = rate
        self.last_checked = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_checked
            refill = (elapsed / self.interval) * self.rate
            self.tokens = min(self.rate, self.tokens + refill)
            self.last_checked = now

            if self.tokens >= 1:
                self.tokens -= 1
                return
            # Wait until next available token
            wait_time = self.interval / self.rate
            await asyncio.sleep(wait_time)

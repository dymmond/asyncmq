from collections import deque
from typing import Any, Awaitable, Callable, Deque

import anyio


class RateLimiter:
    """
    A simple token‐bucket rate limiter, ported to AnyIO.

    Use `await acquire()` before sending a job, or use `schedule_job()`
    to both acquire and run a coroutine under the rate limit.
    """

    def __init__(self, rate: int, interval: float):
        """
        :param rate:     Max number of jobs per interval
        :param interval: Time window in seconds
        """
        self.rate = rate
        self.interval = interval
        self.timestamps: Deque[float] = deque()  # when tokens were taken

    async def acquire(self) -> None:
        """
        Wait until a token is available, then consume it.
        """
        now = anyio.current_time()  # monotonic clock
        # drop old timestamps
        while self.timestamps and (now - self.timestamps[0]) >= self.interval:
            self.timestamps.popleft()

        if len(self.timestamps) < self.rate:
            # token available immediately
            self.timestamps.append(now)
            return

        # otherwise wait for the oldest token to expire
        earliest = self.timestamps[0]
        wait = self.interval - (now - earliest)
        if wait > 0:
            await anyio.sleep(wait)

        # consume a new token
        self.timestamps.append(anyio.current_time())

    async def schedule_job(
        self,
        job: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """
        Acquire a token, then run the given async `job(*args, **kwargs)`.
        Returns whatever the job returns.
        """
        await self.acquire()
        return await job(*args, **kwargs)

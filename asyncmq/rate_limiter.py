from collections import deque
from typing import Any, Awaitable, Callable

import anyio


class RateLimiter:
    """
    Implements a token-bucket rate limiting mechanism adapted for use with AnyIO.

    This class controls the rate at which operations (such as processing jobs)
    can occur. It uses a queue (`timestamps`) to track the timestamps when
    "tokens" (representing allowed operations) were acquired. When `acquire()`
    is called, it checks if acquiring a token would violate the rate limit
    within the defined time window (`interval`). If so, it waits until the
    oldest token in the bucket expires, freeing up capacity.

    The rate limit is defined by the maximum number of operations (`rate`)
    allowed within a specific `interval` of time.

    Provides two primary ways to use the limiter:
    1. Call `await acquire()` directly before performing a rate-limited operation.
    2. Use `await schedule_job(coroutine_function, *args, **kwargs)` to automatically
       acquire a token and then run a given asynchronous function.
    """

    def __init__(self, rate: int, interval: float) -> None:
        """
        Initializes the RateLimiter instance.

        Args:
            rate: The maximum number of tokens (operations) allowed within
                  any given `interval`. Must be a non-negative integer.
            interval: The time window in seconds over which the `rate` applies.
                      Must be a positive float.
        """
        self.rate: int = rate
        self.interval: float = interval
        # A deque storing the monotonic timestamps when each token was acquired.
        # This acts as the "bucket" for the token-bucket algorithm.
        self.timestamps: deque[float] = deque()

    async def acquire(self) -> None:
        """
        Waits asynchronously until a token is available in the token bucket
        and then acquires it.

        This method implements the core rate limiting logic. It first removes
        any timestamps from the `timestamps` deque that are older than the
        `interval` from the current time, freeing up space in the bucket.
        If the number of remaining timestamps is less than the maximum `rate`,
        a token is immediately available, and the current time is appended
        to the deque.

        If the bucket is full (number of timestamps equals `rate`), it calculates
        how long to wait until the oldest timestamp (the front of the deque)
        is older than the `interval`, effectively waiting for a token to expire
        and be removed in the next cleanup cycle. It then waits using `anyio.sleep`.
        After waiting, the current time is appended to the deque as a new token
        acquisition.

        Uses `anyio.current_time()` which provides a monotonic clock, suitable
        for measuring elapsed time intervals.
        """
        # Get the current time using a monotonic clock.
        now: float = anyio.current_time()

        # Remove timestamps that are older than the interval from the left of the deque.
        while self.timestamps and (now - self.timestamps[0]) >= self.interval:
            self.timestamps.popleft()

        # If the bucket has space (fewer timestamps than the rate), acquire immediately.
        if len(self.timestamps) < self.rate:
            self.timestamps.append(now)
            return

        # If the bucket is full, calculate how long to wait for the oldest token to expire.
        earliest: float = self.timestamps[0]
        wait: float = self.interval - (now - earliest)

        # If waiting is required, sleep for the calculated duration.
        if wait > 0:
            await anyio.sleep(wait)

        # After waiting (if any), acquire the token by adding the current time.
        self.timestamps.append(anyio.current_time())

    async def schedule_job(self, job: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        """
        Acquires a token from the rate limiter and then executes the provided
        asynchronous callable (job).

        This is a convenience method that combines the token acquisition step
        with the execution of an async function. It first calls `await self.acquire()`
        to respect the rate limit. Once a token is successfully acquired (and
        any necessary waiting is done), it awaits the execution of the `job`
        coroutine function with the given positional and keyword arguments.

        Args:
            job: An asynchronous callable (e.g., a coroutine function) that
                 represents the task to be executed under the rate limit.
                 Expected to be an Awaitable and can accept arbitrary arguments.
            *args: Positional arguments to pass to the `job` callable.
            **kwargs: Keyword arguments to pass to the `job` callable.

        Returns:
            The result returned by the `job` callable.
        """
        # Wait until a token is available from the rate limiter.
        await self.acquire()
        # Execute the provided asynchronous job function with its arguments.
        return await job(*args, **kwargs)

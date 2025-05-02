import time

from asyncmq.logging import logger
from asyncmq.tasks import task


@task(queue="email", retries=2, ttl=120)
async def send_welcome(email: str):
    """
    Simulate sending a welcome email.
    If this were real, you'd integrate with SMTP or SendGrid.
    """
    # time.sleep runs in a thread if the function is async—no event loop blockage.
    time.sleep(0.1)
    logger.info(f"✉️  Welcome email sent to {email}")

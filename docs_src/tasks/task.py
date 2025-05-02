from asyncmq.tasks import task


@task(queue="default", retries=1, ttl=60)
async def send_email(recipient: str, subject: str, body: str):
    """
    Sends an email asynchronously. Retries once on failure, expires after 60s.
    """
    # Simulate I/O-bound work
    import time; time.sleep(0.1)
    print(f"✉️ Email sent to {recipient}: {subject}")

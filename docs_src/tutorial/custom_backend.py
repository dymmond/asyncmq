import json
from typing import Optional

import aiobotocore.session

from asyncmq.backends.base import BaseBackend


class SQSBackend(BaseBackend):
    def __init__(self, queue_url: str):
        session = aiobotocore.session.get_session()
        self.client = session.create_client('sqs')
        self.queue_url = queue_url

    async def enqueue(self, queue: str, job_dict: dict) -> None:
        await self.client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps(job_dict),
        )

    async def dequeue(self, queue: str, timeout: int) -> Optional[dict]:
        resp = await self.client.receive_message(
            QueueUrl=self.queue_url,
            WaitTimeSeconds=timeout,
            MaxNumberOfMessages=1,
        )
        msgs = resp.get('Messages')
        if not msgs:
            return None
        msg = msgs[0]
        body = json.loads(msg['Body'])
        # Acknowledge
        await self.client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=msg['ReceiptHandle'],
        )
        return body

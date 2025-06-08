import asyncio
import json
from typing import Any, AsyncIterator, Dict, List, Optional

try:
    import aio_pika
    from aio_pika import DeliveryMode, ExchangeType, Message
except ImportError:
    raise ImportError("Please install aio_pika and aio_pika from aio_pika") from None

from asyncmq.backends.base import BaseBackend, RepeatableInfo, WorkerInfo
from asyncmq.backends.redis import RedisBackend
from asyncmq.core.event import event_emitter


class RabbitMQBackend(BaseBackend):
    """
    RabbitMQ-backed queue implementation using aio-pika for message transport,
    delegating stateful operations (delayed, repeatable, stats, workers, dependencies)
    to an internal RedisBackend instance.
    """

    def __init__(
        self,
        rabbit_url: str,
        redis_url: str,
        prefetch_count: int = 1,
        loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        self.rabbit_url = rabbit_url
        self.prefetch_count = prefetch_count
        self.loop = loop or asyncio.get_event_loop()
        self._connection: Optional[aio_pika.RobustConnection] = None
        self._channel: Optional[aio_pika.abc.AbstractChannel] = None
        self._queues: Dict[str, aio_pika.abc.AbstractQueue] = {}
        # Stateful backend for delayed, repeatable, stats, workers, dependencies
        self._state = RedisBackend(redis_url)

    async def _connect(self) -> None:
        """
        Establish connection and channel to RabbitMQ broker if not already open.
        """
        if self._connection and not self._connection.is_closed:
            return
        self._connection = await aio_pika.connect_robust(self.rabbit_url, loop=self.loop)
        self._channel = await self._connection.channel(publisher_confirms=True)
        await self._channel.set_qos(prefetch_count=self.prefetch_count)

    async def enqueue(
        self,
        queue_name: str,
        payload: Dict[str, Any]
    ) -> None:
        """
        Enqueue a job for immediate processing: persist metadata then publish.
        """
        # Persist job in state backend (sets status and payload)
        await self._state.enqueue(queue_name, payload)
        # Publish to RabbitMQ
        await self._connect()
        exchange = await self._channel.declare_exchange(
            name=queue_name,
            type=ExchangeType.DIRECT,
            durable=True
        )
        body = json.dumps(payload).encode()
        message = Message(
            body,
            message_id=str(payload.get("id", "")),
            delivery_mode=DeliveryMode.PERSISTENT
        )
        await exchange.publish(message, routing_key=queue_name)

    async def dequeue(
        self,
        queue_name: str
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Consume messages from the specified queue, yielding job dicts.
        """
        await self._connect()
        if queue_name not in self._queues:
            self._queues[queue_name] = await self._channel.declare_queue(
                name=queue_name,
                durable=True
            )
        async with self._queues[queue_name].iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    payload = json.loads(message.body.decode())
                    yield {
                        'job_id': message.message_id,
                        'payload': payload,
                        'delivery_tag': message.delivery_tag,
                    }

    async def ack(
        self,
        queue_name: str,
        job_id: str
    ) -> None:
        """Acknowledge successful processing of a job."""
        if self._channel:
            await self._channel.basic_client_ack(int(job_id))

    async def move_to_dlq(
        self,
        queue_name: str,
        payload: Dict[str, Any]
    ) -> None:
        """
        Move a job to dead-letter queue: persist state then publish to DLQ.
        """
        await self._state.move_to_dlq(queue_name, payload)
        await self._connect()
        dlq = f"{queue_name}.dlq"
        exchange = await self._channel.declare_exchange(
            name=dlq,
            type=ExchangeType.DIRECT,
            durable=True
        )
        body = json.dumps(payload).encode()
        message = Message(
            body,
            message_id=str(payload.get("id", "")),
            delivery_mode=DeliveryMode.PERSISTENT
        )
        await exchange.publish(message, routing_key=dlq)

    async def enqueue_delayed(
        self,
        queue_name: str,
        payload: Dict[str, Any],
        run_at: float
    ) -> None:
        """Schedule a job for delayed processing via state backend."""
        await self._state.enqueue_delayed(queue_name, payload, run_at)

    async def get_due_delayed(
        self,
        queue_name: str
    ) -> List[Dict[str, Any]]:
        return await self._state.get_due_delayed(queue_name)

    async def remove_delayed(
        self,
        queue_name: str,
        job_id: str
    ) -> None:
        await self._state.remove_delayed(queue_name, job_id)

    async def list_delayed(
        self,
        queue_name: str
    ) -> List[Dict[str, Any]]:
        return await self._state.list_delayed(queue_name)

    async def update_job_state(
        self,
        queue_name: str,
        job_id: str,
        state: str
    ) -> None:
        await self._state.update_job_state(queue_name, job_id, state)

    async def save_job_result(
        self,
        queue_name: str,
        job_id: str,
        result: Any
    ) -> None:
        await self._state.save_job_result(queue_name, job_id, result)

    async def get_job_state(
        self,
        queue_name: str,
        job_id: str
    ) -> Optional[str]:
        return await self._state.get_job_state(queue_name, job_id)

    async def get_job_result(
        self,
        queue_name: str,
        job_id: str
    ) -> Any:
        return await self._state.get_job_result(queue_name, job_id)

    async def add_dependencies(
        self,
        queue_name: str,
        job_dict: Dict[str, Any]
    ) -> None:
        await self._state.add_dependencies(queue_name, job_dict)

    async def resolve_dependency(
        self,
        queue_name: str,
        parent_id: str
    ) -> None:
        await self._state.resolve_dependency(queue_name, parent_id)

    async def pause_queue(self, queue_name: str) -> None:
        await self._state.pause_queue(queue_name)

    async def resume_queue(self, queue_name: str) -> None:
        await self._state.resume_queue(queue_name)

    async def is_queue_paused(self, queue_name: str) -> bool:
        return await self._state.is_queue_paused(queue_name)

    async def reenqueue_stalled(
        self,
        queue_name: str,
        job_data: Dict[str, Any]
    ) -> None:
        await self._state.reenqueue_stalled(queue_name, job_data)

    async def list_repeatables(self, queue_name: str) -> List[RepeatableInfo]:
        return await self._state.list_repeatables(queue_name)

    async def pause_repeatable(
        self,
        queue_name: str,
        job_def: Dict[str, Any]
    ) -> None:
        await self._state.pause_repeatable(queue_name, job_def)

    async def resume_repeatable(
        self,
        queue_name: str,
        job_def: Dict[str, Any]
    ) -> float:
        return await self._state.resume_repeatable(queue_name, job_def)

    async def cancel_job(
        self,
        queue_name: str,
        job_id: str
    ) -> None:
        await self._state.cancel_job(queue_name, job_id)

    async def remove_job(
        self,
        queue_name: str,
        job_id: str
    ) -> None:
        await self._state.remove_job(queue_name, job_id)

    async def retry_job(
        self,
        queue_name: str,
        job_id: str
    ) -> None:
        await self._state.retry_job(queue_name, job_id)

    async def is_job_cancelled(
        self,
        queue_name: str,
        job_id: str
    ) -> bool:
        return await self._state.is_job_cancelled(queue_name, job_id)

    async def list_jobs(
        self,
        queue: str,
        state: str
    ) -> List[Dict[str, Any]]:
        return await self._state.list_jobs(queue, state)

    async def queue_stats(self, queue_name: str) -> Dict[str, Any]:
        return await self._state.queue_stats(queue_name)

    async def list_queues(self) -> List[str]:
        return await self._state.list_queues()

    async def register_worker(
        self,
        worker_id: str,
        queue: str,
        concurrency: int,
        timestamp: float
    ) -> None:
        await self._state.register_worker(worker_id, queue, concurrency, timestamp)

    async def deregister_worker(self, worker_id: str) -> None:
        await self._state.deregister_worker(worker_id)

    async def list_workers(self) -> List[WorkerInfo]:
        return await self._state.list_workers()

    async def fetch_stalled_jobs(self, older_than: float) -> List[Dict[str, Any]]:
        return await self._state.fetch_stalled_jobs(older_than)

    async def bulk_enqueue(self, queue_name: str, jobs: List[Dict[str, Any]]) -> None:
        """Asynchronously enqueue multiple jobs."""
        for job in jobs:
            await self.enqueue(queue_name, job)

    async def create_lock(self, key: str, ttl: int) -> Any:
        """Create a distributed lock via state backend."""
        return await self._state.create_lock(key, ttl)

    async def emit_event(self, event: str, data: Dict[str, Any]) -> None:
        """Emit a lifecycle event."""
        await event_emitter.emit(event, data)

    async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
        """Record last heartbeat timestamp for a running job."""
        await self._state.save_heartbeat(queue_name, job_id, timestamp)

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        """Record job progress percentage."""
        await self._state.save_job_progress(queue_name, job_id, progress)

    async def purge(self, queue_name: str, state: str, older_than: Optional[float] = None) -> None:
        """Remove jobs from state based on state and age."""
        await self._state.purge(queue_name, state, older_than)

    async def close(self) -> None:
        """Cleanly close RabbitMQ connection."""
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
        if self._connection and not self._connection.is_closed:
            await self._connection.close()

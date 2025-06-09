import json
import time
from typing import Any, Optional

try:
    import aio_pika
    from aio_pika import DeliveryMode, Message
    from aio_pika.exceptions import QueueEmpty
except ImportError:
    raise ImportError("Please install aio_pika to use this backend.") from None

from asyncmq.backends.base import BaseBackend, DelayedInfo, RepeatableInfo, WorkerInfo
from asyncmq.core.event import event_emitter
from asyncmq.stores.base import BaseJobStore
from asyncmq.stores.rabbitmq import RabbitMQJobStore


class RabbitMQBackend(BaseBackend):
    """
    RabbitMQ-backed implementation using aio-pika and a BaseJobStore.
    """

    def __init__(
        self,
        rabbit_url: str,
        job_store: Optional[BaseJobStore] = None,
        redis_url: Optional[str] = None,
        prefetch_count: int = 1,
    ):
        self.rabbit_url = rabbit_url
        self.prefetch_count = prefetch_count
        self._state: BaseJobStore = job_store or RabbitMQJobStore(redis_url=redis_url)
        self._conn: Optional[aio_pika.RobustConnection] = None
        self._chan: Optional[aio_pika.abc.AbstractChannel] = None
        self._queues: dict[str, aio_pika.abc.AbstractQueue] = {}

    async def _connect(self) -> None:
        if self._conn and not self._conn.is_closed:
            return
        self._conn = await aio_pika.connect_robust(self.rabbit_url)
        self._chan = await self._conn.channel(publisher_confirms=True)
        await self._chan.set_qos(prefetch_count=self.prefetch_count)

    async def enqueue(self, queue_name: str, payload: dict[str, Any]) -> str:
        job_id = str(payload["id"])
        await self._state.save(queue_name, job_id, {'id': job_id, 'payload': payload, 'status': 'waiting'})
        await self._connect()

        msg = Message(json.dumps(payload).encode(), message_id=job_id, delivery_mode=DeliveryMode.PERSISTENT)
        await self._chan.default_exchange.publish(msg, routing_key=queue_name)
        return job_id

    async def dequeue(self, queue_name: str) -> Optional[dict[str, Any]]:
        await self._connect()
        if queue_name not in self._queues:
            self._queues[queue_name] = await self._chan.declare_queue(queue_name, durable=True)
        try:
            msg = await self._queues[queue_name].get(no_ack=False, fail=False)
        except QueueEmpty:
            return None
        if msg is None:
            return None
        async with msg.process():
            payload = json.loads(msg.body.decode())
            return {'job_id': msg.message_id, 'payload': payload}

    async def ack(self, queue_name: str, job_id: str) -> None:
        """Acknowledge successful processing: update metadata."""
        await self._state.save(queue_name, job_id, {'id': job_id, 'status': 'completed'})

    async def move_to_dlq(self, queue_name: str, payload: dict[str, Any]) -> None:
        job_id = str(payload['id'])
        await self._state.save(queue_name, job_id, {'id': job_id, 'payload': payload, 'status': 'failed'})
        await self._connect()
        dlq = f"{queue_name}.dlq"
        await self._chan.declare_queue(dlq, durable=True)
        msg = Message(json.dumps(payload).encode(), message_id=job_id, delivery_mode=DeliveryMode.PERSISTENT)
        await self._chan.default_exchange.publish(msg, routing_key=dlq)

    async def enqueue_delayed(self, queue_name: str, payload: dict[str, Any], run_at: float) -> None:
        job_id = str(payload['id'])
        await self._state.save(queue_name, job_id, {'id': job_id, 'payload': payload, 'status': 'scheduled', 'run_at': run_at})

    async def get_due_delayed(self, queue_name: str) -> list[DelayedInfo]:
        now = time.time()
        due: list[DelayedInfo] = []
        for j in await self._state.jobs_by_status(queue_name, 'scheduled'):
            if j['run_at'] <= now:
                due.append(DelayedInfo(job_id=j['id'], run_at=j['run_at'], payload=j['payload']))
                await self._state.delete(queue_name, j['id'])
        return due

    async def remove_delayed(self, queue_name: str, job_id: str) -> None:
        await self._state.delete(queue_name, job_id)

    async def list_delayed(self, queue_name: str) -> list[DelayedInfo]:
        return [DelayedInfo(job_id=j['id'], run_at=j['run_at'], payload=j['payload'])
                for j in await self._state.jobs_by_status(queue_name, 'scheduled')]

    async def enqueue_repeatable(self, queue_name: str, payload: dict[str, Any], interval: float, repeat_id: Optional[str] = None) -> str:
        rid = repeat_id or str(payload['id'])
        next_run = time.time() + interval
        # Save metadata with repeatable status
        await self._state.save(queue_name, rid, {
            'id': rid,
            'payload': payload,
            'repeatable': True,
            'interval': interval,
            'next_run': next_run,
            'status': 'repeatable'
        })
        return rid

    async def list_repeatables(self, queue_name: str) -> list[RepeatableInfo]:
        return [RepeatableInfo(job_def=j['payload'], next_run=j['next_run'], paused=j.get('paused', False))
                for j in await self._state.jobs_by_status(queue_name, 'repeatable')]

    async def pause_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> None:
        entry = await self._state.load(queue_name, job_def['id'])
        entry['paused'] = True
        await self._state.save(queue_name, job_def['id'], entry)

    async def resume_repeatable(self, queue_name: str, job_def: dict[str, Any]) -> float:
        entry = await self._state.load(queue_name, job_def['id'])
        entry.pop('paused', None)
        next_run = time.time() + entry['interval']
        entry['next_run'] = next_run
        await self._state.save(queue_name, job_def['id'], entry)
        return next_run

    async def remove_repeatable(self, queue_name: str, repeat_id: str) -> None:
        await self._state.delete(queue_name, repeat_id)

    async def update_job_state(self, queue_name: str, job_id: str, state: str) -> None:
        entry = await self._state.load(queue_name, job_id) or {}
        entry['status'] = state
        await self._state.save(queue_name, job_id, entry)

    async def save_job_result(self, queue_name: str, job_id: str, result: Any) -> None:
        entry = await self._state.load(queue_name, job_id) or {}
        entry['result'] = result
        await self._state.save(queue_name, job_id, entry)

    async def get_job_state(self, queue_name: str, job_id: str) -> Optional[str]:
        entry = await self._state.load(queue_name, job_id)
        return entry.get('status') if entry else None

    async def get_job_result(self, queue_name: str, job_id: str) -> Any:
        entry = await self._state.load(queue_name, job_id)
        return entry.get('result') if entry else None

    async def add_dependencies(self, queue_name: str, job_dict: dict[str, Any]) -> None:
        entry = await self._state.load(queue_name, job_dict['id']) or {}
        entry['depends_on'] = job_dict.get('depends_on', [])
        await self._state.save(queue_name, job_dict['id'], entry)

    async def resolve_dependency(self, queue_name: str, parent_id: str) -> None:
        for j in await self._state.all_jobs(queue_name):
            deps = j.get('depends_on', [])
            if parent_id in deps:
                deps.remove(parent_id)
                j['depends_on'] = deps
                await self._state.save(queue_name, j['id'], j)
                if not deps:
                    await self.enqueue(queue_name, j['payload'])

    async def pause_queue(self, queue_name: str) -> None:
        await self._state.save(queue_name, '_pause', {'paused': True})

    async def resume_queue(self, queue_name: str) -> None:
        await self._state.delete(queue_name, '_pause')

    async def is_queue_paused(self, queue_name: str) -> bool:
        return bool(await self._state.load(queue_name, '_pause'))

    async def save_job_progress(self, queue_name: str, job_id: str, progress: float) -> None:
        entry = await self._state.load(queue_name, job_id) or {}
        entry['progress'] = progress
        await self._state.save(queue_name, job_id, entry)

    async def bulk_enqueue(self, queue_name: str, jobs: list[dict[str, Any]]) -> None:
        for j in jobs:
            await self.enqueue(queue_name, j)

    async def purge(self, queue_name: str, state: str, older_than: Optional[float] = None) -> None:
        for j in await self._state.jobs_by_status(queue_name, state):
            if older_than is None or j.get('timestamp', 0) < older_than:
                await self._state.delete(queue_name, j['id'])

    async def atomic_add_flow(self, queue_name: str, job_dicts: list[dict[str, Any]], dependency_links: list[tuple[str, str]]) -> list[str]:
        child_ids = {child for parent, child in dependency_links}
        created: list[str] = []
        for jd in job_dicts:
            jid = jd['id']
            created.append(jid)
            if jid not in child_ids:
                await self.enqueue(queue_name, jd)
            else:
                # just save metadata
                await self._state.save(queue_name, jid, {'id': jid, 'payload': jd, 'status': 'waiting'})
        for parent, child in dependency_links:
            await self.add_dependencies(queue_name, {'id': child, 'depends_on': [parent]})
        return created

    async def cancel_job(self, queue_name: str, job_id: str) -> bool:
        await self._state.save(queue_name, job_id, {'id': job_id, 'status': 'cancelled'})
        return True

    async def remove_job(self, queue_name: str, job_id: str) -> bool:
        await self._state.delete(queue_name, job_id)
        return True

    async def retry_job(self, queue_name: str, job_id: str) -> bool:
        entry = await self._state.load(queue_name, job_id)
        if not entry:
            return False
        await self.enqueue(queue_name, entry['payload'])
        await self._state.save(queue_name, job_id, {'id': job_id, 'status': 'waiting'})
        return True

    async def is_job_cancelled(self, queue_name: str, job_id: str) -> bool:
        entry = await self._state.load(queue_name, job_id)
        return entry.get('status') == 'cancelled' if entry else False

    async def register_worker(self, worker_id: str, queue: str, concurrency: int, timestamp: float) -> None:
        await self._state.save('workers', worker_id, {'id': worker_id, 'queue': queue, 'concurrency': concurrency, 'heartbeat': timestamp})

    async def deregister_worker(self, worker_id: str) -> None:
        await self._state.delete('workers', worker_id)

    async def list_workers(self) -> list[WorkerInfo]:
        return [WorkerInfo(**w) for w in await self._state.all_jobs('workers')]

    async def queue_stats(self, queue_name: str) -> dict[str, int]:
        await self._connect()
        q = await self._chan.declare_queue(queue_name, durable=True, passive=True)
        return {'message_count': q.declaration_result.message_count}

    async def drain_queue(self, queue_name: str) -> None:
        await self._connect()
        q = await self._chan.declare_queue(queue_name, durable=True)
        await q.purge()

    async def create_lock(self, key: str, ttl: int) -> Any:
        return await self._state.create_lock(key, ttl)

    async def emit_event(self, event: str, data: dict[str, Any]) -> None:
        await event_emitter.emit(event, data)

    async def save_heartbeat(self, queue_name: str, job_id: str, timestamp: float) -> None:
        entry = await self._state.load(queue_name, job_id) or {}
        entry['heartbeat'] = timestamp
        await self._state.save(queue_name, job_id, entry)

    async def fetch_stalled_jobs(self, older_than: float) -> list[dict[str, Any]]:
        stalled: list[dict[str, Any]] = []
        for q_name in await self.list_queues():
            for j in await self._state.all_jobs(q_name):
                if j.get('heartbeat', 0) < older_than:
                    stalled.append({'queue': q_name, 'job_data': j})
        return stalled

    async def reenqueue_stalled(self, queue_name: str, job_data: dict[str, Any]) -> None:
        await self.enqueue(queue_name, job_data['payload'])

    async def list_jobs(self, queue_name: str, state: str) -> list[dict[str, Any]]:
        return await self._state.jobs_by_status(queue_name, state)

    async def list_queues(self) -> list[str]:
        return await self._state.list_queues()

    async def close(self) -> None:
        if self._chan and not self._chan.is_closed:
            await self._chan.close()
        if self._conn and not self._conn.is_closed:
            await self._conn.close()

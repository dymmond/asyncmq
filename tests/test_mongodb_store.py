import pytest

from asyncmq.stores.mongodb import MongoDBStore

pytestmark = pytest.mark.anyio


class _DummyCollection:
    def __init__(self) -> None:
        self.last_filter = None
        self.last_update = None
        self.last_upsert = None

    async def update_one(self, flt, update, upsert=False):  # type: ignore[no-untyped-def]
        self.last_filter = flt
        self.last_update = update
        self.last_upsert = upsert


async def test_save_strips_mongo_id_from_set_payload():
    store = MongoDBStore.__new__(MongoDBStore)
    store.collection = _DummyCollection()

    payload = {"_id": "mongo-id", "status": "waiting", "task_id": "t"}
    await store.save("queue", "job-1", payload)

    assert payload["_id"] == "mongo-id"
    assert store.collection.last_filter == {"queue_name": "queue", "job_id": "job-1"}
    assert store.collection.last_upsert is True
    assert "_id" not in store.collection.last_update["$set"]
    assert store.collection.last_update["$set"]["queue_name"] == "queue"
    assert store.collection.last_update["$set"]["job_id"] == "job-1"

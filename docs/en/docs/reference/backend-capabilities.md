# Backend Capabilities

This page summarizes practical capability expectations across built-in backends.

## Shared Contract

All built-in backends implement `BaseBackend` methods used by:

- enqueue/dequeue/ack
- delayed jobs
- retries/DLQ support
- queue pause/resume
- dependency resolution
- worker registry and stalled-job support methods

## Important Differences

`InMemoryBackend`:

- no durable storage across process restarts
- queue pause/resume is process-local
- lock implementation is in-process only
- repeatable schedules are process-local by design
- deduplication and scheduler ownership coordinate only inside one process

`RedisBackend`:

- default production-oriented backend
- queue pause/resume state is stored in Redis and shared by backend instances
- distributed lock via Redis lock primitives
- durable repeatable schedules stored in Redis
- deduplication and scheduler ownership are coordinated across producers/workers

`PostgresBackend`:

- SQL persistence and advisory-lock-based lock helper
- requires schema bootstrap
- queue pause/resume state is stored in PostgreSQL and shared by backend instances
- durable repeatable schedules stored in the repeatables table
- deduplication and scheduler ownership are coordinated across producers/workers

`MongoDBBackend`:

- document persistence
- waiting and delayed job acquisition is shared through MongoDB documents
- queue pause/resume state is stored in MongoDB and shared by backend instances
- cancellation markers are stored in MongoDB and shared by backend instances
- lock helper is in-process (`anyio.Lock`), not distributed
- durable repeatable schedules stored as `status="repeatable"` documents
- active-job heartbeats are persisted in job documents for stalled recovery
- deduplication and scheduler ownership coordinate only inside one process

`RabbitMQBackend`:

- broker delivery handled by RabbitMQ
- metadata/state persistence delegated to a job store (Redis-backed by default)
- queue pause/resume state is stored in the metadata store and shared by backend instances
- durable repeatable schedules stored in the metadata job store
- stalled recovery discovers queues through the metadata store, publishes
  tokenized replacement deliveries, and ignores stale broker redeliveries after
  recovery
- coordination quality depends on the metadata store lock implementation

## Choosing by Requirement

1. Need simplest durable queue backend: Redis.
2. Need SQL-native persistence: Postgres.
3. Need Mongo-native persistence: MongoDB.
4. Need AMQP broker integration: RabbitMQ.
5. Need lightweight local tests: InMemory.

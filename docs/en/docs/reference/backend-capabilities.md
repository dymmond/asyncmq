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
- lock implementation is in-process only

`RedisBackend`:

- default production-oriented backend
- distributed lock via Redis lock primitives

`PostgresBackend`:

- SQL persistence and advisory-lock-based lock helper
- requires schema bootstrap

`MongoDBBackend`:

- document persistence
- lock helper is in-process (`anyio.Lock`), not distributed

`RabbitMQBackend`:

- broker delivery handled by RabbitMQ
- metadata/state persistence delegated to a job store (Redis-backed by default)

## Choosing by Requirement

1. Need simplest durable queue backend: Redis.
2. Need SQL-native persistence: Postgres.
3. Need Mongo-native persistence: MongoDB.
4. Need AMQP broker integration: RabbitMQ.
5. Need lightweight local tests: InMemory.

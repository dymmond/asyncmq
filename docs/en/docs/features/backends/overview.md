# Backend Overview

AsyncMQ backend implementations all satisfy `BaseBackend`, but operational characteristics differ.

## Available Backends

- `InMemoryBackend` (`asyncmq.backends.memory.InMemoryBackend`)
- `RedisBackend` (`asyncmq.backends.redis.RedisBackend`)
- `PostgresBackend` (`asyncmq.backends.postgres.PostgresBackend`)
- `MongoDBBackend` (`asyncmq.backends.mongodb.MongoDBBackend`)
- `RabbitMQBackend` (`asyncmq.backends.rabbitmq.RabbitMQBackend`)

## Capability Notes

In-memory:

- best for tests/local development
- no cross-process durability

Redis:

- default backend in settings
- strong feature coverage and good operational fit for queue workloads
- worker lifecycle transitions use Redis-side scripts for active-job
  completion, retry/defer, expiration, cancellation, and DLQ routing

Postgres:

- durable SQL-backed queue state
- suitable when you prefer SQL operational tooling and transactions
- waiting jobs are dequeued by priority, then FIFO within the same priority
- worker lifecycle transitions are written through database transactions so
  retry/defer rows are preserved and terminal states remain inspectable

MongoDB:

- durable document-backed queue state
- good fit when your stack is already Mongo-centric
- process-local waiting queues are dequeued by priority, then FIFO within the
  same priority
- worker lifecycle transitions update the MongoDB job document and process-local
  runtime mirrors through one backend-owned path

RabbitMQ:

- AMQP broker for delivery + separate job metadata store
- by default, metadata store is Redis-backed (`RabbitMQJobStore`)
- worker lifecycle transitions are backend-owned and persist metadata before
  acknowledging broker deliveries; RabbitMQ broker state and metadata storage
  are not a single distributed transaction
- broker-level priority requires a priority-enabled RabbitMQ queue declaration
  and is not normalized by AsyncMQ

## Selection Guidance

Choose based on operational model first:

1. Need simplest production path for queue semantics: use Redis.
2. Need SQL-native persistence and operations: use Postgres.
3. Need document-store alignment: use MongoDB.
4. Need AMQP integration with existing RabbitMQ estate: use RabbitMQ backend.
5. Need zero-dependency local tests: use InMemory.

## Per-Backend Details

- [Postgres Backend](postgres-backend.md)
- [RabbitMQ Backend](rabbitmq.md)
- [Backend Capability Reference](../../reference/backend-capabilities.md)

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

Postgres:

- durable SQL-backed queue state
- suitable when you prefer SQL operational tooling and transactions

MongoDB:

- durable document-backed queue state
- good fit when your stack is already Mongo-centric

RabbitMQ:

- AMQP broker for delivery + separate job metadata store
- by default, metadata store is Redis-backed (`RabbitMQJobStore`)

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

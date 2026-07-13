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
- cancellation updates canonical in-process state and late lifecycle writes do
  not overwrite cancellation

Redis:

- default backend in settings
- strong feature coverage and good operational fit for queue workloads
- due delayed jobs are promoted to waiting through a Redis-side script
- worker lifecycle transitions use Redis-side scripts for active-job
  completion, retry/defer, expiration, cancellation, and DLQ routing
- cancellation markers are stored in Redis and respected by lifecycle scripts
  so late completions cannot overwrite cancelled jobs; dequeue also suppresses
  stale cancelled waiting members, and removal clears markers even when only
  cancellation state remains

Postgres:

- durable SQL-backed queue state
- suitable when you prefer SQL operational tooling and transactions
- waiting jobs are dequeued by priority, then FIFO within the same priority
- due delayed jobs are promoted to waiting in one SQL update
- worker lifecycle transitions are written through database transactions so
  retry/defer rows are preserved and terminal states remain inspectable
- cancellation markers are stored in PostgreSQL; waiting and delayed rows are
  removed, active rows are marked cancelled, and late lifecycle writes do not
  overwrite cancellation; removing a cancelled job also clears its cancellation
  marker

MongoDB:

- durable document-backed queue state
- good fit when your stack is already Mongo-centric
- waiting jobs are claimed from MongoDB by priority, then FIFO within the same
  priority, so separate producer and worker processes share the queue
- due delayed jobs are promoted to waiting through MongoDB document updates
- worker lifecycle transitions update the MongoDB job document and local runtime
  mirrors through one backend-owned path
- cancellation markers are stored in MongoDB so separate backend instances stop
  claiming cancelled waiting, delayed, or active jobs; removal clears markers
  even when only cancellation state remains

RabbitMQ:

- AMQP broker for delivery + separate job metadata store
- by default, metadata store is Redis-backed (`RabbitMQJobStore`)
- waiting jobs are dequeued by RabbitMQ broker priority, then FIFO within the
  same priority; AsyncMQ maps lower numeric priorities to RabbitMQ's higher
  AMQP priority values
- worker lifecycle transitions are backend-owned and persist metadata before
  acknowledging broker deliveries; RabbitMQ broker state and metadata storage
  are not a single distributed transaction
- cancellation updates metadata, removes ready broker deliveries, acknowledges
  locally owned active deliveries, and prevents late lifecycle writes from
  overwriting cancellation
- stalled recovery publishes a tokenized replacement delivery when the original
  broker delivery is still owned by another connection, and stale broker
  redeliveries are acknowledged and ignored after recovery
- queue draining follows the shared backend contract by removing queued
  metadata, purging ready broker messages, honoring `include_delayed`, and
  returning removed job ids
- retrying failed jobs removes the matching ready DLQ broker delivery before
  publishing the retry to the main queue
- removing a job records a removed-job marker, deletes metadata, acknowledges a
  locally owned delivery, removes matching ready deliveries from the main queue
  and DLQ, and ignores later stale broker redeliveries for that removed job
- due delayed jobs are promoted by the backend without deleting delayed
  metadata before broker publish; broker publish and metadata update are still
  not a single distributed transaction
- queue declarations enable RabbitMQ priority queues by default; existing
  non-priority RabbitMQ queues must be recreated before switching a queue to
  priority mode because RabbitMQ queue arguments are immutable

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

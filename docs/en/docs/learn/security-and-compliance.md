# Security and Compliance

AsyncMQ provides runtime primitives; secure operation is your responsibility.

## Core Practices

- run backends on private networks
- enforce TLS where supported
- use least-privilege credentials for workers/producers
- avoid putting secrets or sensitive raw PII in job payloads
- enforce idempotency and authorization in producer endpoints

## Dashboard Security

- enable authentication (`AsyncMQAdmin(enable_login=True, backend=...)`)
- use strong session/JWT secrets
- restrict dashboard exposure by network controls and identity provider layers

## Logging and Data Handling

- do not log secrets from job args/kwargs
- control retention of completed/failed jobs (`purge` / cleanup jobs)
- ensure storage encryption requirements are met by your backend deployment

## Compliance Notes

For GDPR/HIPAA/PCI-like requirements:

- keep payloads minimal (prefer references over full records)
- define clear retention and deletion workflows
- audit who can enqueue, inspect, retry, and delete jobs

AsyncMQ can be part of compliant architectures, but compliance is system-level, not library-only.

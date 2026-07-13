# Security and Compliance

AsyncMQ provides runtime primitives; secure operation is your responsibility.

## Core Practices

- run backends on private networks
- enforce TLS where supported
- use least-privilege credentials for workers/producers
- avoid putting secrets or sensitive raw PII in job payloads
- enforce idempotency and authorization in producer endpoints
- keep the default `max_job_payload_bytes` guard enabled unless producers are
  independently authenticated, rate-limited, and bounded upstream

## Dashboard Security

- enable authentication (`AsyncMQAdmin(enable_login=True, backend=...)`)
- use strong session/JWT secrets
- restrict dashboard exposure by network controls and identity provider layers
- keep dashboard CORS same-origin unless an exact operator origin is required
- never combine wildcard CORS origins with credentialed dashboard sessions
- leave same-origin mutation checks enabled unless an upstream gateway enforces
  equivalent CSRF protections
- authenticated dashboard mutations require an explicit same-origin `Origin`
  header by default

## Logging and Data Handling

- do not log secrets from job args/kwargs
- control retention of completed/failed jobs (`purge` / cleanup jobs)
- ensure storage encryption requirements are met by your backend deployment
- prefer references to large objects instead of embedding bulk data in job
  payloads; public `Queue` and task enqueue APIs reject JSON-encoded payloads
  above `max_job_payload_bytes`

## Compliance Notes

For GDPR/HIPAA/PCI-like requirements:

- keep payloads minimal (prefer references over full records)
- define clear retention and deletion workflows
- audit who can enqueue, inspect, retry, and delete jobs

AsyncMQ can be part of compliant architectures, but compliance is system-level, not library-only.

# Dashboard Capabilities

This page maps current AsyncMQ dashboard behavior against common Flower-style operational expectations.

## Capability Matrix

| Capability | Status | Notes |
| --- | --- | --- |
| Queue visibility (waiting/active/delayed/failed/completed) | Supported | Queue and overview pages with live updates |
| Worker list and heartbeat visibility | Supported | `/workers` with heartbeat-based listing |
| Queue pause/resume control | Supported | Queue overview and detail pages |
| Job listing by state | Supported | Queue jobs page (`state` filtering) |
| Job retry/remove/cancel controls | Supported | Bulk and single-action flows |
| DLQ inspection and retry/remove | Supported | Dedicated DLQ page per queue |
| Repeatable definition pause/resume/remove | Supported | Repeatables page and actions |
| Live dashboard updates | Supported | SSE-driven updates (`/events`) |
| Historical execution analytics | Partial | Live metrics available; long-term history/analytics are backend-dependent and limited |
| Remote worker process control (start/stop/scale) | Not available | Use CLI/process supervisor/orchestrator |
| Multi-cluster federation view | Not available | One configured backend context per deployment |
| Role-based authorization model | Partial | Pluggable auth backend; fine-grained RBAC is app-defined |

## What This Means Operationally

The dashboard is production-usable for queue triage and job recovery workflows, but it is not yet a full process-control plane for distributed worker orchestration.

For process lifecycle operations, use:
- CLI (`asyncmq worker ...`)
- your orchestrator (systemd, Kubernetes, Nomad, etc.)

# AsyncMQ Repository Audit Plan

## Phase 1 - Discovery
- [x] Read core package layout, queue/worker lifecycle, and backend implementations.
- [x] Read dashboard controllers, templates, auth integration, and static assets.
- [x] Read tests (unit, integration, CLI, dashboard) to identify coverage gaps.
- [x] Read documentation set and navigation structure.

## Phase 2 - Gap Analysis
- [x] Compare docs claims against implemented behavior.
- [x] Identify correctness/safety issues in queue/job/backends/workers.
- [x] Identify dashboard gaps relative to Flower-style operational workflows.
- [x] Identify test blind spots and brittle patterns.

## Phase 3 - Implementation Fixes
- [x] Fix backend/worker correctness issues discovered during audit.
- [x] Migrate CLI implementation to Sayer while preserving command surface/outputs.
- [x] Improve queue/controller safety and fallback behavior.
- [x] Add dashboard action audit trail (queue/job/dlq/repeatable actions).
- [x] Add richer dashboard job filtering/search.
- [x] Add dashboard metrics history storage and `/metrics/history` endpoint.

## Phase 4 - Test Expansion
- [x] Add regression tests for fixed backend/worker/CLI issues.
- [x] Add dashboard controller tests for filtering, audit events, and metrics history endpoint.
- [x] Add unit tests for dashboard audit/history stores.
- [x] Run and stabilize full relevant test matrix (including optional backend-dependent suites).

## Phase 5 - Documentation Overhaul
- [x] Rewrite and expand core docs, how-to guides, and reference pages.
- [x] Improve `docs/en/docs/index.md` as the main project entrypoint.
- [x] Add dashboard capability and operations docs.
- [x] Expand dashboard docs with deeper visuals/examples for filtering, audit, and metrics history.
- [x] Validate internal links and cross-references after latest dashboard changes.

## Phase 6 - Validation
- [x] Run type checks, targeted tests, and docs build after latest changes.
- [x] Ensure release notes include all material updates for `0.7.0`.
- [x] Produce final audit summary with fixed issues, tests, docs updates, and remaining concerns.

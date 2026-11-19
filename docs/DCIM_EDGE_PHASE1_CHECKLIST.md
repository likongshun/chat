# DCIM Edge Engine Phase 1 — Work Breakdown and Acceptance

This checklist is the execution ledger for `docs/DCIM_EDGE_PHASE1_ARCHITECTURE.md`.

## A. Baseline protection
- [ ] Confirm the phase branch is based on the latest reviewed main commit.
- [ ] Run `gofmt`, `go vet ./...`, `go test ./...`, and `go test -race ./...` before changing production code.
- [ ] Capture current API response examples and Prometheus metric names.
- [ ] Record existing WAL and configuration compatibility tests; no on-disk format changes without migration tests.

## B. Canonical model
- [x] Inventory existing config.Device, config.Point, collector sample/result, latest-store and WAL record types.
- [x] Define canonical protocol, quality and reason enums with stable serialized values.
- [x] Add adapters from existing config and poller results; avoid duplicating device configuration.
- [x] Add model and adapter contract tests for Modbus/DL-T645 sample shapes (CI execution pending).
- [ ] Verify config-version provenance survives WAL replay and downstream delivery.

## C. Communication statistics
- [x] Extend atomic device counters for attempts, success, failure, timeout and reconnect fields; reconnect event sourcing from persistent protocol clients remains a follow-up.
- [x] Track last attempt/success/error and consecutive failures.
- [x] Add fixed latency buckets and max/total latency counters.
- [x] Expose fleet/protocol counters with bounded protocol labels (CI execution pending).
- [x] Add per-device communication diagnostics and derived online/offline/unknown status endpoints with tests.
- [ ] Verify counters remain race-free under concurrent poll/reload/shutdown.

## D. Point quality
- [x] Normalize latest-point quality states and stable reason codes.
- [x] Keep last value/last-success timestamp when a poll fails or decode fails.
- [x] Implement stale evaluation from expected poll interval and configured multiplier; runtime monitor reevaluates on a 5-second cadence.
- [ ] Ensure non-finite numeric values cannot be GOOD.
- [x] Add bounded latest quality counts and expose quality gauges; per-point diagnostics include quality reason.
- [ ] Test timeout, malformed frame, CRC error, decode error, out-of-range, stale recovery and configuration changes.

## E. Query and aggregation
- [ ] Specify API request/response types and error envelope.
- [ ] Validate device/point ownership against an immutable config snapshot.
- [ ] Implement latest-state query with bounded result count.
- [x] Implement time-bucket aggregate functions min/max/mean/count/last with bounded InfluxDB Flux queries.
- [x] Explicitly filter historical aggregates to GOOD samples; expose the quality policy in the API response. Historical per-bucket quality counts remain pending.
- [x] Bound time range (7 days), step (1 second–24 hours), bucket count (1,000) and response bytes (4 MiB). Existing raw-history endpoint also bounds rows and offsets.
- [x] Existing raw-history endpoint returns explicit dependency errors; aggregate endpoint has explicit unavailable/error responses and no latest-value fallback.

## F. Mixed-protocol load and soak
- [ ] Implement wire-level deterministic Modbus TCP simulator (the new CLI currently simulates the scheduler/router path).
- [ ] Implement wire-level deterministic DL/T645 transport simulator including valid multi-frame and malformed-frame cases (the new CLI currently simulates the scheduler/router path).
- [x] Add smoke, 100-device, 1,000-device and opt-in 5,000-device scheduler/router simulation profiles in cmd/loadtest; wire-level simulators remain pending.
- [x] Emit machine-readable JSON summary with runtime/config parameters; host/hardware details must be recorded alongside the run.
- [ ] Measure throughput, latency percentiles, CPU, RSS, goroutines, FDs, queue depth, WAL backlog, ACK lag and quality distribution.
- [ ] Run race tests and a multi-hour soak before any production capacity statement.

## G. Release gates
- [ ] All unit and integration tests pass.
- [ ] No race detector findings.
- [ ] No unbounded queues, goroutines, caches or metric cardinality.
- [ ] WAL recovery and independent sink ACK invariants remain intact.
- [ ] Compose config validates and container image builds.
- [ ] Physical device interoperability and deployment-filesystem fault tests are recorded separately.
- [ ] Update `docs/IMPLEMENTATION_STATUS.md` only with features actually implemented and verified.

## Current state

Architecture, canonical telemetry types, legacy adapters, quality/staleness primitives, runtime stale evaluator, last-value preservation, stable quality reason codes, quality count metrics, atomic per-device/protocol communication counters, scheduler instrumentation, fleet Prometheus metrics, and the per-device communication diagnostics endpoint have been added on the phase branch. Automated CI validation is running; historical persistence of quality transitions and aggregate historical query APIs remain pending. No release-readiness claim is made.

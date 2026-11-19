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
- [x] Extend atomic device counters for attempts, success, failure, timeout and reconnect.
- [x] Track last attempt/success/error and consecutive failures.
- [x] Add fixed latency buckets and max/total latency counters.
- [x] Expose fleet/protocol counters with bounded protocol labels (CI execution pending).
- [x] Add per-device communication diagnostics endpoint and tests (CI execution pending).
- [ ] Verify counters remain race-free under concurrent poll/reload/shutdown.

## D. Point quality
- [ ] Normalize quality strings and reason codes.
- [ ] Keep last value/last-good timestamp when a poll fails.
- [ ] Implement stale evaluation from expected poll interval and configured multiplier.
- [ ] Ensure non-finite numeric values cannot be GOOD.
- [ ] Add quality transition counters and point diagnostics.
- [ ] Test timeout, malformed frame, CRC error, decode error, out-of-range, stale recovery and configuration changes.

## E. Query and aggregation
- [ ] Specify API request/response types and error envelope.
- [ ] Validate device/point ownership against an immutable config snapshot.
- [ ] Implement latest-state query with bounded result count.
- [ ] Implement time-bucket aggregate functions min/max/mean/count/last.
- [ ] Preserve quality counts and exclude BAD/STALE from valid numeric aggregates.
- [ ] Bound time range, step, points, rows, query timeout and response bytes.
- [ ] Test storage unavailable behavior and ensure no fake historical fallback.

## F. Mixed-protocol load and soak
- [ ] Implement deterministic Modbus TCP simulator.
- [ ] Implement deterministic DL/T645 transport simulator including valid multi-frame and malformed-frame cases.
- [ ] Add smoke, 100-device, 1,000-device and opt-in 5,000-device profiles.
- [ ] Emit machine-readable benchmark summary with hardware/runtime/config parameters.
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

Architecture, canonical telemetry types, legacy adapters, quality/staleness primitives, atomic per-device/protocol communication counters, scheduler instrumentation, fleet Prometheus metrics, and the per-device communication diagnostics endpoint have been added on the phase branch. Automated CI execution and integration with persisted point-quality transitions remain pending; no release-readiness claim is made.

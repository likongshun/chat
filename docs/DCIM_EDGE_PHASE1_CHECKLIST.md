# DCIM Edge Engine Phase 1 — Work Breakdown and Acceptance

This checklist is the execution ledger for `docs/DCIM_EDGE_PHASE1_ARCHITECTURE.md`.

## A. Baseline protection
- [ ] Confirm the phase branch is based on the latest reviewed main commit.
- [ ] Run `gofmt`, `go vet ./...`, `go test ./...`, and `go test -race ./...` before changing production code.
- [ ] Capture current API response examples and Prometheus metric names.
- [ ] Record existing WAL and configuration compatibility tests; no on-disk format changes without migration tests.

## B. Canonical model
- [ ] Inventory existing config.Device, config.Point, collector sample/result, latest-store and WAL record types.
- [ ] Define canonical protocol, quality and reason enums with stable serialized values.
- [ ] Add adapters from existing config and poller results; avoid duplicating device configuration.
- [ ] Add model contract tests for Modbus and DL/T645 samples.
- [ ] Verify config-version provenance survives WAL replay and downstream delivery.

## C. Communication statistics
- [ ] Extend atomic device counters for attempts, success, failure, timeout and reconnect.
- [ ] Track last attempt/success/error and consecutive failures.
- [ ] Add bounded latency histogram or fixed buckets.
- [ ] Expose fleet/protocol counters with bounded Prometheus labels.
- [ ] Add bounded device diagnostic endpoint and tests.
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

Architecture and acceptance contract have been documented. This checklist does not claim that the implementation items above are complete.

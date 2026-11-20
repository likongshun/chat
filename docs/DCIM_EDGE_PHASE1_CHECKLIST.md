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
- [x] Extend atomic device counters for attempts, success, failure, timeout and reconnect fields; persistent Modbus TCP client now emits reconnect events only after a previously connected client successfully redials, wired into per-device fleet statistics.
- [x] Track last attempt/success/error and consecutive failures.
- [x] Add fixed latency buckets and max/total latency counters.
- [x] Expose fleet/protocol counters with bounded protocol labels (CI execution pending).
- [x] Add per-device communication diagnostics and derived online/offline/unknown status endpoints with tests.
- [x] Add focused tests for reconnect callback semantics and reconnect counters remaining independent of poll-attempt counts.
- [ ] Verify counters remain race-free under concurrent poll/reload/shutdown.

## D. Point quality
- [x] Normalize latest-point quality states and stable reason codes.
- [x] Keep last value/last-success timestamp when a poll fails or decode fails.
- [x] Implement stale evaluation from expected poll interval and configured multiplier; runtime monitor reevaluates on a 5-second cadence.
- [ ] Ensure non-finite numeric values cannot be GOOD.
- [x] Add bounded latest quality counts and expose quality gauges; per-point diagnostics include quality reason.
- [ ] Test timeout, malformed frame, CRC error, decode error, out-of-range, stale recovery and configuration changes.
- [x] Expand the built-in DI catalog with demand, power, power-factor, phase-angle and meter date/time/rate parameter examples; templates explicitly flag composite demand/time payloads for dedicated decoding.
- [x] Add format-driven BCD/date/time/ASCII decoding with strict digit, calendar and byte-length validation; load data_format from the Points workbook sheet and validate before config activation.
- [x] Route DL/T645 Poller through typed decoding when data_format is configured; preserve legacy BCD decoding when omitted.
- [x] Add composite maximum-demand decoding into numeric value plus occurrence timestamp.
- [x] Separate decimal precision in data_format from the optional calibration multiplier/offset; standard templates use scale=1 when precision is already encoded in the format.
- [x] Add DL/T645-2007 Appendix A.6 scheduled-freeze DI templates for previous scheduled-freeze timestamp, forward active energy and forward/reverse maximum demand with occurrence time; keep meter freeze-mode/model compatibility explicitly documented.
- [x] Add and test DL/T645-2007 0x11 load-record request encoding for block-count and meter-local start-time + block-count forms; document the 0x12 continuation exchange and keep record-layout decoding as a model-specific follow-up.

## E. Query and aggregation
- [ ] Specify API request/response types and error envelope.
- [ ] Validate device/point ownership against an immutable config snapshot.
- [ ] Implement latest-state query with bounded result count.
- [x] Implement time-bucket aggregate functions min/max/mean/count/last with bounded InfluxDB Flux queries.
- [x] Explicitly filter historical aggregates to GOOD samples; expose the quality policy in the API response. Historical per-bucket quality counts remain pending.
- [x] Bound time range (7 days), step (1 second–24 hours), bucket count (1,000) and response bytes (4 MiB). Existing raw-history endpoint also bounds rows and offsets.
- [x] Existing raw-history endpoint returns explicit dependency errors; aggregate endpoint has explicit unavailable/error responses and no latest-value fallback.

## F. Mixed-protocol load and soak
- [x] Add deterministic wire-level Modbus TCP peer tests for valid requests, exception responses, malformed MBAP headers, transaction mismatch and silent-peer timeout; reusable production simulator CLI remains a follow-up.
- [x] Add a real TCP-to-RS485 gateway simulator test for DL/T645 read requests, valid continuation frames and checksum corruption; serial/PTY fault profiles and reusable simulator CLI remain follow-ups.
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

Architecture, canonical telemetry types, legacy adapters, quality/staleness primitives, runtime stale evaluator, last-value preservation, stable quality reason codes, quality count metrics, atomic per-device/protocol communication counters, scheduler instrumentation, fleet Prometheus metrics, per-device communication diagnostics/status endpoints, first wire-level Modbus TCP/DL/T645 simulator tests, persistent Modbus TCP reconnect event sourcing, expanded DL/T645 DI templates, format-driven BCD/date/time/ASCII decoding wired through workbook configuration and the DL/T645 Poller (including composite maximum-demand timestamps), and a validated protocol-layer DL/T645-2007 Appendix A.7 load-record request encoder with real-TCP continuation coverage have been added on the phase branch. Wire-level tests exercise real localhost TCP sockets; physical serial/PTY interoperability, historical quality-transition persistence, historical per-bucket quality counts, full sink/WAL soak, and target-hardware capacity measurements remain pending. No release-readiness claim is made.
\n\n## DL/T645 reference\n\nThe DL/T645 extension uses the public Go implementation at https://github.com/600888/dlt645 as a capability and data-dictionary reference (Apache-2.0). Its layered separation of protocol, model, conversion and transport informed this work. The existing collector-owned frame parser and transport abstraction remain authoritative; upstream code is not vendored. Data identifier lengths, scale factors and manufacturer support must be verified against the actual meter model before deployment.\n
# DCIM Edge Engine — Phase 1 Architecture

Status: implementation baseline and acceptance contract  
Target branch: `phase1/dcim-edge-foundation`

## 1. Scope

Evolve the existing Go collector in place into the edge acquisition platform for DCIM. Do not replace the existing configuration loader, Modbus/DL-T645 pollers, scheduler, WAL, independent sink ACKs, PostgreSQL configuration version store, or Compose deployment.

Phase 1 delivers:
1. A canonical device/point/sample identity model.
2. Per-device and per-protocol communication statistics.
3. Point quality evaluation independent of transport success.
4. Bounded aggregation/query APIs over latest state and historical storage.
5. Repeatable mixed-protocol load and soak tests.
6. Operational metrics and health/readiness semantics.

The first implementation remains a modular Go application. Split services only when deployment scale or fault isolation demonstrates a need.

## 2. Existing baseline and constraints

The current repository already contains:
- Excel workbook configuration with Devices, Points, Processing, WriteRules and Enums.
- Immutable PostgreSQL configuration versions and active-version polling.
- Modbus TCP read functions FC01–FC04, batching and serialized per-device clients.
- DL/T645-2007 read support, multi-frame assembly, address discovery and TCP/serial transports.
- A bounded scheduler, per-device backoff, global in-flight cap and graceful shutdown.
- Latest-value cache, WAL persistence, independent MQTT/InfluxDB ACKs and Prometheus observability.
- Docker Compose deployment and historical InfluxDB query endpoint.

Safety constraints:
- Read-only remains the default and only supported execution mode in this phase.
- Do not add Modbus/DL-T645 write operations, arbitrary address scans, or automatic write retries.
- DL/T645 discovery remains explicit, point-to-point and read-only; isolate a single meter on the RS485 segment.
- A sample's transport result, data quality, WAL durability and downstream delivery ACK are distinct states.
- Never report production durability or a 5,000-device capacity claim without target-hardware/filesystem and sustained-load evidence.

## 3. Canonical information model

### 3.1 Identity hierarchy

`Site -> Building -> Room -> Row -> Rack -> Device -> Point -> Sample`

Spatial identifiers are optional at the edge. The collector must work when only site/device/point IDs are configured. The central DCIM asset service can enrich these fields later.

### 3.2 Device identity

A device identity is stable across protocol reconnects and configuration reloads:
- `device_id`: globally unique within the tenant/site namespace.
- `protocol`: canonical protocol ID (for example `modbus-tcp`, `dl-t645-2007`).
- `model`: versioned model/template reference.
- `config_version`: immutable configuration snapshot version.
- `enabled`, `discovery_only`: operational eligibility flags.

Connection/session identifiers and network addresses are attributes, not identity.

### 3.3 Point identity and metadata

A point is identified by `(device_id, point_code)`; its model-level definition is resolved from the active configuration snapshot. Metadata includes data type, engineering unit, scale/offset, read group, source register or DL/T645 DI, expected polling interval, precision and enabled state.

### 3.4 Sample envelope

Every normalized sample must carry:
- `device_id`, `point_id`, `protocol`, `config_version`
- typed value (numeric, boolean, string or enum representation)
- `unit`, `quality`, `quality_reason`
- `observed_at` (device/source timestamp when trustworthy)
- `collected_at` (edge acquisition time)
- `received_at` (central ingestion time, assigned centrally)
- monotonically increasing edge `sequence` where applicable
- optional `raw_value`/diagnostic metadata subject to bounded size

Use UTC timestamps internally. Preserve source timestamps separately from edge collection timestamps. Do not silently convert a missing timestamp to a plausible device time.

## 4. Quality model

Quality is independent of communication status and storage/delivery state.

Canonical quality values:
- `GOOD`: decoded, validated value within representable and configured validity constraints.
- `UNCERTAIN`: value exists but has a known limitation (for example stale source timestamp, substituted/estimated value, or configured warning range).
- `BAD`: no trustworthy value due to timeout, protocol/CRC/decode error, invalid/non-finite value, or device-reported failure.
- `STALE`: previously GOOD/UNCERTAIN value exceeded its freshness deadline without a newer valid sample.

Each non-GOOD state must include a stable reason code, such as `TIMEOUT`, `CRC_ERROR`, `PROTOCOL_ERROR`, `DECODE_ERROR`, `OUT_OF_RANGE`, `NON_FINITE`, `STALE_TIMEOUT`, `DEVICE_ERROR`.

Rules:
- A failed poll updates communication counters and point quality; it must not overwrite the last known value with zero.
- Preserve last value and last-good timestamp separately from current quality.
- Staleness is calculated from the point's configured expected interval and a configurable multiplier; it must not depend on a browser refresh.
- Quality transitions are timestamped and counted.
- Unknown/unsupported quality reasons fail closed to `BAD/UNKNOWN_ERROR`, not `GOOD`.

## 5. Communication statistics

Track monotonic counters and bounded rolling-window measurements.

### Device dimensions
- poll attempts, successes, failures, timeouts
- consecutive failures and current backoff
- last attempt, last success, last error time and error code
- latency count/sum/histogram (p50/p95/p99 derived from histogram)
- current in-flight requests and reconnect count
- points expected, points returned, points GOOD/BAD/UNCERTAIN/STALE

### Protocol dimensions
- request/response/error counts
- timeout and decode/CRC/protocol error counts
- bytes/frames read and written (read-only commands only)
- active sessions, reconnects and request latency

### Fleet dimensions
- configured/enabled/collecting/online/offline devices
- sample rate and queue/WAL backlog
- dropped/rejected samples (must remain zero under accepted steady-state conditions)
- per-sink pending and ACK lag

Avoid high-cardinality Prometheus labels. Device-level details belong in bounded diagnostic/API queries, not unbounded metric labels. Prometheus labels should be limited to protocol, result, reason, and other bounded enums.

Online status is derived from recent successful communication against a configurable threshold; it is not equivalent to a point having GOOD quality.

## 6. Aggregation and query contract

Expose a versioned read-only API. Initial endpoints:
- `GET /api/v1/devices/{device_id}/status`
- `GET /api/v1/devices/{device_id}/communication`
- `GET /api/v1/devices/{device_id}/points`
- `GET /api/v1/telemetry/latest?device_id=&point_id=`
- `GET /api/v1/telemetry/aggregate?device_id=&point_id=&from=&to=&step=&fn=`
- `GET /api/v1/telemetry/quality?device_id=&from=&to=`

Supported aggregate functions: `min`, `max`, `mean`, `count`, `last`; reject unknown functions. Validate device/point ownership against a configuration snapshot before querying storage. Enforce maximum time range, step, point count, result count and response bytes. Use parameterized storage queries; user values must never be concatenated into query-language expressions.

Latest-state reads use the bounded in-memory latest store. Historical aggregate reads use the configured time-series backend. If historical storage is unavailable, return an explicit dependency error; do not silently return latest values as historical data.

Every response reports query range, aggregation step, data quality semantics, and configuration version where relevant. Aggregates must preserve quality counts and must not present BAD/STALE values as valid numeric measurements.

## 7. Load and reliability test profile

Provide deterministic simulators; never run load tests against production control equipment.

Required profiles:
- smoke: 10 devices, mixed Modbus TCP and simulated DL/T645 transport.
- baseline: 100 devices, configurable points/device and polling interval.
- scale: 1,000 devices.
- target: 5,000 devices, only when CI/runner resources and test duration are explicitly recorded.

Record:
- configured devices/points, actual successful polls and samples/sec
- p50/p95/p99 poll and end-to-end persistence latency
- CPU, RSS, goroutines, open FDs, network bytes
- scheduler queue depth, in-flight count, WAL bytes/segments, pending sink ACKs
- quality distribution, timeout/error rates and dropped samples

Acceptance:
- no data races under `go test -race ./...`
- bounded goroutines, queues, caches and WAL growth
- no unbounded per-device Prometheus label cardinality
- clean shutdown drains accepted samples within configured timeout
- restart recovers WAL sequence and independent sink ACK state without duplicate logical ingestion (sink idempotency contract required)
- report results as measured facts; do not extrapolate a small test to a 5,000-device production guarantee.

## 8. Delivery sequence

1. Inspect and document existing types, APIs and metrics; avoid duplicate implementations.
2. Add canonical model adapters while preserving compatibility with current config and WAL formats.
3. Extend communication metrics and device diagnostics.
4. Formalize quality reasons, stale evaluation and quality aggregation.
5. Add bounded latest/aggregate/quality query APIs.
6. Add deterministic mixed-protocol simulator and benchmark/soak profiles.
7. Update Compose, operational docs and implementation status only after tests pass.

## 9. Out of scope

- Device write/control execution.
- Arbitrary DL/T645 multidrop discovery.
- AI fault prediction before sufficient validated historical data exists.
- Kubernetes migration before single-edge Compose deployment is operationally proven.
- Claiming HA or production certification without corresponding deployment and failure testing.

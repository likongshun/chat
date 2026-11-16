# Implementation status

Repository: `https://github.com/likongshun/chat`

## Implemented source scope

### Stage 1 — configuration
- Five-sheet Excel loader: Devices, Points, Processing, WriteRules, Enums.
- Header-name based column resolution.
- Validation of IDs, model references, function codes, data types, register ranges, numeric values, and disabled write policy.
- Atomic reload retaining the last known-good configuration snapshot.

### Stage 2 — Modbus TCP
- Persistent TCP connection per client, serialized requests, automatic reconnect after transport/protocol errors.
- Modbus TCP MBAP transaction/protocol/unit validation.
- Read functions FC01, FC02, FC03 and FC04.
- Protocol limits enforced: 2,000 bits and 125 registers per request.
- Read-batch planner coalesces adjacent/overlapping points without crossing function, model, or read-group boundaries.
- Register decoding for signed/unsigned integers, FLOAT32/FLOAT64, and STRING; bit decoding for BOOL.
- No Modbus write API is exposed by the current client.

### Stage 3 — collection scheduler
- One long-running serialized loop per enabled device.
- Global in-flight request/poll cap (default 500).
- Per-device poll interval, configurable timeout, initial stagger, exponential reconnect backoff with jitter.
- Bounded result channel; backpressure is explicit and results are not silently dropped.
- Atomic poll/error/timeout/sample/in-flight/duration metrics.
- Modbus point poller maps batch responses back to configured point samples and preserves point-level quality/errors.
- Graceful cancellation and client close support.

### Stage 4 — durability (incomplete)
- Frame writes handle short writes; frame readers reject invalid lengths/checksums; active data/ACK logs repair incomplete physical tails before append.
- A reusable segmented log writer with deterministic segment naming, rotation, reopen, and directory sync is present with unit coverage; it is not yet wired into the main WAL/manifest lifecycle.
- ACK validation checks the referenced sequence and exact event ID before persisting the ACK.
- Linux opens preallocate a dedicated `ack.reserve` file with `fallocate`; ACK write/sync ENOSPC paths attempt to release the reserve and retry.
- The current WAL still uses the legacy data/ACK layout and old checkpoint/reclaim implementation.
- The approved segmented Data WAL + segmented ACK WAL + checkpoint snapshot + manifest commit/recovery/reclaim protocol, reserve replenishment, capacity watermarks, unified fsync metrics, and disk-full/process-kill fault tests are not complete.
- Do not use the current WAL implementation as a production durability guarantee.

### Stage 4 — downstream pipeline (not integrated)
- A minimal Sink/Dispatcher interface exists.
- MQTT QoS 1 and InfluxDB adapters, independent durable ACK handling, and end-to-end replay are not implemented.
- The current collector command loads Excel configuration and opens WAL, then waits for shutdown; it does not yet start the full polling and delivery pipeline.

### Deployment foundation
- Multi-stage Dockerfile builds a non-root collector image.
- Docker Compose provisions collector, PostgreSQL 16, Mosquitto 2, and InfluxDB 2.7 with persistent volumes.
- Compose is infrastructure scaffolding only: PostgreSQL/MQTT/InfluxDB are not yet wired into the Go application.
- Mosquitto is configured for anonymous local development only; production authentication, ACLs, and TLS remain required.
- See `docs/DOCKER_COMPOSE.md`.

## Verification status

- GitHub Actions run `43957997176` for commit `7e51b40ce0cf3ea7641e905baae9bcc48cea2112` completed successfully: `go mod tidy`, `gofmt -w .`, `go vet ./...`, `go test ./...`, and the configured race-test packages all passed.
- That successful run predates the Docker/Compose commits. The new Docker build and Compose configuration have not yet been verified by a subsequent CI run or by a live Docker host.
- No physical Modbus device interoperability test, crash-recovery fault test, disk-full test, or 5,000-device load test has been completed.

## Remaining work in sequence

1. Finish segmented Data WAL and ACK WAL, checkpoint snapshot, manifest commit/recovery, crash-safe reclaim, reserve replenishment, capacity watermarks, and unified fsync metrics; add process-kill and disk-full fault tests.
2. Add MQTT QoS 1 and InfluxDB adapters with independent durable ACKs and idempotent event identity.
3. Connect the scheduler output to WAL and the durable downstream pipeline; add bounded backpressure and shutdown/replay behavior.
4. Add PostgreSQL configuration/version persistence, validated Excel import, immutable snapshots, and safe hot reload.
5. Add multi-instance device leases/sharding, health/metrics endpoints, security hardening, and production Compose profiles.
6. Run integration tests, physical-device interoperability tests, fault injection, and 5,000-device load tests.

## Safety

All write execution remains disabled. Do not connect to production control equipment until the full read path is validated against actual devices and all operational safety reviews are complete.

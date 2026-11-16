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

### Durability baseline
- The WAL format and crash-consistency contract are documented in `docs/WAL_FORMAT.md`.
- Frame writes now handle short writes; frame readers reject invalid lengths/checksums; active data/ACK logs repair incomplete physical tails before append.
- ACK validation checks the referenced sequence and exact event ID before persisting the ACK.
- Linux opens preallocate a dedicated `ack.reserve` file with `fallocate`; ACK write/sync ENOSPC paths attempt to release the reserve and retry.
- This remains an incremental hardening step, not completion of the approved segmented Data WAL + segmented ACK WAL + checkpoint snapshot + manifest commit protocol. The current WAL still uses the legacy data/ACK layout and old checkpoint/reclaim implementation.
- Do not use the current WAL implementation as a production durability guarantee.

## Verification status

Source changes are committed to `main`. The current execution environment cannot reach GitHub to clone the repository or download Go dependencies, and no successful GitHub Actions result has been confirmed. Therefore compilation, unit tests, race tests, device interoperability, crash recovery, and load tests are **not yet verified**.

## Remaining work in sequence

1. Stage 4: finish the segmented Data WAL and ACK WAL, checkpoint snapshot, manifest commit/recovery, crash-safe reclaim, reserve replenishment, capacity watermarks, and fsync metrics; then add process-kill and disk-full fault tests.
2. Add MQTT QoS 1 and InfluxDB adapters with independent durable ACKs and idempotent event identity.
3. Add PostgreSQL configuration/version persistence, deployment assets, fault injection, and 5,000-device load tests.

## Safety

All write execution remains disabled. Do not connect to production control equipment until the full read path is validated against actual devices and all operational safety reviews are complete.

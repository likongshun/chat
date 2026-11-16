# Implementation status

Repository: `https://github.com/likongshun/chat`

## Implemented source scope

### Configuration
- Five-sheet Excel loader: Devices, Points, Processing, WriteRules, Enums.
- Header-name based column resolution.
- Validation of IDs, model references, function codes, data types, register ranges, numeric values, and disabled write policy.
- Atomic reload retaining the last known-good configuration snapshot.

### Modbus TCP
- Persistent TCP connection per client, serialized requests, automatic reconnect after transport/protocol errors.
- Modbus TCP MBAP transaction/protocol/unit validation.
- Read functions FC01, FC02, FC03 and FC04.
- Protocol limits enforced: 2,000 bits and 125 registers per request.
- Read-batch planner coalesces adjacent/overlapping points without crossing function, model, or read-group boundaries.
- Register decoding for signed/unsigned integers, FLOAT32/FLOAT64, and STRING; bit decoding for BOOL.
- No Modbus write API is exposed by the current client.

### Durability baseline
- Existing WAL code is an early implementation scaffold only. Its reserve accounting is not a preallocated ACK reserve and it does not yet satisfy the agreed Data WAL + ACK WAL + Checkpoint + Manifest crash-consistency protocol.
- Do not use the current WAL implementation as a production durability guarantee.

## Verification status

Source changes are committed to `main`. The current execution environment cannot reach GitHub to clone the repository or download Go dependencies, and no successful GitHub Actions result has been confirmed. Therefore compilation, unit tests, race tests, device interoperability, crash recovery, and load tests are **not yet verified**.

## Remaining work in sequence

1. Complete high-concurrency scheduler: per-device serial polling, global concurrency cap, interval alignment, timeout, jittered exponential backoff, bounded result queue, metrics and graceful shutdown.
2. Rebuild the WAL according to the approved crash-consistency sequence, including real preallocated ACK reserve, capacity protection, checkpoint/manifest recovery, and reclaim.
3. Add MQTT QoS 1 and InfluxDB adapters with independent durable ACKs and idempotent event identity.
4. Add PostgreSQL configuration/version persistence, deployment assets, fault injection, and 5,000-device load tests.

## Safety

All write execution remains disabled. Do not connect to production control equipment until the full read path is validated against actual devices and all operational safety reviews are complete.

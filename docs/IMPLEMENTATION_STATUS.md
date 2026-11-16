# Implementation status

Repository: `https://github.com/likongshun/chat`

## Implemented source scope

### Stage 1 — Excel configuration
- Five-sheet loader: Devices, Points, Processing, WriteRules, Enums.
- Header-name based column resolution and validation of IDs, model references, function codes, data types, register ranges, numeric values, and disabled write policy.
- Atomic reload retaining the last known-good configuration snapshot.

### Stage 2 — Modbus TCP
- Persistent TCP connection per client, serialized requests, reconnect after transport/protocol errors.
- MBAP transaction/protocol/unit validation.
- Read functions FC01, FC02, FC03, FC04 with protocol limits of 2,000 bits and 125 registers.
- Read batching by function/model/read group, with address-span mapping.
- Decoding for signed/unsigned integers, FLOAT32/FLOAT64, STRING and BOOL.
- No Modbus write API is exposed.

### Stage 3 — collection scheduler
- One serialized polling loop per enabled device and global in-flight cap (default 500).
- Per-device intervals, configurable timeout, initial stagger, exponential backoff with jitter.
- Bounded output channel with explicit backpressure.
- Poll/error/timeout/sample/in-flight/duration metrics and graceful shutdown.
- Point-level quality/error mapping.

### Stage 4 — WAL durability (implemented foundation; not production complete)
- Exclusive Linux process lock held for WAL lifetime.
- Framed Data WAL with CRC32, short-write handling, strict frame validation and active-tail repair before recovery.
- Segmented Data WAL rotation with manifest-managed active/first segment boundaries.
- Independent MQTT and Influx ACK records, strict sink/sequence/event-ID validation, and recovery-time ACK-to-data identity checks.
- `PendingWithAcks()` returns independent per-sink ACK state for replay decisions.
- Immutable sequence-specific checkpoint generations. Checkpoint advances only across a fully ACKed sequence prefix; MANIFEST is the commit point.
- ACK log compaction occurs only after the checkpoint MANIFEST commit and preserves ACKs above the checkpoint boundary.
- Reclaim advances MANIFEST before deleting only fully checkpoint-covered, non-active segments. Recovery cleans obsolete leftovers after a partial deletion and fails closed if a required segment is missing.
- Sequence numbering resumes from the committed checkpoint and retained records, preventing reuse after reclaim.
- Linux ACK reserve preallocation via `fallocate`; ACK write/sync ENOSPC paths release reserve and retry. Startup can still recover when reserve allocation encounters ENOSPC.

Remaining WAL gaps:
- ACK WAL is still a single file, not segmented.
- Checkpoint currently represents a fully acknowledged prefix; it is not a full pending-record snapshot with per-sink state.
- Manifest/checkpoint checksum and explicit checkpoint reference fields are not implemented.
- Automatic ACK reserve replenishment, filesystem-wide warning/critical/stop watermarks, and capacity health metrics are not implemented.
- Unified fsync metrics and process-kill/disk-full fault injection are not complete.
- No production durability guarantee should be claimed until these gaps and target-filesystem fault tests are closed.

### Stage 4 — downstream delivery (not integrated)
- A basic Sink/Dispatcher interface exists. Scheduler results are now serialized and batch-appended to WAL with a 10 ms / 1,000-record batching bound.
- MQTT QoS 1 and InfluxDB adapters, live WAL replay loop, and end-to-end delivery are not yet integrated.
- Current collector command loads Excel configuration and opens WAL, then waits for shutdown; it does not yet run the Modbus polling pipeline.

### Deployment foundation
- Multi-stage Dockerfile builds a non-root collector image.
- Docker Compose provisions collector, PostgreSQL 16, Mosquitto 2, and InfluxDB 2.7 with persistent volumes.
- PostgreSQL/MQTT/InfluxDB are infrastructure services only; they are not yet wired into the Go application.
- Mosquitto allows anonymous access for local development only. Production requires authentication, ACLs, TLS, and secret management.
- See `docs/DOCKER_COMPOSE.md`.

## Verification status

- Commit `31ac68b74368942f8b285b53ebd4ce347ca399e1` passed Go CI, Docker Compose configuration validation, and Docker image build.
- WAL changes after that commit are undergoing their own Go CI runs; do not infer their result from the earlier successful run.
- Unit tests cover lock exclusion, ACK validation and identity recovery, torn active-tail repair, checkpoint generation isolation, ACK compaction, sequence continuity after reclaim, partial deletion cleanup, missing-segment fail-closed behavior, per-sink pending state, batch append, scheduler-to-WAL persistence, InfluxDB line protocol, and independent sink retry. The newest changes still require a successful CI run.
- No physical Modbus device interoperability test, process-kill fault matrix, real disk-full test, or 5,000-device load test has been completed.

## Remaining work in sequence

1. Finish WAL format checksums/migration, segmented ACK WAL, reserve replenishment, capacity watermarks, unified fsync instrumentation, and process-kill/disk-full tests.
2. Implement MQTT QoS 1 and InfluxDB adapters with independent durable ACKs and idempotent event identity.
3. Connect scheduler output to WAL and durable downstream delivery; implement bounded replay, backpressure, and graceful shutdown.
4. Add PostgreSQL configuration/version persistence, validated Excel import, immutable snapshots, and safe hot reload.
5. Add device leases/sharding for multi-instance operation, health/metrics endpoints, security hardening, and production Compose profiles.
6. Run integration tests, physical-device tests, fault injection, and 5,000-device load tests.

## Safety

All write execution remains disabled. Do not connect to production control equipment until the read path is validated against actual devices and operational safety reviews are complete.

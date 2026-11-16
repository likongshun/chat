# Implementation status

Repository: `https://github.com/likongshun/chat`

## Implemented

### Stage 1 — Excel configuration
- Five-sheet workbook loader: Devices, Points, Processing, WriteRules, Enums.
- Header-name based column resolution and validation of IDs, model references, function codes, data types, register ranges, numeric values, processing parameters, and disabled write policy.
- Atomic configuration snapshot manager; failed reload retains the last known-good snapshot.
- See `docs/CONFIGURATION.md`.

### Stage 2 — Modbus TCP
- Persistent TCP connection per device client, serialized requests, reconnect after transport/protocol errors.
- MBAP transaction/protocol/unit validation.
- Read functions FC01, FC02, FC03, FC04 with protocol limits of 2,000 bits and 125 registers.
- Read batching by function/model/read group.
- Decoding for signed/unsigned integers, FLOAT32/FLOAT64, STRING and BOOL.
- No Modbus write API is exposed.

### Stage 3 — collection scheduler
- One serialized polling loop per enabled device and global in-flight cap (default 500).
- Per-device interval, timeout, initial stagger, exponential backoff with jitter.
- Bounded output channel and explicit backpressure.
- Atomic poll/error/timeout/sample/in-flight/duration metrics and graceful shutdown.
- Point-level quality/error mapping.

### Stage 4 — WAL and downstream delivery (integrated foundation; not production complete)
- Exclusive Linux process lock held for WAL lifetime.
- Framed Data WAL with CRC32, short-write handling, strict frame validation, active-tail repair before recovery.
- Segmented Data WAL rotation with manifest-managed active/first segment boundaries.
- Independent MQTT and Influx ACK records; strict sink/sequence/event-ID validation and recovery-time ACK-to-data identity checks.
- `PendingWithAcks()` exposes independent sink state for replay.
- Immutable sequence-specific checkpoints; only a fully acknowledged sequence prefix is checkpointed, with MANIFEST as commit point.
- ACK compaction runs after checkpoint commit and preserves ACKs above the committed boundary.
- Reclaim commits the new manifest boundary before deleting only fully covered non-active segments. Recovery cleans leftover old segments after partial deletion and fails closed on missing required segments.
- Sequence numbering resumes from the checkpoint and retained records; no reuse after reclaim.
- Batched Data WAL append: scheduler samples are buffered up to 1,000 records or 10 ms, reducing fsync frequency.
- Delivery worker retries MQTT and Influx independently, skips a sink already ACKed, and persists each ACK before checkpoint advancement.
- MQTT sink uses Eclipse Paho and waits for QoS 1 publish acknowledgements.
- InfluxDB v2 sink writes line protocol over HTTP; successful 2xx response is treated as the write acknowledgement.
- Collector command now starts Modbus polling, WAL persistence, and downstream delivery when InfluxDB credentials are configured.

Remaining WAL/delivery gaps:
- ACK WAL is still a single file, not segmented.
- Checkpoint is a fully ACKed prefix, not a complete pending-record snapshot with per-sink state.
- Manifest/checkpoint checksum and explicit checkpoint reference fields are not implemented.
- Automatic ACK reserve replenishment and filesystem-wide warning/critical/stop watermarks are not implemented. Basic WAL capacity rejection, disk usage, pending and segment metrics are exposed.
- Unified fsync metrics and process-kill/disk-full fault injection are not complete.
- MQTT, InfluxDB and PostgreSQL integration tests are included in the Compose-backed CI job; physical-device and production-filesystem tests are still required.
- Do not claim production durability until these gaps and target-filesystem fault tests are closed.

### Stage 5 — PostgreSQL configuration management (version store integrated)
- PostgreSQL schema stores immutable JSONB configuration snapshots, SHA-256 checksums, source metadata, and creation timestamps.
- Excel import validates before inserting a version and switching the active pointer in one transaction.
- Startup loads and validates the active version; if none exists, the workbook is imported as the initial version.
- Explicit version activation and version history listing are implemented.
- The active configuration version is attached to each sample and persisted to InfluxDB.
- Automatic hot reload, an administrative import/activation API, and device leases/sharding are not implemented.

### Deployment
- Multi-stage Dockerfile builds a non-root collector image.
- Compose provisions collector, PostgreSQL 16, Mosquitto 2, and InfluxDB 2.7 with persistent volumes.
- Collector polls Modbus devices, buffers to WAL, and delivers to MQTT/Influx when configured.
- PostgreSQL is connected to the collector as the versioned configuration repository.
- Mosquitto anonymous access is development-only; production requires authentication, ACLs, TLS, and managed secrets.
- See `docs/DOCKER_COMPOSE.md`.

## Verification

- Commit `5e7b5d52be2f1b6ea21afbe2e2f3637c9b31bbb7` passed Go unit tests, `go vet`, race tests across all Go packages, Compose config validation, and Docker image build. This was before the newest delivery integration changes.
- The newest MQTT/Influx, delivery worker, WAL checkpoint and processing changes have separate GitHub Actions runs in progress; their success must be checked before release.
- Unit tests cover WAL locking, ACK identity, torn-tail recovery, checkpoint generation isolation, ACK compaction, sequence continuity, partial reclaim cleanup, missing-segment fail-closed behavior, per-sink ACK state, batch append, delivery retry, Influx line protocol, processing operators, and scheduler-to-WAL persistence.
- No physical Modbus device test, full process-kill fault matrix, real disk-full test, or 5,000-device load test has been completed.

## Remaining work

1. Finish WAL checksum/migration, segmented ACK WAL, reserve replenishment, capacity watermarks, fsync instrumentation, and crash/disk-full fault tests.
2. Add PostgreSQL configuration/version storage, Excel import transactions, immutable snapshots, and safe hot reload.
3. Add device leases/sharding for multi-instance collection.
4. Expand Prometheus fsync/capacity metrics, structured logs, production Compose profile, MQTT authentication/TLS and Docker secrets.
5. Run broker/Influx integration tests, physical device interoperability, fault injection, and 5,000-device load tests.

## Safety

All write execution remains disabled. Do not connect to production control equipment until the read path is validated against actual devices and operational safety reviews are complete.

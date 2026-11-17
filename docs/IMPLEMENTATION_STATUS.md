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
- Segmented Data WAL rotation with manifest-managed active/first segment boundaries; the next segment file and directory entry are synced before MANIFEST can point to it, so a pre-commit crash leaves only an ignorable orphan.
- Recovery rejects duplicate or missing post-checkpoint sequence numbers; checkpoint advancement independently verifies a contiguous acknowledged prefix and refuses to jump over missing records.
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
- ACK WAL uses Manifest v6-managed numbered segments. Legacy `ack.wal` is copied and synced into ACK segment 1 before the Manifest migration commit; the legacy file is removed only afterward. Rotation publishes and syncs the next segment before updating the Manifest. Checkpoint compaction writes a fresh retained-ACK segment, syncs it, commits new ACK boundaries, and only then removes obsolete segments. Recovery removes uncommitted or obsolete ACK segment orphans and fails closed when a committed ACK segment is missing.
- Checkpoint sequence remains the fully ACKed contiguous prefix; Manifest v6 additionally snapshots the complete pending suffix and each sink's independent ACK state, including a sequence-zero initial backlog snapshot.
- Manifest v6 binds an explicit checkpoint filename (including valid sequence-zero snapshots) and SHA-256 of the exact payload bytes, in addition to the Manifest checksum and Data/ACK segment boundaries. Checkpoint v2 snapshots every not-fully-ACKed retained record above the fully acknowledged prefix, including payload and independent MQTT/Influx ACK flags. Same-prefix state changes use immutable checkpoint generations. Versions 1–5 remain readable and are upgraded during safe open/migration. Formal operator-facing migration tooling remains outstanding.
- Linux ACK reserve is preallocated at open, released only as an emergency last-resort ACK allowance, and replenished by the checkpoint/reclaim worker after space is freed. Linux `statfs` and WAL-budget pressure levels expose warning/critical/stop at 70%/85%/95%; Data admission stops at 95% or earlier at the configured WAL budget/reserve boundary, while ACK and reclaim remain available. Filesystem usage and pressure level are exported to Prometheus.
- Unified fsync histogram/error/P99 telemetry is implemented across WAL data, ACK, checkpoint, manifest, reserve, and directory sync paths. Subprocess crash tests now exercise checkpoint publication, uncertain Manifest rename, ACK segment rotation/compaction, and reclaim commit boundaries. Injected ENOSPC tests verify reserve release, torn-tail repair, ACK retry, and fail-closed reserve exhaustion. A disposable ext4 loopback CI filesystem is filled to zero allocatable blocks; the test verifies Data admission stops, ACK consumes the reserve, checkpoint/reclaim completes, and ACK durability fails closed after reserve exhaustion.
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

- Commit `43f9c31d3578da456036bc9018d5cf557a959a4c` passed Go formatting, `go vet`, unit tests, race tests, Docker Compose validation, collector image build, and PostgreSQL/MQTT/InfluxDB integration tests (GitHub Actions run `44016264596`).
- Commit `b798185f81f01abd7d702c7943d2320aa8fa151b` passed the same CI suite after adding explicit checkpoint-reference and Reclaim coverage (run `44015889194`).
- Commit `d422391444e051dd9e55498a3dac1c53a287fd92` passed the same suite with concurrent ACK writes and ACK reserve replenishment coverage (run `44016094424`).
- Unit tests cover WAL locking, ACK identity and idempotency, legacy ACK migration, ACK segment rotation and two-phase compaction, compaction manifest-failure recovery, checkpoint checksums and explicit references, path traversal rejection, torn-tail recovery, Data/ACK orphan cleanup, missing committed segment fail-closed behavior, sequence continuity, and per-sink delivery state.
- The newest Data-orphan recovery and checkpoint migration additions have their own CI runs; verify their final conclusions before release.
- No physical Modbus device test, multi-hour production-filesystem soak, or 5,000-device load test has been completed.

## Remaining work

1. Run multi-hour backlog/recovery soaks and repeat the ext4 crash/ENOSPC matrix on the actual deployment filesystem; validate reserve sizing, inode/quota exhaustion, and alert thresholds.
2. Implement automatic configuration hot reload and an administrative import/activation API.
3. Add device leases/sharding for multi-instance collection.
4. Expand structured logs and production Compose hardening; configure MQTT authentication, ACL/TLS, and Docker secrets.
5. Run physical-device interoperability, production-filesystem fault tests, and 5,000-device load tests.

## Safety

All write execution remains disabled. Do not connect to production control equipment until the read path is validated against actual devices and operational safety reviews are complete.

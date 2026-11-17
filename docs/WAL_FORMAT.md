# WAL format and crash-consistency contract

This document describes the current WAL implementation in this repository and explicitly separates implemented guarantees from the remaining production-hardening work.

## Current on-disk layout

```text
data/wal/
  .wal.lock
  MANIFEST
  MANIFEST.tmp
  checkpoint-00000000000000000012.json
  checkpoint-00000000000000000012.json.tmp
  data-00000000000000000001.wal
  data-00000000000000000002.wal
  ack.wal
  ack.wal.tmp
  ack.reserve
```

The current implementation uses segmented Data WAL files and one ACK WAL file. It does **not** yet use segmented ACK WAL files or a dedicated `ack/` and `reserve/` directory hierarchy.

The process obtains an exclusive OS lock on `.wal.lock` before recovery and holds it until close. Temporary files are not authoritative.

## Frame format

Data and ACK logs use the same frame envelope:

| Offset | Size | Meaning |
|---|---:|---|
| 0 | 4 bytes | unsigned big-endian payload length |
| 4 | 4 bytes | CRC32-IEEE of payload |
| 8 | N bytes | UTF-8 JSON payload |

A frame length of zero or greater than 64 MiB is invalid. Short headers or payloads at the physical end of the active Data/ACK log are truncated to the last complete valid frame and the repaired file is synced before recovery continues. A complete frame with a bad checksum is treated as corruption and recovery fails.

Writes handle short writes. Data records require a non-empty event ID and JSON payload.

## Data record

Each Data frame contains:
- `seq`: globally increasing, non-zero sequence number;
- `event_id`: stable, non-empty idempotency key;
- `payload`: original event payload;
- `created_at`: UTC timestamp.

Sequence assignment occurs under the WAL mutex. A record is accepted only after the configured append sync boundary succeeds.

## ACK record

Each ACK frame contains:
- `seq`: sequence number of an existing data record;
- `event_id`: exact event ID of that record;
- `sink`: exactly `mqtt` or `influx`.

An ACK is durable only after ACK WAL sync succeeds. MQTT and InfluxDB states are independent. Recovery validates ACK JSON, sink names, sequence references, and event IDs against retained data records. Duplicate ACKs are safe but may occupy extra log space until checkpoint compaction.

`PendingWithAcks()` returns each pending record together with `MQTTAcked` and `InfluxAcked`. A dispatcher must publish only to sinks that have not acknowledged that event.

## Checkpoint semantics currently implemented

The current checkpoint is a **fully acknowledged sequence prefix**, not a snapshot containing pending records:

1. Sync the active Data and ACK logs.
2. Verify that every record in the newly covered range has both MQTT and Influx ACKs.
3. Write an immutable `checkpoint-<seq>.json.tmp`, sync it, rename it to its sequence-specific final name, and sync the directory.
4. Update `MANIFEST.tmp`, sync it, atomically rename it to `MANIFEST`, and sync the WAL directory.
5. After the manifest commit, compact `ack.wal` to retain only ACK records with sequence numbers greater than the committed checkpoint boundary.

Versioned checkpoint filenames are important: a checkpoint written before a failed manifest commit must not overwrite the checkpoint referenced by the old manifest. Orphan checkpoint generations are ignored.

ACK compaction happens only after the manifest commit. If the process crashes before compaction, the old ACK log is still safe. If it crashes after compaction, all ACK state at or below the boundary is already represented by the committed fully-ACKed prefix; later ACKs are retained. If compaction cannot complete, the checkpoint is committed but the call returns an error explaining that ACK compaction failed.

Manifest version 3 records format version, checkpoint sequence, checkpoint payload SHA-256, first required Data segment, active Data segment, and a SHA-256 checksum over the canonical JSON fields excluding the manifest checksum itself. Recovery rejects missing or mismatched v3 checkpoint/manifest checksums. Version 1 and version 2 manifests remain readable; the next successful metadata commit upgrades them to version 3. Version 2 introduced the manifest checksum, while version 3 binds the exact checkpoint bytes to the manifest. The checkpoint filename is still derived from the sequence; an explicit checkpoint filename/reference field is not yet implemented.

## Recovery and reclaim order

- Acquire the exclusive WAL lock before any recovery.
- Repair torn tails in the active Data and ACK files before scanning.
- Verify that every Data segment in the manifest's inclusive `[first_segment, active_segment]` range exists.
- Validate Data records and ACK references; fail closed on malformed frames, malformed JSON, missing segments, or mismatched event IDs.
- Initialize the next sequence from the maximum of the committed checkpoint sequence and retained Data records, preventing sequence reuse after old segments have been deleted.
- Reclaim only complete, non-active Data segments whose every record is at or below the committed checkpoint boundary.
- Commit the advanced `first_segment` in MANIFEST before unlinking covered segments.
- Sync the WAL directory after deletion.
- On the next open, remove leftover segment files below the committed `first_segment` boundary. This resumes cleanup after a crash during partial deletion.

A missing segment inside the manifest's required range is a recovery error. Reclaim does not silently skip it or advance the boundary.

## Disk-full behavior currently implemented

- Data admission enforces a configured WAL byte budget and reserves a separate logical capacity budget for ACK space.
- On Linux, `statfs` filesystem usage and WAL budget usage produce warning/critical/stop levels at 70%/85%/95%; new Data admission stops at 95% or earlier at the configured WAL budget/reserve boundary, while ACK and reclaim remain available.
- Linux preallocates `ack.reserve` using `fallocate`; reserve growth is prepared in a temporary file, synced, atomically renamed, and followed by a directory sync so the previous reserve remains intact until replacement is ready.
- If reserve allocation returns ENOSPC during open, recovery is still allowed to proceed; ACK durability may remain unavailable until space is freed.
- On ACK write/sync ENOSPC, the reserve file is released and the ACK operation is retried.
- The reserve is attempted again on the next process open; automatic reserve replenishment and filesystem-wide watermarks are not yet implemented.
- A failed ACK is never reported as durable and cannot authorize checkpointing.

## fsync telemetry

All WAL file and directory sync paths use one instrumented sync wrapper. It exposes a Prometheus histogram (`modbus_wal_fsync_duration_seconds`), error counter (`modbus_wal_fsync_errors_total`), and approximate P99 gauge (`modbus_wal_fsync_p99_seconds`). The histogram buckets are 1, 5, 10, 25, 50, 100, 250, and 1,000 ms plus +Inf. Metrics are process-wide and cumulative; validate bucket thresholds against the target storage before setting alerts.

## Remaining production-hardening work

The following items are **not complete** and must not be assumed from the current implementation:

- Segmented ACK WAL and a checkpoint snapshot with explicit pending-record and per-sink ACK state.
- Explicit checkpoint filename/reference fields and formal format migration tooling.
- Automatic ACK reserve replenishment is attempted by the checkpoint/reclaim worker; real disk-full and reserve-exhaustion behavior still needs target-filesystem fault tests.
- Process-kill fault injection at every checkpoint/manifest/unlink boundary.
- Real disk-full and reserve-exhaustion tests.
- Long-running soak tests and target-storage benchmarks.

The current WAL has unit tests for locking, ACK validation, torn-tail repair, checkpoint ordering, sequence continuity after reclaim, partial deletion recovery, and independent sink ACK state. These tests do not replace process-kill fault injection or production filesystem testing.

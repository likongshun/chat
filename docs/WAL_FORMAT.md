# WAL on-disk format and crash-consistency contract

This document defines the durable storage contract for Stage 4. It supersedes the early single-directory WAL scaffold. The implementation must not be described as production-safe until the fault-injection tests in this document's companion test plan pass on the target filesystem.

## Directory layout

```text
data/wal/
  .wal.lock
  MANIFEST
  MANIFEST.tmp
  checkpoint-000012.tmp
  checkpoint-000012.chk
  segment-000001.wal
  segment-000002.wal
  ack/
    ack-000001.wal
  reserve/
    ack.reserve
```

All files are owned by one process at a time. The process obtains an exclusive OS file lock on `.wal.lock` before reading or mutating any WAL state and holds it until close. Temporary files are never authoritative.

## Framing

Data and ACK logs use the same frame envelope:

| Offset | Size | Meaning |
|---|---:|---|
| 0 | 4 bytes | unsigned big-endian payload length |
| 4 | 4 bytes | CRC32-IEEE of payload |
| 8 | N bytes | UTF-8 JSON payload |

A frame length of zero or greater than 64 MiB is invalid. A short header or short payload at the physical end of the active log is a torn tail and is truncated to the last complete valid frame before append. A CRC mismatch in the middle of a segment is corruption and must fail recovery; it must not be silently skipped. The same rule applies to ACK logs. Writes must handle short writes and return errors.

## Data record

Each data frame contains a JSON `Record` with:
- `seq`: globally increasing, non-zero sequence number;
- `event_id`: stable, non-empty idempotency key;
- `payload`: original event payload;
- `created_at`: UTC timestamp.

Sequence numbers are assigned under the WAL mutex. A batch append is all-or-error at the API boundary; a partially written final frame is repaired on restart. A caller may treat a record as durable only after the configured sync boundary succeeds.

## ACK record

Each ACK frame contains:
- `seq`: sequence number of an existing data record;
- `event_id`: exact event ID of that record;
- `sink`: exactly `mqtt` or `influx`.

An ACK is durable only after its ACK log sync succeeds. Duplicate ACKs are idempotent. Unknown sequence numbers, event-ID mismatches, and unsupported sinks are rejected. MQTT and InfluxDB acknowledgements are independent; a record is reclaimable only when both are durable.

## Checkpoint

A checkpoint is a complete snapshot of the uncompleted data records at a frozen sequence boundary. It includes each retained record and its per-sink ACK state, plus the checkpoint sequence and creation time. The checkpoint must be self-validating (version and checksum). The checkpoint is not authoritative merely because its file exists.

## Manifest

The manifest is the commit point for checkpoint advancement and segment reclamation. It contains:
- format version;
- checkpoint sequence;
- checkpoint filename and checksum;
- first segment still required after the checkpoint;
- active data segment;
- active ACK segment.

Manifest replacement uses a temporary file, file sync, atomic rename, then parent-directory sync. Recovery trusts only the last fully committed `MANIFEST`; orphan temporary files and unreferenced checkpoints are ignored.

## Required checkpoint and reclaim ordering

1. Acquire and hold the exclusive WAL lock before recovery.
2. Freeze the sequence boundary while holding the WAL mutex.
3. Build a complete checkpoint containing all records not fully acknowledged at or below that boundary, including both sink states.
4. Write the checkpoint temporary file, sync it, close it, and rename it to its final name.
5. Sync the checkpoint directory.
6. Write `MANIFEST.tmp` with the checkpoint name/checksum and new segment boundaries.
7. Sync and close `MANIFEST.tmp`, atomically rename it to `MANIFEST`, then sync the WAL directory.
8. Only after step 7 succeeds may segments wholly covered by the committed checkpoint be deleted.
9. Sync the WAL directory after segment deletion.

A failure before the manifest commit leaves the old manifest and old segments authoritative. A failure after manifest commit leaves the new checkpoint authoritative even if old segments remain. Recovery uses the committed checkpoint and only replays data events beyond its sequence boundary. Partial deletion after manifest commit is safe and may be completed on the next open.

Segment deletion is never decided from filenames alone. A segment is removable only when every record in it is represented by the committed checkpoint boundary and the checkpoint preserves all still-pending records and ACK states.

## Disk-full behavior and ACK reserve

Data admission and ACK durability use separate capacity budgets. The ACK reserve is a real preallocated file (Linux `fallocate` where supported), not merely a logical subtraction from the data budget. Ordinary data appends must never consume this reserve.

When the data budget or filesystem stop watermark is reached, new data appends are rejected, but ACK append/sync, recovery, checkpointing, and reclaim remain available. ACK reserve exhaustion is a critical durability fault: do not report delivery completion, do not mark the in-memory ACK state durable, and do not reclaim the corresponding data. Raise a critical health/metrics signal.

The reserve size must be sized from the maximum outstanding records, maximum ACK frame size, rotation overhead, and filesystem behavior; 64 MiB is only an initial example, not a universal safe value.

## Capacity and observability

Enforce both configured WAL maximum bytes and filesystem usage watermarks. Initial defaults: warning 70%, critical 85%, stop 95%. At stop, reject new data while preserving ACK and cleanup paths.

All sync operations (data, ACK, checkpoint, manifest, and directory sync) must use one instrumentation wrapper. Export count, errors, total/max and rolling p50/p95/p99 latency, including:
- `modbus_wal_fsync_duration_seconds`;
- `modbus_wal_fsync_errors_total`;
- `modbus_wal_fsync_p99_seconds`;
- `modbus_wal_disk_used_ratio`;
- `modbus_wal_disk_level`;
- `modbus_wal_capacity_rejections_total`;
- `modbus_wal_pending_records`.

A p99 fsync latency above 50 ms is an initial alert threshold only; production thresholds require measurements on the deployment storage.

## Recovery and fault-injection acceptance

Tests must terminate a subprocess at each persistence boundary: checkpoint temp write, checkpoint sync, checkpoint rename, checkpoint directory sync, manifest temp write, manifest sync, manifest rename, manifest directory sync, each segment unlink, and post-unlink directory sync. After restart, recovery must produce no lost acknowledged state, no lost pending record, and no duplicate sequence assignment.

Additional tests must cover torn tails, middle-of-file CRC corruption, duplicate ACK, invalid ACK identity, ACK-only ENOSPC, data-capacity exhaustion while ACK/reclaim still progress, reserve exhaustion, partial segment deletion, lock exclusion, and repeated recovery idempotence.

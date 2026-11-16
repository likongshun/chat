# Rebuild and verification plan

## Source inventory (2026-11-17)

The uploaded Phase 1 archive was inspected directly. It contains a five-sheet workbook and a configuration loader prototype. The ChatGPT share URL was not retrievable in this environment, so no code has been inferred from its inaccessible contents.

## Completed source increments

1. Excel five-sheet loader, header-based parsing, range/type validation, fail-closed write policy, and atomic last-known-good reload.
2. Modbus TCP persistent client, serialized requests, FC01/02/03/04 reads, protocol limits, contiguous read batching, and typed register/bit decoding.
3. Long-running per-device scheduler, global concurrency cap, timeout, interval handling, stagger, jittered exponential backoff, bounded result delivery, metrics, and a configured Modbus point poller.

These are committed source increments, not yet verified releases. The environment could not clone the repository or fetch Go dependencies, and no successful CI run has been confirmed.

## Remaining rebuild order

4. Replace the current WAL scaffold with the agreed crash-consistent Data WAL + ACK WAL + Checkpoint + Manifest design. A preallocated ACK reserve must allow ACK/recovery/reclaim after data admission stops. Checkpoint/manifest commit must precede segment deletion.
5. Add MQTT QoS 1 and InfluxDB adapters with independent durable ACK state and idempotent event identity.
6. Add Prometheus metrics, crash subprocess tests, disk-quota tests, MQTT/Influx fault injection, and 5,000-device load tests.
7. Add PostgreSQL configuration/version persistence, hot reload, deployment examples, and operational dashboards.

## Known durability blockers

- Current `ReserveBytes` only reduces the data admission limit; it is not a preallocated ACK reserve.
- Frame recovery currently ignores incomplete/corrupt tails without repairing the append position.
- ACK events are not validated against existing sequence/event IDs.
- Checkpoint and reclaim need the full committed-manifest crash ordering.
- Current WAL is not safe to treat as a production durability guarantee.

## Safety and acceptance

- No Modbus write path is enabled. Future write support requires a separate allow-list, explicit authorization, equipment-specific validation, and safety review.
- Do not describe the engine as production-ready until CI, race tests, crash-recovery tests, disk-full tests, sink fault injection, real-device interoperability, and representative load tests have passed.
- Every implementation increment is synchronized to this repository with a scoped commit and an honest verification status.

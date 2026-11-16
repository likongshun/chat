# Rebuild and verification plan

## Source inventory (2026-11-17)

The public repository already contains an initial Go baseline. The uploaded Phase 1 archive was inspected and contains an Excel workbook plus a configuration loader prototype. The shared ChatGPT URL could not be fetched in this environment, so no source code is being inferred from that page.

## Verified gaps in the current repository

- Excel workbook ingestion is not integrated into the current `internal/config` package. The uploaded workbook has five sheets: Devices, Points, Processing, WriteRules, Enums.
- The current WAL `ReserveBytes` setting only reduces the data admission limit. It does not create a preallocated ACK reserve, so it does not guarantee ACK progress when the filesystem is full.
- WAL frame recovery silently stops on truncated/corrupt frames but does not truncate the invalid tail before future appends. A later valid append could become unreachable during recovery.
- ACK records are not validated against an existing data sequence and event ID.
- Checkpoint sequence monotonicity and strict corruption handling need strengthening.
- The current Modbus client opens a new TCP connection per request; connection reuse and FC01/02 reads are not implemented.
- MQTT and InfluxDB concrete sink adapters, PostgreSQL config persistence, production metrics, and full integration/fault-injection coverage are not present.
- Existing documentation correctly labels the repository as an initial baseline and does not claim successful test execution.

## Rebuild order

1. Integrate and harden the five-sheet Excel configuration loader; reject integer overflow before narrowing conversions; preserve last-known-good snapshots on failed reload.
2. Complete Modbus TCP framing, connection lifecycle/reuse, FC01/02/03/04 reads, register decoding, and bounded read coalescing.
3. Complete scheduler lifecycle, per-device serialization, bounded output, backoff with jitter, and observability.
4. Replace the WAL reserve approximation with a crash-consistent data-log/ACK-log/checkpoint/manifest implementation. ACK durability and reclaim must remain possible under data-capacity exhaustion.
5. Add MQTT QoS 1 and InfluxDB adapters with independent ACK state and idempotent event identity.
6. Add Prometheus metrics, subprocess crash tests, disk-quota tests, sink fault injection, CI and deployment examples.
7. Add PostgreSQL configuration/version management and hot reload after the collection/persistence contracts are stable.

## Safety and acceptance rules

- No Modbus write path is enabled by this rebuild. Any future write support requires a separate allow-list, explicit authorization, equipment-specific validation, and safety review.
- Do not describe the system as production-ready until CI, race tests, crash-recovery tests, disk-full tests, and representative load tests have actually passed.
- Each completed implementation increment is committed to this repository with a scoped commit message and a recorded verification status.

# Modbus Collection Engine

Go-based high-concurrency Modbus TCP collection engine with a durable WAL pipeline.

> Initial engineering scaffold. See docs/IMPLEMENTATION_STATUS.md for implemented scope and verification status.

## Quick start

```bash
go test ./...
go run ./cmd/collector
```

Configuration examples are in `configs/`. WAL data is runtime state and must not be committed.

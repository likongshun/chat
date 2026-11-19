# DCIM Edge Engine mixed-protocol load testing

The `cmd/loadtest` command is a deterministic software-pipeline load generator. It exercises the collector scheduler, protocol router, bounded result channel, per-device/per-protocol communication counters, latest-value cache, and quality counters using simulated Modbus TCP and DL/T645 pollers.

It does **not** emulate electrical equipment, serial timing, actual Modbus/DL/T645 wire framing, MQTT/Influx delivery, WAL fsync, or the target deployment filesystem. Those require separate integration and hardware-in-the-loop tests.

## Profiles

Run from the repository root:

```sh
# Smoke: 10 devices, 20 points/device
go run ./cmd/loadtest -devices 10 -points 20 -workers 10 -duration 30s

# Baseline: 100 devices
go run ./cmd/loadtest -devices 100 -points 50 -workers 100 -interval 1s -duration 5m

# Scale: 1,000 devices
go run ./cmd/loadtest -devices 1000 -points 50 -workers 250 -interval 1s -duration 15m

# Target experiment: 5,000 simulated devices
go run ./cmd/loadtest -devices 5000 -points 50 -workers 500 -interval 1s -duration 30m
```

## Fault injection

Inject one deterministic poll timeout every 100 polls per protocol:

```sh
go run ./cmd/loadtest -devices 100 -points 20 -error-every 100 -duration 2m
```

## Output

The command prints one JSON summary containing:
- device count, points/device, worker count, polling interval and duration
- completed polls, poll errors, returned samples and throughput
- average and maximum poll latency
- heap allocation and goroutine count
- per-protocol attempts, successes, failures and timeouts
- latest point quality counts

Record the host CPU, memory, Go version, OS/kernel, container limits, exact command and commit SHA alongside each result. Compare repeated runs on the same hardware. A software simulation result is not a production capacity guarantee.

## Recommended additional validation

1. Run `go test -race ./...`.
2. Run the existing WAL recovery/ENOSPC tests independently.
3. Test with a real Modbus TCP simulator and a DL/T645 gateway/meter emulator.
4. Test RS485 bus serialization, baud/parity/stop-bit combinations and physical line faults.
5. Run MQTT/Influx delivery and WAL backlog soak tests.
6. Repeat on the target filesystem and deployment resource limits.

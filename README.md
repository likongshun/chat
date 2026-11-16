# Modbus Collection Engine

Go-based Modbus TCP collection engine with Excel-driven device configuration and a durable local WAL foundation.

> The repository is under active staged development. See [implementation status](docs/IMPLEMENTATION_STATUS.md) for exact implemented scope and verification status.

## Docker Compose

```sh
cp .env.example .env
# Replace all CHANGE_ME values in .env with strong random secrets.
docker compose config
docker compose up -d --build
docker compose ps
docker compose logs -f collector
```

See [Docker Compose deployment](docs/DOCKER_COMPOSE.md) for volumes and operations, and [Excel configuration](docs/CONFIGURATION.md) for workbook structure, data types, processing operators, and write safety.

The current collector reads the Excel configuration, polls enabled Modbus TCP devices, batches samples into the local WAL, and replays pending records to MQTT (QoS 1) and InfluxDB v2 with independent durable ACKs. The anonymous Mosquitto configuration is development-only.

## Local development

```sh
go mod tidy
go test ./...
go run ./cmd/collector
```

Configuration examples are in `configs/`. WAL data is runtime state and must not be committed.

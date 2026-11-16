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

See [Docker Compose deployment](docs/DOCKER_COMPOSE.md) for volumes, operations, and production hardening.

The current collector reads the Excel configuration, polls enabled Modbus TCP devices, and batches samples into the local WAL. MQTT and InfluxDB containers are provisioned by Compose but their delivery adapters and replay worker are not yet integrated into the application. The anonymous Mosquitto configuration is development-only.

## Local development

```sh
go mod tidy
go test ./...
go run ./cmd/collector
```

Configuration examples are in `configs/`. WAL data is runtime state and must not be committed.

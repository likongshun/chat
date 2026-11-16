# Modbus Collection Engine

Go-based Modbus TCP collection engine with Excel-driven device configuration and a durable WAL foundation.

> The repository is under active staged development. See [implementation status](docs/IMPLEMENTATION_STATUS.md) for implemented scope and verification status.

## Docker Compose

Copy `.env.example` to `.env`, replace all `CHANGE_ME` secrets, then run:

```sh
cp .env.example .env
docker compose config
docker compose up -d --build
docker compose ps
```

See [Docker Compose deployment](docs/DOCKER_COMPOSE.md) for volumes, operations, and production hardening.

**Current limitation:** Compose provisions PostgreSQL, Mosquitto, and InfluxDB, but the current collector process only loads the Excel configuration and opens the WAL. The live Modbus polling and MQTT/InfluxDB delivery path is not yet integrated. The bundled anonymous MQTT configuration is development-only.

## Local development

```sh
go mod tidy
go test ./...
go run ./cmd/collector
```

Configuration examples are in `configs/`. WAL data is runtime state and must not be committed.

# Docker Compose deployment

## Current scope

The Compose stack provides containers and persistent volumes for:
- the current Go collector executable;
- PostgreSQL 16;
- Eclipse Mosquitto 2;
- InfluxDB 2.7.

**Important:** the current collector executable loads the Excel configuration and opens the local WAL, then waits for shutdown. PostgreSQL, MQTT, and InfluxDB are provisioned as infrastructure services, but the application has not yet integrated the PostgreSQL configuration repository or MQTT/InfluxDB sinks. Starting the stack does not mean telemetry is already flowing.

The bundled Mosquitto configuration allows anonymous connections and is for local development only. Do not expose this broker to untrusted networks or use it in production. Before production, configure password authentication, ACLs, TLS, and secret management.

## Start

```sh
cp .env.example .env
# Edit .env and replace every CHANGE_ME value with strong random secrets.
docker compose config
docker compose up -d --build
docker compose ps
docker compose logs -f collector
```

The collector WAL is stored in the named volume `collector_wal`. PostgreSQL, MQTT, and InfluxDB each have independent persistent volumes. The collector health endpoint is available at `http://127.0.0.1:9090/healthz`, readiness at `/readyz`, and Prometheus metrics at `/metrics`; the host port is bound to loopback.

## Stop and upgrade

```sh
docker compose down
docker compose pull
docker compose up -d --build
```

Do not use `docker compose down -v` for routine maintenance: it deletes persistent database and WAL volumes.

## Configuration

The Excel file is baked into the image from `configs/modbus_device_config.xlsx`. To change it, update the file and rebuild the collector image. A later deployment iteration will add a read-only host-mounted config option and database-backed version management.

## Production checklist

- Replace anonymous MQTT with authenticated users, topic ACLs, and TLS.
- Put secrets in Docker secrets or an external secret manager; do not commit `.env`.
- Set resource limits and host-level disk alerts based on measured WAL throughput.
- Back up PostgreSQL and InfluxDB; define retention and restore drills.
- Complete the segmented WAL/checkpoint/manifest crash-recovery protocol before claiming production durability.
- Integrate and test the real Modbus -> WAL -> MQTT/InfluxDB path before treating this stack as a production collector.

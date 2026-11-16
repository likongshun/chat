# Implementation status

This repository is an initial, standard-library-only implementation baseline.

Implemented in this revision: configuration validation primitives; Modbus TCP read holding/input registers and single-register writes; bounded concurrent device polling; framed append-only data and ACK logs; segment rotation; process lock on Linux; checkpoint/manifest atomic replacement; recovery scanning; capacity admission; unit tests and CI workflow.

Not yet production-complete: Excel workbook ingestion, MQTT and InfluxDB concrete adapters, PostgreSQL persistence, full Modbus function-code coverage, TLS, distributed scheduling, exhaustive power-loss testing, quota-backed disk-full tests, and deployment dashboards. Do not deploy to control equipment without integration testing and an approved write policy.

Verification status: source is committed; CI must run on GitHub Actions. No claim of successful test execution is made by this document.

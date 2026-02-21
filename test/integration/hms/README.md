# HMS Integration Tests

This directory contains Docker-backed HMS integration checks.

Run locally:

```bash
./scripts/run_hms_integration.sh
```

What it validates:

- HMS standalone metastore container starts (`apache/hive:4.0.0`, `SERVICE_NAME=metastore`)
- Metastore Thrift endpoint is reachable on port `9083`
- Startup log marker is present (`Starting Hive Metastore Server`)

Optional extended C++ harness:

```bash
HMS_RUN_CPP_HARNESS=true ./scripts/run_hms_integration.sh
```

The harness file `test/integration/hms/hms_integration_harness.cpp` contains checks for endpoint parsing, mapper/retry behavior, and current connector stub contract. It is disabled by default because direct standalone compilation requires additional DuckDB link dependencies.

# HMS Integration Tests

This directory contains Docker-backed HMS integration checks.

Run locally:

```bash
./test/integration/hms/run_hms_tests.sh
```

This script is for local testing convenience. CI uses explicit workflow steps (`docker compose`, `create-hms-tables.sh`, then `make test`).

To run only the targeted HMS integration SQL test:

```bash
HMS_TEST_MODE=integration ./test/integration/hms/run_hms_tests.sh
```

What it validates:

- HMS standalone metastore container starts (`apache/hive:4.0.0`, `SERVICE_NAME=metastore`)
- Metastore Thrift endpoint is reachable on port `9083`
- Startup log marker is present (`Starting Hive Metastore Server`)

Seed fixtures only (when the HMS compose stack is already running):

```bash
./test/integration/hms/create-hms-tables.sh
```

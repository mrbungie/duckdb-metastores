# Testing the Metastore extension

This directory contains all extension tests.

- `test/sql/`: SQLLogicTests for metastore behavior.
- `test/integration/hms/`: local helpers for HMS-backed test dependencies.

DuckDB favors SQLLogicTests as the primary format, so most extension coverage should live under `test/sql/`.

## Standard test commands

Run extension tests with the project Makefile targets:

```bash
make test
```

Debug variant:

```bash
make test_debug
```

## HMS-backed local flow

The HMS scripts are local developer helpers. They start Docker dependencies and run tests in your local runtime.

Run full HMS-backed local flow:

```bash
test/integration/hms/run_hms_tests.sh
```

Run only the targeted HMS integration SQL test:

```bash
HMS_TEST_MODE=integration test/integration/hms/run_hms_tests.sh
```

Seed fixtures only (requires the HMS compose stack to already be up):

```bash
test/integration/hms/create-hms-tables.sh
```

## CI behavior

CI does not use the local helper runner script. In GitHub Actions the flow is explicit:

1. `docker compose -f test/integration/hms/docker-compose.yml up -d`
2. `test/integration/hms/create-hms-tables.sh`
3. `make test LINUX_CI_IN_DOCKER=1`

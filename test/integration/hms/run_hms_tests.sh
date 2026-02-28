#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT_DIR}/test/integration/hms/docker-compose.yml}"
HMS_SHARED_DIR="${HMS_SHARED_DIR:-${ROOT_DIR}/build/hms_shared}"
HMS_DB_NAME="${HMS_DB_NAME:-metastore_ci}"
HMS_TABLE_COUNT="${HMS_TABLE_COUNT:-5}"
HMS_TEST_MODE="${HMS_TEST_MODE:-all}"
KEEP_HMS="${KEEP_HMS:-0}"
GEN="${GEN:-ninja}"
HMS_LOCAL_ENDPOINT="${HMS_LOCAL_ENDPOINT:-127.0.0.1:9083}"

export COMPOSE_FILE
export HMS_SHARED_DIR
export HMS_DB_NAME
export HMS_TABLE_COUNT
export GEN
export HMS_LOCAL_ENDPOINT

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
	cat <<'EOF'
Run HMS-backed tests with Docker dependencies and local runtime.

Usage:
  test/integration/hms/run_hms_tests.sh

Optional env vars:
  HMS_TEST_MODE    all | integration (default: all)
  HMS_SHARED_DIR   Shared host path mounted into HMS containers
  HMS_DB_NAME      Fixture database name (default: metastore_ci)
  HMS_TABLE_COUNT  Number of fixture table pairs (default: 5)
  KEEP_HMS         Set to 1 to keep HMS containers running after tests
  GEN              Build generator for make invocations (default: ninja)
EOF
	exit 0
fi

if [[ "${HMS_TEST_MODE}" != "all" && "${HMS_TEST_MODE}" != "integration" ]]; then
	echo "HMS_TEST_MODE must be 'all' or 'integration'" >&2
	exit 1
fi

if ! docker info >/dev/null 2>&1; then
	echo "Docker daemon is not reachable." >&2
	exit 1
fi

if [[ ! -d "${ROOT_DIR}/duckdb/src/include" ]]; then
	echo "DuckDB submodule is missing. Run: git submodule update --init --recursive" >&2
	exit 1
fi

mkdir -p "${HMS_SHARED_DIR}"

cleanup() {
	if [[ "${STARTED_BY_SCRIPT}" == "1" && "${KEEP_HMS}" != "1" ]]; then
		docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans >/dev/null 2>&1 || true
	fi
}

wait_for_service_log() {
	local service="$1"
	local marker="$2"
	local label="$3"

	for idx in $(seq 1 90); do
		if ! docker compose -f "${COMPOSE_FILE}" ps --status running --services | grep -q "^${service}$"; then
			echo "${label} container is not running." >&2
			docker compose -f "${COMPOSE_FILE}" logs --no-color >&2 || true
			exit 1
		fi
		if docker compose -f "${COMPOSE_FILE}" logs --no-color "${service}" | grep -q "${marker}"; then
			return 0
		fi
		sleep 2
	done

	echo "${label} did not report readiness marker in time." >&2
	docker compose -f "${COMPOSE_FILE}" logs --no-color >&2 || true
	exit 1
}

wait_for_beeline() {
	for idx in $(seq 1 60); do
		if docker compose -f "${COMPOSE_FILE}" exec -T hms-hiveserver2 \
			bash -lc "/opt/hive/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/default' -n hive -e 'SHOW DATABASES;' >/tmp/hms_beeline_ready.log 2>&1"; then
			return 0
		fi
		sleep 2
	done

	echo "HiveServer2 did not accept beeline connections in time." >&2
	docker compose -f "${COMPOSE_FILE}" logs --no-color >&2 || true
	exit 1
}

STARTED_BY_SCRIPT="0"
running_services="$(docker compose -f "${COMPOSE_FILE}" ps --status running --services | tr '\n' ' ')"
if [[ "${running_services}" != *"hms-metastore"* ]] || [[ "${running_services}" != *"hms-hiveserver2"* ]]; then
	STARTED_BY_SCRIPT="1"
fi

trap cleanup EXIT

docker compose -f "${COMPOSE_FILE}" up -d
wait_for_service_log "hms-metastore" "Starting Hive Metastore Server" "Hive Metastore"
wait_for_service_log "hms-hiveserver2" "Starting HiveServer2" "HiveServer2"
wait_for_beeline

"${ROOT_DIR}/test/integration/hms/create-hms-tables.sh"

if [[ "${HMS_TEST_MODE}" == "integration" ]]; then
	TEST_RUNNER="${ROOT_DIR}/build/release/test/unittest"
	HMS_DUCKDB_TEST="${ROOT_DIR}/test/sql/metastore/integration/hms_duckdb_smoke_generated.test"
	if [[ ! -x "${TEST_RUNNER}" ]]; then
		echo "DuckDB test runner not found at ${TEST_RUNNER}. Run 'make' before integration tests." >&2
		exit 1
	fi
	"${TEST_RUNNER}" "${HMS_DUCKDB_TEST}"
	printf 'HMS integration checks passed\n'
	exit 0
fi

(cd "${ROOT_DIR}" && make test)
printf 'HMS-backed test run completed\n'

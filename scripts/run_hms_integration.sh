#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/test/integration/hms/docker-compose.yml"

if ! docker info >/dev/null 2>&1; then
	echo "Docker daemon is not reachable. Ensure Docker is running and your user can access /var/run/docker.sock." >&2
	exit 1
fi

if [[ ! -d "${ROOT_DIR}/duckdb/src/include" ]]; then
	echo "DuckDB submodule is missing. Run: git submodule update --init --recursive" >&2
	exit 1
fi

cleanup() {
	docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans >/dev/null 2>&1 || true
}

trap cleanup EXIT

docker compose -f "${COMPOSE_FILE}" up -d

ready="false"
for _ in $(seq 1 90); do
	if (echo >/dev/tcp/127.0.0.1/9083) >/dev/null 2>&1; then
		ready="true"
		break
	fi
	sleep 2
done

if [[ "${ready}" != "true" ]]; then
	echo "HMS metastore did not become reachable on port 9083" >&2
	docker compose -f "${COMPOSE_FILE}" logs --no-color >&2 || true
	exit 1
fi

if ! docker compose -f "${COMPOSE_FILE}" logs --no-color | grep -q "Starting Hive Metastore Server"; then
	echo "HMS logs did not show metastore startup marker" >&2
	docker compose -f "${COMPOSE_FILE}" logs --no-color >&2 || true
	exit 1
fi

if [[ "${HMS_RUN_CPP_HARNESS:-false}" == "true" ]]; then
	docker run --rm \
		-v "${ROOT_DIR}":/work \
		-w /work \
		gcc:13 \
		bash -lc "g++ -std=c++17 -Isrc/include -Isrc -Isrc/providers -Iduckdb/src/include test/integration/hms/hms_integration_harness.cpp src/providers/hms/hms_connector.cpp src/providers/hms/hms_mapper.cpp -o /tmp/hms_integration_harness && /tmp/hms_integration_harness"
fi

echo "HMS integration checks passed (container reachability + startup logs)"

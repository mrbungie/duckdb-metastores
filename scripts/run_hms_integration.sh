#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/test/integration/hms/docker-compose.yml"
HMS_SHARED_DIR="${HMS_SHARED_DIR:-${ROOT_DIR}/build/hms_shared}"
export HMS_SHARED_DIR
mkdir -p "${HMS_SHARED_DIR}"

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

metastore_ready="false"
for idx in $(seq 1 90); do
	if ! docker compose -f "${COMPOSE_FILE}" ps --status running --services | grep -q "^hms-metastore$"; then
		echo "HMS container is no longer running before startup completed" >&2
		docker compose -f "${COMPOSE_FILE}" logs --no-color >&2 || true
		exit 1
	fi

	if docker compose -f "${COMPOSE_FILE}" logs --no-color hms-metastore | grep -q "Starting Hive Metastore Server"; then
		metastore_ready="true"
		break
	fi
	if (( idx % 10 == 0 )); then
		echo "Waiting for Hive metastore startup... (${idx}/90)"
	fi
	sleep 2
done

if [[ "${metastore_ready}" != "true" ]]; then
	echo "HMS metastore did not report startup marker within timeout" >&2
	docker compose -f "${COMPOSE_FILE}" logs --no-color >&2 || true
	exit 1
fi

hiveserver_ready="false"
for idx in $(seq 1 90); do
	if ! docker compose -f "${COMPOSE_FILE}" ps --status running --services | grep -q "^hms-hiveserver2$"; then
		echo "HiveServer2 container is no longer running before startup completed" >&2
		docker compose -f "${COMPOSE_FILE}" logs --no-color >&2 || true
		exit 1
	fi

	if docker compose -f "${COMPOSE_FILE}" logs --no-color hms-hiveserver2 | grep -q "Starting HiveServer2"; then
		hiveserver_ready="true"
		break
	fi
	if (( idx % 10 == 0 )); then
		echo "Waiting for HiveServer2 startup... (${idx}/90)"
	fi
	sleep 2
done

if [[ "${hiveserver_ready}" != "true" ]]; then
	echo "HiveServer2 did not report startup marker within timeout" >&2
	docker compose -f "${COMPOSE_FILE}" logs --no-color >&2 || true
	exit 1
fi

beeline_ready="false"
for idx in $(seq 1 60); do
	if docker compose -f "${COMPOSE_FILE}" exec -T hms-hiveserver2 \
		bash -lc "/opt/hive/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/default' -n hive -e 'SHOW DATABASES;' >/tmp/hms_beeline_ready.log 2>&1"; then
		beeline_ready="true"
		break
	fi
	if (( idx % 10 == 0 )); then
		echo "Waiting for HiveServer2 beeline readiness... (${idx}/60)"
	fi
	sleep 2
done

if [[ "${beeline_ready}" != "true" ]]; then
	echo "HiveServer2 did not accept beeline connections within timeout" >&2
	docker compose -f "${COMPOSE_FILE}" logs --no-color >&2 || true
	exit 1
fi

TEST_RUNNER="${ROOT_DIR}/build/release/test/unittest"
HMS_DUCKDB_TEST="${ROOT_DIR}/test/sql/metastore/integration/hms_duckdb_smoke_generated.test"
HMS_TABLE_COUNT="${HMS_TABLE_COUNT:-5}"
HMS_DB_NAME="${HMS_DB_NAME:-metastore_ci}"
BOOTSTRAP_SQL="/tmp/hms_bootstrap_fixture.sql"

sql_payload="CREATE DATABASE IF NOT EXISTS ${HMS_DB_NAME};\nUSE ${HMS_DB_NAME};\n"
for i in $(seq 1 "${HMS_TABLE_COUNT}"); do
	tbl="fixture_tbl_${i}"
	tbl_path="${HMS_SHARED_DIR}/${HMS_DB_NAME}/${tbl}"
	rm -rf "${tbl_path}"
	mkdir -p "${tbl_path}"
	sql_payload+="DROP TABLE IF EXISTS ${tbl};\n"
	sql_payload+="CREATE EXTERNAL TABLE ${tbl}_partitioned (id INT, value STRING) PARTITIONED BY (year STRING, month STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'file:${tbl_path}_part';\n"
	sql_payload+="ALTER TABLE ${tbl}_partitioned ADD PARTITION (year='2023', month='10') LOCATION 'file:${tbl_path}_part/year=2023/month=10';\n"
	sql_payload+="INSERT INTO TABLE ${tbl}_partitioned PARTITION(year='2023', month='10') VALUES (${i}, 'v${i}');\n"
	sql_payload+="CREATE EXTERNAL TABLE ${tbl} (id INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'file:${tbl_path}';\n"
	sql_payload+="INSERT INTO TABLE ${tbl} VALUES (${i}, 'v${i}');\n"
done

docker compose -f "${COMPOSE_FILE}" exec -T hms-hiveserver2 bash -lc "printf '%b' \"${sql_payload}\" > ${BOOTSTRAP_SQL}"
docker compose -f "${COMPOSE_FILE}" exec -T hms-hiveserver2 bash -lc "/opt/hive/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/default' -n hive -f ${BOOTSTRAP_SQL}"

if [[ ! -x "${TEST_RUNNER}" ]]; then
	echo "DuckDB test runner not found at ${TEST_RUNNER}. Run 'make' before integration tests." >&2
	exit 1
fi

HMS_TABLE_COUNT="${HMS_TABLE_COUNT}" HMS_DB_NAME="${HMS_DB_NAME}" "${TEST_RUNNER}" "${HMS_DUCKDB_TEST}"

echo "HMS integration checks passed"

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

if ! (echo >/dev/tcp/127.0.0.1/9083) >/dev/null 2>&1; then
	echo "HMS metastore startup marker found, but port 9083 is not reachable" >&2
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

if ! (echo >/dev/tcp/127.0.0.1/10000) >/dev/null 2>&1; then
	echo "HiveServer2 startup marker found, but port 10000 is not reachable" >&2
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
	docker compose -f "${COMPOSE_FILE}" exec -T hms-hiveserver2 bash -lc "cat /tmp/hms_beeline_ready.log" >&2 || true
	exit 1
fi

TEST_RUNNER="${ROOT_DIR}/build/release/test/unittest"
HMS_DUCKDB_TEST="test/sql/metastore/integration/hms_duckdb_smoke_generated.test"
HMS_TABLE_FORMAT="${HMS_TABLE_FORMAT:-csv}"
HMS_TABLE_COUNT="${HMS_TABLE_COUNT:-5}"
HMS_DB_NAME="${HMS_DB_NAME:-metastore_ci}"
BOOTSTRAP_SQL="/tmp/hms_bootstrap_fixture.sql"

if ! [[ "${HMS_TABLE_COUNT}" =~ ^[1-9][0-9]*$ ]]; then
	echo "HMS_TABLE_COUNT must be a positive integer, got '${HMS_TABLE_COUNT}'" >&2
	exit 1
fi

if [[ "${HMS_TABLE_FORMAT}" != "csv" ]]; then
	echo "Unsupported HMS_TABLE_FORMAT='${HMS_TABLE_FORMAT}'. Currently supported: csv" >&2
	echo "Add another case in scripts/run_hms_integration.sh to extend formats." >&2
	exit 1
fi

sql_payload="CREATE DATABASE IF NOT EXISTS ${HMS_DB_NAME};\nUSE ${HMS_DB_NAME};\n"
for i in $(seq 1 "${HMS_TABLE_COUNT}"); do
	tbl="fixture_tbl_${i}"
	tbl_path="${HMS_SHARED_DIR}/${HMS_DB_NAME}/${tbl}"
	rm -rf "${tbl_path}"
	mkdir -p "${tbl_path}"
	sql_payload+="DROP TABLE IF EXISTS ${tbl};\n"
	sql_payload+="CREATE EXTERNAL TABLE ${tbl} (id INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'file:${tbl_path}';\n"
	sql_payload+="INSERT INTO TABLE ${tbl} VALUES (${i}, 'v${i}');\n"
done

docker compose -f "${COMPOSE_FILE}" exec -T hms-hiveserver2 bash -lc "printf '%b' \"${sql_payload}\" > ${BOOTSTRAP_SQL}"
docker compose -f "${COMPOSE_FILE}" exec -T hms-hiveserver2 bash -lc "/opt/hive/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/default' -n hive -f ${BOOTSTRAP_SQL}"

count_expr=""
format_union=""
for i in $(seq 1 "${HMS_TABLE_COUNT}"); do
	tbl="fixture_tbl_${i}"
	count_term="(SELECT COUNT(*) FROM metastore_scan('hms', '${HMS_DB_NAME}', '${tbl}'))"
	if [[ -z "${count_expr}" ]]; then
		count_expr="${count_term}"
	else
		count_expr+=" + ${count_term}"
	fi
	format_query="SELECT * FROM metastore_scan('hms', '${HMS_DB_NAME}', '${tbl}') WHERE lower(format) = '${HMS_TABLE_FORMAT}'"
	if [[ -z "${format_union}" ]]; then
		format_union="${format_query}"
	else
		format_union+=" UNION ALL ${format_query}"
	fi
done

test_payload="# name: hms_duckdb_smoke_generated\n"
test_payload+="# description: generated HMS + DuckDB integration checks\n"
test_payload+="# group: [sql]\n\n"
test_payload+="require metastore\n\n"
test_payload+="statement ok\nCREATE TEMP TABLE duckdb_smoke(i INTEGER);\n\n"
test_payload+="statement ok\nINSERT INTO duckdb_smoke VALUES (7), (11);\n\n"
test_payload+="query I\nSELECT SUM(i) FROM duckdb_smoke;\n----\n18\n\n"
test_payload+="statement ok\nATTACH 'thrift://127.0.0.1:9083' AS hms (TYPE metastore);\n\n"
test_payload+="query I\nSELECT ${count_expr};\n----\n${HMS_TABLE_COUNT}\n\n"
test_payload+="query I\nSELECT COUNT(*) FROM (${format_union}) t;\n----\n${HMS_TABLE_COUNT}\n\n"
test_payload+="query I\nSELECT COUNT(*) FROM hms.${HMS_DB_NAME}.fixture_tbl_1;\n----\n1\n\n"
test_payload+="query I\nSELECT SUM(id) FROM hms.${HMS_DB_NAME}.fixture_tbl_1;\n----\n1\n\n"
test_payload+="query T\nSELECT value FROM hms.${HMS_DB_NAME}.fixture_tbl_1 WHERE id = 1;\n----\nv1\n\n"
test_payload+="statement error\nINSERT INTO hms.${HMS_DB_NAME}.fixture_tbl_1 VALUES (100, 'duckdb_write');\n----\n"

printf '%b' "${test_payload}" > "${HMS_DUCKDB_TEST}"

if [[ ! -x "${TEST_RUNNER}" ]]; then
	echo "DuckDB test runner not found at ${TEST_RUNNER}. Run 'make' before integration tests." >&2
	exit 1
fi

HMS_TABLE_COUNT="${HMS_TABLE_COUNT}" HMS_DB_NAME="${HMS_DB_NAME}" "${TEST_RUNNER}" "${HMS_DUCKDB_TEST}"

if [[ "${HMS_RUN_CPP_HARNESS:-false}" == "true" ]]; then
	docker run --rm \
		-v "${ROOT_DIR}":/work \
		-w /work \
		gcc:13 \
		bash -lc "g++ -std=c++17 -Isrc/include -Isrc -Isrc/providers -Iduckdb/src/include test/integration/hms/hms_integration_harness.cpp src/providers/hms/hms_connector.cpp src/providers/hms/hms_mapper.cpp -o /tmp/hms_integration_harness && /tmp/hms_integration_harness"
fi

echo "HMS integration checks passed (container reachability + startup logs)"

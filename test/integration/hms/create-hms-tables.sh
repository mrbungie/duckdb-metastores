#!/usr/bin/env bash
set -euo pipefail

umask 000

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT_DIR}/test/integration/hms/docker-compose.yml}"
HMS_SHARED_DIR="${HMS_SHARED_DIR:-${ROOT_DIR}/build/hms_shared}"
HMS_DB_NAME="${HMS_DB_NAME:-metastore_ci}"
HMS_TABLE_COUNT="${HMS_TABLE_COUNT:-5}"
BOOTSTRAP_SQL="/tmp/hms_bootstrap_fixture.sql"

mkdir -p "${HMS_SHARED_DIR}"
chmod -R 0777 "${HMS_SHARED_DIR}"

if ! docker info >/dev/null 2>&1; then
	echo "Docker daemon is not reachable." >&2
	exit 1
fi

if ! docker compose -f "${COMPOSE_FILE}" ps --status running --services | grep -q "^hms-hiveserver2$"; then
	echo "hms-hiveserver2 is not running. Start the compose stack first." >&2
	exit 1
fi

for idx in $(seq 1 60); do
	if docker compose -f "${COMPOSE_FILE}" exec -T hms-hiveserver2 \
		bash -lc "/opt/hive/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/default' -n hive -e 'SHOW DATABASES;' >/tmp/hms_beeline_ready.log 2>&1"; then
		break
	fi
	if [[ "${idx}" == "60" ]]; then
		echo "HiveServer2 beeline endpoint did not become ready in time." >&2
		docker compose -f "${COMPOSE_FILE}" logs --no-color hms-hiveserver2 >&2 || true
		exit 1
	fi
	sleep 2
done

echo "Generating HMS fixture tables in database '${HMS_DB_NAME}'..."

sql_payload="CREATE DATABASE IF NOT EXISTS ${HMS_DB_NAME};\nUSE ${HMS_DB_NAME};\n"

for i in $(seq 1 "${HMS_TABLE_COUNT}"); do
	tbl="fixture_tbl_${i}"
	tbl_path="${HMS_SHARED_DIR}/${HMS_DB_NAME}/${tbl}"
	rm -rf "${tbl_path}" "${tbl_path}_part"
	mkdir -p "${tbl_path}" "${tbl_path}_part/year=2023/month=10"
	chmod -R 0777 "${tbl_path}" "${tbl_path}_part"
	sql_payload+="DROP TABLE IF EXISTS ${tbl};\n"
	sql_payload+="CREATE EXTERNAL TABLE ${tbl} (id INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'file:${tbl_path}';\n"
	sql_payload+="INSERT INTO TABLE ${tbl} VALUES (${i}, 'v${i}');\n"
	sql_payload+="DROP TABLE IF EXISTS ${tbl}_partitioned;\n"
	sql_payload+="CREATE EXTERNAL TABLE ${tbl}_partitioned (id INT, value STRING) PARTITIONED BY (year STRING, month STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'file:${tbl_path}_part';\n"
	sql_payload+="ALTER TABLE ${tbl}_partitioned ADD PARTITION (year='2023', month='10') LOCATION 'file:${tbl_path}_part/year=2023/month=10';\n"
	sql_payload+="INSERT INTO TABLE ${tbl}_partitioned PARTITION(year='2023', month='10') VALUES (${i}, 'v${i}');\n"
done

part_tbl="fixture_tbl_partitioned"
part_tbl_path="${HMS_SHARED_DIR}/${HMS_DB_NAME}/${part_tbl}"
rm -rf "${part_tbl_path}"
mkdir -p "${part_tbl_path}"
chmod -R 0777 "${part_tbl_path}"
sql_payload+="DROP TABLE IF EXISTS ${part_tbl};\n"
sql_payload+="CREATE EXTERNAL TABLE ${part_tbl} (id INT, value STRING) PARTITIONED BY (year STRING, month STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'file:${part_tbl_path}';\n"
sql_payload+="ALTER TABLE ${part_tbl} ADD PARTITION (year='2023', month='10') LOCATION 'file:${part_tbl_path}/year=2023/month=10';\n"
sql_payload+="ALTER TABLE ${part_tbl} ADD PARTITION (year='2024', month='01') LOCATION 'file:${part_tbl_path}/year=2024/month=01';\n"
sql_payload+="INSERT INTO TABLE ${part_tbl} PARTITION(year='2023', month='10') VALUES (1, 'old');\n"
sql_payload+="INSERT INTO TABLE ${part_tbl} PARTITION(year='2024', month='01') VALUES (2, 'new');\n"
for extra_idx in $(seq 1 8); do
	extra_year=$((2010 + extra_idx))
	extra_month=$(printf "%02d" "${extra_idx}")
	extra_id=$((100 + extra_idx))
	sql_payload+="ALTER TABLE ${part_tbl} ADD PARTITION (year='${extra_year}', month='${extra_month}') LOCATION 'file:${part_tbl_path}/year=${extra_year}/month=${extra_month}';\n"
	sql_payload+="INSERT INTO TABLE ${part_tbl} PARTITION(year='${extra_year}', month='${extra_month}') VALUES (${extra_id}, 'extra_${extra_idx}');\n"
done

part_tbl_parquet="fixture_tbl_partitioned_parquet"
part_tbl_parquet_path="${HMS_SHARED_DIR}/${HMS_DB_NAME}/${part_tbl_parquet}"
rm -rf "${part_tbl_parquet_path}"
mkdir -p "${part_tbl_parquet_path}"
chmod -R 0777 "${part_tbl_parquet_path}"
sql_payload+="DROP TABLE IF EXISTS ${part_tbl_parquet};\n"
sql_payload+="CREATE EXTERNAL TABLE ${part_tbl_parquet} (id INT, value STRING) PARTITIONED BY (year STRING, month STRING) STORED AS PARQUET LOCATION 'file:${part_tbl_parquet_path}';\n"
sql_payload+="ALTER TABLE ${part_tbl_parquet} ADD PARTITION (year='2023', month='10') LOCATION 'file:${part_tbl_parquet_path}/year=2023/month=10';\n"
sql_payload+="ALTER TABLE ${part_tbl_parquet} ADD PARTITION (year='2024', month='01') LOCATION 'file:${part_tbl_parquet_path}/year=2024/month=01';\n"
sql_payload+="INSERT INTO TABLE ${part_tbl_parquet} PARTITION(year='2023', month='10') VALUES (11, 'old_parquet');\n"
sql_payload+="INSERT INTO TABLE ${part_tbl_parquet} PARTITION(year='2024', month='01') VALUES (22, 'new_parquet');\n"
for extra_idx in $(seq 1 8); do
	extra_year=$((2010 + extra_idx))
	extra_month=$(printf "%02d" "${extra_idx}")
	extra_id=$((200 + extra_idx))
	sql_payload+="ALTER TABLE ${part_tbl_parquet} ADD PARTITION (year='${extra_year}', month='${extra_month}') LOCATION 'file:${part_tbl_parquet_path}/year=${extra_year}/month=${extra_month}';\n"
	sql_payload+="INSERT INTO TABLE ${part_tbl_parquet} PARTITION(year='${extra_year}', month='${extra_month}') VALUES (${extra_id}, 'extra_parquet_${extra_idx}');\n"
done

printf '%b' "${sql_payload}" > "${BOOTSTRAP_SQL}"

echo "Applying schema to HMS via HiveServer2 beeline..."
docker compose -f "${COMPOSE_FILE}" exec -T hms-hiveserver2 \
	bash -lc "cat > ${BOOTSTRAP_SQL}" < "${BOOTSTRAP_SQL}"
docker compose -f "${COMPOSE_FILE}" exec -T hms-hiveserver2 \
	bash -lc "/opt/hive/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/default' -n hive -f ${BOOTSTRAP_SQL}"

echo "HMS Seed Data Loaded!"

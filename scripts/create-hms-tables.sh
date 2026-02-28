#!/usr/bin/env bash
set -euo pipefail

# This script mimics the postgres scanner setup: it assumes the HMS server is ALREADY running.
# It simply connects via Beeline and populates the schema.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HMS_SHARED_DIR="${HMS_SHARED_DIR:-${ROOT_DIR}/build/hms_shared}"
HMS_DB_NAME="${HMS_DB_NAME:-metastore_ci}"
HMS_HOST="${HMS_HOST:-127.0.0.1}"
HMS_PORT="${HMS_PORT:-10000}"
BOOTSTRAP_SQL="/tmp/hms_bootstrap_fixture.sql"

mkdir -p "${HMS_SHARED_DIR}"

echo "Generating HMS fixture tables..."

sql_payload="CREATE DATABASE IF NOT EXISTS ${HMS_DB_NAME};\nUSE ${HMS_DB_NAME};\n"

# Standard Flat Tables
for i in $(seq 1 5); do
	tbl="fixture_tbl_${i}"
	tbl_path="${HMS_SHARED_DIR}/${HMS_DB_NAME}/${tbl}"
	rm -rf "${tbl_path}"
	mkdir -p "${tbl_path}"
	sql_payload+="DROP TABLE IF EXISTS ${tbl};\n"
	sql_payload+="CREATE EXTERNAL TABLE ${tbl} (id INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'file:${tbl_path}';\n"
	sql_payload+="INSERT INTO TABLE ${tbl} VALUES (${i}, 'v${i}');\n"
done

# Partitioned Parquet Tables for Pushdown testing
part_tbl="fixture_tbl_partitioned"
part_tbl_path="${HMS_SHARED_DIR}/${HMS_DB_NAME}/${part_tbl}"
rm -rf "${part_tbl_path}"
mkdir -p "${part_tbl_path}"
sql_payload+="DROP TABLE IF EXISTS ${part_tbl};\n"
sql_payload+="CREATE EXTERNAL TABLE ${part_tbl} (id INT, value STRING) PARTITIONED BY (year STRING, month STRING) STORED AS PARQUET LOCATION 'file:${part_tbl_path}';\n"
sql_payload+="ALTER TABLE ${part_tbl} ADD PARTITION (year='2023', month='10') LOCATION 'file:${part_tbl_path}/year=2023/month=10';\n"
sql_payload+="ALTER TABLE ${part_tbl} ADD PARTITION (year='2024', month='01') LOCATION 'file:${part_tbl_path}/year=2024/month=01';\n"
sql_payload+="INSERT INTO TABLE ${part_tbl} PARTITION(year='2023', month='10') VALUES (1, 'old');\n"
sql_payload+="INSERT INTO TABLE ${part_tbl} PARTITION(year='2024', month='01') VALUES (2, 'new');\n"

printf '%b' "${sql_payload}" > "${BOOTSTRAP_SQL}"

echo "Applying schema to HMS via Beeline at ${HMS_HOST}:${HMS_PORT}..."
# In a local/CI env without orchestrated docker execution, we just execute beeline via docker run, 
# or if it's already orchestrated, we run it. We assume docker is available to just run the client if beeline isn't local.
docker run --rm --network host -v /tmp:/tmp -v "${HMS_SHARED_DIR}:${HMS_SHARED_DIR}" apache/hive:4.0.0-alpha-2 \
    /opt/hive/bin/beeline -u "jdbc:hive2://${HMS_HOST}:${HMS_PORT}/default" -n hive -f "${BOOTSTRAP_SQL}"

echo "HMS Seed Data Loaded!"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HMS_DUCKDB_TEST="${ROOT_DIR}/test/sql/metastore/integration/hms_duckdb_smoke_generated.test"
HMS_TABLE_FORMAT="${HMS_TABLE_FORMAT:-csv}"
HMS_TABLE_COUNT="${HMS_TABLE_COUNT:-5}"
HMS_DB_NAME="${HMS_DB_NAME:-metastore_ci}"

if ! [[ "${HMS_TABLE_COUNT}" =~ ^[1-9][0-9]*$ ]]; then
	echo "HMS_TABLE_COUNT must be a positive integer, got '${HMS_TABLE_COUNT}'" >&2
	exit 1
fi

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
test_payload+="query II\nEXPLAIN SELECT * FROM hms.${HMS_DB_NAME}.fixture_tbl_1 WHERE id = 1;\n----\n"
test_payload+="logical_plan\t<REGEX>:.*metastore_read.*\n\n"
test_payload+="query I\nSELECT ${count_expr};\n----\n${HMS_TABLE_COUNT}\n\n"
test_payload+="query I\nSELECT COUNT(*) FROM (${format_union}) t;\n----\n${HMS_TABLE_COUNT}\n\n"
test_payload+="query I\nSELECT COUNT(*) FROM hms.${HMS_DB_NAME}.fixture_tbl_1;\n----\n1\n\n"
test_payload+="query I\nSELECT SUM(id) FROM hms.${HMS_DB_NAME}.fixture_tbl_1;\n----\n1\n\n"
test_payload+="query T\nSELECT value FROM hms.${HMS_DB_NAME}.fixture_tbl_1 WHERE id = 1;\n----\nv1\n\n"
test_payload+="statement error\nINSERT INTO hms.${HMS_DB_NAME}.fixture_tbl_1 VALUES (100, 'duckdb_write');\n----\n"

printf '%b' "${test_payload}" > "${HMS_DUCKDB_TEST}"
echo "Generated ${HMS_DUCKDB_TEST}"

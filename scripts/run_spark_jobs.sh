#!/bin/bash
set -euo pipefail

export PYTHONPATH="/opt/spark/work-dir/spark"
SPARK_SUBMIT="/opt/spark/bin/spark-submit"
# JDBC-драйверы уже в образе (/opt/spark/jars)

echo "Waiting for PostgreSQL..."
until python3 - <<'PY'
import socket
s = socket.socket()
s.settimeout(1)
s.connect(("postgres", 5432))
PY
do
  sleep 2
done

echo "Waiting for ClickHouse..."
until wget -qO- "http://clickhouse:8123/ping" >/dev/null 2>&1; do
  sleep 2
done

COMMON_OPTS=(
  --master "local[2]"
  --conf spark.sql.legacy.timeParserPolicy=LEGACY
)

echo "=== Job 01: load_dwh ==="
"${SPARK_SUBMIT}" "${COMMON_OPTS[@]}" /opt/spark/work-dir/spark/jobs/01_load_dwh.py

echo "=== Job 02: clickhouse marts ==="
"${SPARK_SUBMIT}" "${COMMON_OPTS[@]}" /opt/spark/work-dir/spark/jobs/02_build_clickhouse_marts.py

echo "Spark jobs finished."

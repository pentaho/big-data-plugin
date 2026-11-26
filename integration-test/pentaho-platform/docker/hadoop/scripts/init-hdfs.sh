#!/usr/bin/env bash
#
# Helper to (re)initialise a single-node HDFS inside the integration-test Hadoop container and
# create the base directories used by the tests.
#
# This script is staged at /opt/hadoop/it-scripts and run after HDFS and YARN start.
#
set -euo pipefail

HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export PATH="${HADOOP_HOME}/bin:${PATH}"

echo "[it] Waiting for HDFS to be available..."
for _ in $(seq 1 60); do
  if hdfs dfs -ls / >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "[it] Creating base test directories in HDFS"
hdfs dfs -mkdir -p /it/hdfs
hdfs dfs -mkdir -p /it/formats
hdfs dfs -chmod -R 777 /it

echo "[it] HDFS ready:"
hdfs dfs -ls -R /it

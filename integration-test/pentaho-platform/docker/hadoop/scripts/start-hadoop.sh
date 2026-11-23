#!/usr/bin/env bash
set -euo pipefail

export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
cp /opt/hadoop/it-config/*.xml "$HADOOP_CONF_DIR/"

if [ ! -d /tmp/hdfs/name/current ]; then
  "$HADOOP_HOME/bin/hdfs" namenode -format -force -nonInteractive
fi
"$HADOOP_HOME/bin/hdfs" --daemon start namenode

hdfs_ready=false
for _ in $(seq 1 60); do
  if "$HADOOP_HOME/bin/hdfs" dfs -ls / >/dev/null 2>&1; then
    hdfs_ready=true
    break
  fi
  sleep 1
done
if [ "$hdfs_ready" != true ]; then
  echo "HDFS did not become ready" >&2
  exit 1
fi

"$HADOOP_HOME/bin/hdfs" --daemon start datanode
"$HADOOP_HOME/bin/yarn" --daemon start resourcemanager
"$HADOOP_HOME/bin/yarn" --daemon start nodemanager

yarn_ready=false
for _ in $(seq 1 60); do
  if "$HADOOP_HOME/bin/yarn" node -list 2>/dev/null | grep -q "Total Nodes:1"; then
    yarn_ready=true
    break
  fi
  sleep 2
done
if [ "$yarn_ready" != true ]; then
  echo "YARN did not register a node" >&2
  exit 1
fi

tr -d '\r' < /opt/hadoop/it-scripts/init-hdfs.sh | bash
echo "YARN ready"
exec tail -f /dev/null

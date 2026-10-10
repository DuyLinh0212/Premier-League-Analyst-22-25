#!/usr/bin/env bash
# Stop the three extra/primary DataNodes and NameNode services cleanly.
set -euo pipefail

HADOOP_HOME="${HADOOP_HOME:-/home/linh/hadoop}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
HADOOP_LOG_DIR="${HADOOP_LOG_DIR:-${HADOOP_HOME}/logs}"
HADOOP_PID_DIR="${HADOOP_PID_DIR:-/tmp}"
export HADOOP_HOME JAVA_HOME HADOOP_HDFS_HOME="${HADOOP_HOME}" HADOOP_LOG_DIR HADOOP_PID_DIR

stop_extra_datanode() {
  local id="$1"
  local conf="$2"
  HADOOP_IDENT_STRING="${id}" "${HADOOP_HOME}/bin/hdfs" --config "${conf}" --daemon stop datanode || true
}

stop_extra_datanode dn3 "${DN3_CONF:-/home/linh/hadoop-multi/dn3}"
stop_extra_datanode dn2 "${DN2_CONF:-/home/linh/hadoop-multi/dn2}"
"${HADOOP_HOME}/sbin/stop-dfs.sh" || true

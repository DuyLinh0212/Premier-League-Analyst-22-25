#!/usr/bin/env bash
# Start the existing three-DataNode HDFS layout used by this project.
set -euo pipefail

HADOOP_HOME="${HADOOP_HOME:-/home/linh/hadoop}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
HADOOP_LOG_DIR="${HADOOP_LOG_DIR:-${HADOOP_HOME}/logs}"
HADOOP_PID_DIR="${HADOOP_PID_DIR:-/tmp}"
DN2_CONF="${DN2_CONF:-/home/linh/hadoop-multi/dn2}"
DN3_CONF="${DN3_CONF:-/home/linh/hadoop-multi/dn3}"
DN1_DATA_DIR="${DN1_DATA_DIR:-/home/linh/hadoop_data/datanode1}"
DN2_DATA_DIR="${DN2_DATA_DIR:-/home/linh/hadoop_data/datanode2}"
DN3_DATA_DIR="${DN3_DATA_DIR:-/home/linh/hadoop_data/datanode3}"

export HADOOP_HOME JAVA_HOME HADOOP_HDFS_HOME="${HADOOP_HOME}" HADOOP_LOG_DIR HADOOP_PID_DIR

check_datanode_config() {
  local id="$1"
  local conf="$2"
  local data_dir="$3"
  if [[ ! -d "${data_dir}/current" ]]; then
    echo "DataNode ${id} data directory is missing: ${data_dir}" >&2
    exit 1
  fi
  if ! grep -Fq "<value>file://${data_dir}</value>" "${conf}/hdfs-site.xml"; then
    echo "DataNode ${id} config does not point to ${data_dir}: ${conf}/hdfs-site.xml" >&2
    exit 1
  fi
}

check_datanode_config dn1 "${HADOOP_HOME}/etc/hadoop" "${DN1_DATA_DIR}"
check_datanode_config dn2 "${DN2_CONF}" "${DN2_DATA_DIR}"
check_datanode_config dn3 "${DN3_CONF}" "${DN3_DATA_DIR}"
echo "Using DataNode data directories: ${DN1_DATA_DIR}, ${DN2_DATA_DIR}, ${DN3_DATA_DIR}"

if [[ "${PLCA_LOW_MEMORY:-0}" == "1" ]]; then
  # This local three-node lab shares 4 GiB with Docker and Spark. No formatting
  # or data-directory changes: only limit daemon heaps at startup.
  export HADOOP_HEAPSIZE_MAX=256m
  for task_daemon in namenode datanode secondarynamenode; do
    setsid -f env JAVA_HOME="${JAVA_HOME}" HADOOP_HOME="${HADOOP_HOME}" \
      HADOOP_HDFS_HOME="${HADOOP_HOME}" HADOOP_LOG_DIR="${HADOOP_LOG_DIR}" \
      HADOOP_PID_DIR="${HADOOP_PID_DIR}" HADOOP_HEAPSIZE_MAX=256m \
      "${HADOOP_HOME}/bin/hdfs" --daemon start "${task_daemon}" >/dev/null 2>&1
  done
else
  "${HADOOP_HOME}/sbin/start-dfs.sh"
fi

start_extra_datanode() {
  local id="$1"
  local conf="$2"
  local pid_file="${HADOOP_PID_DIR}/hadoop-${id}-datanode.pid"

  if [[ -f "${pid_file}" ]] && kill -0 "$(cat "${pid_file}")" 2>/dev/null; then
    echo "DataNode ${id} is already running"
    return
  fi
  rm -f "${pid_file}"
  # setsid keeps the daemon independent of the WSL shell that launched it.
  setsid -f env \
    JAVA_HOME="${JAVA_HOME}" HADOOP_HOME="${HADOOP_HOME}" \
    HADOOP_HDFS_HOME="${HADOOP_HOME}" HADOOP_LOG_DIR="${HADOOP_LOG_DIR}" \
    HADOOP_PID_DIR="${HADOOP_PID_DIR}" HADOOP_IDENT_STRING="${id}" \
    "${HADOOP_HOME}/bin/hdfs" --config "${conf}" --daemon start datanode \
    >/dev/null 2>&1
  echo "Started DataNode ${id}"
}

start_extra_datanode dn2 "${DN2_CONF}"
start_extra_datanode dn3 "${DN3_CONF}"

for attempt in {1..30}; do
  report="$(${HADOOP_HOME}/bin/hdfs dfsadmin -report 2>/dev/null || true)"
  if grep -q 'Live datanodes (3)' <<<"${report}"; then
    echo "HDFS has all 3 DataNodes live"
    break
  fi
  if [[ "${attempt}" == 30 ]]; then
    echo "HDFS did not reach 3 live DataNodes; inspect dfsadmin -report" >&2
    exit 1
  fi
  sleep 1
done

safe_mode="$(${HADOOP_HOME}/bin/hdfs dfsadmin -safemode get 2>/dev/null || true)"
if grep -q 'Safe mode is ON' <<<"${safe_mode}"; then
  report="$(${HADOOP_HOME}/bin/hdfs dfsadmin -report 2>/dev/null || true)"
  if grep -q 'Missing blocks: 0' <<<"${report}" && grep -q 'Blocks with corrupt replicas: 0' <<<"${report}"; then
    "${HADOOP_HOME}/bin/hdfs" dfsadmin -safemode leave
  else
    echo "HDFS remains in safe mode because the report contains missing/corrupt blocks" >&2
    exit 1
  fi
fi

"${HADOOP_HOME}/bin/hdfs" dfsadmin -safemode get
"${HADOOP_HOME}/bin/hdfs" dfsadmin -report | grep -E 'Live datanodes|Name: 127|Hostname' -A2
